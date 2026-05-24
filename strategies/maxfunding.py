from __future__ import annotations

import csv
import platform
from time import perf_counter
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_CEILING
from pathlib import Path
from typing import Any

import requests
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy

from models.maxfunding_xgb import MaxFundingXgbScorer


def score_multiplier(
    score: Decimal,
    threshold: Decimal,
    min_multiplier: Decimal,
    base_score: Decimal,
    max_score: Decimal,
    max_multiplier: Decimal,
) -> Decimal:
    if score <= threshold:
        return min_multiplier
    if score < base_score:
        span = max(base_score - threshold, Decimal("0.0001"))
        weight = (score - threshold) / span
        return min_multiplier + (Decimal("1") - min_multiplier) * weight
    span = max(max_score - base_score, Decimal("0.0001"))
    weight = min((score - base_score) / span, Decimal("1"))
    return Decimal("1") + (max_multiplier - Decimal("1")) * weight


class MaxFundingConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    trade_symbols: list[str] | str
    exclude_symbols: list[str]
    trade_notional: Decimal
    notional_min_multiplier: Decimal
    notional_base_score_bps: Decimal
    notional_max_score_bps: Decimal
    notional_max_multiplier: Decimal
    max_trades: int
    min_rate_bps: Decimal
    pre_sec: float
    entry_sec: float
    pre_deadline: float
    entry_before: float
    exit_sec: float
    post_sec: float
    stop_close: bool
    api_url: str
    api_timeout: float
    proxy_url: str
    funding_income_delay_sec: float
    xgb_models: list[dict[str, Any]]
    xgb_primary: str
    event_log_path: str = "auto"


class MaxFundingStrategy(Strategy):
    WARMUP_TIMER = "maxfunding_warmup"
    PRE_TIMER = "maxfunding_pre"
    FREEZE_TIMER = "maxfunding_freeze"
    RATE_TIMER = "maxfunding_rate"
    CLOSE_TIMER = "maxfunding_close"
    POST_TIMER = "maxfunding_post"
    TIMERS = (WARMUP_TIMER, PRE_TIMER, FREEZE_TIMER, RATE_TIMER, CLOSE_TIMER, POST_TIMER)

    def __init__(self, config: MaxFundingConfig) -> None:
        super().__init__(config)
        self.notional = Decimal(str(config.trade_notional))
        self.min_mult = Decimal(str(config.notional_min_multiplier))
        self.base_score = Decimal(str(config.notional_base_score_bps))
        self.max_score = Decimal(str(config.notional_max_score_bps))
        self.max_mult = Decimal(str(config.notional_max_multiplier))
        self.max_trades = int(config.max_trades)
        self.min_rate = Decimal(str(config.min_rate_bps)) / Decimal("10000")
        self.exit_sec = float(config.exit_sec)
        self.trade = None if config.trade_symbols == "all" else {
            symbol.upper().replace("-PERP.BINANCE", "").replace("USDT", "").replace("/", "")
            for symbol in config.trade_symbols
        }
        self.exclude = {
            symbol.upper().replace("-PERP.BINANCE", "").replace("USDT", "").replace("/", "")
            for symbol in config.exclude_symbols
        }
        self.fund_ns = 0
        self.entry_done = False
        self.sent_done = False
        self.close_done = False
        self.ins: dict[InstrumentId, Instrument] = {}
        self.trade_ids: set[InstrumentId] = set()
        self.symbols: dict[InstrumentId, str] = {}
        self.obs_map: dict[InstrumentId, dict[str, Any]] = {}
        self.ins_map: dict[InstrumentId, dict[str, Any]] = {}
        self.open_ids: set[InstrumentId] = set()
        self.order_map: dict[ClientOrderId, InstrumentId] = {}
        self.had_order = False
        self.close_count = 0
        self.trade_written = False
        self.close_submit_ns: dict[InstrumentId, int] = {}
        self.log_path = Path(config.event_log_path)
        use_proxy = platform.system() == "Windows" and config.proxy_url
        self.proxies = {"http": config.proxy_url, "https": config.proxy_url} if use_proxy else None
        self.xgb = (
            MaxFundingXgbScorer(
                config.xgb_models,
                config.xgb_primary,
                api_url=config.api_url,
                api_timeout=config.api_timeout,
                proxies=self.proxies,
            )
            if config.xgb_models
            else None
        )
        if config.xgb_primary and self.xgb is None:
            raise RuntimeError("xgb_primary requires xgb_models")
        self.xgb_columns = self.xgb.metric_columns if self.xgb is not None else []
        self.event_columns = [
            "symbol",
            "funding_time",
            "close_order_submit_time",
            "xgb_primary_model",
            "xgb_primary_score",
            "xgb_primary_pass",
            "notional_multiplier",
            "order_notional",
            *self.xgb_columns,
        ]

    # 启动时注册 NT 定时器，资金费和价格用 REST 观察列表快照。
    def on_start(self) -> None:
        if self.config.entry_sec <= self.config.entry_before:
            raise RuntimeError("entry_sec must be greater than entry_before")
        if self.config.pre_sec <= self.config.entry_sec:
            raise RuntimeError("pre_sec must be greater than entry_sec")
        if self.config.pre_deadline <= 0:
            raise RuntimeError("pre_deadline must be positive")
        if self.config.entry_before <= 0:
            raise RuntimeError("entry_before must be positive")
        if self.config.exit_sec < 0:
            raise RuntimeError("exit_sec must be positive")
        if self.config.post_sec <= self.config.exit_sec:
            raise RuntimeError("post_sec must be greater than exit_sec")
        if self.config.api_timeout <= 0:
            raise RuntimeError("api_timeout must be positive")
        if self.config.max_trades <= 0:
            raise RuntimeError("max_trades must be positive")
        if self.notional <= 0:
            raise RuntimeError("trade_notional must be positive")
        if self.min_mult <= 0:
            raise RuntimeError("notional_min_multiplier must be positive")
        if self.max_mult < self.min_mult:
            raise RuntimeError("notional_max_multiplier must be greater than or equal to notional_min_multiplier")
        if self.max_score <= self.base_score:
            raise RuntimeError("notional_max_score_bps must be greater than notional_base_score_bps")
        if self.config.funding_income_delay_sec <= self.config.exit_sec:
            raise RuntimeError("funding_income_delay_sec must be greater than exit_sec")
        if self.config.post_sec < self.config.funding_income_delay_sec:
            raise RuntimeError("post_sec must be greater than or equal to funding_income_delay_sec")

        self._load_ins()
        self._load_lists()
        self._init_log()

        self._schedule_next()

        self.log.info(
            f"资金费率交易启动，已加载{len(self.ins)}个，"
            f"可交易{len(self.trade_ids)}个，"
            f"排除{len(self.exclude)}个，"
            f"阈值{self._bps(self.min_rate)}bps，"
            f"最多交易{self.max_trades}个，基准名义{self.notional:.2f}USDT，"
            f"XGB主模型{self.config.xgb_primary or '未启用'}"
        )

    # clock alert 负责 t-3/t-2/t-1/t/t+2/t+3 六个确定动作。
    def _on_time(self, event: TimeEvent) -> None:
        name = event.name.split(":", 1)[0]
        if name == self.WARMUP_TIMER:
            self._warmup_funding()
        elif name == self.PRE_TIMER:
            self._pre_funding()
        elif name == self.FREEZE_TIMER:
            self._freeze_funding()
        elif name == self.RATE_TIMER:
            self._close_funding()
        elif name == self.CLOSE_TIMER:
            self._close_funding()
        elif name == self.POST_TIMER:
            self._post_funding()

    # t-3 预拉候选，给 t-2 失败时兜底。
    def _warmup_funding(self) -> None:
        if self.sent_done:
            return
        self.obs_map.clear()
        self.ins_map.clear()
        stats = self._load_snap("pre")
        if stats is None:
            return
        self._log_pre_result(stats, "预拉资金费率成功")
        if self.clock.timestamp_ns() < self.fund_ns - int(self.config.entry_before * 1_000_000_000):
            self.entry_done = True

    # t-2 刷新候选，完整拉完才覆盖 t-3 结果。
    def _pre_funding(self) -> None:
        if self.sent_done:
            return
        previous = dict(self.ins_map)
        previous_obs = dict(self.obs_map)
        previous_ready = self.entry_done
        self.obs_map.clear()
        self.ins_map.clear()
        stats = self._load_snap("pre", deadline_sec=float(self.config.pre_deadline))
        if stats is None:
            self.ins_map = previous
            self.obs_map = previous_obs
            self.entry_done = previous_ready
            return
        if stats["deadline_hit"]:
            self.ins_map = previous
            self.obs_map = previous_obs
            self.entry_done = previous_ready
            self.log.info(
                f"拉取资金费率超时，超过{self._ms(self.config.pre_deadline * 1000)}ms停止，"
                f"保留预拉候选{len(self.ins_map)}个，耗时{self._ms(stats['elapsed_ms'])}ms"
            )
        else:
            self._log_pre_result(stats, "拉取资金费率成功")
            self.entry_done = True
        order_ns = self.fund_ns - int(self.config.entry_before * 1_000_000_000)
        if self.clock.timestamp_ns() >= order_ns:
            self.sent_done = True
            self.log.info("跳过本轮，资金费率拉取太晚")

    # t-1 提交订单。
    def _freeze_funding(self) -> None:
        if self.sent_done:
            return
        if not self.entry_done:
            self.sent_done = True
            self.log.info("跳过本轮，候选未准备好")
            return
        rows = []
        for ins_id, row in self.ins_map.items():
            if ins_id not in self.ins or "rate" not in row or "pre" not in row:
                continue
            rows.append((ins_id, row))
        if not rows:
            self.log.info(f"交易模式，候选{len(self.ins_map)}个，无可下单交易对")
        else:
            self._refresh_entry_prices(rows)
            self._score_xgb(rows)
            if self.config.xgb_primary:
                rows = [
                    (ins_id, row)
                    for ins_id, row in rows
                    if row.get("xgb_primary_pass") is True
                ]
            selected = sorted(rows, key=self._select_key, reverse=True)[:self.max_trades]
            submitted = []
            for ins_id, row in selected:
                ins = self.ins[ins_id]
                rate = Decimal(str(row["rate"]))
                side = self._side(rate)
                row["side"] = side
                order_notional = self._order_notional(row)
                row["order_notional"] = order_notional
                qty = self._qty(ins, Decimal(str(row.get("entry", row["pre"]))), order_notional)
                order = self.order_factory.market(
                    instrument_id=ins_id,
                    order_side=side,
                    quantity=qty,
                    time_in_force=TimeInForce.GTC,
                )
                self.order_map[order.client_order_id] = ins_id
                self.submit_order(order)
                submitted.append(self._submit_label(ins_id, row))
            self.had_order = bool(submitted)
            self.log.info(
                f"交易模式，候选{len(self.ins_map)}个，提交{len(submitted)}个，"
                f"{'，'.join(submitted)}，基准名义{self.notional:.2f}USDT"
            )

        self.sent_done = True

    # t+n 平仓并写本轮交易记录。
    def _close_funding(self) -> None:
        if not self.close_done:
            close_cnt = 0
            for ins_id in sorted(self.open_ids, key=str):
                self.close_submit_ns[ins_id] = self.clock.timestamp_ns()
                self.close_all_positions(ins_id)
                close_cnt += 1
            self.close_done = True
            self.close_count = close_cnt
            self.log.info(
                f"交易模式，平仓{close_cnt}个，候选{len(self.ins_map)}个"
            )

    # t+n 再拉一次全量实际资金费率，用于观察分析。
    def _post_funding(self) -> None:
        start = perf_counter()
        rows, errors = self._fetch_rates()
        elapsed_ms = (perf_counter() - start) * 1000
        if errors:
            self.log.info(f"实际资金费率分析失败，错误{errors}个，耗时{self._ms(elapsed_ms)}ms")
        else:
            current_symbols = {
                self.symbols[ins_id]
                for ins_id in self.obs_map
                if ins_id in self.symbols
            }
            trade_symbols = {
                symbol
                for ins_id, symbol in self.symbols.items()
                if ins_id in self.trade_ids and symbol in current_symbols
            }
            self.log.info(
                f"实际资金费率分析，可交易前三 {self._top_rates(rows, trade_symbols)}，"
                f"耗时{self._ms(elapsed_ms)}ms"
            )
        self._reset_closed_state()
        self._schedule_next()

    # t+n 记录本轮选中 symbol 和平仓提交时间。
    def _record_trade_event(self) -> None:
        if self.trade_written:
            return
        if not self.close_submit_ns:
            return
        if self.close_count <= 0:
            return
        self._write_trade()
        self.trade_written = True

    # 成交后才记录待平仓 instrument。
    def on_order_filled(self, event: OrderFilled) -> None:
        ins_id = self.order_map.pop(event.client_order_id, None)
        if ins_id is not None:
            self.open_ids.add(ins_id)

    # 拒单后清理本地订单映射。
    def on_order_rejected(self, event: OrderRejected) -> None:
        ins_id = self.order_map.pop(event.client_order_id, None)
        if ins_id is not None:
            self.log.warning(f"订单被拒，交易对{self._base(ins_id)}，原因{event.reason}")

    def on_stop(self) -> None:
        names = set(self.clock.timer_names)
        for kind in self.TIMERS:
            name = f"{kind}:{self.fund_ns}"
            if name in names:
                self.clock.cancel_timer(name)
        for ins_id in self.ins:
            self.cancel_all_orders(ins_id)

        if self.config.stop_close:
            for ins_id in self.open_ids:
                self.close_all_positions(ins_id)

        self.log.info("资金费率监控停止")

    def on_reset(self) -> None:
        self._reset_state()

    # 从 cache 读取 node 已加载的 USDT 本位永续。
    def _load_ins(self) -> None:
        if self.trade is None:
            self.ins = {
                ins.id: ins
                for ins in self.cache.instruments()
                if ins is not None and str(ins.id).endswith("USDT-PERP.BINANCE")
            }
            if not self.ins:
                raise RuntimeError("No USDT perpetual instruments loaded")
            return
        ids = set(self.config.instrument_ids)
        self.ins = {
            ins.id: ins
            for ins in self.cache.instruments()
            if ins is not None and ins.id in ids
        }
        missing = ids - set(self.ins)
        if missing:
            raise RuntimeError(f"Missing instruments in cache: {','.join(sorted(map(str, missing)))}")

    # 拉已加载 instrument 的 premiumIndex；只有可交易集合超过阈值才进入候选。
    def _load_snap(
        self,
        phase: str,
        deadline_sec: float | None = None,
    ) -> dict[str, Decimal | float | int | bool] | None:
        start = perf_counter()
        passed = 0
        priced = 0
        skipped = 0
        errors = 0
        deadline_hit = False

        timeout = float(self.config.api_timeout)
        if deadline_sec is not None:
            timeout = min(timeout, max(deadline_sec - (perf_counter() - start), 0.001))
        payload, errors = self._fetch_rates(timeout=timeout)
        if errors and not payload:
            return None

        by_symbol = {str(item.get("symbol", "")): item for item in payload}
        for ins_id, symbol in self.symbols.items():
            if deadline_sec is not None and perf_counter() - start >= deadline_sec:
                deadline_hit = True
                break
            item = by_symbol.get(symbol)
            if item is None:
                skipped += 1
                continue
            try:
                price = Decimal(str(item["markPrice"]))
                if phase == "pre":
                    next_funding_minute = int(item["nextFundingTime"]) // 60_000
                    current_funding_minute = self.fund_ns // 60_000_000_000
                    if next_funding_minute != current_funding_minute:
                        continue
                    rate = Decimal(str(item["lastFundingRate"]))
                    row = self.obs_map.setdefault(ins_id, {})
                    row["rate"] = rate
                    row["pre"] = price
                    if ins_id not in self.trade_ids or abs(rate) <= self.min_rate:
                        self.ins_map.pop(ins_id, None)
                        continue
                    self.ins_map[ins_id] = dict(row)
                    passed += 1
            except (KeyError, ValueError, TypeError):
                skipped += 1

        elapsed_sec = perf_counter() - start
        if deadline_sec is not None and elapsed_sec >= deadline_sec:
            deadline_hit = True
        elapsed_ms = elapsed_sec * 1000
        return {
            "rows": len(self.symbols),
            "elapsed_ms": elapsed_ms,
            "observed": len(self.obs_map),
            "candidates": len(self.ins_map),
            "passed": passed,
            "priced": priced,
            "skipped": skipped,
            "errors": errors,
            "deadline_hit": deadline_hit,
        }

    # 把已加载 instrument 和可交易集合映射到 Binance symbol。
    def _load_lists(self) -> None:
        by_base = {
            str(ins_id).split(".")[0].replace("USDT-PERP", "").upper(): ins_id
            for ins_id in self.ins
        }
        self.symbols = {
            ins_id: str(ins_id).split(".")[0].replace("-PERP", "")
            for ins_id in sorted(self.ins, key=str)
        }
        if self.trade is None:
            ids = set(self.ins)
        else:
            ids = {by_base[symbol] for symbol in self.trade if symbol in by_base}
            missing = sorted(self.trade - set(by_base))
            if not ids:
                raise RuntimeError("trade_symbols is empty")
            if missing:
                raise RuntimeError(f"trade_symbols not loaded: {','.join(missing)}")
        missing_exclude = sorted(self.exclude - set(by_base))
        if missing_exclude:
            raise RuntimeError(f"exclude_symbols not loaded: {','.join(missing_exclude)}")
        ids -= {by_base[symbol] for symbol in self.exclude}
        if not ids:
            raise RuntimeError("trade_symbols is empty after exclude_symbols")
        self.trade_ids = ids

    # 候选已冻结后统一计算 XGB 指标；不额外拉 aggTrades。
    def _score_xgb(self, rows: list[tuple[InstrumentId, dict[str, Any]]]) -> None:
        if self.xgb is None:
            return
        candidates = [
            {
                "symbol": self.symbols[ins_id],
                "rate": row["rate"],
                "pre_cost_bps": row.get("pre_cost_bps"),
            }
            for ins_id, row in rows
        ]
        observed = [
            {
                "symbol": self.symbols[ins_id],
                "rate": row["rate"],
            }
            for ins_id, row in self.obs_map.items()
            if ins_id in self.symbols and "rate" in row
        ]
        scores = self.xgb.score(candidates, observed, self.fund_ns)
        for ins_id, row in rows:
            row.update(scores.get(self.symbols[ins_id], {}))

    # 下单前再拉一次 mark price，估算 t 前价格挤压。
    def _refresh_entry_prices(self, rows: list[tuple[InstrumentId, dict[str, Any]]]) -> None:
        payload, errors = self._fetch_rates()
        if errors and not payload:
            return
        by_symbol = {str(item.get("symbol", "")): item for item in payload}
        for ins_id, row in rows:
            item = by_symbol.get(self.symbols[ins_id])
            if item is None or "markPrice" not in item or "pre" not in row or "rate" not in row:
                continue
            try:
                pre = Decimal(str(row["pre"]))
                entry = Decimal(str(item["markPrice"]))
                if pre <= 0:
                    continue
                rate = Decimal(str(row["rate"]))
                direction = Decimal("-1") if rate > 0 else Decimal("1")
                ret_bps = direction * (entry - pre) / pre * Decimal("10000")
                row["entry"] = entry
                row["pre_cost_bps"] = float(-ret_bps)
            except (ValueError, TypeError):
                continue

    def _select_key(self, item: tuple[InstrumentId, dict[str, Any]]) -> float:
        row = item[1]
        if self.config.xgb_primary:
            score = row.get("xgb_primary_score")
            try:
                value = float(score)
            except (TypeError, ValueError):
                return float("-inf")
            return value if value == value else float("-inf")
        return abs(float(row["rate"]))

    def _submit_label(self, ins_id: InstrumentId, row: dict[str, Any]) -> str:
        rate = Decimal(str(row["rate"]))
        if not self.config.xgb_primary:
            return f"{self._base(ins_id)} {self._bps(rate)}bps notional={self._fmt(row.get('order_notional'))}"
        score = self._fmt(row.get("xgb_primary_score"))
        passed = row.get("xgb_primary_pass")
        mult = self._fmt(row.get("notional_multiplier"))
        notional = self._fmt(row.get("order_notional"))
        return f"{self._base(ins_id)} {self._bps(rate)}bps XGB={score} pass={passed} mult={mult} notional={notional}"

    # 为下一次 funding 注册一次性 clock alert。
    def _schedule_next(self) -> None:
        now_ns = self.clock.timestamp_ns()
        hour_ns = 60 * 60 * 1_000_000_000
        self.fund_ns = ((now_ns // hour_ns) + 1) * hour_ns
        warmup_ns = self.fund_ns - int(self.config.pre_sec * 1_000_000_000)
        pre_ns = self.fund_ns - int(self.config.entry_sec * 1_000_000_000)
        freeze_ns = self.fund_ns - int(self.config.entry_before * 1_000_000_000)
        rate_ns = self.fund_ns
        close_ns = self.fund_ns + int(self.exit_sec * 1_000_000_000)
        post_sec = max(float(self.config.post_sec), self.exit_sec + 1.0)
        post_ns = self.fund_ns + int(post_sec * 1_000_000_000)
        if now_ns >= pre_ns:
            self.fund_ns += hour_ns
            warmup_ns += hour_ns
            pre_ns += hour_ns
            freeze_ns += hour_ns
            rate_ns += hour_ns
            close_ns += hour_ns
            post_ns += hour_ns
        timers = [
            (self.WARMUP_TIMER, warmup_ns),
            (self.PRE_TIMER, pre_ns),
            (self.FREEZE_TIMER, freeze_ns),
            (self.POST_TIMER, post_ns),
        ]
        if self.exit_sec == 0:
            timers.append((self.RATE_TIMER, rate_ns))
        else:
            timers.append((self.CLOSE_TIMER, close_ns))
        for name, ts_ns in timers:
            if name == self.WARMUP_TIMER and now_ns >= ts_ns:
                continue
            self.clock.set_time_alert_ns(
                self._timer_name(name),
                ts_ns,
                callback=self._on_time,
                allow_past=False,
            )

    def _timer_name(self, kind: str) -> str:
        return f"{kind}:{self.fund_ns}"

    def _log_pre_result(self, stats: dict, title: str) -> None:
        if not self.ins_map:
            self.log.info(
                f"{title}，已加载{stats['rows']}个，可交易{len(self.trade_ids)}个，"
                f"阈值{self._bps(self.min_rate)}bps，无超过阈值候选，"
                f"前三 {self._rate_list(limit=3, rows=self.obs_map)}，耗时{self._ms(stats['elapsed_ms'])}ms"
            )
            return
        self.log.info(
            f"{title}，已加载{stats['rows']}个，可交易{len(self.trade_ids)}个，"
            f"阈值{self._bps(self.min_rate)}bps，"
            f"候选{len(self.ins_map)}个，耗时{self._ms(stats['elapsed_ms'])}ms，"
            f"候选 {self._rate_list(limit=3)}"
        )

    def _fetch_rates(self, timeout: float | None = None) -> tuple[list[dict], int]:
        try:
            response = requests.get(
                f"{self.config.api_url}/fapi/v1/premiumIndex",
                proxies=self.proxies,
                timeout=timeout or float(self.config.api_timeout),
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError):
            return [], 1
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)], 0
        if isinstance(data, dict):
            return [data], 0
        return [], 1

    def _top_rates(self, rows: list[dict], symbols: set[str] | None = None) -> str:
        rated = []
        for item in rows:
            symbol = str(item.get("symbol", ""))
            if symbols is not None and symbol not in symbols:
                continue
            try:
                rated.append((symbol, Decimal(str(item["lastFundingRate"]))))
            except (KeyError, ValueError):
                continue
        rated.sort(key=lambda item: abs(item[1]), reverse=True)
        parts = [
            f"{symbol.replace('USDT', '')} {self._bps(rate)}bps"
            for symbol, rate in rated[:3]
        ]
        return "，".join(parts) if parts else "无"

    def _rate_list(
        self,
        limit: int,
        key: str = "rate",
        rows: dict[InstrumentId, dict[str, Any]] | None = None,
    ) -> str:
        rows = [
            (ins_id, row)
            for ins_id, row in (rows or self.ins_map).items()
            if key in row
        ]
        rows.sort(key=lambda item: abs(item[1][key]), reverse=True)
        parts = []
        for ins_id, row in rows[:limit]:
            parts.append(f"{self._base(ins_id)} {self._bps(Decimal(str(row[key])))}bps")
        if not parts:
            return "无"
        return "，".join(parts)

    def _base(self, ins_id: InstrumentId) -> str:
        return str(ins_id).split(".")[0].replace("USDT-PERP", "")

    def _side_cn(self, side: OrderSide) -> str:
        return "买入" if side == OrderSide.BUY else "卖出"

    def _bps(self, value: Decimal) -> str:
        return f"{value * Decimal('10000'):.2f}"

    def _ms(self, value: Decimal | float | int) -> str:
        return f"{float(value):.2f}"

    def _side(self, rate: Decimal) -> OrderSide:
        return OrderSide.SELL if rate > 0 else OrderSide.BUY

    def _order_notional(self, row: dict[str, Any]) -> Decimal:
        if not self.config.xgb_primary:
            row["notional_multiplier"] = Decimal("1")
            return self.notional
        score = self._decimal(row.get("xgb_primary_score"))
        threshold = self._decimal(row.get(f"xgb_{self.config.xgb_primary}_threshold_bps"))
        if score is None or threshold is None:
            row["notional_multiplier"] = Decimal("1")
            return self.notional
        mult = score_multiplier(score, threshold, self.min_mult, self.base_score, self.max_score, self.max_mult)
        row["notional_multiplier"] = mult
        return self.notional * mult

    def _decimal(self, value: Any) -> Decimal | None:
        if value is None:
            return None
        try:
            result = Decimal(str(value))
        except Exception:
            return None
        return result if result.is_finite() else None

    def _qty(self, ins: Instrument, price: Decimal, notional: Decimal):
        raw_qty = notional / price
        step = Decimal(str(ins.size_increment))
        if step > 0:
            steps = (raw_qty / step).to_integral_value(rounding=ROUND_CEILING)
            raw_qty = steps * step
        return ins.make_qty(raw_qty)

    # 写本轮选中交易的整点和平仓提交偏移。
    def _write_trade(self) -> None:
        if not self.close_submit_ns:
            return
        with self.log_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for ins_id, close_ns in sorted(
                self.close_submit_ns.items(),
                key=lambda item: str(item[0]),
            ):
                writer.writerow(
                    [
                        self.symbols[ins_id],
                        self._iso(self.fund_ns),
                        self._offset(close_ns - self.fund_ns),
                        *self._event_metrics(self.ins_map.get(ins_id, {})),
                    ],
                )

    def _init_log(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(self.event_columns)

    def _event_metrics(self, row: dict[str, Any]) -> list[Any]:
        return [self._fmt(row.get(column)) for column in self.event_columns[3:]]

    def _fmt(self, value: Any) -> Any:
        if isinstance(value, bool):
            return int(value)
        if value is None:
            return ""
        if isinstance(value, float):
            if value != value:
                return ""
            return f"{value:.4f}"
        return value

    def _reset_state(self) -> None:
        self.obs_map.clear()
        self.ins_map.clear()
        self.open_ids.clear()
        self.order_map.clear()
        self.had_order = False
        self.close_count = 0
        self.trade_written = False
        self.close_submit_ns.clear()
        self.entry_done = False
        self.sent_done = False
        self.close_done = False

    def _reset_closed_state(self) -> None:
        self._record_trade_event()
        self.obs_map.clear()
        self.ins_map.clear()
        self.order_map.clear()
        self.open_ids = {ins_id for ins_id in self.open_ids if not self.portfolio.is_flat(ins_id)}
        self.had_order = False
        self.close_count = 0
        self.trade_written = False
        self.close_submit_ns.clear()
        self.entry_done = False
        self.sent_done = False
        self.close_done = False
        if self.open_ids:
            self.log.warning(f"平仓后仍有持仓跟踪，交易对{','.join(map(self._base, self.open_ids))}")

    def _iso(self, ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()

    def _offset(self, delta_ns: int) -> str:
        delta_ms = delta_ns // 1_000_000
        sign = "+" if delta_ms >= 0 else "-"
        return f"T{sign}{abs(delta_ms)}ms"
