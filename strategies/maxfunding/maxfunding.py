from __future__ import annotations

import csv
import platform
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_CEILING
from pathlib import Path
from time import perf_counter
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

from .maxfunding_xgb import MaxFundingXgbScorer


@dataclass
class PreparedData:
    data_type: str
    rows: list[tuple[InstrumentId, dict[str, Any]]]
    obs_map: dict[InstrumentId, dict[str, Any]]
    frame: Any
    created_ns: int
    elapsed_ms: float


def score_multiplier(
    score: Decimal,
    threshold: Decimal,
    min_multiplier: Decimal,
    base_score: Decimal,
    max_score: Decimal,
    max_notional: Decimal,
    base_notional: Decimal,
) -> Decimal:
    max_multiplier = max_notional / base_notional
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
    notional_max: Decimal
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
        self.max_notional = Decimal(str(config.notional_max))
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
        self.backup_data: PreparedData | None = None
        self.main_data: PreparedData | None = None
        self.active_data: PreparedData | None = None
        self.log_path = Path(config.event_log_path)
        use_proxy = platform.system() == "Windows" and config.proxy_url
        self.proxies = {"http": config.proxy_url, "https": config.proxy_url} if use_proxy else None
        if not config.xgb_primary:
            raise RuntimeError("max_funding requires xgb_primary")
        if not config.xgb_models:
            raise RuntimeError("max_funding requires xgb_models")
        self.xgb = MaxFundingXgbScorer(
            config.xgb_models,
            config.xgb_primary,
            api_url=config.api_url,
            api_timeout=config.api_timeout,
            proxies=self.proxies,
        )
        self.xgb_columns = self.xgb.metric_columns
        self.event_columns = [
            "symbol",
            "funding_time",
            "close_order_submit_time",
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
        if self.max_notional < self.notional:
            raise RuntimeError("notional_max must be greater than or equal to trade_notional")
        if self.min_mult > self.max_notional / self.notional:
            raise RuntimeError("notional_min_multiplier is above the max notional multiplier")
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
            f"XGB主模型{self.config.xgb_primary}"
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

    # t-3 准备备用模型数据，成功时不打 info。
    def _warmup_funding(self) -> None:
        if self.sent_done:
            return
        start = perf_counter()
        try:
            self.backup_data = self._prepare_model_data("备用")
        except Exception as exc:
            elapsed_ms = (perf_counter() - start) * 1000
            self.log.warning(f"备用模型数据准备失败，原因：{exc}，耗时：{self._ms(elapsed_ms)}ms")

    # t-1.5 准备主用模型数据；失败时使用 t-3 备用。
    def _pre_funding(self) -> None:
        if self.sent_done:
            return
        start = perf_counter()
        try:
            self.main_data = self._prepare_model_data("主用", deadline_sec=float(self.config.pre_deadline))
            self.active_data = self.main_data
            self.entry_done = True
            self.log.info(
                f"模型数据准备完成，数据类型：主用，候选数：{len(self.main_data.rows)}，"
                f"市场数据：BTC/ETH/SOL，耗时：{self._ms(self.main_data.elapsed_ms)}ms"
            )
        except Exception as exc:
            elapsed_ms = (perf_counter() - start) * 1000
            if self.backup_data is not None:
                self.active_data = self.backup_data
                self.entry_done = True
                age_ms = (self.clock.timestamp_ns() - self.backup_data.created_ns) // 1_000_000
                self.log.warning(
                    f"使用备用模型数据，原因：主用数据准备失败，候选数：{len(self.backup_data.rows)}，"
                    f"备用数据年龄：{age_ms}ms"
                )
            else:
                self.active_data = None
                self.entry_done = False
                self.log.warning(
                    f"模型数据准备失败，原因：主用和备用数据都不可用，耗时：{self._ms(elapsed_ms)}ms"
                )
        order_ns = self.fund_ns - int(self.config.entry_before * 1_000_000_000)
        if self.clock.timestamp_ns() >= order_ns:
            self.sent_done = True
            self.log.warning("跳过本轮，原因：模型数据准备太晚")

    # t-0.5 使用已准备的数据完成模型判断并提交订单。
    def _freeze_funding(self) -> None:
        if self.sent_done:
            return
        if not self.entry_done or self.active_data is None:
            self.sent_done = True
            self.log.warning("跳过本轮，原因：模型数据未准备好")
            return
        rows = [(ins_id, dict(row)) for ins_id, row in self.active_data.rows]
        self.ins_map = {ins_id: row for ins_id, row in rows}
        self.obs_map = {ins_id: dict(row) for ins_id, row in self.active_data.obs_map.items()}
        model_start = perf_counter()
        try:
            scores = self.xgb.score_frame(self.active_data.frame)
            for ins_id, row in rows:
                row.update(scores.get(self.symbols[ins_id], {}))
            rows = [
                (ins_id, row)
                for ins_id, row in rows
                if row.get("xgb_primary_pass") is True
            ]
            selected = sorted(rows, key=self._select_key, reverse=True)[:self.max_trades]
            for _ins_id, row in selected:
                rate = Decimal(str(row["rate"]))
                row["side"] = self._side(rate)
                row["order_notional"] = self._order_notional(row)
            model_elapsed_ms = (perf_counter() - model_start) * 1000
            self.log.info(
                f"模型判断完成，候选数：{len(self.active_data.rows)}，通过数：{len(rows)}，"
                f"选中数：{len(selected)}，耗时：{self._ms(model_elapsed_ms)}ms，"
                f"选中：{self._decision_list(selected)}"
            )
        except Exception as exc:
            model_elapsed_ms = (perf_counter() - model_start) * 1000
            self.log.warning(f"模型判断失败，原因：{exc}，耗时：{self._ms(model_elapsed_ms)}ms")
            raise

        order_start = perf_counter()
        submitted = []
        for ins_id, row in selected:
            ins = self.ins[ins_id]
            side = row["side"]
            order_notional = row["order_notional"]
            qty = self._qty(ins, Decimal(str(row.get("entry", row["pre"]))), order_notional)
            order = self.order_factory.market(
                instrument_id=ins_id,
                order_side=side,
                quantity=qty,
                time_in_force=TimeInForce.GTC,
            )
            self.order_map[order.client_order_id] = ins_id
            self.submit_order(order)
            submitted.append(
                f"{self._base(ins_id)} {self._side_cn(side)} 名义：{self._fmt(order_notional)} 数量：{qty}"
            )
        self.had_order = bool(submitted)
        order_elapsed_ms = (perf_counter() - order_start) * 1000
        self.log.info(
            f"订单提交完成，提交数：{len(submitted)}，订单：{self._join_items(submitted)}，"
            f"耗时：{self._ms(order_elapsed_ms)}ms"
        )

        self.sent_done = True

    # t+0.15 提交本轮已开仓仓位的平仓。
    def _close_funding(self) -> None:
        if not self.close_done:
            start = perf_counter()
            close_cnt = 0
            symbols = []
            for ins_id in sorted(self.open_ids, key=str):
                self.close_submit_ns[ins_id] = self.clock.timestamp_ns()
                self.close_all_positions(ins_id)
                close_cnt += 1
                symbols.append(self._base(ins_id))
            self.close_done = True
            self.close_count = close_cnt
            elapsed_ms = (perf_counter() - start) * 1000
            self.log.info(
                f"提交平仓，仓位数：{close_cnt}，交易对：{self._join_items(symbols)}，"
                f"耗时：{self._ms(elapsed_ms)}ms"
            )

    # t+10 总结本轮仓位状态并落盘事件。
    def _post_funding(self) -> None:
        start = perf_counter()
        event_written = self._record_trade_event()
        opened_count = len(self.open_ids)
        flat_count = sum(1 for ins_id in self.open_ids if self.portfolio.is_flat(ins_id))
        not_flat_ids = [ins_id for ins_id in self.open_ids if not self.portfolio.is_flat(ins_id)]
        elapsed_ms = (perf_counter() - start) * 1000
        self.log.info(
            f"本轮总结，开仓数：{opened_count}，提交平仓数：{self.close_count}，"
            f"已平仓数：{flat_count}，未平仓数：{len(not_flat_ids)}，"
            f"事件落盘：{'是' if event_written else '否'}，耗时：{self._ms(elapsed_ms)}ms"
        )
        if not_flat_ids:
            self.log.warning(f"平仓后仍有持仓跟踪，交易对：{self._join_items(map(self._base, not_flat_ids))}")
        self._reset_closed_state()
        self._schedule_next()

    # t+n 记录本轮选中 symbol 和平仓提交时间。
    def _record_trade_event(self) -> bool:
        if self.trade_written:
            return False
        if not self.close_submit_ns:
            return False
        if self.close_count <= 0:
            return False
        self._write_trade()
        self.trade_written = True
        return True

    # 成交后才记录待平仓 instrument。
    def on_order_filled(self, event: OrderFilled) -> None:
        ins_id = self.order_map.pop(event.client_order_id, None)
        if ins_id is not None:
            self.open_ids.add(ins_id)

    # 拒单后清理本地订单映射。
    def on_order_rejected(self, event: OrderRejected) -> None:
        ins_id = self.order_map.pop(event.client_order_id, None)
        if ins_id is not None:
            self.log.warning(f"订单被拒，交易对：{self._base(ins_id)}，原因：{event.reason}")

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
    def _prepare_model_data(self, data_type: str, deadline_sec: float | None = None) -> PreparedData:
        start = perf_counter()
        obs_map, ins_map, stats = self._load_model_snapshot(deadline_sec=deadline_sec)
        if stats["deadline_hit"]:
            raise RuntimeError("premiumIndex 拉取超过截止时间")
        rows = [
            (ins_id, row)
            for ins_id, row in ins_map.items()
            if ins_id in self.ins and "rate" in row and "pre" in row
        ]
        if rows:
            self._refresh_entry_prices(rows)
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
            for ins_id, row in obs_map.items()
            if ins_id in self.symbols and "rate" in row
        ]
        frame = self.xgb.prepare_frame(candidates, observed, self.fund_ns)
        elapsed_ms = (perf_counter() - start) * 1000
        return PreparedData(
            data_type=data_type,
            rows=[(ins_id, dict(row)) for ins_id, row in rows],
            obs_map={ins_id: dict(row) for ins_id, row in obs_map.items()},
            frame=frame,
            created_ns=self.clock.timestamp_ns(),
            elapsed_ms=elapsed_ms,
        )

    def _load_model_snapshot(
        self,
        deadline_sec: float | None = None,
    ) -> tuple[dict[InstrumentId, dict[str, Any]], dict[InstrumentId, dict[str, Any]], dict[str, Decimal | float | int | bool]]:
        start = perf_counter()
        passed = 0
        priced = 0
        skipped = 0
        errors = 0
        deadline_hit = False
        obs_map: dict[InstrumentId, dict[str, Any]] = {}
        ins_map: dict[InstrumentId, dict[str, Any]] = {}

        timeout = float(self.config.api_timeout)
        if deadline_sec is not None:
            timeout = min(timeout, max(deadline_sec - (perf_counter() - start), 0.001))
        payload, errors = self._fetch_rates(timeout=timeout)
        if errors and not payload:
            raise RuntimeError("premiumIndex REST 失败")

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
                next_funding_minute = int(item["nextFundingTime"]) // 60_000
                current_funding_minute = self.fund_ns // 60_000_000_000
                if next_funding_minute != current_funding_minute:
                    continue
                rate = Decimal(str(item["lastFundingRate"]))
                row = obs_map.setdefault(ins_id, {})
                row["rate"] = rate
                row["pre"] = price
                if ins_id not in self.trade_ids or abs(rate) <= self.min_rate:
                    continue
                ins_map[ins_id] = dict(row)
                passed += 1
            except (KeyError, ValueError, TypeError):
                skipped += 1

        elapsed_sec = perf_counter() - start
        if deadline_sec is not None and elapsed_sec >= deadline_sec:
            deadline_hit = True
        elapsed_ms = elapsed_sec * 1000
        stats = {
            "rows": len(self.symbols),
            "elapsed_ms": elapsed_ms,
            "observed": len(obs_map),
            "candidates": len(ins_map),
            "passed": passed,
            "priced": priced,
            "skipped": skipped,
            "errors": errors,
            "deadline_hit": deadline_hit,
        }
        return obs_map, ins_map, stats

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

    # 下单前再拉一次 mark price，估算 t 前价格挤压。
    def _refresh_entry_prices(self, rows: list[tuple[InstrumentId, dict[str, Any]]]) -> None:
        payload, errors = self._fetch_rates()
        if errors and not payload:
            raise RuntimeError("刷新入场价格失败")
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
        score = row.get("xgb_primary_score")
        try:
            value = float(score)
        except (TypeError, ValueError):
            return float("-inf")
        return value if value == value else float("-inf")

    def _decision_list(self, rows: list[tuple[InstrumentId, dict[str, Any]]]) -> str:
        parts = []
        for ins_id, row in rows:
            parts.append(
                f"{self._base(ins_id)} 分数：{self._fmt(row.get('xgb_primary_score'))} "
                f"名义：{self._fmt(row.get('order_notional'))}"
            )
        return self._join_items(parts)

    def _join_items(self, items) -> str:
        parts = [str(item) for item in items if str(item)]
        return "; ".join(parts) if parts else "无"

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
        score = self._decimal(row.get("xgb_primary_score"))
        threshold = self._decimal(row.get(f"xgb_{self.config.xgb_primary}_threshold_bps"))
        if score is None or threshold is None:
            row["notional_multiplier"] = Decimal("1")
            return self.notional
        mult = score_multiplier(
            score,
            threshold,
            self.min_mult,
            self.base_score,
            self.max_score,
            self.max_notional,
            self.notional,
        )
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
                    ],
                )

    def _init_log(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(self.event_columns)

    def _fmt(self, value: Any) -> Any:
        if isinstance(value, bool):
            return int(value)
        if value is None:
            return ""
        if isinstance(value, float):
            if value != value:
                return ""
            return f"{value:.4f}"
        if isinstance(value, Decimal):
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
        self.backup_data = None
        self.main_data = None
        self.active_data = None
        self.entry_done = False
        self.sent_done = False
        self.close_done = False

    def _reset_closed_state(self) -> None:
        self.obs_map.clear()
        self.ins_map.clear()
        self.order_map.clear()
        self.open_ids = {ins_id for ins_id in self.open_ids if not self.portfolio.is_flat(ins_id)}
        self.had_order = False
        self.close_count = 0
        self.trade_written = False
        self.close_submit_ns.clear()
        self.backup_data = None
        self.main_data = None
        self.active_data = None
        self.entry_done = False
        self.sent_done = False
        self.close_done = False

    def _iso(self, ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()

    def _offset(self, delta_ns: int) -> str:
        delta_ms = delta_ns // 1_000_000
        sign = "+" if delta_ms >= 0 else "-"
        return f"T{sign}{abs(delta_ms)}ms"
