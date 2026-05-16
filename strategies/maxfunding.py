from __future__ import annotations

import csv
import platform
from time import perf_counter
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_CEILING
from pathlib import Path

import requests
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import EVENT_ACCOUNT_TOPIC


class MaxfundingConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    trade_symbols: list[str] | str
    exclude_symbols: list[str]
    trade_notional: Decimal
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
    event_log_path: str = "auto"


class Maxfunding(Strategy):
    WARMUP_TIMER = "maxfunding_warmup"
    PRE_TIMER = "maxfunding_pre"
    FREEZE_TIMER = "maxfunding_freeze"
    RATE_TIMER = "maxfunding_rate"
    CLOSE_TIMER = "maxfunding_close"
    POST_TIMER = "maxfunding_post"
    TIMERS = (WARMUP_TIMER, PRE_TIMER, FREEZE_TIMER, RATE_TIMER, CLOSE_TIMER, POST_TIMER)

    def __init__(self, config: MaxfundingConfig) -> None:
        super().__init__(config)
        self.notional = Decimal(str(config.trade_notional))
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
        self.obs_map: dict[InstrumentId, dict[str, Decimal | OrderSide]] = {}
        self.ins_map: dict[InstrumentId, dict[str, Decimal | OrderSide]] = {}
        self.open_ids: set[InstrumentId] = set()
        self.order_map: dict[ClientOrderId, InstrumentId] = {}
        self.chosen_id: InstrumentId | None = None
        self.had_order = False
        self.post_account_count = 0
        self.funding_logged = False
        self.log_path = Path(config.event_log_path)
        use_proxy = platform.system() == "Windows" and config.proxy_url
        self.proxies = {"http": config.proxy_url, "https": config.proxy_url} if use_proxy else None

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

        self._load_ins()
        self._load_lists()
        self._init_log()
        self.msgbus.subscribe(EVENT_ACCOUNT_TOPIC, self._on_account)

        self._schedule_next()

        self.log.info(
            f"资金费率交易启动，已加载{len(self.ins)}个，"
            f"可交易{len(self.trade_ids)}个，"
            f"排除{len(self.exclude)}个，"
            f"阈值{self._bps(self.min_rate)}bps，单币名义{self.notional:.2f}USDT"
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
            ins_id, row = max(rows, key=lambda item: abs(item[1]["rate"]))
            ins = self.ins[ins_id]
            side = self._side(row["rate"])
            row["side"] = side
            qty = self._qty(ins, row["pre"])
            order = self.order_factory.market(
                instrument_id=ins_id,
                order_side=side,
                quantity=qty,
                time_in_force=TimeInForce.GTC,
            )
            self.order_map[order.client_order_id] = ins_id
            self.chosen_id = ins_id
            self.submit_order(order)
            self.had_order = True
            self.post_account_count = 0
            self.funding_logged = False
            self.log.info(
                f"交易模式，候选{len(self.ins_map)}个，选择{self._base(ins_id)}，"
                f"费率{self._bps(row['rate'])}bps"
                f"名义{self.notional:.2f}USDT"
            )

        self.sent_done = True

    # t+n 平仓并写本轮交易记录。
    def _close_funding(self) -> None:
        if not self.close_done:
            close_cnt = 0
            for ins_id in sorted(self.open_ids, key=str):
                self.close_all_positions(ins_id)
                close_cnt += 1
            close_cnt += self._close_btc_for_test()
            self.close_done = True
            self.log.info(
                f"交易模式，平仓{close_cnt}个，候选{len(self.ins_map)}个"
            )
            self._write_trade(close_cnt)

    # 临时测试：准点一起平 BTC，用于观察大币是否也有订单创建延迟。
    def _close_btc_for_test(self) -> int:
        eight_hour_ns = 8 * 60 * 60 * 1_000_000_000
        if self.fund_ns % eight_hour_ns != 0:
            return 0
        for ins_id in self.ins:
            if self._base(ins_id) == "BNB":
                self.close_all_positions(ins_id)
                return 1
        return 0

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

    # t 后第二次账户事件视为本轮资金费已到账，只记录不参与决策。
    def _on_account(self, event: AccountState) -> None:
        if not self.had_order:
            return
        if not (self.fund_ns <= event.ts_event <= self.fund_ns + 10_000_000_000):
            return
        self.post_account_count += 1
        if self.post_account_count == 2 and not self.funding_logged:
            self.funding_logged = True
            self.log.info("收到资金费账户事件")

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
        self.msgbus.unsubscribe(EVENT_ACCOUNT_TOPIC, self._on_account)
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
                elif phase == "rate" and ins_id in self.ins_map:
                    self.ins_map[ins_id]["settle_rate"] = Decimal(str(item["lastFundingRate"]))
                    self.ins_map[ins_id]["rate_px"] = price
                    priced += 1
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
        rows: dict[InstrumentId, dict[str, Decimal | OrderSide]] | None = None,
    ) -> str:
        rows = [
            (ins_id, row)
            for ins_id, row in (rows or self.ins_map).items()
            if key in row
        ]
        rows.sort(key=lambda item: abs(item[1][key]), reverse=True)
        parts = []
        for ins_id, row in rows[:limit]:
            parts.append(f"{self._base(ins_id)} {self._bps(row[key])}bps")
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

    def _qty(self, ins: Instrument, price: Decimal):
        raw_qty = self.notional / price
        step = Decimal(str(ins.size_increment))
        if step > 0:
            steps = (raw_qty / step).to_integral_value(rounding=ROUND_CEILING)
            raw_qty = steps * step
        return ins.make_qty(raw_qty)

    # 写本轮选中交易的资金费率记录。
    def _write_trade(self, close_cnt: int) -> None:
        ins_id = self.chosen_id
        if ins_id is None:
            return
        row = self.ins_map.get(ins_id)
        if row is None:
            return
        rate = row.get("rate")
        settle_rate = row.get("settle_rate", "")
        pre = row.get("pre", "")
        side = row.get("side") or (self._side(rate) if isinstance(rate, Decimal) else "")
        fund_gain = abs(rate) * self.notional if isinstance(rate, Decimal) else ""
        settle_gain = abs(settle_rate) * self.notional if isinstance(settle_rate, Decimal) else ""
        with self.log_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    self._iso(self.fund_ns),
                    ins_id,
                    rate,
                    settle_rate,
                    ("BUY" if side == OrderSide.BUY else "SELL") if isinstance(side, OrderSide) else "",
                    pre,
                    self.notional,
                    fund_gain,
                    settle_gain,
                    close_cnt,
                ],
            )

    def _init_log(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "funding_time",
                    "instrument",
                    "entry_rate",
                    "settle_rate",
                    "side",
                    "pre_px",
                    "notional",
                    "entry_funding_gain",
                    "settle_funding_gain",
                    "close_count",
                ],
            )

    def _reset_state(self) -> None:
        self.obs_map.clear()
        self.ins_map.clear()
        self.open_ids.clear()
        self.order_map.clear()
        self.chosen_id = None
        self.had_order = False
        self.post_account_count = 0
        self.funding_logged = False
        self.entry_done = False
        self.sent_done = False
        self.close_done = False

    def _reset_closed_state(self) -> None:
        self.obs_map.clear()
        self.ins_map.clear()
        self.order_map.clear()
        self.open_ids = {ins_id for ins_id in self.open_ids if not self.portfolio.is_flat(ins_id)}
        self.chosen_id = next(iter(self.open_ids), None)
        self.had_order = False
        self.post_account_count = 0
        self.funding_logged = False
        self.entry_done = False
        self.sent_done = False
        self.close_done = False
        if self.open_ids:
            self.log.warning(f"平仓后仍有持仓跟踪，交易对{','.join(map(self._base, self.open_ids))}")

    def _iso(self, ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()
