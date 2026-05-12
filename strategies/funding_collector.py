from __future__ import annotations

import csv
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_CEILING
from pathlib import Path

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import FundingRateUpdate
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class FundingCollectorConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_notional: Decimal = Decimal("5.5")
    mode: str = "watch"
    min_rate: Decimal = Decimal("0.0050")
    trade_scope: str = "all"
    collect_before: float = 5.0
    rate_end_before: float = 1.0
    entry_before: float = 1.0
    close_after: float = 3.0
    stop_close: bool = True
    event_log_path: str = "auto"


class FundingCollector(Strategy):
    def __init__(self, config: FundingCollectorConfig) -> None:
        super().__init__(config)
        self.cid = ClientId("BINANCE")
        self.mode = config.mode.lower()
        self.notional = Decimal(str(config.trade_notional))
        self.min_rate = Decimal(str(config.min_rate))
        self.fund_ns = 0
        self.sent = False
        self.closed = False
        self.logged = False
        self.ins: dict[InstrumentId, Instrument] = {}
        self.trade_ids: set[InstrumentId] | None = None
        self.ins_map: dict[InstrumentId, dict[str, Decimal]] = {}
        self.open_ids: set[InstrumentId] = set()
        self.log_path = Path(config.event_log_path)

    # 启动时订阅所有永续的资金费和标记价，只用一个 tick 做触发器。
    def on_start(self) -> None:
        if self.mode not in {"watch", "trade"}:
            raise RuntimeError(f"Invalid mode: {self.mode}")
        if self.config.collect_before <= self.config.rate_end_before:
            raise RuntimeError("collect_before must be greater than rate_end_before")
        if self.config.rate_end_before < self.config.entry_before:
            raise RuntimeError("rate_end_before must be greater than or equal to entry_before")
        if self.config.entry_before <= 0:
            raise RuntimeError("entry_before must be positive")
        if self.config.close_after <= 0:
            raise RuntimeError("close_after must be positive")

        self._load_ins()
        self._load_scope()
        if self.mode == "watch":
            self._init_log()

        self._next_time()
        for ins_id in self.ins:
            self.subscribe_funding_rates(ins_id, client_id=self.cid)
            self.subscribe_mark_prices(ins_id, client_id=self.cid)

        self.subscribe_trade_ticks(self.config.instrument_id)

        self.log.info(
            "FundingCollector started: "
            f"mode={self.mode}, heartbeat={self.config.instrument_id}, "
            f"instruments={len(self.ins)}, notional={self.notional}, "
            f"min_rate={self.min_rate}, trade_scope={self.config.trade_scope}, "
            f"collect_before={self.config.collect_before}s, "
            f"close_after={self.config.close_after}s"
        )

    # 只在 t-5 到 t-1 收集达标 rate。
    def on_funding_rate(self, data: FundingRateUpdate) -> None:
        now_ns = self.clock.timestamp_ns()
        start_ns = self.fund_ns - int(self.config.collect_before * 1_000_000_000)
        end_ns = self.fund_ns - int(self.config.rate_end_before * 1_000_000_000)
        if now_ns < start_ns or now_ns >= end_ns:
            return
        if data.next_funding_ns != self.fund_ns:
            return

        rate = Decimal(str(data.rate))
        if abs(rate) < self.min_rate:
            self.ins_map.pop(data.instrument_id, None)
            return

        row = self.ins_map.setdefault(data.instrument_id, {})
        row["rate"] = rate

    # watch 收到 t 后第一口价，trade 只保留 t 前价格用于下单数量。
    def on_mark_price(self, data) -> None:
        now_ns = self.clock.timestamp_ns()
        if self.mode == "watch":
            if now_ns < self._open_ns() or now_ns > self._close_ns():
                return
        elif now_ns < self._open_ns() or now_ns >= self.fund_ns:
            return
        elif self.sent:
            return

        ins_id = getattr(data, "instrument_id", None)
        if ins_id not in self.ins_map:
            return

        px = (
            getattr(data, "value", None)
            or getattr(data, "price", None)
            or getattr(data, "mark", None)
            or getattr(data, "mark_price", None)
        )
        if px is None:
            self.log.warning(f"FundingCollector mark ignored: {data}")
            return

        row = self.ins_map[ins_id]
        price = Decimal(str(px))
        if now_ns < self.fund_ns:
            row["pre"] = price
        elif "post" not in row:
            row["post"] = price

    # heartbeat tick 负责最后一秒下单和 t+3 平仓/记账。
    def on_trade_tick(self, tick: TradeTick) -> None:
        now_ns = self.clock.timestamp_ns()
        if now_ns >= self._close_ns():
            if self.mode == "trade" and not self.closed:
                close_cnt = 0
                for ins_id in list(self.open_ids):
                    self.close_all_positions(ins_id)
                    close_cnt += 1
                self.closed = True
                self.log.info(
                    "FundingCollector close summary: "
                    f"funding={self._iso(self.fund_ns)}, close_count={close_cnt}, "
                    f"candidates={len(self.ins_map)}"
                )
                self._reset_state()
            elif self.mode == "watch" and not self.logged:
                row_cnt, fund_sum, pnl_sum, net_sum = self._write_watch()
                self.log.info(
                    "FundingCollector watch summary: "
                    f"funding={self._iso(self.fund_ns)}, rows={row_cnt}, "
                    f"candidates={len(self.ins_map)}, funding_gain={fund_sum}, "
                    f"px_pnl={pnl_sum}, net_est={net_sum}"
                )
                self._reset_state()
            return

        if self.sent:
            return
        if now_ns < self.fund_ns - int(self.config.entry_before * 1_000_000_000):
            return
        if now_ns >= self.fund_ns:
            return

        if self.mode == "trade":
            px_cnt = 0
            sub_cnt = 0
            for ins_id, row in list(self.ins_map.items()):
                if self.trade_ids is not None and ins_id not in self.trade_ids:
                    continue
                if "rate" not in row or "pre" not in row:
                    continue
                px_cnt += 1
                ins = self.ins.get(ins_id)
                if ins is None:
                    continue
                side = self._side(row["rate"])
                qty = self._qty(ins, row["pre"])
                order = self.order_factory.market(
                    instrument_id=ins_id,
                    order_side=side,
                    quantity=qty,
                    time_in_force=TimeInForce.GTC,
                )
                self.submit_order(order)
                self.open_ids.add(ins_id)
                sub_cnt += 1
            self.log.info(
                "FundingCollector entry summary: "
                f"funding={self._iso(self.fund_ns)}, candidates={len(self.ins_map)}, "
                f"priced={px_cnt}, submitted={sub_cnt}"
            )
        else:
            px_cnt = sum(1 for row in self.ins_map.values() if "pre" in row)
            self.log.info(
                "FundingCollector watch frozen: "
                f"funding={self._iso(self.fund_ns)}, candidates={len(self.ins_map)}, "
                f"priced={px_cnt}"
            )

        self.sent = True

    def on_stop(self) -> None:
        self.unsubscribe_trade_ticks(self.config.instrument_id)
        for ins_id in self.ins:
            self.unsubscribe_funding_rates(ins_id, client_id=self.cid)
            self.unsubscribe_mark_prices(ins_id, client_id=self.cid)
            self.cancel_all_orders(ins_id)

        if self.config.stop_close and self.mode == "trade":
            for ins_id in self.open_ids:
                self.close_all_positions(ins_id)

        self.log.info("FundingCollector stopped.")

    def on_reset(self) -> None:
        self.ins_map.clear()
        self.open_ids.clear()
        self.sent = False
        self.closed = False
        self.logged = False

    # 从 cache 读取 node 已加载的 USDT 本位永续。
    def _load_ins(self) -> None:
        self.ins = {
            ins.id: ins
            for ins in self.cache.instruments()
            if ins is not None and str(ins.id).split(".")[0].endswith("USDT-PERP")
        }
        if not self.ins:
            raise RuntimeError("No USDT perpetual instruments found in cache")

    # 解析 trade 模式允许下单的 base symbol 范围。
    def _load_scope(self) -> None:
        raw = self.config.trade_scope.strip()
        if raw.lower() == "all":
            self.trade_ids = None
            return

        by_base = {
            str(ins_id).split(".")[0].replace("USDT-PERP", "").upper(): ins_id
            for ins_id in self.ins
        }
        ids = set()
        for item in raw.split(","):
            key = item.strip().upper()
            if not key:
                continue
            if key not in by_base:
                raise RuntimeError(f"Unknown trade_scope instrument: {item}")
            ids.add(by_base[key])
        self.trade_ids = ids

    # 推进到下一个 UTC 4h 准点。
    def _next_time(self) -> None:
        now_ns = self.clock.timestamp_ns()
        four_ns = 4 * 60 * 60 * 1_000_000_000
        self.fund_ns = ((now_ns // four_ns) + 1) * four_ns
        self.log.info(
            "FundingCollector next time: "
            f"now={self._iso(now_ns)}, funding={self._iso(self.fund_ns)}"
        )

    def _open_ns(self) -> int:
        return self.fund_ns - int(self.config.collect_before * 1_000_000_000)

    def _close_ns(self) -> int:
        return self.fund_ns + int(self.config.close_after * 1_000_000_000)

    def _side(self, rate: Decimal) -> OrderSide:
        return OrderSide.SELL if rate > 0 else OrderSide.BUY

    def _side_name(self, side: OrderSide) -> str:
        return "BUY" if side == OrderSide.BUY else "SELL"

    def _qty(self, ins: Instrument, price: Decimal):
        raw_qty = self.notional / price
        step = Decimal(str(ins.size_increment))
        if step > 0:
            steps = (raw_qty / step).to_integral_value(rounding=ROUND_CEILING)
            raw_qty = steps * step
        return ins.make_qty(raw_qty)

    # watch 模式只写粗略收益估算。
    def _write_watch(self) -> tuple[int, Decimal, Decimal, Decimal]:
        row_cnt = 0
        fund_sum = Decimal("0")
        pnl_sum = Decimal("0")
        net_sum = Decimal("0")
        with self.log_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for ins_id, row in sorted(self.ins_map.items(), key=lambda item: str(item[0])):
                if "rate" not in row or "pre" not in row or "post" not in row:
                    continue
                rate = row["rate"]
                pre = row["pre"]
                post = row["post"]
                side = self._side(rate)
                fund_gain = abs(rate) * self.notional
                if side == OrderSide.SELL:
                    px_pnl = (pre - post) / pre * self.notional
                else:
                    px_pnl = (post - pre) / pre * self.notional
                net_est = fund_gain + px_pnl
                row_cnt += 1
                fund_sum += fund_gain
                pnl_sum += px_pnl
                net_sum += net_est
                writer.writerow(
                    [
                        self._iso(self.fund_ns),
                        ins_id,
                        rate,
                        self._side_name(side),
                        pre,
                        post,
                        self.notional,
                        fund_gain,
                        px_pnl,
                        net_est,
                    ],
                )
        self.logged = True
        return row_cnt, fund_sum, pnl_sum, net_sum

    def _init_log(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "funding_time",
                    "instrument",
                    "rate",
                    "side",
                    "pre_px",
                    "post_px",
                    "notional",
                    "funding_gain",
                    "px_pnl",
                    "net_est",
                ],
            )

    def _reset_state(self) -> None:
        self.ins_map.clear()
        self.open_ids.clear()
        self.sent = False
        self.closed = False
        self.logged = False
        self._next_time()

    def _iso(self, ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()
