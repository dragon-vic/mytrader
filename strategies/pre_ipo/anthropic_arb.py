from __future__ import annotations

import json
from dataclasses import dataclass
from collections import deque
from decimal import Decimal
from pathlib import Path

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


BPS = Decimal("10000")
MINUTE_NS = 60_000_000_000
STATE_FLAT = "flat"
STATE_PENDING = "pending"
STATE_LONG = "long"
STATE_SHORT = "short"
STATE_FAIL = "fail"


@dataclass
class PendingLeg:
    order_id: str
    instrument_id: InstrumentId
    side: OrderSide
    target_qty: Decimal
    filled_qty: Decimal = Decimal("0")
    failed: bool = False

    def filled(self) -> bool:
        return self.filled_qty >= self.target_qty

    def done(self) -> bool:
        return self.failed or self.filled()


@dataclass
class PendingPair:
    legs: dict[str, PendingLeg]
    repair: PendingLeg | None = None

    def record_fill(self, order_id: str, qty: Decimal) -> None:
        self.leg(order_id).filled_qty += qty

    def record_failed(self, order_id: str) -> None:
        self.leg(order_id).failed = True

    def is_done(self) -> bool:
        return all(leg.done() for leg in self.legs.values())

    def failed_count(self) -> int:
        return sum(1 for leg in self.legs.values() if leg.failed)

    def is_complete(self) -> bool:
        return all(leg.filled() for leg in self.legs.values())

    def is_all_failed(self) -> bool:
        return self.is_done() and self.failed_count() == len(self.legs)

    def has_order(self, order_id: str) -> bool:
        return order_id in self.legs or (self.repair is not None and self.repair.order_id == order_id)

    def leg(self, order_id: str) -> PendingLeg:
        if order_id in self.legs:
            return self.legs[order_id]
        return self.repair

    def filled_legs(self) -> list[PendingLeg]:
        return [leg for leg in self.legs.values() if leg.filled()]

    def repair_done(self) -> bool:
        return self.repair is not None and self.repair.filled()


@dataclass
class EdgePair:
    window_ns: int
    long_mean_bps: Decimal
    short_mean_bps: Decimal
    entry_bps: Decimal
    exit_bps: Decimal
    long_max_bps: Decimal
    short_min_bps: Decimal
    long_bps: Decimal | None = None
    short_bps: Decimal | None = None
    long_values: deque[tuple[int, Decimal]] = None
    short_values: deque[tuple[int, Decimal]] = None
    minute_ns: int | None = None
    minute_prices: dict[InstrumentId, tuple[Decimal, Decimal, int]] = None
    last_price_means: dict[InstrumentId, tuple[Decimal, Decimal]] = None

    def __post_init__(self) -> None:
        self.long_values = deque()
        self.short_values = deque()
        self.minute_prices = {}
        self.last_price_means = {}

    def update(self, binance: QuoteTick, okx: QuoteTick) -> None:
        bn_bid = Decimal(str(binance.bid_price))
        bn_ask = Decimal(str(binance.ask_price))
        okx_bid = Decimal(str(okx.bid_price))
        okx_ask = Decimal(str(okx.ask_price))
        self.long_bps, self.short_bps = self.from_prices(bn_bid, bn_ask, okx_bid, okx_ask)

    def record_quote(self, tick: QuoteTick, binance_id: InstrumentId, okx_id: InstrumentId) -> None:
        minute_ns = int(tick.ts_event) // MINUTE_NS * MINUTE_NS
        if self.minute_ns is None:
            self.minute_ns = minute_ns
        while minute_ns > self.minute_ns:
            self.close_minute(self.minute_ns, binance_id, okx_id)
            self.minute_ns += MINUTE_NS
        bid_sum, ask_sum, count = self.minute_prices.get(tick.instrument_id, (Decimal("0"), Decimal("0"), 0))
        self.minute_prices[tick.instrument_id] = (
            bid_sum + Decimal(str(tick.bid_price)),
            ask_sum + Decimal(str(tick.ask_price)),
            count + 1,
        )

    def fill_to(self, now_ns: int, binance_id: InstrumentId, okx_id: InstrumentId) -> None:
        current_minute_ns = now_ns // MINUTE_NS * MINUTE_NS
        while self.minute_ns is not None and self.minute_ns < current_minute_ns:
            self.close_minute(self.minute_ns, binance_id, okx_id)
            self.minute_ns += MINUTE_NS

    def close_minute(self, minute_ns: int, binance_id: InstrumentId, okx_id: InstrumentId) -> None:
        for instrument_id, (bid_sum, ask_sum, count) in self.minute_prices.items():
            self.last_price_means[instrument_id] = bid_sum / Decimal(count), ask_sum / Decimal(count)
        bn_bid, bn_ask = self.last_price_means[binance_id]
        okx_bid, okx_ask = self.last_price_means[okx_id]
        long_bps, short_bps = self.from_prices(bn_bid, bn_ask, okx_bid, okx_ask)
        self._add_value(self.long_values, minute_ns, long_bps)
        self._add_value(self.short_values, minute_ns, short_bps)
        self.update_mean(self._mean(self.long_values), self._mean(self.short_values))
        self.minute_prices.clear()

    def _add_value(self, values: deque[tuple[int, Decimal]], ts_ns: int, value: Decimal) -> None:
        values.append((ts_ns, value))
        cutoff = ts_ns - self.window_ns
        while values and values[0][0] < cutoff:
            values.popleft()

    def _mean(self, values: deque[tuple[int, Decimal]]) -> Decimal:
        return sum((value for _, value in values), Decimal("0")) / Decimal(len(values))

    def from_prices(
        self,
        bn_bid: Decimal,
        bn_ask: Decimal,
        okx_bid: Decimal,
        okx_ask: Decimal,
    ) -> tuple[Decimal, Decimal]:
        bn_mid = (bn_bid + bn_ask) / Decimal("2")
        # edge 定义为 OKX 相对 Binance 的溢价；long 买 OKX/卖 Binance，short 卖 OKX/买 Binance。
        return (okx_ask - bn_bid) / bn_mid * BPS, (okx_bid - bn_ask) / bn_mid * BPS

    def update_mean(self, long_mean_bps: Decimal, short_mean_bps: Decimal) -> None:
        self.long_mean_bps = long_mean_bps
        self.short_mean_bps = short_mean_bps

    def signal(self, state: str) -> str | None:
        long_threshold = self.exit_bps if state == STATE_SHORT else self.entry_bps
        short_threshold = self.exit_bps if state == STATE_LONG else self.entry_bps
        if self.long_mean_bps - self.long_bps > long_threshold and self.long_bps <= self.long_max_bps:
            return "long"
        if self.short_bps - self.short_mean_bps > short_threshold and self.short_bps >= self.short_min_bps:
            return "short"
        return None


class AnthropicArbConfig(StrategyConfig, frozen=True):
    instruments: list[str]
    window_minutes: Decimal
    snapshot_path: str
    long_mean_bps: Decimal
    short_mean_bps: Decimal
    entry_bps: Decimal
    exit_bps: Decimal
    long_max_bps: Decimal
    short_min_bps: Decimal
    qty: Decimal
    okx_qty_multiplier: Decimal


class AnthropicArbStrategy(Strategy):
    def __init__(self, config: AnthropicArbConfig) -> None:
        super().__init__(config)
        self.instruments = [InstrumentId.from_str(value) for value in config.instruments]
        self.quotes: dict[InstrumentId, QuoteTick] = {}
        self.housekeeping_interval_ns = 1_000_000_000
        self.snapshot_path = Path(config.snapshot_path)
        self.edge = EdgePair(
            window_ns=int(config.window_minutes * Decimal(MINUTE_NS)),
            long_mean_bps=config.long_mean_bps,
            short_mean_bps=config.short_mean_bps,
            entry_bps=config.entry_bps,
            exit_bps=config.exit_bps,
            long_max_bps=config.long_max_bps,
            short_min_bps=config.short_min_bps,
        )
        self.qty = config.qty
        self.okx_qty_multiplier = config.okx_qty_multiplier
        self.state = STATE_FLAT
        self.pending: PendingPair | None = None
        self.housekeeping_seq = 0

    # 策略启动入口，后续逐步补订阅、warmup 和状态初始化。
    def on_start(self) -> None:
        for instrument_id in self.instruments:
            self.subscribe_quote_ticks(instrument_id)
        self._schedule_housekeeping()

    # 策略停止入口，后续补充清理和退出状态写入。
    def on_stop(self) -> None:
        for instrument_id in self.instruments:
            self.unsubscribe_quote_ticks(instrument_id)


    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.quotes[tick.instrument_id] = tick
        self.edge.record_quote(tick, self.instruments[0], self.instruments[1])
        self._update_edges()
        if self.state not in {STATE_PENDING, STATE_FAIL}:
            signal = self._signal_determination()
            if signal is not None:
                self._submit_signal(signal)

    def on_order_filled(self, event: OrderFilled) -> None:
        order_id = str(event.client_order_id)
        if self.pending is None or not self.pending.has_order(order_id):
            return
        self.pending.record_fill(order_id, Decimal(str(event.last_qty)))
        self._resolve_pending_if_done()

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def on_order_denied(self, event: OrderDenied) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def on_order_canceled(self, event: OrderCanceled) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def on_order_expired(self, event: OrderExpired) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def _update_edges(self) -> None:
        binance = self.quotes[self.instruments[0]]
        okx = self.quotes[self.instruments[1]]
        self.edge.update(binance, okx)

    def _signal_determination(self) -> str | None:
        return self.edge.signal(self.state)

    # 低频维护任务：更新 rolling mean，并按间隔写 snapshot。
    def _schedule_housekeeping(self) -> None:
        self.housekeeping_seq += 1
        self.clock.set_time_alert_ns(
            f"anthropic_arb_housekeeping_{self.housekeeping_seq}",
            self.clock.timestamp_ns() + self.housekeeping_interval_ns,
            callback=lambda _event: self._on_housekeeping(),
            allow_past=True,
        )

    def _on_housekeeping(self) -> None:
        now_ns = self.clock.timestamp_ns()
        self._update_mean(now_ns)
        self._maybe_write_snapshot(now_ns)
        self._schedule_housekeeping()

    def _update_mean(self, now_ns: int) -> None:
        self.edge.fill_to(now_ns, self.instruments[0], self.instruments[1])

    def _maybe_write_snapshot(self, now_ns: int) -> None:
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "state": self.state,
            "long_edge_bps": str(self.edge.long_bps),
            "short_edge_bps": str(self.edge.short_bps),
            "long_mean_bps": str(self.edge.long_mean_bps),
            "short_mean_bps": str(self.edge.short_mean_bps),
            "pending": self.pending is not None,
        }
        tmp = self.snapshot_path.with_suffix(f"{self.snapshot_path.suffix}.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.snapshot_path)

    def _submit_signal(self, signal: str) -> None:
        self.state = STATE_PENDING
        if signal == "long":
            buy_id, sell_id = self.instruments[1], self.instruments[0]
            buy_qty, sell_qty = self.qty * self.okx_qty_multiplier, self.qty
        else:
            buy_id, sell_id = self.instruments[0], self.instruments[1]
            buy_qty, sell_qty = self.qty, self.qty * self.okx_qty_multiplier

        buy_quantity = self.cache.instrument(buy_id).make_qty(buy_qty)
        sell_quantity = self.cache.instrument(sell_id).make_qty(sell_qty)
        buy_order = self.order_factory.market(
            instrument_id=buy_id,
            order_side=OrderSide.BUY,
            quantity=buy_quantity,
            time_in_force=TimeInForce.GTC,
        )
        sell_order = self.order_factory.market(
            instrument_id=sell_id,
            order_side=OrderSide.SELL,
            quantity=sell_quantity,
            time_in_force=TimeInForce.GTC,
        )
        legs = {
            str(buy_order.client_order_id): PendingLeg(
                order_id=str(buy_order.client_order_id),
                instrument_id=buy_id,
                side=OrderSide.BUY,
                target_qty=Decimal(str(buy_quantity)),
            ),
            str(sell_order.client_order_id): PendingLeg(
                order_id=str(sell_order.client_order_id),
                instrument_id=sell_id,
                side=OrderSide.SELL,
                target_qty=Decimal(str(sell_quantity)),
            ),
        }
        self.pending = PendingPair(legs=legs)
        self.submit_order(buy_order)
        self.submit_order(sell_order)

    def _sync_state_from_inventory(self) -> None:
        positions = self.cache.positions_open(instrument_id=self.instruments[0], strategy_id=self.id)
        if not positions:
            self.state = STATE_FLAT
            return
        position = positions[0]
        self.state = STATE_SHORT if bool(position.is_long) else STATE_LONG

    def _mark_order_failed(self, order_id: str) -> None:
        if self.pending is not None and self.pending.has_order(order_id):
            leg = self.pending.leg(order_id)
            self.log.error(
                f"pending_order_failed order={order_id} instrument={leg.instrument_id} side={leg.side} "
                f"filled={leg.filled_qty} target={leg.target_qty}",
            )
            self.pending.record_failed(order_id)
            self._resolve_pending_if_done()

    def _resolve_pending_if_done(self) -> None:
        if self.pending is None:
            return
        pending = self.pending
        if pending.repair is not None:
            if pending.repair_done() and self._inventory_balanced():
                self.pending = None
                self._sync_state_from_inventory()
            return
        if not pending.is_done():
            return
        if pending.is_complete():
            self.pending = None
            self._sync_state_from_inventory()
            return
        if pending.is_all_failed():
            self.log.warning(f"pending_pair_all_failed orders={','.join(pending.legs)}")
            self.pending = None
            self._sync_state_from_inventory()
            return
        self.state = STATE_FAIL
        self._submit_repair_order(pending.filled_legs()[0])

    def _submit_repair_order(self, leg: PendingLeg) -> None:
        side = OrderSide.SELL if leg.side == OrderSide.BUY else OrderSide.BUY
        quantity = self.cache.instrument(leg.instrument_id).make_qty(leg.filled_qty)
        order = self.order_factory.market(
            instrument_id=leg.instrument_id,
            order_side=side,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )
        self.pending.repair = PendingLeg(
            order_id=str(order.client_order_id),
            instrument_id=leg.instrument_id,
            side=side,
            target_qty=Decimal(str(quantity)),
        )
        self.submit_order(order)

    def _inventory_balanced(self) -> bool:
        bn_qty = self._net_qty(self.instruments[0])
        okx_qty = self._net_qty(self.instruments[1])
        return bn_qty + okx_qty / self.okx_qty_multiplier == Decimal("0")

    def _net_qty(self, instrument_id: InstrumentId) -> Decimal:
        positions = self.cache.positions_open(instrument_id=instrument_id, strategy_id=self.id)
        if not positions:
            return Decimal("0")
        position = positions[0]
        qty = Decimal(str(position.quantity))
        return qty if bool(position.is_long) else -qty
