from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from decimal import Decimal
from pathlib import Path

import pandas as pd
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


LONG = "long"
SHORT = "short"


def decimal_param(value: object) -> Decimal:
    if isinstance(value, bool):
        raise TypeError("numeric parameter must not be bool")
    return Decimal(str(value))


def float_param(value: object) -> float:
    if isinstance(value, bool):
        raise TypeError("numeric parameter must not be bool")
    return float(str(value))


def event_decimal(value: object) -> Decimal:
    return Decimal(str(value).split()[0].replace("_", ""))


@dataclass
class Pending:
    target: str
    qty: Decimal
    edge: float = 0.0
    ts_ns: int = 0
    force: bool = False
    order_ids: set[str] = field(default_factory=set)
    order_instrument: dict[str, str] = field(default_factory=dict)
    filled_qty: dict[str, Decimal] = field(default_factory=dict)
    filled_value: dict[str, Decimal] = field(default_factory=dict)
    completed: set[str] = field(default_factory=set)


@dataclass
class Candidate:
    target: str
    edge_side: str
    edge: float


@dataclass
class Signal:
    target: str
    edge_side: str
    version: int


class FeatureStore:
    def __init__(self, path: str, window: int) -> None:
        column_map = {
            "short_mean": f"short_mean_{window}",
            "short_std": f"short_std_{window}",
            "long_mean": f"long_mean_{window}",
            "long_std": f"long_std_{window}",
            "count": f"window_count_{window}",
        }
        data = pd.read_parquet(
            Path(path),
            columns=["instrument_id", "ts_ns", "short_edge", "long_edge", *column_map.values()],
        ).sort_values("ts_ns", kind="mergesort")
        self.instrument_ids = data["instrument_id"].astype(str).to_numpy()
        self.ts_ns = data["ts_ns"].to_numpy()
        self.short_edge = data["short_edge"].to_numpy(float)
        self.long_edge = data["long_edge"].to_numpy(float)
        self.short_mean = data[column_map["short_mean"]].to_numpy(float)
        self.short_std = data[column_map["short_std"]].to_numpy(float)
        self.long_mean = data[column_map["long_mean"]].to_numpy(float)
        self.long_std = data[column_map["long_std"]].to_numpy(float)
        self.count = data[column_map["count"]].to_numpy(int)
        self.index = 0

    def next(self, tick: QuoteTick, window_minutes: int) -> tuple[float, float, float, float, float, float] | None:
        if self.index >= len(self.ts_ns):
            raise RuntimeError("feature file ended before quote ticks")
        instrument_id = str(tick.instrument_id)
        ts_ns = int(tick.ts_init)
        if self.ts_ns[self.index] != ts_ns or self.instrument_ids[self.index] != instrument_id:
            raise RuntimeError(
                f"feature row mismatch: expected {instrument_id}@{ts_ns}, "
                f"got {self.instrument_ids[self.index]}@{self.ts_ns[self.index]}",
            )
        row = self.index
        self.index += 1
        if self.count[row] < window_minutes or pd.isna(self.short_edge[row]) or pd.isna(self.long_edge[row]):
            return None
        return (
            self.short_edge[row],
            self.short_mean[row],
            self.short_std[row],
            self.long_edge[row],
            self.long_mean[row],
            self.long_std[row],
        )


class PreIpoQuoteBacktestConfig(StrategyConfig, frozen=True):
    binance_id: str
    okx_id: str
    feature_path: str
    qty: Decimal
    max_position: Decimal
    capture_bps: float
    std_mult: float
    entry_bps: float
    # long_entry_bps: float
    short_exit_bps: float
    long_exit_bps: float
    window_minutes: int
    min_hold_sec: float
    signal_delay_ms: float
    end_ns: int


class PreIpoQuoteBacktestStrategy(Strategy):
    def __init__(self, config: PreIpoQuoteBacktestConfig) -> None:
        super().__init__(config)
        self.binance_id = InstrumentId.from_str(config.binance_id)
        self.okx_id = InstrumentId.from_str(config.okx_id)
        self.window_minutes = int(config.window_minutes)
        self.features = FeatureStore(config.feature_path, self.window_minutes)
        self.qty = decimal_param(config.qty)
        self.max_position = decimal_param(config.max_position)
        if self.max_position <= 0:
            raise RuntimeError("max_position must be positive")
        self.capture_bps = float_param(config.capture_bps)
        self.std_mult = float_param(config.std_mult)
        self.short_entry_bps = float_param(config.entry_bps)
        self.long_entry_bps = float_param(config.entry_bps)
        self.short_exit_bps = float_param(config.short_exit_bps)
        self.long_exit_bps = float_param(config.long_exit_bps)
        self.min_hold_ns = int(float_param(config.min_hold_sec) * 1_000_000_000)
        self.signal_delay_ns = int(float_param(config.signal_delay_ms) * 1_000_000)
        if self.signal_delay_ns < 0:
            raise RuntimeError("signal_delay_ms must be non-negative")
        self.end_ns = int(config.end_ns)
        self.side = "flat"
        self.position_qty = Decimal("0")
        self.pending: Pending | None = None
        self.halted = False
        self.signal: Signal | None = None
        self.signal_version = 0
        self.last_state: tuple[float, float, float, float, float, float] | None = None
        self.entry_ns = 0
        self.entry_edge = 0.0

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.binance_id)
        self.subscribe_quote_ticks(self.okx_id)
        # 给最终强平市价单留出 latency 后的 quote 触发成交。
        self.clock.set_time_alert_ns(
            "preipo_quote_bt_flatten",
            max(0, self.end_ns - 1_000_000_000),
            callback=lambda _event: self._flatten(force=True),
            allow_past=True,
        )

    def on_quote_tick(self, tick: QuoteTick) -> None:
        state = self.features.next(tick, self.window_minutes)
        self.last_state = state
        if self.halted:
            self._cancel_signal()
            return
        if self.pending is not None:
            self._cancel_signal()
            return
        if state is None:
            self._cancel_signal()
            return
        candidate = self._candidate(state, int(tick.ts_init))
        if candidate is None:
            self._cancel_signal()
            return
        self._queue_candidate(candidate, int(tick.ts_init))

    def on_order_filled(self, event: OrderFilled) -> None:
        if self.pending is None:
            return
        order_id = str(event.client_order_id)
        if order_id not in self.pending.order_ids:
            return
        fill_qty = event_decimal(event.last_qty)
        self.pending.filled_qty[order_id] += fill_qty
        self.pending.filled_value[order_id] += fill_qty * event_decimal(event.last_px)
        if self.pending.filled_qty[order_id] >= self.pending.qty:
            self.pending.completed.add(order_id)
        if len(self.pending.completed) >= 2:
            keep_flattening = self._confirm_pending()
            self.pending = None
            if keep_flattening:
                self._flatten(force=True)

    def on_order_denied(self, event: OrderDenied) -> None:
        pending = self.pending
        self.pending = None
        self.halted = True
        self._cancel_signal()
        pending_desc = "none" if pending is None else f"{pending.target} qty={pending.qty} completed={len(pending.completed)}"
        message = (
            f"order_denied {event.instrument_id} order={event.client_order_id} "
            f"reason={event.reason} pending={pending_desc}"
        )
        self.log.warning(message)

    def on_stop(self) -> None:
        self.unsubscribe_quote_ticks(self.binance_id)
        self.unsubscribe_quote_ticks(self.okx_id)

    def _confirm_pending(self) -> bool:
        pending = self.pending
        if pending is None:
            return False
        if pending.target == "flat":
            self.position_qty = max(self.position_qty - pending.qty, Decimal("0"))
            if self.position_qty <= 0:
                self.side = "flat"
                self.entry_ns = 0
                self.entry_edge = 0.0
                return False
            return pending.force
        previous_qty = self.position_qty if self.side == pending.target else Decimal("0")
        new_qty = previous_qty + pending.qty
        if new_qty <= 0:
            raise RuntimeError("pending open qty must be positive")
        entry_edge = self._pending_entry_edge(pending)
        if previous_qty > 0:
            self.entry_edge = (
                self.entry_edge * float(previous_qty) + entry_edge * float(pending.qty)
            ) / float(new_qty)
        else:
            self.entry_edge = entry_edge
        self.entry_ns = pending.ts_ns
        self.position_qty = new_qty
        self.side = pending.target
        return False

    # 开仓 edge 使用两腿实际成交均价；拿不到成交价时退回信号 edge。
    def _pending_entry_edge(self, pending: Pending) -> float:
        binance_avg = self._filled_avg(pending, self.binance_id)
        okx_avg = self._filled_avg(pending, self.okx_id)
        if binance_avg is None or okx_avg is None or binance_avg <= 0:
            return pending.edge
        return float((okx_avg - binance_avg) / binance_avg * Decimal("10000"))

    def _filled_avg(self, pending: Pending, instrument_id: InstrumentId) -> Decimal | None:
        qty, value = self._filled_totals(pending, instrument_id)
        if qty <= 0:
            return None
        return value / qty

    def _filled_totals(self, pending: Pending, instrument_id: InstrumentId) -> tuple[Decimal, Decimal]:
        wanted = str(instrument_id)
        qty = Decimal("0")
        value = Decimal("0")
        for order_id in pending.order_ids:
            if pending.order_instrument.get(order_id) != wanted:
                continue
            qty += pending.filled_qty[order_id]
            value += pending.filled_value[order_id]
        return qty, value

    def _trade_to(self, target: str, edge: float, ts_ns: int) -> None:
        if self.halted or self.pending is not None or not self._can_open(target):
            return
        qty = min(self.qty, self.max_position - self.position_qty)
        if qty <= 0:
            return
        pending = Pending(target=target, qty=qty, edge=edge, ts_ns=ts_ns)
        self.pending = pending
        if target == SHORT:
            self._submit(self.binance_id, OrderSide.BUY, qty)
            if self.pending is pending:
                self._submit(self.okx_id, OrderSide.SELL, qty)
        else:
            self._submit(self.binance_id, OrderSide.SELL, qty)
            if self.pending is pending:
                self._submit(self.okx_id, OrderSide.BUY, qty)

    def _candidate(
        self,
        state: tuple[float, float, float, float, float, float],
        ts_ns: int,
        edge_side: str | None = None,
    ) -> Candidate | None:
        short_edge, short_mean, short_std, long_edge, long_mean, long_std = state
        short_band = max(self.short_entry_bps, self.std_mult * short_std)
        long_band = max(self.long_entry_bps, self.std_mult * long_std)
        if self.side == SHORT and edge_side in (None, SHORT) and self._can_exit(ts_ns) and self._short_exit(short_edge, short_mean):
            return Candidate(target="flat", edge_side=SHORT, edge=short_edge)
        if self.side == LONG and edge_side in (None, LONG) and self._can_exit(ts_ns) and self._long_exit(long_edge, long_mean):
            return Candidate(target="flat", edge_side=LONG, edge=long_edge)
        if self._can_open(SHORT) and edge_side in (None, SHORT) and short_edge >= short_mean + short_band:
            return Candidate(target=SHORT, edge_side=SHORT, edge=short_edge)
        if self._can_open(LONG) and edge_side in (None, LONG) and long_edge <= long_mean - long_band:
            return Candidate(target=LONG, edge_side=LONG, edge=long_edge)
        return None

    def _queue_candidate(self, candidate: Candidate, ts_ns: int) -> None:
        if self.signal_delay_ns == 0:
            self._submit_candidate(candidate, ts_ns)
            return
        if self.signal is not None:
            return
        self.signal_version += 1
        version = self.signal_version
        self.signal = Signal(candidate.target, candidate.edge_side, version)
        self.clock.set_time_alert_ns(
            "preipo_quote_bt_signal",
            ts_ns + self.signal_delay_ns,
            callback=lambda _event, target=candidate.target, edge_side=candidate.edge_side, version=version: self._on_signal_alert(
                target,
                edge_side,
                version,
            ),
            allow_past=True,
        )

    def _on_signal_alert(self, target: str, edge_side: str, version: int) -> None:
        if self.signal is None or self.signal.version != version:
            return
        self.signal = None
        if self.pending is not None or self.last_state is None:
            return
        candidate = self._candidate(self.last_state, self.clock.timestamp_ns(), edge_side=edge_side)
        if candidate is None or candidate.target != target:
            return
        self._submit_candidate(candidate, self.clock.timestamp_ns())

    def _cancel_signal(self) -> None:
        if self.signal is None:
            return
        self.signal = None
        self.signal_version += 1
        self.clock.cancel_timer("preipo_quote_bt_signal")

    def _submit_candidate(self, candidate: Candidate, ts_ns: int) -> None:
        if candidate.target == "flat":
            self._flatten()
            return
        self._trade_to(candidate.target, candidate.edge, ts_ns)

    def _can_exit(self, ts_ns: int) -> bool:
        return ts_ns >= self.entry_ns + self.min_hold_ns

    def _can_open(self, target: str) -> bool:
        return self.side == "flat" or (self.side == target and self.position_qty < self.max_position)

    def _short_exit(self, edge: float, mean: float) -> bool:
        return (edge <= mean + self.short_exit_bps) or (edge <= self.entry_edge - self.capture_bps)

    def _long_exit(self, edge: float, mean: float) -> bool:
        return (edge >= mean - self.long_exit_bps) or (edge >= self.entry_edge + self.capture_bps)

    def _flatten(self, force: bool = False) -> None:
        if self.halted or self.pending is not None or self.side == "flat" or self.position_qty <= 0:
            return
        qty = self.position_qty if force else min(self.qty, self.position_qty)
        pending = Pending(target="flat", qty=qty, force=force)
        self.pending = pending
        if self.side == SHORT:
            self._submit(self.binance_id, OrderSide.SELL, qty)
            if self.pending is pending:
                self._submit(self.okx_id, OrderSide.BUY, qty)
        else:
            self._submit(self.binance_id, OrderSide.BUY, qty)
            if self.pending is pending:
                self._submit(self.okx_id, OrderSide.SELL, qty)

    def _submit(self, instrument_id: InstrumentId, side: OrderSide, qty: Decimal) -> None:
        instrument = self.cache.instrument(instrument_id)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=instrument.make_qty(qty),
            time_in_force=TimeInForce.GTC,
        )
        if self.pending is not None:
            order_id = str(order.client_order_id)
            self.pending.order_ids.add(order_id)
            self.pending.order_instrument[order_id] = str(instrument_id)
            self.pending.filled_qty[order_id] = Decimal("0")
            self.pending.filled_value[order_id] = Decimal("0")
        self.submit_order(order)
