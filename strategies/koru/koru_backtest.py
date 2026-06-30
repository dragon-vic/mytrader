from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


SHORT = "short"
LONG = "long"


@dataclass
class Pending:
    target: str
    remaining: int


class KoruFeatureStore:
    def __init__(self, path: str, window: int) -> None:
        columns = [
            "instrument_id",
            "ts_ns",
            "short_edge",
            "long_edge",
            f"short_mean_{window}",
            f"long_mean_{window}",
            f"window_count_{window}",
        ]
        data = pd.read_parquet(Path(path), columns=columns).sort_values("ts_ns", kind="mergesort")
        self.instrument_ids = data["instrument_id"].astype(str).to_numpy()
        self.ts_ns = data["ts_ns"].to_numpy(np.int64)
        self.short_edge = data["short_edge"].to_numpy(float)
        self.long_edge = data["long_edge"].to_numpy(float)
        self.short_mean = data[f"short_mean_{window}"].to_numpy(float)
        self.long_mean = data[f"long_mean_{window}"].to_numpy(float)
        self.count = data[f"window_count_{window}"].to_numpy(int)
        self.index = 0

    def next(self, tick: QuoteTick, warmup_minutes: int, slope_ms: int) -> tuple[float, float, float, float, float, float] | None:
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
        if self.count[row] < warmup_minutes or pd.isna(self.short_edge[row]) or pd.isna(self.long_edge[row]):
            return None
        past = max(0, int(np.searchsorted(self.ts_ns, ts_ns - slope_ms * 1_000_000, side="right") - 1))
        short_chg = self.short_edge[row] - self.short_edge[past]
        long_chg = self.long_edge[row] - self.long_edge[past]
        return self.short_edge[row], self.short_mean[row], long_edge_value(self.long_edge[row]), self.long_mean[row], short_chg, long_chg


def long_edge_value(value: float) -> float:
    return value


class KoruQuoteBacktestConfig(StrategyConfig, frozen=True):
    binance_id: str
    okx_id: str
    feature_path: str
    qty: Decimal
    capture_bps: float
    short_entry_bps: float
    long_entry_bps: float
    window_minutes: int
    warmup_minutes: int
    min_hold_sec: float
    entry_filter: str
    entry_slope_ms: int
    end_ns: int


class KoruQuoteBacktestStrategy(Strategy):
    def __init__(self, config: KoruQuoteBacktestConfig) -> None:
        super().__init__(config)
        self.binance_id = InstrumentId.from_str(config.binance_id)
        self.okx_id = InstrumentId.from_str(config.okx_id)
        self.features = KoruFeatureStore(config.feature_path, int(config.window_minutes))
        self.qty = Decimal(str(config.qty))
        self.capture_bps = float(config.capture_bps)
        self.short_entry_bps = float(config.short_entry_bps)
        self.long_entry_bps = float(config.long_entry_bps)
        self.warmup_minutes = int(config.warmup_minutes)
        self.min_hold_ns = int(float(config.min_hold_sec) * 1_000_000_000)
        self.entry_filter = str(config.entry_filter)
        self.entry_slope_ms = int(config.entry_slope_ms)
        self.end_ns = int(config.end_ns)
        self.side = "flat"
        self.pending: Pending | None = None
        self.entry_ns = 0
        self.entry_edge = 0.0

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.binance_id)
        self.subscribe_quote_ticks(self.okx_id)
        self.clock.set_time_alert_ns(
            "koru_quote_bt_flatten",
            max(0, self.end_ns - 1_000_000_000),
            callback=lambda _event: self._flatten(),
            allow_past=True,
        )

    def on_quote_tick(self, tick: QuoteTick) -> None:
        state = self.features.next(tick, self.warmup_minutes, self.entry_slope_ms)
        if self.pending is not None or state is None:
            return
        short_edge, short_mean, long_edge, long_mean, short_chg, long_chg = state
        if self.side == "flat" and short_edge >= short_mean + self.short_entry_bps and self._entry_ok(SHORT, short_chg):
            self._trade_to(SHORT)
            self.entry_ns = tick.ts_init
            self.entry_edge = short_edge
        elif self.side == "flat" and long_edge <= long_mean - self.long_entry_bps and self._entry_ok(LONG, long_chg):
            self._trade_to(LONG)
            self.entry_ns = tick.ts_init
            self.entry_edge = long_edge
        elif self.side == SHORT and self._can_exit(tick.ts_init) and short_edge <= self.entry_edge - self.capture_bps:
            self._flatten()
        elif self.side == LONG and self._can_exit(tick.ts_init) and long_edge >= self.entry_edge + self.capture_bps:
            self._flatten()

    def on_order_filled(self, event: OrderFilled) -> None:
        if self.pending is None:
            return
        self.pending.remaining -= 1
        if self.pending.remaining <= 0:
            self.side = self.pending.target
            self.pending = None

    def on_stop(self) -> None:
        self.unsubscribe_quote_ticks(self.binance_id)
        self.unsubscribe_quote_ticks(self.okx_id)

    def _entry_ok(self, side: str, edge_chg: float) -> bool:
        if self.entry_filter == "slow":
            return edge_chg > 0.0 if side == SHORT else edge_chg < 0.0
        return True

    def _trade_to(self, target: str) -> None:
        if target == self.side or self.pending is not None:
            return
        if target == SHORT:
            self._submit(self.binance_id, OrderSide.BUY, self.qty)
            self._submit(self.okx_id, OrderSide.SELL, self.qty)
        else:
            self._submit(self.binance_id, OrderSide.SELL, self.qty)
            self._submit(self.okx_id, OrderSide.BUY, self.qty)
        self.pending = Pending(target=target, remaining=2)

    def _can_exit(self, ts_ns: int) -> bool:
        return ts_ns >= self.entry_ns + self.min_hold_ns

    def _flatten(self) -> None:
        if self.pending is not None or self.side == "flat":
            return
        if self.side == SHORT:
            self._submit(self.binance_id, OrderSide.SELL, self.qty)
            self._submit(self.okx_id, OrderSide.BUY, self.qty)
        else:
            self._submit(self.binance_id, OrderSide.BUY, self.qty)
            self._submit(self.okx_id, OrderSide.SELL, self.qty)
        self.pending = Pending(target="flat", remaining=2)

    def _submit(self, instrument_id: InstrumentId, side: OrderSide, qty: Decimal) -> None:
        instrument = self.cache.instrument(instrument_id)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=instrument.make_qty(qty),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
