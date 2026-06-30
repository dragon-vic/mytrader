from __future__ import annotations

import json
import random
import sys
import time
from collections import defaultdict
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from math import floor
from math import sqrt
from pathlib import Path
from threading import Event as ThreadEvent
from threading import Lock
from threading import Thread

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from rich.console import Console
from rich.live import Live
from rich.table import Table

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import NODE_STOP_TOPIC


FLAT = "FLAT"
OPENING = "OPENING"
OPEN = "OPEN"
CLOSE = "CLOSE"
FLIP = "FLIP"
LONG_EDGE = "long_edge"
SHORT_EDGE = "short_edge"
BEIJING_TZ = timezone(timedelta(hours=8))
MINUTE_NS = 60_000_000_000
COLLECTOR_RAW_MTIME_SAFETY_SEC = 2
COLLECTOR_COLUMNS = ("ts_local_ns", "ts_exchange_ms", "venue", "symbol", "bid", "ask", "bid_size", "ask_size")


@dataclass
class ArbLeg:
    instrument_id: InstrumentId
    price: Decimal


@dataclass
class ArbPos:
    lot_id: int
    asset: str
    buy_id: InstrumentId
    sell_id: InstrumentId
    buy_px: Decimal
    sell_px: Decimal
    entry_buy_avg_px: Decimal | None
    entry_sell_avg_px: Decimal | None
    entry_fee: Decimal
    buy_qty: Decimal
    sell_qty: Decimal
    edge_bps: Decimal
    actual_entry_edge_bps: Decimal | None
    mean_bps: float
    std_bps: float
    z_score: float
    opened_ns: int
    edge_side: str = SHORT_EDGE
    grid_level: int | None = None


@dataclass
class SpreadState:
    buy: ArbLeg
    sell: ArbLeg
    edge_side: str
    edge_bps: Decimal
    mean_bps: float
    std_bps: float
    z_score: float
    samples: int
    window_sec: float


@dataclass
class PendingLeg:
    instrument_id: InstrumentId
    side: OrderSide
    order: MarketOrder
    target_qty: Decimal
    filled_qty: Decimal
    filled_value: Decimal
    filled_fee: Decimal
    best_px: Decimal | None = None


@dataclass
class PendingBatch:
    asset: str
    action: str
    lot_id: int
    buy_id: InstrumentId
    sell_id: InstrumentId
    buy_px: Decimal
    sell_px: Decimal
    edge_bps: Decimal
    mean_bps: float
    std_bps: float
    z_score: float
    created_ns: int
    legs: dict[str, PendingLeg]
    edge_side: str = SHORT_EDGE
    before_inventory: int = 0
    after_inventory: int = 0
    grid_level: int | None = None
    close_lot_id: int | None = None
    expected_capture_bps: Decimal | None = None
    inventory_delta: int = 0
    open_qty: Decimal = Decimal("0")
    close_qty: Decimal = Decimal("0")


# 按分钟等权统计 edge；缺失分钟沿用上一分钟，避免 quote burst 改变 3h 均线权重。
class SpreadWindow:
    def __init__(self) -> None:
        self.points: deque[tuple[int, float]] = deque()
        self.total = 0.0
        self.total_sq = 0.0
        self.current_minute_ns: int | None = None
        self.current_total = 0.0
        self.current_count = 0
        self.last_value: float | None = None

    def add(self, ts_ns: int, value: float, lookback_ns: int) -> None:
        minute_ns = ts_ns // MINUTE_NS * MINUTE_NS
        if self.current_minute_ns is None:
            self.current_minute_ns = minute_ns
        elif minute_ns < self.current_minute_ns:
            return
        elif minute_ns > self.current_minute_ns:
            self._finalize_current()
            fill_ns = self.current_minute_ns + MINUTE_NS
            while fill_ns < minute_ns and self.last_value is not None:
                self._append_minute(fill_ns, self.last_value)
                fill_ns += MINUTE_NS
            self.current_minute_ns = minute_ns
            self.current_total = 0.0
            self.current_count = 0
        self.current_total += value
        self.current_count += 1
        self._trim(lookback_ns)

    def stats(self) -> tuple[int, float, float, float]:
        current = self._current_value()
        count = len(self.points) + (1 if current is not None else 0)
        if count == 0:
            return 0, 0.0, 0.0, 0.0
        total = self.total + (current if current is not None else 0.0)
        total_sq = self.total_sq + (current * current if current is not None else 0.0)
        mean = total / count
        variance = max(total_sq / count - mean * mean, 0.0)
        first_ns = self.points[0][0] if self.points else self.current_minute_ns
        last_ns = self.current_minute_ns if current is not None else self.points[-1][0]
        window_sec = (last_ns - first_ns) / 1_000_000_000 + 60.0
        return count, mean, sqrt(variance), window_sec

    def _current_value(self) -> float | None:
        if self.current_count <= 0:
            return None
        return self.current_total / self.current_count

    def _finalize_current(self) -> None:
        value = self._current_value()
        if value is None or self.current_minute_ns is None:
            return
        self._append_minute(self.current_minute_ns, value)
        self.last_value = value

    def _append_minute(self, minute_ns: int, value: float) -> None:
        self.points.append((minute_ns, value))
        self.total += value
        self.total_sq += value * value

    def _trim(self, lookback_ns: int) -> None:
        max_points = max(int(lookback_ns // MINUTE_NS), 1)
        current_count = 1 if self.current_count > 0 else 0
        while len(self.points) + current_count > max_points:
            _, old = self.points.popleft()
            self.total -= old
            self.total_sq -= old * old


class PreIpoConfig(StrategyConfig, frozen=True):
    instruments: list[str]
    assets: list[str]
    min_window_sec: float
    init_fetch_sec: float
    grid_center_sec: float
    asset_grid_params: dict[str, dict[str, object]]
    trade_qty: Decimal
    snapshot_interval_sec: float
    snapshot_display: str
    snapshot_path: str
    slippage_bps: Decimal
    margin_leverage: Decimal
    risk_enabled: bool
    risk_max_unrealized_loss_ratio: Decimal
    max_quote_delay_ms: float
    quote_summary_interval_sec: float
    quote_sample_rates: dict[str, float]
    fee_bps: dict[str, float]


class PreIpoStrategy(Strategy):
    def __init__(self, config: PreIpoConfig) -> None:
        super().__init__(config)
        self.instruments = [InstrumentId.from_str(value) for value in config.instruments]
        self.assets = [asset.upper() for asset in config.assets]
        self.instrument_assets = {instrument_id: self._parse_asset(instrument_id) for instrument_id in self.instruments}
        self.instrument_venues = {instrument_id: str(instrument_id.venue).upper() for instrument_id in self.instruments}
        self.min_window_ns = int(float(config.min_window_sec) * 1_000_000_000)
        self.init_fetch_sec = float(config.init_fetch_sec)
        self.grid_center_ns = int(float(config.grid_center_sec) * 1_000_000_000)
        self.asset_grid_params = {key.upper(): dict(value) for key, value in config.asset_grid_params.items()}
        self.trade_qty = Decimal(str(config.trade_qty))
        self.snapshot_interval_ns = int(float(config.snapshot_interval_sec) * 1_000_000_000)
        self.snapshot_display = str(config.snapshot_display).lower()
        self.snapshot_path = Path(config.snapshot_path)
        self.margin_leverage = Decimal(str(config.margin_leverage))
        self.risk_enabled = bool(config.risk_enabled)
        self.risk_max_unrealized_loss_ratio = Decimal(str(config.risk_max_unrealized_loss_ratio))
        self.max_quote_delay_ns = int(float(config.max_quote_delay_ms) * 1_000_000)
        self.quote_summary_interval_ns = int(float(config.quote_summary_interval_sec) * 1_000_000_000)
        self.quote_sample_rates = {key.upper(): float(value) for key, value in config.quote_sample_rates.items()}
        self.fee_bps = {key.upper(): Decimal(str(value)) for key, value in config.fee_bps.items()}
        self.quotes: dict[InstrumentId, QuoteTick] = {}
        self.windows: dict[tuple[str, str, InstrumentId, InstrumentId], SpreadWindow] = {}
        self.stopped = False
        self.stop_requested = False
        self.next_lot_id = 1
        self.positions: dict[int, ArbPos] = {}
        self.pending: dict[int, PendingBatch] = {}
        self.failed_orders: dict[str, PendingBatch] = {}
        self.order_lot: dict[str, int] = {}
        self.signal_alerts: set[str] = set()
        self.signal_alert_versions: dict[str, int] = defaultdict(int)
        self.signal_alert_sides: dict[str, str] = {}
        self.grid_last_open_ns: dict[str, int] = {asset: 0 for asset in self.assets}
        self.action_rows: list[dict[str, str]] = []
        self.realized_pnl_usdt: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        self.realized_edge_bps: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        self.last_snapshot_ns = 0
        self.snapshot_rows: dict[str, object] = {}
        self.last_spread_states: dict[str, dict[tuple[str, InstrumentId, InstrumentId], SpreadState]] = {}
        self.snapshot_lock = Lock()
        self.snapshot_stop = ThreadEvent()
        self.snapshot_thread: Thread | None = None
        self.snapshot_live: Live | None = None
        self.quote_delay_stats: dict[InstrumentId, list[int]] = defaultdict(list)
        self.quote_profile_stats: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
        self.quote_sample_counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.last_quote_summary_ns = 0
        self.housekeeping_alert_seq = 0

    def on_start(self) -> None:
        if self.min_window_ns < 0:
            raise RuntimeError("min_window_sec must be non-negative")
        if self.init_fetch_sec <= 0:
            raise RuntimeError("init_fetch_sec must be positive")
        if self.grid_center_ns <= 0:
            raise RuntimeError("grid_center_sec must be positive")
        missing_assets = sorted(set(self.assets) - set(self.asset_grid_params))
        if missing_assets:
            raise RuntimeError(f"asset_grid_params missing assets: {','.join(missing_assets)}")
        for asset in self.asset_grid_params:
            if asset not in self.assets:
                raise RuntimeError(f"asset_grid_params contains unknown asset: {asset}")
            if self._grid_step_bps(asset) <= 0:
                raise RuntimeError(f"grid_step_bps must be positive for {asset}")
            if self._grid_open_gap_ns(asset) < 0:
                raise RuntimeError(f"grid_open_gap_sec must be non-negative for {asset}")
            for edge_side in (LONG_EDGE, SHORT_EDGE):
                if self._grid_min_band_bps(asset, edge_side) <= 0:
                    raise RuntimeError(f"grid_band_bps.{self._edge_side_key(edge_side)}.min must be positive for {asset}")
                if self._grid_std_mult(asset, edge_side) < 0:
                    raise RuntimeError(f"grid_band_bps.{self._edge_side_key(edge_side)}.std_mult must be non-negative for {asset}")
                if self._grid_signal_delay_ns(asset, edge_side) < 0:
                    raise RuntimeError(f"grid_signal_delay_ms.{self._edge_side_key(edge_side)} must be non-negative for {asset}")
            if self._grid_max_inventory(asset) <= 0:
                raise RuntimeError(f"grid_max_inventory must be positive for {asset}")
            if self._min_capture_bps(asset) < 0:
                raise RuntimeError(f"min_capture_bps must be non-negative for {asset}")
            if self._asset_trade_qty(asset) < 0:
                raise RuntimeError(f"trade_qty must be non-negative for {asset}")
        self._warm_initial_windows()
        if self.trade_qty < 0:
            raise RuntimeError("trade_qty must be non-negative")
        if self.config.snapshot_interval_sec < 0:
            raise RuntimeError("snapshot_interval_sec must be non-negative")
        if self.snapshot_display not in {"rich", "log", "file", "off"}:
            raise RuntimeError("snapshot_display must be rich, log, file, or off")
        if self.margin_leverage <= 0:
            raise RuntimeError("margin_leverage must be positive")
        if self.risk_enabled and not Decimal("0") < self.risk_max_unrealized_loss_ratio <= Decimal("1"):
            raise RuntimeError("risk_max_unrealized_loss_ratio must be in (0, 1]")
        if self.max_quote_delay_ns < 0:
            raise RuntimeError("max_quote_delay_ms must be non-negative")
        if self.quote_summary_interval_ns < 0:
            raise RuntimeError("quote_summary_interval_sec must be non-negative")
        for asset, sample_rate in self.quote_sample_rates.items():
            if asset not in self.assets:
                raise RuntimeError(f"quote_sample_rates contains unknown asset: {asset}")
            if not 0 < sample_rate <= 1:
                raise RuntimeError(f"quote_sample_rates must be in (0, 1] for {asset}")
        self._check_start_account_state()
        if self.stopped:
            return
        for instrument_id in self.instruments:
            self.subscribe_quote_ticks(instrument_id)
        self._start_snapshot_display()
        self._schedule_housekeeping_alert()
        self.log.info(
            f"pre_ipo started assets={','.join(self.assets)} instruments={len(self.instruments)} "
            f"mode=two_line_grid asset_grid_params={self.asset_grid_params} "
            f"warm_windows={len(self.windows)} "
            f"default_qty={self.trade_qty} asset_qty={self._asset_qty_log()} snapshot_display={self.snapshot_display} "
            f"snapshot_path={self.snapshot_path}",
        )

    def on_quote_tick(self, tick: QuoteTick) -> None:
        if self.stopped:
            return
        asset = self._asset(tick.instrument_id)
        if asset is not None and self._drop_quote(asset):
            self._maybe_log_quote_summary()
            return
        total_start = time.perf_counter_ns()
        section_start = total_start
        self._record_quote_delay(tick)
        self._record_quote_profile(tick.instrument_id, "delay_check", time.perf_counter_ns() - section_start)
        section_start = time.perf_counter_ns()
        self.quotes[tick.instrument_id] = tick
        self._record_quote_profile(tick.instrument_id, "cache_quote", time.perf_counter_ns() - section_start)
        if asset is None:
            self._record_quote_profile(tick.instrument_id, "total", time.perf_counter_ns() - total_start)
            self._maybe_log_quote_summary()
            return
        section_start = time.perf_counter_ns()
        states = self._update_spreads(asset, update_window=False)
        self._record_quote_profile(tick.instrument_id, "edge_state", time.perf_counter_ns() - section_start)
        section_start = time.perf_counter_ns()
        self._maybe_open(asset, states)
        self._record_quote_profile(tick.instrument_id, "signal_and_order_check", time.perf_counter_ns() - section_start)
        self._record_quote_profile(tick.instrument_id, "total", time.perf_counter_ns() - total_start)
        self._maybe_log_quote_summary()

    def _drop_quote(self, asset: str) -> bool:
        sample_rate = self.quote_sample_rates.get(asset.upper(), 1.0)
        if sample_rate >= 1.0:
            return False
        counts = self.quote_sample_counts[asset.upper()]
        counts[0] += 1
        if random.random() < sample_rate:
            return False
        counts[1] += 1
        return True

    def _record_quote_delay(self, tick: QuoteTick) -> None:
        if self.max_quote_delay_ns <= 0:
            return
        delay_ns = self.clock.timestamp_ns() - int(tick.ts_init)
        self.quote_delay_stats[tick.instrument_id].append(delay_ns)

    def _record_quote_profile(self, instrument_id: InstrumentId, section: str, elapsed_ns: int) -> None:
        self.quote_profile_stats[str(instrument_id)][section].append(elapsed_ns)

    def _maybe_log_quote_summary(self) -> None:
        if self.quote_summary_interval_ns <= 0:
            return
        now_ns = self.clock.timestamp_ns()
        if now_ns - self.last_quote_summary_ns < self.quote_summary_interval_ns:
            return
        self.last_quote_summary_ns = now_ns
        self._log_quote_delay_summary()
        self.quote_delay_stats.clear()
        self.quote_profile_stats.clear()
        self.quote_sample_counts.clear()

    def _log_quote_delay_summary(self) -> None:
        threshold_ms = self.max_quote_delay_ns / 1_000_000
        for instrument_id, values in sorted(self.quote_delay_stats.items(), key=lambda item: str(item[0])):
            high = [value for value in values if value > self.max_quote_delay_ns]
            if not high:
                continue
            sorted_values = sorted(values)
            self.log.warning(
                f"quote_delay_summary {instrument_id} n={len(values)} high_n={len(high)} "
                f"threshold_ms={threshold_ms:.3f} "
                f"p50_ms={self._quote_percentile_ms(sorted_values, 0.50):.3f} "
                f"p95_ms={self._quote_percentile_ms(sorted_values, 0.95):.3f} "
                f"p99_ms={self._quote_percentile_ms(sorted_values, 0.99):.3f} "
                f"max_ms={max(values) / 1_000_000:.3f}",
            )

    def _log_quote_profile_summary(self) -> None:
        section_labels = {
            "delay_check": "延迟计算/内存聚合",
            "cache_quote": "更新最新盘口缓存",
            "edge_state": "实时刷新edge状态",
            "signal_and_order_check": "信号复查和下单判断",
            "total": "未drop总耗时",
        }
        for instrument_id, sections in sorted(self.quote_profile_stats.items()):
            parts = []
            for section in (
                "total",
                "delay_check",
                "cache_quote",
                "edge_state",
                "signal_and_order_check",
            ):
                values = sections.get(section)
                if not values:
                    continue
                avg_us = sum(values) / len(values) / 1_000
                max_us = max(values) / 1_000
                parts.append(f"{section}({section_labels[section]})=n{len(values)} avg_us={avg_us:.1f} max_us={max_us:.1f}")
            if parts:
                self.log.info(f"quote_profile_summary {instrument_id} {' | '.join(parts)}")

    def _log_quote_sample_summary(self) -> None:
        for asset, counts in sorted(self.quote_sample_counts.items()):
            seen, dropped = counts
            if seen == 0 or dropped == 0:
                continue
            self.log.info(
                f"quote_sample_summary {asset} seen={seen} dropped={dropped} "
                f"processed={seen - dropped} drop_pct={dropped / seen * 100:.1f}",
            )

    def _quote_percentile_ms(self, sorted_values: list[int], pct: float) -> float:
        if not sorted_values:
            return 0.0
        index = min(len(sorted_values) - 1, int((len(sorted_values) - 1) * pct))
        return sorted_values[index] / 1_000_000

    def on_order_filled(self, event: OrderFilled) -> None:
        order_id = str(event.client_order_id)
        failed_batch = self.failed_orders.get(order_id)
        if failed_batch is not None:
            leg = failed_batch.legs.get(order_id)
            if leg is not None:
                fill_qty = Decimal(str(event.last_qty))
                fill_px = self._event_px(event)
                leg.filled_qty += fill_qty
                leg.filled_value += fill_px * fill_qty
                self._update_best_fill_px(leg, fill_px)
                leg.filled_fee += self._event_fee(event)
                self.log.error(
                    f"failed_action_late_fill {failed_batch.asset} action={failed_batch.action} "
                    f"lot={failed_batch.lot_id} order={order_id} last_qty={event.last_qty}",
                )
                self._try_submit_emergency(leg.instrument_id, self._opposite(leg.side), fill_qty)
            return
        lot_id = self.order_lot.get(order_id)
        if lot_id is None:
            return
        batch = self.pending.get(lot_id)
        if batch is None or order_id not in batch.legs:
            return
        leg = batch.legs[order_id]
        fill_qty = Decimal(str(event.last_qty))
        fill_px = self._event_px(event)
        leg.filled_qty += fill_qty
        leg.filled_value += fill_px * fill_qty
        self._update_best_fill_px(leg, fill_px)
        leg.filled_fee += self._event_fee(event)
        fill_state = "filled" if self._leg_filled(leg) else "partial"
        self.log.info(
            f"{fill_state}_fill {batch.asset} action={batch.action} lot={lot_id} order={order_id} "
            f"{leg.instrument_id} filled={leg.filled_qty}/{leg.target_qty}",
        )
        if not self._batch_filled(batch):
            return
        self._confirm_open(batch)

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._handle_order_failed(str(event.client_order_id), f"rejected: {event.reason}")

    def on_order_canceled(self, event: OrderCanceled) -> None:
        self._handle_order_failed(str(event.client_order_id), "canceled")

    def on_order_expired(self, event: OrderExpired) -> None:
        self._handle_order_failed(str(event.client_order_id), "expired")

    def on_stop(self) -> None:
        self._flatten_on_stop()
        self._stop_snapshot_display()
        for instrument_id in self.instruments:
            self.unsubscribe_quote_ticks(instrument_id)

    def _asset(self, instrument_id: InstrumentId) -> str | None:
        return self.instrument_assets.get(instrument_id) or self._parse_asset(instrument_id)

    def _parse_asset(self, instrument_id: InstrumentId) -> str | None:
        symbol = str(instrument_id.symbol).upper().replace("PF_", "").replace("VNTL-", "")
        symbol = symbol.replace("OPENAIX", "OPENAI").replace("ANTHROPICX", "ANTHROPIC")
        for asset in self.assets:
            if symbol.startswith(asset):
                return asset
        return None

    def _venue(self, instrument_id: InstrumentId) -> str:
        return self.instrument_venues.get(instrument_id) or str(instrument_id.venue).upper()

    def _route(self, buy_id: InstrumentId, sell_id: InstrumentId) -> str:
        return f"buy_{self._venue(buy_id).lower()}_sell_{self._venue(sell_id).lower()}"

    def _warm_initial_windows(self) -> None:
        end_ns = time.time_ns()
        start_ns = end_ns - int(self.init_fetch_sec * 1_000_000_000)
        paths = self._collector_quote_files(start_ns, end_ns)
        started = time.perf_counter()
        quotes = self._load_collector_quotes(paths, start_ns, end_ns)
        self.log.info(
            f"initial_bidask_fetch files={len(paths)} rows={len(quotes)} "
            f"elapsed_ms={(time.perf_counter() - started) * 1000:.1f}",
        )
        warmed = self._warm_windows_from_quotes(quotes, end_ns)
        self._seed_initial_quotes(quotes)
        for asset in self.assets:
            if not any(key[0] == asset for key in warmed):
                raise RuntimeError(f"failed to warm initial window for {asset}")
        active_assets: set[str] = set()
        for key in sorted(warmed, key=lambda item: (item[0], item[1], self._route(item[2], item[3]))):
            asset, edge_side, buy_id, sell_id = key
            samples, mean, std, window_sec = self.windows[key].stats()
            if samples < 2 or std <= 0:
                self.log.warning(
                    f"initial_window_skipped {asset} side={edge_side} route={self._route(buy_id, sell_id)} "
                    f"reason=no_variance samples={samples}",
                )
                continue
            active_assets.add(asset)
            self.log.info(
                f"initial_window {asset} side={edge_side} route={self._route(buy_id, sell_id)} "
                f"samples={samples} mean={mean:.2f}bps std={std:.2f}bps window={window_sec:.1f}s",
            )
        for asset in self.assets:
            if asset not in active_assets:
                raise RuntimeError(f"failed to warm usable initial window for {asset}")
        for asset in self.assets:
            self._update_spreads(asset)

    def _collector_quote_files(self, start_ns: int, end_ns: int) -> list[Path]:
        base_dir = Path(__file__).resolve().parent / "collector" / "bidask1-live"
        merged_dir = base_dir / "quote_merged"
        raw_dir = base_dir / "quote_raw"
        current_key = self._collector_hour_key(time.time_ns())
        cutoff_mtime = time.time() - COLLECTOR_RAW_MTIME_SAFETY_SEC
        paths: list[Path] = []
        for key in self._collector_hour_keys(start_ns, end_ns):
            merged = merged_dir / f"bidask1-{key}.parquet"
            if merged.exists():
                paths.append(merged)
            hour_dir = raw_dir / key
            if hour_dir.exists():
                for path in sorted(hour_dir.glob("*.parquet")):
                    if key == current_key and path.stat().st_mtime > cutoff_mtime:
                        continue
                    paths.append(path)
        return sorted(set(paths), key=lambda path: str(path))

    def _collector_hour_keys(self, start_ns: int, end_ns: int) -> list[str]:
        start = datetime.fromtimestamp(start_ns / 1_000_000_000, BEIJING_TZ).replace(minute=0, second=0, microsecond=0)
        end = datetime.fromtimestamp(end_ns / 1_000_000_000, BEIJING_TZ).replace(minute=0, second=0, microsecond=0)
        keys = []
        current = start
        while current <= end:
            keys.append(current.strftime("%Y%m%d%H"))
            current += timedelta(hours=1)
        return keys

    def _collector_hour_key(self, ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, BEIJING_TZ).strftime("%Y%m%d%H")

    def _load_collector_quotes(self, paths: list[Path], start_ns: int, end_ns: int) -> pd.DataFrame:
        if not paths:
            raise RuntimeError("no bidask1 collector parquet files found for initial window")
        dataset = ds.dataset([str(path) for path in paths], format="parquet")
        filt = (pc.field("ts_local_ns") >= pa.scalar(start_ns, pa.int64())) & (
            pc.field("ts_local_ns") <= pa.scalar(end_ns, pa.int64())
        )
        table = dataset.to_table(columns=list(COLLECTOR_COLUMNS), filter=filt)
        quotes = table.to_pandas()
        if quotes.empty:
            raise RuntimeError("no bidask1 collector rows found for initial window")
        return (
            quotes.drop_duplicates(["ts_local_ns", "venue", "symbol", "bid", "ask"])
            .sort_values("ts_local_ns")
            .reset_index(drop=True)
        )

    # 用 collector 真实 bid/ask1 初始化 long/short 两条 edge 线。
    def _warm_windows_from_quotes(self, quotes: pd.DataFrame, end_ns: int) -> set[tuple[str, str, InstrumentId, InstrumentId]]:
        warmed: set[tuple[str, str, InstrumentId, InstrumentId]] = set()
        for asset in self.assets:
            binance_id = self._instrument_id(asset, "BINANCE")
            other_ids = [instrument_id for instrument_id in self.instruments if self._asset(instrument_id) == asset and self._venue(instrument_id) != "BINANCE"]
            if binance_id is None or not other_ids:
                continue
            part = quotes[quotes["symbol"].eq(asset)].copy()
            if part.empty:
                continue
            timeline = pd.DataFrame({"ts_local_ns": np.sort(part["ts_local_ns"].unique())})
            for other_id in other_ids:
                other_venue = self._venue(other_id)
                edge = timeline
                for venue in ("BINANCE", other_venue):
                    venue_quotes = (
                        part[part["venue"].eq(venue)][["ts_local_ns", "bid", "ask"]]
                        .sort_values("ts_local_ns")
                        .rename(columns={"bid": f"{venue.lower()}_bid", "ask": f"{venue.lower()}_ask"})
                    )
                    edge = pd.merge_asof(edge.sort_values("ts_local_ns"), venue_quotes, on="ts_local_ns", direction="backward")
                edge = edge.dropna()
                if edge.empty:
                    continue
                binance_mid = (edge["binance_bid"] + edge["binance_ask"]) / 2.0
                edge["long_edge"] = (edge[f"{other_venue.lower()}_ask"] - edge["binance_bid"]) / binance_mid * 10000.0
                edge["short_edge"] = (edge[f"{other_venue.lower()}_bid"] - edge["binance_ask"]) / binance_mid * 10000.0
                long_key = (asset, LONG_EDGE, other_id, binance_id)
                short_key = (asset, SHORT_EDGE, binance_id, other_id)
                for row in edge.itertuples(index=False):
                    ts_ns = int(row.ts_local_ns)
                    self.windows.setdefault(long_key, SpreadWindow()).add(ts_ns, float(row.long_edge), self.grid_center_ns)
                    self.windows.setdefault(short_key, SpreadWindow()).add(ts_ns, float(row.short_edge), self.grid_center_ns)
                last = edge.iloc[-1]
                if int(last["ts_local_ns"]) < end_ns:
                    self.windows.setdefault(long_key, SpreadWindow()).add(end_ns, float(last["long_edge"]), self.grid_center_ns)
                    self.windows.setdefault(short_key, SpreadWindow()).add(end_ns, float(last["short_edge"]), self.grid_center_ns)
                warmed.add(long_key)
                warmed.add(short_key)
        return warmed

    def _seed_initial_quotes(self, quotes: pd.DataFrame) -> None:
        latest = quotes.sort_values("ts_local_ns").groupby(["symbol", "venue"], sort=False).tail(1)
        ts_init = self.clock.timestamp_ns()
        for row in latest.itertuples(index=False):
            instrument_id = self._instrument_id(str(row.symbol), str(row.venue))
            if instrument_id is None:
                continue
            instrument = self.cache.instrument(instrument_id)
            fallback_qty = self._asset_trade_qty(str(row.symbol))
            if fallback_qty <= 0:
                fallback_qty = Decimal("1")
            bid_size = Decimal(str(row.bid_size))
            ask_size = Decimal(str(row.ask_size))
            bid_size = bid_size if bid_size > 0 else fallback_qty
            ask_size = ask_size if ask_size > 0 else fallback_qty
            ts_event = int(row.ts_exchange_ms) * 1_000_000 if int(row.ts_exchange_ms) > 0 else int(row.ts_local_ns)
            self.quotes[instrument_id] = QuoteTick(
                instrument_id=instrument_id,
                bid_price=instrument.make_price(Decimal(str(row.bid))),
                ask_price=instrument.make_price(Decimal(str(row.ask))),
                bid_size=instrument.make_qty(bid_size),
                ask_size=instrument.make_qty(ask_size),
                ts_event=ts_event,
                ts_init=ts_init,
            )

    def _instrument_id(self, asset: str, venue: str) -> InstrumentId | None:
        asset = asset.upper()
        venue = venue.upper()
        return next(
            (instrument_id for instrument_id in self.instruments if self._asset(instrument_id) == asset and self._venue(instrument_id) == venue),
            None,
        )

    def _binance_quote(self, asset: str) -> tuple[InstrumentId, Decimal, Decimal, Decimal] | None:
        for instrument_id, quote in self.quotes.items():
            if self._asset(instrument_id) != asset or self._venue(instrument_id) != "BINANCE":
                continue
            bid = Decimal(str(quote.bid_price))
            ask = Decimal(str(quote.ask_price))
            if bid <= 0 or ask <= 0:
                return None
            return instrument_id, bid, ask, (bid + ask) / Decimal("2")
        return None

    def _edge_bps(self, numerator: Decimal, price: Decimal) -> Decimal | None:
        if price <= 0:
            return None
        return numerator / price * Decimal("10000")

    # two-line grid 分别维护可成交 long_edge 和 short_edge，避免 mid 信号低估真实价差。
    def _grid_candidates(self, asset: str, update_window: bool) -> list[SpreadState]:
        binance = self._binance_quote(asset)
        if binance is None:
            return []
        binance_id, binance_bid, binance_ask, binance_mid = binance
        now_ns = self.clock.timestamp_ns()
        states: list[SpreadState] = []
        for instrument_id, quote in self.quotes.items():
            if self._asset(instrument_id) != asset or self._venue(instrument_id) == "BINANCE":
                continue
            bid = Decimal(str(quote.bid_price))
            ask = Decimal(str(quote.ask_price))
            if bid <= 0 or ask <= 0:
                continue
            long_edge = self._edge_bps(ask - binance_bid, binance_mid)
            short_edge = self._edge_bps(bid - binance_ask, binance_mid)
            if long_edge is None or short_edge is None:
                continue
            for edge_side, buy, sell, edge in (
                (LONG_EDGE, ArbLeg(instrument_id, ask), ArbLeg(binance_id, binance_bid), long_edge),
                (SHORT_EDGE, ArbLeg(binance_id, binance_ask), ArbLeg(instrument_id, bid), short_edge),
            ):
                key = (asset, edge_side, buy.instrument_id, sell.instrument_id)
                window = self.windows.setdefault(key, SpreadWindow())
                if update_window:
                    window.add(now_ns, float(edge), self.grid_center_ns)
                samples, mean, std, window_sec = window.stats()
                stats = self._active_stats(samples, mean, std, window_sec)
                if stats is None:
                    continue
                mean, std = stats
                z_score = (float(edge) - mean) / std
                states.append(
                    SpreadState(
                        buy=buy,
                        sell=sell,
                        edge_side=edge_side,
                        edge_bps=edge,
                        mean_bps=mean,
                        std_bps=std,
                        z_score=z_score,
                        samples=samples,
                        window_sec=window_sec,
                    ),
                )
        return states

    def _update_spreads(self, asset: str, update_window: bool = True) -> dict[tuple[str, InstrumentId, InstrumentId], SpreadState]:
        states: dict[tuple[str, InstrumentId, InstrumentId], SpreadState] = {}
        for state in self._grid_candidates(asset, update_window):
            states[(state.edge_side, state.buy.instrument_id, state.sell.instrument_id)] = state
        if states:
            self.last_spread_states[asset] = states
        return states

    # 低频维护任务：rolling window、风控和 snapshot 不占用 quote 热路径。
    def _schedule_housekeeping_alert(self) -> None:
        if self.stopped:
            return
        self.housekeeping_alert_seq += 1
        self.clock.set_time_alert_ns(
            f"preipo_housekeeping_{self.housekeeping_alert_seq}",
            self.clock.timestamp_ns() + 1_000_000_000,
            callback=lambda _event: self._on_housekeeping_alert(),
            allow_past=True,
        )

    def _on_housekeeping_alert(self) -> None:
        if self.stopped:
            return
        for asset in self.assets:
            self._update_spreads(asset, update_window=True)
        self._check_risk_limits()
        if not self.stopped:
            self._maybe_update_snapshot()
        self._schedule_housekeeping_alert()

    def _active_stats(self, samples: int, mean: float, std: float, window_sec: float) -> tuple[float, float] | None:
        if samples < 2 or std <= 0:
            return None
        if window_sec * 1_000_000_000 < self.min_window_ns:
            return None
        return mean, std

    def _maybe_open(self, asset: str, states: dict[tuple[str, InstrumentId, InstrumentId], SpreadState]) -> None:
        if self.stopped:
            return
        self._maybe_apply_grid(asset, states, self.clock.timestamp_ns())

    def _maybe_apply_grid(self, asset: str, states: dict[tuple[str, InstrumentId, InstrumentId], SpreadState], now_ns: int) -> None:
        if self._asset_trade_qty(asset) <= 0:
            self._cancel_signal_alert(asset)
            return
        if any(batch.asset == asset for batch in self.pending.values()):
            self._cancel_signal_alert(asset)
            return
        candidates = self._grid_action_candidates(asset, states, now_ns)
        if not candidates:
            self._cancel_signal_alert(asset)
            return
        if asset in self.signal_alerts:
            return
        candidate = max(candidates, key=lambda item: item[0])
        edge_side = candidate[3].edge_side
        if self._candidate_has_open_balance(asset, candidate):
            self._schedule_signal_alert(asset, edge_side, now_ns)

    def _grid_action_candidates(
        self,
        asset: str,
        states: dict[tuple[str, InstrumentId, InstrumentId], SpreadState],
        now_ns: int,
        edge_side: str | None = None,
    ) -> list[tuple[float, int | None, Decimal | None, SpreadState, int]]:
        candidates: list[tuple[float, int | None, Decimal | None, SpreadState, int]] = []
        for state in states.values():
            if edge_side is not None and state.edge_side != edge_side:
                continue
            grid_level = self._grid_signal(asset, state)
            if grid_level is None:
                continue
            close_pos, capture = self._close_candidate(asset, state.edge_side, state.edge_bps)
            if self._can_apply_grid_signal(asset, state, grid_level, now_ns, close_pos, capture):
                candidates.append((abs(float(state.edge_bps) - state.mean_bps), close_pos.lot_id if close_pos else None, capture, state, grid_level))
        return candidates

    def _submit_grid_candidate(
        self,
        asset: str,
        candidate: tuple[float, int | None, Decimal | None, SpreadState, int],
        now_ns: int,
    ) -> None:
        _, close_lot_id, capture, state, grid_level = candidate
        before_inventory = self._asset_inventory(asset, include_pending=True)
        sign = 1 if state.edge_side == LONG_EDGE else -1
        reducing = self._reduces_inventory(before_inventory, state.edge_side)
        inventory_delta = sign * (2 if reducing and abs(before_inventory) == 1 else 1)
        after_inventory = before_inventory + inventory_delta
        self._submit_edge_action(
            asset,
            state.buy,
            state.sell,
            state.edge_bps,
            state.mean_bps,
            state.std_bps,
            state.z_score,
            now_ns,
            grid_level=grid_level,
            edge_side=state.edge_side,
            before_inventory=before_inventory,
            after_inventory=after_inventory,
            inventory_delta=inventory_delta,
            close_lot_id=close_lot_id,
            expected_capture_bps=capture,
        )

    def _candidate_has_open_balance(self, asset: str, candidate: tuple[float, int | None, Decimal | None, SpreadState, int]) -> bool:
        _, close_lot_id, _, state, _ = candidate
        before_inventory = self._asset_inventory(asset, include_pending=True)
        sign = 1 if state.edge_side == LONG_EDGE else -1
        reducing = self._reduces_inventory(before_inventory, state.edge_side)
        inventory_delta = sign * (2 if reducing and abs(before_inventory) == 1 else 1)
        after_inventory = before_inventory + inventory_delta
        qty = self._shared_open_qty(asset, state.buy.instrument_id, state.sell.instrument_id, state.buy.price, state.sell.price)
        if qty is None:
            return False
        balance_qty = qty * Decimal(max(abs(after_inventory) - abs(before_inventory), 0))
        if close_lot_id is not None or balance_qty <= 0:
            return True
        return self._check_open_balances(
            asset,
            state.buy.instrument_id,
            state.buy.price,
            balance_qty,
            state.sell.instrument_id,
            state.sell.price,
            balance_qty,
        )

    def _schedule_signal_alert(self, asset: str, edge_side: str, now_ns: int) -> None:
        if asset in self.signal_alerts:
            return
        self.signal_alert_versions[asset] += 1
        version = self.signal_alert_versions[asset]
        self.signal_alerts.add(asset)
        self.signal_alert_sides[asset] = edge_side
        alert_name = self._signal_alert_name(asset)
        alert_ns = now_ns + self._grid_signal_delay_ns(asset, edge_side)
        self.clock.set_time_alert_ns(
            alert_name,
            alert_ns,
            callback=lambda _event, asset=asset, edge_side=edge_side, version=version: self._on_signal_alert(asset, edge_side, version),
            allow_past=True,
        )

    def _on_signal_alert(self, asset: str, edge_side: str, version: int) -> None:
        if self.signal_alert_versions.get(asset) != version or asset not in self.signal_alerts:
            return
        self.signal_alerts.discard(asset)
        self.signal_alert_sides.pop(asset, None)
        if self.stopped:
            return
        if self._asset_trade_qty(asset) <= 0:
            return
        if any(batch.asset == asset for batch in self.pending.values()):
            return
        # Quote 回调已经刷新最新 edge；alert 只读取最新状态，不再写窗口。
        states = self.last_spread_states.get(asset, {})
        now_ns = self.clock.timestamp_ns()
        candidates = self._grid_action_candidates(asset, states, now_ns, edge_side=edge_side)
        if not candidates:
            return
        self._submit_grid_candidate(asset, max(candidates, key=lambda item: item[0]), now_ns)

    def _cancel_signal_alert(self, asset: str) -> None:
        if asset not in self.signal_alerts:
            return
        self.signal_alerts.discard(asset)
        self.signal_alert_sides.pop(asset, None)
        self.signal_alert_versions[asset] += 1
        self.clock.cancel_timer(self._signal_alert_name(asset))

    def _signal_alert_name(self, asset: str) -> str:
        return f"preipo_grid_signal_{asset}"

    def _grid_signal(self, asset: str, state: SpreadState) -> int | None:
        band = self._grid_entry_band_bps(asset, state)
        step = float(self._grid_step_bps(asset))
        if step <= 0:
            return None
        deviation = float(state.edge_bps) - state.mean_bps
        if state.edge_side == SHORT_EDGE and deviation >= band:
            return int(floor((deviation - band) / step))
        if state.edge_side == LONG_EDGE and deviation <= -band:
            return int(floor((abs(deviation) - band) / step))
        return None

    def _can_apply_grid_signal(
        self,
        asset: str,
        state: SpreadState,
        grid_level: int,
        now_ns: int,
        close_pos: ArbPos | None,
        capture: Decimal | None,
    ) -> bool:
        if grid_level < 0:
            return False
        inventory = self._asset_inventory(asset, include_pending=True)
        reducing = self._reduces_inventory(inventory, state.edge_side)
        if reducing and (close_pos is None or capture is None or capture < self._min_capture_bps(asset)):
            return False
        max_inventory = self._grid_max_inventory(asset)
        if not reducing and state.edge_side == LONG_EDGE and inventory >= max_inventory:
            return False
        if not reducing and state.edge_side == SHORT_EDGE and inventory <= -max_inventory:
            return False
        open_gap_ns = self._grid_open_gap_ns(asset)
        if not reducing and open_gap_ns > 0 and now_ns - self.grid_last_open_ns.get(asset, 0) < open_gap_ns:
            return False
        if not reducing and not self._passes_grid_step(asset, state.edge_side, state.edge_bps):
            return False
        return True

    def _asset_grid(self, asset: str) -> dict[str, object]:
        if asset is None:
            raise RuntimeError("asset is required for grid params")
        return self.asset_grid_params[asset.upper()]

    def _grid_decimal(self, asset: str, key: str) -> Decimal:
        params = self._asset_grid(asset)
        return Decimal(str(params[key]))

    def _grid_float(self, asset: str, key: str) -> float:
        params = self._asset_grid(asset)
        return float(params[key])

    def _grid_int(self, asset: str, key: str) -> int:
        params = self._asset_grid(asset)
        return int(params[key])

    def _grid_step_bps(self, asset: str) -> Decimal:
        return self._grid_decimal(asset, "grid_step_bps")

    def _grid_open_gap_ns(self, asset: str) -> int:
        return int(self._grid_float(asset, "grid_open_gap_sec") * 1_000_000_000)

    def _grid_entry_band_bps(self, asset: str, state: SpreadState) -> float:
        min_band = self._grid_min_band_bps(asset, state.edge_side)
        std_mult = self._grid_std_mult(asset, state.edge_side)
        return max(min_band, std_mult * state.std_bps)

    def _grid_min_band_bps(self, asset: str, edge_side: str) -> float:
        return float(self._grid_side_value(asset, "grid_band_bps", edge_side, "min"))

    def _grid_std_mult(self, asset: str, edge_side: str) -> float:
        return float(self._grid_side_value(asset, "grid_band_bps", edge_side, "std_mult"))

    def _grid_signal_delay_ns(self, asset: str, edge_side: str) -> int:
        return int(self._grid_side_float(asset, "grid_signal_delay_ms", edge_side) * 1_000_000)

    def _grid_max_inventory(self, asset: str) -> int:
        return self._grid_int(asset, "grid_max_inventory")

    def _min_capture_bps(self, asset: str) -> Decimal:
        return self._grid_decimal(asset, "min_capture_bps")

    def _grid_side_decimal(self, asset: str, key: str, edge_side: str) -> Decimal:
        return Decimal(str(self._grid_side_value(asset, key, edge_side)))

    def _grid_side_float(self, asset: str, key: str, edge_side: str) -> float:
        return float(self._grid_side_value(asset, key, edge_side))

    def _grid_side_value(self, asset: str, key: str, edge_side: str, subkey: str | None = None) -> object:
        params = self._asset_grid(asset)
        value = params[key][self._edge_side_key(edge_side)]
        if subkey is not None:
            return value[subkey]
        return value

    def _edge_side_key(self, edge_side: str) -> str:
        if edge_side == LONG_EDGE:
            return "long"
        if edge_side == SHORT_EDGE:
            return "short"
        raise RuntimeError(f"unknown edge_side: {edge_side}")

    def _asset_trade_qty(self, asset: str) -> Decimal:
        return Decimal(str(self._asset_grid(asset).get("trade_qty", self.trade_qty)))

    def _asset_qty_log(self) -> dict[str, str]:
        return {asset: str(self._asset_trade_qty(asset)) for asset in self.assets}

    def _asset_inventory(self, asset: str, include_pending: bool = False) -> int:
        inventory = sum(
            1 if pos.edge_side == LONG_EDGE else -1
            for pos in self.positions.values()
            if pos.asset == asset
        )
        if include_pending:
            inventory += sum(
                batch.inventory_delta
                for batch in self.pending.values()
                if batch.asset == asset
            )
        return inventory

    # 加仓网格以已有同方向开仓 edge 为锚，mean 只负责确认仍处于波动区。
    def _passes_grid_step(self, asset: str, edge_side: str, edge_bps: Decimal) -> bool:
        edges = [
            pos.actual_entry_edge_bps or pos.edge_bps
            for pos in self.positions.values()
            if pos.asset == asset and pos.edge_side == edge_side
        ]
        edges.extend(
            batch.edge_bps
            for batch in self.pending.values()
            if batch.asset == asset and batch.edge_side == edge_side and batch.open_qty > 0
        )
        if not edges:
            return True
        step = self._grid_step_bps(asset)
        if edge_side == SHORT_EDGE:
            return edge_bps >= max(edges) + step
        return edge_bps <= min(edges) - step

    def _reduces_inventory(self, inventory: int, edge_side: str) -> bool:
        return (inventory > 0 and edge_side == SHORT_EDGE) or (inventory < 0 and edge_side == LONG_EDGE)

    def _close_candidate(
        self,
        asset: str,
        edge_side: str,
        exit_edge: Decimal,
        require_min_capture: bool = True,
    ) -> tuple[ArbPos | None, Decimal | None]:
        opposite = SHORT_EDGE if edge_side == LONG_EDGE else LONG_EDGE
        best_pos: ArbPos | None = None
        best_capture: Decimal | None = None
        for pos in self.positions.values():
            if pos.asset != asset or pos.edge_side != opposite:
                continue
            entry_edge = pos.actual_entry_edge_bps or pos.edge_bps
            capture = self._capture_bps(pos.edge_side, entry_edge, exit_edge)
            if require_min_capture and capture < self._min_capture_bps(asset):
                continue
            if best_capture is None or capture > best_capture:
                best_pos = pos
                best_capture = capture
        return best_pos, best_capture

    def _submit_edge_action(
        self,
        asset: str,
        buy: ArbLeg,
        sell: ArbLeg,
        edge_bps: Decimal,
        mean_bps: float,
        std_bps: float,
        z_score: float,
        now_ns: int,
        grid_level: int | None = None,
        edge_side: str = SHORT_EDGE,
        before_inventory: int = 0,
        after_inventory: int = 0,
        inventory_delta: int = 0,
        close_lot_id: int | None = None,
        expected_capture_bps: Decimal | None = None,
    ) -> None:
        qty = self._shared_open_qty(asset, buy.instrument_id, sell.instrument_id, buy.price, sell.price)
        if qty is None:
            return
        close_qty = qty if close_lot_id is not None else Decimal("0")
        open_qty = qty if close_lot_id is None or abs(inventory_delta) == 2 else Decimal("0")
        balance_qty = qty * Decimal(max(abs(after_inventory) - abs(before_inventory), 0))
        order_qty = close_qty + open_qty
        if order_qty <= 0:
            return
        action = FLIP if close_lot_id is not None and open_qty > 0 else CLOSE if close_lot_id is not None else OPEN
        lot_id = self._new_lot_id()
        self.log.info(
            f"edge_signal {asset} action={action} lot={lot_id} close_lot={close_lot_id or '-'} "
            f"route={self._route(buy.instrument_id, sell.instrument_id)} "
            f"edge={edge_bps:.2f}bps side={edge_side} mean={mean_bps:.2f}bps std={std_bps:.2f}bps z={z_score:.2f} "
            f"qty={order_qty} open_qty={open_qty} close_qty={close_qty} "
            f"expected_capture={self._fmt(expected_capture_bps) if expected_capture_bps is not None else '-'}bps "
            f"buy={buy.instrument_id}@{buy.price} sell={sell.instrument_id}@{sell.price}",
        )
        batch = self._submit_batch(
            asset=asset,
            action=action,
            lot_id=lot_id,
            buy_id=buy.instrument_id,
            sell_id=sell.instrument_id,
            buy_px=buy.price,
            sell_px=sell.price,
            buy_side=OrderSide.BUY,
            sell_side=OrderSide.SELL,
            edge_bps=edge_bps,
            mean_bps=mean_bps,
            std_bps=std_bps,
            z_score=z_score,
            now_ns=now_ns,
            buy_qty=order_qty,
            sell_qty=order_qty,
            grid_level=grid_level,
            edge_side=edge_side,
            before_inventory=before_inventory,
            after_inventory=after_inventory,
            inventory_delta=inventory_delta,
            open_qty=open_qty,
            close_qty=close_qty,
            close_lot_id=close_lot_id,
            balance_qty=balance_qty,
            expected_capture_bps=expected_capture_bps,
        )
        if batch is not None and batch.lot_id in self.pending and open_qty > 0:
            self.grid_last_open_ns[asset] = now_ns

    def _submit_batch(
        self,
        asset: str,
        action: str,
        lot_id: int,
        buy_id: InstrumentId,
        sell_id: InstrumentId,
        buy_px: Decimal,
        sell_px: Decimal,
        buy_side: OrderSide,
        sell_side: OrderSide,
        edge_bps: Decimal,
        mean_bps: float,
        std_bps: float,
        z_score: float,
        now_ns: int,
        buy_qty: Decimal | None = None,
        sell_qty: Decimal | None = None,
        grid_level: int | None = None,
        edge_side: str = SHORT_EDGE,
        before_inventory: int = 0,
        after_inventory: int = 0,
        inventory_delta: int = 0,
        open_qty: Decimal = Decimal("0"),
        close_qty: Decimal = Decimal("0"),
        close_lot_id: int | None = None,
        balance_qty: Decimal = Decimal("0"),
        expected_capture_bps: Decimal | None = None,
    ) -> PendingBatch | None:
        buy_order, buy_target = self._make_order(buy_id, buy_side, buy_qty)
        sell_order, sell_target = self._make_order(sell_id, sell_side, sell_qty)
        if balance_qty > 0 and not self._check_open_balances(
            asset,
            buy_id,
            buy_px,
            balance_qty,
            sell_id,
            sell_px,
            balance_qty,
        ):
            self.log.warning(f"open_skipped {asset} lot={lot_id} reason=insufficient_usdt")
            return None
        buy_order_id = str(buy_order.client_order_id)
        sell_order_id = str(sell_order.client_order_id)
        self.order_lot[buy_order_id] = lot_id
        self.order_lot[sell_order_id] = lot_id
        batch = PendingBatch(
            asset=asset,
            action=action,
            lot_id=lot_id,
            buy_id=buy_id,
            sell_id=sell_id,
            buy_px=buy_px,
            sell_px=sell_px,
            edge_bps=edge_bps,
            mean_bps=mean_bps,
            std_bps=std_bps,
            z_score=z_score,
            created_ns=now_ns,
            legs={
                buy_order_id: PendingLeg(
                    instrument_id=buy_id,
                    side=buy_side,
                    order=buy_order,
                    target_qty=buy_target,
                    filled_qty=Decimal("0"),
                    filled_value=Decimal("0"),
                    filled_fee=Decimal("0"),
                ),
                sell_order_id: PendingLeg(
                    instrument_id=sell_id,
                    side=sell_side,
                    order=sell_order,
                    target_qty=sell_target,
                    filled_qty=Decimal("0"),
                    filled_value=Decimal("0"),
                    filled_fee=Decimal("0"),
                ),
            },
            edge_side=edge_side,
            before_inventory=before_inventory,
            after_inventory=after_inventory,
            grid_level=grid_level,
            close_lot_id=close_lot_id,
            expected_capture_bps=expected_capture_bps,
            inventory_delta=inventory_delta,
            open_qty=open_qty,
            close_qty=close_qty,
        )
        self.pending[lot_id] = batch
        self.log.info(
            f"submit_action_batch {asset} action={action} lot={lot_id} buy_order={buy_order_id} sell_order={sell_order_id} "
            f"side={edge_side} inventory={before_inventory}->{after_inventory} "
            f"buy={buy_id} {buy_side} qty={buy_target} sell={sell_id} {sell_side} qty={sell_target}",
        )
        try:
            self.submit_order(buy_order)
            self.submit_order(sell_order)
        except Exception as exc:
            self.log.error(f"submit_batch_failed {asset} action={action} lot={lot_id} error={exc}")
            self._fail_batch(batch, f"submit exception: {exc}")
        return batch

    # 启动时只确认没有遗留持仓并且账户数据已加载；余额只在开仓前检查。
    def _check_start_account_state(self) -> None:
        open_positions = []
        for instrument_id in self.instruments:
            try:
                open_positions.extend(self.cache.positions_open(instrument_id=instrument_id))
            except TypeError:
                open_positions.extend(
                    position
                    for position in self.cache.positions_open()
                    if position.instrument_id == instrument_id
                )
        if open_positions:
            details = ", ".join(str(position) for position in open_positions)
            self.log.error(f"start_check_failed open_positions={details}")
            self._request_stop("preipo 启动检查发现已有持仓")
            return
        accounts = list(self.cache.accounts())
        if not accounts:
            self.log.error("start_check_failed accounts=0")
            self._request_stop("preipo 启动检查没有账户数据")
            return

    # 开仓检查按配置杠杆估算初始保证金，再加 10% 缓冲覆盖手续费和盘口滑动。
    def _required_margin(self, price: Decimal, qty: Decimal) -> Decimal:
        return price * qty / self.margin_leverage * Decimal("1.1")

    # 开仓前按两条腿所在账户合并检查可用 USDT。
    def _check_open_balances(
        self,
        asset: str,
        buy_id: InstrumentId,
        buy_px: Decimal,
        buy_qty: Decimal,
        sell_id: InstrumentId,
        sell_px: Decimal,
        sell_qty: Decimal,
    ) -> bool:
        required: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        accounts = {}
        for instrument_id, price, qty in ((buy_id, buy_px, buy_qty), (sell_id, sell_px, sell_qty)):
            venue = self._venue(instrument_id)
            account = self._account_for_venue(venue)
            if account is None:
                self.log.error(f"open_check_failed {asset} venue={venue} account=missing")
                return False
            account_id = str(account.id)
            accounts[account_id] = account
            required[account_id] += self._required_margin(price, qty)
        ok = True
        for account_id, need in required.items():
            free = self._free_usdt(accounts[account_id])
            if free < need:
                self.log.error(f"open_check_failed {asset} account={account_id} free_usdt={free} required={need}")
                ok = False
            else:
                self.log.info(f"open_check_ok {asset} account={account_id} free_usdt={free} required={need}")
        return ok

    # 每次状态变化后记录是否还有足够 USDT 支持下一次开仓。
    def _log_next_notional(self, asset: str) -> None:
        if self._asset_trade_qty(asset) <= 0:
            self.log.info(f"next_open_balance_skip {asset} reason=trade_qty_zero")
            return
        requirements = self._next_open_requirements(asset)
        if not requirements:
            self.log.warning(f"next_open_balance_blocked {asset} reason=missing_quotes")
            return
        for account_id, required in requirements.items():
            account = self._account_by_id(account_id)
            if account is None:
                self.log.error(f"next_open_balance_blocked {asset} account={account_id} missing")
                continue
            free = self._free_usdt(account)
            if free < required:
                self.log.warning(f"next_open_balance_blocked {asset} account={account.id} free_usdt={free} required={required}")
            else:
                self.log.info(f"next_open_balance_ok {asset} account={account.id} free_usdt={free} required={required}")

    def _next_open_requirements(self, asset: str) -> dict[str, Decimal]:
        required: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        trade_qty = self._asset_trade_qty(asset)
        if trade_qty <= 0:
            return {}
        for instrument_id in self.instruments:
            if self._asset(instrument_id) != asset:
                continue
            quote = self.quotes.get(instrument_id)
            if quote is None:
                return {}
            price = max(Decimal(str(quote.bid_price)), Decimal(str(quote.ask_price)))
            qty = self._round_qty(instrument_id, trade_qty)
            account = self._account_for_venue(self._venue(instrument_id))
            if account is None:
                return {}
            required[str(account.id)] += self._required_margin(price, qty)
        return dict(required)

    # 按 Instrument venue 匹配 NT 账户。
    def _account_for_venue(self, venue: str):
        venue_text = venue.upper()
        for account in self.cache.accounts():
            if str(account.id).upper().startswith(venue_text):
                return account
        return None

    def _account_by_id(self, account_id: str):
        for account in self.cache.accounts():
            if str(account.id) == account_id:
                return account
        return None

    def _account_venue(self, account: object, venues: set[str]) -> str | None:
        account_id = str(account.id).upper()
        for venue in sorted(venues, key=len, reverse=True):
            if account_id.startswith(venue.upper()):
                return venue
        return None

    # 从账户对象读取 USDT 可用余额，兼容 cash/margin 账户暴露方式差异。
    def _free_usdt(self, account) -> Decimal:
        money = None
        if hasattr(account, "balance_free"):
            try:
                money = account.balance_free(USDT)
            except Exception:
                money = None
        if money is None and hasattr(account, "balances_free"):
            try:
                balances = account.balances_free()
            except Exception:
                balances = {}
            money = balances.get(USDT)
            if money is None:
                for currency, value in balances.items():
                    if str(currency) == "USDT":
                        money = value
                        break
        if money is None:
            return Decimal("0")
        if hasattr(money, "as_decimal"):
            return Decimal(str(money.as_decimal()))
        return Decimal(str(money).replace("_", "").split()[0])

    # 从账户对象读取 USDT 钱包余额，风险阈值按 total/wallet 口径计算。
    def _total_usdt(self, account) -> Decimal:
        money = None
        if hasattr(account, "balance_total"):
            try:
                money = account.balance_total(USDT)
            except Exception:
                money = None
        if money is None and hasattr(account, "balances_total"):
            try:
                balances = account.balances_total()
            except Exception:
                balances = {}
            money = balances.get(USDT)
            if money is None:
                for currency, value in balances.items():
                    if str(currency) == "USDT":
                        money = value
                        break
        if money is None:
            return Decimal("0")
        if hasattr(money, "as_decimal"):
            return Decimal(str(money.as_decimal()))
        return Decimal(str(money).replace("_", "").split()[0])

    def _strategy_open_positions(self) -> list[object]:
        result = []
        seen = set()
        for instrument_id in self.instruments:
            try:
                positions = self.cache.positions_open(instrument_id=instrument_id)
            except TypeError:
                positions = [position for position in self.cache.positions_open() if position.instrument_id == instrument_id]
            for position in positions:
                key = str(getattr(position, "id", position))
                if key in seen:
                    continue
                seen.add(key)
                result.append(position)
        return result

    def _position_qty(self, position: object) -> Decimal:
        return abs(Decimal(str(position.quantity)))

    def _position_avg_px(self, position: object) -> Decimal:
        return Decimal(str(position.avg_px_open))

    # 风险表按当前可平仓价估算，不用 mid，避免低估真实退出成本。
    def _position_exit_px(self, position: object) -> Decimal | None:
        quote = self.quotes.get(position.instrument_id)
        if quote is None:
            return None
        bid = Decimal(str(quote.bid_price))
        ask = Decimal(str(quote.ask_price))
        if bid <= 0 or ask <= 0:
            return None
        if bool(position.is_long):
            return bid
        return ask

    def _position_unrealized_pnl(self, position: object) -> Decimal | None:
        exit_px = self._position_exit_px(position)
        if exit_px is None:
            return None
        qty = self._position_qty(position)
        avg_px = self._position_avg_px(position)
        fee = exit_px * qty * self.fee_bps[self._venue(position.instrument_id)] / Decimal("10000")
        if bool(position.is_long):
            return (exit_px - avg_px) * qty - fee
        return (avg_px - exit_px) * qty - fee

    def _risk_rows(self) -> dict[str, dict[str, str]]:
        data = self._risk_state()
        rows = {}
        for venue, item in data.items():
            rows[venue] = {
                "wallet_usdt": self._fmt(item["wallet_usdt"]),
                "unrealized_usdt": self._fmt(item["unrealized_usdt"]),
                "risk_rate": self._fmt(item["risk_rate"] * Decimal("100"), "%") if item["wallet_usdt"] > 0 else "-",
                "positions": str(item["positions"]),
                "status": "HIGH" if item["high_risk"] else "OK",
            }
        return rows

    def _risk_state(self) -> dict[str, dict[str, object]]:
        rows: dict[str, dict[str, object]] = {}
        venues = sorted({self._venue(instrument_id) for instrument_id in self.instruments})
        for venue in venues:
            rows[venue] = {
                "wallet_usdt": Decimal("0"),
                "unrealized_usdt": Decimal("0"),
                "risk_rate": Decimal("0"),
                "positions": 0,
                "high_risk": False,
                "missing_quotes": 0,
            }
        for account in self.cache.accounts():
            venue = self._account_venue(account, set(rows))
            if venue not in rows:
                continue
            rows[venue] = {
                "wallet_usdt": self._total_usdt(account),
                "unrealized_usdt": Decimal("0"),
                "risk_rate": Decimal("0"),
                "positions": 0,
                "high_risk": False,
                "missing_quotes": 0,
            }
        for position in self._strategy_open_positions():
            venue = self._venue(position.instrument_id)
            account = self._account_for_venue(venue)
            if account is None:
                continue
            row = rows.setdefault(
                venue,
                {
                    "wallet_usdt": self._total_usdt(account),
                    "unrealized_usdt": Decimal("0"),
                    "risk_rate": Decimal("0"),
                    "positions": 0,
                    "high_risk": False,
                    "missing_quotes": 0,
                },
            )
            row["positions"] = int(row["positions"]) + 1
            pnl = self._position_unrealized_pnl(position)
            if pnl is None:
                row["missing_quotes"] = int(row["missing_quotes"]) + 1
                continue
            row["unrealized_usdt"] = Decimal(str(row["unrealized_usdt"])) + pnl
        for row in rows.values():
            wallet = Decimal(str(row["wallet_usdt"]))
            unrealized = Decimal(str(row["unrealized_usdt"]))
            if wallet > 0 and unrealized < 0:
                row["risk_rate"] = abs(unrealized) / wallet
                row["high_risk"] = row["risk_rate"] >= self.risk_max_unrealized_loss_ratio
        return rows

    def _check_risk_limits(self) -> None:
        if not self.risk_enabled:
            return
        for venue, row in self._risk_state().items():
            if int(row["positions"]) <= 0:
                continue
            wallet = Decimal(str(row["wallet_usdt"]))
            unrealized = Decimal(str(row["unrealized_usdt"]))
            risk_rate = Decimal(str(row["risk_rate"]))
            if wallet <= 0:
                self.log.error(f"risk_check_failed venue={venue} wallet_usdt={wallet}")
                self._flatten_cache_positions(f"risk wallet missing {venue}")
                self._request_stop(f"preipo 风控 {venue} 钱包余额异常")
                return
            if bool(row["high_risk"]):
                self.log.error(
                    f"risk_limit_triggered venue={venue} wallet_usdt={wallet} unrealized_usdt={unrealized} "
                    f"risk_rate={risk_rate * Decimal('100'):.2f}% threshold={self.risk_max_unrealized_loss_ratio * Decimal('100'):.2f}%",
                )
                self._flatten_cache_positions(f"risk limit {venue}")
                self._request_stop(f"preipo 风控 {venue} 未实现亏损超过阈值")
                return

    # 策略停止时按内部持仓记录提交反向市价单。
    def _flatten_on_stop(self) -> None:
        if not self.positions:
            return
        for lot_id, pos in list(self.positions.items()):
            self.log.warning(f"flatten_action_inventory {pos.asset} lot={lot_id} reason=strategy_stop")
            self._try_submit_emergency(pos.buy_id, OrderSide.SELL, pos.buy_qty)
            self._try_submit_emergency(pos.sell_id, OrderSide.BUY, pos.sell_qty)

    # 风控触发时以交易系统真实持仓为准，避免内部 pair 状态不完整。
    def _flatten_cache_positions(self, reason: str) -> None:
        for position in self._strategy_open_positions():
            side = OrderSide.SELL if bool(position.is_long) else OrderSide.BUY
            qty = self._position_qty(position)
            self.log.warning(f"flatten_cache_position reason={reason} instrument={position.instrument_id} side={side} qty={qty}")
            self._try_submit_emergency(position.instrument_id, side, qty)

    def _make_order(self, instrument_id: InstrumentId, side: OrderSide, qty: Decimal) -> tuple[MarketOrder, Decimal]:
        instrument = self.cache.instrument(instrument_id)
        quantity = instrument.make_qty(qty)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )
        return order, Decimal(str(quantity))

    def _round_qty(self, instrument_id: InstrumentId, qty: Decimal) -> Decimal:
        instrument = self.cache.instrument(instrument_id)
        return Decimal(str(instrument.make_qty(qty)))

    def _shared_open_qty(self, asset: str, buy_id: InstrumentId, sell_id: InstrumentId, buy_px: Decimal, sell_px: Decimal) -> Decimal | None:
        trade_qty = self._asset_trade_qty(asset)
        if trade_qty <= 0:
            self.log.info(f"skip_asset_trade_disabled {asset} buy={buy_id} sell={sell_id} trade_qty={trade_qty}")
            return None
        buy_qty = self._round_qty(buy_id, trade_qty)
        sell_qty = self._round_qty(sell_id, trade_qty)
        if buy_qty != sell_qty:
            self.log.warning(f"skip_open_qty_mismatch {asset} buy={buy_id} qty={buy_qty} sell={sell_id} qty={sell_qty} trade_qty={trade_qty}")
            return None
        if buy_qty <= 0:
            self.log.warning(f"skip_open_qty_too_small {asset} buy={buy_id} sell={sell_id} trade_qty={trade_qty}")
            return None
        return buy_qty

    def _leg_filled(self, leg: PendingLeg) -> bool:
        return leg.filled_qty >= leg.target_qty

    # 只有两条腿都累计全量成交后，才确认一次 edge 动作。
    def _batch_filled(self, batch: PendingBatch) -> bool:
        return all(self._leg_filled(leg) for leg in batch.legs.values())

    def _confirm_open(self, batch: PendingBatch) -> None:
        buy_qty = self._filled_qty(batch, batch.buy_id)
        sell_qty = self._filled_qty(batch, batch.sell_id)
        buy_avg = self._filled_avg_px(batch, batch.buy_id)
        sell_avg = self._filled_avg_px(batch, batch.sell_id)
        self._apply_grid_action(batch, buy_qty, sell_qty, buy_avg, sell_avg)

    def _apply_grid_action(
        self,
        batch: PendingBatch,
        buy_qty: Decimal,
        sell_qty: Decimal,
        buy_avg: Decimal | None,
        sell_avg: Decimal | None,
    ) -> None:
        actual_edge = self._actual_entry_edge_bps(batch)
        current_inventory = self._asset_inventory(batch.asset)
        reducing = self._reduces_inventory(current_inventory, batch.edge_side)
        reduced_pos = None
        close_fee = self._filled_fee_share(batch, batch.close_qty)
        open_fee = self._filled_fee_share(batch, batch.open_qty)
        if reducing:
            reduced_pos = self._remove_closed_position(batch)
            if reduced_pos is not None:
                self._record_realized(
                    batch.asset,
                    reduced_pos,
                    actual_edge if actual_edge is not None else batch.edge_bps,
                    self._round_trip_pnl_usdt(reduced_pos, batch, close_fee),
                )
        if batch.open_qty > 0 and (not reducing or reduced_pos is not None):
            self.positions[batch.lot_id] = ArbPos(
                lot_id=batch.lot_id,
                asset=batch.asset,
                buy_id=batch.buy_id,
                sell_id=batch.sell_id,
                buy_px=batch.buy_px,
                sell_px=batch.sell_px,
                entry_buy_avg_px=buy_avg,
                entry_sell_avg_px=sell_avg,
                entry_fee=open_fee,
                buy_qty=batch.open_qty,
                sell_qty=batch.open_qty,
                edge_bps=batch.edge_bps,
                actual_entry_edge_bps=actual_edge,
                mean_bps=batch.mean_bps,
                std_bps=batch.std_bps,
                z_score=batch.z_score,
                opened_ns=batch.created_ns,
                edge_side=batch.edge_side,
                grid_level=batch.grid_level,
            )
        after_inventory = self._asset_inventory(batch.asset)
        self._clear_pending(batch)
        if buy_qty != sell_qty:
            self.log.warning(f"filled_qty_mismatch {batch.asset} lot={batch.lot_id} buy_qty={buy_qty} sell_qty={sell_qty}")
        self.action_rows.append(
            self._filled_action_row(
                batch=batch,
                asset=batch.asset,
                action=batch.action,
                edge_side=batch.edge_side,
                route=self._route(batch.buy_id, batch.sell_id),
                qty=buy_qty,
                signal_edge=batch.edge_bps,
                actual_edge=actual_edge,
                mean_bps=batch.mean_bps,
                std_bps=batch.std_bps,
                grid_level=batch.grid_level,
                before_inventory=current_inventory,
                after_inventory=after_inventory,
                created_ns=batch.created_ns,
                close_lot_id=reduced_pos.lot_id if reduced_pos is not None else batch.close_lot_id,
                expected_capture_bps=batch.expected_capture_bps,
                realized_capture_bps=(
                    self._capture_bps(
                        reduced_pos.edge_side,
                        reduced_pos.actual_entry_edge_bps or reduced_pos.edge_bps,
                        actual_edge if actual_edge is not None else batch.edge_bps,
                    )
                    if reduced_pos is not None
                    else None
                ),
            ),
        )
        actual_text = self._fmt(actual_edge) if actual_edge is not None else "-"
        self.log.info(
            f"edge_action_filled {batch.asset} action={batch.action} lot={batch.lot_id} side={batch.edge_side} "
            f"inventory={current_inventory}->{after_inventory} level={batch.grid_level} qty={buy_qty} "
            f"signal_edge={batch.edge_bps:.2f}bps actual_edge={actual_text}bps",
        )
        self._log_next_notional(batch.asset)

    def _remove_closed_position(self, batch: PendingBatch) -> ArbPos | None:
        if batch.close_lot_id is not None and batch.close_lot_id in self.positions:
            return self.positions.pop(batch.close_lot_id)
        actual_edge = self._actual_entry_edge_bps(batch)
        exit_edge = actual_edge if actual_edge is not None else batch.edge_bps
        pos, _ = self._close_candidate(batch.asset, batch.edge_side, exit_edge, require_min_capture=False)
        if pos is None:
            self.log.error(f"close_position_missing {batch.asset} action_lot={batch.lot_id} close_lot={batch.close_lot_id}")
            return None
        return self.positions.pop(pos.lot_id)

    def _record_realized(self, asset: str, pos: ArbPos, exit_edge: Decimal, pnl_usdt: Decimal | None) -> None:
        entry_edge = pos.actual_entry_edge_bps or pos.edge_bps
        self.realized_edge_bps[asset] += self._capture_bps(pos.edge_side, entry_edge, exit_edge)
        if pnl_usdt is not None:
            self.realized_pnl_usdt[asset] += pnl_usdt

    def _capture_bps(self, edge_side: str, entry_edge: Decimal, exit_edge: Decimal) -> Decimal:
        if edge_side == LONG_EDGE:
            return exit_edge - entry_edge
        return entry_edge - exit_edge

    # 滑点沿用旧符号：正数表示变好，负数表示变差。
    def _slippage_bps(self, edge_side: str, from_edge: Decimal, to_edge: Decimal | None) -> Decimal | None:
        if to_edge is None:
            return None
        if edge_side == LONG_EDGE:
            return from_edge - to_edge
        return to_edge - from_edge

    def _filled_qty(self, batch: PendingBatch, instrument_id: InstrumentId) -> Decimal:
        total = Decimal("0")
        for leg in batch.legs.values():
            if leg.instrument_id == instrument_id:
                total += leg.filled_qty
        return total

    def _filled_avg_px(self, batch: PendingBatch, instrument_id: InstrumentId) -> Decimal | None:
        qty = Decimal("0")
        value = Decimal("0")
        for leg in batch.legs.values():
            if leg.instrument_id == instrument_id:
                qty += leg.filled_qty
                value += leg.filled_value
        if qty <= 0:
            return None
        return value / qty

    def _filled_best_avg_px(self, batch: PendingBatch, instrument_id: InstrumentId) -> Decimal | None:
        for leg in batch.legs.values():
            if leg.instrument_id == instrument_id and leg.best_px is not None:
                return leg.best_px
        return None

    def _update_best_fill_px(self, leg: PendingLeg, px: Decimal) -> None:
        if leg.best_px is None:
            leg.best_px = px
            return
        if leg.side == OrderSide.BUY:
            leg.best_px = min(leg.best_px, px)
        else:
            leg.best_px = max(leg.best_px, px)

    def _filled_fee(self, batch: PendingBatch) -> Decimal:
        return sum((leg.filled_fee for leg in batch.legs.values()), Decimal("0"))

    def _filled_fee_share(self, batch: PendingBatch, qty: Decimal) -> Decimal:
        if qty <= 0:
            return Decimal("0")
        target_qty = next((leg.target_qty for leg in batch.legs.values() if leg.target_qty > 0), Decimal("0"))
        if target_qty <= 0:
            return Decimal("0")
        return self._filled_fee(batch) * qty / target_qty

    # 实际入场 edge 用两腿成交均价计算，分母使用 Binance 腿成交价。
    def _actual_entry_edge_bps(self, batch: PendingBatch) -> Decimal | None:
        buy_avg = self._filled_avg_px(batch, batch.buy_id)
        sell_avg = self._filled_avg_px(batch, batch.sell_id)
        if buy_avg is None and sell_avg is None:
            buy_avg = batch.buy_px
            sell_avg = batch.sell_px
        if buy_avg is None or sell_avg is None:
            return None
        if batch.edge_side == SHORT_EDGE:
            denom = buy_avg if self._venue(batch.buy_id) == "BINANCE" else sell_avg
            return self._edge_bps(sell_avg - buy_avg, denom)
        denom = sell_avg if self._venue(batch.sell_id) == "BINANCE" else buy_avg
        return self._edge_bps(buy_avg - sell_avg, denom)

    # 用实际撮合里的最优价格计算理论最优成交 edge；成交均价相对它的偏离才是成交滑点。
    def _best_entry_edge_bps(self, batch: PendingBatch) -> Decimal | None:
        buy_best = self._filled_best_avg_px(batch, batch.buy_id)
        sell_best = self._filled_best_avg_px(batch, batch.sell_id)
        if buy_best is None or sell_best is None:
            return None
        if batch.edge_side == SHORT_EDGE:
            denom = buy_best if self._venue(batch.buy_id) == "BINANCE" else sell_best
            return self._edge_bps(sell_best - buy_best, denom)
        denom = sell_best if self._venue(batch.sell_id) == "BINANCE" else buy_best
        return self._edge_bps(buy_best - sell_best, denom)

    def _round_trip_pnl_usdt(self, pos: ArbPos, batch: PendingBatch, exit_fee: Decimal) -> Decimal | None:
        exit_sell_avg = self._filled_avg_px(batch, pos.buy_id)
        exit_buy_avg = self._filled_avg_px(batch, pos.sell_id)
        if pos.entry_buy_avg_px is None or pos.entry_sell_avg_px is None:
            return None
        if exit_sell_avg is None or exit_buy_avg is None:
            return None
        long_pnl = (exit_sell_avg - pos.entry_buy_avg_px) * pos.buy_qty
        short_pnl = (pos.entry_sell_avg_px - exit_buy_avg) * pos.sell_qty
        return long_pnl + short_pnl - pos.entry_fee - exit_fee

    def _event_px(self, event: OrderFilled) -> Decimal:
        text = str(event.last_px).split()[0].replace("_", "")
        return Decimal(text)

    def _event_fee(self, event: OrderFilled) -> Decimal:
        commission = getattr(event, "commission", None)
        if commission is None:
            return Decimal("0")
        text = str(commission).split()[0].replace("_", "")
        return Decimal(text)

    def _handle_order_failed(self, order_id: str, reason: str) -> None:
        lot_id = self.order_lot.get(order_id)
        if lot_id is None:
            return
        batch = self.pending.get(lot_id)
        if batch is None:
            self.order_lot.pop(order_id, None)
            return
        self.log.error(f"order_failed {batch.asset} action={batch.action} lot={lot_id} order={order_id} reason={reason}")
        self._fail_batch(batch, reason)

    # 任一腿失败只隔离当前动作；其它库存不受影响。
    def _fail_batch(self, batch: PendingBatch, reason: str) -> None:
        for order_id in batch.legs:
            self.failed_orders[order_id] = batch
        self._emergency_flatten(batch)
        self._clear_pending(batch)
        self.log.error(f"action_failed_removed {batch.asset} action={batch.action} lot={batch.lot_id} reason={reason}")

    # 请求 live 入口停止整个 node，保证 finally 仍能写报告。
    def _request_stop(self, reason: str) -> None:
        self.stopped = True
        if self.stop_requested:
            return
        self.stop_requested = True
        self.log.error(f"strategy_stop reason={reason}")
        self.msgbus.publish(NODE_STOP_TOPIC, {"source": "pre_ipo", "reason": reason})

    def _emergency_flatten(self, batch: PendingBatch) -> None:
        for leg in batch.legs.values():
            if leg.filled_qty > 0:
                self._try_submit_emergency(leg.instrument_id, self._opposite(leg.side), leg.filled_qty)

    # 应急平仓失败只能记录，不能让策略回到可开仓状态。
    def _try_submit_emergency(self, instrument_id: InstrumentId, side: OrderSide, qty: Decimal) -> None:
        try:
            self._submit_emergency(instrument_id, side, qty)
        except Exception as exc:
            self.log.error(f"emergency_flatten_failed {instrument_id} side={side} qty={qty} error={exc}")

    def _submit_emergency(self, instrument_id: InstrumentId, side: OrderSide, qty: Decimal) -> None:
        instrument = self.cache.instrument(instrument_id)
        quantity: Quantity = instrument.make_qty(qty)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
        self.log.warning(f"emergency_flatten {instrument_id} side={side} qty={quantity}")

    def _opposite(self, side: OrderSide) -> OrderSide:
        return OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY

    def _clear_pending(self, batch: PendingBatch) -> None:
        for order_id in batch.legs:
            self.order_lot.pop(order_id, None)
        self.pending.pop(batch.lot_id, None)

    def _new_lot_id(self) -> int:
        lot_id = self.next_lot_id
        self.next_lot_id += 1
        return lot_id

    def _fmt(self, value: object, suffix: str = "") -> str:
        try:
            text = f"{Decimal(str(value)):.2f}"
        except Exception:
            return str(value)
        return f"{text}{suffix}"

    def _beijing_time(self, ts_ns: int | None = None) -> str:
        if ts_ns is None:
            ts_ns = self.clock.timestamp_ns()
        ts_sec = ts_ns / 1_000_000_000
        return datetime.fromtimestamp(ts_sec, tz=timezone.utc).astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

    def _beijing_time_short(self, ts_ns: int) -> str:
        ts_sec = ts_ns / 1_000_000_000
        return datetime.fromtimestamp(ts_sec, tz=timezone.utc).astimezone(BEIJING_TZ).strftime("%m-%d %H:%M:%S")

    def _maybe_update_snapshot(self) -> None:
        if self.snapshot_interval_ns <= 0 or self.snapshot_display == "off":
            return
        now_ns = self.clock.timestamp_ns()
        if now_ns - self.last_snapshot_ns < self.snapshot_interval_ns:
            return
        self.last_snapshot_ns = now_ns
        rows: dict[str, dict[str, str]] = {}
        market_tables: dict[str, list[dict[str, str]]] = {}
        log_parts = []
        for asset in self.assets:
            asset_states = self.last_spread_states.get(asset, {})
            asset_state = self._asset_state(asset)
            pending_count = sum(1 for batch in self.pending.values() if batch.asset == asset)
            inventory = self._asset_inventory(asset)
            active_count = abs(inventory)
            state = max(asset_states.values(), key=lambda item: abs(float(item.edge_bps) - item.mean_bps), default=None)
            if state is None:
                quotes = self._quote_text(asset) or "waiting"
                rows[asset] = {
                    "asset": asset,
                    "state": asset_state,
                    "active": "Y" if active_count else "N",
                    "inventory": str(inventory),
                    "pending": str(pending_count),
                    "buy": "-",
                    "sell": "-",
                    "edge_side": "-",
                    "edge": "-",
                    "z": "-",
                    "mean": "-",
                    "std": "-",
                    "samples": "0",
                    "window": "0s",
                    "quotes": quotes,
                }
                market_tables[asset] = self._market_rows(asset, asset_states)
                log_parts.append(f"{asset} state={asset_state} inventory={inventory} pending={pending_count} quotes=[{quotes}]")
                continue
            buy, sell = state.buy, state.sell
            quotes = self._quote_text(asset)
            rows[asset] = {
                "asset": asset,
                "state": asset_state,
                "active": "Y" if active_count else "N",
                "inventory": str(inventory),
                "pending": str(pending_count),
                "buy": f"{buy.instrument_id}@{self._fmt(buy.price)}",
                "sell": f"{sell.instrument_id}@{self._fmt(sell.price)}",
                "edge_side": state.edge_side,
                "edge": self._fmt(state.edge_bps),
                "z": self._fmt(state.z_score),
                "mean": self._fmt(state.mean_bps),
                "std": self._fmt(state.std_bps),
                "samples": str(state.samples),
                "window": self._fmt(state.window_sec, "s"),
                "quotes": quotes,
            }
            market_tables[asset] = self._market_rows(asset, asset_states)
            log_parts.append(
                f"{asset} state={asset_state} inventory={inventory} pending={pending_count} "
                f"edge={state.edge_bps:.2f}bps z={state.z_score:.2f} buy={buy.instrument_id}@{buy.price} "
                f"sell={sell.instrument_id}@{sell.price} quotes=[{quotes}]",
            )
        action_rows = self._action_history_rows(now_ns)
        if self.snapshot_display == "log":
            self.log.info("market_snapshot | " + " | ".join(log_parts))
            return
        with self.snapshot_lock:
            self.snapshot_rows.update(rows)
            self.snapshot_rows["__market_tables__"] = market_tables
            self.snapshot_rows["__action_rows__"] = action_rows
            self.snapshot_rows["__risk_rows__"] = self._risk_rows()
            self.snapshot_rows["__beijing_time__"] = self._beijing_time(now_ns)

    def _market_rows(self, asset: str, states: dict[tuple[str, InstrumentId, InstrumentId], SpreadState]) -> list[dict[str, str]]:
        now_ns = self.clock.timestamp_ns()
        instrument_ids = [instrument_id for instrument_id in self.instruments if self._asset(instrument_id) == asset]
        instrument_ids.sort(key=lambda item: (0 if self._venue(item) == "BINANCE" else 1, self._venue(item)))
        venues = [self._venue(instrument_id) for instrument_id in instrument_ids]
        metrics = (
            "bid",
            "ask",
            "age",
            "spread_bps",
            "long_edge",
            "long_mean",
            "long_std",
            "short_edge",
            "short_mean",
            "short_std",
        )
        rows = [{"metric": metric, **{venue: "-" for venue in venues}} for metric in metrics]
        by_metric = {row["metric"]: row for row in rows}
        binance_id = next((instrument_id for instrument_id in instrument_ids if self._venue(instrument_id) == "BINANCE"), None)
        binance_quote = self.quotes.get(binance_id) if binance_id is not None else None
        quotes: dict[InstrumentId, tuple[Decimal, Decimal]] = {}
        for instrument_id in instrument_ids:
            venue = self._venue(instrument_id)
            quote = self.quotes.get(instrument_id)
            if quote is None:
                continue
            bid = Decimal(str(quote.bid_price))
            ask = Decimal(str(quote.ask_price))
            if bid <= 0 or ask <= 0:
                continue
            quotes[instrument_id] = (bid, ask)
            age_sec = (now_ns - quote.ts_event) / 1_000_000_000
            by_metric["bid"][venue] = self._fmt(bid)
            by_metric["ask"][venue] = self._fmt(ask)
            by_metric["age"][venue] = self._fmt(age_sec, "s")
        if binance_quote is None:
            return rows
        binance_prices = quotes.get(binance_id)
        if binance_prices is None:
            return rows
        binance_bid, binance_ask = binance_prices
        binance_mid = (binance_bid + binance_ask) / Decimal("2")
        for instrument_id in instrument_ids:
            venue = self._venue(instrument_id)
            prices = quotes.get(instrument_id)
            if prices is None:
                continue
            bid, ask = prices
            mid = (bid + ask) / Decimal("2")
            spread = self._edge_bps(ask - bid, mid)
            by_metric["spread_bps"][venue] = self._fmt(spread) if spread is not None else "-"
            if venue == "BINANCE":
                continue
            long_edge = self._edge_bps(ask - binance_bid, binance_mid)
            short_edge = self._edge_bps(bid - binance_ask, binance_mid)
            by_metric["long_edge"][venue] = self._fmt(long_edge) if long_edge is not None else "-"
            by_metric["short_edge"][venue] = self._fmt(short_edge) if short_edge is not None else "-"
            long_state = states.get((LONG_EDGE, instrument_id, binance_id))
            if long_state is not None:
                by_metric["long_mean"][venue] = self._fmt(long_state.mean_bps)
                by_metric["long_std"][venue] = self._fmt(long_state.std_bps)
            short_state = states.get((SHORT_EDGE, binance_id, instrument_id))
            if short_state is not None:
                by_metric["short_mean"][venue] = self._fmt(short_state.mean_bps)
                by_metric["short_std"][venue] = self._fmt(short_state.std_bps)
        return rows

    def _action_history_rows(self, now_ns: int) -> list[dict[str, str]]:
        rows = []
        for row in self.action_rows:
            item = dict(row)
            try:
                created_ns = int(item.get("created_ns", "0"))
                item["age_min"] = self._fmt(max((now_ns - created_ns) / 60_000_000_000, 0.0))
            except Exception:
                pass
            rows.append(item)
        for batch in self.pending.values():
            rows.append(self._pending_action_row(batch))
        return sorted(rows, key=lambda row: row.get("created_ns", ""), reverse=True)

    def _filled_action_row(
        self,
        batch: PendingBatch | None,
        asset: str,
        action: str,
        edge_side: str,
        route: str,
        qty: Decimal,
        signal_edge: Decimal,
        actual_edge: Decimal | None,
        mean_bps: float,
        std_bps: float,
        grid_level: int | None,
        before_inventory: int,
        after_inventory: int,
        created_ns: int,
        close_lot_id: int | None = None,
        expected_capture_bps: Decimal | None = None,
        realized_capture_bps: Decimal | None = None,
    ) -> dict[str, str]:
        best_edge = self._best_entry_edge_bps(batch) if batch is not None else None
        return {
            "created_ns": str(created_ns),
            "lot": str(batch.lot_id) if batch is not None else "-",
            "asset": asset,
            "action": action,
            "edge_side": edge_side,
            "route": route,
            "status": "filled",
            "qty": self._fmt(qty),
            "signal_edge": self._fmt(signal_edge),
            "best_edge": self._fmt(best_edge) if best_edge is not None else "-",
            "actual_edge": self._fmt(actual_edge) if actual_edge is not None else "-",
            "edge_slippage": self._fmt(self._slippage_bps(edge_side, signal_edge, best_edge)) if best_edge is not None else "-",
            "fill_slippage": self._fmt(self._slippage_bps(edge_side, best_edge, actual_edge)) if best_edge is not None and actual_edge is not None else "-",
            "mean": self._fmt(mean_bps),
            "std": self._fmt(std_bps),
            "level": str(grid_level) if grid_level is not None else "-",
            "expected_capture": self._fmt(expected_capture_bps) if expected_capture_bps is not None else "-",
            "realized_capture": self._fmt(realized_capture_bps) if realized_capture_bps is not None else "-",
            "inventory": f"{before_inventory}->{after_inventory}",
            "time": self._beijing_time_short(created_ns),
            "age_min": self._fmt(max((self.clock.timestamp_ns() - created_ns) / 60_000_000_000, 0.0)),
        }

    def _pending_action_row(self, batch: PendingBatch) -> dict[str, str]:
        return {
            "created_ns": str(batch.created_ns),
            "lot": str(batch.lot_id),
            "asset": batch.asset,
            "action": batch.action,
            "edge_side": batch.edge_side,
            "route": self._route(batch.buy_id, batch.sell_id),
            "status": "pending",
            "qty": self._fmt(self._batch_target_qty(batch)),
            "signal_edge": self._fmt(batch.edge_bps),
            "best_edge": "-",
            "actual_edge": "-",
            "edge_slippage": "-",
            "fill_slippage": "-",
            "mean": self._fmt(batch.mean_bps),
            "std": self._fmt(batch.std_bps),
            "level": str(batch.grid_level) if batch.grid_level is not None else "-",
            "expected_capture": self._fmt(batch.expected_capture_bps) if batch.expected_capture_bps is not None else "-",
            "realized_capture": "-",
            "inventory": f"{batch.before_inventory}->{batch.after_inventory}",
            "time": self._beijing_time_short(batch.created_ns),
            "age_min": self._fmt(max((self.clock.timestamp_ns() - batch.created_ns) / 60_000_000_000, 0.0)),
        }

    def _batch_target_qty(self, batch: PendingBatch) -> Decimal:
        return next((leg.target_qty for leg in batch.legs.values()), batch.open_qty + batch.close_qty)

    def _summary_rows(self) -> dict[str, dict[str, str]]:
        rows = {}
        for asset in self.assets:
            unrealized = self._unrealized_edge_bps(asset)
            rows[asset] = {
                "inventory": str(self._asset_inventory(asset)),
                "realized_usdt": self._fmt(self.realized_pnl_usdt[asset]),
                "unrealized_usdt": self._fmt(self._unrealized_pnl_usdt(asset)),
                "realized_bps": self._fmt(self.realized_edge_bps[asset]),
                "unrealized_bps": self._fmt(unrealized),
                "total_bps": self._fmt(self.realized_edge_bps[asset] + unrealized),
            }
        return rows

    def _unrealized_pnl_usdt(self, asset: str) -> Decimal:
        total = Decimal("0")
        for position in self._strategy_open_positions():
            if self._asset(position.instrument_id) != asset:
                continue
            pnl = self._position_unrealized_pnl(position)
            if pnl is not None:
                total += pnl
        return total

    def _unrealized_edge_bps(self, asset: str) -> Decimal:
        total = Decimal("0")
        for pos in self.positions.values():
            if pos.asset != asset:
                continue
            current_edge = self._current_edge_for_pos(pos)
            if current_edge is None:
                continue
            entry_edge = pos.actual_entry_edge_bps or pos.edge_bps
            total += self._capture_bps(pos.edge_side, entry_edge, current_edge)
        return total

    def _current_edge_for_pos(self, pos: ArbPos) -> Decimal | None:
        states = self.last_spread_states.get(pos.asset, {})
        exit_side = SHORT_EDGE if pos.edge_side == LONG_EDGE else LONG_EDGE
        ids = {pos.buy_id, pos.sell_id}
        for state in states.values():
            if state.edge_side == exit_side and {state.buy.instrument_id, state.sell.instrument_id} == ids:
                return state.edge_bps
        return None

    def _quote_text(self, asset: str) -> str:
        now_ns = self.clock.timestamp_ns()
        rows = []
        for instrument_id, quote in self.quotes.items():
            if self._asset(instrument_id) != asset:
                continue
            age_sec = (now_ns - quote.ts_event) / 1_000_000_000
            rows.append(f"{self._venue(instrument_id)} bid={self._fmt(quote.bid_price)} ask={self._fmt(quote.ask_price)} age={self._fmt(age_sec, 's')}")
        return "; ".join(rows)

    def _asset_state(self, asset: str) -> str:
        for batch in self.pending.values():
            if batch.asset == asset:
                return OPENING
        inventory = self._asset_inventory(asset)
        if inventory > 0:
            return LONG_EDGE
        if inventory < 0:
            return SHORT_EDGE
        return FLAT

    def _start_snapshot_display(self) -> None:
        if self.snapshot_display == "file" and self.snapshot_interval_ns > 0:
            self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            self.snapshot_stop.clear()
            self.snapshot_thread = Thread(target=self._snapshot_file_loop, name="preipo-arb-snapshot-file", daemon=True)
            self.snapshot_thread.start()
            return
        if self.snapshot_display != "rich" or self.snapshot_interval_ns <= 0:
            return
        if not sys.stdout.isatty():
            self.log.warning("snapshot_display=rich requires a TTY; disabling terminal snapshot")
            self.snapshot_display = "off"
            return
        self.log.info("snapshot_display=rich using alternate screen; stop node to restore terminal")
        self.snapshot_stop.clear()
        self.snapshot_thread = Thread(target=self._snapshot_loop, name="preipo-arb-snapshot", daemon=True)
        self.snapshot_thread.start()

    def _stop_snapshot_display(self) -> None:
        self.snapshot_stop.set()
        if self.snapshot_thread is not None:
            self.snapshot_thread.join(timeout=2)
            self.snapshot_thread = None
        if self.snapshot_live is not None:
            self.snapshot_live.stop()
            self.snapshot_live = None

    def _snapshot_loop(self) -> None:
        console = Console()
        self.snapshot_live = Live(
            self._snapshot_table({}),
            console=console,
            refresh_per_second=max(1, min(4, int(1_000_000_000 / max(self.snapshot_interval_ns, 1)))),
            screen=True,
            redirect_stdout=True,
            redirect_stderr=True,
            transient=False,
        )
        self.snapshot_live.start()
        try:
            while not self.snapshot_stop.wait(self.snapshot_interval_ns / 1_000_000_000):
                with self.snapshot_lock:
                    rows = dict(self.snapshot_rows)
                self.snapshot_live.update(self._snapshot_table(rows), refresh=True)
        finally:
            if self.snapshot_live is not None:
                self.snapshot_live.update(self._snapshot_table(dict(self.snapshot_rows)), refresh=True)

    def _snapshot_file_loop(self) -> None:
        while not self.snapshot_stop.wait(self.snapshot_interval_ns / 1_000_000_000):
            with self.snapshot_lock:
                rows = dict(self.snapshot_rows)
            self._write_snapshot(rows)
        with self.snapshot_lock:
            rows = dict(self.snapshot_rows)
        self._write_snapshot(rows)

    def _write_snapshot(self, rows: dict[str, object]) -> None:
        market_tables = rows.get("__market_tables__", {})
        action_rows = rows.get("__action_rows__", [])
        asset_rows = {key: value for key, value in rows.items() if not key.startswith("__")}
        payload = {
            "strategy": "pre_ipo",
            "assets": self.assets,
            "rows": [asset_rows[asset] for asset in self.assets if asset in asset_rows],
            "market_tables": market_tables,
            "action_rows": action_rows,
            "summary": self._summary_rows(),
            "risk": rows.get("__risk_rows__", self._risk_rows()),
            "inventories": {asset: self._asset_inventory(asset) for asset in self.assets},
        }
        tmp = self.snapshot_path.with_suffix(f"{self.snapshot_path.suffix}.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.snapshot_path)

    def _snapshot_table(self, rows: dict[str, object]) -> Table:
        title = f"PREIPO Arbitrage Actions | 北京时间 {rows.get('__beijing_time__') or self._beijing_time()}"
        table = Table(title=title, expand=True)
        columns = (
            ("lot", "lot", "right"),
            ("asset", "asset", "left"),
            ("status", "status", "center"),
            ("qty", "qty", "right"),
            ("signal_edge", "signal_edge", "right"),
            ("edge_slippage", "edge_slip", "right"),
            ("fill_slippage", "fill_slip", "right"),
            ("mean", "mean", "right"),
            ("std", "std", "right"),
            ("inventory", "inventory", "right"),
            ("time", "time", "left"),
            ("age_min", "age_min", "right"),
        )
        for key, label, justify in columns:
            table.add_column(label, justify=justify, no_wrap=key != "time")
        action_rows = rows.get("__action_rows__") or []
        if not action_rows:
            table.add_row(*("-" for _ in columns))
            return table
        for row in action_rows:
            table.add_row(*(self._snapshot_action_value(row, key) for key, _, _ in columns))
        return table

    def _snapshot_action_value(self, row: dict[str, str], key: str) -> str:
        if key == "action":
            side = str(row.get("edge_side", ""))
            if side == LONG_EDGE:
                return "long"
            if side == SHORT_EDGE:
                return "short"
        return str(row.get(key, "-"))

