from __future__ import annotations

import sys
import json
import time
import requests
from threading import Lock
from threading import Thread
from threading import Event as ThreadEvent
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from math import sqrt
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.table import Table

from nautilus_trader.config import StrategyConfig
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


FLAT = "FLAT"
OPENING = "OPENING"
PAIRED = "PAIRED"
CLOSING = "CLOSING"
OPEN = "OPEN"
CLOSE = "CLOSE"


@dataclass
class ArbLeg:
    instrument_id: InstrumentId
    price: Decimal


@dataclass
class ArbPos:
    buy_id: InstrumentId
    sell_id: InstrumentId
    buy_px: Decimal
    sell_px: Decimal
    buy_qty: Decimal
    sell_qty: Decimal
    edge_bps: Decimal
    mean_bps: float
    z_score: float
    opened_ns: int


@dataclass
class SpreadState:
    buy: ArbLeg
    sell: ArbLeg
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


@dataclass
class PendingBatch:
    asset: str
    action: str
    buy_id: InstrumentId
    sell_id: InstrumentId
    buy_px: Decimal
    sell_px: Decimal
    edge_bps: Decimal
    mean_bps: float
    z_score: float
    created_ns: int
    legs: dict[str, PendingLeg]


class SpreadWindow:
    def __init__(self) -> None:
        self.points: deque[tuple[int, float]] = deque()
        self.total = 0.0
        self.total_sq = 0.0

    def add(self, ts_ns: int, value: float, lookback_ns: int) -> None:
        self.points.append((ts_ns, value))
        self.total += value
        self.total_sq += value * value
        cutoff = ts_ns - lookback_ns
        while self.points and self.points[0][0] < cutoff:
            _, old = self.points.popleft()
            self.total -= old
            self.total_sq -= old * old

    def stats(self) -> tuple[int, float, float, float]:
        count = len(self.points)
        if count == 0:
            return 0, 0.0, 0.0, 0.0
        mean = self.total / count
        variance = max(self.total_sq / count - mean * mean, 0.0)
        window_sec = (self.points[-1][0] - self.points[0][0]) / 1_000_000_000
        return count, mean, sqrt(variance), window_sec


class PreipoArbConfig(StrategyConfig, frozen=True):
    instruments: list[str]
    assets: list[str]
    spread_mode: str
    one_sided: bool
    entry_bps: Decimal
    exit_bps: Decimal
    lookback_sec: float
    min_window_sec: float
    init_fetch_sec: float
    init_fetch_timeout_sec: float
    init_blend_sec: float
    initial_mean_bps: dict[str, float]
    initial_std_bps: dict[str, float]
    min_spread_samples: int
    entry_z: Decimal
    exit_z: Decimal
    min_entry_bps: Decimal
    trade_notional: Decimal
    max_hold_sec: float
    min_hold_sec: float
    cooldown_sec: float
    max_quote_age_sec: float
    snapshot_interval_sec: float
    snapshot_display: str
    snapshot_path: str
    slippage_bps: Decimal
    fee_bps: dict[str, float]
    dry_run: bool


class PreipoArbStrategy(Strategy):
    def __init__(self, config: PreipoArbConfig) -> None:
        super().__init__(config)
        self.instruments = [InstrumentId.from_str(value) for value in config.instruments]
        self.assets = [asset.upper() for asset in config.assets]
        self.instrument_assets = {
            instrument_id: self._parse_asset(instrument_id)
            for instrument_id in self.instruments
        }
        self.instrument_venues = {
            instrument_id: str(instrument_id.venue).upper()
            for instrument_id in self.instruments
        }
        self.spread_mode = config.spread_mode
        self.one_sided = bool(config.one_sided)
        self.entry_bps = Decimal(str(config.entry_bps))
        self.exit_bps = Decimal(str(config.exit_bps))
        self.lookback_ns = int(float(config.lookback_sec) * 1_000_000_000)
        self.min_window_ns = int(float(config.min_window_sec) * 1_000_000_000)
        self.init_fetch_sec = float(config.init_fetch_sec)
        self.init_fetch_timeout_sec = float(config.init_fetch_timeout_sec)
        self.init_blend_ns = int(float(config.init_blend_sec) * 1_000_000_000)
        self.initial_mean_bps = {key.upper(): float(value) for key, value in config.initial_mean_bps.items()}
        self.initial_std_bps = {key.upper(): float(value) for key, value in config.initial_std_bps.items()}
        self.min_spread_samples = int(config.min_spread_samples)
        self.entry_z = float(config.entry_z)
        self.exit_z = float(config.exit_z)
        self.min_entry_bps = Decimal(str(config.min_entry_bps))
        self.notional = Decimal(str(config.trade_notional))
        self.max_hold_ns = int(float(config.max_hold_sec) * 1_000_000_000)
        self.min_hold_ns = int(float(config.min_hold_sec) * 1_000_000_000)
        self.cooldown_ns = int(float(config.cooldown_sec) * 1_000_000_000)
        self.max_quote_age_ns = int(float(config.max_quote_age_sec) * 1_000_000_000)
        self.snapshot_interval_ns = int(float(config.snapshot_interval_sec) * 1_000_000_000)
        self.snapshot_display = str(config.snapshot_display).lower()
        self.snapshot_path = Path(config.snapshot_path)
        self.slippage_bps = Decimal(str(config.slippage_bps))
        self.fee_bps = {key.upper(): Decimal(str(value)) for key, value in config.fee_bps.items()}
        self.quotes: dict[InstrumentId, QuoteTick] = {}
        self.windows: dict[tuple[str, InstrumentId, InstrumentId], SpreadWindow] = {}
        self.arb_state = FLAT
        self.active_asset: str | None = None
        self.positions: dict[str, ArbPos] = {}
        self.pending: dict[str, PendingBatch] = {}
        self.order_asset: dict[str, str] = {}
        self.last_close_ns: dict[str, int] = {}
        self.last_snapshot_ns = 0
        self.snapshot_rows: dict[str, dict[str, str]] = {}
        self.snapshot_lock = Lock()
        self.snapshot_stop = ThreadEvent()
        self.snapshot_thread: Thread | None = None
        self.snapshot_live: Live | None = None

    def on_start(self) -> None:
        if self.spread_mode not in {"threshold", "mean_deviation"}:
            raise RuntimeError("spread_mode must be threshold or mean_deviation")
        if self.spread_mode == "threshold" and self.entry_bps <= self.exit_bps:
            raise RuntimeError("entry_bps must be greater than exit_bps")
        if self.spread_mode == "mean_deviation":
            if self.lookback_ns <= 0:
                raise RuntimeError("lookback_sec must be positive")
            if self.min_window_ns < 0:
                raise RuntimeError("min_window_sec must be non-negative")
            if self.init_fetch_sec <= 0:
                raise RuntimeError("init_fetch_sec must be positive")
            if self.init_fetch_timeout_sec <= 0:
                raise RuntimeError("init_fetch_timeout_sec must be positive")
            if self.init_blend_ns <= 0:
                raise RuntimeError("init_blend_sec must be positive")
            if self.min_spread_samples <= 1:
                raise RuntimeError("min_spread_samples must be greater than 1")
            if self.entry_z <= self.exit_z:
                raise RuntimeError("entry_z must be greater than exit_z")
            self._refresh_initial_stats()
            for asset in self.assets:
                if asset not in self.initial_mean_bps:
                    raise RuntimeError(f"initial_mean_bps missing asset: {asset}")
                if self.initial_std_bps.get(asset, 0) <= 0:
                    raise RuntimeError(f"initial_std_bps must be positive for asset: {asset}")
        if self.notional <= 0:
            raise RuntimeError("trade_notional must be positive")
        if self.config.min_hold_sec < 0:
            raise RuntimeError("min_hold_sec must be positive")
        if self.config.cooldown_sec < 0:
            raise RuntimeError("cooldown_sec must be positive")
        if self.config.max_quote_age_sec <= 0:
            raise RuntimeError("max_quote_age_sec must be positive")
        if self.config.snapshot_interval_sec < 0:
            raise RuntimeError("snapshot_interval_sec must be non-negative")
        if self.snapshot_display not in {"rich", "log", "file", "off"}:
            raise RuntimeError("snapshot_display must be rich, log, file, or off")
        for instrument_id in self.instruments:
            self.subscribe_quote_ticks(instrument_id)
        self._start_snapshot_display()
        self.log.info(
            f"preipo_arb started assets={','.join(self.assets)} instruments={len(self.instruments)} "
            f"mode={self.spread_mode} one_sided={self.one_sided} "
            f"lookback={self.config.lookback_sec}s min_window={self.config.min_window_sec}s "
            f"init_fetch={self.config.init_fetch_sec}s init_blend={self.config.init_blend_sec}s "
            f"entry_z={self.entry_z} exit_z={self.exit_z} initial_mean={self.initial_mean_bps} "
            f"initial_std={self.initial_std_bps} notional={self.notional} "
            f"snapshot_interval={self.config.snapshot_interval_sec}s snapshot_display={self.snapshot_display} "
            f"snapshot_path={self.snapshot_path} "
            f"dry_run={self.config.dry_run}",
        )

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.quotes[tick.instrument_id] = tick
        asset = self._asset(tick.instrument_id)
        if asset is None:
            return
        states = self._update_spreads(asset)
        self._maybe_update_snapshot(states)
        if self.active_asset is not None and asset != self.active_asset:
            return
        if self.arb_state in {OPENING, CLOSING}:
            return
        if self.arb_state == PAIRED:
            self._maybe_close(asset, states)
        else:
            self._maybe_open(asset, states)

    def on_order_filled(self, event: OrderFilled) -> None:
        order_id = str(event.client_order_id)
        asset = self.order_asset.get(order_id)
        if asset is None:
            return
        batch = self.pending.get(asset)
        if batch is None or order_id not in batch.legs:
            return
        leg = batch.legs[order_id]
        leg.filled_qty += Decimal(str(event.last_qty))
        self.log.info(
            f"fill {asset} action={batch.action} order={order_id} "
            f"{leg.instrument_id} filled={leg.filled_qty}/{leg.target_qty}",
        )
        if not self._batch_done(batch):
            return
        if batch.action == OPEN:
            self._confirm_open(batch)
        else:
            self._confirm_close(batch)

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._handle_order_failed(str(event.client_order_id), f"rejected: {event.reason}")

    def on_order_canceled(self, event: OrderCanceled) -> None:
        self._handle_order_failed(str(event.client_order_id), "canceled")

    def on_order_expired(self, event: OrderExpired) -> None:
        self._handle_order_failed(str(event.client_order_id), "expired")

    def on_stop(self) -> None:
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

    def _fee(self, instrument_id: InstrumentId) -> Decimal:
        return self.fee_bps[self._venue(instrument_id)]

    def _refresh_initial_stats(self) -> None:
        start_ms = int((time.time() - self.init_fetch_sec) * 1000)
        end_ms = int(time.time() * 1000)
        deadline = time.monotonic() + self.init_fetch_timeout_sec
        rows = self._fetch_recent_trades(start_ms, end_ms, deadline)
        stats = self._initial_stats_from_trades(rows)
        for asset in self.assets:
            values = stats.get(asset)
            if values is None:
                raise RuntimeError(f"failed to compute initial stats for {asset}")
            mean, std, samples = values
            if std <= 0:
                raise RuntimeError(f"initial std must be positive for {asset}: samples={samples}")
            self.initial_mean_bps[asset] = mean
            self.initial_std_bps[asset] = std
            self.log.info(f"initial_stats {asset} samples={samples} mean={mean:.2f}bps std={std:.2f}bps")

    def _fetch_recent_trades(
        self,
        start_ms: int,
        end_ms: int,
        deadline: float,
    ) -> list[tuple[int, str, InstrumentId, Decimal]]:
        rows: list[tuple[int, str, InstrumentId, Decimal]] = []
        for instrument_id in self.instruments:
            venue = self._venue(instrument_id)
            asset = self._asset(instrument_id)
            if asset is None:
                continue
            if venue == "BINANCE":
                rows.extend(self._fetch_binance_trades(asset, instrument_id, start_ms, end_ms, deadline))
            elif venue == "OKX":
                rows.extend(self._fetch_okx_trades(asset, instrument_id, start_ms, end_ms, deadline))
            else:
                self.log.warning(f"initial_stats_skip_venue {instrument_id} venue={venue}")
        if not rows:
            raise RuntimeError("no initial trade ticks fetched")
        rows.sort(key=lambda item: item[0])
        return rows

    def _fetch_binance_trades(
        self,
        asset: str,
        instrument_id: InstrumentId,
        start_ms: int,
        end_ms: int,
        deadline: float,
    ) -> list[tuple[int, str, InstrumentId, Decimal]]:
        symbol = str(instrument_id.symbol).upper().replace("-PERP", "")
        rows: list[tuple[int, str, InstrumentId, Decimal]] = []
        next_ms = start_ms
        while next_ms <= end_ms:
            self._check_init_deadline(deadline)
            payload = self._get_json(
                "https://fapi.binance.com/fapi/v1/aggTrades",
                {"symbol": symbol, "startTime": next_ms, "endTime": end_ms, "limit": 1000},
            )
            if not payload:
                break
            for item in payload:
                ts_ms = int(item["T"])
                rows.append((ts_ms, asset, instrument_id, Decimal(str(item["p"]))))
            new_next = int(payload[-1]["T"]) + 1
            if new_next <= next_ms:
                break
            next_ms = new_next
            time.sleep(0.03)
        self.log.info(f"initial_fetch {asset} {instrument_id} venue=BINANCE ticks={len(rows)}")
        return rows

    def _fetch_okx_trades(
        self,
        asset: str,
        instrument_id: InstrumentId,
        start_ms: int,
        end_ms: int,
        deadline: float,
    ) -> list[tuple[int, str, InstrumentId, Decimal]]:
        inst_id = str(instrument_id.symbol).upper()
        rows: list[tuple[int, str, InstrumentId, Decimal]] = []
        seen: set[str] = set()
        after: str | None = None
        while True:
            self._check_init_deadline(deadline)
            params = {"instId": inst_id, "limit": 100}
            if after is not None:
                params["after"] = after
            payload = self._get_json("https://www.okx.com/api/v5/market/history-trades", params)
            if payload.get("code") != "0":
                raise RuntimeError(f"OKX history trades failed for {inst_id}: {payload}")
            data = payload.get("data") or []
            if not data:
                break
            oldest_ms = min(int(item["ts"]) for item in data)
            for item in data:
                trade_id = str(item["tradeId"])
                ts_ms = int(item["ts"])
                if ts_ms > end_ms or ts_ms < start_ms or trade_id in seen:
                    continue
                seen.add(trade_id)
                rows.append((ts_ms, asset, instrument_id, Decimal(str(item["px"]))))
            after = str(data[-1]["tradeId"])
            if oldest_ms < start_ms:
                break
            time.sleep(0.08)
        self.log.info(f"initial_fetch {asset} {instrument_id} venue=OKX ticks={len(rows)}")
        return rows

    def _get_json(self, url: str, params: dict[str, object]) -> object:
        last_error: Exception | None = None
        headers = {"User-Agent": "nt_quant_preipo_arb/1.0", "Connection": "close"}
        for attempt in range(5):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=15)
                if response.status_code == 200:
                    return response.json()
                last_error = RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
            except Exception as exc:
                last_error = exc
            time.sleep(0.4 * (attempt + 1))
        raise RuntimeError(f"historical tick request failed: {last_error}")

    def _check_init_deadline(self, deadline: float) -> None:
        if time.monotonic() > deadline:
            raise RuntimeError("initial historical tick fetch timed out")

    def _initial_stats_from_trades(
        self,
        rows: list[tuple[int, str, InstrumentId, Decimal]],
    ) -> dict[str, tuple[float, float, int]]:
        last: dict[str, dict[InstrumentId, tuple[int, Decimal]]] = {asset: {} for asset in self.assets}
        values: dict[str, list[float]] = {asset: [] for asset in self.assets}
        max_age_ms = int(self.max_quote_age_ns / 1_000_000)
        for ts_ms, asset, instrument_id, price in rows:
            last[asset][instrument_id] = (ts_ms, price)
            fresh = [
                (inst_id, px)
                for inst_id, (last_ms, px) in last[asset].items()
                if ts_ms - last_ms <= max_age_ms
            ]
            if len(fresh) < 2:
                continue
            best_edge: Decimal | None = None
            for buy_id, buy_px in fresh:
                for sell_id, sell_px in fresh:
                    if buy_id == sell_id:
                        continue
                    edge = self._net_bps(ArbLeg(buy_id, buy_px), ArbLeg(sell_id, sell_px))
                    if self.one_sided and edge <= 0:
                        continue
                    if best_edge is None or edge > best_edge:
                        best_edge = edge
            if best_edge is not None:
                values[asset].append(float(best_edge))

        stats: dict[str, tuple[float, float, int]] = {}
        for asset, samples in values.items():
            if len(samples) < 2:
                continue
            mean = sum(samples) / len(samples)
            variance = max(sum((value - mean) ** 2 for value in samples) / len(samples), 0.0)
            stats[asset] = (mean, sqrt(variance), len(samples))
        return stats

    def _valid_quotes(self, asset: str) -> tuple[list[ArbLeg], list[ArbLeg]]:
        bids: list[ArbLeg] = []
        asks: list[ArbLeg] = []
        now_ns = self.clock.timestamp_ns()
        for instrument_id, quote in self.quotes.items():
            if self._asset(instrument_id) != asset:
                continue
            if now_ns - quote.ts_event > self.max_quote_age_ns:
                continue
            bid = Decimal(str(quote.bid_price))
            ask = Decimal(str(quote.ask_price))
            if bid > 0:
                bids.append(ArbLeg(instrument_id, bid))
            if ask > 0:
                asks.append(ArbLeg(instrument_id, ask))
        return bids, asks

    def _best(self, asset: str) -> tuple[ArbLeg, ArbLeg] | None:
        bids, asks = self._valid_quotes(asset)
        if not bids or not asks:
            return None
        buy = min(asks, key=lambda leg: leg.price)
        sell = max(bids, key=lambda leg: leg.price)
        if buy.instrument_id == sell.instrument_id:
            return None
        return buy, sell

    def _net_bps(self, buy: ArbLeg, sell: ArbLeg) -> Decimal:
        gross = (sell.price - buy.price) / buy.price * Decimal("10000")
        costs = self._fee(buy.instrument_id) + self._fee(sell.instrument_id) + self.slippage_bps * Decimal("2")
        return gross - costs

    def _update_spreads(self, asset: str) -> dict[tuple[InstrumentId, InstrumentId], SpreadState]:
        states: dict[tuple[InstrumentId, InstrumentId], SpreadState] = {}
        if self.spread_mode != "mean_deviation":
            return states

        bids, asks = self._valid_quotes(asset)
        now_ns = self.clock.timestamp_ns()
        for buy in asks:
            for sell in bids:
                if buy.instrument_id == sell.instrument_id:
                    continue
                edge = self._net_bps(buy, sell)
                key = (asset, buy.instrument_id, sell.instrument_id)
                window = self.windows.setdefault(key, SpreadWindow())
                window.add(now_ns, float(edge), self.lookback_ns)
                samples, mean, std, window_sec = window.stats()
                stats = self._active_stats(asset, samples, mean, std, window_sec)
                if stats is None:
                    continue
                mean, std = stats
                z_score = (float(edge) - mean) / std
                states[(buy.instrument_id, sell.instrument_id)] = SpreadState(
                    buy=buy,
                    sell=sell,
                    edge_bps=edge,
                    mean_bps=mean,
                    std_bps=std,
                    z_score=z_score,
                    samples=samples,
                    window_sec=window_sec,
                )
        return states

    def _active_stats(
        self,
        asset: str,
        samples: int,
        mean: float,
        std: float,
        window_sec: float,
    ) -> tuple[float, float] | None:
        initial_std = self.initial_std_bps.get(asset, 0.0)
        if initial_std <= 0:
            return None
        initial_mean = self.initial_mean_bps[asset]
        if samples < 2 or std <= 0:
            return initial_mean, initial_std
        if window_sec * 1_000_000_000 < self.min_window_ns:
            return initial_mean, initial_std
        weight = min(max(window_sec * 1_000_000_000 / self.init_blend_ns, 0.0), 1.0)
        blended_mean = initial_mean * (1.0 - weight) + mean * weight
        blended_var = (
            (1.0 - weight) * (initial_std * initial_std + (initial_mean - blended_mean) ** 2)
            + weight * (std * std + (mean - blended_mean) ** 2)
        )
        return blended_mean, sqrt(max(blended_var, 0.0))

    def _maybe_open(self, asset: str, states: dict[tuple[InstrumentId, InstrumentId], SpreadState]) -> None:
        if self.arb_state != FLAT or self.active_asset is not None:
            return
        last_close_ns = self.last_close_ns.get(asset)
        now_ns = self.clock.timestamp_ns()
        if last_close_ns is not None and now_ns - last_close_ns < self.cooldown_ns:
            return

        if self.spread_mode == "threshold":
            self._maybe_open_threshold(asset, now_ns)
            return

        candidates = [state for state in states.values() if self._can_open(state)]
        if not candidates:
            return
        state = max(candidates, key=lambda item: item.z_score)
        self._open_pair(asset, state.buy, state.sell, state.edge_bps, state.mean_bps, state.z_score, now_ns)

    def _can_open(self, state: SpreadState) -> bool:
        if state.z_score < self.entry_z:
            return False
        if state.edge_bps < self.min_entry_bps:
            return False
        return not self.one_sided or state.edge_bps > 0

    def _maybe_open_threshold(self, asset: str, now_ns: int) -> None:
        if self.arb_state != FLAT or self.active_asset is not None:
            return
        best = self._best(asset)
        if best is None:
            return
        buy, sell = best
        net = self._net_bps(buy, sell)
        if net < self.entry_bps:
            return
        self._open_pair(asset, buy, sell, net, float(net), 0.0, now_ns)

    def _open_pair(
        self,
        asset: str,
        buy: ArbLeg,
        sell: ArbLeg,
        edge_bps: Decimal,
        mean_bps: float,
        z_score: float,
        now_ns: int,
    ) -> None:
        qty = self._shared_open_qty(buy.instrument_id, sell.instrument_id, buy.price, sell.price)
        if qty is None:
            return
        self.log.info(
            f"open_signal {asset} edge={edge_bps:.2f}bps mean={mean_bps:.2f}bps "
            f"z={z_score:.2f} qty={qty} "
            f"buy={buy.instrument_id}@{buy.price} sell={sell.instrument_id}@{sell.price}",
        )
        if self.config.dry_run:
            self.arb_state = OPENING
            self.active_asset = asset
            self.positions[asset] = ArbPos(
                buy_id=buy.instrument_id,
                sell_id=sell.instrument_id,
                buy_px=buy.price,
                sell_px=sell.price,
                buy_qty=qty,
                sell_qty=qty,
                edge_bps=edge_bps,
                mean_bps=mean_bps,
                z_score=z_score,
                opened_ns=now_ns,
            )
            self.arb_state = PAIRED
            self.active_asset = asset
            self.log.info(f"state {asset} {OPENING}->{PAIRED} dry_run qty={qty}")
            return

        self._submit_batch(
            asset=asset,
            action=OPEN,
            buy_id=buy.instrument_id,
            sell_id=sell.instrument_id,
            buy_px=buy.price,
            sell_px=sell.price,
            buy_side=OrderSide.BUY,
            sell_side=OrderSide.SELL,
            edge_bps=edge_bps,
            mean_bps=mean_bps,
            z_score=z_score,
            now_ns=now_ns,
            buy_qty=qty,
            sell_qty=qty,
        )

    def _maybe_close(self, asset: str, states: dict[tuple[InstrumentId, InstrumentId], SpreadState]) -> None:
        if self.arb_state != PAIRED or self.active_asset != asset:
            return
        pos = self.positions.get(asset)
        if pos is None:
            self.log.error(f"state_error {asset} state={self.arb_state} missing_position")
            self.arb_state = FLAT
            self.active_asset = None
            return
        buy_quote = self.quotes.get(pos.buy_id)
        sell_quote = self.quotes.get(pos.sell_id)
        if buy_quote is None or sell_quote is None:
            return
        now_ns = self.clock.timestamp_ns()
        if now_ns - buy_quote.ts_event > self.max_quote_age_ns:
            return
        if now_ns - sell_quote.ts_event > self.max_quote_age_ns:
            return

        age_ns = now_ns - pos.opened_ns
        expired = age_ns >= self.max_hold_ns
        if age_ns < self.min_hold_ns and not expired:
            return

        close_buy = ArbLeg(pos.sell_id, Decimal(str(sell_quote.ask_price)))
        close_sell = ArbLeg(pos.buy_id, Decimal(str(buy_quote.bid_price)))
        close_edge = self._net_bps(close_buy, close_sell)

        if self.spread_mode == "threshold":
            should_close = close_edge <= self.exit_bps or expired
            signal = f"close_edge={close_edge:.2f}bps"
            z_score = 0.0
        else:
            state = states.get((pos.buy_id, pos.sell_id))
            if state is None and not expired:
                return
            z_score = state.z_score if state is not None else float("nan")
            should_close = z_score <= self.exit_z or expired
            signal = f"edge_z={z_score:.2f} close_edge={close_edge:.2f}bps"

        if not should_close:
            return
        self.log.info(
            f"close_signal {asset} {signal} expired={expired} "
            f"sell_long={pos.buy_id}@{close_sell.price} buy_short={pos.sell_id}@{close_buy.price}",
        )
        if self.config.dry_run:
            self.arb_state = CLOSING
            self.last_close_ns[asset] = now_ns
            self.positions.pop(asset, None)
            self.arb_state = FLAT
            self.active_asset = None
            self.log.info(f"state {asset} {CLOSING}->{FLAT} dry_run")
            return

        self._submit_batch(
            asset=asset,
            action=CLOSE,
            buy_id=pos.buy_id,
            sell_id=pos.sell_id,
            buy_px=close_sell.price,
            sell_px=close_buy.price,
            buy_side=OrderSide.SELL,
            sell_side=OrderSide.BUY,
            edge_bps=close_edge,
            mean_bps=pos.mean_bps,
            z_score=z_score,
            now_ns=now_ns,
            buy_qty=pos.buy_qty,
            sell_qty=pos.sell_qty,
        )

    def _submit_batch(
        self,
        asset: str,
        action: str,
        buy_id: InstrumentId,
        sell_id: InstrumentId,
        buy_px: Decimal,
        sell_px: Decimal,
        buy_side: OrderSide,
        sell_side: OrderSide,
        edge_bps: Decimal,
        mean_bps: float,
        z_score: float,
        now_ns: int,
        buy_qty: Decimal | None = None,
        sell_qty: Decimal | None = None,
    ) -> PendingBatch:
        buy_order, buy_target = self._make_order(buy_id, buy_side, buy_px, buy_qty)
        sell_order, sell_target = self._make_order(sell_id, sell_side, sell_px, sell_qty)
        buy_order_id = str(buy_order.client_order_id)
        sell_order_id = str(sell_order.client_order_id)
        self.order_asset[buy_order_id] = asset
        self.order_asset[sell_order_id] = asset
        batch = PendingBatch(
            asset=asset,
            action=action,
            buy_id=buy_id,
            sell_id=sell_id,
            buy_px=buy_px,
            sell_px=sell_px,
            edge_bps=edge_bps,
            mean_bps=mean_bps,
            z_score=z_score,
            created_ns=now_ns,
            legs={
                buy_order_id: PendingLeg(buy_id, buy_side, buy_order, buy_target, Decimal("0")),
                sell_order_id: PendingLeg(sell_id, sell_side, sell_order, sell_target, Decimal("0")),
            },
        )
        self.pending[asset] = batch
        if action == OPEN:
            self.arb_state = OPENING
            self.active_asset = asset
        else:
            self.arb_state = CLOSING
        self.log.info(
            f"submit_batch {asset} action={action} buy_order={buy_order_id} sell_order={sell_order_id} "
            f"buy={buy_id} {buy_side} qty={buy_target} sell={sell_id} {sell_side} qty={sell_target}",
        )
        try:
            self.submit_order(buy_order)
            self.submit_order(sell_order)
        except Exception as exc:
            self.log.error(f"submit_batch_failed {asset} action={action} error={exc}")
            self._emergency_flatten(batch)
            self.positions.pop(asset, None)
            self.last_close_ns[asset] = self.clock.timestamp_ns()
            self.arb_state = FLAT
            self.active_asset = None
            self._clear_pending(batch)
            raise
        return batch

    def _make_order(
        self,
        instrument_id: InstrumentId,
        side: OrderSide,
        price: Decimal,
        qty: Decimal | None = None,
    ) -> tuple[MarketOrder, Decimal]:
        instrument = self.cache.instrument(instrument_id)
        quantity = instrument.make_qty(qty if qty is not None else self.notional / price)
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

    def _shared_open_qty(
        self,
        buy_id: InstrumentId,
        sell_id: InstrumentId,
        buy_px: Decimal,
        sell_px: Decimal,
    ) -> Decimal | None:
        raw_qty = min(self.notional / buy_px, self.notional / sell_px)
        qty = raw_qty
        for _ in range(4):
            buy_qty = self._round_qty(buy_id, qty)
            sell_qty = self._round_qty(sell_id, qty)
            next_qty = min(buy_qty, sell_qty)
            if next_qty == qty:
                break
            qty = next_qty
        buy_qty = self._round_qty(buy_id, qty)
        sell_qty = self._round_qty(sell_id, qty)
        if buy_qty != sell_qty:
            self.log.warning(
                f"skip_open_qty_mismatch buy={buy_id} qty={buy_qty} sell={sell_id} qty={sell_qty} notional={self.notional}",
            )
            return None
        if buy_qty <= 0:
            self.log.warning(
                f"skip_open_qty_too_small buy={buy_id} sell={sell_id} notional={self.notional} raw_qty={qty}",
            )
            return None
        if buy_qty * buy_px > self.notional or sell_qty * sell_px > self.notional:
            self.log.warning(
                f"skip_open_qty_over_notional buy={buy_id} sell={sell_id} qty={buy_qty} "
                f"notional={self.notional} raw_qty={raw_qty}",
            )
            return None
        return buy_qty

    def _batch_done(self, batch: PendingBatch) -> bool:
        return all(leg.filled_qty >= leg.target_qty for leg in batch.legs.values())

    def _confirm_open(self, batch: PendingBatch) -> None:
        buy_qty = self._filled_qty(batch, batch.buy_id)
        sell_qty = self._filled_qty(batch, batch.sell_id)
        self.positions[batch.asset] = ArbPos(
            buy_id=batch.buy_id,
            sell_id=batch.sell_id,
            buy_px=batch.buy_px,
            sell_px=batch.sell_px,
            buy_qty=buy_qty,
            sell_qty=sell_qty,
            edge_bps=batch.edge_bps,
            mean_bps=batch.mean_bps,
            z_score=batch.z_score,
            opened_ns=batch.created_ns,
        )
        self.arb_state = PAIRED
        self.active_asset = batch.asset
        self._clear_pending(batch)
        if buy_qty != sell_qty:
            self.log.warning(f"filled_qty_mismatch {batch.asset} buy_qty={buy_qty} sell_qty={sell_qty}")
        self.log.info(f"state {batch.asset} {OPENING}->{PAIRED} qty={buy_qty}")

    def _confirm_close(self, batch: PendingBatch) -> None:
        self.positions.pop(batch.asset, None)
        self.last_close_ns[batch.asset] = self.clock.timestamp_ns()
        self.arb_state = FLAT
        self.active_asset = None
        self._clear_pending(batch)
        self.log.info(f"state {batch.asset} {CLOSING}->{FLAT}")

    def _filled_qty(self, batch: PendingBatch, instrument_id: InstrumentId) -> Decimal:
        total = Decimal("0")
        for leg in batch.legs.values():
            if leg.instrument_id == instrument_id:
                total += leg.filled_qty
        return total

    def _handle_order_failed(self, order_id: str, reason: str) -> None:
        asset = self.order_asset.get(order_id)
        if asset is None:
            return
        batch = self.pending.get(asset)
        if batch is None:
            self.order_asset.pop(order_id, None)
            return
        self.log.error(f"order_failed {asset} action={batch.action} order={order_id} reason={reason}")
        self._emergency_flatten(batch)
        self.positions.pop(asset, None)
        self.last_close_ns[asset] = self.clock.timestamp_ns()
        self.arb_state = FLAT
        self.active_asset = None
        self._clear_pending(batch)

    def _emergency_flatten(self, batch: PendingBatch) -> None:
        if self.config.dry_run:
            return
        if batch.action == OPEN:
            for leg in batch.legs.values():
                if leg.filled_qty > 0:
                    self._submit_emergency(leg.instrument_id, self._opposite(leg.side), leg.filled_qty)
            return

        for leg in batch.legs.values():
            remaining = leg.target_qty - leg.filled_qty
            if remaining > 0:
                self._submit_emergency(leg.instrument_id, leg.side, remaining)

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
            self.order_asset.pop(order_id, None)
        self.pending.pop(batch.asset, None)

    def _asset_state(self, asset: str) -> str:
        if self.active_asset is None or self.active_asset == asset:
            return self.arb_state
        return FLAT

    def _maybe_update_snapshot(self, current_states: dict[tuple[InstrumentId, InstrumentId], SpreadState]) -> None:
        if self.snapshot_interval_ns <= 0 or self.snapshot_display == "off":
            return
        now_ns = self.clock.timestamp_ns()
        if now_ns - self.last_snapshot_ns < self.snapshot_interval_ns:
            return
        self.last_snapshot_ns = now_ns
        rows: dict[str, dict[str, str]] = {}
        market_tables: dict[str, list[dict[str, str]]] = {}
        position_rows: list[dict[str, str]] = []
        state_rows: list[dict[str, str]] = []
        log_parts = []
        for asset in self.assets:
            best = self._best(asset)
            asset_state = self._asset_state(asset)
            active = "Y" if self.active_asset == asset else "N"
            pending = "Y" if asset in self.pending else "N"
            position = self.positions.get(asset)
            position_rows.append(self._position_snapshot(asset, position, current_states, now_ns))
            state_rows.append({
                "asset": asset,
                "state": asset_state,
                "active": active,
                "pending": pending,
                "has_position": "Y" if position is not None else "N",
            })
            if best is None:
                rows[asset] = {
                    "asset": asset,
                    "state": asset_state,
                    "active": active,
                    "pending": pending,
                    "buy": "-",
                    "sell": "-",
                    "edge": "-",
                    "z": "-",
                    "mean": "-",
                    "std": "-",
                    "samples": "0",
                    "window": "0s",
                    "quotes": "waiting",
                }
                market_tables[asset] = self._market_rows(asset, None, "-", "-", "-", "-", "-", "-")
                log_parts.append(f"{asset} state={asset_state} active={active} quotes=waiting")
                continue
            buy, sell = best
            edge = self._net_bps(buy, sell)
            state = current_states.get((buy.instrument_id, sell.instrument_id))
            if state is None:
                samples, mean, std, window_sec = self._window_debug(asset, buy.instrument_id, sell.instrument_id)
                initial = "initial" if samples < self.min_spread_samples else "rolling_pending"
                mean = self.initial_mean_bps.get(asset, mean)
                std = self.initial_std_bps.get(asset, std)
                z_score = (float(edge) - mean) / std if std > 0 else 0.0
                source = initial
            else:
                samples = state.samples
                mean = state.mean_bps
                std = state.std_bps
                z_score = state.z_score
                window_sec = state.window_sec
                weight = min(max(window_sec * 1_000_000_000 / self.init_blend_ns, 0.0), 1.0)
                source = "rolling" if weight >= 1.0 else f"blend {weight:.0%}"
            quotes = self._quote_text(asset)
            rows[asset] = {
                "asset": asset,
                "state": asset_state,
                "active": active,
                "pending": pending,
                "buy": f"{buy.instrument_id}@{buy.price}",
                "sell": f"{sell.instrument_id}@{sell.price}",
                "edge": f"{edge:.2f}",
                "z": f"{z_score:.2f}",
                "mean": f"{mean:.2f}",
                "std": f"{std:.2f}",
                "samples": str(samples),
                "window": f"{window_sec:.0f}s",
                "source": source,
                "quotes": quotes,
            }
            market_tables[asset] = self._market_rows(
                asset,
                (buy, sell),
                f"{edge:.2f}",
                f"{mean:.2f}",
                f"{std:.2f}",
                f"{z_score:.2f}",
                source,
                f"{window_sec:.0f}s",
            )
            log_parts.append(
                f"{asset} state={asset_state} active={active} pending={asset in self.pending} "
                f"edge={edge:.2f}bps z={z_score:.2f} buy={buy.instrument_id}@{buy.price} "
                f"sell={sell.instrument_id}@{sell.price} source={source} quotes=[{quotes}]",
            )
        if self.snapshot_display == "log":
            self.log.info("market_snapshot | " + " | ".join(log_parts))
            return
        with self.snapshot_lock:
            self.snapshot_rows.update(rows)
            self.snapshot_rows["__market_tables__"] = market_tables
            self.snapshot_rows["__position_rows__"] = position_rows
            self.snapshot_rows["__state_rows__"] = state_rows

    def _market_rows(
        self,
        asset: str,
        best: tuple[ArbLeg, ArbLeg] | None,
        edge: str,
        mean: str,
        std: str,
        z_score: str,
        source: str,
        window: str,
    ) -> list[dict[str, str]]:
        now_ns = self.clock.timestamp_ns()
        best_buy = best[0].instrument_id if best is not None else None
        best_sell = best[1].instrument_id if best is not None else None
        rows = []
        for instrument_id in sorted(self.instruments, key=lambda item: str(item)):
            if self._asset(instrument_id) != asset:
                continue
            quote = self.quotes.get(instrument_id)
            if quote is None:
                rows.append({
                    "exchange": self._venue(instrument_id),
                    "instrument": str(instrument_id),
                    "bid1": "-",
                    "ask1": "-",
                    "age": "-",
                    "role": "-",
                    "edge": edge,
                    "mean": mean,
                    "std": std,
                    "z": z_score,
                    "source": source,
                    "window": window,
                })
                continue
            age_sec = (now_ns - quote.ts_event) / 1_000_000_000
            role = "-"
            if instrument_id == best_buy:
                role = "BUY"
            elif instrument_id == best_sell:
                role = "SELL"
            rows.append({
                "exchange": self._venue(instrument_id),
                "instrument": str(instrument_id),
                "bid1": str(quote.bid_price),
                "ask1": str(quote.ask_price),
                "age": f"{age_sec:.1f}s",
                "role": role,
                "edge": edge,
                "mean": mean,
                "std": std,
                "z": z_score,
                "source": source,
                "window": window,
            })
        return rows

    def _position_snapshot(
        self,
        asset: str,
        position: ArbPos | None,
        states: dict[tuple[InstrumentId, InstrumentId], SpreadState],
        now_ns: int,
    ) -> dict[str, str]:
        if position is None:
            return {
                "asset": asset,
                "side": "-",
                "entry_buy": "-",
                "entry_sell": "-",
                "qty": "-",
                "entry_edge": "-",
                "entry_z": "-",
                "current_close_edge": "-",
                "current_z": "-",
                "hold": "-",
            }
        buy_quote = self.quotes.get(position.buy_id)
        sell_quote = self.quotes.get(position.sell_id)
        close_edge = "-"
        current_z = "-"
        if buy_quote is not None and sell_quote is not None:
            close_buy = ArbLeg(position.sell_id, Decimal(str(sell_quote.ask_price)))
            close_sell = ArbLeg(position.buy_id, Decimal(str(buy_quote.bid_price)))
            close_edge = f"{self._net_bps(close_buy, close_sell):.2f}"
            state = states.get((position.buy_id, position.sell_id))
            if state is not None:
                current_z = f"{state.z_score:.2f}"
        hold_sec = max((now_ns - position.opened_ns) / 1_000_000_000, 0.0)
        return {
            "asset": asset,
            "side": f"buy_{self._venue(position.buy_id).lower()}_sell_{self._venue(position.sell_id).lower()}",
            "entry_buy": f"{position.buy_id}@{position.buy_px}",
            "entry_sell": f"{position.sell_id}@{position.sell_px}",
            "qty": f"{position.buy_qty}/{position.sell_qty}",
            "entry_edge": f"{position.edge_bps:.2f}",
            "entry_z": f"{position.z_score:.2f}",
            "current_close_edge": close_edge,
            "current_z": current_z,
            "hold": f"{hold_sec:.0f}s",
        }

    def _window_debug(
        self,
        asset: str,
        buy_id: InstrumentId,
        sell_id: InstrumentId,
    ) -> tuple[int, float, float, float]:
        window = self.windows.get((asset, buy_id, sell_id))
        if window is None:
            return 0, 0.0, 0.0, 0.0
        return window.stats()

    def _quote_text(self, asset: str) -> str:
        now_ns = self.clock.timestamp_ns()
        rows = []
        for instrument_id, quote in self.quotes.items():
            if self._asset(instrument_id) != asset:
                continue
            age_sec = (now_ns - quote.ts_event) / 1_000_000_000
            if age_sec > self.max_quote_age_ns / 1_000_000_000:
                continue
            rows.append(
                f"{self._venue(instrument_id)} bid={quote.bid_price} ask={quote.ask_price} age={age_sec:.1f}s",
            )
        return "; ".join(rows)

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

    def _write_snapshot(self, rows: dict[str, dict[str, str]]) -> None:
        market_tables = rows.get("__market_tables__", {})
        position_rows = rows.get("__position_rows__", [])
        state_rows = rows.get("__state_rows__", [])
        asset_rows = {key: value for key, value in rows.items() if not key.startswith("__")}
        payload = {
            "ts_ns": self.clock.timestamp_ns(),
            "strategy": "preipo_arb",
            "assets": self.assets,
            "rows": [asset_rows[asset] for asset in self.assets if asset in asset_rows],
            "market_tables": market_tables,
            "position_rows": position_rows,
            "state_rows": state_rows,
        }
        tmp = self.snapshot_path.with_suffix(f"{self.snapshot_path.suffix}.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.snapshot_path)

    def _snapshot_table(self, rows: dict[str, dict[str, str]]) -> Table:
        table = Table(title="PREIPO Arbitrage Live", expand=True)
        for column, justify in (
            ("asset", "left"),
            ("state", "left"),
            ("active", "center"),
            ("pending", "center"),
            ("edge", "right"),
            ("z", "right"),
            ("mean", "right"),
            ("std", "right"),
            ("src", "left"),
            ("samples", "right"),
            ("window", "right"),
            ("buy", "left"),
            ("sell", "left"),
            ("quotes", "left"),
        ):
            table.add_column(column, justify=justify, no_wrap=column not in {"quotes", "buy", "sell"})
        for asset in self.assets:
            row = rows.get(asset)
            if row is None:
                table.add_row(asset, "FLAT", "N", "N", "-", "-", "-", "-", "-", "0", "0s", "-", "-", "waiting")
                continue
            table.add_row(
                row["asset"],
                row["state"],
                row.get("active", "N"),
                row["pending"],
                row["edge"],
                row["z"],
                row["mean"],
                row["std"],
                row.get("source", "-"),
                row["samples"],
                row["window"],
                row["buy"],
                row["sell"],
                row["quotes"],
            )
        return table
