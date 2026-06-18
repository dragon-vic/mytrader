from __future__ import annotations

import sys
import json
import time
import requests
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from threading import Lock
from threading import Thread
from threading import Event as ThreadEvent
from collections import defaultdict
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from math import sqrt
from pathlib import Path

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
PAIRED = "PAIRED"
CLOSING = "CLOSING"
STOPPING = "STOPPING"
OPEN = "OPEN"
CLOSE = "CLOSE"
INIT_TICK_MATCH_MS = 15_000
BEIJING_TZ = timezone(timedelta(hours=8))


@dataclass
class ArbLeg:
    instrument_id: InstrumentId
    price: Decimal


@dataclass
class InitialTrade:
    ts_ms: int
    asset: str
    instrument_id: InstrumentId
    price: Decimal
    side: str


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
    std_bps: float
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
    std_bps: float
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
    entry_z_step: Decimal
    exit_z: Decimal
    min_entry_bps: Decimal
    trade_notional: Decimal
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
        self.entry_z_step = float(config.entry_z_step)
        self.exit_z = float(config.exit_z)
        self.min_entry_bps = Decimal(str(config.min_entry_bps))
        self.notional = Decimal(str(config.trade_notional))
        self.max_quote_age_ns = int(float(config.max_quote_age_sec) * 1_000_000_000)
        self.snapshot_interval_ns = int(float(config.snapshot_interval_sec) * 1_000_000_000)
        self.snapshot_display = str(config.snapshot_display).lower()
        self.snapshot_path = Path(config.snapshot_path)
        self.quotes: dict[InstrumentId, QuoteTick] = {}
        self.windows: dict[tuple[str, InstrumentId, InstrumentId], SpreadWindow] = {}
        self.arb_state = FLAT
        self.active_asset: str | None = None
        self.stopped = False
        self.stop_requested = False
        self.positions: dict[str, list[ArbPos]] = {}
        self.pending: dict[str, PendingBatch] = {}
        self.failed_assets: set[str] = set()
        self.order_asset: dict[str, str] = {}
        self.last_snapshot_ns = 0
        self.snapshot_rows: dict[str, dict[str, str]] = {}
        self.last_spread_states: dict[str, dict[tuple[InstrumentId, InstrumentId], SpreadState]] = {}
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
            if self.entry_z_step <= 0:
                raise RuntimeError("entry_z_step must be positive")
            self._refresh_initial_stats()
            for asset in self.assets:
                if asset not in self.initial_mean_bps:
                    raise RuntimeError(f"initial_mean_bps missing asset: {asset}")
                if self.initial_std_bps.get(asset, 0) <= 0:
                    raise RuntimeError(f"initial_std_bps must be positive for asset: {asset}")
        if self.notional <= 0:
            raise RuntimeError("trade_notional must be positive")
        if self.config.max_quote_age_sec < 0:
            raise RuntimeError("max_quote_age_sec must be non-negative")
        if self.config.snapshot_interval_sec < 0:
            raise RuntimeError("snapshot_interval_sec must be non-negative")
        if self.snapshot_display not in {"rich", "log", "file", "off"}:
            raise RuntimeError("snapshot_display must be rich, log, file, or off")
        self._check_start_account_state()
        if self.stopped:
            return
        for instrument_id in self.instruments:
            self.subscribe_quote_ticks(instrument_id)
        self._start_snapshot_display()
        self.log.info(
            f"preipo_arb started assets={','.join(self.assets)} instruments={len(self.instruments)} "
            f"mode={self.spread_mode} one_sided={self.one_sided} "
            f"lookback={self.config.lookback_sec}s min_window={self.config.min_window_sec}s "
            f"init_fetch={self.config.init_fetch_sec}s init_blend={self.config.init_blend_sec}s "
            f"entry_z={self.entry_z} entry_z_step={self.entry_z_step} exit_z={self.exit_z} "
            f"exit_bps={self.exit_bps} initial_mean={self.initial_mean_bps} "
            f"initial_std={self.initial_std_bps} notional={self.notional} "
            f"snapshot_interval={self.config.snapshot_interval_sec}s snapshot_display={self.snapshot_display} "
            f"snapshot_path={self.snapshot_path} "
            f"dry_run={self.config.dry_run}",
        )

    def on_quote_tick(self, tick: QuoteTick) -> None:
        if self.stopped:
            return
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
            if self._maybe_close(asset, states):
                return
            self._maybe_open(asset, states)
            return
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
        fill_state = "filled" if self._leg_filled(leg) else "partial"
        self.log.info(
            f"{fill_state}_fill {asset} action={batch.action} order={order_id} "
            f"{leg.instrument_id} filled={leg.filled_qty}/{leg.target_qty}",
        )
        if asset in self.failed_assets:
            if batch.action == OPEN:
                self._try_submit_emergency(leg.instrument_id, self._opposite(leg.side), Decimal(str(event.last_qty)))
            return
        if not self._batch_filled(batch):
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
            self.log.info(f"initial_stats_quote_est {asset} samples={samples} mean={mean:.2f}bps std={std:.2f}bps")

    def _fetch_recent_trades(
        self,
        start_ms: int,
        end_ms: int,
        deadline: float,
    ) -> list[InitialTrade]:
        rows: list[InitialTrade] = []
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
        rows.sort(key=lambda item: item.ts_ms)
        return rows

    def _fetch_binance_trades(
        self,
        asset: str,
        instrument_id: InstrumentId,
        start_ms: int,
        end_ms: int,
        deadline: float,
    ) -> list[InitialTrade]:
        symbol = str(instrument_id.symbol).upper().replace("-PERP", "")
        rows: list[InitialTrade] = []
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
                side = "sell" if bool(item.get("m")) else "buy"
                rows.append(InitialTrade(ts_ms, asset, instrument_id, Decimal(str(item["p"])), side))
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
    ) -> list[InitialTrade]:
        inst_id = str(instrument_id.symbol).upper()
        rows: list[InitialTrade] = []
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
                rows.append(
                    InitialTrade(
                        ts_ms,
                        asset,
                        instrument_id,
                        Decimal(str(item["px"])),
                        str(item.get("side", "")).lower(),
                    ),
                )
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
        rows: list[InitialTrade],
    ) -> dict[str, tuple[float, float, int]]:
        quotes: dict[str, dict[InstrumentId, dict[str, tuple[int, Decimal]]]] = {asset: {} for asset in self.assets}
        values: dict[str, list[float]] = {asset: [] for asset in self.assets}
        max_age_ms = INIT_TICK_MATCH_MS
        for row in rows:
            if row.side not in {"buy", "sell"}:
                continue
            side = "ask" if row.side == "buy" else "bid"
            quotes[row.asset].setdefault(row.instrument_id, {})[side] = (row.ts_ms, row.price)

            binance_id = next(
                (instrument_id for instrument_id in quotes[row.asset] if self._venue(instrument_id) == "BINANCE"),
                None,
            )
            if binance_id is None:
                continue
            binance_bid = self._fresh_initial_quote(quotes[row.asset][binance_id], "bid", row.ts_ms, max_age_ms)
            binance_ask = self._fresh_initial_quote(quotes[row.asset][binance_id], "ask", row.ts_ms, max_age_ms)
            if binance_bid is None or binance_ask is None:
                continue
            binance_mid = (binance_bid + binance_ask) / Decimal("2")
            best_edge: Decimal | None = None
            for instrument_id, book in quotes[row.asset].items():
                if self._venue(instrument_id) == "BINANCE":
                    continue
                other_bid = self._fresh_initial_quote(book, "bid", row.ts_ms, max_age_ms)
                other_ask = self._fresh_initial_quote(book, "ask", row.ts_ms, max_age_ms)
                edges = []
                if other_bid is not None:
                    edges.append(self._edge_bps(other_bid - binance_ask, binance_mid))
                if other_ask is not None:
                    edges.append(self._edge_bps(binance_bid - other_ask, binance_mid))
                for edge in edges:
                    if edge is None:
                        continue
                    if self.one_sided and edge <= 0:
                        continue
                    if best_edge is None or edge > best_edge:
                        best_edge = edge
            if best_edge is not None:
                values[row.asset].append(float(best_edge))

        stats: dict[str, tuple[float, float, int]] = {}
        for asset, samples in values.items():
            if len(samples) < 2:
                continue
            mean = sum(samples) / len(samples)
            variance = max(sum((value - mean) ** 2 for value in samples) / len(samples), 0.0)
            stats[asset] = (mean, sqrt(variance), len(samples))
        return stats

    # 用主动成交方向近似初始化阶段的 bid/ask。
    def _fresh_initial_quote(
        self,
        book: dict[str, tuple[int, Decimal]],
        side: str,
        ts_ms: int,
        max_age_ms: int,
    ) -> Decimal | None:
        value = book.get(side)
        if value is None:
            return None
        quote_ts_ms, price = value
        if ts_ms - quote_ts_ms > max_age_ms:
            return None
        return price

    def _valid_quotes(self, asset: str) -> tuple[list[ArbLeg], list[ArbLeg]]:
        bids: list[ArbLeg] = []
        asks: list[ArbLeg] = []
        for instrument_id, quote in self.quotes.items():
            if self._asset(instrument_id) != asset:
                continue
            bid = Decimal(str(quote.bid_price))
            ask = Decimal(str(quote.ask_price))
            if bid > 0:
                bids.append(ArbLeg(instrument_id, bid))
            if ask > 0:
                asks.append(ArbLeg(instrument_id, ask))
        return bids, asks

    def _best(self, asset: str) -> tuple[ArbLeg, ArbLeg] | None:
        candidates = self._route_candidates(asset)
        if not candidates:
            return None
        _, buy, sell = max(candidates, key=lambda item: item[0])
        return buy, sell

    def _binance_mid(self, asset: str) -> Decimal | None:
        for instrument_id, quote in self.quotes.items():
            if self._asset(instrument_id) != asset or self._venue(instrument_id) != "BINANCE":
                continue
            bid = Decimal(str(quote.bid_price))
            ask = Decimal(str(quote.ask_price))
            if bid <= 0 or ask <= 0:
                return None
            return (bid + ask) / Decimal("2")
        return None

    def _edge_bps(self, numerator: Decimal, price: Decimal) -> Decimal | None:
        if price <= 0:
            return None
        return numerator / price * Decimal("10000")

    # 策略信号以 Binance mid 归一化，只看盘口价差，暂不扣手续费和滑点。
    def _route_edge_bps(self, asset: str, buy: ArbLeg, sell: ArbLeg) -> Decimal | None:
        buy_is_binance = self._venue(buy.instrument_id) == "BINANCE"
        sell_is_binance = self._venue(sell.instrument_id) == "BINANCE"
        if buy_is_binance == sell_is_binance:
            return None
        price = self._binance_mid(asset)
        if price is None:
            return None
        return self._edge_bps(sell.price - buy.price, price)

    def _route_candidates(self, asset: str) -> list[tuple[Decimal, ArbLeg, ArbLeg]]:
        bids, asks = self._valid_quotes(asset)
        candidates: list[tuple[Decimal, ArbLeg, ArbLeg]] = []
        for buy in asks:
            for sell in bids:
                if buy.instrument_id == sell.instrument_id:
                    continue
                edge = self._route_edge_bps(asset, buy, sell)
                if edge is not None:
                    candidates.append((edge, buy, sell))
        return candidates

    def _close_edge_bps(self, asset: str, long_leg: ArbLeg, short_leg: ArbLeg) -> Decimal | None:
        price = self._binance_mid(asset)
        if price is None:
            return None
        return self._edge_bps(short_leg.price - long_leg.price, price)

    def _update_spreads(self, asset: str) -> dict[tuple[InstrumentId, InstrumentId], SpreadState]:
        states: dict[tuple[InstrumentId, InstrumentId], SpreadState] = {}
        if self.spread_mode != "mean_deviation":
            return states

        now_ns = self.clock.timestamp_ns()
        for edge, buy, sell in self._route_candidates(asset):
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
        self.last_spread_states[asset] = states
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
        if self.stopped:
            return
        if self.arb_state not in {FLAT, PAIRED}:
            return
        if self.active_asset is not None and self.active_asset != asset:
            return
        now_ns = self.clock.timestamp_ns()

        if self.spread_mode == "threshold":
            self._maybe_open_threshold(asset, now_ns)
            return

        threshold = self._next_entry_z(asset)
        candidates = [
            state
            for state in states.values()
            if self._can_open(state, threshold) and self._same_route(asset, state)
        ]
        if not candidates:
            return
        state = max(candidates, key=lambda item: item.z_score)
        if self.positions.get(asset):
            self.log.info(
                f"add_signal {asset} next_z={threshold:.2f} lots={len(self.positions[asset])} "
                f"z={state.z_score:.2f} buy={state.buy.instrument_id} sell={state.sell.instrument_id}",
            )
        self._open_pair(asset, state.buy, state.sell, state.edge_bps, state.mean_bps, state.std_bps, state.z_score, now_ns)

    def _next_entry_z(self, asset: str) -> float:
        return self.entry_z + len(self.positions.get(asset, [])) * self.entry_z_step

    def _same_route(self, asset: str, state: SpreadState) -> bool:
        lots = self.positions.get(asset)
        if not lots:
            return True
        first = lots[0]
        return state.buy.instrument_id == first.buy_id and state.sell.instrument_id == first.sell_id

    def _can_open(self, state: SpreadState, threshold: float) -> bool:
        if state.z_score < threshold:
            return False
        if state.edge_bps < self.min_entry_bps:
            return False
        return not self.one_sided or state.edge_bps > 0

    def _maybe_open_threshold(self, asset: str, now_ns: int) -> None:
        if self.stopped:
            return
        if self.arb_state != FLAT or self.active_asset is not None:
            return
        best = self._best(asset)
        if best is None:
            return
        buy, sell = best
        net = self._route_edge_bps(asset, buy, sell)
        if net is None:
            return
        if net < self.entry_bps:
            return
        self._open_pair(asset, buy, sell, net, float(net), 0.0, 0.0, now_ns)

    def _open_pair(
        self,
        asset: str,
        buy: ArbLeg,
        sell: ArbLeg,
        edge_bps: Decimal,
        mean_bps: float,
        std_bps: float,
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
            self.positions.setdefault(asset, []).append(ArbPos(
                buy_id=buy.instrument_id,
                sell_id=sell.instrument_id,
                buy_px=buy.price,
                sell_px=sell.price,
                buy_qty=qty,
                sell_qty=qty,
                edge_bps=edge_bps,
                mean_bps=mean_bps,
                std_bps=std_bps,
                z_score=z_score,
                opened_ns=now_ns,
            ))
            self.arb_state = PAIRED
            self.active_asset = asset
            self.log.info(
                f"state {asset} {OPENING}->{PAIRED} dry_run qty={qty} "
                f"lots={len(self.positions[asset])} next_z={self._next_entry_z(asset):.2f}",
            )
            self._log_next_notional(asset)
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
            std_bps=std_bps,
            z_score=z_score,
            now_ns=now_ns,
            buy_qty=qty,
            sell_qty=qty,
        )

    def _maybe_close(self, asset: str, states: dict[tuple[InstrumentId, InstrumentId], SpreadState]) -> bool:
        if self.arb_state != PAIRED or self.active_asset != asset:
            return False
        lots = self.positions.get(asset, [])
        if not lots:
            self.log.error(f"state_error {asset} state={self.arb_state} missing_position")
            self.arb_state = FLAT
            self.active_asset = None
            return False
        pos = lots[0]
        buy_quote = self.quotes.get(pos.buy_id)
        sell_quote = self.quotes.get(pos.sell_id)
        if buy_quote is None or sell_quote is None:
            return False
        now_ns = self.clock.timestamp_ns()

        close_sell = ArbLeg(pos.buy_id, Decimal(str(buy_quote.bid_price)))
        close_buy = ArbLeg(pos.sell_id, Decimal(str(sell_quote.ask_price)))
        close_edge = self._close_edge_bps(asset, close_sell, close_buy)
        if close_edge is None:
            return False

        first = lots[0]
        entry_edge = first.edge_bps
        capture_bps = entry_edge - close_edge
        state = states.get((pos.buy_id, pos.sell_id))
        edge_stop = close_edge <= entry_edge - self.exit_bps
        z_score = float("nan")
        mean = first.mean_bps
        std = first.std_bps
        stat_stop = False
        if state is not None and state.std_bps > 0:
            mean = state.mean_bps
            std = state.std_bps
            z_score = (float(close_edge) - state.mean_bps) / state.std_bps
            stat_stop = close_edge <= Decimal(str(state.mean_bps + self.exit_z * state.std_bps))
        should_close = edge_stop or stat_stop
        signal = (
            f"entry_edge={entry_edge:.2f}bps close_edge={close_edge:.2f}bps "
            f"capture={capture_bps:.2f}bps edge_stop={edge_stop} "
            f"mean={mean:.2f}bps std={std:.2f}bps "
            f"close_z={z_score:.2f} stat_stop={stat_stop}"
        )

        if not should_close:
            return False
        buy_qty = sum((lot.buy_qty for lot in lots), Decimal("0"))
        sell_qty = sum((lot.sell_qty for lot in lots), Decimal("0"))
        self.log.info(
            f"close_signal {asset} {signal} lots={len(lots)} "
            f"sell_long={pos.buy_id}@{close_sell.price} qty={buy_qty} "
            f"buy_short={pos.sell_id}@{close_buy.price} qty={sell_qty}",
        )
        if self.config.dry_run:
            self.arb_state = CLOSING
            self.positions.pop(asset, None)
            self.arb_state = FLAT
            self.active_asset = None
            self.log.info(f"state {asset} {CLOSING}->{FLAT} dry_run")
            return True

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
            std_bps=pos.std_bps,
            z_score=z_score,
            now_ns=now_ns,
            buy_qty=buy_qty,
            sell_qty=sell_qty,
        )
        return True

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
        std_bps: float,
        z_score: float,
        now_ns: int,
        buy_qty: Decimal | None = None,
        sell_qty: Decimal | None = None,
    ) -> PendingBatch | None:
        buy_order, buy_target = self._make_order(buy_id, buy_side, buy_px, buy_qty)
        sell_order, sell_target = self._make_order(sell_id, sell_side, sell_px, sell_qty)
        if action == OPEN and not self._check_open_balances(asset, buy_id, buy_px, buy_target, sell_id, sell_px, sell_target):
            self._fail_before_submit(asset, "insufficient USDT for opening pair")
            return None
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
            std_bps=std_bps,
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
            self._fail_batch(batch, f"submit exception: {exc}")
        return batch

    # 启动时确认没有遗留持仓，并且每个执行账户至少够开一条腿。
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

        for venue in sorted({self._venue(instrument_id) for instrument_id in self.instruments}):
            account = self._account_for_venue(venue)
            if account is None:
                self.log.error(f"start_check_failed venue={venue} account=missing")
                self._request_stop(f"preipo 启动检查缺少 {venue} 账户")
                return
            free = self._free_usdt(account)
            if free < self.notional:
                self.log.error(
                    f"start_check_failed venue={venue} account={account.id} free_usdt={free} required={self.notional}",
                )
                self._request_stop(f"preipo 启动检查 {venue} USDT 不足")
                return

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
        for instrument_id, price, qty in (
            (buy_id, buy_px, buy_qty),
            (sell_id, sell_px, sell_qty),
        ):
            venue = self._venue(instrument_id)
            account = self._account_for_venue(venue)
            if account is None:
                self.log.error(f"open_check_failed {asset} venue={venue} account=missing")
                return False
            account_id = str(account.id)
            accounts[account_id] = account
            required[account_id] += price * qty

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
        for venue in sorted({self._venue(instrument_id) for instrument_id in self.instruments if self._asset(instrument_id) == asset}):
            account = self._account_for_venue(venue)
            if account is None:
                self.log.error(f"next_open_balance_blocked {asset} venue={venue} account=missing")
                continue
            free = self._free_usdt(account)
            if free < self.notional:
                self.log.warning(
                    f"next_open_balance_blocked {asset} venue={venue} account={account.id} "
                    f"free_usdt={free} required={self.notional}",
                )
            else:
                self.log.info(
                    f"next_open_balance_ok {asset} venue={venue} account={account.id} "
                    f"free_usdt={free} required={self.notional}",
                )

    # 按 Instrument venue 匹配 NT 账户。
    def _account_for_venue(self, venue: str):
        venue_text = venue.upper()
        for account in self.cache.accounts():
            if str(account.id).upper().startswith(venue_text):
                return account
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

    # 下单前检查失败时，不提交订单，直接停止。
    def _fail_before_submit(self, asset: str, reason: str) -> None:
        self.log.error(f"strategy_stop {asset} reason={reason}")
        self._flatten_positions(asset, "open_check_failed")
        self.arb_state = STOPPING
        self.active_asset = asset
        self._request_stop(reason)

    # 策略停止时按内部持仓记录提交反向市价单。
    def _flatten_on_stop(self) -> None:
        if self.config.dry_run or not self.positions:
            return
        if any(batch.action == CLOSE for batch in self.pending.values()):
            self.log.warning("stop_flatten_skipped reason=close_batch_pending")
            return
        for asset in list(self.positions):
            self._flatten_positions(asset, "strategy_stop")

    def _flatten_positions(self, asset: str, reason: str) -> None:
        if self.config.dry_run:
            return
        lots = self.positions.get(asset, [])
        if not lots:
            return
        self.log.warning(f"flatten_positions {asset} reason={reason} lots={len(lots)}")
        for lot in lots:
            self._try_submit_emergency(lot.buy_id, OrderSide.SELL, lot.buy_qty)
            self._try_submit_emergency(lot.sell_id, OrderSide.BUY, lot.sell_qty)

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

    def _leg_filled(self, leg: PendingLeg) -> bool:
        return leg.filled_qty >= leg.target_qty

    # 只有所有腿都累计全量成交后，才确认开仓或平仓。
    def _batch_filled(self, batch: PendingBatch) -> bool:
        return all(self._leg_filled(leg) for leg in batch.legs.values())

    def _confirm_open(self, batch: PendingBatch) -> None:
        buy_qty = self._filled_qty(batch, batch.buy_id)
        sell_qty = self._filled_qty(batch, batch.sell_id)
        self.positions.setdefault(batch.asset, []).append(ArbPos(
            buy_id=batch.buy_id,
            sell_id=batch.sell_id,
            buy_px=batch.buy_px,
            sell_px=batch.sell_px,
            buy_qty=buy_qty,
            sell_qty=sell_qty,
            edge_bps=batch.edge_bps,
            mean_bps=batch.mean_bps,
            std_bps=batch.std_bps,
            z_score=batch.z_score,
            opened_ns=batch.created_ns,
        ))
        self.arb_state = PAIRED
        self.active_asset = batch.asset
        self._clear_pending(batch)
        if buy_qty != sell_qty:
            self.log.warning(f"filled_qty_mismatch {batch.asset} buy_qty={buy_qty} sell_qty={sell_qty}")
        self.log.info(
            f"state {batch.asset} {OPENING}->{PAIRED} qty={buy_qty} "
            f"lots={len(self.positions[batch.asset])} next_z={self._next_entry_z(batch.asset):.2f}",
        )
        self._log_next_notional(batch.asset)

    def _confirm_close(self, batch: PendingBatch) -> None:
        self.positions.pop(batch.asset, None)
        self.arb_state = FLAT
        self.active_asset = None
        self._clear_pending(batch)
        self.log.info(f"state {batch.asset} {CLOSING}->{FLAT}")
        self._log_next_notional(batch.asset)

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
        if asset in self.failed_assets:
            self.log.error(f"order_failed_after_stop {asset} action={batch.action} order={order_id} reason={reason}")
            return
        self.log.error(f"order_failed {asset} action={batch.action} order={order_id} reason={reason}")
        self._fail_batch(batch, reason)

    # 任一腿失败后只做平仓和停机，不再回到 FLAT 继续开仓。
    def _fail_batch(self, batch: PendingBatch, reason: str) -> None:
        self.failed_assets.add(batch.asset)
        if batch.action == OPEN:
            self._flatten_positions(batch.asset, "order_failed")
        self._emergency_flatten(batch)
        self.positions.pop(batch.asset, None)
        self.arb_state = STOPPING
        self.active_asset = batch.asset
        self._request_stop(f"preipo order failed: {reason}")

    # 请求 live 入口停止整个 node，保证 finally 仍能写报告。
    def _request_stop(self, reason: str) -> None:
        self.stopped = True
        if self.stop_requested:
            return
        self.stop_requested = True
        self.log.error(f"strategy_stop reason={reason}")
        self.msgbus.publish(NODE_STOP_TOPIC, {"source": "preipo_arb", "reason": reason})

    def _emergency_flatten(self, batch: PendingBatch) -> None:
        if self.config.dry_run:
            return
        if batch.action == OPEN:
            for leg in batch.legs.values():
                if leg.filled_qty > 0:
                    self._try_submit_emergency(leg.instrument_id, self._opposite(leg.side), leg.filled_qty)
            return

        for leg in batch.legs.values():
            remaining = leg.target_qty - leg.filled_qty
            if remaining > 0:
                self._try_submit_emergency(leg.instrument_id, leg.side, remaining)

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
            self.order_asset.pop(order_id, None)
        self.pending.pop(batch.asset, None)

    def _asset_state(self, asset: str) -> str:
        if self.active_asset is None or self.active_asset == asset:
            return self.arb_state
        return FLAT

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
            asset_states = self.last_spread_states.get(asset, current_states)
            best = self._best(asset)
            asset_state = self._asset_state(asset)
            active = "Y" if self.active_asset == asset else "N"
            pending = "Y" if asset in self.pending else "N"
            lots = self.positions.get(asset, [])
            position_rows.append(self._position_snapshot(asset, lots, asset_states, now_ns))
            state_rows.append({
                "asset": asset,
                "state": asset_state,
                "active": active,
                "pending": pending,
                "has_position": str(len(lots)) if lots else "N",
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
                market_tables[asset] = self._market_rows(asset, asset_states)
                log_parts.append(f"{asset} state={asset_state} active={active} quotes=waiting")
                continue
            buy, sell = best
            edge = self._route_edge_bps(asset, buy, sell)
            if edge is None:
                continue
            state = asset_states.get((buy.instrument_id, sell.instrument_id))
            if state is None:
                samples, mean, std, window_sec = self._window_debug(asset, buy.instrument_id, sell.instrument_id)
                mean = self.initial_mean_bps.get(asset, mean)
                std = self.initial_std_bps.get(asset, std)
                z_score = (float(edge) - mean) / std if std > 0 else 0.0
            else:
                samples = state.samples
                mean = state.mean_bps
                std = state.std_bps
                z_score = state.z_score
                window_sec = state.window_sec
            quotes = self._quote_text(asset)
            rows[asset] = {
                "asset": asset,
                "state": asset_state,
                "active": active,
                "pending": pending,
                "buy": f"{buy.instrument_id}@{self._fmt(buy.price)}",
                "sell": f"{sell.instrument_id}@{self._fmt(sell.price)}",
                "edge": self._fmt(edge),
                "z": self._fmt(z_score),
                "mean": self._fmt(mean),
                "std": self._fmt(std),
                "samples": str(samples),
                "window": self._fmt(window_sec, "s"),
                "quotes": quotes,
            }
            market_tables[asset] = self._market_rows(
                asset,
                asset_states,
            )
            log_parts.append(
                f"{asset} state={asset_state} active={active} pending={asset in self.pending} "
                f"edge={edge:.2f}bps z={z_score:.2f} buy={buy.instrument_id}@{buy.price} "
                f"sell={sell.instrument_id}@{sell.price} quotes=[{quotes}]",
            )
        if self.snapshot_display == "log":
            self.log.info("market_snapshot | " + " | ".join(log_parts))
            return
        with self.snapshot_lock:
            self.snapshot_rows.update(rows)
            self.snapshot_rows["__market_tables__"] = market_tables
            self.snapshot_rows["__position_rows__"] = position_rows
            self.snapshot_rows["__state_rows__"] = state_rows
            self.snapshot_rows["__beijing_time__"] = self._beijing_time(now_ns)

    def _market_rows(
        self,
        asset: str,
        states: dict[tuple[InstrumentId, InstrumentId], SpreadState],
    ) -> list[dict[str, str]]:
        now_ns = self.clock.timestamp_ns()
        instrument_ids = [
            instrument_id
            for instrument_id in self.instruments
            if self._asset(instrument_id) == asset
        ]
        instrument_ids.sort(key=lambda item: (0 if self._venue(item) == "BINANCE" else 1, self._venue(item)))
        venues = [self._venue(instrument_id) for instrument_id in instrument_ids]
        rows = [{"metric": metric, **{venue: "-" for venue in venues}} for metric in (
            "bid",
            "ask",
            "age",
            "open_edge",
            "close_edge",
            "mean",
            "std",
            "z",
        )]
        by_metric = {row["metric"]: row for row in rows}

        binance_id = next((instrument_id for instrument_id in instrument_ids if self._venue(instrument_id) == "BINANCE"), None)
        binance_quote = self.quotes.get(binance_id) if binance_id is not None else None
        if binance_quote is None:
            return rows
        binance_bid = Decimal(str(binance_quote.bid_price))
        binance_ask = Decimal(str(binance_quote.ask_price))
        binance_mid = (binance_bid + binance_ask) / Decimal("2")

        for instrument_id in instrument_ids:
            venue = self._venue(instrument_id)
            quote = self.quotes.get(instrument_id)
            if quote is None:
                continue
            bid = Decimal(str(quote.bid_price))
            ask = Decimal(str(quote.ask_price))
            by_metric["bid"][venue] = self._fmt(bid)
            by_metric["ask"][venue] = self._fmt(ask)
            by_metric["age"][venue] = self._fmt((now_ns - quote.ts_event) / 1_000_000_000, "s")
            if venue == "BINANCE":
                spread = self._edge_bps(binance_ask - binance_bid, binance_mid)
                by_metric["open_edge"][venue] = f"spread {self._fmt(spread)}" if spread is not None else "-"
                continue

            buy_binance = ArbLeg(binance_id, binance_ask)
            sell_binance = ArbLeg(binance_id, binance_bid)
            buy_other = ArbLeg(instrument_id, ask)
            sell_other = ArbLeg(instrument_id, bid)
            edge_a = self._route_edge_bps(asset, buy_binance, sell_other)
            edge_b = self._route_edge_bps(asset, buy_other, sell_binance)
            if edge_a is None and edge_b is None:
                continue
            if edge_b is None or (edge_a is not None and edge_a >= edge_b):
                open_edge = edge_a
                close_edge = self._close_edge_bps(asset, sell_binance, buy_other)
                state = states.get((buy_binance.instrument_id, sell_other.instrument_id))
            else:
                open_edge = edge_b
                close_edge = self._close_edge_bps(asset, sell_other, buy_binance)
                state = states.get((buy_other.instrument_id, sell_binance.instrument_id))
            by_metric["open_edge"][venue] = self._fmt(open_edge)
            if close_edge is not None:
                by_metric["close_edge"][venue] = self._fmt(close_edge)
            if state is not None:
                by_metric["mean"][venue] = self._fmt(state.mean_bps)
                by_metric["std"][venue] = self._fmt(state.std_bps)
                by_metric["z"][venue] = self._fmt(state.z_score)
        return rows

    def _position_snapshot(
        self,
        asset: str,
        lots: list[ArbPos],
        states: dict[tuple[InstrumentId, InstrumentId], SpreadState],
        now_ns: int,
    ) -> dict[str, str]:
        if not lots:
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
        position = lots[0]
        buy_quote = self.quotes.get(position.buy_id)
        sell_quote = self.quotes.get(position.sell_id)
        close_edge = "-"
        current_z = "-"
        if buy_quote is not None and sell_quote is not None:
            close_sell = ArbLeg(position.buy_id, Decimal(str(buy_quote.bid_price)))
            close_buy = ArbLeg(position.sell_id, Decimal(str(sell_quote.ask_price)))
            edge = self._close_edge_bps(asset, close_sell, close_buy)
            if edge is not None:
                close_edge = self._fmt(edge)
                state = states.get((position.buy_id, position.sell_id))
                if state is not None and state.std_bps > 0:
                    current_z = self._fmt((float(edge) - state.mean_bps) / state.std_bps)
        hold_sec = max((now_ns - min(lot.opened_ns for lot in lots)) / 1_000_000_000, 0.0)
        buy_qty = sum((lot.buy_qty for lot in lots), Decimal("0"))
        sell_qty = sum((lot.sell_qty for lot in lots), Decimal("0"))
        entry_edge = sum((lot.edge_bps for lot in lots), Decimal("0")) / Decimal(len(lots))
        entry_z = sum(lot.z_score for lot in lots) / len(lots)
        return {
            "asset": asset,
            "side": f"lots={len(lots)} buy_{self._venue(position.buy_id).lower()}_sell_{self._venue(position.sell_id).lower()}",
            "entry_buy": f"{position.buy_id}@{self._fmt(position.buy_px)}",
            "entry_sell": f"{position.sell_id}@{self._fmt(position.sell_px)}",
            "qty": f"{self._fmt(buy_qty)}/{self._fmt(sell_qty)}",
            "entry_edge": self._fmt(entry_edge),
            "entry_z": self._fmt(entry_z),
            "current_close_edge": close_edge,
            "current_z": current_z,
            "hold": self._fmt(hold_sec, "s"),
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
            rows.append(
                f"{self._venue(instrument_id)} bid={self._fmt(quote.bid_price)} "
                f"ask={self._fmt(quote.ask_price)} age={self._fmt(age_sec, 's')}",
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
        title = f"PREIPO Arbitrage Live | 北京时间 {rows.get('__beijing_time__') or self._beijing_time()}"
        table = Table(title=title, expand=True)
        for column, justify in (
            ("asset", "left"),
            ("state", "left"),
            ("active", "center"),
            ("pending", "center"),
            ("edge", "right"),
            ("z", "right"),
            ("mean", "right"),
            ("std", "right"),
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
                table.add_row(asset, "FLAT", "N", "N", "-", "-", "-", "-", "0", "0s", "-", "-", "waiting")
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
                row["samples"],
                row["window"],
                row["buy"],
                row["sell"],
                row["quotes"],
            )
        return table
