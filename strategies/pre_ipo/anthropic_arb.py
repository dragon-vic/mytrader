from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from adapters.external_command import ExternalCommand
from adapters.external_command import external_command_type
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data import CustomData
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
from utils.arguments import NODE_STOP_TOPIC


BPS = Decimal("10000")
MINUTE_NS = 60_000_000_000
ASSET = "ANTHROPIC"
LONG_EDGE = "long_edge"
SHORT_EDGE = "short_edge"
BEIJING_TZ = timezone(timedelta(hours=8))
COLLECTOR_COLUMNS = ("ts_local_ns", "ts_exchange_ms", "venue", "symbol", "bid", "ask", "bid_size", "ask_size")
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
    filled_notional: Decimal = Decimal("0")
    submit_ns: int | None = None
    fill_event_ns: int | None = None
    failed: bool = False

    def filled(self) -> bool:
        return self.filled_qty >= self.target_qty

    def done(self) -> bool:
        return self.failed or self.filled()


@dataclass
class PendingPair:
    legs: dict[str, PendingLeg]
    signal: str
    edge_side: str
    signal_edge_bps: Decimal
    mean_bps: Decimal
    created_ns: int
    before_inventory: Decimal
    after_inventory: Decimal
    repair: PendingLeg | None = None

    def record_fill(self, order_id: str, qty: Decimal, px: Decimal, event_ns: int) -> None:
        leg = self.leg(order_id)
        leg.filled_qty += qty
        leg.filled_notional += qty * px
        leg.fill_event_ns = max(leg.fill_event_ns or 0, event_ns)

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

    def avg_px(self, instrument_id: InstrumentId) -> Decimal | None:
        for leg in self.legs.values():
            if leg.instrument_id == instrument_id and leg.filled_qty > 0:
                return leg.filled_notional / leg.filled_qty
        return None


@dataclass
class EdgePair:
    window_ns: int
    okx_price_multiplier: Decimal
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
        okx_bid = Decimal(str(okx.bid_price)) * self.okx_price_multiplier
        okx_ask = Decimal(str(okx.ask_price)) * self.okx_price_multiplier
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
        okx_bid *= self.okx_price_multiplier
        okx_ask *= self.okx_price_multiplier
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

    def warm_from_rows(self, rows: list[dict[str, object]], end_ns: int, binance_id: InstrumentId, okx_id: InstrumentId) -> None:
        minute_prices: dict[int, dict[str, tuple[Decimal, Decimal, int]]] = {}
        for row in rows:
            minute_ns = int(row["ts_local_ns"]) // MINUTE_NS * MINUTE_NS
            venue = str(row["venue"]).upper()
            item = minute_prices.setdefault(minute_ns, {})
            bid = Decimal(str(row["bid"]))
            ask = Decimal(str(row["ask"]))
            old = item.get(venue)
            if old is None:
                item[venue] = bid, ask, 1
            else:
                item[venue] = old[0] + bid, old[1] + ask, old[2] + 1
        last: dict[str, tuple[Decimal, Decimal]] = {}
        minute_ns = min(minute_prices)
        end_minute_ns = end_ns // MINUTE_NS * MINUTE_NS
        while minute_ns <= end_minute_ns:
            for venue, (bid_sum, ask_sum, count) in minute_prices.get(minute_ns, {}).items():
                last[venue] = bid_sum / Decimal(count), ask_sum / Decimal(count)
            if "BINANCE" in last and "OKX" in last:
                bn_bid, bn_ask = last["BINANCE"]
                okx_bid, okx_ask = last["OKX"]
                long_bps, short_bps = self.from_prices(
                    bn_bid,
                    bn_ask,
                    okx_bid * self.okx_price_multiplier,
                    okx_ask * self.okx_price_multiplier,
                )
                self._add_value(self.long_values, minute_ns, long_bps)
                self._add_value(self.short_values, minute_ns, short_bps)
            minute_ns += MINUTE_NS
        if not self.long_values or not self.short_values:
            return
        self.update_mean(self._mean(self.long_values), self._mean(self.short_values))
        self.last_price_means[binance_id] = last["BINANCE"]
        self.last_price_means[okx_id] = last["OKX"]
        if self.minute_ns is None:
            self.minute_ns = end_minute_ns

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
    okx_price_multiplier: Decimal
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
            okx_price_multiplier=config.okx_price_multiplier,
            long_mean_bps=Decimal("0"),
            short_mean_bps=Decimal("0"),
            entry_bps=config.entry_bps,
            exit_bps=config.exit_bps,
            long_max_bps=config.long_max_bps,
            short_min_bps=config.short_min_bps,
        )
        self.qty = config.qty
        self.okx_qty_multiplier = config.okx_qty_multiplier
        self.trade_state = STATE_FLAT
        self.reduce_mode = False
        self.pending: PendingPair | None = None
        self.housekeeping_seq = 0
        self.action_rows: deque[dict[str, str]] = deque(maxlen=200)

    # 策略启动入口，后续逐步补订阅、warmup 和状态初始化。
    def on_start(self) -> None:
        self._warm_initial_window()
        for instrument_id in self.instruments:
            self.subscribe_quote_ticks(instrument_id)
        self.subscribe_data(external_command_type())
        self._schedule_housekeeping()
        self._maybe_write_snapshot(self.clock.timestamp_ns())

    # 策略停止入口，后续补充清理和退出状态写入。
    def on_stop(self) -> None:
        self._flatten_on_stop()
        self.unsubscribe_data(external_command_type())
        for instrument_id in self.instruments:
            self.unsubscribe_quote_ticks(instrument_id)

    def on_data(self, data) -> None:
        command = self._external_command(data)
        if command is None:
            return
        name = command.command.strip().lower()
        if name == "stop":
            self.log.warning(f"external_command_stop source={command.source} reason={command.reason}")
            self._flatten_on_stop()
            self.msgbus.publish(NODE_STOP_TOPIC, {"source": "anthropic_arb", "reason": command.reason or "monitor"})
            return
        if name == "reduce":
            self.reduce_mode = True
            self.log.warning(f"reduce_mode_on source={command.source}")
            return
        if name == "resume":
            self.reduce_mode = False
            self.log.warning(f"reduce_mode_off source={command.source}")
            return
        self.log.warning(f"external_command_ignored command={command.command} source={command.source}")

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.quotes[tick.instrument_id] = tick
        self.edge.record_quote(tick, self.instruments[0], self.instruments[1])
        self._update_edges()
        if self.trade_state not in {STATE_PENDING, STATE_FAIL}:
            signal = self._signal_determination()
            if signal is not None and self._signal_allowed(signal):
                self._submit_signal(signal)

    def on_order_filled(self, event: OrderFilled) -> None:
        order_id = str(event.client_order_id)
        if self.pending is None or not self.pending.has_order(order_id):
            return
        self.pending.record_fill(
            order_id,
            Decimal(str(event.last_qty)),
            Decimal(str(event.last_px).split()[0].replace("_", "")),
            int(event.ts_event),
        )
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
        return self.edge.signal(self.trade_state)

    def _signal_allowed(self, signal: str) -> bool:
        if not self.reduce_mode:
            return True
        if self.trade_state == STATE_LONG:
            return signal == "short"
        if self.trade_state == STATE_SHORT:
            return signal == "long"
        return False

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
            "strategy": "anthropic_arb",
            "strategy_state": self._strategy_state(),
            "assets": [ASSET],
            "rows": [],
            "market_tables": {ASSET: self._market_rows(now_ns)},
            "action_rows": list(self.action_rows) + self._pending_rows(now_ns),
            "summary": {ASSET: self._summary_row()},
            "risk": self._risk_rows(),
            "inventories": {ASSET: self._inventory()},
        }
        tmp = self.snapshot_path.with_suffix(f"{self.snapshot_path.suffix}.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.snapshot_path)

    def _submit_signal(self, signal: str) -> None:
        before_inventory = self._inventory()
        self.trade_state = STATE_PENDING
        if signal == "long":
            buy_id, sell_id = self.instruments[1], self.instruments[0]
            buy_qty, sell_qty = self.qty * self.okx_qty_multiplier, self.qty
            edge_side = LONG_EDGE
            signal_edge = self.edge.long_bps
            mean_bps = self.edge.long_mean_bps
            after_inventory = before_inventory + self.qty
        else:
            buy_id, sell_id = self.instruments[0], self.instruments[1]
            buy_qty, sell_qty = self.qty, self.qty * self.okx_qty_multiplier
            edge_side = SHORT_EDGE
            signal_edge = self.edge.short_bps
            mean_bps = self.edge.short_mean_bps
            after_inventory = before_inventory - self.qty

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
                submit_ns=self.clock.timestamp_ns(),
            ),
            str(sell_order.client_order_id): PendingLeg(
                order_id=str(sell_order.client_order_id),
                instrument_id=sell_id,
                side=OrderSide.SELL,
                target_qty=Decimal(str(sell_quantity)),
                submit_ns=self.clock.timestamp_ns(),
            ),
        }
        self.pending = PendingPair(
            legs=legs,
            signal=signal,
            edge_side=edge_side,
            signal_edge_bps=signal_edge,
            mean_bps=mean_bps,
            created_ns=self.clock.timestamp_ns(),
            before_inventory=before_inventory,
            after_inventory=after_inventory,
        )
        self.submit_order(buy_order)
        self.submit_order(sell_order)

    def _sync_state_from_inventory(self) -> None:
        positions = self.cache.positions_open(instrument_id=self.instruments[0], strategy_id=self.id)
        if not positions:
            self.trade_state = STATE_FLAT
            return
        position = positions[0]
        self.trade_state = STATE_SHORT if bool(position.is_long) else STATE_LONG

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
            self._record_action(pending, "filled")
            self.pending = None
            self._sync_state_from_inventory()
            return
        if pending.is_all_failed():
            self.log.warning(f"pending_pair_all_failed orders={','.join(pending.legs)}")
            self.pending = None
            self._sync_state_from_inventory()
            return
        self.trade_state = STATE_FAIL
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

    def _external_command(self, data) -> ExternalCommand | None:
        if isinstance(data, ExternalCommand):
            return data
        if isinstance(data, CustomData) and isinstance(data.data, ExternalCommand):
            return data.data
        return None

    # 启动时用 collector 真实 bid/ask 初始化 6h 分钟窗口。
    def _warm_initial_window(self) -> None:
        end_ns = time.time_ns()
        start_ns = end_ns - self.edge.window_ns
        paths = self._collector_quote_files(start_ns, end_ns)
        rows = self._load_collector_quotes(paths, start_ns, end_ns)
        self.edge.warm_from_rows(rows, end_ns, self.instruments[0], self.instruments[1])
        self._seed_initial_quotes(rows)
        self.log.info(
            f"initial_window asset={ASSET} rows={len(rows)} long_mean={self.edge.long_mean_bps:.2f} "
            f"short_mean={self.edge.short_mean_bps:.2f}",
        )

    def _collector_quote_files(self, start_ns: int, end_ns: int) -> list[Path]:
        base_dir = Path(__file__).resolve().parent / "collector" / "bidask1-live"
        merged_dir = base_dir / "quote_merged"
        raw_dir = base_dir / "quote_raw"
        paths: list[Path] = []
        for key in self._collector_hour_keys(start_ns, end_ns):
            merged = merged_dir / ASSET / f"bidask1-{key}.parquet"
            if merged.exists():
                paths.append(merged)
            hour_dir = raw_dir / ASSET / key
            if hour_dir.exists():
                paths.extend(sorted(hour_dir.glob("*.parquet")))
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

    def _load_collector_quotes(self, paths: list[Path], start_ns: int, end_ns: int) -> list[dict[str, object]]:
        if not paths:
            raise RuntimeError("no bidask1 collector parquet files found for initial window")
        dataset = ds.dataset([str(path) for path in paths], format="parquet")
        filt = (
            (pc.field("ts_local_ns") >= pa.scalar(start_ns, pa.int64()))
            & (pc.field("ts_local_ns") <= pa.scalar(end_ns, pa.int64()))
            & pc.field("symbol").isin([ASSET])
        )
        table = dataset.to_table(columns=list(COLLECTOR_COLUMNS), filter=filt)
        rows = table.to_pylist()
        if not rows:
            raise RuntimeError("no bidask1 collector rows found for initial window")
        return sorted(rows, key=lambda row: int(row["ts_local_ns"]))

    def _seed_initial_quotes(self, rows: list[dict[str, object]]) -> None:
        latest: dict[str, dict[str, object]] = {}
        for row in rows:
            latest[str(row["venue"]).upper()] = row
        ts_init = self.clock.timestamp_ns()
        for instrument_id in self.instruments:
            venue = self._venue(instrument_id)
            row = latest[venue]
            instrument = self.cache.instrument(instrument_id)
            bid_size = Decimal(str(row["bid_size"]))
            ask_size = Decimal(str(row["ask_size"]))
            ts_event = int(row["ts_exchange_ms"]) * 1_000_000 if int(row["ts_exchange_ms"]) > 0 else int(row["ts_local_ns"])
            self.quotes[instrument_id] = QuoteTick(
                instrument_id=instrument_id,
                bid_price=instrument.make_price(Decimal(str(row["bid"]))),
                ask_price=instrument.make_price(Decimal(str(row["ask"]))),
                bid_size=instrument.make_qty(bid_size if bid_size > 0 else self.qty),
                ask_size=instrument.make_qty(ask_size if ask_size > 0 else self.qty),
                ts_event=ts_event,
                ts_init=ts_init,
            )
        self._update_edges()

    def _flatten_on_stop(self) -> None:
        for position in self._strategy_open_positions():
            side = OrderSide.SELL if bool(position.is_long) else OrderSide.BUY
            qty = Decimal(str(position.quantity))
            self.log.warning(f"flatten_on_stop instrument={position.instrument_id} side={side} qty={qty}")
            self._submit_emergency(position.instrument_id, side, qty)

    def _submit_emergency(self, instrument_id: InstrumentId, side: OrderSide, qty: Decimal) -> None:
        quantity = self.cache.instrument(instrument_id).make_qty(qty)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

    def _strategy_open_positions(self) -> list[object]:
        result = []
        for instrument_id in self.instruments:
            result.extend(self.cache.positions_open(instrument_id=instrument_id, strategy_id=self.id))
        return result

    def _inventory(self) -> Decimal:
        return -self._net_qty(self.instruments[0])

    def _strategy_state(self) -> str:
        if self.reduce_mode:
            return f"{self.trade_state}:reduce"
        return self.trade_state

    def _market_rows(self, now_ns: int) -> list[dict[str, str]]:
        venues = [self._venue(instrument_id) for instrument_id in self.instruments]
        rows = [{"metric": metric, **{venue: "-" for venue in venues}} for metric in (
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
        )]
        by_metric = {row["metric"]: row for row in rows}
        for instrument_id in self.instruments:
            venue = self._venue(instrument_id)
            quote = self.quotes.get(instrument_id)
            if quote is None:
                continue
            bid = Decimal(str(quote.bid_price))
            ask = Decimal(str(quote.ask_price))
            mid = (bid + ask) / Decimal("2")
            by_metric["bid"][venue] = self._fmt(bid)
            by_metric["ask"][venue] = self._fmt(ask)
            by_metric["age"][venue] = self._fmt((now_ns - quote.ts_event) / 1_000_000_000, "s")
            by_metric["spread_bps"][venue] = self._fmt((ask - bid) / mid * BPS)
        by_metric["long_edge"]["OKX"] = self._fmt(self.edge.long_bps)
        by_metric["long_mean"]["OKX"] = self._fmt(self.edge.long_mean_bps)
        by_metric["short_edge"]["OKX"] = self._fmt(self.edge.short_bps)
        by_metric["short_mean"]["OKX"] = self._fmt(self.edge.short_mean_bps)
        return rows

    def _pending_rows(self, now_ns: int) -> list[dict[str, str]]:
        if self.pending is None:
            return []
        row = self._action_row(self.pending, "pending")
        row["age_min"] = self._fmt(max((now_ns - self.pending.created_ns) / 60_000_000_000, 0.0))
        return [row]

    def _record_action(self, pending: PendingPair, status: str) -> None:
        self.action_rows.appendleft(self._action_row(pending, status))

    def _action_row(self, pending: PendingPair, status: str) -> dict[str, str]:
        actual_edge = self._actual_edge_bps(pending)
        latency_rows = self._latency_rows(pending)
        return {
            "created_ns": str(pending.created_ns),
            "asset": ASSET,
            "action": self._display_action(pending.before_inventory, pending.after_inventory),
            "edge_side": pending.edge_side,
            "status": status,
            "qty": self._fmt(abs(pending.after_inventory)),
            "signal_edge": self._fmt(pending.signal_edge_bps),
            "actual_edge": self._fmt(actual_edge),
            "edge_slippage": self._fmt(self._edge_slippage(pending.edge_side, pending.signal_edge_bps, actual_edge)),
            "fill_slippage": "-",
            "mean": self._fmt(pending.mean_bps),
            "std": "-",
            "bn_latency": latency_rows["BINANCE"],
            "okx_latency": latency_rows["OKX"],
            "time": self._beijing_time_short(pending.created_ns),
        }

    def _actual_edge_bps(self, pending: PendingPair) -> Decimal | None:
        buy_id = self.instruments[1] if pending.signal == "long" else self.instruments[0]
        sell_id = self.instruments[0] if pending.signal == "long" else self.instruments[1]
        buy_avg = pending.avg_px(buy_id)
        sell_avg = pending.avg_px(sell_id)
        if buy_avg is None or sell_avg is None:
            return None
        if buy_id == self.instruments[1]:
            buy_avg *= self.edge.okx_price_multiplier
        if sell_id == self.instruments[1]:
            sell_avg *= self.edge.okx_price_multiplier
        if pending.edge_side == SHORT_EDGE:
            return (sell_avg - buy_avg) / buy_avg * BPS
        return (buy_avg - sell_avg) / sell_avg * BPS

    def _edge_slippage(self, edge_side: str, signal_edge: Decimal, actual_edge: Decimal | None) -> Decimal | None:
        if actual_edge is None:
            return None
        if edge_side == SHORT_EDGE:
            return actual_edge - signal_edge
        return signal_edge - actual_edge

    def _latency_rows(self, pending: PendingPair) -> dict[str, str]:
        rows = {"BINANCE": "-", "OKX": "-"}
        for leg in pending.legs.values():
            venue = self._venue(leg.instrument_id)
            if leg.submit_ns is not None and leg.fill_event_ns is not None:
                rows[venue] = self._fmt((leg.fill_event_ns - leg.submit_ns) / 1_000_000)
        return rows

    def _summary_row(self) -> dict[str, str]:
        return {
            "inventory": str(self._inventory()),
            "realized_usdt": "-",
            "unrealized_usdt": self._fmt(self._unrealized_pnl_usdt()),
            "realized_bps": "-",
            "unrealized_bps": "-",
            "total_bps": "-",
        }

    def _risk_rows(self) -> dict[str, dict[str, str]]:
        rows = {}
        for venue in sorted({self._venue(instrument_id) for instrument_id in self.instruments}):
            account = self._account_for_venue(venue)
            wallet = self._total_usdt(account) if account is not None else Decimal("0")
            unrealized = sum(
                (pnl for pnl in (self._position_unrealized_pnl(position) for position in self._strategy_open_positions() if self._venue(position.instrument_id) == venue) if pnl is not None),
                Decimal("0"),
            )
            rate = abs(unrealized) / wallet * Decimal("100") if wallet > 0 and unrealized < 0 else Decimal("0")
            rows[venue] = {
                "wallet_usdt": self._fmt(wallet),
                "unrealized_usdt": self._fmt(unrealized),
                "risk_rate": self._fmt(rate, "%") if wallet > 0 else "-",
                "positions": str(len([position for position in self._strategy_open_positions() if self._venue(position.instrument_id) == venue])),
                "status": "OK",
            }
        return rows

    def _unrealized_pnl_usdt(self) -> Decimal:
        return sum((pnl for pnl in (self._position_unrealized_pnl(position) for position in self._strategy_open_positions()) if pnl is not None), Decimal("0"))

    def _position_unrealized_pnl(self, position: object) -> Decimal | None:
        quote = self.quotes.get(position.instrument_id)
        if quote is None:
            return None
        exit_px = Decimal(str(quote.bid_price if bool(position.is_long) else quote.ask_price))
        avg_px = Decimal(str(position.avg_px_open))
        qty = Decimal(str(position.quantity))
        return (exit_px - avg_px) * qty if bool(position.is_long) else (avg_px - exit_px) * qty

    def _account_for_venue(self, venue: str):
        venue_text = venue.upper()
        for account in self.cache.accounts():
            if str(account.id).upper().startswith(venue_text):
                return account
        return None

    def _total_usdt(self, account) -> Decimal:
        money = account.balance_total(USDT)
        return Decimal(str(money.as_decimal())) if hasattr(money, "as_decimal") else Decimal(str(money))

    def _venue(self, instrument_id: InstrumentId) -> str:
        return str(instrument_id.venue).upper()

    def _display_action(self, before: Decimal, after: Decimal) -> str:
        if before == 0 and after != 0:
            return "open"
        if before != 0 and after == 0:
            return "close"
        if abs(after) > abs(before):
            return "add"
        if abs(after) < abs(before):
            return "reduce"
        return "-"

    def _fmt(self, value: object, suffix: str = "") -> str:
        if value is None:
            return "-"
        return f"{Decimal(str(value)):.2f}{suffix}"

    def _beijing_time_short(self, ts_ns: int) -> str:
        ts_sec = ts_ns / 1_000_000_000
        return datetime.fromtimestamp(ts_sec, tz=timezone.utc).astimezone(BEIJING_TZ).strftime("%m-%d %H:%M:%S")
