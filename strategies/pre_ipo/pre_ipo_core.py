from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId


BPS = Decimal("10000")
MINUTE_NS = 60_000_000_000
BEIJING_TZ = timezone(timedelta(hours=8))
COLLECTOR_COLUMNS = ("ts_local_ns", "ts_exchange_ms", "venue", "symbol", "bid", "ask", "bid_size", "ask_size")
BINANCE_EXIT_SLIPPAGE_BPS = Decimal("4")
OKX_EXIT_SLIPPAGE_BPS = Decimal("10")
EXIT_FEE_BPS = Decimal("5")
LONG_EDGE = "long_edge"
SHORT_EDGE = "short_edge"
STATE_FLAT = "flat"
STATE_PENDING = "pending"
STATE_LONG = "long"
STATE_SHORT = "short"
STATE_UNBALANCE = "unbalance"


@dataclass(frozen=True)
class VenueMetrics:
    instrument_id: InstrumentId
    qty: Decimal
    avg_px: Decimal | None
    realized_usdt: Decimal
    unrealized_usdt: Decimal | None
    fee_usdt: Decimal
    locked_usdt: Decimal | None


@dataclass(frozen=True)
class StrategyMetrics:
    venues: dict[str, VenueMetrics]
    realized_usdt: Decimal
    unrealized_usdt: Decimal | None
    fee_usdt: Decimal


@dataclass(frozen=True)
class WarmupLoader:
    base_dir: Path
    asset: str

    @staticmethod
    def event_ns(row: dict[str, object]) -> int:
        return int(row["ts_exchange_ms"]) * 1_000_000

    def _hour_keys(self, start_ns: int, end_ns: int) -> list[str]:
        start = datetime.fromtimestamp(start_ns / 1_000_000_000, BEIJING_TZ).replace(
            minute=0,
            second=0,
            microsecond=0,
        )
        end = datetime.fromtimestamp(end_ns / 1_000_000_000, BEIJING_TZ).replace(
            minute=0,
            second=0,
            microsecond=0,
        )
        keys = []
        current = start
        while current <= end:
            keys.append(current.strftime("%Y%m%d%H"))
            current += timedelta(hours=1)
        return keys

    # 从 collector 的 merged/raw 文件读取当前 asset 的启动窗口。
    def load(self, start_ns: int, end_ns: int) -> list[dict[str, object]]:
        merged_dir = self.base_dir / "quote_merged"
        raw_dir = self.base_dir / "quote_raw"
        paths: list[Path] = []
        for key in self._hour_keys(start_ns, end_ns):
            merged = merged_dir / self.asset / f"bidask1-{key}.parquet"
            if merged.exists():
                paths.append(merged)
            hour_dir = raw_dir / self.asset / key
            if hour_dir.exists():
                paths.extend(sorted(hour_dir.glob("*.parquet")))
        paths = sorted(set(paths), key=str)
        if not paths:
            raise RuntimeError(f"warmup_files_missing asset={self.asset}")

        dataset = ds.dataset([str(path) for path in paths], format="parquet")
        filt = (
            (pc.field("ts_local_ns") >= pa.scalar(start_ns, pa.int64()))
            & (pc.field("ts_local_ns") <= pa.scalar(end_ns, pa.int64()))
            & (pc.field("ts_exchange_ms") > pa.scalar(0, pa.int64()))
            & pc.field("symbol").isin([self.asset])
        )
        rows = dataset.to_table(columns=list(COLLECTOR_COLUMNS), filter=filt).to_pylist()
        if not rows:
            raise RuntimeError(f"warmup_rows_missing asset={self.asset}")
        return sorted(rows, key=self.event_ns)


@dataclass
class PendingLeg:
    order_id: str
    instrument_id: InstrumentId
    side: OrderSide
    target_qty: Decimal
    filled_qty: Decimal = Decimal("0")
    filled_notional: Decimal = Decimal("0")
    best_px: Decimal | None = None
    submit_event_ns: int | None = None
    accept_event_ns: int | None = None
    fill_event_ns: int | None = None
    full_fill_event_ns: int | None = None
    failed: bool = False

    # 单腿实际成交量达到目标量才算完成。
    def filled(self) -> bool:
        return self.filled_qty >= self.target_qty

    # fill 或失败都是最终反馈。
    def done(self) -> bool:
        return self.failed or self.filled()

    # 返回当前累计成交均价。
    def avg_px(self) -> Decimal | None:
        if self.filled_qty == 0:
            return None
        return self.filled_notional / self.filled_qty


@dataclass
class PendingPair:
    legs: dict[str, PendingLeg]
    signal: str
    edge_side: str
    signal_edge_bps: Decimal
    mean_bps: Decimal
    std_bps: Decimal
    signal_event_ns: int
    signal_ts_ns: int
    signal_venue: str
    on_quote_ns: int
    before_inventory: Decimal
    after_inventory: Decimal
    okx_price_multiplier: Decimal
    reservation_id: str | None = None
    repairs: dict[str, PendingLeg] = field(default_factory=dict)

    # 记录一笔订单的部分或完整成交。
    def record_fill(self, order_id: str, qty: Decimal, px: Decimal, event_ns: int) -> None:
        leg = self.leg(order_id)
        leg.filled_qty += qty
        leg.filled_notional += qty * px
        if leg.best_px is None:
            leg.best_px = px
        elif leg.side == OrderSide.BUY:
            leg.best_px = min(leg.best_px, px)
        else:
            leg.best_px = max(leg.best_px, px)
        leg.fill_event_ns = max(leg.fill_event_ns or 0, event_ns)
        if leg.full_fill_event_ns is None and leg.filled():
            leg.full_fill_event_ns = event_ns

    # 记录 NT 框架发出的 submitted 事件时间。
    def record_submit(self, order_id: str, event_ns: int) -> None:
        self.leg(order_id).submit_event_ns = event_ns

    # 记录 NT 框架发出的 accepted 事件时间。
    def record_accept(self, order_id: str, event_ns: int) -> None:
        self.leg(order_id).accept_event_ns = event_ns

    # 记录一笔订单的最终失败反馈。
    def record_failed(self, order_id: str) -> None:
        self.leg(order_id).failed = True

    # 主订单和修复订单都收到最终反馈后，pending 才能收口。
    def is_done(self) -> bool:
        return all(leg.done() for leg in self.legs.values()) and all(leg.done() for leg in self.repairs.values())

    # 没有修复单，且两条主订单腿都完整成交，才算正常完成。
    def is_complete(self) -> bool:
        return not self.repairs and all(leg.filled() for leg in self.legs.values())

    # 两条主订单腿都失败时，没有裸仓需要修复。
    def is_all_failed(self) -> bool:
        return self.is_done() and sum(1 for leg in self.legs.values() if leg.failed) == len(self.legs)

    # 是否已经提交过修复订单。
    def has_repairs(self) -> bool:
        return bool(self.repairs)

    # 主订单和修复订单都属于这个 pending 生命周期。
    def has_order(self, order_id: str) -> bool:
        return order_id in self.legs or order_id in self.repairs

    # 根据 order id 找 pending leg。
    def leg(self, order_id: str) -> PendingLeg:
        if order_id in self.legs:
            return self.legs[order_id]
        if order_id in self.repairs:
            return self.repairs[order_id]
        raise KeyError(order_id)

    # 获取主订单指定方向的交易腿。
    def _side_leg(self, side: OrderSide) -> PendingLeg:
        return next(leg for leg in self.legs.values() if leg.side == side)

    # 用双腿价格计算实际操作方向的 edge。
    def _edge_bps(self, buy_px: Decimal, sell_px: Decimal) -> Decimal:
        buy_leg = self._side_leg(OrderSide.BUY)
        sell_leg = self._side_leg(OrderSide.SELL)
        if str(buy_leg.instrument_id.venue).upper() == "OKX":
            buy_px *= self.okx_price_multiplier
        if str(sell_leg.instrument_id.venue).upper() == "OKX":
            sell_px *= self.okx_price_multiplier
        if self.edge_side == SHORT_EDGE:
            return (sell_px - buy_px) / buy_px * BPS
        return (buy_px - sell_px) / sell_px * BPS

    # 用双腿成交均价计算完整成交 edge。
    def actual_edge_bps(self) -> Decimal | None:
        buy_leg = self._side_leg(OrderSide.BUY)
        sell_leg = self._side_leg(OrderSide.SELL)
        buy_px = buy_leg.avg_px()
        sell_px = sell_leg.avg_px()
        if buy_px is None or sell_px is None:
            return None
        return self._edge_bps(buy_px, sell_px)

    # 用两条腿实际撮合中的最优 fill 价格计算理论最优成交 edge。
    def best_edge_bps(self) -> Decimal | None:
        buy_leg = self._side_leg(OrderSide.BUY)
        sell_leg = self._side_leg(OrderSide.SELL)
        if buy_leg.best_px is None or sell_leg.best_px is None:
            return None
        return self._edge_bps(buy_leg.best_px, sell_leg.best_px)

    # signal edge 到最优 fill edge 的变化。
    def edge_slippage_bps(self) -> Decimal | None:
        best_edge = self.best_edge_bps()
        if best_edge is None:
            return None
        if self.edge_side == SHORT_EDGE:
            return best_edge - self.signal_edge_bps
        return self.signal_edge_bps - best_edge

    # 最优 fill edge 到完整成交 edge 的变化。
    def fill_slippage_bps(self) -> Decimal | None:
        best_edge = self.best_edge_bps()
        actual_edge = self.actual_edge_bps()
        if best_edge is None or actual_edge is None:
            return None
        if self.edge_side == SHORT_EDGE:
            return actual_edge - best_edge
        return best_edge - actual_edge


@dataclass
class PnlLedger:
    signed_qty: Decimal = Decimal("0")
    avg_px: Decimal | None = None
    realized: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")

    # 从策略启动时接管的交易所仓位初始化会计基准。
    def seed_position(self, signed_qty: Decimal, avg_px: Decimal) -> None:
        self.signed_qty = signed_qty
        self.avg_px = avg_px
        self.realized = Decimal("0")
        self.fee = Decimal("0")

    # 只用本策略 fill 累计已实现盈亏和手续费。
    def record_fill(self, side: OrderSide, qty: Decimal, px: Decimal, fee: Decimal) -> None:
        fill_qty = qty if side == OrderSide.BUY else -qty
        self.fee += fee
        self.realized -= fee
        if self.signed_qty == 0 or (self.signed_qty > 0) == (fill_qty > 0):
            old_qty = abs(self.signed_qty)
            new_qty = old_qty + abs(fill_qty)
            self.avg_px = px if old_qty == 0 else (self.avg_px * old_qty + px * abs(fill_qty)) / new_qty
            self.signed_qty += fill_qty
            return

        close_qty = min(abs(self.signed_qty), abs(fill_qty))
        if self.signed_qty > 0:
            self.realized += (px - self.avg_px) * close_qty
        else:
            self.realized += (self.avg_px - px) * close_qty
        self.signed_qty += fill_qty
        if self.signed_qty == 0:
            self.avg_px = None
        elif abs(fill_qty) > close_qty:
            self.avg_px = px


@dataclass
class EdgePair:
    window_ns: int
    okx_price_multiplier: Decimal
    long_mean_bps: Decimal
    short_mean_bps: Decimal
    long_std_bps: Decimal
    short_std_bps: Decimal
    entry_bps: Decimal
    exit_bps: Decimal
    std_mult: Decimal
    long_max_bps: Decimal
    short_min_bps: Decimal
    long_bps: Decimal | None = None
    short_bps: Decimal | None = None
    long_values: deque[tuple[int, Decimal]] = field(default_factory=deque)
    short_values: deque[tuple[int, Decimal]] = field(default_factory=deque)
    quote_sums: dict[InstrumentId, tuple[Decimal, Decimal, int]] = field(default_factory=dict)
    last_price_means: dict[InstrumentId, tuple[Decimal, Decimal]] = field(default_factory=dict)
    minute_counts: deque[dict[str, int]] = field(default_factory=lambda: deque(maxlen=10))

    # 每个 quote 事件后更新当前 long/short edge。
    def update(self, binance: QuoteTick, okx: QuoteTick) -> None:
        bn_bid = binance.bid_price.as_decimal()
        bn_ask = binance.ask_price.as_decimal()
        okx_bid = okx.bid_price.as_decimal() * self.okx_price_multiplier
        okx_ask = okx.ask_price.as_decimal() * self.okx_price_multiplier
        self.long_bps, self.short_bps = self.from_prices(bn_bid, bn_ask, okx_bid, okx_ask)

    # 将 quote 累加到当前 housekeeping interval。
    def record_quote(self, tick: QuoteTick) -> None:
        bid_sum, ask_sum, count = self.quote_sums.get(tick.instrument_id, (Decimal("0"), Decimal("0"), 0))
        self.quote_sums[tick.instrument_id] = (
            bid_sum + tick.bid_price.as_decimal(),
            ask_sum + tick.ask_price.as_decimal(),
            count + 1,
        )

    # 用当前 interval 的 bid/ask 均值生成一个时间加权 edge 样本。
    def close_bucket(self, ts_ns: int, binance_id: InstrumentId, okx_id: InstrumentId) -> None:
        bucket = self.quote_sums
        self.quote_sums = {}
        counts = {
            str(binance_id): bucket.get(binance_id, (Decimal("0"), Decimal("0"), 0))[2],
            str(okx_id): bucket.get(okx_id, (Decimal("0"), Decimal("0"), 0))[2],
        }
        for instrument_id, (bid_sum, ask_sum, count) in bucket.items():
            self.last_price_means[instrument_id] = bid_sum / Decimal(count), ask_sum / Decimal(count)
        bn_bid, bn_ask = self.last_price_means[binance_id]
        okx_bid, okx_ask = self.last_price_means[okx_id]
        okx_bid *= self.okx_price_multiplier
        okx_ask *= self.okx_price_multiplier
        long_bps, short_bps = self.from_prices(bn_bid, bn_ask, okx_bid, okx_ask)
        self._add_value(self.long_values, ts_ns, long_bps)
        self._add_value(self.short_values, ts_ns, short_bps)
        self.update_stats()
        self.minute_counts.append({"minute_ns": ts_ns, **counts})

    def _add_value(self, values: deque[tuple[int, Decimal]], ts_ns: int, value: Decimal) -> None:
        values.append((ts_ns, value))
        cutoff = ts_ns - self.window_ns
        while values and values[0][0] <= cutoff:
            values.popleft()

    def _mean(self, values: list[tuple[int, Decimal]]) -> Decimal:
        return sum((value for _, value in values), Decimal("0")) / Decimal(len(values))

    def _std(self, values: list[tuple[int, Decimal]], mean: Decimal) -> Decimal:
        if len(values) < 2:
            return Decimal("0")
        variance = sum(((value - mean) ** 2 for _, value in values), Decimal("0")) / Decimal(len(values))
        return variance.sqrt()

    # 启动时用 collector 历史 quote 初始化 rolling window。
    def warm_from_rows(
        self,
        rows: list[dict[str, object]],
        start_ns: int,
        end_ns: int,
        binance_id: InstrumentId,
        okx_id: InstrumentId,
    ) -> None:
        minute_quote_sums: dict[int, dict[str, tuple[Decimal, Decimal, int]]] = {}
        for row in rows:
            event_ns = int(row["ts_exchange_ms"]) * 1_000_000
            minute_ns = event_ns // MINUTE_NS * MINUTE_NS
            venue = str(row["venue"]).upper()
            item = minute_quote_sums.setdefault(minute_ns, {})
            bid = Decimal(str(row["bid"]))
            ask = Decimal(str(row["ask"]))
            old = item.get(venue)
            if old is None:
                item[venue] = bid, ask, 1
            else:
                item[venue] = old[0] + bid, old[1] + ask, old[2] + 1
        self.long_values.clear()
        self.short_values.clear()
        self.quote_sums.clear()
        self.minute_counts.clear()
        last: dict[str, tuple[Decimal, Decimal]] = {}
        minute_ns = start_ns // MINUTE_NS * MINUTE_NS
        end_minute_ns = end_ns // MINUTE_NS * MINUTE_NS
        for seed_minute_ns in sorted(key for key in minute_quote_sums if key < minute_ns):
            for venue, (bid_sum, ask_sum, count) in minute_quote_sums[seed_minute_ns].items():
                last[venue] = bid_sum / Decimal(count), ask_sum / Decimal(count)
        while minute_ns <= end_minute_ns:
            for venue, (bid_sum, ask_sum, count) in minute_quote_sums.get(minute_ns, {}).items():
                last[venue] = bid_sum / Decimal(count), ask_sum / Decimal(count)
            if "BINANCE" not in last or "OKX" not in last:
                raise RuntimeError(f"warmup_window_not_full minute_ns={minute_ns} venues={','.join(sorted(last))}")
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
            self.minute_counts.append({
                "minute_ns": minute_ns,
                str(binance_id): minute_quote_sums.get(minute_ns, {}).get("BINANCE", (Decimal("0"), Decimal("0"), 0))[2],
                str(okx_id): minute_quote_sums.get(minute_ns, {}).get("OKX", (Decimal("0"), Decimal("0"), 0))[2],
            })
            minute_ns += MINUTE_NS
        self.update_stats()
        self.last_price_means[binance_id] = last["BINANCE"]
        self.last_price_means[okx_id] = last["OKX"]

    # 用 Binance/OKX 的 bid/ask 计算 long/short 两条 edge。
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

    # 更新 rolling window 的均值和标准差。
    def update_stats(self) -> None:
        long_values = list(self.long_values)
        short_values = list(self.short_values)
        long_mean = self._mean(long_values)
        short_mean = self._mean(short_values)
        self.long_mean_bps = long_mean
        self.short_mean_bps = short_mean
        self.long_std_bps = self._std(long_values, long_mean)
        self.short_std_bps = self._std(short_values, short_mean)

    # 根据当前状态和 edge 偏离返回 long/short signal。
    def signal(self, state: str) -> str | None:
        long_threshold = self.exit_bps if state == STATE_SHORT else max(self.entry_bps, self.long_std_bps * self.std_mult)
        short_threshold = self.exit_bps if state == STATE_LONG else max(self.entry_bps, self.short_std_bps * self.std_mult)
        if self.long_mean_bps - self.long_bps > long_threshold and self.long_bps <= self.long_max_bps:
            return "long"
        if self.short_bps - self.short_mean_bps > short_threshold and self.short_bps >= self.short_min_bps:
            return "short"
        return None

    # 给 snapshot 提供不可变副本，避免 housekeeping 和 monitor 数据竞争。
    def quote_counts(self) -> list[dict[str, int]]:
        return [dict(item) for item in self.minute_counts.copy()]
