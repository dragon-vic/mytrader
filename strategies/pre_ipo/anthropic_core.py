from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal

from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId


BPS = Decimal("10000")
MINUTE_NS = 60_000_000_000
ASSET = "ANTHROPIC"
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


@dataclass
class PendingLeg:
    order_id: str
    instrument_id: InstrumentId
    side: OrderSide
    target_qty: Decimal
    quote_px: Decimal
    filled_qty: Decimal = Decimal("0")
    filled_notional: Decimal = Decimal("0")
    submit_ns: int | None = None
    fill_event_ns: int | None = None
    full_fill_event_ns: int | None = None
    failed: bool = False

    # 单腿实际成交量达到目标量才算完成。
    def filled(self) -> bool:
        return self.filled_qty >= self.target_qty

    # fill 或失败都是最终反馈。
    def done(self) -> bool:
        return self.failed or self.filled()


@dataclass
class PendingPair:
    legs: dict[str, PendingLeg]
    signal: str
    edge_side: str
    signal_edge_bps: Decimal
    mean_bps: Decimal
    signal_event_ns: int
    signal_ts_ns: int
    created_ns: int
    before_inventory: Decimal
    after_inventory: Decimal
    repairs: dict[str, PendingLeg] = None

    def __post_init__(self) -> None:
        self.repairs = {}

    # 记录一笔订单的部分或完整成交。
    def record_fill(self, order_id: str, qty: Decimal, px: Decimal, event_ns: int) -> None:
        leg = self.leg(order_id)
        leg.filled_qty += qty
        leg.filled_notional += qty * px
        leg.fill_event_ns = max(leg.fill_event_ns or 0, event_ns)
        if leg.full_fill_event_ns is None and leg.filled():
            leg.full_fill_event_ns = event_ns

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

    # 获取某条交易腿的成交均价。
    def avg_px(self, instrument_id: InstrumentId) -> Decimal | None:
        for leg in self.legs.values():
            if leg.instrument_id == instrument_id and leg.filled_qty > 0:
                return leg.filled_notional / leg.filled_qty
        return None

@dataclass
class PositionState:
    instrument_id: InstrumentId
    signed_qty: Decimal = Decimal("0")
    avg_px: Decimal | None = None
    realized_pnl: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")

    # 用成交事件推进单 instrument 仓位、均价和已实现盈亏。
    def apply_fill(self, side: OrderSide, qty: Decimal, px: Decimal, fee: Decimal) -> None:
        fill_qty = qty if side == OrderSide.BUY else -qty
        self.fee += fee
        self.realized_pnl -= fee
        if self.signed_qty == 0 or (self.signed_qty > 0) == (fill_qty > 0):
            old_abs = abs(self.signed_qty)
            new_abs = old_abs + abs(fill_qty)
            self.avg_px = px if old_abs == 0 else (self.avg_px * old_abs + px * abs(fill_qty)) / new_abs
            self.signed_qty += fill_qty
            return

        close_qty = min(abs(self.signed_qty), abs(fill_qty))
        if self.signed_qty > 0:
            self.realized_pnl += (px - self.avg_px) * close_qty
        else:
            self.realized_pnl += (self.avg_px - px) * close_qty
        self.signed_qty += fill_qty
        if self.signed_qty == 0:
            self.avg_px = None
        elif abs(fill_qty) > close_qty:
            self.avg_px = px

    # 用最新 quote 估算立刻平仓后的未实现盈亏，包含预估平仓滑点和手续费。
    def mark(self, bid: Decimal, ask: Decimal, slippage_bps: Decimal) -> None:
        if self.signed_qty == 0 or self.avg_px is None:
            self.unrealized_pnl = Decimal("0")
            return
        slippage = slippage_bps / BPS
        fee_rate = EXIT_FEE_BPS / BPS
        if self.signed_qty > 0:
            exit_px = bid * (Decimal("1") - slippage)
            exit_notional = exit_px * self.signed_qty
            self.unrealized_pnl = (exit_px - self.avg_px) * self.signed_qty - exit_notional * fee_rate
            return
        qty = abs(self.signed_qty)
        exit_px = ask * (Decimal("1") + slippage)
        exit_notional = exit_px * qty
        self.unrealized_pnl = (self.avg_px - exit_px) * qty - exit_notional * fee_rate

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
    long_values: deque[tuple[int, Decimal]] = None
    short_values: deque[tuple[int, Decimal]] = None
    minute_ns: int | None = None
    minute_quote_sums: dict[InstrumentId, tuple[Decimal, Decimal, int]] = None
    last_price_means: dict[InstrumentId, tuple[Decimal, Decimal]] = None
    minute_counts: deque[dict[str, int]] = None

    def __post_init__(self) -> None:
        self.long_values = deque()
        self.short_values = deque()
        self.minute_quote_sums = {}
        self.last_price_means = {}
        self.minute_counts = deque(maxlen=10)

    # 每个 quote 事件后更新当前 long/short edge。
    def update(self, binance: QuoteTick, okx: QuoteTick) -> None:
        bn_bid = Decimal(str(binance.bid_price))
        bn_ask = Decimal(str(binance.ask_price))
        okx_bid = Decimal(str(okx.bid_price)) * self.okx_price_multiplier
        okx_ask = Decimal(str(okx.ask_price)) * self.okx_price_multiplier
        self.long_bps, self.short_bps = self.from_prices(bn_bid, bn_ask, okx_bid, okx_ask)

    # 将 quote 放入当前分钟桶；跨分钟时先结算旧桶。
    def record_quote(self, tick: QuoteTick, binance_id: InstrumentId, okx_id: InstrumentId) -> None:
        minute_ns = int(tick.ts_event) // MINUTE_NS * MINUTE_NS
        if self.minute_ns is None:
            self.minute_ns = minute_ns
        while minute_ns > self.minute_ns:
            self.close_minute(self.minute_ns, binance_id, okx_id)
            self.minute_ns += MINUTE_NS
        bid_sum, ask_sum, count = self.minute_quote_sums.get(tick.instrument_id, (Decimal("0"), Decimal("0"), 0))
        self.minute_quote_sums[tick.instrument_id] = (
            bid_sum + Decimal(str(tick.bid_price)),
            ask_sum + Decimal(str(tick.ask_price)),
            count + 1,
        )

    # housekeeping 补齐已经结束但没有新 quote 触发的分钟。
    def fill_to(self, now_ns: int, binance_id: InstrumentId, okx_id: InstrumentId) -> None:
        current_minute_ns = now_ns // MINUTE_NS * MINUTE_NS
        while self.minute_ns is not None and self.minute_ns < current_minute_ns:
            self.close_minute(self.minute_ns, binance_id, okx_id)
            self.minute_ns += MINUTE_NS

    # 用一分钟内 bid/ask 均值生成一个时间加权 edge 样本。
    def close_minute(self, minute_ns: int, binance_id: InstrumentId, okx_id: InstrumentId) -> None:
        counts = {
            str(binance_id): self.minute_quote_sums.get(binance_id, (Decimal("0"), Decimal("0"), 0))[2],
            str(okx_id): self.minute_quote_sums.get(okx_id, (Decimal("0"), Decimal("0"), 0))[2],
        }
        for instrument_id, (bid_sum, ask_sum, count) in self.minute_quote_sums.items():
            self.last_price_means[instrument_id] = bid_sum / Decimal(count), ask_sum / Decimal(count)
        bn_bid, bn_ask = self.last_price_means[binance_id]
        okx_bid, okx_ask = self.last_price_means[okx_id]
        okx_bid *= self.okx_price_multiplier
        okx_ask *= self.okx_price_multiplier
        long_bps, short_bps = self.from_prices(bn_bid, bn_ask, okx_bid, okx_ask)
        self._add_value(self.long_values, minute_ns, long_bps)
        self._add_value(self.short_values, minute_ns, short_bps)
        self.update_stats()
        self.minute_counts.append({"minute_ns": minute_ns, **counts})
        self.minute_quote_sums.clear()

    def _add_value(self, values: deque[tuple[int, Decimal]], ts_ns: int, value: Decimal) -> None:
        values.append((ts_ns, value))
        cutoff = ts_ns - self.window_ns
        while values and values[0][0] < cutoff:
            values.popleft()

    def _mean(self, values: deque[tuple[int, Decimal]]) -> Decimal:
        return sum((value for _, value in values), Decimal("0")) / Decimal(len(values))

    def _std(self, values: deque[tuple[int, Decimal]], mean: Decimal) -> Decimal:
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
            minute_ns = int(row["ts_local_ns"]) // MINUTE_NS * MINUTE_NS
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
        self.minute_quote_sums.clear()
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
        self.minute_ns = end_minute_ns

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
        long_mean = self._mean(self.long_values)
        short_mean = self._mean(self.short_values)
        self.long_mean_bps = long_mean
        self.short_mean_bps = short_mean
        self.long_std_bps = self._std(self.long_values, long_mean)
        self.short_std_bps = self._std(self.short_values, short_mean)

    # 根据当前状态和 edge 偏离返回 long/short signal。
    def signal(self, state: str) -> str | None:
        long_threshold = self.exit_bps if state == STATE_SHORT else max(self.entry_bps, self.long_std_bps * self.std_mult)
        short_threshold = self.exit_bps if state == STATE_LONG else max(self.entry_bps, self.short_std_bps * self.std_mult)
        if self.long_mean_bps - self.long_bps > long_threshold and self.long_bps <= self.long_max_bps:
            return "long"
        if self.short_bps - self.short_mean_bps > short_threshold and self.short_bps >= self.short_min_bps:
            return "short"
        return None
