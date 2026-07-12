from __future__ import annotations

from collections import deque
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from pathlib import Path

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderAccepted
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import OrderSubmitted
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from strategies.pre_ipo.coordinator import BIND_ENDPOINT
from strategies.pre_ipo.coordinator import StrategyBindRequest
from strategies.pre_ipo.pre_ipo_core import BPS
from strategies.pre_ipo.pre_ipo_core import BINANCE_EXIT_SLIPPAGE_BPS
from strategies.pre_ipo.pre_ipo_core import EXIT_FEE_BPS
from strategies.pre_ipo.pre_ipo_core import LONG_EDGE
from strategies.pre_ipo.pre_ipo_core import MINUTE_NS
from strategies.pre_ipo.pre_ipo_core import OKX_EXIT_SLIPPAGE_BPS
from strategies.pre_ipo.pre_ipo_core import SHORT_EDGE
from strategies.pre_ipo.pre_ipo_core import STATE_FLAT
from strategies.pre_ipo.pre_ipo_core import STATE_LONG
from strategies.pre_ipo.pre_ipo_core import STATE_PENDING
from strategies.pre_ipo.pre_ipo_core import STATE_SHORT
from strategies.pre_ipo.pre_ipo_core import STATE_UNBALANCE
from strategies.pre_ipo.pre_ipo_core import EdgePair
from strategies.pre_ipo.pre_ipo_core import PendingLeg
from strategies.pre_ipo.pre_ipo_core import PendingPair
from strategies.pre_ipo.pre_ipo_core import PnlLedger
from strategies.pre_ipo.pre_ipo_core import StrategyMetrics
from strategies.pre_ipo.pre_ipo_core import VenueMetrics
from strategies.pre_ipo.pre_ipo_core import WarmupLoader


BEIJING_TZ = timezone(timedelta(hours=8))
EDGE_SUSPEND_BPS = Decimal("3000")


class PreIpoArbConfig(StrategyConfig, frozen=True):
    asset: str
    instruments: list[str]
    window_minutes: Decimal
    okx_multiplier: Decimal
    entry_bps: Decimal
    exit_bps: Decimal
    std_mult: Decimal
    long_max_bps: Decimal
    short_min_bps: Decimal
    qty: Decimal
    margin_leverage: Decimal
    margin_buffer: Decimal


class PreIpoArbStrategy(Strategy):
    def __init__(self, config: PreIpoArbConfig) -> None:
        super().__init__(config)
        self.asset = config.asset.upper()
        instruments = [InstrumentId.from_str(value) for value in config.instruments]
        by_venue = {self._venue(instrument_id): instrument_id for instrument_id in instruments}
        if len(instruments) != 2 or set(by_venue) != {"BINANCE", "OKX"}:
            raise ValueError(f"pre_ipo_arb requires one BINANCE and one OKX instrument asset={self.asset}")
        self.binance_id = by_venue["BINANCE"]
        self.okx_id = by_venue["OKX"]
        self.instrument_ids = [self.binance_id, self.okx_id]
        self.quotes: dict[InstrumentId, QuoteTick] = {}
        self.edge = EdgePair(
            window_ns=int(config.window_minutes * Decimal(MINUTE_NS)),
            okx_price_multiplier=config.okx_multiplier,
            long_mean_bps=Decimal("0"),
            short_mean_bps=Decimal("0"),
            long_std_bps=Decimal("0"),
            short_std_bps=Decimal("0"),
            entry_bps=config.entry_bps,
            exit_bps=config.exit_bps,
            std_mult=config.std_mult,
            long_max_bps=config.long_max_bps,
            short_min_bps=config.short_min_bps,
        )
        self.qty = config.qty
        self.okx_qty_multiplier = config.okx_multiplier
        self.margin_leverage = config.margin_leverage
        self.margin_buffer = config.margin_buffer
        self.trade_state = STATE_FLAT
        self.mode = "normal"
        self.fail_count = 0
        self.pending: PendingPair | None = None
        self.pnl_ledgers = {instrument_id: PnlLedger() for instrument_id in self.instrument_ids}
        self.metrics: StrategyMetrics
        self.action_rows: deque[dict[str, object]] = deque(maxlen=200)

    # 策略启动入口：接管仓位、初始化窗口，再绑定 node 级协调器。
    def on_start(self) -> None:
        self._check_startup()
        self._warm_initial_window()
        self._refresh_metrics()
        request = StrategyBindRequest(strategy=self)
        self.msgbus.send(BIND_ENDPOINT, request)
        if request.coordinator is None:
            raise RuntimeError(f"coordinator_bind_failed asset={self.asset}")
        self.coordinator = request.coordinator
        for instrument_id in self.instrument_ids:
            self.subscribe_quote_ticks(instrument_id)

    # 策略停止入口：stop mode 已负责减仓，这里只取消订阅。
    def on_stop(self) -> None:
        for instrument_id in self.instrument_ids:
            self.unsubscribe_quote_ticks(instrument_id)

    # Coordinator 统一调用该入口控制 normal/reduce/stop。
    def handle_command(self, name: str, source: str, reason: str) -> None:
        if name == "stop":
            self.log.warning(f"mode_stop source={source} reason={reason}")
            self.mode = "stop"
            return
        if name == "reduce":
            self.mode = "normal" if self.trade_state == STATE_FLAT else "reduce"
            self.log.warning(f"mode_{self.mode} source={source}")
            return
        if name == "normal":
            if self.trade_state == STATE_UNBALANCE:
                if not self._inventory_balanced():
                    self.log.warning("mode_normal_rejected reason=inventory_unbalanced")
                    return
                self._sync_state_from_inventory()
            self.mode = "normal"
            self.fail_count = 0
            self.log.warning(f"mode_normal source={source}")
            return
        self.log.warning(f"coordinator_command_ignored command={name} source={source}")

    # quote 主入口：更新 edge，生成 signal，检查通过后提交双腿订单。
    def on_quote_tick(self, tick: QuoteTick) -> None:
        on_quote_ns = self.clock.timestamp_ns()
        self.quotes[tick.instrument_id] = tick
        self.edge.record_quote(tick)
        self.edge.update(self.quotes[self.binance_id], self.quotes[self.okx_id])
        if self.trade_state not in {STATE_PENDING, STATE_UNBALANCE}:
            signal = self.edge.signal(self.trade_state)
            signal = self._checked_signal(signal)
            if signal is not None:
                self._submit_signal(signal, tick, on_quote_ns)

    def on_order_submitted(self, event: OrderSubmitted) -> None:
        order_id = str(event.client_order_id)
        if self.pending is not None and self.pending.has_order(order_id):
            self.pending.record_submit(order_id, int(event.ts_event))

    def on_order_accepted(self, event: OrderAccepted) -> None:
        order_id = str(event.client_order_id)
        if self.pending is not None and self.pending.has_order(order_id):
            self.pending.record_accept(order_id, int(event.ts_event))

    # 成交事件入口：记录双腿成交并推进 pending 生命周期。
    def on_order_filled(self, event: OrderFilled) -> None:
        order_id = str(event.client_order_id)
        if self.pending is None or not self.pending.has_order(order_id):
            return
        self.pnl_ledgers[event.instrument_id].record_fill(
            event.order_side,
            event.last_qty.as_decimal(),
            event.last_px.as_decimal(),
            event.commission.as_decimal(),
        )
        self.pending.record_fill(
            order_id,
            event.last_qty.as_decimal(),
            event.last_px.as_decimal(),
            int(event.ts_event),
        )
        self._refresh_metrics()
        self._resolve_pending_if_done()

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def on_order_denied(self, event: OrderDenied) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def on_order_canceled(self, event: OrderCanceled) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def on_order_expired(self, event: OrderExpired) -> None:
        self._mark_order_failed(str(event.client_order_id))

    # 构建窗口前检查订单并接管平衡的双腿仓位。
    def _check_startup(self) -> None:
        positions = {}
        for instrument_id in self.instrument_ids:
            if self.cache.instrument(instrument_id) is None:
                raise RuntimeError(f"startup_instrument_missing instrument={instrument_id}")
            venue = self._venue(instrument_id)
            if self._account_for_venue(venue) is None:
                raise RuntimeError(f"startup_account_missing venue={venue} instrument={instrument_id}")
            open_orders = self.cache.orders_open(instrument_id=instrument_id)
            if open_orders:
                order_ids = ",".join(str(order.client_order_id) for order in open_orders)
                raise RuntimeError(f"startup_open_orders instrument={instrument_id} orders={order_ids}")
            open_positions = self.cache.positions_open(instrument_id=instrument_id)
            if len(open_positions) > 1:
                position_ids = ",".join(str(position.id) for position in open_positions)
                raise RuntimeError(f"startup_multiple_positions instrument={instrument_id} positions={position_ids}")
            positions[instrument_id] = open_positions[0] if open_positions else None

        binance_position = positions[self.binance_id]
        okx_position = positions[self.okx_id]
        if binance_position is None and okx_position is None:
            self._sync_state_from_inventory()
            return
        if binance_position is None or okx_position is None:
            raise RuntimeError("startup_position_unbalanced reason=missing_leg")
        for position in (binance_position, okx_position):
            if position.strategy_id != self.id:
                raise RuntimeError(
                    f"startup_position_unclaimed instrument={position.instrument_id} "
                    f"position={position.id} strategy_id={position.strategy_id}",
                )

        binance_qty = binance_position.signed_decimal_qty()
        okx_qty = okx_position.signed_decimal_qty()
        if binance_qty == 0 or okx_qty == 0 or binance_qty + okx_qty / self.okx_qty_multiplier != 0:
            raise RuntimeError(f"startup_position_unbalanced binance_qty={binance_qty} okx_qty={okx_qty}")

        self.pnl_ledgers[self.binance_id].seed_position(
            binance_qty,
            Decimal(str(binance_position.avg_px_open)),
        )
        self.pnl_ledgers[self.okx_id].seed_position(
            okx_qty,
            Decimal(str(okx_position.avg_px_open)),
        )
        self._sync_state_from_inventory()
        self.log.info(
            f"startup_position_adopted state={self.trade_state} "
            f"binance_qty={binance_qty} okx_qty={okx_qty}",
        )

    # signal 生成后的所有交易前检查集中在这里。
    def _checked_signal(self, signal: str | None) -> str | None:
        if abs(self.edge.long_bps) > EDGE_SUSPEND_BPS or abs(self.edge.short_bps) > EDGE_SUSPEND_BPS:
            if self.mode != "suspend":
                self.log.error(
                    f"possible_rebase_suspend long={self.edge.long_bps:.2f} "
                    f"short={self.edge.short_bps:.2f}",
                )
            self.mode = "suspend"
            return None
        if self.mode == "suspend":
            return None
        if self.mode == "stop":
            if self.trade_state == STATE_FLAT:
                return None
            if self.qty == 0:
                return None
            signal = "short" if self.trade_state == STATE_LONG else "long"
        if signal is None:
            return None
        if self.qty == 0:
            return None
        if self.mode == "reduce":
            if self.trade_state == STATE_LONG and signal != "short":
                return None
            if self.trade_state == STATE_SHORT and signal != "long":
                return None
            if self.trade_state == STATE_FLAT:
                return None
        return signal

    # stop/reduce 模式只减仓，不允许一次反向 signal 翻仓。
    def _trade_qty(self, signal: str, before_inventory: Decimal) -> Decimal:
        if self.mode in {"stop", "reduce"} and before_inventory != 0:
            if before_inventory > 0 and signal == "short":
                return min(self.qty, before_inventory)
            if before_inventory < 0 and signal == "long":
                return min(self.qty, abs(before_inventory))
        return self.qty

    # 把 long/short signal 转为两条实际订单腿。
    def _signal_legs(self, signal: str, base_qty: Decimal | None = None) -> list[tuple[InstrumentId, OrderSide, Decimal]]:
        qty = self.qty if base_qty is None else base_qty
        if signal == "long":
            return [
                (self.okx_id, OrderSide.BUY, qty * self.okx_qty_multiplier),
                (self.binance_id, OrderSide.SELL, qty),
            ]
        return [
            (self.binance_id, OrderSide.BUY, qty),
            (self.okx_id, OrderSide.SELL, qty * self.okx_qty_multiplier),
        ]

    # Coordinator 在统一的整分钟时点调用，结算窗口并刷新策略派生数据。
    def run_housekeeping(self, now_ns: int) -> None:
        self.edge.close_bucket(now_ns, self.binance_id, self.okx_id)
        self._refresh_metrics()

    # 从本策略 PnlLedger 一次性计算 snapshot、风控和保证金预留共用的指标。
    def _refresh_metrics(self) -> None:
        venues = {}
        totals = {"realized": Decimal("0"), "unrealized": Decimal("0"), "fee": Decimal("0")}
        for instrument_id in self.instrument_ids:
            instrument = self.cache.instrument(instrument_id)
            ledger = self.pnl_ledgers[instrument_id]
            qty = ledger.signed_qty
            avg_px = ledger.avg_px
            realized = ledger.realized
            fees = ledger.fee
            locked = Decimal("0")
            unrealized = Decimal("0")
            if qty != 0:
                quantity = instrument.make_qty(abs(qty))
                avg_price = instrument.make_price(avg_px)
                notional = self._money_decimal(instrument.notional_value(quantity, avg_price))
                locked = None if notional is None else notional / self.margin_leverage
                quote = self.quotes[instrument_id]
                slippage_bps = (
                    BINANCE_EXIT_SLIPPAGE_BPS if instrument_id == self.binance_id else OKX_EXIT_SLIPPAGE_BPS
                )
                if qty > 0:
                    exit_px = quote.bid_price.as_decimal() * (Decimal("1") - slippage_bps / BPS)
                    raw_pnl = (exit_px - avg_px) * abs(qty)
                else:
                    exit_px = quote.ask_price.as_decimal() * (Decimal("1") + slippage_bps / BPS)
                    raw_pnl = (avg_px - exit_px) * abs(qty)
                exit_price = instrument.make_price(exit_px)
                exit_notional = self._money_decimal(instrument.notional_value(quantity, exit_price))
                unrealized = None if exit_notional is None else raw_pnl - exit_notional * EXIT_FEE_BPS / BPS
            venue = self._venue(instrument_id)
            venues[venue] = VenueMetrics(
                instrument_id=instrument_id,
                qty=qty,
                avg_px=avg_px,
                realized_usdt=realized,
                unrealized_usdt=unrealized,
                fee_usdt=fees,
                locked_usdt=locked,
            )
            totals["realized"] += realized
            totals["unrealized"] = None if totals["unrealized"] is None or unrealized is None else totals["unrealized"] + unrealized
            totals["fee"] += fees
        self.metrics = StrategyMetrics(
            venues=venues,
            realized_usdt=totals["realized"],
            unrealized_usdt=totals["unrealized"],
            fee_usdt=totals["fee"],
        )

    # 向 Coordinator 提供已经计算好的策略状态，不负责文件落盘。
    def snapshot(self, now_ns: int) -> dict[str, object]:
        metrics = self.metrics
        positions = {
            venue: {
                "instrument": str(values.instrument_id),
                "qty": self._fmt(values.qty),
                "avg_px": self._fmt(values.avg_px),
                "realized_usdt": self._fmt(values.realized_usdt),
                "unrealized_usdt": self._fmt(values.unrealized_usdt),
                "fee_usdt": self._fmt(values.fee_usdt),
            }
            for venue, values in metrics.venues.items()
        }
        pnl = {
            "realized_usdt": self._fmt(metrics.realized_usdt),
            "unrealized_usdt": self._fmt(metrics.unrealized_usdt),
            "fee_usdt": self._fmt(metrics.fee_usdt),
        }
        quotes = {
            self._venue(instrument_id): {
                "instrument": str(instrument_id),
                "bid": self._fmt(quote.bid_price.as_decimal()),
                "ask": self._fmt(quote.ask_price.as_decimal()),
                "bid_size": self._fmt(quote.bid_size.as_decimal()),
                "ask_size": self._fmt(quote.ask_size.as_decimal()),
                "ts_event": str(quote.ts_event),
                "age_ms": self._fmt((now_ns - int(quote.ts_event)) / 1_000_000),
            }
            for instrument_id, quote in self.quotes.copy().items()
        }
        pending = None
        if self.pending is not None:
            pending_pair = self.pending
            main_legs = pending_pair.legs.copy()
            repair_legs = pending_pair.repairs.copy()
            best_edge = pending_pair.best_edge_bps()
            actual_edge = pending_pair.actual_edge_bps()
            pending = {
                "signal": pending_pair.signal,
                "edge_side": pending_pair.edge_side,
                "signal_venue": pending_pair.signal_venue,
                "signal_edge_bps": self._fmt(pending_pair.signal_edge_bps),
                "mean_bps": self._fmt(pending_pair.mean_bps),
                "std_bps": self._fmt(pending_pair.std_bps),
                "best_edge_bps": self._fmt(best_edge),
                "actual_edge_bps": self._fmt(actual_edge),
                "signal_event_ns": str(pending_pair.signal_event_ns),
                "signal_ts_ns": str(pending_pair.signal_ts_ns),
                "on_quote_ns": str(pending_pair.on_quote_ns),
                "edge_slip": self._fmt(pending_pair.edge_slippage_bps()),
                "fill_slip": self._fmt(pending_pair.fill_slippage_bps()),
                "age_sec": self._fmt(max((now_ns - pending_pair.on_quote_ns) / 1_000_000_000, 0.0)),
                "orders": [
                    {
                        "order_id": leg.order_id,
                        "kind": "main",
                        "instrument": str(leg.instrument_id),
                        "side": str(leg.side),
                        "filled_qty": self._fmt(leg.filled_qty),
                        "target_qty": self._fmt(leg.target_qty),
                        "best_px": self._fmt(leg.best_px),
                        "submit_event_ns": str(leg.submit_event_ns) if leg.submit_event_ns is not None else "-",
                        "accept_event_ns": str(leg.accept_event_ns) if leg.accept_event_ns is not None else "-",
                        "fill_event_ns": str(leg.fill_event_ns) if leg.fill_event_ns is not None else "-",
                        "full_fill_event_ns": str(leg.full_fill_event_ns) if leg.full_fill_event_ns is not None else "-",
                        "failed": leg.failed,
                    }
                    for leg in main_legs.values()
                ] + [
                    {
                        "order_id": leg.order_id,
                        "kind": "repair",
                        "instrument": str(leg.instrument_id),
                        "side": str(leg.side),
                        "filled_qty": self._fmt(leg.filled_qty),
                        "target_qty": self._fmt(leg.target_qty),
                        "best_px": self._fmt(leg.best_px),
                        "submit_event_ns": str(leg.submit_event_ns) if leg.submit_event_ns is not None else "-",
                        "accept_event_ns": str(leg.accept_event_ns) if leg.accept_event_ns is not None else "-",
                        "fill_event_ns": str(leg.fill_event_ns) if leg.fill_event_ns is not None else "-",
                        "full_fill_event_ns": str(leg.full_fill_event_ns) if leg.full_fill_event_ns is not None else "-",
                        "failed": leg.failed,
                    }
                    for leg in repair_legs.values()
                ],
            }
        return {
            "strategy": "pre_ipo_arb",
            "asset": self.asset,
            "time_ns": now_ns,
            "state": self.trade_state,
            "mode": self.mode,
            "inventory": self._fmt(self._inventory()),
            "quotes": quotes,
            "edge": {
                "long_bps": self._fmt(self.edge.long_bps),
                "long_mean_bps": self._fmt(self.edge.long_mean_bps),
                "long_std_bps": self._fmt(self.edge.long_std_bps),
                "short_bps": self._fmt(self.edge.short_bps),
                "short_mean_bps": self._fmt(self.edge.short_mean_bps),
                "short_std_bps": self._fmt(self.edge.short_std_bps),
            },
            "positions": positions,
            "pnl": pnl,
            "quote_counts": self.edge.quote_counts(),
            "pending": pending,
            "actions": [dict(row) for row in self.action_rows.copy()],
        }

    # 提交双腿市价单；submit 前先写 pending，避免同步事件早于状态。
    def _submit_signal(self, signal: str, tick: QuoteTick, on_quote_ns: int) -> None:
        if self.pending is not None:
            self.log.warning(f"pending_create_skipped_existing signal={signal} existing_signal={self.pending.signal}")
            return
        before_inventory = self._inventory()
        base_qty = self._trade_qty(signal, before_inventory)
        signal_event_ns = int(tick.ts_event)
        signal_ts_ns = int(tick.ts_init)
        if signal == "long":
            buy_id, sell_id = self.okx_id, self.binance_id
            buy_qty, sell_qty = base_qty * self.okx_qty_multiplier, base_qty
            edge_side = LONG_EDGE
            signal_edge = self.edge.long_bps
            mean_bps = self.edge.long_mean_bps
            std_bps = self.edge.long_std_bps
            after_inventory = before_inventory + base_qty
        else:
            buy_id, sell_id = self.binance_id, self.okx_id
            buy_qty, sell_qty = base_qty, base_qty * self.okx_qty_multiplier
            edge_side = SHORT_EDGE
            signal_edge = self.edge.short_bps
            mean_bps = self.edge.short_mean_bps
            std_bps = self.edge.short_std_bps
            after_inventory = before_inventory - base_qty

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
        reservation_id = None
        if abs(after_inventory) > abs(before_inventory):
            lock_amounts = {}
            for instrument_id, side, qty in self._signal_legs(signal, base_qty):
                quote = self.quotes[instrument_id]
                price = (quote.ask_price if side == OrderSide.BUY else quote.bid_price).as_decimal()
                lock_amounts[self._venue(instrument_id)] = price * qty / self.margin_leverage
            reservation_id = self.coordinator.reserve(self, lock_amounts)
            if reservation_id is None:
                return
        legs = {
            str(buy_order.client_order_id): PendingLeg(
                order_id=str(buy_order.client_order_id),
                instrument_id=buy_id,
                side=OrderSide.BUY,
                target_qty=buy_quantity.as_decimal(),
            ),
            str(sell_order.client_order_id): PendingLeg(
                order_id=str(sell_order.client_order_id),
                instrument_id=sell_id,
                side=OrderSide.SELL,
                target_qty=sell_quantity.as_decimal(),
            ),
        }
        self.trade_state = STATE_PENDING
        self.pending = PendingPair(
            legs=legs,
            signal=signal,
            edge_side=edge_side,
            signal_edge_bps=signal_edge,
            mean_bps=mean_bps,
            std_bps=std_bps,
            signal_event_ns=signal_event_ns,
            signal_ts_ns=signal_ts_ns,
            signal_venue=self._venue(tick.instrument_id),
            on_quote_ns=on_quote_ns,
            before_inventory=before_inventory,
            after_inventory=after_inventory,
            okx_price_multiplier=self.edge.okx_price_multiplier,
            reservation_id=reservation_id,
        )
        self.submit_order(buy_order)
        self.submit_order(sell_order)

    # 按 Binance 腿库存同步 flat/long/short 状态。
    def _sync_state_from_inventory(self) -> None:
        qty = self._net_qty(self.binance_id)
        if qty == 0:
            self.trade_state = STATE_FLAT
            return
        self.trade_state = STATE_SHORT if qty > 0 else STATE_LONG

    # 记录 pending 订单失败，等两腿都有最终反馈后统一处理。
    def _mark_order_failed(self, order_id: str) -> None:
        if self.pending is not None and self.pending.has_order(order_id):
            leg = self.pending.leg(order_id)
            self.log.error(
                f"pending_order_failed order={order_id} instrument={leg.instrument_id} side={leg.side} "
                f"filled={leg.filled_qty} target={leg.target_qty}",
            )
            self.pending.record_failed(order_id)
            self._resolve_pending_if_done()

    # pending 生命周期收口：成功、全失败、单腿失败补反向单。
    def _resolve_pending_if_done(self) -> None:
        pending = self.pending
        if pending is None or not pending.is_done():
            return
        if pending.is_complete():
            self._finish_record("filled")
            return
        if pending.has_repairs():
            failed_repairs = [leg for leg in pending.repairs.values() if leg.failed]
            if failed_repairs:
                self.mode = "suspend"
                failed_text = ",".join(leg.order_id for leg in failed_repairs)
                self.log.error(
                    f"repair_order_failed_suspend orders={failed_text}",
                )
                self._finish_record("repair_unbalance")
                return
            if self._inventory_balanced():
                self._finish_record("repaired")
            else:
                self.mode = "suspend"
                bn_qty = self._net_qty(self.binance_id)
                okx_qty = self._net_qty(self.okx_id)
                self.log.error(f"repair_finished_still_unbalanced bn_qty={bn_qty} okx_qty={okx_qty}")
                self._finish_record("repair_unbalance")
            return
        if pending.is_all_failed():
            self.log.warning(f"pending_pair_all_failed orders={','.join(pending.legs)}")
            self.fail_count += 1
            if any(leg.filled_qty > 0 for leg in pending.legs.values()):
                self.trade_state = STATE_UNBALANCE
                self._submit_repair_orders()
            else:
                self._finish_record("failed")
            return
        self.log.error(f"pending_pair_one_leg_failed_enter_unbalance orders={','.join(pending.legs)}")
        self.fail_count += 1
        self.trade_state = STATE_UNBALANCE
        self._submit_repair_orders()

    # 按主订单已成交数量生成反向修复单，把账户退回下单前状态。
    def _submit_repair_orders(self) -> None:
        orders = []
        repairs = {}
        for leg in self.pending.legs.values():
            if leg.filled_qty <= 0:
                continue
            side = OrderSide.SELL if leg.side == OrderSide.BUY else OrderSide.BUY
            quantity = self.cache.instrument(leg.instrument_id).make_qty(leg.filled_qty)
            order = self.order_factory.market(
                instrument_id=leg.instrument_id,
                order_side=side,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
            )
            order_id = str(order.client_order_id)
            repairs[order_id] = PendingLeg(
                order_id=order_id,
                instrument_id=leg.instrument_id,
                side=side,
                target_qty=quantity.as_decimal(),
            )
            orders.append(order)
        if not repairs:
            self.mode = "suspend"
            self.log.error(f"repair_no_filled_qty orders={','.join(self.pending.legs)}")
            self._finish_record("repair_unbalance")
            return
        self.pending.repairs = repairs
        for order in orders:
            self.submit_order(order)

    # PnlLedger 是策略运行期仓位的唯一数据源。
    def _net_qty(self, instrument_id: InstrumentId) -> Decimal:
        return self.pnl_ledgers[instrument_id].signed_qty

    def _inventory_balanced(self) -> bool:
        bn_qty = self._net_qty(self.binance_id)
        okx_qty = self._net_qty(self.okx_id)
        return bn_qty + okx_qty / self.okx_qty_multiplier == 0

    # 启动时用 collector 真实 bid/ask 初始化配置指定的分钟窗口。
    def _warm_initial_window(self) -> None:
        scan_end_ns = self.clock.timestamp_ns()
        scan_start_ns = scan_end_ns - (int(self.config.window_minutes) + 60) * MINUTE_NS
        collector_dir = Path(__file__).resolve().parent / "collector" / "bidask1-live"
        loader = WarmupLoader(collector_dir, self.asset)
        rows = loader.load(scan_start_ns, scan_end_ns)
        latest_event_ns = max(loader.event_ns(row) for row in rows)
        end_minute_ns = latest_event_ns // MINUTE_NS * MINUTE_NS - MINUTE_NS
        start_minute_ns = end_minute_ns - (int(self.config.window_minutes) - 1) * MINUTE_NS
        self.edge.warm_from_rows(rows, start_minute_ns, end_minute_ns, self.binance_id, self.okx_id)
        self._seed_initial_quotes(rows)
        self.log.info(
            f"initial_window asset={self.asset} rows={len(rows)} long_mean={self.edge.long_mean_bps:.2f} "
            f"short_mean={self.edge.short_mean_bps:.2f}",
        )

    # 用 warmup 最后一条 bid/ask 构造初始 QuoteTick。
    def _seed_initial_quotes(self, rows: list[dict[str, object]]) -> None:
        latest: dict[str, dict[str, object]] = {}
        for row in rows:
            venue = str(row["venue"]).upper()
            old = latest.get(venue)
            if old is None or WarmupLoader.event_ns(row) >= WarmupLoader.event_ns(old):
                latest[venue] = row
        ts_init = self.clock.timestamp_ns()
        for instrument_id in self.instrument_ids:
            venue = self._venue(instrument_id)
            row = latest[venue]
            instrument = self.cache.instrument(instrument_id)
            bid_size = Decimal(str(row["bid_size"]))
            ask_size = Decimal(str(row["ask_size"]))
            ts_event = WarmupLoader.event_ns(row)
            self.quotes[instrument_id] = QuoteTick(
                instrument_id=instrument_id,
                bid_price=instrument.make_price(Decimal(str(row["bid"]))),
                ask_price=instrument.make_price(Decimal(str(row["ask"]))),
                bid_size=instrument.make_qty(bid_size if bid_size > 0 else self.qty),
                ask_size=instrument.make_qty(ask_size if ask_size > 0 else self.qty),
                ts_event=ts_event,
                ts_init=ts_init,
            )
        self.edge.update(self.quotes[self.binance_id], self.quotes[self.okx_id])

    # 统一用 Binance 腿方向表示策略库存：正数 long，负数 short。
    def _inventory(self) -> Decimal:
        return -self._net_qty(self.binance_id)

    # pending 生命周期结束时统一固化阶段性数据并清理状态。
    def _finish_record(self, status: str) -> None:
        pending = self.pending
        row = self._action_row(pending, status)
        self.action_rows.appendleft(row)
        self.pending = None
        if status == "repair_unbalance":
            self.trade_state = STATE_UNBALANCE
        else:
            self._sync_state_from_inventory()
        if status == "filled":
            self.fail_count = 0
        elif self.fail_count > 2:
            self.mode = "suspend"
        if self.mode == "reduce" and self.trade_state == STATE_FLAT:
            self.mode = "normal"
            self.log.info("mode_normal reason=reduce_flat")
        self.coordinator.release(pending.reservation_id)
        self.coordinator.position_changed()

    # 记录一笔 pending 完成后的动作摘要。
    def _action_row(self, pending: PendingPair, status: str) -> dict[str, object]:
        best_edge = pending.best_edge_bps()
        actual_edge = pending.actual_edge_bps()
        edge_slippage = pending.edge_slippage_bps()
        fill_slippage = pending.fill_slippage_bps()
        submit_events = {"BINANCE": "-", "OKX": "-"}
        accept_events = {"BINANCE": "-", "OKX": "-"}
        fill_events = {"BINANCE": "-", "OKX": "-"}
        for leg in pending.legs.values():
            venue = self._venue(leg.instrument_id)
            if leg.submit_event_ns is not None:
                submit_events[venue] = str(leg.submit_event_ns)
            if leg.accept_event_ns is not None:
                accept_events[venue] = str(leg.accept_event_ns)
            if leg.full_fill_event_ns is not None:
                fill_events[venue] = str(leg.full_fill_event_ns)
        submit_event_values = [leg.submit_event_ns for leg in pending.legs.values() if leg.submit_event_ns is not None]
        time_text = "-"
        if submit_event_values:
            time_text = datetime.fromtimestamp(min(submit_event_values) / 1_000_000_000, tz=timezone.utc).astimezone(BEIJING_TZ).strftime("%m-%d %H:%M:%S")
        return {
            "metadata": {
                "signal_event_ns": str(pending.signal_event_ns),
                "signal_ts_ns": str(pending.signal_ts_ns),
                "on_quote_ns": str(pending.on_quote_ns),
                "signal_venue": pending.signal_venue,
                "bn_submit_event_ns": submit_events["BINANCE"],
                "okx_submit_event_ns": submit_events["OKX"],
                "bn_accept_event_ns": accept_events["BINANCE"],
                "okx_accept_event_ns": accept_events["OKX"],
                "bn_full_fill_event_ns": fill_events["BINANCE"],
                "okx_full_fill_event_ns": fill_events["OKX"],
            },
            "asset": self.asset,
            "action": self._display_action(pending.before_inventory, pending.after_inventory),
            "edge_side": pending.edge_side,
            "status": status,
            "qty": self._fmt(abs(self._inventory())),
            "signal_edge": self._fmt(pending.signal_edge_bps),
            "best_edge": self._fmt(best_edge),
            "actual_edge": self._fmt(actual_edge),
            "edge_slippage": self._fmt(edge_slippage),
            "fill_slippage": self._fmt(fill_slippage),
            "mean": self._fmt(pending.mean_bps),
            "std": self._fmt(pending.std_bps),
            "time": time_text,
        }

    # 按 venue 找账户，用于下单前余额检查。
    def _account_for_venue(self, venue: str):
        venue_text = venue.upper()
        for account in self.cache.accounts():
            if str(account.id).upper().startswith(venue_text):
                return account
        return None

    # 把 NT Money 转为 snapshot 使用的 Decimal。
    def _money_decimal(self, money) -> Decimal | None:
        if money is None:
            return None
        return money.as_decimal()

    # InstrumentId 的 venue 标准化为大写字符串。
    def _venue(self, instrument_id: InstrumentId) -> str:
        return str(instrument_id.venue).upper()

    # 根据库存变化显示 open/add/reduce/close。
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

    # snapshot 数值格式化；缺失值显示为短横线。
    def _fmt(self, value: object, suffix: str = "") -> str:
        if value is None:
            return "-"
        return f"{Decimal(str(value)):.2f}{suffix}"
