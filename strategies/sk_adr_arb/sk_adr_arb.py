from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from pathlib import Path

from nautilus_trader.config import StrategyConfig
from nautilus_trader.common.actor import Actor
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.events import (
    OrderCanceled,
    OrderDenied,
    OrderExpired,
    OrderFilled,
    OrderRejected,
)
from nautilus_trader.model.identifiers import InstrumentId
from utils.live_strategy import LiveStrategy, guarded_callback
from utils.wecom import WeComNotifier
from utils.execution import retry_delay_ns, order_alert_due
from utils.runtime_lock import RuntimeLock
from utils.state import read_state, write_state
from adapters.binance_account import NativeAccountWorker

from utils.constants import NODE_STOP_TOPIC
from utils.live_control import NodeStopRequest

ADR_COMMON_SHARE_RATIO = Decimal("0.1")
NANOSECONDS_PER_SECOND = 1_000_000_000


@dataclass
class PendingLeg:
    instrument_id: InstrumentId
    target_qty: Decimal
    filled_qty: Decimal = Decimal(0)
    terminal: bool = False


class SkAdrArbConfig(StrategyConfig, frozen=True):
    sk_instrument: str
    adr_instrument: str
    leverage: Decimal
    current_add_position: int
    first_add_position: int
    large_add_position: int
    max_add_position: int
    standard_stage_notional: Decimal
    large_stage_notional: Decimal
    order_notional: Decimal
    round_interval_sec: Decimal
    exit_retry_sec: Decimal
    price_check_interval_sec: Decimal
    max_mark_age_sec: Decimal
    notification_step: Decimal
    notification_path: str
    lock_path: str
    wecom_notifications: bool
    state_path: str
    account_environment: str
    capital_fraction: Decimal
    taker_fee: Decimal
    exit_premium: Decimal


class SkAdrArbStrategy(LiveStrategy):
    def __init__(self, config: SkAdrArbConfig) -> None:
        super().__init__(config)
        self.sk_id = InstrumentId.from_str(config.sk_instrument)
        self.adr_id = InstrumentId.from_str(config.adr_instrument)
        self.instrument_ids = (self.sk_id, self.adr_id)
        self.leverage = Decimal(str(config.leverage))
        self.add_position = config.current_add_position
        self.first_add_position = config.first_add_position
        self.large_add_position = config.large_add_position
        self.max_add_position = config.max_add_position
        self.standard_stage_notional = Decimal(str(config.standard_stage_notional))
        self.large_stage_notional = Decimal(str(config.large_stage_notional))
        self.order_notional = Decimal(str(config.order_notional))
        self.round_interval_ns = int(Decimal(str(config.round_interval_sec)) * NANOSECONDS_PER_SECOND)
        self.exit_retry_ns = int(Decimal(str(config.exit_retry_sec)) * NANOSECONDS_PER_SECOND)
        self.price_check_interval_ns = int(
            Decimal(str(config.price_check_interval_sec)) * NANOSECONDS_PER_SECOND,
        )
        self.max_mark_age_ns = int(Decimal(str(config.max_mark_age_sec)) * NANOSECONDS_PER_SECOND)
        self.notification_step = Decimal(str(config.notification_step))
        self.exit_premium = Decimal(str(config.exit_premium))
        if not Decimal(0) <= self.exit_premium < Decimal(self.first_add_position) / 100:
            raise ValueError("exit_premium must be below the first add threshold")
        notionals = (
            self.leverage,
            self.standard_stage_notional,
            self.large_stage_notional,
            self.order_notional,
        )
        if min(notionals) <= 0:
            raise ValueError("leverage and notionals must be positive")
        if not 0 < self.first_add_position < self.large_add_position <= self.max_add_position:
            raise ValueError("add positions are invalid")
        if self.add_position != 0 and not (
            self.first_add_position <= self.add_position <= self.max_add_position
        ):
            raise ValueError("current_add_position is invalid")
        if (
            self.standard_stage_notional % self.order_notional != 0
            or self.large_stage_notional % self.order_notional != 0
        ):
            raise ValueError("stage notionals must be multiples of order_notional")
        if min(self.round_interval_ns, self.exit_retry_ns, self.price_check_interval_ns, self.max_mark_age_ns) <= 0:
            raise ValueError("alert intervals and max_mark_age_sec are invalid")
        if self.notification_step <= 0:
            raise ValueError("notification step must be positive")
        if not 0 < config.capital_fraction < 1 or config.taker_fee < 0:
            raise ValueError('invalid capital fraction or taker fee')
        self.marks: dict[InstrumentId, MarkPriceUpdate] = {}
        self.pending: dict[str, PendingLeg] | None = None
        self.active_add_position: int | None = None
        self.stage_round = 0
        self.stage_rounds = 0
        self.ready = True
        self.halted = False
        self.alert_name: str | None = None
        self.alert_seq = 0
        self.round_count = 0
        self.notifier = None
        self.runtime_lock = None
        self.stopping = False
        self.fill_ids = set()
        self.order_checks = {}
        self.order_alert_ns = {}
        self.rollback_requested = False
        self.rollback_orders = {}
        self.rollback_failures = 0
        self.rollback_retry_ns = 0
        self.rollback_alert_ns = 0
        self.account_reader = None
        self.account_inflight = False
        self.account_retry_ns = 0
        self.available_margin = Decimal(0)
        self.capital_ready = False
        self.last_notify_premium: Decimal | None = None
        self.exit_requested = False
        self.exit_orders: list = []
        self.unresolved_exits = set()
        self.stop_requested = False
        self.flat_checks = 0
        self.next_exit_attempt_ns = 0
        self.next_add_attempt_ns = 0
        self.startup_failed = False

    def on_start(self) -> None:
        self.bind_owner()
        try:
            self._start_strategy()
        except Exception as exc:
            self.startup_failed = True
            self.stopping = True
            self.log.error(f'startup_failed error={type(exc).__name__} reason={exc}')
            self.event_loop.call_soon(self._abort_startup)

    def _abort_startup(self):
        self._assert_thread()
        Actor.stop(self)
        self.msgbus.publish(NODE_STOP_TOPIC, NodeStopRequest(source='sk_adr_arb', reason='startup_failed'))

    def _start_strategy(self):
        Path(self.config.lock_path).parent.mkdir(parents=True, exist_ok=True)
        self.runtime_lock = RuntimeLock(Path(self.config.lock_path))
        self._check_startup()
        checkpoint = read_state(self.config.state_path)
        if checkpoint:
            if checkpoint['strategy_id'] != str(self.id):
                raise RuntimeError('SK checkpoint identity mismatch')
            self.add_position = checkpoint['completed_threshold']
            self.active_add_position = checkpoint['active_threshold']
            self.stage_round, self.stage_rounds = checkpoint['round'], checkpoint['rounds']
            self.halted = checkpoint['halted'] or bool(checkpoint['pending'])
            self.exit_requested = checkpoint['exit_requested']
            actual = self._position_quantities()
            if checkpoint['pending'] or checkpoint.get('exit_orders') or checkpoint.get('quantities', actual) != actual:
                raise RuntimeError('SK checkpoint requires native order and position reconciliation')
        self.account_reader = NativeAccountWorker(self.config.account_environment, self._account_wakeup)
        self.account_reader.worker.start()
        self.msgbus.subscribe(f'events.order_resolution.{self.id}', self._receive_order_resolution)
        if self.config.wecom_notifications:
            try:
                self.notifier = WeComNotifier(Path(self.config.notification_path))
            except Exception as exc:
                self.log.error(f'notification_start_failed error={type(exc).__name__}')
        for instrument_id in self.instrument_ids:
            self.subscribe_mark_prices(instrument_id)
        self._schedule_attempt(self.price_check_interval_ns)
        self.log.info(
            f"started sk={self.sk_id} adr={self.adr_id} leverage={self.leverage} "
            f"add_position={self.add_position} first_add_position={self.first_add_position} "
            f"large_add_position={self.large_add_position} max_add_position={self.max_add_position} "
            f"standard_stage_notional={self.standard_stage_notional} "
            f"large_stage_notional={self.large_stage_notional} "
            f"order_notional={self.order_notional} exit_premium={self.exit_premium:.2%}",
        )

    def on_stop(self) -> None:
        self._assert_thread()
        # 人工停止不自动平仓；阈值退出路径在两腿归零后才请求停止 node。
        if self.alert_name is not None:
            self.cancel_timer_if_active(self.alert_name)
            self.alert_name = None
        self.stopping = True
        if self.msgbus.is_subscribed(f'events.order_resolution.{self.id}', self._receive_order_resolution):
            self.msgbus.unsubscribe(f'events.order_resolution.{self.id}', self._receive_order_resolution)
        if self.notifier is not None:
            try:
                self.notifier.close()
            except Exception as exc:
                self.log.error(f'notification_close_failed error={type(exc).__name__}')
            self.notifier = None
        if not self.startup_failed:
            self._save_checkpoint()
        if self.runtime_lock:
            self.runtime_lock.close()
            self.runtime_lock = None
        if self.account_reader:
            self.account_reader.close()

    def on_callback_error(self, name, error):
        self._pause(f'callback_error:{name}:{type(error).__name__}')

    @guarded_callback
    def on_order_filled(self, event: OrderFilled) -> None:
        self._assert_thread()
        self._audit('fill', order=str(event.client_order_id), trade_id=str(event.trade_id),
                    instrument=str(event.instrument_id), quantity=str(event.last_qty),
                    price=str(event.last_px), commission=str(event.commission), ts_event=event.ts_event)
        order = self.cache.order(event.client_order_id)
        if order and order.filled_qty >= order.quantity:
            self.unresolved_exits.discard(str(event.client_order_id))
        rollback = self.rollback_orders.get(str(event.client_order_id))
        if rollback is not None:
            key = (str(event.client_order_id), str(event.trade_id))
            if key not in self.fill_ids:
                self.fill_ids.add(key)
                rollback.filled_qty += event.last_qty.as_decimal()
                rollback.terminal = rollback.filled_qty >= rollback.target_qty
                self._save_checkpoint()
            return
        if self.pending is None:
            return
        leg = self.pending.get(str(event.client_order_id))
        if leg is None:
            return
        key = (str(event.client_order_id), str(event.trade_id))
        if key in self.fill_ids:
            return
        self.fill_ids.add(key)
        leg.filled_qty += event.last_qty.as_decimal()
        leg.terminal = leg.filled_qty >= leg.target_qty
        # 本轮所有订单都累计到各自目标数量，才允许进入下一轮等待。
        if not all(item.filled_qty >= item.target_qty for item in self.pending.values()):
            self._save_checkpoint()
            return

        self.pending = None
        self.round_count += 1
        self.stage_round += 1
        self.log.info(
            f"orders_filled round={self.round_count} add_position={self.active_add_position} "
            f"stage_round={self.stage_round}/{self.stage_rounds} margin={self._margin_used():.4f}",
        )
        if self.stage_round >= self.stage_rounds:
            completed_position = self.active_add_position
            if completed_position is None:
                self._pause("completed_stage_missing_add_position")
                return
            self.add_position = completed_position
            self.active_add_position = None
            self.stage_round = 0
            self.stage_rounds = 0
            self.capital_ready = False
            self.log.warning(f"add_stage_completed add_position={self.add_position}")
        # 加仓仍间隔五秒，但等待期间继续检查退出阈值。
        self.next_add_attempt_ns = self.clock.timestamp_ns() + self.round_interval_ns
        self._save_checkpoint()
        self._schedule_attempt(self.price_check_interval_ns)

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._handle_order_failure(str(event.client_order_id), "rejected")

    def on_order_denied(self, event: OrderDenied) -> None:
        self._handle_order_failure(str(event.client_order_id), "denied")

    def on_order_canceled(self, event: OrderCanceled) -> None:
        self._handle_order_failure(str(event.client_order_id), "canceled")

    def on_order_expired(self, event: OrderExpired) -> None:
        self._handle_order_failure(str(event.client_order_id), "expired")

    def _check_startup(self) -> None:
        account = next(
            (item for item in self.cache.accounts() if str(item.id).upper().startswith("BINANCE")),
            None,
        )
        if account is None:
            raise RuntimeError("startup_binance_account_missing")
        signed_qty: dict[InstrumentId, Decimal] = {}
        for instrument_id in self.instrument_ids:
            if self.cache.instrument(instrument_id) is None:
                raise RuntimeError(f"startup_instrument_missing instrument={instrument_id}")
            actual_leverage = account.leverage(instrument_id)
            if actual_leverage != self.leverage:
                raise RuntimeError(
                    f"startup_wrong_leverage instrument={instrument_id} "
                    f"expected={self.leverage} actual={actual_leverage}",
                )
            open_orders = self.cache.orders_open(instrument_id=instrument_id)
            if open_orders:
                ids = ",".join(str(order.client_order_id) for order in open_orders)
                raise RuntimeError(f"startup_open_orders instrument={instrument_id} orders={ids}")
            positions = self.cache.positions_open(instrument_id=instrument_id)
            quantities = [position.signed_decimal_qty() for position in positions]
            has_long = any(quantity > 0 for quantity in quantities)
            has_short = any(quantity < 0 for quantity in quantities)
            if has_long and has_short:
                raise RuntimeError(
                    f"startup_offsetting_positions instrument={instrument_id} quantities={quantities}",
                )
            signed_qty[instrument_id] = sum(quantities, Decimal(0))
            if len(positions) > 1:
                # NT reconciliation can split one venue-side position across same-side position IDs.
                self.log.warning(
                    f"startup_multiple_same_side_positions instrument={instrument_id} "
                    f"count={len(positions)} aggregate_qty={signed_qty[instrument_id]}",
                )

        sk_qty = signed_qty[self.sk_id]
        adr_qty = signed_qty[self.adr_id]
        if (sk_qty == 0) != (adr_qty == 0):
            raise RuntimeError(f"startup_unpaired_position sk_qty={sk_qty} adr_qty={adr_qty}")
        if sk_qty < 0 or adr_qty > 0:
            raise RuntimeError(f"startup_wrong_position_sides sk_qty={sk_qty} adr_qty={adr_qty}")

    def _try_submit_pair(self) -> None:
        if self.exit_requested:
            self._try_exit()
            return
        if not self._marks_are_current():
            return
        sk_mark = self.marks[self.sk_id]
        adr_mark = self.marks[self.adr_id]
        sk_price = sk_mark.value.as_decimal()
        adr_price = adr_mark.value.as_decimal()
        if sk_price <= 0 or adr_price <= 0:
            return

        premium = adr_price / ADR_COMMON_SHARE_RATIO / sk_price - Decimal(1)
        self._notify_market(sk_price, adr_price, premium)

        # 退出优先于在途加仓、加仓等待和暂停状态；触发后不可恢复加仓。
        if self._trigger_exit(premium):
            self._try_exit()
            return
        if (
            self.halted
            or self.rollback_requested
            or not self.ready
            or self.pending is not None
            or self.clock.timestamp_ns() < self.next_add_attempt_ns
        ):
            return
        if not self._prepare_add_stage(premium):
            return

        sk_instrument = self.cache.instrument(self.sk_id)
        adr_instrument = self.cache.instrument(self.adr_id)
        try:
            quantities = {
                self.sk_id: self._add_qty(sk_instrument, self.order_notional, sk_price),
                self.adr_id: self._add_qty(adr_instrument, self.order_notional, adr_price),
            }
        except ValueError:
            self._pause(f"open_quantity_invalid add_position={self.active_add_position}")
            return
        orders = []
        pending = {}
        sides = {self.sk_id: OrderSide.BUY, self.adr_id: OrderSide.SELL}
        instruments = {self.sk_id: sk_instrument, self.adr_id: adr_instrument}
        marks = {self.sk_id: sk_mark.value, self.adr_id: adr_mark.value}
        for instrument_id in self.instrument_ids:
            quantity = quantities[instrument_id]
            if not self._meets_minimum(instruments[instrument_id], quantity, marks[instrument_id]):
                self._pause(f"open_quantity_below_minimum instrument={instrument_id}")
                return
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=sides[instrument_id],
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
            )
            orders.append(order)
            pending[str(order.client_order_id)] = PendingLeg(instrument_id, quantity.as_decimal())
        if not orders:
            raise RuntimeError("open target produced no orders")

        self.ready = False
        self.pending = pending
        margin = sum((quantity.as_decimal() * self.marks[iid].value.as_decimal() *
                      (1 / self.leverage + 2 * self.config.taker_fee)
                      for iid, quantity in quantities.items()), Decimal(0))
        if margin > self.available_margin:
            self.pending = None
            self._pause('native_available_margin_insufficient')
            return
        self.available_margin -= margin
        if not self._save_checkpoint():
            self.pending = None
            return
        self.log.info(
            f"submit_pair add_position={self.active_add_position} "
            f"stage_round={self.stage_round + 1}/{self.stage_rounds} "
            f"premium={premium:.4%} order_notional={self.order_notional:.4f}",
        )
        for order in orders:
            try:
                self.submit_order(order)
            except Exception as exc:
                if self.cache.order(order.client_order_id) is None:
                    self.pending[str(order.client_order_id)].terminal = True
                self.rollback_requested = True
                self._pause(f'order_submit_error:{type(exc).__name__}')
        self._audit('orders_sent', orders=[str(order.client_order_id) for order in orders],
                    threshold=self.active_add_position, round=self.stage_round)

    def _trigger_exit(self, premium):
        if premium > self.exit_premium:
            return False
        self.exit_requested = True
        self.log.warning(f'exit_triggered premium={premium:.4%} threshold={self.exit_premium:.2%}')
        self._save_checkpoint()
        for instrument_id in self.instrument_ids:
            self.cancel_all_orders(instrument_id)
        return True

    def _try_exit(self) -> None:
        if self.stop_requested:
            return
        if self.unresolved_exits:
            return
        if any(not leg.terminal for leg in self.rollback_orders.values()):
            return
        if self.pending and any(not leg.terminal for leg in self.pending.values()):
            return
        # 等待上一批及正在撤销的加仓订单进入终态，避免重复平仓。
        if any(not order.is_closed for order in self.exit_orders):
            return
        for instrument_id in self.instrument_ids:
            if (
                self.cache.orders_open(instrument_id=instrument_id)
                or self.cache.orders_inflight(instrument_id=instrument_id)
            ):
                return
        positions = [
            position
            for instrument_id in self.instrument_ids
            for position in self.cache.positions_open(instrument_id=instrument_id)
            if position.signed_decimal_qty() != 0
        ]
        if not positions:
            # 成交回调之后再核对两次缓存，确认两腿均无仓位、无在途订单。
            self.flat_checks += 1
            if self.flat_checks >= 2:
                self.stop_requested = True
                self.log.warning("exit_completed both_legs_flat; stopping node")
                self.msgbus.publish(
                    NODE_STOP_TOPIC,
                    NodeStopRequest(source="sk_adr_arb", reason="exit_premium_both_legs_flat"),
                )
            return
        self.flat_checks = 0
        if self.clock.timestamp_ns() < self.next_exit_attempt_ns:
            return

        # 同一事件循环内提交两腿；按缓存 position_id 逐笔关闭，兼容重启对账拆仓。
        orders = []
        for position in positions:
            signed_qty = position.signed_decimal_qty()
            instrument = self.cache.instrument(position.instrument_id)
            quantity = instrument.make_qty(abs(signed_qty))
            if quantity.as_decimal() != abs(signed_qty):
                self.log.error(f"exit_quantity_not_exact position={position.id}")
                self.next_exit_attempt_ns = self.clock.timestamp_ns() + self.exit_retry_ns
                return
            order = self.order_factory.market(
                instrument_id=position.instrument_id,
                order_side=OrderSide.SELL if signed_qty > 0 else OrderSide.BUY,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
                reduce_only=True,
            )
            orders.append((order, position.id))
        self.exit_orders = [order for order, _ in orders]
        self.next_exit_attempt_ns = self.clock.timestamp_ns() + self.exit_retry_ns
        for order, position_id in orders:
            self.log.warning(
                f"exit_submit instrument={order.instrument_id} qty={order.quantity} "
                f"position={position_id} reduce_only=True",
            )
            self.submit_order(order, position_id=position_id)

    def _prepare_add_stage(self, premium: Decimal) -> bool:
        if self.active_add_position is not None and self.capital_ready:
            return True
        next_position = self.first_add_position if self.add_position == 0 else self.add_position + 1
        if next_position > self.max_add_position:
            return False
        if self.active_add_position is None and premium <= Decimal(next_position) / Decimal(100):
            return False
        if not self.capital_ready:
            if not self.account_inflight and self.clock.timestamp_ns() >= self.account_retry_ns:
                self.account_inflight = True
                self.account_reader.request(1)
            return False
        if self.active_add_position is not None:
            return True
        stage_notional = (
            self.large_stage_notional
            if next_position >= self.large_add_position
            else self.standard_stage_notional
        )
        self.active_add_position = next_position
        self.stage_round = 0
        self.stage_rounds = int(stage_notional / self.order_notional)
        self.log.warning(
            f"add_stage_entered add_position={next_position} premium={premium:.4%} "
            f"stage_notional={stage_notional:.4f} rounds={self.stage_rounds}",
        )
        return True

    def _try_rollback(self):
        if (self.pending and any(not leg.terminal for leg in self.pending.values())) or any(
                not leg.terminal for leg in self.rollback_orders.values()):
            return
        now = self.clock.timestamp_ns()
        if now < self.rollback_retry_ns:
            return
        orders = []
        for iid in self.instrument_ids:
            opened = sum((leg.filled_qty for leg in (self.pending or {}).values() if leg.instrument_id == iid), Decimal(0))
            reverted = sum((leg.filled_qty for leg in self.rollback_orders.values() if leg.instrument_id == iid), Decimal(0))
            remaining = opened - reverted
            if remaining <= 0:
                continue
            instrument = self.cache.instrument(iid)
            quantity = instrument.make_qty(remaining)
            held = sum((p.signed_decimal_qty() for p in self.cache.positions_open(instrument_id=iid)), Decimal(0))
            sign = Decimal(1) if iid == self.sk_id else Decimal(-1)
            if quantity.as_decimal() != remaining or held * sign < remaining:
                self.rollback_retry_ns = now + 5_000_000_000
                self.log.error(f'rollback_position_review_required instrument={iid} remaining={remaining} held={held}')
                if not self.rollback_alert_ns or now - self.rollback_alert_ns >= 300_000_000_000:
                    self.rollback_alert_ns = now
                    if self.notifier:
                        try:
                            self.notifier.send(f'{self.id}:rollback_review:{now}',
                                f'SK/ADR 回退仓位需核对\n合约 {iid}\n待回退 {remaining}\n实际持仓 {held}')
                        except Exception as exc:
                            self.log.error(f'notification_send_failed error={type(exc).__name__}')
                return
            order = self.order_factory.market(instrument_id=iid, quantity=quantity,
                order_side=OrderSide.SELL if iid == self.sk_id else OrderSide.BUY, reduce_only=True)
            orders.append(order)
        if not orders:
            self.rollback_requested = False
            self.pending = None
            self.rollback_orders.clear()
            self._audit('rollback_completed')
            self._save_checkpoint()
            return
        for order in orders:
            self.rollback_orders[str(order.client_order_id)] = PendingLeg(order.instrument_id, order.quantity.as_decimal())
        if not self._save_checkpoint():
            for order in orders:
                self.rollback_orders.pop(str(order.client_order_id))
            return
        self._audit('rollback_submitted', orders=[str(order.client_order_id) for order in orders])
        for order in orders:
            try:
                self.submit_order(order)
            except Exception as exc:
                if self.cache.order(order.client_order_id) is None:
                    self.rollback_orders[str(order.client_order_id)].terminal = True
                self.rollback_failures += 1
                self.rollback_retry_ns = now + retry_delay_ns(self.rollback_failures)
                self.log.error(f'rollback_submit_error error={type(exc).__name__}')

    def _account_wakeup(self):
        if not self.stopping:
            self.dispatch_to_owner(self._receive_account)

    def _receive_account(self):
        self._assert_thread()
        if self.stopping:
            return
        while not self.account_reader.results.empty():
            _, snapshot, error = self.account_reader.results.get_nowait()
            self.account_inflight = False
            if error:
                self.account_retry_ns = self.clock.timestamp_ns() + 5_000_000_000
                self.log.warning(f'native_capital_unavailable error={error}')
                continue
            if self.clock.timestamp_ns() - snapshot.received_ns > 10_000_000_000:
                self.account_retry_ns = self.clock.timestamp_ns() + 5_000_000_000
                continue
            for iid in self.instrument_ids:
                symbol = str(iid.symbol).removesuffix('-PERP')
                native = snapshot.configs.get(symbol, {})
                if Decimal(str(native.get('leverage', 0))) != self.leverage or native.get('marginType') != 'CROSSED':
                    self._pause('native_account_configuration_mismatch')
                    return
            self.available_margin = Decimal(str(snapshot.available)) * self.config.capital_fraction
            self.capital_ready = True

    def _save_checkpoint(self):
        try:
            write_state(self.config.state_path, {'version': 1, 'strategy_id': str(self.id),
                'completed_threshold': self.add_position, 'active_threshold': self.active_add_position,
                'round': self.stage_round, 'rounds': self.stage_rounds,
                'pending': {key: {'instrument': str(leg.instrument_id), 'target': str(leg.target_qty),
                                  'filled': str(leg.filled_qty), 'terminal': leg.terminal}
                            for key, leg in (self.pending or {}).items() if not leg.terminal or self.rollback_requested},
                'exit_orders': sorted(self.unresolved_exits | {str(order.client_order_id) for order in self.exit_orders if not order.is_closed}
                                      | {key for key, leg in self.rollback_orders.items() if not leg.terminal}),
                'quantities': self._position_quantities(),
                'halted': self.halted, 'exit_requested': self.exit_requested})
            return True
        except (OSError, ValueError, TypeError) as exc:
            self.halted = True
            self.log.error(f'checkpoint_failed error={type(exc).__name__}')
            return False

    def _position_quantities(self):
        return {str(iid): str(sum((p.signed_decimal_qty() for p in self.cache.positions_open(instrument_id=iid)), Decimal(0)))
                for iid in self.instrument_ids}

    def _audit(self, kind, **values):
        self.log.info(json.dumps({'kind': kind, 'time_ns': self.clock.timestamp_ns(), **values}, separators=(',', ':')))

    # 市价单数量向上对齐合约步长，使每腿至少达到本轮名义金额。
    @staticmethod
    def _add_qty(instrument, notional: Decimal, price: Decimal):
        raw_qty = notional / price
        if instrument.min_quantity is not None:
            raw_qty = max(raw_qty, instrument.min_quantity.as_decimal())
        if instrument.min_notional is not None:
            raw_qty = max(raw_qty, instrument.min_notional.as_decimal() / price)
        increment = instrument.size_increment.as_decimal()
        quantity = (raw_qty / increment).to_integral_value(rounding=ROUND_CEILING) * increment
        return instrument.make_qty(quantity)

    # 用单次 alert 控制下一次价格检查，避免按行情事件运行策略逻辑。
    def _schedule_attempt(self, delay_ns: int) -> None:
        if self.stop_requested:
            return
        if self.alert_name is not None:
            self.cancel_timer_if_active(self.alert_name)
        self.alert_seq += 1
        self.alert_name = f"sk_adr_price_check_{self.alert_seq}"
        self.clock.set_time_alert_ns(
            self.alert_name,
            self.clock.timestamp_ns() + delay_ns,
            callback=self._timer_event,
            allow_past=True,
        )

    def _timer_event(self, event):
        if not self.stopping:
            self.dispatch_to_owner(self._handle_timer, event)

    def _handle_timer(self, event):
        self._assert_thread()
        if not self.stopping and event.name == self.alert_name:
            try:
                self._on_attempt_alert()
            except Exception as exc:
                self._pause(f'timer_error:{type(exc).__name__}')

    def _resolve_pending_orders(self):
        now = self.clock.timestamp_ns()
        orders = {str(order.client_order_id): order for order in self.exit_orders}
        for key in set(self.pending or {}) | set(self.rollback_orders):
            from nautilus_trader.model.identifiers import ClientOrderId
            order = self.cache.order(ClientOrderId(key))
            if order:
                orders[key] = order
        for key, order in orders.items():
            leg = (self.pending or {}).get(key) or self.rollback_orders.get(key)
            if order.is_closed and (leg is None or leg.terminal) and key not in self.unresolved_exits:
                continue
            checked, attempts = self.order_checks.get(key, (order.ts_init, 0))
            if now - checked < retry_delay_ns(attempts + 1):
                continue
            self.order_checks[key] = (now, attempts + 1)
            self.query_order(order)
            if order_alert_due(order.ts_init, self.order_alert_ns.get(key, 0), now):
                self.order_alert_ns[key] = now
                self._audit('order_resolution_alert', order=key, instrument=str(order.instrument_id))
                if self.notifier:
                    try:
                        self.notifier.send(f'{self.id}:{key}:unresolved:{now}',
                            f'SK/ADR 订单核单超过60秒\n订单 {key}\n合约 {order.instrument_id}\n仓位 {self._position_quantities()}')
                    except Exception as exc:
                        self.log.error(f'notification_send_failed error={type(exc).__name__}')

    @guarded_callback
    def _receive_order_resolution(self, resolution):
        self._assert_thread()
        if self.stopping:
            return
        if resolution.client_order_id in self.rollback_orders:
            leg = self.rollback_orders[resolution.client_order_id]
            leg.filled_qty = resolution.filled
            leg.terminal = True
            if leg.filled_qty < leg.target_qty:
                self.rollback_failures += 1
                self.rollback_retry_ns = self.clock.timestamp_ns() + retry_delay_ns(self.rollback_failures)
            self._save_checkpoint()
            return
        if resolution.client_order_id in self.unresolved_exits:
            self.unresolved_exits.discard(resolution.client_order_id)
            self._save_checkpoint()
        if self.pending and resolution.client_order_id in self.pending:
            leg = self.pending[resolution.client_order_id]
            if leg.filled_qty != resolution.filled:
                leg.filled_qty = resolution.filled
                leg.terminal = True
                self.rollback_requested = True
                self._pause('native_fill_quantity_mismatch')
            elif leg.filled_qty < leg.target_qty:
                leg.terminal = True
                self._handle_order_failure(resolution.client_order_id, resolution.status.lower())
            self._audit('order_resolved', order=resolution.client_order_id, status=resolution.status,
                        native_filled=str(resolution.filled))
            self._save_checkpoint()

    # 每秒监控退出条件，包括在途订单、加仓等待或已暂停加仓时。
    def _on_attempt_alert(self) -> None:
        self._assert_thread()
        self.alert_name = None
        self._receive_account()
        self._resolve_pending_orders()
        marks = {iid: self.cache.mark_price(iid) for iid in self.instrument_ids}
        marks_ready = all(mark is not None for mark in marks.values())
        if marks_ready:
            self.marks = marks
            self.ready = True
        if self.rollback_requested and not self.exit_requested and marks_ready and self._marks_are_current():
            sk_price = self.marks[self.sk_id].value.as_decimal()
            if sk_price > 0:
                premium = self.marks[self.adr_id].value.as_decimal() / ADR_COMMON_SHARE_RATIO / sk_price - 1
                self._trigger_exit(premium)
        if self.exit_requested:
            # 触发后不再依赖行情；断流或溢价回升都不能中断平仓。
            self._try_exit()
        elif self.rollback_requested:
            self._try_rollback()
        elif marks_ready:
            self._try_submit_pair()
        if not self.stop_requested:
            self._schedule_attempt(self.price_check_interval_ns)

    def _marks_are_current(self) -> bool:
        if any(instrument_id not in self.marks for instrument_id in self.instrument_ids):
            return False
        now_ns = self.clock.timestamp_ns()
        return all(
            0 <= now_ns - int(self.marks[instrument_id].ts_init) <= self.max_mark_age_ns
            for instrument_id in self.instrument_ids
        )

    # 首次以及较上次通知变化达到设定百分点时推送行情。
    def _notify_market(self, sk_price: Decimal, adr_price: Decimal, premium: Decimal) -> None:
        if self.notifier is None:
            return
        if self.last_notify_premium is None:
            self._send_market_notice("策略启动", sk_price, adr_price, premium)
            return

        moved = abs(premium - self.last_notify_premium) >= self.notification_step
        if moved:
            direction = "价差扩大" if premium > self.last_notify_premium else "价差收窄"
            self._send_market_notice(direction, sk_price, adr_price, premium)

    def _send_market_notice(
        self,
        trigger: str,
        sk_price: Decimal,
        adr_price: Decimal,
        premium: Decimal,
    ) -> None:
        previous = self.last_notify_premium
        change = Decimal(0) if previous is None else premium - previous
        message = (
            f"SK/ADR {trigger}\n"
            f"SKHYNIX 标记价: {sk_price}\n"
            f"SKHY ADR 标记价: {adr_price}\n"
            f"换算溢价: {premium:.2%}\n"
            f"较上次通知: {change:+.2%}"
        )
        try:
            self.notifier.send(f'{self.id}:{self.clock.timestamp_ns()}', message)
        except Exception as exc:
            self.log.error(f'notification_send_failed error={type(exc).__name__}')
        self.last_notify_premium = premium

    def _margin_used(self) -> Decimal:
        sk_notional, adr_notional = self._position_notionals()
        return (sk_notional + adr_notional) / self.leverage

    # 按最新标记价格计算两腿当前仓位的 USDT 名义价值。
    def _position_notionals(self) -> tuple[Decimal, Decimal]:
        notionals: dict[InstrumentId, Decimal] = {}
        for instrument_id in self.instrument_ids:
            mark = self.marks.get(instrument_id)
            if mark is None:
                notionals[instrument_id] = Decimal(0)
                continue
            positions = self.cache.positions_open(instrument_id=instrument_id)
            signed_quantity = sum(
                (position.signed_decimal_qty() for position in positions),
                Decimal(0),
            )
            notionals[instrument_id] = abs(signed_quantity) * mark.value.as_decimal()
        return notionals[self.sk_id], notionals[self.adr_id]

    @staticmethod
    def _meets_minimum(instrument, quantity, price) -> bool:
        if instrument.min_quantity is not None and quantity < instrument.min_quantity:
            return False
        minimum = instrument.min_notional
        return minimum is None or instrument.notional_value(quantity, price) >= minimum

    def _handle_order_failure(self, order_id: str, status: str) -> None:
        self._assert_thread()
        self._audit('order_failure', order=order_id, status=status)
        if order_id in self.rollback_orders:
            leg = self.rollback_orders[order_id]
            if status in ('canceled', 'expired'):
                return
            leg.terminal = True
            self.rollback_failures += 1
            self.rollback_retry_ns = self.clock.timestamp_ns() + retry_delay_ns(self.rollback_failures)
            self._save_checkpoint()
            return
        leg = (self.pending or {}).get(order_id)
        if leg and status in ('denied', 'rejected'):
            leg.terminal = True
        if leg and status in ('canceled', 'expired') and not leg.terminal:
            return
        if status in ('canceled', 'expired') and any(str(order.client_order_id) == order_id for order in self.exit_orders):
            self.unresolved_exits.add(order_id)
            self._save_checkpoint()
            return
        if self.exit_requested:
            self.log.warning(
                f"exit_order_{status} order={order_id}; recheck remaining positions before retry",
            )
            self.next_exit_attempt_ns = self.clock.timestamp_ns() + self.exit_retry_ns
            return
        if self.pending is None or order_id not in self.pending:
            return
        self.rollback_requested = True
        self._pause(f"pair_order_{status} order={order_id}")
        for instrument_id in self.instrument_ids:
            self.cancel_all_orders(instrument_id)

    # 订单异常暂停加仓，但保持退出阈值监控。
    def _pause(self, reason: str) -> None:
        self.halted = True
        self.ready = False
        if self.alert_name is not None:
            self.cancel_timer_if_active(self.alert_name)
            self.alert_name = None
        self.log.error(f"strategy_paused reason={reason}; exit monitoring remains active")
        if self.notifier:
            try:
                self.notifier.send(f'{self.id}:pause:{self.clock.timestamp_ns()}',
                                   f'SK/ADR 暂停加仓\n{reason}\n仓位 {self._position_quantities()}')
            except Exception as exc:
                self.log.error(f'notification_send_failed error={type(exc).__name__}')
        self._save_checkpoint()
        self._schedule_attempt(self.price_check_interval_ns)
