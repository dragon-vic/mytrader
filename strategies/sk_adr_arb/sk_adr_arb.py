from __future__ import annotations

import os
import queue
import threading
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from pathlib import Path

import requests
from dotenv import load_dotenv
from nautilus_trader.config import StrategyConfig
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
from nautilus_trader.trading.strategy import Strategy

ADR_COMMON_SHARE_RATIO = Decimal("0.1")
NANOSECONDS_PER_SECOND = 1_000_000_000
ROOT = Path(__file__).resolve().parents[2]


@dataclass
class PendingLeg:
    instrument_id: InstrumentId
    target_qty: Decimal
    filled_qty: Decimal = Decimal(0)


class TelegramSender:
    def __init__(self, token: str, chat_id: str) -> None:
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.messages: queue.Queue[str | None] = queue.Queue()
        self.errors: queue.SimpleQueue[str] = queue.SimpleQueue()
        self.worker = threading.Thread(target=self._run, name="sk-adr-telegram", daemon=True)
        self.worker.start()

    def send(self, message: str) -> None:
        self.messages.put_nowait(message)

    def close(self) -> None:
        self.messages.put_nowait(None)

    def pop_error(self) -> str | None:
        try:
            return self.errors.get_nowait()
        except queue.Empty:
            return None

    # Telegram 网络请求放在后台线程，不能阻塞策略的价格检查和下单。
    def _run(self) -> None:
        session = requests.Session()
        while True:
            message = self.messages.get()
            if message is None:
                return
            try:
                response = session.post(
                    self.url,
                    data={"chat_id": self.chat_id, "text": message},
                    timeout=5,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                # 异常文本可能包含带 token 的 URL，只记录异常类型避免泄露凭据。
                self.errors.put(type(exc).__name__)


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
    retry_delay_sec: Decimal
    price_check_interval_sec: Decimal
    max_mark_age_sec: Decimal
    telegram_notify_step: Decimal


class SkAdrArbStrategy(Strategy):
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
        self.retry_delay_ns = int(Decimal(str(config.retry_delay_sec)) * NANOSECONDS_PER_SECOND)
        self.price_check_interval_ns = int(
            Decimal(str(config.price_check_interval_sec)) * NANOSECONDS_PER_SECOND,
        )
        self.max_mark_age_ns = int(Decimal(str(config.max_mark_age_sec)) * NANOSECONDS_PER_SECOND)
        self.telegram_notify_step = Decimal(str(config.telegram_notify_step))
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
        if self.retry_delay_ns <= 0 or self.price_check_interval_ns <= 0 or self.max_mark_age_ns <= 0:
            raise ValueError("alert intervals and max_mark_age_sec are invalid")
        if self.telegram_notify_step <= 0:
            raise ValueError("telegram notification step must be positive")
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
        self.telegram: TelegramSender | None = None
        self.last_notify_premium: Decimal | None = None

    def on_start(self) -> None:
        self._check_startup()
        load_dotenv(ROOT / ".env")
        self.telegram = TelegramSender(
            token=os.environ["TELEGRAM_MONIOR_BOT_TOKEN"],
            chat_id=os.environ["TELEGRAM_MONIOR_CHAT_ID"],
        )
        for instrument_id in self.instrument_ids:
            self.subscribe_mark_prices(instrument_id)
        self._schedule_attempt(self.price_check_interval_ns)
        self.log.info(
            f"started sk={self.sk_id} adr={self.adr_id} leverage={self.leverage} "
            f"add_position={self.add_position} first_add_position={self.first_add_position} "
            f"large_add_position={self.large_add_position} max_add_position={self.max_add_position} "
            f"standard_stage_notional={self.standard_stage_notional} "
            f"large_stage_notional={self.large_stage_notional} "
            f"order_notional={self.order_notional}",
        )

    def on_stop(self) -> None:
        # 停止策略时只退出，不自动平仓；现有仓位由人工处理。
        if self.alert_name is not None:
            self.clock.cancel_timer(self.alert_name)
            self.alert_name = None
        if self.telegram is not None:
            self.telegram.close()
            self.telegram = None

    def on_order_filled(self, event: OrderFilled) -> None:
        if self.pending is None:
            return
        leg = self.pending.get(str(event.client_order_id))
        if leg is None:
            return
        leg.filled_qty += event.last_qty.as_decimal()
        # 本轮所有订单都累计到各自目标数量，才允许进入下一轮等待。
        if not all(item.filled_qty >= item.target_qty for item in self.pending.values()):
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
            self.log.warning(f"add_stage_completed add_position={self.add_position}")
        # 任意两组订单之间至少等待五秒，包括跨档位连续补仓。
        self._schedule_attempt(self.retry_delay_ns)

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
        if self.halted or not self.ready or self.pending is not None or not self._marks_are_current():
            return
        sk_mark = self.marks[self.sk_id]
        adr_mark = self.marks[self.adr_id]
        sk_price = sk_mark.value.as_decimal()
        adr_price = adr_mark.value.as_decimal()
        if sk_price <= 0 or adr_price <= 0:
            return

        premium = adr_price / ADR_COMMON_SHARE_RATIO / sk_price - Decimal(1)
        self._notify_market(sk_price, adr_price, premium)

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
        self.log.info(
            f"submit_pair add_position={self.active_add_position} "
            f"stage_round={self.stage_round + 1}/{self.stage_rounds} "
            f"premium={premium:.4%} order_notional={self.order_notional:.4f}",
        )
        for order in orders:
            if self.halted:
                break
            self.submit_order(order)

    def _prepare_add_stage(self, premium: Decimal) -> bool:
        if self.active_add_position is not None:
            return True
        next_position = self.first_add_position if self.add_position == 0 else self.add_position + 1
        if next_position > self.max_add_position:
            return False
        if premium <= Decimal(next_position) / Decimal(100):
            return False
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
        if self.halted:
            return
        self.alert_seq += 1
        self.alert_name = f"sk_adr_price_check_{self.alert_seq}"
        self.clock.set_time_alert_ns(
            self.alert_name,
            self.clock.timestamp_ns() + delay_ns,
            callback=lambda _event: self._on_attempt_alert(),
            allow_past=True,
        )

    # alert 到点后只从 NT cache 读取最新标记价格；未触发交易则一秒后再检查。
    def _on_attempt_alert(self) -> None:
        self.alert_name = None
        marks = {
            instrument_id: self.cache.mark_price(instrument_id)
            for instrument_id in self.instrument_ids
        }
        if any(mark is None for mark in marks.values()):
            self._schedule_attempt(self.price_check_interval_ns)
            return
        self.marks = marks
        self.ready = True
        self._try_submit_pair()
        if self.ready and self.pending is None and not self.halted:
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
        if self.telegram is None:
            return
        error = self.telegram.pop_error()
        if error is not None:
            self.log.warning(f"telegram_send_failed error={error}")

        if self.last_notify_premium is None:
            self._send_market_notice("策略启动", sk_price, adr_price, premium)
            return

        moved = abs(premium - self.last_notify_premium) >= self.telegram_notify_step
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
        self.telegram.send(message)
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
        if self.halted or self.pending is None or order_id not in self.pending:
            return
        self._pause(f"pair_order_{status} order={order_id}")
        for instrument_id in self.instrument_ids:
            self.cancel_all_orders(instrument_id)

    # 订单异常时只暂停本策略并保留 node，等待人工处理现有敞口。
    def _pause(self, reason: str) -> None:
        self.halted = True
        self.ready = False
        if self.alert_name is not None:
            self.clock.cancel_timer(self.alert_name)
            self.alert_name = None
        self.log.error(f"strategy_paused reason={reason}; manual intervention required")
