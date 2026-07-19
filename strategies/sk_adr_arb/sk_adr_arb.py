from __future__ import annotations

import json
import os
import queue
import threading
from dataclasses import dataclass
from decimal import Decimal
from decimal import ROUND_CEILING
from pathlib import Path

import requests
from dotenv import load_dotenv
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from utils.constants import NODE_STOP_TOPIC
from utils.live_control import NodeStopRequest


ADR_COMMON_SHARE_RATIO = Decimal("0.1")
NANOSECONDS_PER_SECOND = 1_000_000_000
ROOT = Path(__file__).resolve().parents[2]


@dataclass
class PendingLeg:
    instrument_id: InstrumentId
    target_qty: Decimal
    filled_qty: Decimal = Decimal("0")


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
    add_step_notional: Decimal
    tier_target_notional: Decimal
    max_target_notional: Decimal
    final_target_notional: Decimal
    tier_one_premium: Decimal
    tier_two_premium: Decimal
    tier_three_premium: Decimal
    close_premium: Decimal
    close_leg_notional: Decimal
    rebalance_threshold: Decimal
    retry_delay_sec: Decimal
    price_check_interval_sec: Decimal
    max_mark_age_sec: Decimal
    telegram_notify_step: Decimal
    initial_completed_stage: int
    stage_snapshot_path: str


class SkAdrArbStrategy(Strategy):
    def __init__(self, config: SkAdrArbConfig) -> None:
        super().__init__(config)
        self.sk_id = InstrumentId.from_str(config.sk_instrument)
        self.adr_id = InstrumentId.from_str(config.adr_instrument)
        self.instrument_ids = (self.sk_id, self.adr_id)
        self.leverage = config.leverage
        self.add_step_notional = config.add_step_notional
        self.tier_target_notional = config.tier_target_notional
        self.max_target_notional = config.max_target_notional
        self.final_target_notional = config.final_target_notional
        self.tier_one_premium = config.tier_one_premium
        self.tier_two_premium = config.tier_two_premium
        self.tier_three_premium = config.tier_three_premium
        self.stage_premiums = (
            self.tier_one_premium,
            self.tier_two_premium,
            self.tier_three_premium,
        )
        self.stage_targets = (
            self.tier_target_notional,
            self.max_target_notional,
            self.final_target_notional,
        )
        self.close_premium = config.close_premium
        self.close_leg_notional = config.close_leg_notional
        self.rebalance_threshold = config.rebalance_threshold
        self.retry_delay_ns = int(config.retry_delay_sec * NANOSECONDS_PER_SECOND)
        self.price_check_interval_ns = int(config.price_check_interval_sec * NANOSECONDS_PER_SECOND)
        self.max_mark_age_ns = int(config.max_mark_age_sec * NANOSECONDS_PER_SECOND)
        self.telegram_notify_step = config.telegram_notify_step
        notionals = (
            self.leverage,
            self.add_step_notional,
            self.tier_target_notional,
            self.max_target_notional,
            self.final_target_notional,
            self.close_leg_notional,
            self.rebalance_threshold,
        )
        if min(notionals) <= 0:
            raise ValueError("leverage and notionals must be positive")
        if not (
            self.add_step_notional
            <= self.tier_target_notional
            <= self.max_target_notional
            <= self.final_target_notional
        ):
            raise ValueError("add notional targets must be increasing")
        if not (
            Decimal("0")
            <= self.close_premium
            < self.tier_one_premium
            < self.tier_two_premium
            < self.tier_three_premium
        ):
            raise ValueError("premium thresholds must be strictly increasing")
        if self.retry_delay_ns < 0 or self.price_check_interval_ns <= 0 or self.max_mark_age_ns <= 0:
            raise ValueError("alert intervals and max_mark_age_sec are invalid")
        if self.telegram_notify_step <= 0:
            raise ValueError("telegram notification step must be positive")
        if not 0 <= config.initial_completed_stage <= len(self.stage_targets):
            raise ValueError("initial_completed_stage is invalid")

        self.marks: dict[InstrumentId, MarkPriceUpdate] = {}
        self.pending: dict[str, PendingLeg] | None = None
        self.pending_action: str | None = None
        self.pending_stage: int | None = None
        self.close_mode = False
        self.ready = True
        self.halted = False
        self.alert_name: str | None = None
        self.alert_seq = 0
        self.round_count = 0
        self.completed_stage = config.initial_completed_stage
        self.open_stage: int | None = None
        self.open_target: Decimal | None = None
        self.stage_snapshot_path = ROOT / config.stage_snapshot_path
        self.telegram: TelegramSender | None = None
        self.last_notify_premium: Decimal | None = None

    def on_start(self) -> None:
        self._check_startup()
        self._load_stage_snapshot()
        load_dotenv(ROOT / ".env")
        self.telegram = TelegramSender(
            token=os.environ["TELEGRAM_BOT_TOKEN"],
            chat_id=os.environ["TELEGRAM_CHAT_ID"],
        )
        for instrument_id in self.instrument_ids:
            self.subscribe_mark_prices(instrument_id)
        self._schedule_attempt(self.price_check_interval_ns)
        self.log.info(
            f"started sk={self.sk_id} adr={self.adr_id} leverage={self.leverage} "
            f"add_step_notional={self.add_step_notional} tier_target_notional={self.tier_target_notional} "
            f"max_target_notional={self.max_target_notional} final_target_notional={self.final_target_notional} "
            f"tier_premiums={self.stage_premiums} completed_stage={self.completed_stage} "
            f"close_premium={self.close_premium}",
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

        action = self.pending_action
        stage = self.pending_stage
        self.pending = None
        self.pending_action = None
        self.pending_stage = None
        self.round_count += 1
        margin = self._margin_used()
        self.log.info(
            f"orders_filled round={self.round_count} action={action} stage={stage} margin={margin:.4f}",
        )
        if action in {"close_pair", "close_rebalance"}:
            self._continue_close()
            return
        if action == "open_pair":
            self._continue_open(stage)
            return
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
            if len(positions) > 1:
                raise RuntimeError(f"startup_multiple_positions instrument={instrument_id}")
            signed_qty[instrument_id] = positions[0].signed_decimal_qty() if positions else Decimal("0")

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

        margin = self._margin_used()
        sk_notional, adr_notional = self._position_notionals()
        premium = adr_price / ADR_COMMON_SHARE_RATIO / sk_price - Decimal("1")
        self._notify_market(sk_price, adr_price, premium)
        if not self.close_mode and premium < self.close_premium:
            self.close_mode = True
            self.log.warning(f"close_mode_entered premium={premium:.4%}")
        if self.close_mode:
            self._submit_close_pair(sk_price, adr_price)
            return
        stage = self.completed_stage + 1
        if stage > len(self.stage_targets) or premium <= self.stage_premiums[stage - 1]:
            return
        target_limit = self.stage_targets[stage - 1]
        if max(sk_notional, adr_notional) >= target_limit:
            self._complete_stage(stage, "position_reached")
            return
        if self.open_stage != stage or self.open_target is None:
            self.open_stage = stage
            self.open_target = self._next_add_target(
                sk_notional,
                adr_notional,
                target_limit,
                self.add_step_notional,
            )
        while self.open_target < target_limit and min(sk_notional, adr_notional) >= self.open_target:
            self.open_target = min(self.open_target + self.add_step_notional, target_limit)
        target_notional = self.open_target

        sk_instrument = self.cache.instrument(self.sk_id)
        adr_instrument = self.cache.instrument(self.adr_id)
        sk_add = max(target_notional - sk_notional, Decimal("0"))
        adr_add = max(target_notional - adr_notional, Decimal("0"))
        try:
            quantities = {
                self.sk_id: self._add_qty(sk_instrument, sk_add, sk_price) if sk_add > 0 else None,
                self.adr_id: self._add_qty(adr_instrument, adr_add, adr_price) if adr_add > 0 else None,
            }
        except ValueError:
            self._pause(f"open_quantity_invalid margin={margin:.4f}")
            return
        orders = []
        pending = {}
        sides = {self.sk_id: OrderSide.BUY, self.adr_id: OrderSide.SELL}
        instruments = {self.sk_id: sk_instrument, self.adr_id: adr_instrument}
        marks = {self.sk_id: sk_mark.value, self.adr_id: adr_mark.value}
        prices = {self.sk_id: sk_price, self.adr_id: adr_price}
        added = Decimal("0")
        for instrument_id in self.instrument_ids:
            quantity = quantities[instrument_id]
            if quantity is None:
                continue
            if not self._meets_minimum(instruments[instrument_id], quantity, marks[instrument_id]):
                self._pause(f"open_quantity_below_minimum instrument={instrument_id} margin={margin:.4f}")
                return
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=sides[instrument_id],
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
            )
            orders.append(order)
            pending[str(order.client_order_id)] = PendingLeg(instrument_id, quantity.as_decimal())
            added += quantity.as_decimal() * prices[instrument_id]
        if not orders:
            raise RuntimeError("open target produced no orders")
        projected_margin = margin + added / self.leverage

        self.ready = False
        self.pending_action = "open_pair"
        self.pending_stage = stage
        self.pending = pending
        self.log.info(
            f"submit_pair stage={stage} premium={premium:.4%} margin={margin:.4f} "
            f"target={target_notional:.4f} projected={projected_margin:.4f} "
            f"sk_add={sk_add:.4f} adr_add={adr_add:.4f}",
        )
        for order in orders:
            if self.halted:
                break
            self.submit_order(order)

    # 每轮成交后等待十秒，再按两腿实时价值推进到下一个 500 USDT 档位。
    def _continue_open(self, stage: int | None) -> None:
        sk_notional, adr_notional = self._position_notionals()
        self.log.info(
            f"add_round_complete stage={stage} sk_notional={sk_notional:.4f} "
            f"adr_notional={adr_notional:.4f}",
        )
        if stage is None or self.open_stage != stage or self.open_target is None:
            raise RuntimeError("open stage state missing after fill")
        self._advance_target(stage)

    # 本轮目标成交后只推进一次；档位上限完成后锁定，等待下一溢价阈值。
    def _advance_target(self, stage: int) -> None:
        target_limit = self.stage_targets[stage - 1]
        if self.open_target >= target_limit:
            self._complete_stage(stage, "target_filled")
            self._schedule_attempt(self.price_check_interval_ns)
            return
        self.open_target = min(self.open_target + self.add_step_notional, target_limit)
        self._schedule_attempt(self.retry_delay_ns)

    # 完成状态先持久化；第三档完成后开仓永久禁用，但减仓检查继续运行。
    def _complete_stage(self, stage: int, reason: str) -> None:
        if stage <= self.completed_stage:
            return
        self.completed_stage = stage
        self.open_stage = None
        self.open_target = None
        self._save_stage_snapshot()
        self.log.warning(
            f"open_stage_completed stage={stage} reason={reason} "
            f"next_stage={stage + 1 if stage < len(self.stage_targets) else 'disabled'}",
        )

    # 新阶段首次开启时，以较大腿为基准选择下一个共同 500 USDT 档位。
    @staticmethod
    def _next_add_target(
        sk_notional: Decimal,
        adr_notional: Decimal,
        target_limit: Decimal,
        add_step: Decimal,
    ) -> Decimal:
        larger = max(sk_notional, adr_notional)
        next_target = (larger // add_step + Decimal("1")) * add_step
        return min(next_target, target_limit)

    # 启动时恢复已完成档位；首次部署用配置值建立快照。
    def _load_stage_snapshot(self) -> None:
        if self.stage_snapshot_path.exists():
            data = json.loads(self.stage_snapshot_path.read_text(encoding="utf-8"))
            completed_stage = int(data["completed_stage"])
            if not 0 <= completed_stage <= len(self.stage_targets):
                raise ValueError("stage snapshot completed_stage is invalid")
            self.completed_stage = max(self.completed_stage, completed_stage)
        self._save_stage_snapshot()

    # 原子写入小型状态文件，防止重启后再次执行已经完成的开仓档位。
    def _save_stage_snapshot(self) -> None:
        self.stage_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.stage_snapshot_path.with_suffix(f"{self.stage_snapshot_path.suffix}.tmp")
        tmp.write_text(
            json.dumps({"completed_stage": self.completed_stage}, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self.stage_snapshot_path)

    # 市价单数量向上对齐合约步长，使目标腿在当前标记价下至少到达本轮档位。
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

    # 平仓模式每轮同步减少两腿；尾仓不足一轮时直接全部 reduce-only 平掉。
    def _submit_close_pair(self, sk_price: Decimal, adr_price: Decimal) -> None:
        sk_position_qty = self._position_qty(self.sk_id)
        adr_position_qty = abs(self._position_qty(self.adr_id))
        if sk_position_qty == 0 and adr_position_qty == 0:
            self._stop_node("close_mode_positions_flat")
            return
        if sk_position_qty == 0 or adr_position_qty == 0:
            self._submit_close_rebalance()
            return

        sk_notional = sk_position_qty * sk_price
        adr_notional = adr_position_qty * adr_price
        sk_instrument = self.cache.instrument(self.sk_id)
        adr_instrument = self.cache.instrument(self.adr_id)
        if min(sk_notional, adr_notional) <= self.close_leg_notional:
            sk_qty = sk_instrument.make_qty(sk_position_qty, round_down=True)
            adr_qty = adr_instrument.make_qty(adr_position_qty, round_down=True)
        else:
            sk_qty = sk_instrument.make_qty(self.close_leg_notional / sk_price, round_down=True)
            adr_qty = adr_instrument.make_qty(self.close_leg_notional / adr_price, round_down=True)

        sk_order = self.order_factory.market(
            instrument_id=self.sk_id,
            order_side=OrderSide.SELL,
            quantity=sk_qty,
            time_in_force=TimeInForce.GTC,
            reduce_only=True,
        )
        adr_order = self.order_factory.market(
            instrument_id=self.adr_id,
            order_side=OrderSide.BUY,
            quantity=adr_qty,
            time_in_force=TimeInForce.GTC,
            reduce_only=True,
        )
        self.ready = False
        self.pending_action = "close_pair"
        self.pending = {
            str(sk_order.client_order_id): PendingLeg(self.sk_id, sk_qty.as_decimal()),
            str(adr_order.client_order_id): PendingLeg(self.adr_id, adr_qty.as_decimal()),
        }
        self.log.info(
            f"submit_close_pair sk_qty={sk_qty} adr_qty={adr_qty} "
            f"sk_notional={sk_notional:.4f} adr_notional={adr_notional:.4f}",
        )
        self.submit_order(sk_order)
        if not self.halted:
            self.submit_order(adr_order)

    # 每次减仓成交后检查两腿价值，差额超过阈值时先减较大侧。
    def _continue_close(self) -> None:
        sk_notional, adr_notional = self._position_notionals()
        if sk_notional == 0 and adr_notional == 0:
            self._stop_node("close_mode_positions_flat")
            return
        difference = abs(sk_notional - adr_notional)
        self.log.info(
            f"close_balance sk_notional={sk_notional:.4f} adr_notional={adr_notional:.4f} "
            f"difference={difference:.4f}",
        )
        if difference > self.rebalance_threshold or min(sk_notional, adr_notional) == 0:
            self._submit_close_rebalance()
            return
        self._schedule_attempt(self.retry_delay_ns)

    # 仅对名义价值较大的腿追加一次 reduce-only 市价减仓。
    def _submit_close_rebalance(self) -> None:
        sk_notional, adr_notional = self._position_notionals()
        if sk_notional == 0 and adr_notional == 0:
            self._stop_node("close_mode_positions_flat")
            return
        if sk_notional >= adr_notional:
            instrument_id = self.sk_id
            side = OrderSide.SELL
            price = self.marks[self.sk_id].value.as_decimal()
        else:
            instrument_id = self.adr_id
            side = OrderSide.BUY
            price = self.marks[self.adr_id].value.as_decimal()

        position_qty = abs(self._position_qty(instrument_id))
        difference = abs(sk_notional - adr_notional)
        instrument = self.cache.instrument(instrument_id)
        if min(sk_notional, adr_notional) == 0:
            quantity = instrument.make_qty(position_qty, round_down=True)
        else:
            quantity = instrument.make_qty(min(difference / price, position_qty), round_down=True)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
            reduce_only=True,
        )
        self.ready = False
        self.pending_action = "close_rebalance"
        self.pending = {
            str(order.client_order_id): PendingLeg(instrument_id, quantity.as_decimal()),
        }
        self.log.info(
            f"submit_close_rebalance instrument={instrument_id} side={side} qty={quantity} "
            f"difference={difference:.4f}",
        )
        self.submit_order(order)

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
        change = Decimal("0") if previous is None else premium - previous
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
                notionals[instrument_id] = Decimal("0")
                continue
            positions = self.cache.positions_open(instrument_id=instrument_id)
            quantity = sum((abs(position.signed_decimal_qty()) for position in positions), Decimal("0"))
            notionals[instrument_id] = quantity * mark.value.as_decimal()
        return notionals[self.sk_id], notionals[self.adr_id]

    def _position_qty(self, instrument_id: InstrumentId) -> Decimal:
        positions = self.cache.positions_open(instrument_id=instrument_id)
        return sum(
            (position.signed_decimal_qty() for position in positions),
            Decimal("0"),
        )

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

    def _stop_node(self, reason: str) -> None:
        if self.halted:
            return
        self.halted = True
        self.ready = False
        self.log.warning(f"strategy_halted reason={reason}")
        self.msgbus.publish(
            NODE_STOP_TOPIC,
            NodeStopRequest(source="sk_adr_arb", reason=reason),
        )
