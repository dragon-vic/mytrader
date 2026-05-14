from __future__ import annotations

from decimal import Decimal
from decimal import ROUND_UP

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderAccepted
from nautilus_trader.model.events import OrderCancelRejected
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import OrderSubmitted
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import BINANCE_CLIENT_NAME
from utils.arguments import NODE_STOP_TOPIC


class ManualOrderProbeConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    trade_notional: Decimal
    price_offset_bps: int = 300
    cancel_retries: int = 3
    stop_when_done: bool = True


class ManualOrderProbe(Strategy):
    def __init__(self, config: ManualOrderProbeConfig) -> None:
        super().__init__(config)
        self.index = 0
        self.active_order = None
        self.active_id: ClientOrderId | None = None
        self.cancel_attempts = 0
        self.latest_quotes: dict[InstrumentId, QuoteTick] = {}
        self.rejected: list[InstrumentId] = []
        self.canceled: list[InstrumentId] = []
        self.cancel_failed: list[InstrumentId] = []

    # 启动后订阅全部白名单 quote，但按列表顺序一次只测一个。
    def on_start(self) -> None:
        for instrument_id in self.config.instrument_ids:
            self.subscribe_quote_ticks(
                instrument_id,
                client_id=ClientId(BINANCE_CLIENT_NAME),
            )
        self.log.info(
            "manual_order_probe_start "
            f"total={len(self.config.instrument_ids)} "
            f"instruments={','.join(str(item) for item in self.config.instrument_ids)}",
        )

    # 只用当前目标品种的 quote 触发下单。
    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.latest_quotes[tick.instrument_id] = tick
        self._try_submit()

    # 记录本地提交事件。
    def on_order_submitted(self, event: OrderSubmitted) -> None:
        if event.client_order_id == self.active_id:
            self.log.info(f"manual_order_probe_submitted event={event}")

    # 交易所接受后立刻撤单。
    def on_order_accepted(self, event: OrderAccepted) -> None:
        if event.client_order_id != self.active_id:
            return
        self.log.info(f"manual_order_probe_accepted event={event}")
        self._cancel_active("accepted")

    # 确认撤单后进入下一个品种。
    def on_order_canceled(self, event: OrderCanceled) -> None:
        if event.client_order_id != self.active_id:
            return
        self.log.info(f"manual_order_probe_canceled event={event}")
        self.canceled.append(event.instrument_id)
        self._advance()

    # 下单拒绝也算本品种测试完成，继续下一个。
    def on_order_rejected(self, event: OrderRejected) -> None:
        if event.client_order_id != self.active_id:
            return
        self.log.error(f"manual_order_probe_rejected event={event}")
        self.rejected.append(event.instrument_id)
        self._advance()

    # 撤单失败时重试；超过次数就停，避免继续叠加挂单。
    def on_order_cancel_rejected(self, event: OrderCancelRejected) -> None:
        if event.client_order_id != self.active_id:
            return
        self.log.error(f"manual_order_probe_cancel_rejected event={event}")
        if self.cancel_attempts < self.config.cancel_retries:
            self._cancel_active("retry")
            return
        self.cancel_failed.append(event.instrument_id)
        self._finish("cancel_rejected")

    # 停止时取消订阅；若还有当前活动订单，再尝试撤一次。
    def on_stop(self) -> None:
        if self.active_order is not None:
            self._cancel_active("stop")
        for instrument_id in self.config.instrument_ids:
            self.unsubscribe_quote_ticks(
                instrument_id,
                client_id=ClientId(BINANCE_CLIENT_NAME),
            )

    # 当前品种有 quote 且没有活动订单时，提交一张远离成交区的 post-only 空单。
    def _try_submit(self) -> None:
        if self.active_order is not None or self.index >= len(self.config.instrument_ids):
            return

        instrument_id = self.config.instrument_ids[self.index]
        tick = self.latest_quotes.get(instrument_id)
        if tick is None:
            return

        instrument = self.cache.instrument(instrument_id)
        ask = Decimal(str(tick.ask_price))
        offset = Decimal("1") + Decimal(str(self.config.price_offset_bps)) / Decimal("10000")
        price = self._ceil_step(ask * offset, Decimal(str(instrument.price_increment)))
        quantity = self._ceil_step(
            self.config.trade_notional / price,
            Decimal(str(instrument.size_increment)),
        )
        order = self.order_factory.limit(
            instrument_id=instrument_id,
            order_side=OrderSide.SELL,
            quantity=instrument.make_qty(quantity),
            price=instrument.make_price(price),
            time_in_force=TimeInForce.GTC,
            post_only=True,
        )
        self.active_order = order
        self.active_id = order.client_order_id
        self.cancel_attempts = 0
        self.log.info(
            "manual_order_probe_submit "
            f"seq={self.index + 1}/{len(self.config.instrument_ids)} "
            f"instrument_id={instrument_id} ask={ask} price={price} "
            f"quantity={quantity} notional={quantity * price} "
            f"client_order_id={order.client_order_id}",
        )
        self.submit_order(order, client_id=ClientId(BINANCE_CLIENT_NAME))

    # 撤当前活动订单。
    def _cancel_active(self, reason: str) -> None:
        if self.active_order is None:
            return
        self.cancel_attempts += 1
        self.log.info(
            "manual_order_probe_cancel "
            f"reason={reason} attempt={self.cancel_attempts} "
            f"client_order_id={self.active_id}",
        )
        self.cancel_order(self.active_order, client_id=ClientId(BINANCE_CLIENT_NAME))

    # 清掉当前订单状态并开始下一个品种。
    def _advance(self) -> None:
        self.active_order = None
        self.active_id = None
        self.cancel_attempts = 0
        self.index += 1
        if self.index >= len(self.config.instrument_ids):
            self._finish("done")
            return
        self._try_submit()

    # 输出汇总并按配置停止节点。
    def _finish(self, reason: str) -> None:
        self.log.info(
            "manual_order_probe_finish "
            f"reason={reason} canceled={len(self.canceled)} "
            f"rejected={len(self.rejected)} cancel_failed={len(self.cancel_failed)} "
            f"canceled_symbols={','.join(str(item) for item in self.canceled)} "
            f"rejected_symbols={','.join(str(item) for item in self.rejected)} "
            f"cancel_failed_symbols={','.join(str(item) for item in self.cancel_failed)}",
        )
        if self.config.stop_when_done:
            self.msgbus.publish(NODE_STOP_TOPIC, {"reason": f"manual_order_probe_{reason}"})

    # 按交易所步长向上取整，保证名义金额不低于配置值。
    @staticmethod
    def _ceil_step(value: Decimal, step: Decimal) -> Decimal:
        return (value / step).to_integral_value(rounding=ROUND_UP) * step
