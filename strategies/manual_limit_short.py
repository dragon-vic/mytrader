from __future__ import annotations

from decimal import Decimal
from decimal import ROUND_UP

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderAccepted
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import OrderSubmitted
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import BINANCE_CLIENT_NAME
from utils.arguments import NODE_STOP_TOPIC


class ManualLimitShortConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_notional: Decimal
    price_offset_bps: int = 300
    stop_after_terminal: bool = True


class ManualLimitShort(Strategy):
    def __init__(self, config: ManualLimitShortConfig) -> None:
        super().__init__(config)
        self.sent = False

    # 启动后订阅 quote tick，用第一条 ask 计算被动卖出限价。
    def on_start(self) -> None:
        self.subscribe_quote_ticks(
            self.config.instrument_id,
            client_id=ClientId(BINANCE_CLIENT_NAME),
        )

    # 用当前 ask 上方的限价提交一张 post-only 空单。
    def on_quote_tick(self, tick: QuoteTick) -> None:
        if self.sent:
            return

        self.sent = True
        instrument = self.cache.instrument(self.config.instrument_id)
        ask = Decimal(str(tick.ask_price))
        offset = Decimal("1") + Decimal(str(self.config.price_offset_bps)) / Decimal("10000")
        price = self._ceil_step(ask * offset, Decimal(str(instrument.price_increment)))
        quantity = self._ceil_step(
            self.config.trade_notional / price,
            Decimal(str(instrument.size_increment)),
        )

        order = self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.SELL,
            quantity=instrument.make_qty(quantity),
            price=instrument.make_price(price),
            time_in_force=TimeInForce.GTC,
            post_only=True,
        )
        self.log.info(
            "manual_limit_short_submit "
            f"instrument_id={self.config.instrument_id} "
            f"ask={ask} price={price} quantity={quantity} "
            f"notional={quantity * price} client_order_id={order.client_order_id}",
        )
        self.submit_order(order, client_id=ClientId(BINANCE_CLIENT_NAME))

    # 记录本地已提交事件。
    def on_order_submitted(self, event: OrderSubmitted) -> None:
        self.log.info(f"manual_limit_short_submitted event={event}")

    # 接受后停止节点；不在 on_stop 撤单，让交易所挂单保留。
    def on_order_accepted(self, event: OrderAccepted) -> None:
        self.log.info(f"manual_limit_short_accepted event={event}")
        if self.config.stop_after_terminal:
            self.msgbus.publish(NODE_STOP_TOPIC, {"reason": "manual_order_accepted"})

    # 拒单后停止节点，保留拒单原因在日志里。
    def on_order_rejected(self, event: OrderRejected) -> None:
        self.log.error(f"manual_limit_short_rejected event={event}")
        if self.config.stop_after_terminal:
            self.msgbus.publish(NODE_STOP_TOPIC, {"reason": "manual_order_rejected"})

    # 停止时只取消数据订阅，不撤订单。
    def on_stop(self) -> None:
        self.unsubscribe_quote_ticks(
            self.config.instrument_id,
            client_id=ClientId(BINANCE_CLIENT_NAME),
        )

    # 按交易所步长向上取整，保证名义金额不低于配置值。
    @staticmethod
    def _ceil_step(value: Decimal, step: Decimal) -> Decimal:
        return (value / step).to_integral_value(rounding=ROUND_UP) * step
