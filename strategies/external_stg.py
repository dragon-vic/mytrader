from __future__ import annotations

from decimal import Decimal

from external.data_engine import EXTERNAL_SIGNAL_CLIENT_NAME
from external.data_engine import ExternalSignal
from external.data_engine import external_signal_type
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class ExternalStgConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal
    close_positions_on_stop: bool = True
    setting:dict= None


class ExternalStg(Strategy):
    def __init__(self, config: ExternalStgConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument | None = None

    # 读取 instrument 并订阅外部时间随机信号。
    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        self.subscribe_data(
            data_type=external_signal_type(),
            client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME),
        )

    # 收到外部信号后按信号方向提交一笔 BTC 合约市价单。
    def on_data(self, data: ExternalSignal) -> None:
        now_ns = self.clock.timestamp_ns()
        delay_ms = (now_ns - data.sent_ns) / 1_000_000
        self.log.info(f"External signal delay: {delay_ms:.3f} ms")

        side = {"BUY": OrderSide.BUY, "SELL": OrderSide.SELL}[data.side]
        self._market(side)

    # 按配置数量提交市价单。
    def _market(self, side: OrderSide) -> None:
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

    # 停止时取消订单、按配置平仓并取消订阅。
    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        if self.config.close_positions_on_stop:
            self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_data(
            external_signal_type(),
            client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME),
        )
