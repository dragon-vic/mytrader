from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from external.data_engine import EXTERNAL_SIGNAL_CLIENT_NAME
from external.data_engine import ExternalSignal
from external.data_engine import external_signal_type
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderEvent
from nautilus_trader.model.events import PositionEvent
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class WatchConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal


class Watch(Strategy):
    def __init__(self, config: WatchConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument | None = None
        self.entry_submitted = False
        self.entry_notional = Decimal("6")
        self.request_bars_on_start = True
        self.warmup_minutes = 10
        self.subscribe_ticks_on_start = False
        self.subscribe_external_on_start = True

    # 启动时订阅行情、外部信号和全局订单/仓位/账户事件。
    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            raise RuntimeError(f"Instrument not found: {self.config.instrument_id}")

        if self.request_bars_on_start:
            self.request_bars(
                self.config.bar_type,
                start=self._clock.utc_now() - timedelta(minutes=self.warmup_minutes),
            )
        self.subscribe_bars(self.config.bar_type)
        if self.subscribe_ticks_on_start:
            self.subscribe_trade_ticks(self.config.instrument_id)
        if self.subscribe_external_on_start:
            self.subscribe_data(external_signal_type(), client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME))
        self.log.info(f"Watch started: instrument={self.config.instrument_id}, bar_type={self.config.bar_type}")

        last_bar = self.cache.bar(self.config.bar_type)
        if last_bar is not None:
            self.submit_entry_sell(Decimal(str(last_bar.close)))

    # 按 USDT 名义金额换算数量并提交市价空单。
    def submit_entry_sell(self, reference_price: Decimal) -> None:
        if self.entry_submitted:
            return

        quantity = self.instrument.make_qty(self.entry_notional / reference_price)
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.SELL,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
        self.entry_submitted = True
        self.log.info(f"Watch entry sell submitted: notional={self.entry_notional}, quantity={quantity}")

    # 每根 1m bar 到达时记录行情摘要。
    def on_bar(self, bar: Bar) -> None:
        self.submit_entry_sell(Decimal(str(bar.close)))

    # 可选 tick 订阅开启时记录最新成交。
    def on_trade_tick(self, tick: TradeTick) -> None:
        self.log.info(f"tick ts={tick.ts_event} price={tick.price} size={tick.size}")

    # 外部信号到达时只记录，不触发交易。
    def on_data(self, data: ExternalSignal) -> None:
        self.log.info(f"external_signal={data}")

    # 被 external_order_claims 认领到本策略的订单事件会走这里。
    def on_order_event(self, event: OrderEvent) -> None:
        pass

    # 被 NT 归属到本策略的仓位事件会走这里。
    def on_position_event(self, event: PositionEvent) -> None:
        pass

    # 非订单事件完整打出来，临时观察 live 会收到哪些事件。
    def on_event(self, event) -> None:
        if isinstance(event, OrderEvent):
            return
        self.log.info(f"non_order_event type={type(event).__name__} payload={self.event_payload(event)}")

    # 把 NT 事件尽量展开成字段，方便从日志里观察。
    def event_payload(self, event):
        if hasattr(type(event), "to_dict"):
            return type(event).to_dict(event)
        if hasattr(event, "__dict__"):
            return event.__dict__
        return event

    # 停止时平掉本策略关注 instrument 的全部仓位。
    def on_stop(self) -> None:
        self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_bars(self.config.bar_type)
        if self.subscribe_ticks_on_start:
            self.unsubscribe_trade_ticks(self.config.instrument_id)
        if self.subscribe_external_on_start:
            self.unsubscribe_data(external_signal_type(), client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME))
        self.log.info("Watch stopped.")

    # 重置时没有本地状态需要清理。
    def on_reset(self) -> None:
        self.log.info("Watch reset.")
