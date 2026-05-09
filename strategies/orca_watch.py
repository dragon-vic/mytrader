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
from nautilus_trader.model.events import OrderEvent
from nautilus_trader.model.events import PositionEvent
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


class OrcaWatchConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal
    request_bars: bool = True
    warmup_minutes: int = 10
    subscribe_ticks: bool = False
    subscribe_external: bool = True



class OrcaWatch(Strategy):
    def __init__(self, config: OrcaWatchConfig) -> None:
        super().__init__(config)

    # 启动时只订阅 ORCA 1m bar、可选 tick 和外部信号，不做任何下单动作。
    def on_start(self) -> None:
        if self.config.request_bars:
            self.request_bars(
                self.config.bar_type,
                start=self._clock.utc_now() - timedelta(minutes=self.config.warmup_minutes),
            )
        self.subscribe_bars(self.config.bar_type)
        if self.config.subscribe_ticks:
            self.subscribe_trade_ticks(self.config.instrument_id)
        if self.config.subscribe_external:
            self.subscribe_data(external_signal_type(), client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME))
        self.log.info(f"OrcaWatch started: instrument={self.config.instrument_id}, bar_type={self.config.bar_type}")

    # 每根 1m bar 到达时记录行情摘要。
    def on_bar(self, bar: Bar) -> None:
        pass

    # 可选 tick 订阅开启时记录最新成交。
    def on_trade_tick(self, tick: TradeTick) -> None:
        self.log.info(f"tick ts={tick.ts_event} price={tick.price} size={tick.size}")

    # 外部信号到达时只记录，不触发交易。
    def on_data(self, data: ExternalSignal) -> None:
        self.log.info(f"external_signal={data}")

    # 订单事件只记录；本策略正常情况下不会主动产生订单。
    def on_order_event(self, event: OrderEvent) -> None:
        self.log.info(f"order_event={event}")

    # 仓位事件只记录，用来观察账户已有仓位变化。
    def on_position_event(self, event: PositionEvent) -> None:
        self.log.info(f"position_event={event}")

    # 其他事件只打 debug，避免正常日志太吵。
    def on_event(self, event) -> None:
        self.log.debug(f"event={event}")

    # 停止时取消订阅，不撤单、不平仓。
    def on_stop(self) -> None:
        self.unsubscribe_bars(self.config.bar_type)
        if self.config.subscribe_ticks:
            self.unsubscribe_trade_ticks(self.config.instrument_id)
        if self.config.subscribe_external:
            self.unsubscribe_data(external_signal_type(), client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME))
        self.log.info("OrcaWatch stopped.")

    # 重置时没有本地状态需要清理。
    def on_reset(self) -> None:
        self.log.info("OrcaWatch reset.")
