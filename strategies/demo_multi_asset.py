from __future__ import annotations

from decimal import Decimal

import pandas as pd
from external.data_engine import EXTERNAL_SIGNAL_CLIENT_NAME
from external.data_engine import ExternalSignal
from external.data_engine import external_signal_type
from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderEvent
from nautilus_trader.model.events import PositionEvent
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


class DemoMultiAssetConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    trade_notional: Decimal
    fast_ema_period: int = 10
    slow_ema_period: int = 30
    request_bars: bool = True
    warmup_days: int = 1
    close_positions_on_stop: bool = True


class DemoMultiAsset(Strategy):
    def __init__(self, config: DemoMultiAssetConfig) -> None:
        if config.fast_ema_period >= config.slow_ema_period:
            raise ValueError("fast_ema_period must be less than slow_ema_period")
        super().__init__(config)
        self.fast_emas = {bar_type: ExponentialMovingAverage(config.fast_ema_period) for bar_type in config.bar_types}
        self.slow_emas = {bar_type: ExponentialMovingAverage(config.slow_ema_period) for bar_type in config.bar_types}

    # 注册多品种 bar、指标和外部信号订阅。
    def on_start(self) -> None:
        for bar_type in self.config.bar_types:
            self.register_indicator_for_bars(bar_type, self.fast_emas[bar_type])
            self.register_indicator_for_bars(bar_type, self.slow_emas[bar_type])
            if self.config.request_bars:
                self.request_bars(bar_type, start=self._clock.utc_now() - pd.Timedelta(days=self.config.warmup_days))
            self.subscribe_bars(bar_type)
        self.subscribe_data(external_signal_type(), client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME))

    # bar 驱动的多品种 EMA 示例。
    def on_bar(self, bar: Bar) -> None:
        if not self.indicators_initialized():
            return
        fast = self.fast_emas[bar.bar_type].value
        slow = self.slow_emas[bar.bar_type].value
        if fast > slow:
            self._target(bar.bar_type.instrument_id, OrderSide.BUY)
        elif fast < slow:
            self._target(bar.bar_type.instrument_id, OrderSide.SELL)

    # 外部信号覆盖对应品种方向。
    def on_data(self, data: ExternalSignal) -> None:
        side = {"BUY": OrderSide.BUY, "SELL": OrderSide.SELL}[data.side]
        self._target(data.instrument_id, side)

    # 记录订单事件，真实策略里可以接通知。
    def on_order_event(self, event: OrderEvent) -> None:
        self.log.info(f"order_event={event}")

    # 记录持仓事件，真实策略里可以做风控状态同步。
    def on_position_event(self, event: PositionEvent) -> None:
        self.log.info(f"position_event={event}")

    # 兜底观察 NT 事件流。
    def on_event(self, event) -> None:
        self.log.debug(f"event={event}")

    # 把指定 instrument 调成目标方向。
    def _target(self, instrument_id: InstrumentId, side: OrderSide) -> None:
        if side == OrderSide.BUY and self.portfolio.is_net_long(instrument_id):
            return
        if side == OrderSide.SELL and self.portfolio.is_net_short(instrument_id):
            return
        if not self.portfolio.is_flat(instrument_id):
            self.close_all_positions(instrument_id)
        self._market(instrument_id, side)

    # 按名义金额估算数量并提交市价单。
    def _market(self, instrument_id: InstrumentId, side: OrderSide) -> None:
        instrument = self.cache.instrument(instrument_id)
        last = self.cache.bar(self._bar_type_for(instrument_id))
        reference_px = Decimal(str(last.close)) if last is not None else Decimal(str(instrument.price_increment))
        qty = instrument.make_qty(self.config.trade_notional / reference_px)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=qty,
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

    # 按 instrument 找到对应 bar type。
    def _bar_type_for(self, instrument_id: InstrumentId) -> BarType:
        for bar_type in self.config.bar_types:
            if bar_type.instrument_id == instrument_id:
                return bar_type
        raise RuntimeError(f"BarType not found for {instrument_id}")

    # 停止时取消订阅、撤单并按配置平仓。
    def on_stop(self) -> None:
        for instrument_id in self.config.instrument_ids:
            self.cancel_all_orders(instrument_id)
            if self.config.close_positions_on_stop:
                self.close_all_positions(instrument_id)
        for bar_type in self.config.bar_types:
            self.unsubscribe_bars(bar_type)
        self.unsubscribe_data(external_signal_type(), client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME))

    # 重置本地指标。
    def on_reset(self) -> None:
        for indicator in [*self.fast_emas.values(), *self.slow_emas.values()]:
            indicator.reset()
