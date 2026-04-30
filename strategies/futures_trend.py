from __future__ import annotations

from decimal import Decimal

import pandas as pd

from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import AverageTrueRange
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class FuturesTrendConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal
    fast_ema_period: PositiveInt = 12
    slow_ema_period: PositiveInt = 36
    atr_period: PositiveInt = 14
    min_atr: float = 0.0
    request_bars: bool = True
    warmup_days: PositiveInt = 1
    close_positions_on_stop: bool = True


class FuturesTrend(Strategy):
    def __init__(self, config: FuturesTrendConfig) -> None:
        if config.fast_ema_period >= config.slow_ema_period:
            raise ValueError("fast_ema_period must be less than slow_ema_period")
        super().__init__(config)
        self.instrument: Instrument | None = None
        self.fast_ema = ExponentialMovingAverage(config.fast_ema_period)
        self.slow_ema = ExponentialMovingAverage(config.slow_ema_period)
        self.atr = AverageTrueRange(config.atr_period)

    # 注册指标，先请求历史 bar 预热，再订阅实时 bar。
    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Instrument not found: {self.config.instrument_id}")
            self.stop()
            return

        self.register_indicator_for_bars(self.config.bar_type, self.fast_ema)
        self.register_indicator_for_bars(self.config.bar_type, self.slow_ema)
        self.register_indicator_for_bars(self.config.bar_type, self.atr)
        if self.config.request_bars:
            self.request_bars(
                self.config.bar_type,
                start=self._clock.utc_now() - pd.Timedelta(days=self.config.warmup_days),
            )
        self.subscribe_bars(self.config.bar_type)

    # 简单趋势逻辑：快线在慢线上方做多，在下方做空。
    def on_bar(self, bar: Bar) -> None:
        if not self.indicators_initialized() or self.atr.value < self.config.min_atr:
            return

        if self.fast_ema.value > self.slow_ema.value:
            self._target_long()
        elif self.fast_ema.value < self.slow_ema.value:
            self._target_short()

    # 把当前持仓调整成净多。
    def _target_long(self) -> None:
        if self.portfolio.is_flat(self.config.instrument_id):
            self._market(OrderSide.BUY)
        elif self.portfolio.is_net_short(self.config.instrument_id):
            self.close_all_positions(self.config.instrument_id)
            self._market(OrderSide.BUY)

    # 把当前持仓调整成净空。
    def _target_short(self) -> None:
        if self.portfolio.is_flat(self.config.instrument_id):
            self._market(OrderSide.SELL)
        elif self.portfolio.is_net_long(self.config.instrument_id):
            self.close_all_positions(self.config.instrument_id)
            self._market(OrderSide.SELL)

    # 第一版合约策略先用市价单，方便验证整条链路。
    def _market(self, side: OrderSide) -> None:
        if self.instrument is None:
            return
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
        self.unsubscribe_bars(self.config.bar_type)

    # 重置本地指标状态。
    def on_reset(self) -> None:
        self.fast_ema.reset()
        self.slow_ema.reset()
        self.atr.reset()
