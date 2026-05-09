from __future__ import annotations

from decimal import Decimal

import pandas as pd
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


class GridRebalanceConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_notional: Decimal
    grid_step_bps: int = 300
    max_levels: int = 2
    reset_bars: int = 4
    close_positions_on_stop: bool = True


class GridRebalance(Strategy):
    def __init__(self, config: GridRebalanceConfig) -> None:
        super().__init__(config)
        self.center_price: Decimal | None = None
        self.bars_since_reset = 0
        self.order_levels: dict[ClientOrderId, tuple[OrderSide, int]] = {}

    # 订阅回测或实盘传入的 1m bar。
    def on_start(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    # 用 bar 收盘价初始化中心，按固定周期重置网格。
    def on_bar(self, bar: Bar) -> None:
        if self.center_price is None:
            self._reset_grid(Decimal(str(bar.close)))
            return

        self.bars_since_reset += 1
        if self.bars_since_reset >= self.config.reset_bars:
            self.cancel_all_orders(self.config.instrument_id)
            self.close_all_positions(self.config.instrument_id)
            self._reset_grid(Decimal(str(bar.close)))

    # 订单成交后在相邻网格挂反向单。
    def on_order_filled(self, event: OrderFilled) -> None:
        item = self.order_levels.pop(event.client_order_id, None)
        if item is None:
            return

        side, level = item
        next_level = level + 1 if side == OrderSide.BUY else level - 1
        next_side = OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY
        self._submit_level(next_side, next_level)

    # 清掉本地订单层级记录。
    def on_order_canceled(self, event: OrderCanceled) -> None:
        self.order_levels.pop(event.client_order_id, None)

    # 清掉本地订单层级记录。
    def on_order_expired(self, event: OrderExpired) -> None:
        self.order_levels.pop(event.client_order_id, None)

    # 清掉本地订单层级记录。
    def on_order_rejected(self, event: OrderRejected) -> None:
        self.order_levels.pop(event.client_order_id, None)

    # 停止时撤单，按配置决定是否平仓。
    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        if self.config.close_positions_on_stop:
            self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_bars(self.config.bar_type)

    # 重置中心价并重新挂上下两侧网格。
    def _reset_grid(self, center_price: Decimal) -> None:
        self.center_price = center_price
        self.bars_since_reset = 0
        self.order_levels.clear()
        for level in range(1, self.config.max_levels + 1):
            self._submit_level(OrderSide.BUY, -level)
            self._submit_level(OrderSide.SELL, level)

    # 按网格层级提交一张限价单。
    def _submit_level(self, side: OrderSide, level: int) -> None:
        if abs(level) > self.config.max_levels:
            return
        if self.center_price is None:
            return

        instrument = self.cache.instrument(self.config.instrument_id)
        step = Decimal("1") + Decimal(str(self.config.grid_step_bps)) / Decimal("10000")
        price = self.center_price * (step ** level)
        qty = instrument.make_qty(self.config.trade_notional / price)
        order = self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=qty,
            price=instrument.make_price(price),
            time_in_force=TimeInForce.GTC,
            post_only=True,
        )
        self.order_levels[order.client_order_id] = (side, level)
        self.submit_order(order)

    # 重置本地状态。
    def on_reset(self) -> None:
        self.center_price = None
        self.bars_since_reset = 0
        self.order_levels.clear()
