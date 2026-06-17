from __future__ import annotations

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import NODE_STOP_TOPIC


class BintestConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]


class BintestStrategy(Strategy):
    def __init__(self, config: BintestConfig) -> None:
        super().__init__(config)
        self.instrument_id = config.instrument_ids[0]
        self.bar_type = config.bar_types[0]
        self.quote_count = 0
        self.trade_count = 0
        self.bar_count = 0

    # 订阅 BTCUSDT-PERP 的 tick 和 K 线，然后立即请求停止 node。
    def on_start(self) -> None:
        self.subscribe_quote_ticks(self.instrument_id)
        self.subscribe_trade_ticks(self.instrument_id)
        self.subscribe_bars(self.bar_type)
        self.log.info(f"bintest started instrument={self.instrument_id} bar_type={self.bar_type}")
        self.msgbus.publish(NODE_STOP_TOPIC, {"source": "bintest", "reason": "on_start complete"})

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.quote_count += 1
        self.log.info(f"bintest quote_tick count={self.quote_count} instrument={tick.instrument_id}")

    def on_trade_tick(self, tick: TradeTick) -> None:
        self.trade_count += 1
        self.log.info(f"bintest trade_tick count={self.trade_count} instrument={tick.instrument_id}")

    def on_bar(self, bar: Bar) -> None:
        self.bar_count += 1
        self.log.info(f"bintest bar count={self.bar_count} bar_type={bar.bar_type}")

    # 立即停止场景下由 node 统一断开 data client，避免 websocket 未连接时主动退订。
    def on_stop(self) -> None:
        self.log.info(
            f"bintest stopped quotes={self.quote_count} trades={self.trade_count} bars={self.bar_count}",
        )
