from __future__ import annotations

from nautilus_trader.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy


class AnthropicArbConfig(StrategyConfig, frozen=True):
    pass


class AnthropicArbStrategy(Strategy):
    def __init__(self, config: AnthropicArbConfig) -> None:
        super().__init__(config)

    # 策略启动入口，后续逐步补订阅、warmup 和状态初始化。
    def on_start(self) -> None:
        self.log.info("anthropic_arb started")

    # 策略停止入口，后续补充清理和退出状态写入。
    def on_stop(self) -> None:
        self.log.info("anthropic_arb stopped")
