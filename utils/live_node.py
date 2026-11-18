import asyncio

from nautilus_trader.live.node import TradingNode


class LiveTradingNode(TradingNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.managed_stop_task = None
        self.kernel.msgbus.unsubscribe('commands.system.shutdown', self.kernel._on_shutdown_system)
        self.kernel.msgbus.subscribe('commands.system.shutdown', self._on_shutdown_system)

    def _on_shutdown_system(self, command):
        if command.trader_id == self.kernel.trader_id:
            self.get_event_loop().call_soon_threadsafe(self.stop)

    async def stop_async(self):
        if self.managed_stop_task is None:
            self.managed_stop_task = asyncio.create_task(self._stop_after_managed_exit())
        await asyncio.shield(self.managed_stop_task)

    async def _drain_managed_strategies(self):
        strategies = [strategy for strategy in self.trader.strategies()
                      if strategy.is_running and strategy.config.manage_stop]
        for strategy in strategies:
            strategy.stop()
        loop = asyncio.get_running_loop()
        notice = loop.time() + 30
        while any(strategy.is_running for strategy in strategies):
            if loop.time() >= notice:
                self.kernel.logger.warning('STOP_WAITING strategies=' + ','.join(
                    str(strategy.id) for strategy in strategies if strategy.is_running))
                notice = loop.time() + 30
            await asyncio.sleep(.05)

    async def _stop_after_managed_exit(self):
        # NT的固定post_stop等待从策略完成退出之后开始，核单与退避期间保留连接。
        await self._drain_managed_strategies()
        await super().stop_async()
