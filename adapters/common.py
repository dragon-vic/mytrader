from __future__ import annotations

from nautilus_trader.config import CacheConfig


# 构建 NT cache 配置，当前只收紧 bar/tick 内存容量。
def cache_config(settings: dict) -> CacheConfig:
    return CacheConfig(
        tick_capacity=int(settings["cache"]["tick_capacity"]),
        bar_capacity=int(settings["cache"]["bar_capacity"]),
        drop_instruments_on_reset=False,
    )
