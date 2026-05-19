from __future__ import annotations

from nautilus_trader.config import CacheConfig


# 构建 NT cache 配置，当前只收紧 bar/tick 内存容量。
def cache_config(settings: dict) -> CacheConfig:
    capacity = int(settings["runtime"]["cache_capacity"])
    return CacheConfig(
        tick_capacity=capacity,
        bar_capacity=capacity,
        drop_instruments_on_reset=False,
    )
