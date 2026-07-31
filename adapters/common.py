from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Callable

from nautilus_trader.config import CacheConfig
from nautilus_trader.model.identifiers import InstrumentId


@dataclass(frozen=True)
class LiveContext:
    mode: str
    proxy_url: str | None


def market_dict(value: str | dict[str, Any], quote_currency: str) -> dict[str, Any]:
    if isinstance(value, str):
        symbol = value if "/" in value else f"{value}/{quote_currency}"
        return {"symbol": symbol}
    if not isinstance(value, dict):
        raise TypeError("markets entries must be strings or mappings")
    return dict(value)


# 统一处理显式 markets 和 all，具体 symbol 规则由 adapter 回调负责。
def normalize_markets(
    cfg: dict[str, Any],
    normalize: Callable[[Any], dict[str, Any]],
    supports_all: bool = True,
) -> None:
    markets = cfg["markets"]
    if markets == "all":
        if not supports_all:
            raise ValueError(f"{cfg['adapter']}.markets does not support all")
        cfg["markets_all"] = True
        cfg["markets"] = []
        return
    if not isinstance(markets, list):
        raise TypeError(f"{cfg['adapter']}.markets must be a list or all")
    cfg["markets_all"] = False
    cfg["markets"] = [normalize(value) for value in markets]


def load_ids(cfg: dict[str, Any]) -> frozenset[InstrumentId]:
    return frozenset(InstrumentId.from_str(market["instrument_id"]) for market in cfg["markets"])


# 构建 NT cache 配置，当前只收紧 bar/tick 内存容量。
def cache_config(settings: dict) -> CacheConfig:
    return CacheConfig(
        tick_capacity=int(settings["cache"]["tick_capacity"]),
        bar_capacity=int(settings["cache"]["bar_capacity"]),
        drop_instruments_on_reset=False,
    )
