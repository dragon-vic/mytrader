from __future__ import annotations

from typing import Any

from adapters.spec import AdapterSpec


# 根据 YAML 里的 exchange.venue 选择交易平台 adapter。
def build_adapter(settings: dict[str, Any]) -> AdapterSpec:
    venue = settings["exchange"]["venue"].upper()
    if venue == "BINANCE":
        from adapters.binance import build_adapter as build

        return build(settings)
    if venue == "POLYMARKET":
        from adapters.polymarket import build_adapter as build

        return build(settings)
    raise ValueError(f"Unsupported exchange.venue: {venue}")
