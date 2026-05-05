from __future__ import annotations

import importlib
from decimal import Decimal
from typing import Any

from utils.instrument_factory import InstrumentFactory


# 根据 set 里的策略类配置动态构建策略实例。
def build_strategy(settings: dict[str, Any]):
    strategy = settings["strategy"]
    module = importlib.import_module(strategy["module"])
    config_cls = getattr(module, strategy["config_class"])
    instruments = InstrumentFactory(settings)

    if "markets" in settings:
        config = config_cls(
            instrument_ids=[instrument.id for instrument in instruments.instruments()],
            bar_types=instruments.bar_types(),
            trade_notional=Decimal(str(strategy["trade_notional"])),
            **strategy.get("params", {}),
        )
    else:
        market = instruments.markets[0]
        config = config_cls(
            instrument_id=instruments.instrument(market).id,
            bar_type=instruments.bar_type(market),
            trade_size=Decimal(str(strategy["trade_size"])),
            **strategy.get("params", {}),
        )
    return getattr(module, strategy["class"])(config)
