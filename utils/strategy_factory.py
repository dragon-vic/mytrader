from __future__ import annotations

import importlib
from decimal import Decimal
from typing import Any

from utils.instrument_factory import make_bar_type
from utils.instrument_factory import make_bar_types
from utils.instrument_factory import make_instrument
from utils.instrument_factory import make_instruments


# 根据 set 里的策略类配置动态构建策略实例。
def build_strategy(settings: dict[str, Any]):
    strategy = settings["strategy"]
    module = importlib.import_module(strategy["module"])
    config_cls = getattr(module, strategy["config_class"])
    if "markets" in settings:
        instruments = make_instruments(settings)
        config = config_cls(
            instrument_ids=[instrument.id for instrument in instruments],
            bar_types=make_bar_types(settings),
            trade_notional=Decimal(str(strategy["trade_notional"])),
            **strategy.get("params", {}),
        )
    else:
        instrument = make_instrument(settings)
        config = config_cls(
            instrument_id=instrument.id,
            bar_type=make_bar_type(settings),
            trade_size=Decimal(str(strategy["trade_size"])),
            **strategy.get("params", {}),
        )
    return getattr(module, strategy["class"])(config)
