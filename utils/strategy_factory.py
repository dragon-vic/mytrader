from __future__ import annotations

import importlib
from decimal import Decimal
from typing import Any
from typing import get_type_hints

from nautilus_trader.core.nautilus_pyo3 import ImportableStrategyConfig

from utils.instrument_factory import InstrumentFactory
from utils.report_writer import run_reports_dir


# 为 PyO3 node/engine 构建可导入策略配置。
def build_importable_strategy(settings: dict[str, Any], run_type: str = "live") -> ImportableStrategyConfig:
    strategy = settings["strategy"]
    module = importlib.import_module(strategy["module"])
    config_cls = getattr(module, strategy["config_class"])
    instruments = InstrumentFactory(settings, run_type)
    params = strategy_params(settings, config_cls, run_type, instruments)
    config = {
        "instrument_ids": [str(instruments.instrument_id(market)) for market in instruments.markets],
        "bar_types": [str(bar_type) for bar_type in instruments.bar_types()],
        **params,
    }
    return ImportableStrategyConfig(
        strategy_path=f"{strategy['module']}:{strategy['class']}",
        config_path=f"{strategy['module']}:{strategy['config_class']}",
        config=config,
    )


# 按策略配置字段自动补充运行类型、报告路径和全局代理。
def strategy_params(
    settings: dict[str, Any],
    config_cls,
    run_type: str,
    instruments: InstrumentFactory,
) -> dict[str, Any]:
    params = dict(settings["strategy"].get("params", {}))
    fields = get_type_hints(config_cls)
    for key, value in list(params.items()):
        if fields.get(key) is Decimal:
            params[key] = Decimal(str(value))
    if params.get("external_order_claims") is True:
        params["external_order_claims"] = [
            str(instruments.instrument_id(market))
            for market in instruments.markets
        ]
    if "event_log_path" in fields and params.get("event_log_path", "auto") == "auto":
        params["event_log_path"] = str(run_reports_dir(settings, run_type) / "strategy_events.csv")
    return params
