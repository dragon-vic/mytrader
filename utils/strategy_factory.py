from __future__ import annotations

import importlib
from decimal import Decimal
from typing import Any
from typing import get_type_hints

from utils.instrument_factory import InstrumentFactory
from utils.report_writer import run_reports_dir


# 根据 set 里的策略类配置动态构建策略实例。
def build_strategy(settings: dict[str, Any], run_type: str = "backtest"):
    strategy = settings["strategy"]
    module = importlib.import_module(strategy["module"])
    config_cls = getattr(module, strategy["config_class"])
    instruments = InstrumentFactory(settings, run_type)
    params = strategy_params(settings, config_cls, run_type, instruments)

    config = config_cls(
        instrument_ids=[instruments.instrument_id(market) for market in instruments.markets],
        bar_types=instruments.bar_types(),
        **params,
    )
    return getattr(module, strategy["class"])(config)


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
            instruments.instrument_id(market)
            for market in instruments.markets
        ]
    if "event_log_path" in fields and params.get("event_log_path", "auto") == "auto":
        params["event_log_path"] = str(run_reports_dir(settings, run_type) / "strategy_events.csv")
    return params
