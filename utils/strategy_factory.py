from __future__ import annotations

import importlib
import os
from decimal import Decimal
from typing import Any

from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory
from utils.report_writer import run_reports_dir


# 根据 set 里的策略类配置动态构建策略实例。
def build_strategy(settings: dict[str, Any], run_type: str = "backtest"):
    strategy = settings["strategy"]
    module = importlib.import_module(strategy["module"])
    config_cls = getattr(module, strategy["config_class"])
    instruments = InstrumentFactory(settings)
    params = strategy_params(settings, config_cls, run_type)

    if "markets" in settings:
        config = config_cls(
            instrument_ids=[instrument.id for instrument in instruments.instruments()],
            bar_types=instruments.bar_types(),
            trade_notional=Decimal(str(strategy["trade_notional"])),
            **params,
        )
    else:
        market = instruments.markets[0]
        if "trade_notional" in getattr(config_cls, "__annotations__", {}):
            config = config_cls(
                instrument_id=instruments.instrument(market).id,
                bar_type=instruments.bar_type(market),
                trade_notional=Decimal(str(strategy["trade_notional"])),
                **params,
            )
        else:
            config = config_cls(
                instrument_id=instruments.instrument(market).id,
                bar_type=instruments.bar_type(market),
                trade_size=Decimal(str(strategy["trade_size"])),
                **params,
            )
    return getattr(module, strategy["class"])(config)


# 按策略配置字段自动补充运行类型、报告路径和全局代理。
def strategy_params(settings: dict[str, Any], config_cls, run_type: str) -> dict[str, Any]:
    params = dict(settings["strategy"].get("params", {}))
    fields = getattr(config_cls, "__annotations__", {})
    if "event_log_path" in fields and params.get("event_log_path", "auto") == "auto":
        params["event_log_path"] = str(run_reports_dir(settings, run_type) / "strategy_events.csv")
    if "proxy_url" in fields and "proxy_url" not in params:
        params["proxy_url"] = proxy_url(settings) or ""
    if "use_live_funding" in fields and "use_live_funding" not in params:
        params["use_live_funding"] = run_type == "live"
    if "api_key" in fields and "api_key" not in params and run_type == "live":
        params["api_key"] = os.environ[settings["live"]["api_key_env"]]
    if "api_secret" in fields and "api_secret" not in params and run_type == "live":
        params["api_secret"] = os.environ[settings["live"]["api_secret_env"]]
    return params
