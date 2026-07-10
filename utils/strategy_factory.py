from __future__ import annotations

import importlib
from decimal import Decimal
from typing import Any
from typing import get_type_hints

from utils.config_loader import backtest_client_config
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory
from utils.report_writer import run_reports_dir


def decimal_param(value: object) -> Decimal:
    if isinstance(value, bool):
        raise TypeError("numeric parameter must not be bool")
    return Decimal(str(value))


# 根据 set 里的策略类配置动态构建策略实例。
def single_strategy(settings: dict[str, Any]) -> dict[str, Any]:
    strategies = settings["strategy"]
    if len(strategies) != 1:
        raise ValueError(f"backtest requires exactly one strategy, got: {','.join(strategies)}")
    return next(iter(strategies.values()))


# 根据一个策略配置动态构建策略实例。
def build_strategy(settings: dict[str, Any], name: str, run_type: str = "backtest"):
    strategy = settings["strategy"][name]
    module = importlib.import_module(strategy["module"])
    config_cls = getattr(module, strategy["config"])
    params = strategy_params(settings, strategy, config_cls, run_type)
    config = config_cls(**params)
    return getattr(module, strategy["class"])(config)


def build_strategies(settings: dict[str, Any], run_type: str = "live"):
    return [build_strategy(settings, name, run_type) for name in settings["strategy"]]


def strategy_instruments(settings: dict[str, Any], run_type: str, params: dict[str, Any]) -> InstrumentFactory:
    client = params["instrument_client"]
    if run_type == "backtest":
        return InstrumentFactory.from_client(backtest_client_config(settings, client))
    node = settings["node"]
    source = node["data"]["clients"].get(client)
    if source is None or not source["enabled"]:
        raise ValueError(f"strategy.params.instrument_client is not an enabled data client: {client}")
    return InstrumentFactory.from_client(source)


# 从 enabled data clients 自动展开 instrument id。
def enabled_instruments(settings: dict[str, Any]) -> list[str]:
    values = []
    for cfg in settings["node"]["data"]["clients"].values():
        if not cfg.get("enabled") or cfg.get("markets_all"):
            continue
        if cfg["adapter"] in {"external_signal", "external_command"}:
            continue
        factory = InstrumentFactory.from_client(cfg)
        values.extend(str(factory.instrument_id(market)) for market in factory.markets)
    return values


# 按策略配置字段自动补充运行类型、报告路径和全局代理。
def strategy_params(
    settings: dict[str, Any],
    strategy: dict[str, Any],
    config_cls,
    run_type: str,
) -> dict[str, Any]:
    params = dict(strategy.get("params", {}))
    fields = get_type_hints(config_cls)
    if "use_hyphens_in_client_order_ids" in fields:
        params["use_hyphens_in_client_order_ids"] = False
    if "instruments" in fields and params.get("instruments") == "enabled":
        params["instruments"] = enabled_instruments(settings)
    if "instrument_ids" in fields or "bar_types" in fields:
        if "instrument_client" not in params:
            raise KeyError("strategy.params.instrument_client is required")
        instruments = strategy_instruments(settings, run_type, params)
        if "instrument_ids" in fields:
            params["instrument_ids"] = [instruments.instrument_id(market) for market in instruments.markets]
        if "bar_types" in fields:
            params["bar_types"] = instruments.bar_types()
    for key, value in list(params.items()):
        if fields.get(key) is Decimal:
            params[key] = decimal_param(value)
    if params.get("external_order_claims") is True:
        instruments = strategy_instruments(settings, run_type, params)
        params["external_order_claims"] = [
            instruments.instrument_id(market)
            for market in instruments.markets
        ]
    if "event_log_path" in fields and params.get("event_log_path", "auto") == "auto":
        params["event_log_path"] = str(run_reports_dir(settings) / "strategy_events.csv")
    if "snapshot_path" in fields and params.get("snapshot_path", "auto") == "auto":
        params["snapshot_path"] = str(run_reports_dir(settings) / f"{settings['project']['config_name']}_snapshot.json")
    if "tick_log_path" in fields and params.get("tick_log_path", "auto") == "auto":
        params["tick_log_path"] = str(run_reports_dir(settings) / "poly_ticks.parquet")
    if "poly_trade_path" in fields and params.get("poly_trade_path", "auto") == "auto":
        params["poly_trade_path"] = str(run_reports_dir(settings) / "poly_trades.parquet")
    if "poly_quote_path" in fields and params.get("poly_quote_path", "auto") == "auto":
        params["poly_quote_path"] = str(run_reports_dir(settings) / "poly_quotes.parquet")
    if "binance_tick_path" in fields and params.get("binance_tick_path", "auto") == "auto":
        params["binance_tick_path"] = str(run_reports_dir(settings) / "binance_btc_ticks.parquet")
    if "event_windows" in fields:
        params["event_windows"] = settings.get("runtime", {}).get("event_windows", {})
    if "proxy_url" in fields:
        params["proxy_url"] = proxy_url(settings) or ""
    params.pop("instrument_client", None)
    return params
