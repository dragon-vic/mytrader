from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_NAME = "ema_cross_1"


# 加载一个具名 set，让每个策略保留自己的市场和参数。
def load_settings(config_name: str | None = None) -> dict[str, Any]:
    name = config_name or DEFAULT_CONFIG_NAME
    with (ROOT / "config" / "global.yaml").open("r", encoding="utf-8") as f:
        global_settings = yaml.safe_load(f)
    with (ROOT / "config" / f"{name}.yaml").open("r", encoding="utf-8") as f:
        strategy_settings = yaml.safe_load(f)
    settings = deep_merge(global_settings, strategy_settings)
    normalize_settings(settings)
    settings["project"]["config_name"] = name
    return settings


# 递归合并配置，右侧 set 配置覆盖左侧 global 配置。
def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


# 把 snake_case 名字转成策略类名。
def snake_to_pascal(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("_"))


# 补齐一个市场的交易所、币种和 Binance/NT symbol。
def normalize_market(market: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    instrument_kind = settings["instrument"]["kind"]
    exchange = settings["exchange"]
    base, quote = market["symbol"].split("/")
    raw_symbol = market.get("raw_symbol") or f"{base}{quote}"
    instrument_symbol = market.get("instrument_symbol") or (
        raw_symbol if instrument_kind == "spot" else f"{raw_symbol}-PERP"
    )
    return {
        "timeframe": "1m",
        "limit": 1000,
        "batches": 3,
        "since": None,
        "exchange": exchange["name"],
        "venue": exchange["venue"],
        "base_currency": base,
        "quote_currency": quote,
        "settlement_currency": quote,
        "raw_symbol": raw_symbol,
        "instrument_symbol": instrument_symbol,
        **market,
    }


# 补齐策略模块名、类名和配置类名。
def normalize_strategy(settings: dict[str, Any]) -> None:
    strategy = settings["strategy"]
    name = strategy["name"]
    class_name = snake_to_pascal(name)
    strategy.setdefault("module", f"strategies.{name}")
    strategy.setdefault("class", class_name)
    strategy.setdefault("config_class", f"{class_name}Config")


# 补齐从 symbol 可推导的市场字段。
def normalize_settings(settings: dict[str, Any]) -> None:
    market_defaults = settings.get("market_defaults") or {}
    if "markets" in settings:
        settings["markets"] = [
            normalize_market({**market_defaults, **market}, settings)
            for market in settings["markets"]
        ]
    else:
        settings["market"] = normalize_market({**market_defaults, **settings["market"]}, settings)

    first_market = market_configs(settings)[0]
    settings["instrument"].setdefault("base_currency", first_market["base_currency"])
    settings["instrument"].setdefault("quote_currency", first_market["quote_currency"])
    settings["instrument"].setdefault("settlement_currency", first_market["settlement_currency"])
    if "external" in settings:
        settings["external"].setdefault("source", settings["strategy"]["name"])
    normalize_strategy(settings)


# 所有生成的数据、报告和日志都放在项目目录内。
def ensure_dirs(settings: dict[str, Any]) -> None:
    for path in (
        ROOT / settings["project"]["data_dir"] / "raw",
        reports_dir(settings),
        ROOT / "logs",
    ):
        path.mkdir(parents=True, exist_ok=True)


# 返回当前 set 的报告目录。
def reports_dir(settings: dict[str, Any]) -> Path:
    return ROOT / settings["project"]["reports_dir"] / settings["project"]["config_name"]


# 返回当前 set 的市场列表；旧 set 只有 market，新 set 可以有 markets。
def market_configs(settings: dict[str, Any]) -> list[dict[str, Any]]:
    return settings.get("markets") or [settings["market"]]


# 读取当前 set 的代理地址。
def proxy_url(settings: dict[str, Any]) -> str | None:
    return settings["exchange"].get("proxy_url")
