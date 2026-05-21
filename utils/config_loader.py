from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from utils.arguments import DEFAULT_CONFIG_NAME

ROOT = Path(__file__).resolve().parent.parent

CLIENTS = {
    "binance_spot": {
        "venue": "BINANCE",
        "adapter": "binance",
        "account_type": "SPOT",
        "instrument_kind": "spot",
        "client_id": "BINANCE_SPOT",
        "quote_currency": "USDT",
    },
    "binance_futures": {
        "venue": "BINANCE",
        "adapter": "binance",
        "account_type": "USDT_FUTURES",
        "instrument_kind": "perpetual",
        "client_id": "BINANCE_USDT_FUTURES",
        "quote_currency": "USDT",
    },
    "polymarket": {
        "venue": "POLYMARKET",
        "adapter": "polymarket",
        "instrument_kind": "binary_option",
        "client_id": "POLYMARKET",
    },
    "external_signal": {
        "venue": "EXTERNAL_SIGNAL",
        "adapter": "external_signal",
        "client_id": "EXTERNAL_SIGNAL",
    },
}


# 加载一个具名 set，让每个策略保留自己的市场和参数。
def load_settings(config_name: str | None = None, mode: str | None = None) -> dict[str, Any]:
    name = config_name or DEFAULT_CONFIG_NAME
    with (ROOT / "config" / "global.yaml").open("r", encoding="utf-8") as f:
        global_settings = yaml.safe_load(f)
    with (ROOT / "config" / f"{name}.yaml").open("r", encoding="utf-8") as f:
        strategy_settings = yaml.safe_load(f)
    settings = deep_merge(global_settings, strategy_settings)
    settings["mode"] = mode
    settings["project"]["config_name"] = name
    normalize_settings(settings, mode)
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


# 把 snake_case 策略名转成全小写模块名。
def strategy_module_name(name: str) -> str:
    return name.replace("_", "").lower()


# 补齐策略模块名、类名和配置类名。
def normalize_strategy(settings: dict[str, Any]) -> None:
    strategy = settings["strategy"]
    name = strategy["name"]
    class_name = snake_to_pascal(name)
    strategy.setdefault("module", f"strategies.{strategy_module_name(name)}")
    strategy.setdefault("class", f"{class_name}Strategy")
    strategy.setdefault("config_class", f"{class_name}Config")


def client_meta(key: str) -> dict[str, str]:
    if key not in CLIENTS:
        raise ValueError(f"Unsupported client key: {key}")
    return CLIENTS[key]


# 把 BTC 或 BTC/USDT 这种短写转成市场 dict。
def market_dict(value: str | dict[str, Any], quote_currency: str) -> dict[str, Any]:
    if isinstance(value, str):
        symbol = value if "/" in value else f"{value}/{quote_currency}"
        return {"symbol": symbol}
    return dict(value)


# 补齐一个 Binance 市场的基础字段。
def normalize_binance_market(
    market: dict[str, Any],
    key: str,
    cfg: dict[str, Any],
    defaults: dict[str, Any],
) -> dict[str, Any]:
    meta = client_meta(key)
    if "symbol" not in market:
        raise KeyError(f"{key}.markets[] requires symbol")
    base, quote = market["symbol"].split("/")
    raw_symbol = market.get("raw_symbol") or f"{base}{quote}"
    kind = cfg.get("instrument_kind", meta["instrument_kind"])
    instrument_symbol = market.get("instrument_symbol") or (
        raw_symbol if kind == "spot" else f"{raw_symbol}-PERP"
    )
    return {
        **market,
        "client_key": key,
        "exchange": "binance",
        "venue": meta["venue"],
        "account_type": meta["account_type"],
        "instrument_kind": kind,
        "base_currency": market.get("base_currency", base),
        "quote_currency": market.get("quote_currency", quote),
        "settlement_currency": market.get("settlement_currency", quote),
        "raw_symbol": raw_symbol,
        "instrument_symbol": instrument_symbol,
        "timeframe": market.get("timeframe", defaults.get("timeframe")),
        "limit": market.get("limit", defaults.get("limit")),
        "batches": market.get("batches", defaults.get("batches")),
    }


# 补齐一个 Polymarket 市场的 condition/token instrument id。
def normalize_poly_market(market: dict[str, Any], key: str) -> dict[str, Any]:
    meta = client_meta(key)
    raw_id = market.get("instrument_id")
    if raw_id:
        symbol = str(raw_id).split(".", 1)[0]
    else:
        symbol = f"{market['condition_id']}-{market['token_id']}"
    return {
        **market,
        "client_key": key,
        "exchange": "polymarket",
        "venue": meta["venue"],
        "instrument_kind": "binary_option",
        "base_currency": market.get("outcome", "POLY"),
        "quote_currency": "USD",
        "settlement_currency": "USD",
        "raw_symbol": symbol,
        "instrument_symbol": symbol,
    }


# 按 client key 补齐 markets，并记录 all 语义。
def normalize_client_markets(
    key: str,
    cfg: dict[str, Any],
    settings: dict[str, Any],
) -> None:
    meta = client_meta(key)
    cfg["key"] = key
    cfg["adapter"] = meta["adapter"]
    cfg["venue"] = meta["venue"]
    cfg["client_id"] = meta["client_id"]
    if "account_type" in meta:
        cfg["account_type"] = meta["account_type"]
    if "instrument_kind" in meta:
        cfg["instrument_kind"] = meta["instrument_kind"]

    if meta["adapter"] == "external_signal":
        return

    markets = cfg["markets"]
    if markets == "all":
        cfg["markets_all"] = True
        cfg["markets"] = []
        return

    cfg["markets_all"] = False
    data_defaults = settings.get("backtest", {}).get("data", {})
    rows = [market_dict(market, meta.get("quote_currency", "USDT")) for market in markets]
    if meta["adapter"] == "binance":
        cfg["markets"] = [
            normalize_binance_market(row, key, cfg, data_defaults)
            for row in rows
        ]
    elif meta["adapter"] == "polymarket":
        cfg["markets"] = [normalize_poly_market(row, key) for row in rows]


# 补齐从短写可推导的字段。
def normalize_settings(settings: dict[str, Any], mode: str | None) -> None:
    normalize_strategy(settings)

    for key, cfg in settings["data"]["clients"].items():
        normalize_client_markets(key, cfg, settings)
    for key, cfg in settings["exec"]["clients"].items():
        normalize_client_markets(key, cfg, settings)

    if mode == "backtest":
        backtest = settings["backtest"]
        key = backtest["client"]
        for cfg in settings["data"]["clients"].values():
            cfg["enabled"] = False
        for cfg in settings["exec"]["clients"].values():
            cfg["enabled"] = False
        backtest.update({
            "key": key,
            "adapter": client_meta(key)["adapter"],
            "venue": client_meta(key)["venue"],
            "account_type": client_meta(key).get("account_type"),
            "instrument_kind": client_meta(key)["instrument_kind"],
        })
        normalize_client_markets(key, backtest, settings)
    elif mode in ("testnet", "live"):
        settings.pop("backtest", None)

    if mode == "backtest":
        settings["markets"] = settings["backtest"]["markets"]
        settings["markets_all"] = bool(settings["backtest"]["markets_all"])
        return

    client = settings["strategy"].get("params", {}).get("instrument_client")
    if client is None:
        settings["markets"] = []
        settings["markets_all"] = False
        return

    source = settings["data"]["clients"].get(client) or settings["exec"]["clients"].get(client)
    if source is None:
        raise ValueError(f"strategy.params.instrument_client not found: {client}")
    settings["markets"] = source["markets"]
    settings["markets_all"] = bool(source.get("markets_all"))


# 读取全局代理地址，Linux 下忽略代理。
def proxy_url(settings: dict[str, Any]) -> str | None:
    if os.name != "nt":
        return None
    return os.environ.get("PROXY_URL")
