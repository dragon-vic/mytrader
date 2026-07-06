from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from utils.arguments import DEFAULT_CONFIG_NAME

ROOT = Path(__file__).resolve().parent.parent
STRATEGIES_DIR = ROOT / "strategies"
GLOBAL_CONFIG_PATH = STRATEGIES_DIR / "global.yaml"
BACKTEST_MODE_KEYS = {"engine", "strategy", "exec", "reports", "actors", "runtime"}

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
    "okx_spot": {
        "venue": "OKX",
        "adapter": "okx",
        "instrument_kind": "spot",
        "client_id": "OKX_SPOT",
        "quote_currency": "USDT",
    },
    "okx_swap": {
        "venue": "OKX",
        "adapter": "okx",
        "instrument_kind": "perpetual",
        "client_id": "OKX_SWAP",
        "quote_currency": "USDT",
    },
    "kraken_spot": {
        "venue": "KRAKEN",
        "adapter": "kraken",
        "instrument_kind": "spot",
        "client_id": "KRAKEN_SPOT",
        "quote_currency": "USD",
    },
    "kraken_futures": {
        "venue": "KRAKEN",
        "adapter": "kraken",
        "instrument_kind": "perpetual",
        "client_id": "KRAKEN_FUTURES",
        "quote_currency": "USD",
    },
    "hyperliquid_perp": {
        "venue": "HYPERLIQUID",
        "adapter": "hyperliquid",
        "instrument_kind": "perpetual",
        "client_id": "HYPERLIQUID",
        "quote_currency": "USD",
    },
    "polymarket": {
        "venue": "POLYMARKET",
        "adapter": "polymarket",
        "instrument_kind": "binary_option",
        "client_id": "POLYMARKET",
    },
    "ibkr": {
        "venue": "INTERACTIVE_BROKERS",
        "adapter": "ibkr",
        "instrument_kind": "ibkr",
        "client_id": "INTERACTIVE_BROKERS",
    },
    "external_signal": {
        "venue": "EXTERNAL_SIGNAL",
        "adapter": "external_signal",
        "client_id": "EXTERNAL_SIGNAL",
    },
    "external_command": {
        "venue": "EXTERNAL_COMMAND",
        "adapter": "external_command",
        "client_id": "EXTERNAL_COMMAND",
    },
}


# 返回所有带可运行配置的策略目录配置。
def config_paths() -> list[Path]:
    return sorted(
        config_path(path.name)
        for path in STRATEGIES_DIR.iterdir()
        if path.is_dir() and has_config(path)
    )


# 返回交互入口可选择的策略目录名。
def config_names() -> list[str]:
    return sorted(path.parent.name for path in config_paths())


# 策略目录只要有传统 config 或拆分后的 live/backtest config 就可出现在菜单里。
def has_config(path: Path) -> bool:
    return any((path / name).exists() for name in ("config.yaml", "live_config.yaml", "backtest_config.yaml"))


# 查找策略目录内的配置文件；拆分配置按运行模式选择。
def config_path(config_name: str, mode: str | None = None) -> Path:
    folder = STRATEGIES_DIR / config_name
    names = config_names_for_mode(mode)
    for name in names:
        path = folder / name
        if path.exists():
            return path
    raise FileNotFoundError(f"Config not found: strategies/{config_name}/{'|'.join(names)}")


def config_names_for_mode(mode: str | None) -> tuple[str, ...]:
    if mode in {"live", "testnet"}:
        return ("live_config.yaml", "config.yaml")
    if mode == "backtest":
        return ("backtest_config.yaml", "config.yaml")
    return ("config.yaml", "live_config.yaml", "backtest_config.yaml")


# 加载一个具名 set，让每个策略保留自己的市场和参数。
def load_settings(config_name: str | None = None, mode: str | None = None) -> dict[str, Any]:
    name = config_name or DEFAULT_CONFIG_NAME
    path = config_path(name, mode)
    with GLOBAL_CONFIG_PATH.open("r", encoding="utf-8") as f:
        global_settings = yaml.safe_load(f)
    with path.open("r", encoding="utf-8") as f:
        strategy_settings = yaml.safe_load(f)
    global_settings = apply_mode_layer(global_settings, mode)
    if not is_split_config(path):
        strategy_settings = apply_mode_layer(strategy_settings, mode)
    settings = deep_merge(global_settings, strategy_settings)
    settings["mode"] = mode
    settings["project"]["config_name"] = name
    settings["project"]["config_path"] = str(path)
    settings["project"]["strategy_dir"] = str(path.parent)
    normalize_settings(settings, mode)
    return settings


def is_split_config(path: Path) -> bool:
    return path.name in {"live_config.yaml", "backtest_config.yaml"}


# 递归合并配置，右侧 set 配置覆盖左侧 global 配置。
def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


# 选择 yaml 里的 live/backtest 模式层；backtest.engine 是 NT 回测引擎参数。
def apply_mode_layer(settings: dict[str, Any], mode: str | None) -> dict[str, Any]:
    if mode is None:
        return settings
    backtest_is_mode = is_backtest_mode_layer(settings.get("backtest"))
    base = {
        key: value
        for key, value in settings.items()
        if key != "live" and not (key == "backtest" and backtest_is_mode)
    }
    section = settings.get(mode)
    if mode == "testnet" and section is None:
        section = settings.get("live")
    if section is None:
        return base
    if mode == "backtest" and backtest_is_mode:
        overlay = dict(section)
        engine = overlay.pop("engine", None)
        selected = merge_mode_overlay(base, overlay)
        if engine is not None:
            selected["backtest"] = deep_merge(base.get("backtest", {}), engine)
        return selected
    return merge_mode_overlay(base, section)


# 模式层里的 strategy 是完整替换，避免 live/backtest 策略参数互相污染。
def merge_mode_overlay(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    left = dict(base)
    if "strategy" in overlay:
        left.pop("strategy", None)
    return deep_merge(left, overlay)


# 旧 backtest 是引擎配置；新 backtest 同时含 strategy/exec/engine 等模式层字段。
def is_backtest_mode_layer(section: Any) -> bool:
    return isinstance(section, dict) and bool(BACKTEST_MODE_KEYS.intersection(section))


# 校验并展开策略模块路径；module 写当前策略目录内的 py 文件名。
def normalize_strategy(settings: dict[str, Any]) -> None:
    strategy = settings["strategy"]
    folder = Path(settings["project"]["strategy_dir"]).name
    for key in ("module", "class", "config"):
        if key not in strategy:
            raise KeyError(f"strategy.{key} is required in {settings['project']['config_path']}")
    module = str(strategy["module"])
    if module.startswith("strategies."):
        raise ValueError("strategy.module should be relative to its strategy folder, for example: pre_ipo")
    strategy["module"] = f"strategies.{folder}.{module}"


def client_meta(key: str) -> dict[str, str]:
    if key not in CLIENTS:
        raise ValueError(f"Unsupported client key: {key}")
    return CLIENTS[key]


# 返回某个 client 的规范化配置；data 优先，找不到再用 exec。
def client_config(settings: dict[str, Any], key: str) -> dict[str, Any]:
    source = settings["data"]["clients"].get(key) or settings["exec"]["clients"].get(key)
    if source is None:
        raise ValueError(f"client not found: {key}")
    return source


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


# 补齐 OKX 现货和 U 本位永续市场字段。
def normalize_okx_market(
    market: dict[str, Any],
    key: str,
    cfg: dict[str, Any],
    defaults: dict[str, Any],
) -> dict[str, Any]:
    meta = client_meta(key)
    if "symbol" not in market:
        raise KeyError(f"{key}.markets[] requires symbol")
    base, quote = market["symbol"].split("/")
    kind = cfg.get("instrument_kind", meta["instrument_kind"])
    raw_symbol = market.get("raw_symbol") or (
        f"{base}-{quote}" if kind == "spot" else f"{base}-{quote}-SWAP"
    )
    return {
        **market,
        "client_key": key,
        "exchange": "okx",
        "venue": meta["venue"],
        "instrument_kind": kind,
        "base_currency": market.get("base_currency", base),
        "quote_currency": market.get("quote_currency", quote),
        "settlement_currency": market.get("settlement_currency", quote),
        "raw_symbol": raw_symbol,
        "instrument_symbol": market.get("instrument_symbol", raw_symbol),
        "timeframe": market.get("timeframe", defaults.get("timeframe")),
        "limit": market.get("limit", defaults.get("limit")),
        "batches": market.get("batches", defaults.get("batches")),
    }


# 补齐 Kraken 现货和永续市场字段，复杂 raw symbol 允许在 YAML 显式覆盖。
def normalize_kraken_market(
    market: dict[str, Any],
    key: str,
    cfg: dict[str, Any],
    defaults: dict[str, Any],
) -> dict[str, Any]:
    meta = client_meta(key)
    if "symbol" not in market:
        raise KeyError(f"{key}.markets[] requires symbol")
    base, quote = market["symbol"].split("/")
    kind = cfg.get("instrument_kind", meta["instrument_kind"])
    raw_symbol = market.get("raw_symbol") or f"{base}{quote}"
    return {
        **market,
        "client_key": key,
        "exchange": "kraken",
        "venue": meta["venue"],
        "instrument_kind": kind,
        "base_currency": market.get("base_currency", base),
        "quote_currency": market.get("quote_currency", quote),
        "settlement_currency": market.get("settlement_currency", quote),
        "raw_symbol": raw_symbol,
        "instrument_symbol": market.get("instrument_symbol", raw_symbol),
        "timeframe": market.get("timeframe", defaults.get("timeframe")),
        "limit": market.get("limit", defaults.get("limit")),
        "batches": market.get("batches", defaults.get("batches")),
    }


# 补齐 Hyperliquid 永续和 HIP-3 市场字段。
def normalize_hyperliquid_market(
    market: dict[str, Any],
    key: str,
    cfg: dict[str, Any],
    defaults: dict[str, Any],
) -> dict[str, Any]:
    meta = client_meta(key)
    if "symbol" not in market:
        raise KeyError(f"{key}.markets[] requires symbol")
    base, quote = market["symbol"].split("/")
    raw_symbol = market.get("raw_symbol", base)
    return {
        **market,
        "client_key": key,
        "exchange": "hyperliquid",
        "venue": meta["venue"],
        "instrument_kind": cfg.get("instrument_kind", meta["instrument_kind"]),
        "base_currency": market.get("base_currency", base),
        "quote_currency": market.get("quote_currency", quote),
        "settlement_currency": market.get("settlement_currency", quote),
        "raw_symbol": raw_symbol,
        "instrument_symbol": market.get("instrument_symbol", raw_symbol),
        "timeframe": market.get("timeframe", defaults.get("timeframe")),
        "limit": market.get("limit", defaults.get("limit")),
        "batches": market.get("batches", defaults.get("batches")),
    }


# 补齐一个 IBKR InstrumentId 市场。
def normalize_ibkr_market(value: str, key: str) -> dict[str, Any]:
    meta = client_meta(key)
    if not isinstance(value, str):
        raise TypeError(f"{key}.markets[] must be an InstrumentId string")
    if "." not in value:
        raise ValueError(f"{key}.markets[] must be an InstrumentId like AAPL.XNAS")
    symbol, venue = value.rsplit(".", 1)
    return {
        "client_key": key,
        "exchange": "ibkr",
        "venue": venue,
        "instrument_kind": meta["instrument_kind"],
        "raw_symbol": symbol,
        "instrument_symbol": symbol,
        "instrument_id": value,
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

    if meta["adapter"] in {"external_signal", "external_command"}:
        return

    markets = cfg["markets"]
    if markets == "all":
        if meta["adapter"] == "ibkr":
            raise ValueError("ibkr.markets does not support all; use explicit InstrumentId strings")
        cfg["markets_all"] = True
        cfg["markets"] = []
        return

    cfg["markets_all"] = False
    data_defaults = settings.get("backtest", {}).get("data", {})
    if meta["adapter"] == "binance":
        rows = [market_dict(market, meta.get("quote_currency", "USDT")) for market in markets]
        cfg["markets"] = [
            normalize_binance_market(row, key, cfg, data_defaults)
            for row in rows
        ]
    elif meta["adapter"] == "okx":
        rows = [market_dict(market, meta.get("quote_currency", "USDT")) for market in markets]
        cfg["markets"] = [
            normalize_okx_market(row, key, cfg, data_defaults)
            for row in rows
        ]
    elif meta["adapter"] == "kraken":
        rows = [market_dict(market, meta.get("quote_currency", "USD")) for market in markets]
        cfg["markets"] = [
            normalize_kraken_market(row, key, cfg, data_defaults)
            for row in rows
        ]
    elif meta["adapter"] == "hyperliquid":
        rows = [market_dict(market, meta.get("quote_currency", "USD")) for market in markets]
        cfg["markets"] = [
            normalize_hyperliquid_market(row, key, cfg, data_defaults)
            for row in rows
        ]
    elif meta["adapter"] == "polymarket":
        rows = [market_dict(market, meta.get("quote_currency", "USDT")) for market in markets]
        cfg["markets"] = [normalize_poly_market(row, key) for row in rows]
    elif meta["adapter"] == "ibkr":
        cfg["markets"] = [normalize_ibkr_market(market, key) for market in markets]


# 从 live client 配置和 backtest venue 覆盖项生成本次回测使用的 client 配置。
def normalize_backtest_venue(settings: dict[str, Any], venue: dict[str, Any]) -> dict[str, Any]:
    key = venue["client"]
    if key == "ibkr":
        raise ValueError("IBKR backtest is not supported; use IBKR only for live/testnet")
    cfg = dict(client_config(settings, key))
    for name in ("markets", "instrument", "instrument_kind"):
        if name in venue:
            cfg[name] = venue[name]
    if "instrument" not in cfg:
        cfg["instrument"] = settings["backtest"]["instrument"]
    normalize_client_markets(key, cfg, settings)
    venue["client_config"] = cfg
    return cfg


# 没有显式 backtest.venues 的旧配置，在加载期转换成统一的新结构。
def normalize_legacy_backtest(settings: dict[str, Any]) -> None:
    backtest = settings["backtest"]
    if backtest.get("venues"):
        return
    key = backtest["client"]
    backtest.update({
        "key": key,
        "adapter": client_meta(key)["adapter"],
        "venue": client_meta(key)["venue"],
        "account_type": client_meta(key).get("account_type"),
        "instrument_kind": client_meta(key)["instrument_kind"],
    })
    normalize_client_markets(key, backtest, settings)
    account = backtest["venue_account"]
    backtest["venues"] = [
        {
            "client": key,
            "markets": backtest["markets"],
            "instrument": backtest["instrument"],
            "starting_balance": account["starting_balance"],
            "account_type": account["account_type"],
            "oms_type": account["oms_type"],
        },
    ]
    if not backtest.get("datasets"):
        data = backtest.get("data", {})
        if "tick_data_path" in data:
            backtest["datasets"] = [{"type": "trade_ticks", "client": key, "path": data["tick_data_path"]}]
        elif "tick_catalog" in data:
            backtest["datasets"] = [{"type": "trade_tick_catalog", "client": key, "path": data["tick_catalog"]}]
        else:
            backtest["datasets"] = [{"type": "bars", "client": key}]


# 回测按 venues 聚合市场，入口只读取这一种规范化结果。
def normalize_backtest(settings: dict[str, Any]) -> None:
    normalize_legacy_backtest(settings)
    markets = []
    markets_all = False
    for venue in settings["backtest"]["venues"]:
        cfg = normalize_backtest_venue(settings, venue)
        markets_all = markets_all or bool(cfg.get("markets_all"))
        markets.extend(cfg["markets"])
    settings["markets"] = markets
    settings["markets_all"] = markets_all


# 回测策略需要自动展开 instrument 时，从当前 backtest venue 中找对应 client。
def backtest_client_config(settings: dict[str, Any], key: str) -> dict[str, Any]:
    for venue in settings["backtest"]["venues"]:
        if venue["client"] == key:
            return venue["client_config"]
    raise ValueError(f"backtest.venues does not include client: {key}")


# 补齐从短写可推导的字段。
def normalize_settings(settings: dict[str, Any], mode: str | None) -> None:
    normalize_strategy(settings)

    for key, cfg in settings["data"]["clients"].items():
        normalize_client_markets(key, cfg, settings)
    for key, cfg in settings["exec"]["clients"].items():
        normalize_client_markets(key, cfg, settings)

    if mode == "backtest":
        normalize_backtest(settings)
    elif mode in ("testnet", "live"):
        settings.pop("backtest", None)

    if mode == "backtest":
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
