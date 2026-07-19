from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.config import BinanceExecClientConfig
from nautilus_trader.adapters.binance.config import BinanceInstrumentProviderConfig
from nautilus_trader.adapters.binance.factories import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.factories import BinanceLiveExecClientFactory
from nautilus_trader.adapters.binance.futures.enums import BinanceFuturesMarginType
from nautilus_trader.config import RoutingConfig
from nautilus_trader.model.identifiers import Venue

from adapters.common import LiveContext
from adapters.common import load_ids
from adapters.common import market_dict
from adapters.common import normalize_markets
from utils.config_loader import ROOT

BINANCE_ENVS = {
    "testnet": {
        "environment": "TESTNET",
        "api_key": "BINANCE_FUTURES_TESTNET_API_KEY",
        "api_secret": "BINANCE_FUTURES_TESTNET_API_SECRET",
    },
    "live": {
        "environment": "LIVE",
        "api_key": "BINANCE_FUTURES_API_KEY",
        "api_secret": "BINANCE_FUTURES_API_SECRET",
    },
}


# 根据运行模式推导 Binance 环境。
def environment(mode: str) -> BinanceEnvironment:
    return getattr(BinanceEnvironment, BINANCE_ENVS[mode]["environment"])


# 根据运行模式从 .env 读取 Binance 执行凭证。
def credentials(mode: str) -> tuple[str, str]:
    envs = BINANCE_ENVS[mode]
    return os.environ[envs["api_key"]], os.environ[envs["api_secret"]]


# 把 Binance 短 symbol 展开为 live 层统一使用的 InstrumentId。
def normalize_client(cfg: dict[str, Any]) -> None:
    def normalize(value: object) -> dict[str, Any]:
        market = market_dict(value, cfg["quote_currency"])
        if "symbol" not in market:
            raise KeyError("binance.markets[] requires symbol")
        base, quote = str(market["symbol"]).split("/")
        raw_symbol = market.get("raw_symbol") or f"{base}{quote}"
        instrument_symbol = market.get("instrument_symbol") or (
            raw_symbol if cfg["instrument_kind"] == "spot" else f"{raw_symbol}-PERP"
        )
        return {
            **market,
            "base_currency": market.get("base_currency", base),
            "quote_currency": market.get("quote_currency", quote),
            "settlement_currency": market.get("settlement_currency", quote),
            "raw_symbol": raw_symbol,
            "instrument_symbol": instrument_symbol,
            "instrument_id": f"{instrument_symbol}.{cfg['venue']}",
        }

    normalize_markets(cfg, normalize)


# 构建 Binance instrument provider 配置。
def instrument_provider(cfg: dict[str, Any]) -> BinanceInstrumentProviderConfig:
    if cfg["markets_all"]:
        return BinanceInstrumentProviderConfig(load_all=True)
    return BinanceInstrumentProviderConfig(
        load_all=False,
        load_ids=load_ids(cfg),
    )


# 从 client 配置构建合约全仓/逐仓设置。
def futures_margin_types(cfg: dict[str, Any]) -> dict[BinanceSymbol, BinanceFuturesMarginType] | None:
    margin_type = cfg.get("margin_type")
    if margin_type is None or cfg["markets_all"]:
        return None
    return {
        BinanceSymbol(market["raw_symbol"]): getattr(BinanceFuturesMarginType, margin_type)
        for market in cfg["markets"]
    }


# 从 set 配置为每个 Binance futures symbol 设置 NT 原生初始杠杆。
def futures_leverages(cfg: dict[str, Any]) -> dict[BinanceSymbol, int] | None:
    leverage = cfg.get("leverage")
    if leverage is None or cfg["markets_all"]:
        return None
    value = int(leverage)
    if value <= 0:
        raise ValueError("binance futures leverage must be positive")
    return {
        BinanceSymbol(market["raw_symbol"]): value
        for market in cfg["markets"]
    }


def routing(cfg: dict[str, Any]) -> RoutingConfig:
    return RoutingConfig(default=True, venues=frozenset({Venue(cfg["venue"])}))


# 构建 Binance live data client 配置。
def build_data_client(context: LiveContext, cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        BinanceDataClientConfig(
            account_type=getattr(BinanceAccountType, cfg["account_type"]),
            environment=environment(context.mode),
            proxy_url=context.proxy_url,
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        BinanceLiveDataClientFactory,
    )


# 构建 Binance live exec client 配置。
def build_exec_client(context: LiveContext, cfg: dict[str, Any]):
    load_dotenv(ROOT / ".env")
    api_key, api_secret = credentials(context.mode)
    return (
        cfg["client_id"],
        BinanceExecClientConfig(
            api_key=api_key,
            api_secret=api_secret,
            account_type=getattr(BinanceAccountType, cfg["account_type"]),
            environment=environment(context.mode),
            proxy_url=context.proxy_url,
            futures_leverages=futures_leverages(cfg),
            futures_margin_types=futures_margin_types(cfg),
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        BinanceLiveExecClientFactory,
    )
