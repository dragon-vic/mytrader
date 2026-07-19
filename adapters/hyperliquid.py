from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from nautilus_trader.adapters.hyperliquid.config import HyperliquidDataClientConfig
from nautilus_trader.adapters.hyperliquid.config import HyperliquidEnvironment
from nautilus_trader.adapters.hyperliquid.config import HyperliquidExecClientConfig
from nautilus_trader.adapters.hyperliquid.enums import HyperliquidProductType
from nautilus_trader.adapters.hyperliquid.factories import HyperliquidLiveDataClientFactory
from nautilus_trader.adapters.hyperliquid.factories import HyperliquidLiveExecClientFactory
from nautilus_trader.common.config import InstrumentProviderConfig
from nautilus_trader.config import RoutingConfig
from nautilus_trader.model.identifiers import Venue

from adapters.common import LiveContext
from adapters.common import load_ids
from adapters.common import market_dict
from adapters.common import normalize_markets
from utils.config_loader import ROOT


HYPERLIQUID_ENVS = {
    "testnet": "TESTNET",
    "live": "MAINNET",
}


# 把 YAML 中的名称转成 Hyperliquid enum tuple。
def enum_tuple(values: list[str] | tuple[str, ...] | None):
    if values is None:
        return None
    return tuple(getattr(HyperliquidProductType, value) for value in values)


# 根据 live/testnet 模式选择 Hyperliquid 环境。
def environment(mode: str) -> HyperliquidEnvironment:
    return getattr(HyperliquidEnvironment, HYPERLIQUID_ENVS[mode])


def normalize_client(cfg: dict[str, Any]) -> None:
    def normalize(value: object) -> dict[str, Any]:
        market = market_dict(value, cfg["quote_currency"])
        if "symbol" not in market:
            raise KeyError("hyperliquid.markets[] requires symbol")
        base, quote = str(market["symbol"]).split("/")
        raw_symbol = market.get("raw_symbol") or base
        instrument_symbol = market.get("instrument_symbol") or raw_symbol
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


# 构建 Hyperliquid instrument provider 配置。
def instrument_provider(cfg: dict[str, Any]) -> InstrumentProviderConfig:
    if cfg["markets_all"]:
        return InstrumentProviderConfig(load_all=True)
    return InstrumentProviderConfig(
        load_all=False,
        load_ids=load_ids(cfg),
    )


def routing(cfg: dict[str, Any]) -> RoutingConfig:
    return RoutingConfig(default=True, venues=frozenset({Venue(cfg["venue"])}))


# 构建 Hyperliquid live data client 配置。
def build_data_client(context: LiveContext, cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        HyperliquidDataClientConfig(
            environment=environment(context.mode),
            proxy_url=context.proxy_url,
            product_types=enum_tuple(cfg["product_types"]),
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        HyperliquidLiveDataClientFactory,
    )


# 构建 Hyperliquid live exec client 配置。
def build_exec_client(context: LiveContext, cfg: dict[str, Any]):
    load_dotenv(ROOT / ".env")
    return (
        cfg["client_id"],
        HyperliquidExecClientConfig(
            private_key=os.environ["HYPERLIQUID_PRIVATE_KEY"],
            vault_address=os.environ.get("HYPERLIQUID_VAULT_ADDRESS"),
            account_address=os.environ.get("HYPERLIQUID_ACCOUNT_ADDRESS"),
            environment=environment(context.mode),
            proxy_url=context.proxy_url,
            product_types=enum_tuple(cfg["product_types"]),
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        HyperliquidLiveExecClientFactory,
    )
