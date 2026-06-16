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

from utils.config_loader import ROOT
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory


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
def environment(settings: dict[str, Any]) -> HyperliquidEnvironment:
    return getattr(HyperliquidEnvironment, HYPERLIQUID_ENVS[settings["mode"]])


# 构建 Hyperliquid instrument provider 配置。
def instrument_provider(cfg: dict[str, Any]) -> InstrumentProviderConfig:
    if cfg["markets_all"]:
        return InstrumentProviderConfig(load_all=True)
    factory = InstrumentFactory.from_client(cfg)
    return InstrumentProviderConfig(
        load_all=False,
        load_ids=frozenset(factory.instrument_id(market) for market in factory.markets),
    )


def routing(cfg: dict[str, Any]) -> RoutingConfig:
    return RoutingConfig(default=True, venues=frozenset({Venue(cfg["venue"])}))


# 构建 Hyperliquid live data client 配置。
def build_data_client(settings: dict[str, Any], cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        HyperliquidDataClientConfig(
            environment=environment(settings),
            proxy_url=proxy_url(settings),
            testnet=bool(settings["mode"] == "testnet"),
            product_types=enum_tuple(cfg["product_types"]),
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        HyperliquidLiveDataClientFactory,
    )


# 构建 Hyperliquid live exec client 配置。
def build_exec_client(settings: dict[str, Any], cfg: dict[str, Any]):
    load_dotenv(ROOT / ".env")
    return (
        cfg["client_id"],
        HyperliquidExecClientConfig(
            private_key=os.environ["HYPERLIQUID_PRIVATE_KEY"],
            vault_address=os.environ.get("HYPERLIQUID_VAULT_ADDRESS"),
            account_address=os.environ.get("HYPERLIQUID_ACCOUNT_ADDRESS"),
            environment=environment(settings),
            proxy_url=proxy_url(settings),
            testnet=bool(settings["mode"] == "testnet"),
            product_types=enum_tuple(cfg["product_types"]),
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        HyperliquidLiveExecClientFactory,
    )
