from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from nautilus_trader.adapters.polymarket.config import PolymarketDataClientConfig
from nautilus_trader.adapters.polymarket.config import PolymarketExecClientConfig
from nautilus_trader.adapters.polymarket.factories import PolymarketLiveDataClientFactory
from nautilus_trader.adapters.polymarket.factories import PolymarketLiveExecClientFactory
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProviderConfig
from nautilus_trader.config import RoutingConfig
from nautilus_trader.model.identifiers import Venue

from utils.config_loader import ROOT
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory


# 从 .env 读取 Polymarket 固定凭证变量。
def credentials() -> dict[str, str]:
    return {
        "private_key": os.environ["POLYMARKET_PK"],
        "funder": os.environ["POLYMARKET_FUNDER"],
        "api_key": os.environ["POLYMARKET_API_KEY"],
        "api_secret": os.environ["POLYMARKET_API_SECRET"],
        "passphrase": os.environ["POLYMARKET_PASSPHRASE"],
    }


# 构建 Polymarket instrument provider 配置。
def instrument_config(cfg: dict[str, Any]) -> PolymarketInstrumentProviderConfig:
    provider = cfg["instrument_provider"]
    load_ids = None
    if not cfg["markets_all"]:
        factory = InstrumentFactory.from_client(cfg)
        load_ids = frozenset(factory.instrument_id(market) for market in factory.markets)
    return PolymarketInstrumentProviderConfig(
        load_all=bool(provider["load_all"]) or cfg["markets_all"],
        load_ids=load_ids,
        use_gamma_markets=bool(provider["use_gamma_markets"]),
        event_slug_builder=provider["event_slug_builder"],
    )


def routing(cfg: dict[str, Any]) -> RoutingConfig:
    return RoutingConfig(default=True, venues=frozenset({Venue(cfg["venue"])}))


# 构建 Polymarket live data client 配置。
def build_data_client(settings: dict[str, Any], cfg: dict[str, Any]):
    load_dotenv(ROOT / ".env")
    provider = instrument_config(cfg)
    venue = Venue(cfg["venue"])
    creds = credentials()
    return (
        cfg["client_id"],
        PolymarketDataClientConfig(
            instrument_config=provider,
            venue=venue,
            signature_type=int(cfg["signature_type"]),
            **creds,
            proxy_url=proxy_url(settings),
            routing=routing(cfg),
        ),
        PolymarketLiveDataClientFactory,
    )


# 构建 Polymarket live exec client 配置。
def build_exec_client(settings: dict[str, Any], cfg: dict[str, Any]):
    load_dotenv(ROOT / ".env")
    provider = instrument_config(cfg)
    venue = Venue(cfg["venue"])
    creds = credentials()
    return (
        cfg["client_id"],
        PolymarketExecClientConfig(
            instrument_config=provider,
            venue=venue,
            signature_type=int(cfg["signature_type"]),
            **creds,
            proxy_url=proxy_url(settings),
            routing=routing(cfg),
        ),
        PolymarketLiveExecClientFactory,
    )
