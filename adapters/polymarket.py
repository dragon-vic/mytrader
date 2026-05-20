from __future__ import annotations

import os

from dotenv import load_dotenv
from nautilus_trader.adapters.polymarket.config import PolymarketDataClientConfig
from nautilus_trader.adapters.polymarket.config import PolymarketExecClientConfig
from nautilus_trader.adapters.polymarket.factories import PolymarketLiveDataClientFactory
from nautilus_trader.adapters.polymarket.factories import PolymarketLiveExecClientFactory
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProviderConfig
from nautilus_trader.config import RoutingConfig
from nautilus_trader.model.identifiers import Venue

from adapters.bundle import ClientBundle
from adapters.common import cache_config
from utils.config_loader import ROOT
from utils.config_loader import markets_all
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory

POLYMARKET_CLIENT_NAME = "POLYMARKET"


# 从 .env 读取 Polymarket 固定凭证变量。
def credentials() -> dict[str, str]:
    return {
        "private_key": os.environ["POLYMARKET_SIGNER"],
        "funder": os.environ["POLYMARKET_FUNDER"],
        "api_key": os.environ["POLYMARKET_API_KEY"],
        "api_secret": os.environ["POLYMARKET_API_SECRET"],
        "passphrase": os.environ["POLYMARKET_PASSPHRASE"],
    }


# 构建 Polymarket instrument provider 配置。
def instrument_config(settings: dict) -> PolymarketInstrumentProviderConfig:
    cfg = settings["polymarket"]["instrument_provider"]
    load_all = bool(cfg["load_all"]) or markets_all(settings)
    load_ids = None
    if not load_all:
        factory = InstrumentFactory(settings)
        load_ids = frozenset(factory.instrument_id(market) for market in factory.markets)
    return PolymarketInstrumentProviderConfig(
        load_all=load_all,
        load_ids=load_ids,
        use_gamma_markets=bool(cfg["use_gamma_markets"]),
        event_slug_builder=cfg["event_slug_builder"],
    )


# 构建 Polymarket live data/exec client 注册包。
def build_client_bundle(settings: dict) -> ClientBundle:
    load_dotenv(ROOT / ".env")
    venue = Venue(settings["exchange"]["venue"])
    routing = RoutingConfig(default=True, venues=frozenset({venue}))
    provider = instrument_config(settings)
    signature_type = int(settings["polymarket"]["signature_type"])
    creds = credentials()

    data_config = PolymarketDataClientConfig(
        instrument_config=provider,
        venue=venue,
        signature_type=signature_type,
        **creds,
        proxy_url=proxy_url(settings),
        routing=routing,
    )
    exec_config = PolymarketExecClientConfig(
        instrument_config=provider,
        venue=venue,
        signature_type=signature_type,
        **creds,
        proxy_url=proxy_url(settings),
        routing=routing,
    )
    return ClientBundle(
        name=POLYMARKET_CLIENT_NAME,
        cache=cache_config(settings),
        data_config=data_config,
        exec_config=exec_config,
        data_factory=PolymarketLiveDataClientFactory,
        exec_factory=PolymarketLiveExecClientFactory,
    )
