from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from nautilus_trader.adapters.kraken.config import KrakenDataClientConfig
from nautilus_trader.adapters.kraken.config import KrakenEnvironment
from nautilus_trader.adapters.kraken.config import KrakenExecClientConfig
from nautilus_trader.adapters.kraken.factories import KrakenLiveDataClientFactory
from nautilus_trader.adapters.kraken.factories import KrakenLiveExecClientFactory
from nautilus_trader.common.config import InstrumentProviderConfig
from nautilus_trader.config import RoutingConfig
from nautilus_trader.core.nautilus_pyo3 import KrakenProductType
from nautilus_trader.model.identifiers import Venue

from adapters.common import LiveContext
from adapters.common import load_ids
from adapters.common import market_dict
from adapters.common import normalize_markets
from utils.config_loader import ROOT


KRAKEN_ENVS = {
    "testnet": "DEMO",
    "live": "MAINNET",
}


# 把 YAML 中的名称转成 Kraken enum tuple。
def enum_tuple(enum_cls, values: list[str] | tuple[str, ...] | None):
    if values is None:
        return None
    return tuple(getattr(enum_cls, value) for value in values)


# 根据 live/testnet 模式选择 Kraken 环境。
def environment(mode: str) -> KrakenEnvironment:
    return getattr(KrakenEnvironment, KRAKEN_ENVS[mode])


# 从 .env 读取 Kraken 执行凭证。
def credentials() -> tuple[str, str]:
    return os.environ["KRAKEN_API_KEY"], os.environ["KRAKEN_API_SECRET"]


def normalize_client(cfg: dict[str, Any]) -> None:
    def normalize(value: object) -> dict[str, Any]:
        market = market_dict(value, cfg["quote_currency"])
        if "symbol" not in market:
            raise KeyError("kraken.markets[] requires symbol")
        base, quote = str(market["symbol"]).split("/")
        raw_symbol = market.get("raw_symbol") or f"{base}{quote}"
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


# 构建 Kraken instrument provider 配置。
def instrument_provider(cfg: dict[str, Any]) -> InstrumentProviderConfig:
    if cfg["markets_all"]:
        return InstrumentProviderConfig(load_all=True)
    return InstrumentProviderConfig(
        load_all=False,
        load_ids=load_ids(cfg),
    )


def routing(cfg: dict[str, Any]) -> RoutingConfig:
    return RoutingConfig(default=True, venues=frozenset({Venue(cfg["venue"])}))


# 构建 Kraken live data client 配置。
def build_data_client(context: LiveContext, cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        KrakenDataClientConfig(
            environment=environment(context.mode),
            proxy_url=context.proxy_url,
            product_types=enum_tuple(KrakenProductType, cfg["product_types"]),
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        KrakenLiveDataClientFactory,
    )


# 构建 Kraken live exec client 配置。
def build_exec_client(context: LiveContext, cfg: dict[str, Any]):
    load_dotenv(ROOT / ".env")
    api_key, api_secret = credentials()
    return (
        cfg["client_id"],
        KrakenExecClientConfig(
            api_key=api_key,
            api_secret=api_secret,
            environment=environment(context.mode),
            proxy_url=context.proxy_url,
            product_types=enum_tuple(KrakenProductType, cfg["product_types"]),
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        KrakenLiveExecClientFactory,
    )
