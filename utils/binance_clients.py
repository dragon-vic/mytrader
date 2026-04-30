from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv

from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.config import BinanceExecClientConfig
from nautilus_trader.adapters.binance.config import BinanceInstrumentProviderConfig
from nautilus_trader.config import CacheConfig
from nautilus_trader.config import RoutingConfig

from utils.config_loader import ROOT
from utils.config_loader import market_configs
from utils.config_loader import proxy_url
from utils.instrument_factory import make_instruments

BINANCE_CLIENT_NAME = "BINANCE"


# 返回当前 set 涉及的全部 NT venue。
def venue_ids(settings: dict[str, Any]) -> frozenset[str]:
    return frozenset(market["venue"] for market in market_configs(settings))


# 构建 NT cache 配置，当前只收紧 bar/tick 内存容量。
def cache_config(settings: dict[str, Any]) -> CacheConfig:
    capacity = int(settings.get("runtime", {}).get("cache_capacity", 1000))
    return CacheConfig(
        tick_capacity=capacity,
        bar_capacity=capacity,
        drop_instruments_on_reset=False,
    )


# 构建 Binance live data/exec client 共用的 instrument provider 配置。
def instrument_provider(settings: dict[str, Any]) -> BinanceInstrumentProviderConfig:
    return BinanceInstrumentProviderConfig(
        load_all=False,
        load_ids=frozenset(instrument.id for instrument in make_instruments(settings)),
    )


# 构建 Binance live data client 配置。
def binance_data_config(settings: dict[str, Any]) -> BinanceDataClientConfig:
    return BinanceDataClientConfig(
        account_type=getattr(BinanceAccountType, settings["live"]["account_type"]),
        environment=getattr(BinanceEnvironment, settings["live"]["environment"]),
        proxy_url=proxy_url(settings),
        instrument_provider=instrument_provider(settings),
        routing=RoutingConfig(default=True, venues=venue_ids(settings)),
    )


# 构建 Binance live exec client 配置。
def binance_exec_config(settings: dict[str, Any]) -> BinanceExecClientConfig:
    load_dotenv(ROOT / ".env")

    return BinanceExecClientConfig(
        api_key=os.environ[settings["live"]["api_key_env"]],
        api_secret=os.environ[settings["live"]["api_secret_env"]],
        account_type=getattr(BinanceAccountType, settings["live"]["account_type"]),
        environment=getattr(BinanceEnvironment, settings["live"]["environment"]),
        proxy_url=proxy_url(settings),
        instrument_provider=instrument_provider(settings),
        routing=RoutingConfig(default=True, venues=venue_ids(settings)),
    )
