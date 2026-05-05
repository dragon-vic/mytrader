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
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory

BINANCE_CLIENT_NAME = "BINANCE"


# 构建 Binance live/testnet node 需要的 client 配置。
class BinanceConfigBuilder:
    def __init__(self, settings: dict[str, Any]) -> None:
        self.settings = settings
        self.factory = InstrumentFactory(settings)
        self.instruments = self.factory.instruments()
        self.venues = frozenset(market["venue"] for market in self.factory.markets)

    # 构建 NT cache 配置，当前只收紧 bar/tick 内存容量。
    def cache_config(self) -> CacheConfig:
        capacity = int(self.settings.get("runtime", {}).get("cache_capacity", 1000))
        return CacheConfig(
            tick_capacity=capacity,
            bar_capacity=capacity,
            drop_instruments_on_reset=False,
        )

    # 构建 Binance data/exec client 共用的 instrument provider 配置。
    def instrument_provider(self) -> BinanceInstrumentProviderConfig:
        return BinanceInstrumentProviderConfig(
            load_all=False,
            load_ids=frozenset(instrument.id for instrument in self.instruments),
        )

    # 构建 Binance live data client 配置。
    def data_config(self) -> BinanceDataClientConfig:
        return BinanceDataClientConfig(
            account_type=getattr(BinanceAccountType, self.settings["live"]["account_type"]),
            environment=getattr(BinanceEnvironment, self.settings["live"]["environment"]),
            proxy_url=proxy_url(self.settings),
            instrument_provider=self.instrument_provider(),
            routing=RoutingConfig(default=True, venues=self.venues),
        )

    # 构建 Binance live exec client 配置。
    def exec_config(self) -> BinanceExecClientConfig:
        load_dotenv(ROOT / ".env")
        return BinanceExecClientConfig(
            api_key=os.environ[self.settings["live"]["api_key_env"]],
            api_secret=os.environ[self.settings["live"]["api_secret_env"]],
            account_type=getattr(BinanceAccountType, self.settings["live"]["account_type"]),
            environment=getattr(BinanceEnvironment, self.settings["live"]["environment"]),
            proxy_url=proxy_url(self.settings),
            instrument_provider=self.instrument_provider(),
            routing=RoutingConfig(default=True, venues=self.venues),
        )
