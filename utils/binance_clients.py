from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnvironment
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.config import BinanceExecClientConfig
from nautilus_trader.adapters.binance.config import BinanceInstrumentProviderConfig
from nautilus_trader.adapters.binance.futures.enums import BinanceFuturesMarginType
from nautilus_trader.config import CacheConfig
from nautilus_trader.config import RoutingConfig

from utils.arguments import BINANCE_CLIENT_NAME
from utils.config_loader import ROOT
from utils.config_loader import market_configs
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory


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

    # 从当前 set 的 live.margin_type 构建 Binance 合约全仓/逐仓设置。
    def futures_margin_types(self) -> dict[BinanceSymbol, BinanceFuturesMarginType] | None:
        margin_type = self.settings["live"].get("margin_type")
        if not margin_type:
            return None
        return {
            BinanceSymbol(market["raw_symbol"]): getattr(BinanceFuturesMarginType, margin_type)
            for market in market_configs(self.settings)
        }

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
            futures_margin_types=self.futures_margin_types(),
            instrument_provider=self.instrument_provider(),
            routing=RoutingConfig(default=True, venues=self.venues),
        )
