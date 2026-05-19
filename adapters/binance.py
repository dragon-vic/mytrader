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

from adapters.bundle import ClientBundle
from adapters.common import cache_config
from utils.arguments import BINANCE_CLIENT_NAME
from utils.config_loader import ROOT
from utils.config_loader import market_configs
from utils.config_loader import markets_all
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory

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
def environment(settings: dict[str, Any]) -> BinanceEnvironment:
    mode = settings["mode"]
    return getattr(BinanceEnvironment, BINANCE_ENVS[mode]["environment"])


# 根据运行模式从 .env 读取 Binance API 凭证。
def credentials(settings: dict[str, Any]) -> tuple[str, str]:
    envs = BINANCE_ENVS[settings["mode"]]
    return os.environ[envs["api_key"]], os.environ[envs["api_secret"]]


# 构建 Binance live/testnet node 需要的 client 配置。
class BinanceBuilder:
    def __init__(self, settings: dict[str, Any]) -> None:
        self.settings = settings
        self.factory = InstrumentFactory(settings)
        self.venues = (
            frozenset({settings["exchange"]["venue"]})
            if markets_all(settings)
            else frozenset(market["venue"] for market in self.factory.markets)
        )

    # 构建 Binance data/exec client 共用的 instrument provider 配置。
    def instrument_provider(self) -> BinanceInstrumentProviderConfig:
        if markets_all(self.settings):
            return BinanceInstrumentProviderConfig(load_all=True)
        return BinanceInstrumentProviderConfig(
            load_all=False,
            load_ids=frozenset(
                self.factory.instrument_id(market)
                for market in self.factory.markets
            ),
        )

    # 从当前 set 的 live.margin_type 构建 Binance 合约全仓/逐仓设置。
    def futures_margin_types(self) -> dict[BinanceSymbol, BinanceFuturesMarginType] | None:
        margin_type = self.settings["live"]["margin_type"]
        if margin_type is None or markets_all(self.settings):
            return None
        return {
            BinanceSymbol(market["raw_symbol"]): getattr(BinanceFuturesMarginType, margin_type)
            for market in market_configs(self.settings)
        }

    # 构建 Binance live data client 配置。
    def data_config(self) -> BinanceDataClientConfig:
        return BinanceDataClientConfig(
            account_type=getattr(BinanceAccountType, self.settings["live"]["account_type"]),
            environment=environment(self.settings),
            proxy_url=proxy_url(self.settings),
            instrument_provider=self.instrument_provider(),
            routing=RoutingConfig(default=True, venues=self.venues),
        )

    # 构建 Binance live exec client 配置。
    def exec_config(self) -> BinanceExecClientConfig:
        load_dotenv(ROOT / ".env")
        api_key, api_secret = credentials(self.settings)
        return BinanceExecClientConfig(
            api_key=api_key,
            api_secret=api_secret,
            account_type=getattr(BinanceAccountType, self.settings["live"]["account_type"]),
            environment=environment(self.settings),
            proxy_url=proxy_url(self.settings),
            futures_margin_types=self.futures_margin_types(),
            instrument_provider=self.instrument_provider(),
            routing=RoutingConfig(default=True, venues=self.venues),
        )


# 构建 Binance live data/exec client 注册包。
def build_client_bundle(settings: dict) -> ClientBundle:
    binance = BinanceBuilder(settings)
    return ClientBundle(
        name=BINANCE_CLIENT_NAME,
        cache=cache_config(settings),
        data_config=binance.data_config(),
        exec_config=binance.exec_config(),
        data_factory=BinanceLiveDataClientFactory,
        exec_factory=BinanceLiveExecClientFactory,
    )
