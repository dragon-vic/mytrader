from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from nautilus_trader.core.nautilus_pyo3 import AccountId
from nautilus_trader.core.nautilus_pyo3 import BinanceDataClientConfig
from nautilus_trader.core.nautilus_pyo3 import BinanceDataClientFactory
from nautilus_trader.core.nautilus_pyo3 import BinanceEnvironment
from nautilus_trader.core.nautilus_pyo3 import BinanceExecClientConfig
from nautilus_trader.core.nautilus_pyo3 import BinanceExecutionClientFactory
from nautilus_trader.core.nautilus_pyo3 import BinanceMarginType
from nautilus_trader.core.nautilus_pyo3 import BinanceProductType
from nautilus_trader.core.nautilus_pyo3 import CacheConfig
from nautilus_trader.core.nautilus_pyo3 import TraderId

from adapters.spec import AdapterSpec
from utils.arguments import BINANCE_CLIENT_NAME
from utils.config_loader import ROOT
from utils.config_loader import market_configs
from utils.instrument_factory import InstrumentFactory


# 构建 Binance v2 PyO3 live/testnet client 配置。
class BinanceConfigBuilder:
    def __init__(self, settings: dict[str, Any]) -> None:
        self.settings = settings
        self.factory = InstrumentFactory(settings)

    # 构建 Rust cache 配置。
    def cache_config(self) -> CacheConfig:
        capacity = int(self.settings["runtime"]["cache_capacity"])
        return CacheConfig(
            tick_capacity=capacity,
            bar_capacity=capacity,
            drop_instruments_on_reset=False,
        )

    # 从 live.account_type 推导 Binance Rust product type。
    def product_type(self) -> BinanceProductType:
        mapping = {
            "USDT_FUTURES": BinanceProductType.USD_M,
            "COIN_FUTURES": BinanceProductType.COIN_M,
            "SPOT": BinanceProductType.SPOT,
            "MARGIN": BinanceProductType.MARGIN,
            "OPTIONS": BinanceProductType.OPTIONS,
        }
        return mapping[self.settings["live"]["account_type"]]

    # 从 live.environment 推导 Binance Rust environment。
    def environment(self) -> BinanceEnvironment:
        mapping = {
            "LIVE": BinanceEnvironment.MAINNET,
            "MAINNET": BinanceEnvironment.MAINNET,
            "TESTNET": BinanceEnvironment.TESTNET,
            "DEMO": BinanceEnvironment.DEMO,
        }
        return mapping[self.settings["live"]["environment"]]

    # 构建 Binance 合约全仓/逐仓设置。
    def futures_margin_types(self) -> dict[str, BinanceMarginType] | None:
        margin_type = self.settings["live"].get("margin_type")
        if margin_type is None:
            return None
        margin = getattr(BinanceMarginType, margin_type)
        return {market["raw_symbol"]: margin for market in market_configs(self.settings)}

    # 构建 Binance 合约杠杆设置。
    def futures_leverages(self) -> dict[str, int] | None:
        leverage = self.settings["live"].get("leverage")
        if leverage is None:
            return None
        return {market["raw_symbol"]: int(leverage) for market in market_configs(self.settings)}

    # 构建 Binance data client 配置。
    def data_config(self) -> BinanceDataClientConfig:
        return BinanceDataClientConfig(
            product_types=[self.product_type()],
            environment=self.environment(),
        )

    # 构建 Binance exec client 配置。
    def exec_config(self) -> BinanceExecClientConfig:
        load_dotenv(ROOT / ".env")
        return BinanceExecClientConfig(
            trader_id=TraderId(self.settings["runtime"]["trader_id"]),
            account_id=AccountId(self.settings["live"].get("account_id", "BINANCE-001")),
            product_types=[self.product_type()],
            environment=self.environment(),
            api_key=os.environ[self.settings["live"]["api_key_env"]],
            api_secret=os.environ[self.settings["live"]["api_secret_env"]],
            futures_leverages=self.futures_leverages(),
            futures_margin_types=self.futures_margin_types(),
        )


# 返回 LiveNodeBuilder 可注册的 Binance PyO3 adapter spec。
def build_adapter(settings: dict[str, Any]) -> AdapterSpec:
    builder = BinanceConfigBuilder(settings)
    return AdapterSpec(
        cache=builder.cache_config(),
        data={BINANCE_CLIENT_NAME: (BinanceDataClientFactory(), builder.data_config())},
        exec={BINANCE_CLIENT_NAME: (BinanceExecutionClientFactory(), builder.exec_config())},
    )

