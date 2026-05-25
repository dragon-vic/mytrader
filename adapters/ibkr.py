from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from ibapi.common import MarketDataTypeEnum as IBMarketDataTypeEnum
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersDataClientConfig
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersExecClientConfig
from nautilus_trader.adapters.interactive_brokers.config import (
    InteractiveBrokersInstrumentProviderConfig,
)
from nautilus_trader.adapters.interactive_brokers.config import SymbologyMethod
from nautilus_trader.adapters.interactive_brokers.factories import (
    InteractiveBrokersLiveDataClientFactory,
)
from nautilus_trader.adapters.interactive_brokers.factories import (
    InteractiveBrokersLiveExecClientFactory,
)
from nautilus_trader.config import RoutingConfig
from nautilus_trader.model.identifiers import InstrumentId

from utils.config_loader import ROOT


# 从配置构建 IBKR 连接参数。
def connection(cfg: dict[str, Any]) -> dict[str, Any]:
    port = cfg["ibg_port"]
    return {
        "ibg_host": str(cfg["ibg_host"]),
        "ibg_port": None if port is None else int(port),
        "ibg_client_id": int(cfg["ibg_client_id"]),
        "connection_timeout": int(cfg["connection_timeout"]),
        "request_timeout_secs": int(cfg["request_timeout_secs"]),
    }


# 把配置里的市场转成 NT InstrumentId。
def load_ids(cfg: dict[str, Any]) -> frozenset[InstrumentId]:
    return frozenset(
        InstrumentId.from_str(market["instrument_id"])
        for market in cfg["markets"]
    )


# 构建 IBKR instrument provider 配置。
def instrument_provider(cfg: dict[str, Any]) -> InteractiveBrokersInstrumentProviderConfig:
    provider = cfg["instrument_provider"]
    return InteractiveBrokersInstrumentProviderConfig(
        load_ids=load_ids(cfg),
        symbology_method=getattr(SymbologyMethod, provider["symbology_method"]),
        build_options_chain=provider["build_options_chain"],
        build_futures_chain=provider["build_futures_chain"],
        min_expiry_days=provider["min_expiry_days"],
        max_expiry_days=provider["max_expiry_days"],
        convert_exchange_to_mic_venue=bool(provider["convert_exchange_to_mic_venue"]),
        symbol_to_mic_venue=dict(provider["symbol_to_mic_venue"]),
        cache_validity_days=provider["cache_validity_days"],
        pickle_path=provider["pickle_path"],
        filter_sec_types=frozenset(provider["filter_sec_types"]),
    )


def routing(cfg: dict[str, Any]) -> RoutingConfig:
    venues = frozenset(market["venue"] for market in cfg["markets"]) or None
    return RoutingConfig(default=True, venues=venues)


# 构建 IBKR live data client 配置。
def build_data_client(settings: dict[str, Any], cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        InteractiveBrokersDataClientConfig(
            **connection(cfg),
            instrument_provider=instrument_provider(cfg),
            use_regular_trading_hours=bool(cfg["use_regular_trading_hours"]),
            market_data_type=getattr(IBMarketDataTypeEnum, cfg["market_data_type"]),
            ignore_quote_tick_size_updates=bool(cfg["ignore_quote_tick_size_updates"]),
            routing=routing(cfg),
        ),
        InteractiveBrokersLiveDataClientFactory,
    )


# 构建 IBKR live exec client 配置。
def build_exec_client(settings: dict[str, Any], cfg: dict[str, Any]):
    load_dotenv(ROOT / ".env")
    return (
        cfg["client_id"],
        InteractiveBrokersExecClientConfig(
            **connection(cfg),
            instrument_provider=instrument_provider(cfg),
            account_id=os.environ["TWS_ACCOUNT"].strip(),
            fetch_all_open_orders=bool(cfg["fetch_all_open_orders"]),
            track_option_exercise_from_position_update=bool(
                cfg["track_option_exercise_from_position_update"],
            ),
            routing=routing(cfg),
        ),
        InteractiveBrokersLiveExecClientFactory,
    )
