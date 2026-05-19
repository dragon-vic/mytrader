from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from nautilus_trader.core.nautilus_pyo3 import CacheConfig
from nautilus_trader.core.nautilus_pyo3 import PolymarketDataClientConfig
from nautilus_trader.core.nautilus_pyo3 import PolymarketDataClientFactory
from nautilus_trader.core.nautilus_pyo3 import PolymarketExecClientConfig
from nautilus_trader.core.nautilus_pyo3 import PolymarketExecutionClientFactory
from nautilus_trader.core.nautilus_pyo3 import SignatureType

from adapters.spec import AdapterSpec
from utils.config_loader import ROOT


POLYMARKET_CLIENT_NAME = "POLYMARKET"


# 构建 Polymarket data client 配置。
def data_config(settings: dict[str, Any]) -> PolymarketDataClientConfig:
    poly = settings.get("polymarket", {})
    kwargs = {
        "base_url_http": poly.get("base_url_http"),
        "base_url_ws": poly.get("base_url_ws"),
        "base_url_gamma": poly.get("base_url_gamma"),
        "base_url_data_api": poly.get("base_url_data_api"),
        "http_timeout_secs": poly.get("http_timeout_secs"),
        "ws_timeout_secs": poly.get("ws_timeout_secs"),
        "ws_max_subscriptions": poly.get("ws_max_subscriptions"),
        "update_instruments_interval_mins": poly.get("update_instruments_interval_mins"),
        "subscribe_new_markets": poly.get("subscribe_new_markets"),
        "auto_load_missing_instruments": poly.get("auto_load_missing_instruments"),
        "auto_load_debounce_ms": poly.get("auto_load_debounce_ms"),
    }
    return PolymarketDataClientConfig(**{key: value for key, value in kwargs.items() if value is not None})


# 构建 Polymarket exec client 配置。
def exec_config(settings: dict[str, Any]) -> PolymarketExecClientConfig:
    load_dotenv(ROOT / ".env")
    live = settings["live"]
    poly = settings.get("polymarket", {})
    signature_types = {
        0: SignatureType.Eoa,
        1: SignatureType.PolyProxy,
        2: SignatureType.PolyGnosisSafe,
    }
    kwargs = {
        "trader_id": settings["runtime"]["trader_id"],
        "account_id": live.get("account_id", "POLYMARKET-001"),
        "private_key": os.environ.get(live.get("private_key_env", "POLYMARKET_PK")),
        "api_key": os.environ.get(live.get("api_key_env", "POLYMARKET_API_KEY")),
        "api_secret": os.environ.get(live.get("api_secret_env", "POLYMARKET_API_SECRET")),
        "passphrase": os.environ.get(live.get("passphrase_env", "POLYMARKET_PASSPHRASE")),
        "funder": os.environ.get(live.get("funder_env", "POLYMARKET_FUNDER")),
        "signature_type": signature_types[int(poly.get("signature_type", 0))],
        "base_url_http": poly.get("base_url_http"),
        "base_url_ws": poly.get("base_url_ws"),
        "base_url_data_api": poly.get("base_url_data_api"),
        "http_timeout_secs": poly.get("http_timeout_secs"),
        "max_retries": poly.get("max_retries"),
        "retry_delay_initial_ms": poly.get("retry_delay_initial_ms"),
        "retry_delay_max_ms": poly.get("retry_delay_max_ms"),
        "ack_timeout_secs": poly.get("ack_timeout_secs"),
    }
    return PolymarketExecClientConfig(**{key: value for key, value in kwargs.items() if value is not None})


# 返回 LiveNodeBuilder 可注册的 Polymarket PyO3 adapter spec。
def build_adapter(settings: dict[str, Any]) -> AdapterSpec:
    capacity = int(settings["runtime"]["cache_capacity"])
    return AdapterSpec(
        cache=CacheConfig(
            tick_capacity=capacity,
            bar_capacity=capacity,
            drop_instruments_on_reset=False,
        ),
        data={POLYMARKET_CLIENT_NAME: (PolymarketDataClientFactory(), data_config(settings))},
        exec={POLYMARKET_CLIENT_NAME: (PolymarketExecutionClientFactory(), exec_config(settings))},
    )
