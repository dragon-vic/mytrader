from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from nautilus_trader.adapters.okx.config import OKXDataClientConfig
from nautilus_trader.adapters.okx.config import OKXEnvironment
from nautilus_trader.adapters.okx.config import OKXExecClientConfig
from nautilus_trader.adapters.okx.factories import OKXLiveDataClientFactory
from nautilus_trader.adapters.okx.factories import OKXLiveExecClientFactory
from nautilus_trader.common.config import InstrumentProviderConfig
from nautilus_trader.config import RoutingConfig
from nautilus_trader.core.nautilus_pyo3 import OKXContractType
from nautilus_trader.core.nautilus_pyo3 import OKXInstrumentType
from nautilus_trader.core.nautilus_pyo3 import OKXMarginMode
from nautilus_trader.model.identifiers import Venue

from utils.config_loader import ROOT
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory


OKX_ENVS = {
    "testnet": "DEMO",
    "live": "LIVE",
}


# 把 YAML 中的名称转成 OKX enum tuple。
def enum_tuple(enum_cls, values: list[str] | tuple[str, ...] | None):
    if values is None:
        return None
    return tuple(getattr(enum_cls, value) for value in values)


# 根据 live/testnet 模式选择 OKX 环境。
def environment(settings: dict[str, Any]) -> OKXEnvironment:
    return getattr(OKXEnvironment, OKX_ENVS[settings["mode"]])


# 从 .env 读取 OKX 执行凭证。
def credentials() -> tuple[str, str, str]:
    return (
        os.environ["OKX_API_KEY"],
        os.environ["OKX_API_SECRET"],
        os.environ["OKX_API_PASSPHRASE"],
    )


# 构建 OKX instrument provider 配置。
def instrument_provider(cfg: dict[str, Any]) -> InstrumentProviderConfig:
    if cfg["markets_all"]:
        return InstrumentProviderConfig(load_all=True)
    factory = InstrumentFactory.from_client(cfg)
    return InstrumentProviderConfig(
        load_all=False,
        load_ids=frozenset(factory.instrument_id(market) for market in factory.markets),
    )


def routing(cfg: dict[str, Any]) -> RoutingConfig:
    return RoutingConfig(default=True, venues=frozenset({Venue(cfg["venue"])}))


# 构建 OKX live data client 配置。
def build_data_client(settings: dict[str, Any], cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        OKXDataClientConfig(
            environment=environment(settings),
            proxy_url=proxy_url(settings),
            instrument_types=enum_tuple(OKXInstrumentType, cfg["instrument_types"]),
            contract_types=enum_tuple(OKXContractType, cfg.get("contract_types")),
            instrument_families=tuple(cfg["instrument_families"]) if cfg.get("instrument_families") else None,
            is_demo=bool(cfg["is_demo"]),
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        OKXLiveDataClientFactory,
    )


# 构建 OKX live exec client 配置。
def build_exec_client(settings: dict[str, Any], cfg: dict[str, Any]):
    load_dotenv(ROOT / ".env")
    api_key, api_secret, passphrase = credentials()
    return (
        cfg["client_id"],
        OKXExecClientConfig(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=passphrase,
            environment=environment(settings),
            proxy_url=proxy_url(settings),
            instrument_types=enum_tuple(OKXInstrumentType, cfg["instrument_types"]),
            contract_types=enum_tuple(OKXContractType, cfg.get("contract_types")),
            instrument_families=tuple(cfg["instrument_families"]) if cfg.get("instrument_families") else None,
            is_demo=bool(cfg["is_demo"]),
            margin_mode=getattr(OKXMarginMode, cfg["margin_mode"]),
            instrument_provider=instrument_provider(cfg),
            routing=routing(cfg),
        ),
        OKXLiveExecClientFactory,
    )
