from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import LatencyModel
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from adapters.common import cache_config
from utils.config_loader import ROOT
from utils.config_loader import load_settings
from utils.instrument_factory import InstrumentFactory
from utils.market_data import MarketDataStore
from utils.report_writer import log_file_settings
from utils.report_writer import prepare_report_dir
from utils.report_writer import print_backtest_summary
from utils.report_writer import write_backtest_result
from utils.report_writer import write_trader_reports
from utils.runtime_ids import claim_run
from utils.strategy_factory import build_strategy


# 为当前 set 创建一个新的 NT 回测引擎。
def build_backtest_engine(settings: dict[str, Any]) -> BacktestEngine:
    engine = BacktestEngine(
        config=BacktestEngineConfig(
            cache=cache_config(settings),
            logging=LoggingConfig(
                log_level=settings["backtest"]["logging"]["log_level"],
                bypass_logging=bool(settings["backtest"]["logging"]["bypass"]),
                **log_file_settings(settings, "backtest"),
            ),
        ),
    )
    factories = backtest_factories(settings)
    add_venues(engine, settings)
    add_instruments(engine, factories)
    add_datasets(engine, settings, factories)
    engine.add_strategy(build_strategy(settings, "backtest"))
    return engine


# 每个 backtest venue 复用一个 InstrumentFactory。
def backtest_factories(settings: dict[str, Any]) -> dict[str, InstrumentFactory]:
    return {
        venue["client"]: InstrumentFactory.from_client(venue["client_config"])
        for venue in settings["backtest"]["venues"]
    }


# 按 yaml 里的 backtest.venues 添加一个或多个撮合 venue。
def add_venues(engine: BacktestEngine, settings: dict[str, Any]) -> None:
    for venue in settings["backtest"]["venues"]:
        kwargs = {}
        optional = (
            "use_market_order_acks",
            "bar_execution",
            "trade_execution",
            "liquidity_consumption",
            "use_position_ids",
            "use_random_ids",
        )
        for key in optional:
            if key in venue:
                kwargs[key] = venue[key]
        if "default_leverage" in venue:
            kwargs["default_leverage"] = Decimal(str(venue["default_leverage"]))
        if "latency" in venue:
            kwargs["latency_model"] = latency_model(venue["latency"])
        engine.add_venue(
            venue=Venue(venue["client_config"]["venue"]),
            oms_type=getattr(OmsType, venue["oms_type"]),
            account_type=getattr(AccountType, venue["account_type"]),
            base_currency=None,
            starting_balances=starting_balances(venue),
            **kwargs,
        )


# YAML 用毫秒配置延迟，传给 NT 时转换为纳秒。
def latency_model(cfg: dict[str, Any]) -> LatencyModel:
    ns_per_ms = 1_000_000
    return LatencyModel(
        base_latency_nanos=latency_ms(cfg["base_latency_ms"], ns_per_ms),
        insert_latency_nanos=latency_ms(cfg["insert_latency_ms"], ns_per_ms),
        update_latency_nanos=latency_ms(cfg["update_latency_ms"], ns_per_ms),
        cancel_latency_nanos=latency_ms(cfg["cancel_latency_ms"], ns_per_ms),
    )


def latency_ms(value: Any, ns_per_ms: int) -> int:
    return int(float(str(value)) * ns_per_ms)


def starting_balances(venue: dict[str, Any]) -> list[Money]:
    values = venue.get("starting_balances") or [venue["starting_balance"]]
    return [Money.from_str(str(value)) for value in values]


# 所有 venue 的 instrument 先注册到回测引擎。
def add_instruments(engine: BacktestEngine, factories: dict[str, InstrumentFactory]) -> None:
    seen: set[str] = set()
    for factory in factories.values():
        for instrument in factory.instruments():
            key = str(instrument.id)
            if key in seen:
                continue
            seen.add(key)
            engine.add_instrument(instrument)


# 按 backtest.datasets 加载行情数据。
def add_datasets(
    engine: BacktestEngine,
    settings: dict[str, Any],
    factories: dict[str, InstrumentFactory],
) -> None:
    instruments = instruments_by_id(factories)
    for dataset in settings["backtest"]["datasets"]:
        data_type = dataset["type"]
        if data_type == "quote_ticks":
            engine.add_data(load_quote_ticks(ROOT / dataset["path"], instruments))
        elif data_type == "trade_ticks":
            engine.add_data(load_trade_ticks(ROOT / dataset["path"], factories[dataset["client"]]))
        elif data_type == "trade_tick_catalog":
            add_trade_tick_catalog(engine, ROOT / dataset["path"], factories[dataset["client"]])
        elif data_type == "bars":
            add_bars(engine, settings, dataset, factories[dataset["client"]])
        else:
            raise ValueError(f"Unsupported backtest dataset type: {data_type}")


def instruments_by_id(factories: dict[str, InstrumentFactory]) -> dict[str, Any]:
    return {
        str(instrument.id): instrument
        for factory in factories.values()
        for instrument in factory.instruments()
    }


# 读取标准 quote parquet：instrument_id,bid,ask,bid_size,ask_size,ts_ns。
def load_quote_ticks(path: Path, instruments: dict[str, Any]) -> list[QuoteTick]:
    df = pd.read_parquet(path, columns=["instrument_id", "bid", "ask", "bid_size", "ask_size", "ts_ns"])
    ticks = []
    for row in df.sort_values("ts_ns", kind="mergesort").itertuples(index=False):
        instrument = instruments[str(row.instrument_id)]
        ticks.append(
            QuoteTick(
                instrument_id=instrument.id,
                bid_price=Price(Decimal(str(row.bid)), instrument.price_precision),
                ask_price=Price(Decimal(str(row.ask)), instrument.price_precision),
                bid_size=Quantity(positive_size(row.bid_size), instrument.size_precision),
                ask_size=Quantity(positive_size(row.ask_size), instrument.size_precision),
                ts_event=int(row.ts_ns),
                ts_init=int(row.ts_ns),
            ),
        )
    return ticks


def positive_size(value: Any) -> Decimal:
    number = Decimal(str(value))
    return number if number > 0 else Decimal("0.001")


# 读取 Binance trade tick parquet：symbol,timestamp_ms,price,quantity,buyer_maker,trade_id。
def load_trade_ticks(path: Path, factory: InstrumentFactory):
    store = market_store(factory)
    return store.load_trade_ticks(path)


# 从 NT parquet catalog 读取 trade ticks。
def add_trade_tick_catalog(engine: BacktestEngine, path: Path, factory: InstrumentFactory) -> None:
    catalog = ParquetDataCatalog(str(path))
    ids = [str(factory.instrument_id(market)) for market in factory.markets]
    for instrument in catalog.instruments(instrument_ids=ids):
        engine.add_instrument(instrument)
    for instrument_id in ids:
        engine.add_data(catalog.trade_ticks(instrument_ids=[instrument_id]))


# K 线数据沿用 MarketDataStore 的 CSV -> NT Bar 转换。
def add_bars(
    engine: BacktestEngine,
    settings: dict[str, Any],
    dataset: dict[str, Any],
    factory: InstrumentFactory,
) -> None:
    store = market_store(factory, settings, dataset)
    for market in factory.markets:
        engine.add_data(store.load_bars(market))


def market_store(
    factory: InstrumentFactory,
    settings: dict[str, Any] | None = None,
    dataset: dict[str, Any] | None = None,
) -> MarketDataStore:
    store = MarketDataStore.__new__(MarketDataStore)
    store.settings = settings or {"project": {"data_dir": "data"}}
    if dataset and "data_dir" in dataset:
        store.settings = {**store.settings, "project": {"data_dir": dataset["data_dir"]}}
    store.factory = factory
    store.markets = factory.markets
    return store


# 把 NT 生成的报告保存到当前 set 对应的目录。
def write_reports(engine: BacktestEngine, result, settings: dict) -> None:
    payload = write_backtest_result(result, settings)
    print("生成报告...")
    write_trader_reports(engine.trader, settings, "backtest")
    print_backtest_summary(payload, settings)


# 运行回测，由 run.py 负责传入配置名。
def main(config_name: str) -> None:
    settings = load_settings(config_name, mode="backtest")
    settings["mode"] = "backtest"
    settings = claim_run(settings)
    engine = None
    try:
        prepare_report_dir(settings, "backtest")
        engine = build_backtest_engine(settings)
        engine.run()
        write_reports(engine, engine.get_result(), settings)
    finally:
        if engine is not None:
            engine.dispose()
