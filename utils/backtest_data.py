from __future__ import annotations

import pickle
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler

from utils.config_loader import ROOT
from utils.instrument_factory import InstrumentFactory


def instrument_map(factories: dict[str, InstrumentFactory]) -> dict[str, Any]:
    return {
        str(instrument.id): instrument
        for factory in factories.values()
        for instrument in factory.instruments()
    }


def add_instruments(engine, factories: dict[str, InstrumentFactory]) -> None:
    for instrument in instrument_map(factories).values():
        engine.add_instrument(instrument)


def load_quote_objects(path: Path) -> list[QuoteTick]:
    with path.open("rb") as stream:
        ticks = pickle.load(stream)
    if any(not isinstance(tick, QuoteTick) for tick in ticks):
        raise TypeError(f"{path} must contain only QuoteTick objects")
    return ticks


def positive(value: object, field: str) -> Decimal:
    number = Decimal(str(value))
    if number <= 0:
        raise ValueError(f"{field} must be positive, got {value}")
    return number


# 标准 quote parquet 字段：instrument_id,bid,ask,bid_size,ask_size,ts_ns。
def load_quotes(path: Path, instruments: dict[str, Any]) -> list[QuoteTick]:
    columns = ["instrument_id", "bid", "ask", "bid_size", "ask_size", "ts_ns"]
    data = pd.read_parquet(path, columns=columns).sort_values("ts_ns", kind="mergesort")
    ticks = []
    for row in data.itertuples(index=False):
        instrument = instruments[str(row.instrument_id)]
        ticks.append(
            QuoteTick(
                instrument_id=instrument.id,
                bid_price=Price(Decimal(str(row.bid)), instrument.price_precision),
                ask_price=Price(Decimal(str(row.ask)), instrument.price_precision),
                bid_size=Quantity(positive(row.bid_size, "bid_size"), instrument.size_precision),
                ask_size=Quantity(positive(row.ask_size, "ask_size"), instrument.size_precision),
                ts_event=int(row.ts_ns),
                ts_init=int(row.ts_ns),
            ),
        )
    return ticks


# 标准 trade parquet 字段：instrument_id,price,size,aggressor_side,trade_id,ts_ns。
def load_trades(path: Path, instruments: dict[str, Any]) -> list[TradeTick]:
    columns = ["instrument_id", "price", "size", "aggressor_side", "trade_id", "ts_ns"]
    data = pd.read_parquet(path, columns=columns).sort_values(["ts_ns", "trade_id"], kind="mergesort")
    ticks = []
    for row in data.itertuples(index=False):
        instrument = instruments[str(row.instrument_id)]
        ticks.append(
            TradeTick(
                instrument_id=instrument.id,
                price=Price(Decimal(str(row.price)), instrument.price_precision),
                size=Quantity(positive(row.size, "size"), instrument.size_precision),
                aggressor_side=getattr(AggressorSide, str(row.aggressor_side).upper()),
                trade_id=TradeId(str(row.trade_id)),
                ts_event=int(row.ts_ns),
                ts_init=int(row.ts_ns),
            ),
        )
    return ticks


def load_bars(path: Path, instrument, bar_type: str):
    data = pd.read_parquet(path, columns=["timestamp", "open", "high", "low", "close", "volume"])
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    data = data.set_index("timestamp").sort_index()
    return BarDataWrangler(
        BarType.from_str(bar_type),
        instrument,
    ).process(data[["open", "high", "low", "close", "volume"]])


def add_catalog_trades(engine, path: Path, ids: list[str]) -> None:
    catalog = ParquetDataCatalog(str(path))
    for instrument_id in ids:
        engine.add_data(catalog.trade_ticks(instrument_ids=[instrument_id]))


# 按显式 dataset type 加载数据，不推断文件 schema 或市场来源。
def add_datasets(
    engine,
    settings: dict[str, Any],
    factories: dict[str, InstrumentFactory],
) -> None:
    instruments = instrument_map(factories)
    for dataset in settings["backtest"]["datasets"]:
        path = ROOT / dataset["path"]
        data_type = dataset["type"]
        if data_type == "quote_tick_objects":
            engine.add_data(load_quote_objects(path))
        elif data_type == "quote_ticks":
            engine.add_data(load_quotes(path, instruments))
        elif data_type == "trade_ticks":
            engine.add_data(load_trades(path, instruments))
        elif data_type == "bars":
            instrument = instruments[dataset["instrument_id"]]
            engine.add_data(load_bars(path, instrument, dataset["bar_type"]))
        elif data_type == "trade_tick_catalog":
            factory = factories[dataset["venue"]]
            ids = [str(factory.instrument_id(market)) for market in factory.markets]
            add_catalog_trades(engine, path, ids)
        else:
            raise ValueError(f"unsupported backtest dataset type: {data_type}")
