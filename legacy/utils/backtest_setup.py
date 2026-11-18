from __future__ import annotations

import copy
import itertools
import os
import pickle
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler

from utils.constants import LOCAL_TZ
from utils.constants import PROJECT_ROOT
from utils.runtime_setup import claim_run
from utils.runtime_setup import strategy_entries


def grid_cases(grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    names = list(grid)
    return [
        dict(zip(names, values))
        for values in itertools.product(*(grid[name] for name in names))
    ]


# base config 保持原样，只有 batch.cases 与 batch.grid 会展开。
def strategy_cases(entry: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    base = entry["config"]
    batch = entry.get("batch")
    if batch is None:
        return [(copy.deepcopy(base), {})]
    cases = batch.get("cases", [{}])
    grid = batch.get("grid", {})
    rows = []
    for case in cases:
        overlap = set(case) & set(grid)
        if overlap:
            raise ValueError(f"batch case and grid overlap: {', '.join(sorted(overlap))}")
        for values in grid_cases(grid):
            selected = {**case, **values}
            rows.append(({**base, **selected}, selected))
    return rows


def case_settings(
    settings: dict[str, Any],
    names: list[str],
    choices: tuple[tuple[dict[str, Any], dict[str, Any]], ...],
) -> dict[str, Any]:
    case = copy.deepcopy(settings)
    selected = {}
    for name, (config, params) in zip(names, choices):
        entry = case["strategy"][name]
        entry["config"] = config
        entry.pop("batch", None)
        selected.update({f"{name}.{key}": value for key, value in params.items()})
    case["runtime"] = {**case.get("runtime", {}), "backtest_params": selected}
    return case


def expand_settings(settings: dict[str, Any]) -> list[dict[str, Any]]:
    active = strategy_entries(settings)
    names = list(active)
    expanded = [strategy_cases(active[name]) for name in names]
    return [
        case_settings(settings, names, choices)
        for choices in itertools.product(*expanded)
    ]


def max_workers(settings: dict[str, Any]) -> int:
    workers = int(settings["backtest"]["max_workers"])
    if workers < 1:
        raise ValueError("backtest.max_workers must be >= 1")
    return workers


def worker_count(settings: dict[str, Any], case_count: int) -> int:
    return min(max_workers(settings), case_count)


def claim_cases(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    started_at = os.environ.get("NT_RUN_STARTED_AT") or datetime.now(
        LOCAL_TZ,
    ).strftime("%Y%m%d%H%M%S")
    claimed = []
    for index, case in enumerate(cases):
        item = claim_run(case, started_at)
        parent = f"backtest-{started_at}"
        item["runtime"]["report_dir_name"] = f"{parent}/backtest-{started_at}-{index:03d}"
        claimed.append(item)
    return claimed


def optional_money(value: Any, currency: Currency) -> Money | None:
    return Money(Decimal(str(value)), currency) if value is not None else None


# 从一个规范化 backtest venue 构造合成 instrument。
class InstrumentFactory:
    def __init__(self, venue: dict[str, Any]) -> None:
        self.markets = venue["markets"]
        self.cfg = venue["instrument"]
        self.kind = venue["instrument_kind"]
        self._instruments = [self.instrument(market) for market in self.markets]

    def raw_symbol(self, market: dict[str, Any]) -> str:
        return str(market["raw_symbol"])

    def instrument_id(self, market: dict[str, Any]) -> InstrumentId:
        return InstrumentId.from_str(market["instrument_id"])

    def currency_pair(self, market: dict[str, Any]) -> CurrencyPair:
        base = Currency.from_str(market["base_currency"])
        quote = Currency.from_str(market["quote_currency"])
        return CurrencyPair(
            instrument_id=self.instrument_id(market),
            raw_symbol=Symbol(self.raw_symbol(market)),
            base_currency=base,
            quote_currency=quote,
            price_precision=int(self.cfg["price_precision"]),
            size_precision=int(self.cfg["size_precision"]),
            price_increment=Price.from_str(str(self.cfg["price_increment"])),
            size_increment=Quantity.from_str(str(self.cfg["size_increment"])),
            lot_size=Quantity.from_str(str(self.cfg["lot_size"])),
            max_quantity=Quantity.from_str(str(self.cfg["max_quantity"])),
            min_quantity=Quantity.from_str(str(self.cfg["min_quantity"])),
            max_notional=optional_money(self.cfg["max_notional"], quote),
            min_notional=optional_money(self.cfg["min_notional"], quote),
            max_price=Price.from_str(str(self.cfg["max_price"])),
            min_price=Price.from_str(str(self.cfg["min_price"])),
            margin_init=Decimal(str(self.cfg["margin_init"])),
            margin_maint=Decimal(str(self.cfg["margin_maint"])),
            maker_fee=Decimal(str(self.cfg["maker_fee"])),
            taker_fee=Decimal(str(self.cfg["taker_fee"])),
            ts_event=0,
            ts_init=0,
        )

    def crypto_perpetual(self, market: dict[str, Any]) -> CryptoPerpetual:
        base = Currency.from_str(market["base_currency"])
        quote = Currency.from_str(market["quote_currency"])
        settlement = Currency.from_str(market["settlement_currency"])
        return CryptoPerpetual(
            instrument_id=self.instrument_id(market),
            raw_symbol=Symbol(self.raw_symbol(market)),
            base_currency=base,
            quote_currency=quote,
            settlement_currency=settlement,
            is_inverse=bool(self.cfg["is_inverse"]),
            price_precision=int(self.cfg["price_precision"]),
            size_precision=int(self.cfg["size_precision"]),
            price_increment=Price.from_str(str(self.cfg["price_increment"])),
            size_increment=Quantity.from_str(str(self.cfg["size_increment"])),
            multiplier=Quantity.from_str(str(self.cfg["multiplier"])),
            lot_size=Quantity.from_str(str(self.cfg["lot_size"])),
            max_quantity=Quantity.from_str(str(self.cfg["max_quantity"])),
            min_quantity=Quantity.from_str(str(self.cfg["min_quantity"])),
            max_notional=optional_money(self.cfg["max_notional"], quote),
            min_notional=optional_money(self.cfg["min_notional"], quote),
            max_price=Price.from_str(str(self.cfg["max_price"])),
            min_price=Price.from_str(str(self.cfg["min_price"])),
            margin_init=Decimal(str(self.cfg["margin_init"])),
            margin_maint=Decimal(str(self.cfg["margin_maint"])),
            maker_fee=Decimal(str(self.cfg["maker_fee"])),
            taker_fee=Decimal(str(self.cfg["taker_fee"])),
            ts_event=0,
            ts_init=0,
        )

    def instrument(self, market: dict[str, Any]) -> Instrument:
        if self.kind == "spot":
            return self.currency_pair(market)
        if self.kind == "perpetual":
            return self.crypto_perpetual(market)
        raise ValueError(f"unsupported backtest instrument_kind: {self.kind}")

    def instruments(self) -> list[Instrument]:
        return list(self._instruments)


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
        path = PROJECT_ROOT / dataset["path"]
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
