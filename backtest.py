from __future__ import annotations

import copy
import itertools
import os
import pickle
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_CEILING
from decimal import ROUND_FLOOR
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.backtest.models import LatencyModel
from nautilus_trader.backtest.models import OneTickSlippageFillModel
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
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
from utils.report_writer import run_reports_dir
from utils.report_writer import write_backtest_result
from utils.report_writer import write_trader_reports
from utils.runtime_ids import claim_run
from utils.strategy_factory import build_strategy
from utils.strategy_factory import single_strategy


class BpsSlippageFillModel(FillModel):
    def __init__(self, slippage_bps: Decimal) -> None:
        super().__init__(prob_fill_on_limit=1.0, prob_slippage=0.0)
        self.slippage_bps = slippage_bps

    # 用模拟盘口强制成交价包含固定 bps 滑点。
    def get_orderbook_for_fill_simulation(
        self,
        instrument,
        order,
        best_bid: Price,
        best_ask: Price,
    ) -> OrderBook:
        unlimited = 1_000_000
        bid_price = bps_price(
            best_bid,
            instrument.price_increment.as_decimal(),
            instrument.price_precision,
            Decimal("-1"),
            self.slippage_bps,
        )
        ask_price = bps_price(
            best_ask,
            instrument.price_increment.as_decimal(),
            instrument.price_precision,
            Decimal("1"),
            self.slippage_bps,
        )

        book = OrderBook(instrument_id=instrument.id, book_type=BookType.L2_MBP)
        book.add(
            BookOrder(
                side=OrderSide.BUY,
                price=bid_price,
                size=Quantity(unlimited, instrument.size_precision),
                order_id=1,
            ),
            0,
            0,
        )
        book.add(
            BookOrder(
                side=OrderSide.SELL,
                price=ask_price,
                size=Quantity(unlimited, instrument.size_precision),
                order_id=2,
            ),
            0,
            0,
        )
        return book


# 为当前 set 创建一个新的 NT 回测引擎。
def build_backtest_engine(settings: dict[str, Any]) -> BacktestEngine:
    engine = BacktestEngine(
        config=BacktestEngineConfig(
            cache=cache_config(settings),
            logging=LoggingConfig(
                log_level=settings["backtest"]["logging"]["log_level"],
                bypass_logging=bool(settings["backtest"]["logging"]["bypass"]),
                **log_file_settings(settings),
            ),
        ),
    )
    factories = backtest_factories(settings)
    add_venues(engine, settings)
    add_instruments(engine, factories)
    add_datasets(engine, settings, factories)
    name = next(iter(settings["strategy"]))
    engine.add_strategy(build_strategy(settings, name, "backtest"))
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
        if "fill_model" in venue:
            kwargs["fill_model"] = fill_model(venue["fill_model"])
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


# YAML 配置回测成交模型；默认模型的 prob_slippage 是一跳滑点概率，不是 bps。
def fill_model(cfg: dict[str, Any]) -> FillModel:
    model_type = str(cfg["type"])
    if model_type == "default":
        random_seed = cfg.get("random_seed")
        return FillModel(
            prob_fill_on_limit=float(str(cfg["prob_fill_on_limit"])),
            prob_slippage=float(str(cfg["prob_slippage"])),
            random_seed=int(random_seed) if random_seed is not None else None,
        )
    if model_type == "one_tick_slippage":
        return OneTickSlippageFillModel()
    if model_type == "bps_slippage":
        return BpsSlippageFillModel(Decimal(str(cfg["slippage_bps"])))
    raise ValueError(f"Unsupported backtest fill_model type: {model_type}")


def bps_price(
    price: Price,
    tick: Decimal,
    precision: int,
    side: Decimal,
    slippage_bps: Decimal,
) -> Price:
    raw = price.as_decimal() * (Decimal("1") + side * slippage_bps / Decimal("10000"))
    rounding = ROUND_CEILING if side > 0 else ROUND_FLOOR
    aligned = (raw / tick).to_integral_value(rounding=rounding) * tick
    return Price.from_str(f"{aligned:.{precision}f}")


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
        elif data_type == "quote_tick_objects":
            engine.add_data(load_quote_tick_objects(ROOT / dataset["path"]))
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


def load_quote_tick_objects(path: Path) -> list[QuoteTick]:
    with path.open("rb") as f:
        return pickle.load(f)


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
    store_settings = settings or {"project": {"data_dir": "data"}}
    if dataset and "data_dir" in dataset:
        store_settings = {**store_settings, "project": {"data_dir": dataset["data_dir"]}}
    return MarketDataStore(store_settings, factory)


# 把 NT 生成的报告保存到当前 set 对应的目录。
def write_reports(engine: BacktestEngine, result, settings: dict) -> dict[str, float]:
    payload = write_backtest_result(result)
    started = time.perf_counter()
    write_trader_reports(engine.trader, settings, "backtest")
    summary_elapsed = print_backtest_summary(payload, settings)
    report_elapsed = time.perf_counter() - started
    return {"report_elapsed_sec": report_elapsed, "summary_elapsed_sec": summary_elapsed}


# 运行一组已经展开参数的回测配置。
def run_case(settings: dict[str, Any]) -> dict[str, Any]:
    engine = None
    total_started = time.perf_counter()
    try:
        prepare_report_dir(settings)
        build_started = time.perf_counter()
        engine = build_backtest_engine(settings)
        build_elapsed = time.perf_counter() - build_started
        run_started = time.perf_counter()
        engine.run()
        run_elapsed = time.perf_counter() - run_started
        report_times = write_reports(engine, engine.get_result(), settings)
        total_elapsed = time.perf_counter() - total_started
        times = {
            "build_elapsed_sec": build_elapsed,
            "run_elapsed_sec": run_elapsed,
            "total_elapsed_sec": total_elapsed,
            **report_times,
            "report_dir": str(run_reports_dir(settings)),
        }
        print_timing(times)
        return times
    finally:
        if engine is not None:
            engine.dispose()


# 批处理 worker 必须是模块顶层函数，Windows spawn 才能序列化。
def run_batch_case(settings: dict[str, Any]) -> dict[str, Any]:
    return run_case(settings)


# 运行回测，由 run.py 负责传入配置名。
def main(config_name: str) -> dict[str, Any]:
    settings = load_settings(config_name, mode="backtest")
    settings["mode"] = "backtest"
    cases = expand_batch_settings(settings)
    if len(cases) == 1:
        return run_case(claim_run(cases[0]))
    return run_batch(cases)


# 回测配置只接受 grid_params/case_params，并在运行前展开成普通 strategy.params。
def expand_batch_settings(settings: dict[str, Any]) -> list[dict[str, Any]]:
    strategy = single_strategy(settings)
    grid = strategy.get("grid_params") or {}
    case_params = strategy["case_params"]
    rows = case_param_rows(case_params)
    combos = grid_param_combos(grid)
    cases = []
    for row in rows:
        for grid_values in combos:
            cases.append(case_settings(settings, row, grid_values))
    return cases


def case_settings(settings: dict[str, Any], base_params: dict[str, Any], grid_values: dict[str, Any]) -> dict[str, Any]:
    case = copy.deepcopy(settings)
    strategy = single_strategy(case)
    case_params = strategy["case_params"]
    params = dict(base_params)
    params.update(grid_values)
    strategy["params"] = params
    case["runtime"] = {**case.get("runtime", {}), "backtest_params": selected_param_rows(grid_values, case_params, params)}
    strategy.pop("grid_params", None)
    strategy.pop("case_params", None)
    return case


def selected_param_rows(grid_values: dict[str, Any], case_params: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    keys = set(grid_values)
    keys.update(name for name, value in case_params.items() if isinstance(value, list))
    return {name: params[name] for name in case_params if name in keys} | {name: params[name] for name in grid_values}


def case_param_count(case_params: dict[str, Any]) -> int:
    lengths = [len(value) for value in case_params.values() if isinstance(value, list) and len(value) > 1]
    if not lengths:
        return 1
    total = lengths[0]
    if any(length != total for length in lengths):
        raise ValueError(f"strategy.case_params list lengths must be 1 or the same n: {lengths}")
    return total


# case_params 先展开成 n 行；单值和长度 1 列表都会广播到 n 行。
def case_param_rows(case_params: dict[str, Any]) -> list[dict[str, Any]]:
    total = case_param_count(case_params)
    return [case_param_row(case_params, total, index) for index in range(total)]


def case_param_row(case_params: dict[str, Any], total: int, index: int) -> dict[str, Any]:
    row = {}
    for name, value in case_params.items():
        if isinstance(value, list):
            if len(value) == 1:
                row[name] = value[0]
            elif len(value) == total:
                row[name] = value[index]
            else:
                raise ValueError(
                    f"strategy.case_params.{name} length must be 1 or match case_params n ({total}), got {len(value)}",
                )
        else:
            row[name] = value
    return row


def grid_param_combos(grid: dict[str, Any]) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    names = list(grid)
    values = []
    for name in names:
        raw = grid[name]
        if not isinstance(raw, list):
            raise TypeError(f"strategy.grid_params.{name} must be a list")
        if not raw:
            raise ValueError(f"strategy.grid_params.{name} must not be empty")
        values.append(raw)
    return [dict(zip(names, combo)) for combo in itertools.product(*values)]


# 多进程运行批处理：同一批次放在一个父目录下，每个组合一个子 report 目录。
def run_batch(cases: list[dict[str, Any]]) -> dict[str, Any]:
    started_at = os.environ.get("NT_RUN_STARTED_AT") or datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")
    claimed = [claim_batch_case(case, started_at, index) for index, case in enumerate(cases)]
    workers = batch_workers(claimed[0], len(claimed))
    print(f"批处理回测：{len(claimed)} 组参数，最多 {settings_max_workers(claimed[0])} 进程，实际 {workers} 进程", flush=True)
    started = time.perf_counter()
    results = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for result in executor.map(run_batch_case, claimed):
            results.append(result)
    total_elapsed = time.perf_counter() - started
    return {"total_elapsed_sec": total_elapsed, "cases": results}


# 从 YAML 读取批量回测最多进程数。
def settings_max_workers(settings: dict[str, Any]) -> int:
    workers = int(settings["backtest"]["max_workers"])
    if workers < 1:
        raise ValueError("backtest.max_workers must be >= 1")
    return workers


def batch_workers(settings: dict[str, Any], case_count: int) -> int:
    return min(settings_max_workers(settings), case_count)


def claim_batch_case(settings: dict[str, Any], started_at: str, index: int) -> dict[str, Any]:
    os.environ["NT_RUN_STARTED_AT"] = started_at
    claimed = claim_run(settings)
    parent = f"backtest-{started_at}"
    claimed["runtime"]["report_dir_name"] = f"{parent}/backtest-{started_at}-{index:03d}"
    return claimed


def print_timing(times: dict[str, Any]) -> None:
    print(
        "耗时："
        f"构建 {format_duration(times['build_elapsed_sec'])}，"
        f"运行 {format_duration(times['run_elapsed_sec'])}，"
        f"报告 {format_duration(times['report_elapsed_sec'])}，"
        f"summary {format_duration(times['summary_elapsed_sec'])}，"
        f"总计 {format_duration(times['total_elapsed_sec'])}",
    )


def format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, rest = divmod(seconds, 60)
    return f"{int(minutes)}m{rest:.1f}s"
