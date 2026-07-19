from __future__ import annotations

import time
from concurrent.futures import ProcessPoolExecutor
from decimal import Decimal
from decimal import ROUND_CEILING
from decimal import ROUND_FLOOR
from typing import Any

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.backtest.models import LatencyModel
from nautilus_trader.backtest.models import OneTickSlippageFillModel
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from adapters.common import cache_config
from utils.backtest_batch import claim_cases
from utils.backtest_batch import expand_settings
from utils.backtest_batch import max_workers
from utils.backtest_batch import worker_count
from utils.backtest_data import add_datasets
from utils.backtest_data import add_instruments
from utils.component_factory import create_strategies
from utils.config_loader import load_settings
from utils.instrument_factory import InstrumentFactory
from utils.report_writer import log_file_settings
from utils.report_writer import prepare_report_dir
from utils.report_writer import print_backtest_summary
from utils.report_writer import run_reports_dir
from utils.report_writer import write_backtest_result
from utils.report_writer import write_trader_reports
from utils.runtime_ids import claim_run


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


def latency_model(config: dict[str, Any]) -> LatencyModel:
    ns_per_ms = 1_000_000
    return LatencyModel(
        base_latency_nanos=int(Decimal(str(config["base_latency_ms"])) * ns_per_ms),
        insert_latency_nanos=int(Decimal(str(config["insert_latency_ms"])) * ns_per_ms),
        update_latency_nanos=int(Decimal(str(config["update_latency_ms"])) * ns_per_ms),
        cancel_latency_nanos=int(Decimal(str(config["cancel_latency_ms"])) * ns_per_ms),
    )


def fill_model(config: dict[str, Any]) -> FillModel:
    model_type = config["type"]
    if model_type == "default":
        return FillModel(
            prob_fill_on_limit=float(config["prob_fill_on_limit"]),
            prob_slippage=float(config["prob_slippage"]),
            random_seed=config["random_seed"],
        )
    if model_type == "one_tick_slippage":
        return OneTickSlippageFillModel()
    if model_type == "bps_slippage":
        return BpsSlippageFillModel(Decimal(str(config["slippage_bps"])))
    raise ValueError(f"unsupported backtest fill_model type: {model_type}")


def instrument_factories(settings: dict[str, Any]) -> dict[str, InstrumentFactory]:
    return {
        name: InstrumentFactory(venue)
        for name, venue in settings["backtest"]["venues"].items()
    }


def starting_balances(venue: dict[str, Any]) -> list[Money]:
    return [Money.from_str(str(value)) for value in venue["starting_balances"]]


# 只转发 YAML 显式声明的 NT venue 选项。
def add_venues(engine: BacktestEngine, settings: dict[str, Any]) -> None:
    optional = (
        "use_market_order_acks",
        "bar_execution",
        "trade_execution",
        "liquidity_consumption",
        "use_position_ids",
        "use_random_ids",
        "use_reduce_only",
        "use_message_queue",
    )
    for venue in settings["backtest"]["venues"].values():
        options = {key: venue[key] for key in optional if key in venue}
        if "default_leverage" in venue:
            options["default_leverage"] = Decimal(str(venue["default_leverage"]))
        if "latency" in venue:
            options["latency_model"] = latency_model(venue["latency"])
        if "fill_model" in venue:
            options["fill_model"] = fill_model(venue["fill_model"])
        engine.add_venue(
            venue=Venue(venue["venue"]),
            oms_type=getattr(OmsType, venue["oms_type"]),
            account_type=getattr(AccountType, venue["account_type"]),
            base_currency=None,
            starting_balances=starting_balances(venue),
            **options,
        )


def build_engine(settings: dict[str, Any]) -> BacktestEngine:
    engine = BacktestEngine(
        config=BacktestEngineConfig(
            cache=cache_config(settings),
            logging=LoggingConfig(
                log_level=settings["backtest"]["logging"]["log_level"],
                bypass_logging=settings["backtest"]["logging"]["bypass"],
                **log_file_settings(settings),
            ),
        ),
    )
    factories = instrument_factories(settings)
    add_venues(engine, settings)
    add_instruments(engine, factories)
    add_datasets(engine, settings, factories)
    for strategy in create_strategies(settings):
        engine.add_strategy(strategy)
    return engine


def write_reports(engine: BacktestEngine, result, settings: dict[str, Any]) -> dict[str, float]:
    payload = write_backtest_result(result)
    started = time.perf_counter()
    write_trader_reports(engine.trader, settings, "backtest")
    summary_elapsed = print_backtest_summary(payload, settings)
    return {
        "report_elapsed_sec": time.perf_counter() - started,
        "summary_elapsed_sec": summary_elapsed,
    }


def run_case(settings: dict[str, Any]) -> dict[str, Any]:
    engine = None
    total_started = time.perf_counter()
    try:
        prepare_report_dir(settings)
        build_started = time.perf_counter()
        engine = build_engine(settings)
        build_elapsed = time.perf_counter() - build_started
        run_started = time.perf_counter()
        engine.run()
        run_elapsed = time.perf_counter() - run_started
        report_times = write_reports(engine, engine.get_result(), settings)
        times = {
            "build_elapsed_sec": build_elapsed,
            "run_elapsed_sec": run_elapsed,
            "total_elapsed_sec": time.perf_counter() - total_started,
            **report_times,
            "report_dir": str(run_reports_dir(settings)),
        }
        print_timing(times)
        return times
    finally:
        if engine is not None:
            engine.dispose()


# Windows ProcessPool worker 必须是模块顶层函数。
def run_batch_case(settings: dict[str, Any]) -> dict[str, Any]:
    return run_case(settings)


def run_batch(cases: list[dict[str, Any]]) -> dict[str, Any]:
    claimed = claim_cases(cases)
    workers = worker_count(claimed[0], len(claimed))
    print(
        f"批处理回测：{len(claimed)} 组参数，最多 {max_workers(claimed[0])} 进程，实际 {workers} 进程",
        flush=True,
    )
    started = time.perf_counter()
    with ProcessPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(run_batch_case, claimed))
    return {"total_elapsed_sec": time.perf_counter() - started, "cases": results}


def main(config_name: str | None = None) -> dict[str, Any]:
    cases = expand_settings(load_settings(config_name, mode="backtest"))
    if len(cases) == 1:
        return run_case(claim_run(cases[0]))
    return run_batch(cases)


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
