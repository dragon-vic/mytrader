from __future__ import annotations

import pandas as pd
from nautilus_trader.core.nautilus_pyo3 import AccountType
from nautilus_trader.core.nautilus_pyo3 import BacktestEngine
from nautilus_trader.core.nautilus_pyo3 import BacktestEngineConfig
from nautilus_trader.core.nautilus_pyo3 import CacheConfig
from nautilus_trader.core.nautilus_pyo3 import Money
from nautilus_trader.core.nautilus_pyo3 import OmsType
from nautilus_trader.core.nautilus_pyo3 import TraderId
from nautilus_trader.core.nautilus_pyo3 import Venue

from utils.config_loader import ROOT
from utils.config_loader import load_settings
from utils.config_loader import normalize_market
from utils.market_data import MarketDataStore
from utils.report_writer import prepare_report_dir
from utils.report_writer import print_backtest_summary
from utils.report_writer import run_reports_dir
from utils.report_writer import write_backtest_result
from utils.runtime_ids import claim_run
from utils.runtime_ids import release_run
from utils.strategy_factory import build_importable_strategy


# tick parquet 回测直接从数据推导市场，避免维护巨大的 markets 列表。
def prepare_tick_backtest(settings: dict) -> None:
    tick_path = settings["backtest"].get("tick_data_path")
    if not tick_path:
        return
    df = pd.read_parquet(ROOT / tick_path, columns=["symbol"])
    symbols = sorted({
        str(symbol)
        for symbol in df["symbol"].dropna().unique()
        if str(symbol).isascii() and str(symbol).endswith("USDT")
    })
    settings["markets"] = [
        normalize_market(
            {
                **settings["market_defaults"],
                "symbol": f"{symbol[:-4]}/USDT",
            },
            settings,
        )
        for symbol in symbols
    ]
    settings["markets_all"] = False


# 为当前 set 创建一个新的 NT 回测引擎。
def build_backtest_engine(settings: dict) -> BacktestEngine:
    prepare_tick_backtest(settings)
    store = MarketDataStore(settings)

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId(settings["runtime"]["trader_id"]),
            cache=backtest_cache_config(settings),
            bypass_logging=bool(settings["backtest"].get("log_bypass", False)),
        ),
    )
    engine.add_venue(
        Venue(store.markets[0]["venue"]),
        getattr(OmsType, settings["backtest"]["oms_type"]),
        getattr(AccountType, settings["backtest"]["account_type"]),
        [Money.from_str(settings["backtest"]["starting_balance"])],
    )
    if "tick_data_path" in settings["backtest"]:
        for instrument in store.factory.instruments():
            engine.add_instrument(instrument)
        engine.add_data(store.load_trade_ticks(ROOT / settings["backtest"]["tick_data_path"]))
    else:
        for instrument in store.factory.instruments():
            engine.add_instrument(instrument)
        for market in store.markets:
            engine.add_data(store.load_bars(market))
    engine.add_strategy_from_config(build_importable_strategy(settings, "backtest"))
    return engine


# 构建 PyO3 回测 cache 配置。
def backtest_cache_config(settings: dict) -> CacheConfig:
    capacity = int(settings["runtime"]["cache_capacity"])
    return CacheConfig(
        tick_capacity=capacity,
        bar_capacity=capacity,
        drop_instruments_on_reset=False,
    )


# 把 NT 生成的报告保存到当前 set 对应的目录。
def write_reports(engine: BacktestEngine, result, settings: dict) -> None:
    payload = write_backtest_result(result, engine)
    print("生成报告...")
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
        release_run(settings)
