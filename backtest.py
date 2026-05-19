from __future__ import annotations

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.catalog import ParquetDataCatalog
import pandas as pd

from adapters.common import cache_config
from utils.config_loader import ROOT
from utils.config_loader import load_settings
from utils.config_loader import normalize_market
from utils.market_data import MarketDataStore
from utils.report_writer import prepare_report_dir
from utils.report_writer import print_backtest_summary
from utils.report_writer import write_backtest_result
from utils.report_writer import write_trader_reports
from utils.runtime_ids import claim_run
from utils.runtime_ids import release_run
from utils.strategy_factory import build_strategy


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
            cache=cache_config(settings),
            logging=LoggingConfig(
                log_level=settings["backtest"]["log_level"],
                bypass_logging=bool(settings["backtest"].get("log_bypass", False)),
            ),
        ),
    )
    engine.add_venue(
        venue=Venue(store.markets[0]["venue"]),
        oms_type=getattr(OmsType, settings["backtest"]["oms_type"]),
        account_type=getattr(AccountType, settings["backtest"]["account_type"]),
        base_currency=None,
        starting_balances=[Money.from_str(settings["backtest"]["starting_balance"])],
    )
    if "tick_data_path" in settings["backtest"]:
        for instrument in store.factory.instruments():
            engine.add_instrument(instrument)
        engine.add_data(store.load_trade_ticks(ROOT / settings["backtest"]["tick_data_path"]))
    elif "tick_catalog" in settings["backtest"]:
        catalog_path = ROOT / settings["backtest"]["tick_catalog"]
        catalog = ParquetDataCatalog(str(catalog_path))
        ids = [str(store.factory.instrument_id(market)) for market in store.markets]
        for instrument in catalog.instruments(instrument_ids=ids):
            engine.add_instrument(instrument)
        for instrument_id in ids:
            engine.add_data(catalog.trade_ticks(instrument_ids=[instrument_id]))
    else:
        for instrument in store.factory.instruments():
            engine.add_instrument(instrument)
        for market in store.markets:
            engine.add_data(store.load_bars(market))
    engine.add_strategy(build_strategy(settings, "backtest"))
    return engine


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
        release_run(settings)
