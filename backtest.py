from __future__ import annotations

import sys

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money

from utils.binance_clients import BinanceConfigBuilder
from utils.config_loader import load_settings
from utils.market_data import MarketDataStore
from utils.report_writer import print_backtest_summary
from utils.report_writer import write_backtest_result
from utils.report_writer import write_trader_reports
from utils.strategy_factory import build_strategy


# 为当前 set 创建一个新的 NT 回测引擎。
def build_backtest_engine(settings: dict) -> BacktestEngine:
    binance = BinanceConfigBuilder(settings)
    store = MarketDataStore(settings)

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            cache=binance.cache_config(),
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
    for instrument in store.factory.instruments():
        engine.add_instrument(instrument)
    for market in store.markets:
        engine.add_data(store.load_bars(market))
    engine.add_strategy(build_strategy(settings))
    return engine


# 把 NT 生成的报告保存到当前 set 对应的目录。
def write_reports(engine: BacktestEngine, result, settings: dict) -> None:
    payload = write_backtest_result(result, settings)
    write_trader_reports(engine.trader, settings, "backtest")
    print_backtest_summary(payload)


# 命令行参数优先；没有命令行参数时才用 main(...) 传入的 set。
def main(config_name: str | None = None) -> None:
    selected = (sys.argv[1] if len(sys.argv) > 1 else None) or config_name
    settings = load_settings(selected)
    engine = build_backtest_engine(settings)
    engine.run()
    write_reports(engine, engine.get_result(), settings)
    engine.dispose()


if __name__ == "__main__":
    main()
