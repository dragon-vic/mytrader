from __future__ import annotations

import json
import sys

from nautilus_trader.adapters.binance.factories import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.factories import BinanceLiveExecClientFactory
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode

from external.data_engine import EXTERNAL_SIGNAL_CLIENT_NAME
from external.data_engine import ExternalSignalDataClientConfig
from external.data_engine import ExternalSignalLiveDataClientFactory
from utils.binance_clients import BINANCE_CLIENT_NAME
from utils.binance_clients import binance_data_config
from utils.binance_clients import binance_exec_config
from utils.binance_clients import cache_config
from utils.config_loader import load_settings
from utils.config_loader import proxy_url
from utils.instrument_factory import make_instruments
from utils.report_writer import write_trader_reports
from utils.strategy_factory import build_strategy

# 为当前 set 构建 Binance live/testnet node。
def build_live_node(settings: dict) -> TradingNode:
    exec_config = binance_exec_config(settings)

    trade_config = TradingNodeConfig(
        trader_id="TRADER-001",
        cache=cache_config(settings),
        logging=LoggingConfig(log_level="INFO", log_colors=True),
        data_clients={
            BINANCE_CLIENT_NAME: binance_data_config(settings),
            EXTERNAL_SIGNAL_CLIENT_NAME: ExternalSignalDataClientConfig(
                host=settings["external_signal"]["host"],
                port=int(settings["external_signal"]["port"]),
            ),
        },
        exec_clients={BINANCE_CLIENT_NAME: exec_config},
    )

    node = TradingNode(config=trade_config)

    node.add_data_client_factory(BINANCE_CLIENT_NAME, BinanceLiveDataClientFactory)
    node.add_data_client_factory(EXTERNAL_SIGNAL_CLIENT_NAME, ExternalSignalLiveDataClientFactory)
    node.add_exec_client_factory(BINANCE_CLIENT_NAME, BinanceLiveExecClientFactory)
    node.trader.add_strategy(build_strategy(settings))
    node.build()
    return node


# 使用 NT node 自己的事件循环，避免和外部 asyncio loop 冲突。
def run_for_seconds(node: TradingNode, seconds: int) -> None:
    loop = node.get_event_loop()
    loop.call_later(seconds, lambda: loop.create_task(node.stop_async()))
    node.run()


# 命令行参数优先；没有命令行参数时才用 main(...) 传入的 set。
def main(config_name: str | None = None) -> None:
    selected = (sys.argv[1] if len(sys.argv) > 1 else None) or config_name
    settings = load_settings(selected)
    node = build_live_node(settings)
    print(
        json.dumps(
            {
                "exchange": "binance",
                "environment": settings["live"]["environment"],
                "account_type": settings["live"]["account_type"],
                "instruments": [str(instrument.id) for instrument in make_instruments(settings)],
                "proxy_url": proxy_url(settings),
            },
            indent=2,
        ),
    )

    run_for_seconds(node, int(settings["live"]["run_seconds"]))
    write_trader_reports(node.trader, settings, "live")
    node.dispose()


if __name__ == "__main__":
    # external_stg_1
    main("external_stg_1")
