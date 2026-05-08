from __future__ import annotations

import sys
from threading import Thread

from nautilus_trader.adapters.binance.factories import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.factories import BinanceLiveExecClientFactory
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode

from actors.data_recorder import DataRecorder
from actors.data_recorder import DataRecorderConfig
from external.data_engine import EXTERNAL_SIGNAL_CLIENT_NAME
from external.data_engine import ExternalSignalDataClientConfig
from external.data_engine import ExternalSignalLiveDataClientFactory
from utils.binance_clients import BINANCE_CLIENT_NAME
from utils.binance_clients import BinanceConfigBuilder
from utils.config_loader import load_settings
from utils.report_writer import TraderReportWriter
from utils.report_writer import run_reports_dir
from utils.strategy_factory import build_strategy


NODE_STOP_TOPIC = "controls.node.stop"


# 在 node 自己的事件循环里请求停止。
def stop_node(node: TradingNode, reason: str) -> None:
    node.get_logger().info(f"NODE_STOP_REQUEST reason={reason}", color=LogColor.RED)
    node.get_event_loop().create_task(node.stop_async())


# 策略发布停止请求后，由 live 入口负责停止整个 TradingNode。
def attach_node_stop_handler(node: TradingNode) -> None:
    def handle_node_stop(_message: dict) -> None:
        loop = node.get_event_loop()
        loop.call_soon_threadsafe(lambda: stop_node(node, "外部信号"))

    node.trader.subscribe(NODE_STOP_TOPIC, handle_node_stop)


# 为当前 set 构建 Binance live/testnet node，并挂上 live 成交落盘。
def build_live_node(settings: dict) -> tuple[TradingNode, TraderReportWriter]:
    binance = BinanceConfigBuilder(settings)
    output_dir = run_reports_dir(settings, "live")
    output_dir.mkdir(parents=True, exist_ok=True)
    trade_config = TradingNodeConfig(
        trader_id="TRADER-001",
        cache=binance.cache_config(),
        logging=LoggingConfig(
            log_level="INFO",
            log_level_file=settings["live"].get("log_level_file", "INFO"),
            log_directory=str(output_dir),
            log_file_name="live_raw.log",
            log_colors=True,
            clear_log_file=True,
        ),
        data_clients={
            BINANCE_CLIENT_NAME: binance.data_config(),
            EXTERNAL_SIGNAL_CLIENT_NAME: ExternalSignalDataClientConfig(
                host=settings["external_signal"]["host"],
                port=int(settings["external_signal"]["port"]),
            ),
        },
        exec_clients={BINANCE_CLIENT_NAME: binance.exec_config()},
    )

    node = TradingNode(config=trade_config)
    node.add_data_client_factory(BINANCE_CLIENT_NAME, BinanceLiveDataClientFactory)
    node.add_data_client_factory(EXTERNAL_SIGNAL_CLIENT_NAME, ExternalSignalLiveDataClientFactory)
    node.add_exec_client_factory(BINANCE_CLIENT_NAME, BinanceLiveExecClientFactory)

    strategy = build_strategy(settings, "live")
    node.trader.add_strategy(strategy)

    report_writer = TraderReportWriter.from_settings(settings, "live")
    node.trader.add_actor(DataRecorder(DataRecorderConfig(output_dir=str(output_dir))))
    attach_node_stop_handler(node)

    node.build()
    return node, report_writer


# 使用 NT node 自己的事件循环，避免和外部 asyncio loop 冲突。
def run_for_seconds(node: TradingNode, seconds: int) -> None:
    loop = node.get_event_loop()

    def wait_enter() -> None:
        input()
        loop.call_soon_threadsafe(lambda: stop_node(node, "回车"))

    Thread(target=wait_enter, daemon=True).start()
    loop.call_later(seconds, lambda: stop_node(node, "到时间"))
    node.run()


# 命令行参数优先；没有命令行参数时才用 main(...) 传入的 set。
def main(config_name: str | None = None) -> None:
    selected = (sys.argv[1] if len(sys.argv) > 1 else None) or config_name
    settings = load_settings(selected)
    node, report_writer = build_live_node(settings)

    run_for_seconds(node, int(settings["live"]["run_seconds"]))
    report_writer.write_final_reports(node.trader, names=("orders", "positions"))
    report_writer.write_clean_live_log()
    node.dispose()


if __name__ == "__main__":
    # orca_short\funding\ 别删这里
    main("orca_short_1")
