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
from utils.arguments import BINANCE_CLIENT_NAME
from utils.arguments import DEFAULT_TRADER_ID
from utils.arguments import NODE_STOP_TOPIC
from external.data_engine import EXTERNAL_SIGNAL_CLIENT_NAME
from external.data_engine import ExternalSignalDataClientConfig
from external.data_engine import ExternalSignalLiveDataClientFactory
from utils.binance_clients import BinanceConfigBuilder
from utils.config_loader import load_settings
from utils.report_writer import live_logs_dir
from utils.report_writer import live_raw_log_name
from utils.report_writer import prepare_report_dir
from utils.report_writer import TraderReportWriter
from utils.runtime_ids import claim_run
from utils.runtime_ids import release_run
from utils.strategy_factory import build_strategy


# 根据 run.py 传入的模式生成一份 live 配置，不修改原始 settings。
def resolve_live_mode(settings: dict, mode: str | None) -> dict:
    if mode is None:
        raise RuntimeError("live.main 必须由 run.py 传入 mode")
    live = settings["live"]
    modes = live["modes"]
    if mode not in modes:
        raise ValueError(f"Unsupported live mode: {mode}")

    base_live = {key: value for key, value in live.items() if key != "modes"}
    resolved = dict(settings)
    resolved["live"] = {**base_live, **modes[mode]}
    resolved["mode"] = mode
    return resolved


# 在 node 自己的事件循环里请求停止。
def stop_node(node: TradingNode, reason: str) -> None:
    node.get_logger().info(f"NODE_STOP_REQUEST reason={reason}", color=LogColor.YELLOW)
    loop = node.get_event_loop()
    loop.create_task(node.stop_async())


# 策略发布停止请求后，由 live 入口负责停止整个 TradingNode。
def attach_node_stop_handler(node: TradingNode) -> None:
    def handle_node_stop(_message: dict) -> None:
        loop = node.get_event_loop()
        loop.call_soon_threadsafe(lambda: stop_node(node, "外部信号"))

    node.trader.subscribe(NODE_STOP_TOPIC, handle_node_stop)


# 为当前 set 构建 Binance live/testnet node，并挂上 live 成交落盘。
def build_live_node(settings: dict) -> tuple[TradingNode, TraderReportWriter]:
    binance = BinanceConfigBuilder(settings)
    output_dir = prepare_report_dir(settings, "live")
    log_dir = live_logs_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    trade_config = TradingNodeConfig(
        trader_id=settings.get("runtime", {}).get("trader_id", DEFAULT_TRADER_ID),
        cache=binance.cache_config(),
        logging=LoggingConfig(
            log_level="INFO",
            log_level_file=settings["live"].get("log_level_file", "INFO"),
            log_directory=str(log_dir),
            log_file_name=live_raw_log_name(settings),
            log_colors=True,
            log_component_levels={
                "Portfolio": "WARNING",
                "ExecClient-BINANCE": "WARNING",
                settings["strategy"]["class"]: "INFO",
                "DataRecorder": "INFO",
            },
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


# 挂上回车停止监听，然后按 NT 标准方式运行 node。
def run_live_node(node: TradingNode) -> None:
    loop = node.get_event_loop()

    def wait_enter() -> None:
        input()
        loop.call_soon_threadsafe(lambda: stop_node(node, "回车"))

    if sys.stdin.isatty():
        Thread(target=wait_enter, daemon=True).start()
    node.run()


# 运行 live/testnet，由 run.py 负责传入配置名和模式。
def main(config_name: str, mode: str | None = None) -> None:
    settings = resolve_live_mode(load_settings(config_name), mode)
    settings = claim_run(settings)
    node = None

    try:
        node, report_writer = build_live_node(settings)
        run_live_node(node)
        report_writer.write_final_reports(node.trader)
        report_writer.write_clean_live_log(settings)
    finally:
        if node is not None:
            node.dispose()
        release_run(settings)
