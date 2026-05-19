from __future__ import annotations

import sys
from threading import Thread

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode

from adapters.registry import build_client_bundle
from actors.data_recorder import DataRecorder
from actors.data_recorder import DataRecorderConfig
from utils.arguments import NODE_STOP_TOPIC
from external.data_engine import EXTERNAL_SIGNAL_CLIENT_NAME
from external.data_engine import ExternalSignalDataClientConfig
from external.data_engine import ExternalSignalLiveDataClientFactory
from utils.config_loader import load_settings
from utils.report_writer import live_logs_dir
from utils.report_writer import live_raw_log_name
from utils.report_writer import prepare_report_dir
from utils.report_writer import print_live_summary
from utils.report_writer import TraderReportWriter
from utils.runtime_ids import claim_run
from utils.runtime_ids import release_run
from utils.strategy_factory import build_strategy


DATA_RECORDER_MODULE = "data_recorder"
EXTERNAL_SIGNAL_MODULE = "external_signal"
LIVE_MODULES = {DATA_RECORDER_MODULE, EXTERNAL_SIGNAL_MODULE}


# live.modules 显式声明需要挂进 node 的可选组件。
def enabled_modules(settings: dict) -> set[str]:
    modules = settings["live"]["modules"]
    if not isinstance(modules, list):
        raise TypeError("live.modules must be a list")
    unknown = set(modules) - LIVE_MODULES
    if unknown:
        raise ValueError(f"Unsupported live.modules: {sorted(unknown)}")
    return set(modules)


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


# 为当前 set 构建 live/testnet node。
def build_live_node(settings: dict) -> TradingNode:
    bundle = build_client_bundle(settings)
    prepare_report_dir(settings, "live")
    log_dir = live_logs_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    modules = enabled_modules(settings)
    data_clients = {bundle.name: bundle.data_config}
    if EXTERNAL_SIGNAL_MODULE in modules:
        data_clients[EXTERNAL_SIGNAL_CLIENT_NAME] = ExternalSignalDataClientConfig(
            host=settings["external_signal"]["host"],
            port=int(settings["external_signal"]["port"]),
        )
    trade_config = TradingNodeConfig(
        trader_id=settings["runtime"]["trader_id"],
        cache=bundle.cache,
        logging=LoggingConfig(
            log_level=settings["live"]["log_level"],
            log_level_file=settings["live"]["log_level_file"],
            log_directory=str(log_dir),
            log_file_name=live_raw_log_name(settings),
            log_colors=bool(settings["live"]["log_colors"]),
            log_component_levels={
                **settings["live"]["log_component_levels"],
                settings["strategy"]["class"]: settings["live"]["strategy_log_level"],
            },
            clear_log_file=bool(settings["live"]["clear_log_file"]),
        ),
        data_clients=data_clients,
        exec_clients={bundle.name: bundle.exec_config},
    )

    node = TradingNode(config=trade_config)
    node.add_data_client_factory(bundle.name, bundle.data_factory)
    if EXTERNAL_SIGNAL_MODULE in modules:
        node.add_data_client_factory(EXTERNAL_SIGNAL_CLIENT_NAME, ExternalSignalLiveDataClientFactory)
    node.add_exec_client_factory(bundle.name, bundle.exec_factory)

    strategy = build_strategy(settings, "live")
    node.trader.add_strategy(strategy)

    if DATA_RECORDER_MODULE in modules:
        node.trader.add_actor(DataRecorder(DataRecorderConfig()))
    attach_node_stop_handler(node)

    node.build()
    return node


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
    settings = load_settings(config_name, mode=mode)
    settings["mode"] = mode
    settings = claim_run(settings)
    node = None
    report_writer = TraderReportWriter.from_settings(settings, "live")

    try:
        node = build_live_node(settings)
        run_live_node(node)
        report_writer.write_final_reports(node.trader)
    finally:
        if node is not None:
            node.dispose()
        report_writer.write_clean_live_log(settings)
        print_live_summary(settings)
        release_run(settings)
