from __future__ import annotations

import sys
from threading import Thread

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.config import LiveExecEngineConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId

from adapters.binance import build_data_client as build_binance_data_client
from adapters.registry import build_client_bundle
from actors.data_recorder import DataRecorder
from actors.data_recorder import DataRecorderConfig
from utils.arguments import BINANCE_CLIENT_NAME
from utils.arguments import NODE_STOP_TOPIC
from external.data_engine import EXTERNAL_SIGNAL_CLIENT_NAME
from external.data_engine import ExternalSignalDataClientConfig
from external.data_engine import ExternalSignalLiveDataClientFactory
from utils.config_loader import load_settings
from utils.config_loader import markets_all
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory
from utils.polymarket_btc5m import up_instrument_windows
from utils.report_writer import live_logs_dir
from utils.report_writer import live_raw_log_name
from utils.report_writer import prepare_report_dir
from utils.report_writer import print_live_summary
from utils.report_writer import TraderReportWriter
from utils.report_writer import write_clean_live_log
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


# 当前 live 是否输出交易报告。
def reports_enabled(settings: dict) -> bool:
    return bool(settings["live"].get("reports", True))


# Polymarket 采集时可选挂 Binance spot 行情，不注册 Binance 执行客户端。
def binance_spot_data(settings: dict):
    config = settings["live"].get("binance_spot_data")
    if not config or not bool(config["enabled"]):
        return None
    if settings["exchange"]["name"] != "polymarket":
        raise ValueError("live.binance_spot_data is only supported with exchange.name=polymarket")
    return build_binance_data_client(settings, config)


# 为 exec reconciliation 限定当前策略关心的 instrument。
def reconciliation_instrument_ids(settings: dict):
    if settings["live"].get("reconciliation_scope") != "configured_markets":
        return None
    if settings["exchange"]["name"] == "polymarket":
        windows = up_instrument_windows(proxy_url(settings))
        instrument_ids = [InstrumentId.from_str(instrument_id) for instrument_id in windows]
        settings["runtime"]["instrument_ids"] = instrument_ids
        settings["runtime"]["event_windows"] = windows
        return instrument_ids
    if markets_all(settings):
        return None
    factory = InstrumentFactory(settings)
    return [factory.instrument_id(market) for market in factory.markets]


# 构建 live exec engine 配置。
def exec_engine_config(settings: dict) -> LiveExecEngineConfig:
    return LiveExecEngineConfig(
        reconciliation_instrument_ids=reconciliation_instrument_ids(settings),
    )


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
    if reports_enabled(settings):
        prepare_report_dir(settings, "live")
    log_dir = live_logs_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    modules = enabled_modules(settings)
    data_clients = {bundle.name: bundle.data_config}
    binance_spot = binance_spot_data(settings)
    if binance_spot is not None:
        data_clients[BINANCE_CLIENT_NAME] = binance_spot[0]
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
        exec_engine=exec_engine_config(settings),
    )

    node = TradingNode(config=trade_config)
    node.add_data_client_factory(bundle.name, bundle.data_factory)
    if binance_spot is not None:
        node.add_data_client_factory(BINANCE_CLIENT_NAME, binance_spot[1])
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
    report_writer = TraderReportWriter.from_settings(settings, "live") if reports_enabled(settings) else None

    try:
        node = build_live_node(settings)
        run_live_node(node)
        if report_writer is not None:
            report_writer.write_final_reports(node.trader)
    finally:
        if node is not None:
            node.dispose()
        write_clean_live_log(settings)
        if report_writer is not None:
            print_live_summary(settings)
        release_run(settings)
