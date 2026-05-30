from __future__ import annotations

import importlib
import os
import sys
from threading import Thread
from typing import Any

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.config import LiveDataEngineConfig
from nautilus_trader.live.config import LiveExecEngineConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId

from actors.data_recorder import DataRecorder
from actors.data_recorder import DataRecorderConfig
from adapters.common import cache_config
from utils.arguments import NODE_STOP_TOPIC
from utils.config_loader import load_settings
from utils.config_loader import proxy_url
from utils.instrument_factory import InstrumentFactory
from utils.polymarket_btc5m import up_down_instrument_windows
from utils.report_writer import print_live_summary
from utils.report_writer import TraderReportWriter
from utils.runtime_ids import claim_run
from utils.runtime_ids import finalize_run_dir
from utils.runtime_ids import release_run
from utils.strategy_factory import build_strategy


def adapter_module(name: str):
    return importlib.import_module(f"adapters.{name}")


# 为 exec reconciliation 限定当前策略关心的 instrument。
def reconciliation_instrument_ids(settings: dict[str, Any]):
    scope = settings["exec"]["engine"]["reconciliation_scope"]
    if scope != "configured_markets":
        return None

    exec_clients = [cfg for cfg in settings["exec"]["clients"].values() if cfg.get("enabled")]
    if len(exec_clients) != 1:
        raise ValueError("exec.engine.reconciliation_scope=configured_markets requires exactly one enabled exec client")

    source = exec_clients[0]
    if source["adapter"] == "polymarket":
        windows = up_down_instrument_windows(proxy_url(settings))
        instrument_ids = [InstrumentId.from_str(instrument_id) for instrument_id in windows]
        settings["runtime"]["instrument_ids"] = instrument_ids
        settings["runtime"]["event_windows"] = windows
        return instrument_ids

    if settings["markets_all"]:
        return None

    factory = InstrumentFactory(settings)
    return [factory.instrument_id(market) for market in factory.markets]


# 构建 NT data engine 配置。
def data_engine_config(settings: dict[str, Any]) -> LiveDataEngineConfig:
    return LiveDataEngineConfig(**settings["data"]["engine"])


# 构建 live exec engine 配置。
def exec_engine_config(settings: dict[str, Any]) -> LiveExecEngineConfig:
    cfg = dict(settings["exec"]["engine"])
    cfg.pop("reconciliation_scope")
    return LiveExecEngineConfig(
        **cfg,
        reconciliation_instrument_ids=reconciliation_instrument_ids(settings),
    )


# 构建所有启用的行情客户端。
def build_data_clients(settings: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    configs = {}
    factories = {}
    for cfg in settings["data"]["clients"].values():
        if not cfg["enabled"]:
            continue
        client_id, config, factory = adapter_module(cfg["adapter"]).build_data_client(settings, cfg)
        configs[client_id] = config
        factories[client_id] = factory
    return configs, factories


# 构建所有启用的执行客户端。
def build_exec_clients(settings: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    configs = {}
    factories = {}
    for cfg in settings["exec"]["clients"].values():
        if not cfg["enabled"]:
            continue
        client_id, config, factory = adapter_module(cfg["adapter"]).build_exec_client(settings, cfg)
        configs[client_id] = config
        factories[client_id] = factory
    return configs, factories


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
def build_live_node(settings: dict[str, Any]) -> TradingNode:
    data_clients, data_factories = build_data_clients(settings)
    exec_clients, exec_factories = build_exec_clients(settings)
    logging = settings["logging"]

    trade_config = TradingNodeConfig(
        trader_id=settings["runtime"]["trader_id"],
        cache=cache_config(settings),
        logging=LoggingConfig(
            log_level=logging["log_level"],
            log_colors=bool(logging["log_colors"]),
            log_component_levels={
                **logging["component_levels"],
                settings["strategy"]["class"]: logging["strategy_log_level"],
            },
        ),
        data_engine=data_engine_config(settings),
        data_clients=data_clients,
        exec_clients=exec_clients,
        exec_engine=exec_engine_config(settings),
    )

    node = TradingNode(config=trade_config)
    for client_id, factory in data_factories.items():
        node.add_data_client_factory(client_id, factory)
    for client_id, factory in exec_factories.items():
        node.add_exec_client_factory(client_id, factory)

    node.trader.add_strategy(build_strategy(settings, "live"))

    if settings["actors"]["data_recorder"]["enabled"]:
        node.trader.add_actor(DataRecorder(DataRecorderConfig()))
    attach_node_stop_handler(node)

    node.build()
    return node


# Windows 保留回车停止；Linux/tmux 用 Ctrl+C 停止。
def run_live_node(node: TradingNode) -> None:
    loop = node.get_event_loop()

    def wait_enter() -> None:
        input()
        loop.call_soon_threadsafe(lambda: stop_node(node, "回车"))

    if os.name == "nt" and sys.stdin.isatty():
        Thread(target=wait_enter, daemon=True).start()
    node.run()


# 运行 live/testnet，由 run.py 负责传入配置名和模式。
def main(config_name: str, mode: str | None = None) -> None:
    settings = load_settings(config_name, mode=mode)
    settings = claim_run(settings)
    report_writer = TraderReportWriter.from_settings(settings, "live")
    node = build_live_node(settings)

    try:
        run_live_node(node)
    finally:
        report_writer.write_final_reports(node.trader)
        node.dispose()
        finalize_run_dir(settings)
        print_live_summary(settings)
        release_run(settings)
