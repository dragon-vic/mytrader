from __future__ import annotations

import importlib
import os
import sys
from threading import Thread
from typing import Any

from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.config import LiveDataEngineConfig
from nautilus_trader.live.config import LiveExecEngineConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId

from adapters.common import cache_config
from adapters.common import LiveContext
from utils.config import load_settings
from utils.config import proxy_url
from utils.live_control import NodeStopController
from utils.live_control import NodeStopRequest
from utils.reports import TraderReportWriter
from utils.runtime_setup import actor_specs
from utils.runtime_setup import claim_run
from utils.runtime_setup import log_file_settings
from utils.runtime_setup import prepare_run_dir
from utils.runtime_setup import runtime_start_ns
from utils.runtime_setup import strategy_components
from utils.runtime_setup import strategy_specs
from utils.summary import print_live_summary


def adapter_module(name: str):
    return importlib.import_module(f"adapters.{name}")


def live_context(settings: dict[str, Any]) -> LiveContext:
    return LiveContext(mode=settings["mode"], proxy_url=proxy_url())


# configured_markets 对所有启用的 exec client 聚合显式 InstrumentId。
def reconciliation_instrument_ids(settings: dict[str, Any]) -> list[InstrumentId] | None:
    engine = settings["node"]["exec"]["engine"]
    scope = engine["reconciliation_scope"]
    if scope is None:
        return None
    if scope != "configured_markets":
        raise ValueError(f"unsupported exec.engine.reconciliation_scope: {scope}")

    values: dict[str, InstrumentId] = {}
    for cfg in settings["node"]["exec"]["clients"].values():
        if not cfg["enabled"]:
            continue
        if cfg["markets_all"]:
            raise ValueError("reconciliation_scope=configured_markets requires explicit exec markets")
        for market in cfg["markets"]:
            instrument_id = InstrumentId.from_str(market["instrument_id"])
            values[str(instrument_id)] = instrument_id
    return list(values.values())


def data_engine_config(settings: dict[str, Any]) -> LiveDataEngineConfig:
    return LiveDataEngineConfig(**settings["node"]["data"]["engine"])


def exec_engine_config(settings: dict[str, Any]) -> LiveExecEngineConfig:
    cfg = dict(settings["node"]["exec"]["engine"])
    cfg.pop("reconciliation_scope")
    return LiveExecEngineConfig(
        **cfg,
        reconciliation_instrument_ids=reconciliation_instrument_ids(settings),
    )


# 构建所有启用的行情客户端。
def build_data_clients(
    settings: dict[str, Any],
    context: LiveContext,
) -> tuple[dict[str, Any], dict[str, Any]]:
    configs = {}
    factories = {}
    for cfg in settings["node"]["data"]["clients"].values():
        if not cfg["enabled"]:
            continue
        client_id, config, factory = adapter_module(cfg["adapter"]).build_data_client(context, cfg)
        if client_id in configs:
            raise ValueError(f"duplicate data client_id: {client_id}")
        configs[client_id] = config
        factories[client_id] = factory
    return configs, factories


# 构建所有启用的执行客户端。
def build_exec_clients(
    settings: dict[str, Any],
    context: LiveContext,
) -> tuple[dict[str, Any], dict[str, Any]]:
    configs = {}
    factories = {}
    for cfg in settings["node"]["exec"]["clients"].values():
        if not cfg["enabled"]:
            continue
        client_id, config, factory = adapter_module(cfg["adapter"]).build_exec_client(context, cfg)
        if client_id in configs:
            raise ValueError(f"duplicate exec client_id: {client_id}")
        configs[client_id] = config
        factories[client_id] = factory
    return configs, factories


# 为当前 set 创建尚未 build 的 TradingNode。
def build_live_node(settings: dict[str, Any]) -> TradingNode:
    context = live_context(settings)
    data_clients, data_factories = build_data_clients(settings, context)
    exec_clients, exec_factories = build_exec_clients(settings, context)
    logging = settings["node"]["logging"]

    config = TradingNodeConfig(
        trader_id=settings["runtime"]["trader_id"],
        cache=cache_config(settings),
        logging=LoggingConfig(
            log_level=logging["log_level"],
            log_colors=bool(logging["log_colors"]),
            log_component_levels={
                **logging["component_levels"],
                **{name: logging["strategy_log_level"] for name in strategy_components(settings)},
            },
            **log_file_settings(settings),
        ),
        data_engine=data_engine_config(settings),
        data_clients=data_clients,
        exec_clients=exec_clients,
        exec_engine=exec_engine_config(settings),
        actors=actor_specs(settings),
        strategies=strategy_specs(settings),
    )

    node = TradingNode(config=config)
    for client_id, factory in data_factories.items():
        node.add_data_client_factory(client_id, factory)
    for client_id, factory in exec_factories.items():
        node.add_exec_client_factory(client_id, factory)
    return node


# Windows 保留回车停止；Linux/tmux 用 Ctrl+C 停止。
def run_live_node(node: TradingNode, stop: NodeStopController) -> None:
    def wait_enter() -> None:
        try:
            input()
        except EOFError:
            return
        stop.request(NodeStopRequest(source="stdin", reason="enter"))

    if os.name == "nt" and sys.stdin.isatty():
        Thread(target=wait_enter, daemon=True).start()
    node.run()


# 无论报告是否成功，都释放 node 并尝试生成结束摘要。
def finalize_live(
    node: TradingNode,
    stop: NodeStopController,
    writer: TraderReportWriter,
    settings: dict[str, Any],
    built: bool,
) -> None:
    stop.detach()
    try:
        if built:
            writer.write(node.trader)
    finally:
        try:
            node.dispose()
        finally:
            print_live_summary(settings, writer.output_dir)


# 运行 live/testnet，由 run.py 负责传入配置名和模式。
def main(config_name: str | None = None, mode: str = "live") -> None:
    settings = claim_run(load_settings(config_name, mode=mode))
    output_dir = prepare_run_dir(settings)
    writer = TraderReportWriter(
        output_dir,
        bool(settings["reports"]["enabled"]),
        runtime_start_ns(settings),
    )
    node = build_live_node(settings)
    stop = NodeStopController(node)
    built = False
    stop.attach()

    try:
        node.build()
        built = True
        run_live_node(node, stop)
    finally:
        finalize_live(node, stop, writer, settings, built)
