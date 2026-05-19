from __future__ import annotations

import sys
from threading import Thread

from nautilus_trader.core.nautilus_pyo3 import Environment
from nautilus_trader.core.nautilus_pyo3 import LiveNode
from nautilus_trader.core.nautilus_pyo3 import TraderId

from adapters.registry import build_adapter
from utils.config_loader import load_settings
from utils.report_writer import prepare_report_dir
from utils.report_writer import run_reports_dir
from utils.runtime_ids import claim_run
from utils.runtime_ids import release_run
from utils.strategy_factory import build_importable_strategy


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


# 为当前 set 构建 PyO3 LiveNode。
def build_live_node(settings: dict) -> LiveNode:
    adapter = build_adapter(settings)
    run_name = settings["runtime"]["run_name"]
    builder = LiveNode.builder(
        run_name,
        TraderId(settings["runtime"]["trader_id"]),
        Environment.LIVE,
    )
    builder.with_cache_config(adapter.cache)

    for name, (factory, config) in adapter.data.items():
        builder.add_data_client(name, factory, config)
    for name, (factory, config) in adapter.exec.items():
        builder.add_exec_client(name, factory, config)

    node = builder.build()
    node.add_strategy_from_config(build_importable_strategy(settings, "live"))
    return node


# 挂上回车停止监听，然后运行 PyO3 LiveNode。
def run_live_node(node: LiveNode) -> None:
    def wait_enter() -> None:
        input()
        node.stop()

    if sys.stdin.isatty():
        Thread(target=wait_enter, daemon=True).start()
    node.run()


# 运行 live/testnet，由 run.py 负责传入配置名和模式。
def main(config_name: str, mode: str | None = None) -> None:
    settings = resolve_live_mode(load_settings(config_name, mode=mode), mode)
    settings = claim_run(settings)
    node = None

    try:
        prepare_report_dir(settings, "live")
        node = build_live_node(settings)
        run_live_node(node)
    finally:
        if node is not None and node.is_running():
            node.stop()
        release_run(settings)
