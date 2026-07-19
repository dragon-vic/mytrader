from __future__ import annotations

import copy
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from nautilus_trader.config import ImportableActorConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.trading.config import StrategyFactory

from utils.constants import LOCAL_TZ


# 分配当前策略唯一 node 身份和本次运行目录。
def claim_run(settings: dict[str, Any], started_at: str | None = None) -> dict[str, Any]:
    run_kind = "backtest" if settings["mode"] == "backtest" else "live"
    start_time = started_at or os.environ.get("NT_RUN_STARTED_AT")
    if not start_time:
        start_time = datetime.now(LOCAL_TZ).strftime("%Y%m%d%H%M%S")
    strategy_name = settings["project"]["config_name"]

    runtime = dict(settings.get("runtime", {}))
    runtime["report_dir_name"] = f"{run_kind}-{start_time}"
    runtime["started_at"] = start_time
    runtime["trader_id"] = f"TRADER-{strategy_name.upper().replace('_', '-')}"
    settings["runtime"] = runtime
    return settings


def reports_root(settings: dict[str, Any]) -> Path:
    root = Path(settings["reports"]["root"])
    if root.is_absolute():
        return root
    return Path(settings["project"]["strategy_dir"]) / root


def run_reports_dir(settings: dict[str, Any]) -> Path:
    return reports_root(settings) / settings["runtime"]["report_dir_name"]


# 创建当前运行目录；每次运行使用新目录，不清理旧文件。
def prepare_run_dir(settings: dict[str, Any]) -> Path:
    root = reports_root(settings).resolve()
    output_dir = run_reports_dir(settings).resolve()
    # Windows 多进程下 resolve() 可能混用 \\?\ 扩展路径，先统一再校验目录边界。
    checked_root = Path(str(root).removeprefix("\\\\?\\"))
    checked_output = Path(str(output_dir).removeprefix("\\\\?\\"))
    checked_output.relative_to(checked_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def runtime_start_ns(settings: dict[str, Any]) -> int:
    started_at = settings["runtime"]["started_at"]
    dt = datetime.strptime(str(started_at), "%Y%m%d%H%M%S").replace(tzinfo=LOCAL_TZ)
    return int(dt.timestamp()) * 1_000_000_000


# 返回 NT LoggingConfig 需要的文件日志参数。
def log_file_settings(settings: dict[str, Any]) -> dict[str, Any]:
    logging = settings["reports"]["logging"]
    return {
        "log_level_file": logging["log_level_file"],
        "log_directory": str(run_reports_dir(settings)),
        "log_file_name": logging["log_file_name"],
        "log_file_format": logging["log_file_format"],
        "log_file_max_size": logging["log_file_max_size"],
        "log_file_max_backup_count": logging["log_file_max_backup_count"],
        "clear_log_file": bool(logging["clear_log_file"]),
    }


def strategy_entries(settings: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        name: entry
        for name, entry in settings["strategy"].items()
        if entry["enabled"]
    }


def component_config(entry: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    config = copy.deepcopy(entry["config"])
    if not entry.get("artifacts"):
        return config
    root = run_reports_dir(settings).resolve()
    for field, filename in entry.get("artifacts", {}).items():
        relative = Path(filename)
        if relative.is_absolute():
            raise ValueError(f"artifact path must be relative: {filename}")
        target = (root / relative).resolve()
        target.relative_to(root)
        config[field] = str(target)
    return config


def strategy_specs(settings: dict[str, Any]) -> list[ImportableStrategyConfig]:
    return [
        ImportableStrategyConfig(
            strategy_path=entry["strategy_path"],
            config_path=entry["config_path"],
            config=component_config(entry, settings),
        )
        for entry in strategy_entries(settings).values()
    ]


def create_strategies(settings: dict[str, Any]) -> list[Any]:
    return [StrategyFactory.create(spec) for spec in strategy_specs(settings)]


def actor_specs(settings: dict[str, Any]) -> list[ImportableActorConfig]:
    return [
        ImportableActorConfig(
            actor_path=entry["actor_path"],
            config_path=entry["config_path"],
            config=component_config(entry, settings),
        )
        for entry in settings["node"]["actors"].values()
    ]


def strategy_components(settings: dict[str, Any]) -> list[str]:
    return [
        entry["strategy_path"].rsplit(":", 1)[1]
        for entry in strategy_entries(settings).values()
    ]
