from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from nautilus_trader.config import ImportableActorConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.trading.config import StrategyFactory

from utils.report_writer import run_reports_dir


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
