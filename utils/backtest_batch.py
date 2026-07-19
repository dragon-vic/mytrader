from __future__ import annotations

import copy
import itertools
import os
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from utils.component_factory import strategy_entries
from utils.runtime_ids import claim_run


def grid_cases(grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    names = list(grid)
    return [
        dict(zip(names, values))
        for values in itertools.product(*(grid[name] for name in names))
    ]


# base config 保持原样，只有 batch.cases 与 batch.grid 会展开。
def strategy_cases(entry: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    base = entry["config"]
    batch = entry.get("batch")
    if batch is None:
        return [(copy.deepcopy(base), {})]
    cases = batch.get("cases", [{}])
    grid = batch.get("grid", {})
    rows = []
    for case in cases:
        overlap = set(case) & set(grid)
        if overlap:
            raise ValueError(f"batch case and grid overlap: {', '.join(sorted(overlap))}")
        for values in grid_cases(grid):
            selected = {**case, **values}
            rows.append(({**base, **selected}, selected))
    return rows


def case_settings(
    settings: dict[str, Any],
    names: list[str],
    choices: tuple[tuple[dict[str, Any], dict[str, Any]], ...],
) -> dict[str, Any]:
    case = copy.deepcopy(settings)
    selected = {}
    for name, (config, params) in zip(names, choices):
        entry = case["strategy"][name]
        entry["config"] = config
        entry.pop("batch", None)
        selected.update({f"{name}.{key}": value for key, value in params.items()})
    case["runtime"] = {**case.get("runtime", {}), "backtest_params": selected}
    return case


def expand_settings(settings: dict[str, Any]) -> list[dict[str, Any]]:
    active = strategy_entries(settings)
    names = list(active)
    expanded = [strategy_cases(active[name]) for name in names]
    return [
        case_settings(settings, names, choices)
        for choices in itertools.product(*expanded)
    ]


def max_workers(settings: dict[str, Any]) -> int:
    workers = int(settings["backtest"]["max_workers"])
    if workers < 1:
        raise ValueError("backtest.max_workers must be >= 1")
    return workers


def worker_count(settings: dict[str, Any], case_count: int) -> int:
    return min(max_workers(settings), case_count)


def claim_cases(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    started_at = os.environ.get("NT_RUN_STARTED_AT") or datetime.now(
        ZoneInfo("Asia/Shanghai"),
    ).strftime("%Y%m%d%H%M%S")
    claimed = []
    for index, case in enumerate(cases):
        item = claim_run(case, started_at)
        parent = f"backtest-{started_at}"
        item["runtime"]["report_dir_name"] = f"{parent}/backtest-{started_at}-{index:03d}"
        claimed.append(item)
    return claimed
