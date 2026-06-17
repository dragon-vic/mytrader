from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


# 分配当前策略唯一 node 身份和本次运行报告目录。
def claim_run(settings: dict[str, Any]) -> dict[str, Any]:
    run_kind = "backtest" if settings["mode"] == "backtest" else "live"
    start_time = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")
    strategy_name = settings["strategy"]["name"]

    runtime = dict(settings.get("runtime", {}))
    runtime["node_id"] = strategy_name
    runtime["node_num"] = 1
    runtime["run_name"] = strategy_name
    runtime["report_dir_name"] = f"{run_kind}-{start_time}"
    runtime["started_at"] = start_time
    runtime["trader_id"] = f"TRADER-{strategy_name.upper().replace('_', '-')}"
    settings["runtime"] = runtime
    return settings

