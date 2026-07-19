from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


# 分配当前策略唯一 node 身份和本次运行报告目录。
def claim_run(settings: dict[str, Any], started_at: str | None = None) -> dict[str, Any]:
    run_kind = "backtest" if settings["mode"] == "backtest" else "live"
    start_time = started_at or os.environ.get("NT_RUN_STARTED_AT")
    if not start_time:
        start_time = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d%H%M%S")
    strategy_name = settings["project"]["config_name"]

    runtime = dict(settings.get("runtime", {}))
    runtime["report_dir_name"] = f"{run_kind}-{start_time}"
    runtime["started_at"] = start_time
    runtime["trader_id"] = f"TRADER-{strategy_name.upper().replace('_', '-')}"
    settings["runtime"] = runtime
    return settings
