from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from utils.config_loader import ROOT


# 判断 pid 是否仍在运行。
def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


# 返回不存在的目录名；base 可直接使用，重复时追加 _1/_2。
def unique_dir_name(root: Path, base: str) -> str:
    if not (root / base).exists():
        return base
    seq = 1
    while (root / f"{base}_{seq}").exists():
        seq += 1
    return f"{base}_{seq}"


# 分配运行时 node 和本次 run 名称。
def claim_run(settings: dict[str, Any]) -> dict[str, Any]:
    reports = ROOT / settings["reports"]["root"]
    lock_dir = reports / ".nodes"
    lock_dir.mkdir(parents=True, exist_ok=True)
    base_name = settings["strategy"]["name"]
    report_dir_name = unique_dir_name(reports, f"{base_name}-running")

    for lock in lock_dir.glob("node*.lock"):
        try:
            data = json.loads(lock.read_text(encoding="utf-8"))
            pid = int(data["pid"])
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            lock.unlink(missing_ok=True)
            continue
        if not pid_alive(pid):
            lock.unlink(missing_ok=True)

    node_num = 1
    while True:
        node_id = f"node{node_num}"
        lock = lock_dir / f"{node_id}.lock"
        try:
            fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            node_num += 1
            continue
        break

    payload = {
        "pid": os.getpid(),
        "config": settings["project"]["config_name"],
        "mode": settings["mode"],
        "node_id": node_id,
        "run_name": base_name,
        "report_dir_name": report_dir_name,
    }
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    runtime = dict(settings.get("runtime", {}))
    runtime["node_id"] = node_id
    runtime["node_num"] = node_num
    runtime["run_name"] = base_name
    runtime["report_dir_name"] = report_dir_name
    runtime["lock_path"] = str(lock)
    runtime["trader_id"] = f"TRADER-{node_id.upper()}"
    settings["runtime"] = runtime

    external = settings["data"]["clients"].get("external_signal")
    if external and external.get("enabled"):
        external["port"] = int(external["port"]) + node_num - 1

    return settings


# 把运行中的报告目录改成最终目录名。
def finalize_run_dir(settings: dict[str, Any]) -> None:
    runtime = settings.get("runtime", {})
    report_dir_name = runtime.get("report_dir_name")
    base_name = runtime.get("run_name")
    end_time = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%m%d%H%M")
    run_name = f"{base_name}-{end_time}" if base_name else ""
    if not report_dir_name or not run_name or report_dir_name == run_name:
        return

    reports = ROOT / settings["reports"]["root"]
    source = reports / report_dir_name
    target = reports / unique_dir_name(reports, run_name)
    if not source.exists():
        return
    if target.exists():
        raise FileExistsError(f"Report directory already exists: {target}")
    source.rename(target)
    runtime["report_dir_name"] = target.name
    settings["runtime"] = runtime


# 释放当前进程持有的 node lock。
def release_run(settings: dict[str, Any]) -> None:
    lock_path = settings.get("runtime", {}).get("lock_path")
    if not lock_path:
        return
    lock = Path(lock_path)
    try:
        data = json.loads(lock.read_text(encoding="utf-8"))
        if int(data["pid"]) != os.getpid():
            return
    except (OSError, ValueError, KeyError, json.JSONDecodeError):
        return
    lock.unlink(missing_ok=True)
    try:
        lock.parent.rmdir()
    except OSError:
        pass
