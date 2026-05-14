from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from utils.config_loader import ROOT


# 判断 pid 是否仍在运行。
def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


# 返回本 node 的报告序号。
def next_seq(root: Path, name: str) -> int:
    seq = 1
    prefix = f"{name}-"
    for path in root.glob(f"{prefix}*"):
        suffix = path.name.removeprefix(prefix)
        if suffix.isdigit():
            seq = max(seq, int(suffix) + 1)
    return seq


# 分配运行时 node 和本次 run 名称。
def claim_run(settings: dict[str, Any]) -> dict[str, Any]:
    reports = ROOT / settings["project"]["reports_dir"]
    lock_dir = reports / ".nodes"
    lock_dir.mkdir(parents=True, exist_ok=True)

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

    stem = f"{settings['project']['config_name']}-{settings['mode']}-{node_id}"
    seq = next_seq(reports, stem)
    run_name = f"{stem}-{seq}"
    payload = {
        "pid": os.getpid(),
        "config": settings["project"]["config_name"],
        "mode": settings["mode"],
        "node_id": node_id,
        "run_name": run_name,
    }
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    runtime = dict(settings.get("runtime", {}))
    runtime["node_id"] = node_id
    runtime["node_num"] = node_num
    runtime["run_seq"] = seq
    runtime["run_name"] = run_name
    runtime["lock_path"] = str(lock)
    runtime["trader_id"] = f"TRADER-{node_id.upper()}"
    settings["runtime"] = runtime

    external = dict(settings.get("external_signal", {}))
    if "port" in external:
        external["port"] = int(external["port"]) + node_num - 1
        settings["external_signal"] = external

    return settings


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
