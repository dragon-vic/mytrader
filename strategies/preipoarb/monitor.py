from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from rich.columns import Columns
from rich.console import Console
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table


BEIJING_TZ = timezone(timedelta(hours=8))
SNAPSHOT_NAME = "preipo_arb_snapshot.json"


def snapshot_path(value: str) -> Path:
    path = Path(value)
    if path.is_dir() or path.suffix == "":
        return path / SNAPSHOT_NAME
    return path


def load_snapshot(path: Path) -> dict:
    if not path.exists():
        return {
            "rows": [],
            "market_tables": {},
            "action_rows": [],
            "summary": {},
            "inventories": {},
        }
    return json.loads(path.read_text(encoding="utf-8"))


def market_table(asset: str, rows: list[dict[str, str]]) -> Table:
    table = Table(title=f"{asset} Market", expand=True)
    venues = [key for key in (rows[0].keys() if rows else []) if key != "metric"]
    table.add_column("metric", justify="left", no_wrap=True)
    for venue in venues:
        table.add_column(venue, justify="right", no_wrap=True)
    if not rows:
        table.add_row("-")
        return table
    for row in rows:
        table.add_row(str(row.get("metric", "-")), *(str(row.get(venue, "-")) for venue in venues))
    return table


def action_table(rows: list[dict[str, str]]) -> Table:
    table = Table(title="Actions", expand=True)
    columns = (
        ("asset", "标的", "left"),
        ("action", "动作", "center"),
        ("edge_side", "方向", "left"),
        ("route", "路线", "left"),
        ("status", "状态", "center"),
        ("qty", "qty", "right"),
        ("signal_edge", "信号edge", "right"),
        ("actual_edge", "实际edge", "right"),
        ("edge_slippage", "偏差bps", "right"),
        ("mean", "均值", "right"),
        ("std", "波动", "right"),
        ("level", "层级", "right"),
        ("close_lot", "平lot", "right"),
        ("expected_capture", "预期收益", "right"),
        ("realized_capture", "实际收益", "right"),
        ("inventory", "库存", "right"),
        ("time", "北京时间", "left"),
        ("age_min", "分钟", "right"),
    )
    for key, label, justify in columns:
        table.add_column(label, justify=justify, no_wrap=key not in {"route", "time"})
    if not rows:
        table.add_row(*("-" for _ in columns))
        return table
    for row in rows:
        table.add_row(*(str(row.get(key, "-")) for key, _, _ in columns))
    return table


def summary_table(rows: dict[str, dict[str, str]]) -> Table:
    table = Table(title="Summary", expand=True)
    columns = (
        ("asset", "标的", "left"),
        ("inventory", "库存", "right"),
        ("realized_usdt", "已实现USDT", "right"),
        ("realized_bps", "已实现bps", "right"),
        ("unrealized_bps", "未实现bps", "right"),
        ("total_bps", "总计bps", "right"),
    )
    for _, label, justify in columns:
        table.add_column(label, justify=justify, no_wrap=True)
    if not rows:
        table.add_row(*("-" for _ in columns))
        return table
    for asset, row in rows.items():
        table.add_row(
            str(asset),
            str(row.get("inventory", "-")),
            str(row.get("realized_usdt", "-")),
            str(row.get("realized_bps", "-")),
            str(row.get("unrealized_bps", "-")),
            str(row.get("total_bps", "-")),
        )
    return table


def build_view(payload: dict, path: Path, session_name: str | None, stopping: bool = False) -> Group:
    market_tables = payload.get("market_tables") or {}
    assets = payload.get("assets") or sorted(market_tables)
    beijing_time = datetime.now(tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    if stopping:
        keys = "正在停止node..."
    else:
        keys = "q退出监控 | s停止node" if session_name else "q退出监控"
    parts = [Panel.fit(f"PREIPO Arbitrage Live | 北京时间 {beijing_time} | {keys} | {path}", border_style="cyan")]
    tables = [market_table(str(asset), market_tables.get(str(asset), [])) for asset in assets]
    if len(tables) >= 2:
        parts.append(Columns(tables[:2], equal=True, expand=True))
        parts.extend(tables[2:])
    else:
        parts.extend(tables)
    parts.append(summary_table(payload.get("summary") or {}))
    parts.append(action_table(payload.get("action_rows") or []))
    return Group(*parts)


def read_key(timeout_sec: float) -> str:
    if not sys.stdin.isatty():
        time.sleep(timeout_sec)
        return ""
    if os.name == "nt":
        import msvcrt

        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if msvcrt.kbhit():
                return msvcrt.getwch().lower()
            time.sleep(0.05)
        return ""

    import select
    import termios
    import tty

    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        ready, _, _ = select.select([sys.stdin], [], [], timeout_sec)
        if ready:
            return sys.stdin.read(1).lower()
        return ""
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


def stop_node(session_name: str | None) -> None:
    if not session_name:
        return
    subprocess.run(["tmux", "send-keys", "-t", session_name, "C-c"], check=False)


def node_running(session_name: str) -> bool:
    result = subprocess.run(["tmux", "has-session", "-t", session_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    return result.returncode == 0


def main(path: Path, refresh_sec: float, session_name: str | None = None) -> None:
    console = Console()
    stopping = False
    try:
        with Live(build_view(load_snapshot(path), path, session_name), console=console, screen=True, refresh_per_second=1) as live:
            while True:
                live.update(build_view(load_snapshot(path), path, session_name, stopping), refresh=True)
                if stopping:
                    if session_name is None or not node_running(session_name):
                        return
                    time.sleep(refresh_sec)
                    continue
                key = read_key(refresh_sec)
                if key == "q":
                    return
                if key == "s" and session_name:
                    stop_node(session_name)
                    stopping = True
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python monitor.py REPORT_DIR_OR_SNAPSHOT [TMUX_SESSION]")
    main(
        snapshot_path(sys.argv[1]),
        1.0,
        sys.argv[2] if len(sys.argv) > 2 else None,
    )
