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
            "risk": {},
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
        ("status", "状态", "center"),
        ("qty", "qty", "right"),
        ("signal_edge", "信号edge", "right"),
        ("edge_slippage", "edge滑点", "right"),
        ("fill_slippage", "成交滑点", "right"),
        ("mean", "均值", "right"),
        ("std", "波动", "right"),
        ("close_lot", "平lot", "right"),
        ("inventory", "库存", "right"),
        ("time", "北京时间", "left"),
        ("age_min", "分钟", "right"),
    )
    for key, label, justify in columns:
        table.add_column(label, justify=justify, no_wrap=key != "time")
    if not rows:
        table.add_row(*("-" for _ in columns))
        return table
    for row in rows:
        table.add_row(*(action_value(row, key) for key, _, _ in columns))
    return table


def action_value(row: dict[str, str], key: str) -> str:
    if key == "action":
        side = str(row.get("edge_side", ""))
        if side == "long_edge":
            return "long"
        if side == "short_edge":
            return "short"
    return str(row.get(key, "-"))


def summary_table(rows: dict[str, dict[str, str]]) -> Table:
    table = Table(title="Summary", expand=True)
    metrics = (
        ("inventory", "库存"),
        ("realized_usdt", "已实现USDT"),
        ("realized_bps", "已实现bps"),
        ("unrealized_bps", "未实现bps"),
        ("total_bps", "总计bps"),
    )
    assets = list(rows)
    table.add_column("指标", justify="left", no_wrap=True)
    for asset in assets:
        table.add_column(str(asset), justify="right", no_wrap=True)
    if not rows:
        table.add_row("-")
        return table
    for key, label in metrics:
        table.add_row(label, *(str(rows[asset].get(key, "-")) for asset in assets))
    return table


def risk_table(rows: dict[str, dict[str, str]]) -> Table:
    table = Table(title="Risk", expand=True)
    metrics = (
        ("wallet_usdt", "钱包USDT"),
        ("unrealized_usdt", "未实现USDT"),
        ("risk_rate", "风险率"),
        ("positions", "持仓数"),
        ("status", "状态"),
    )
    venues = list(rows)
    table.add_column("指标", justify="left", no_wrap=True)
    for venue in venues:
        table.add_column(str(venue), justify="right", no_wrap=True)
    if not rows:
        table.add_row("-")
        return table
    for key, label in metrics:
        table.add_row(label, *(str(rows[venue].get(key, "-")) for venue in venues))
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
    first_row = tables[:2] + [summary_table(payload.get("summary") or {}), risk_table(payload.get("risk") or {})]
    if first_row:
        parts.append(Columns(first_row, equal=True, expand=True))
    if len(tables) > 2:
        parts.extend(tables[2:])
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
