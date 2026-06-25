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
ACTION_PAGE_SIZE = 10


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


def action_table(rows: list[dict[str, str]], page: int, page_size: int) -> Table:
    total_pages = action_pages(rows, page_size)
    page = clamp_page(page, rows, page_size)
    start = page * page_size
    page_rows = rows[start : start + page_size]
    table = Table(title=f"Actions {page + 1}/{total_pages}", expand=True)
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
    if not page_rows:
        table.add_row(*("-" for _ in columns))
        return table
    for row in page_rows:
        table.add_row(*(action_value(row, key) for key, _, _ in columns))
    return table


def action_pages(rows: list[dict[str, str]], page_size: int) -> int:
    if not rows:
        return 1
    return max((len(rows) + page_size - 1) // page_size, 1)


def clamp_page(page: int, rows: list[dict[str, str]], page_size: int) -> int:
    return max(0, min(page, action_pages(rows, page_size) - 1))


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


def build_view(payload: dict, path: Path, session_name: str | None, action_page: int = 0, stopping: bool = False) -> Group:
    market_tables = payload.get("market_tables") or {}
    assets = payload.get("assets") or sorted(market_tables)
    beijing_time = datetime.now(tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    if stopping:
        keys = "正在停止node..."
    else:
        keys = "↑/↓翻订单 | Esc退出监控 | s停止node" if session_name else "↑/↓翻订单 | Esc退出监控"
    parts = [Panel.fit(f"PREIPO Arbitrage Live | 北京时间 {beijing_time} | {keys} | {path}", border_style="cyan")]
    tables = [market_table(str(asset), market_tables.get(str(asset), [])) for asset in assets]
    first_row = tables[:2] + [summary_table(payload.get("summary") or {}), risk_table(payload.get("risk") or {})]
    if first_row:
        parts.append(Columns(first_row, equal=True, expand=True))
    if len(tables) > 2:
        parts.extend(tables[2:])
    parts.append(action_table(payload.get("action_rows") or [], action_page, ACTION_PAGE_SIZE))
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
                key = msvcrt.getwch()
                if key in ("\x00", "\xe0"):
                    direction = msvcrt.getwch()
                    if direction == "H":
                        return "up"
                    if direction == "P":
                        return "down"
                    return ""
                if key == "\x1b":
                    return "escape"
                return key.lower()
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
            key = sys.stdin.read(1)
            if key == "\x1b":
                ready, _, _ = select.select([sys.stdin], [], [], 0.05)
                if not ready:
                    return "escape"
                seq = sys.stdin.read(1)
                ready, _, _ = select.select([sys.stdin], [], [], 0.05)
                if ready:
                    seq += sys.stdin.read(1)
                if seq == "[A":
                    return "up"
                if seq == "[B":
                    return "down"
                return ""
            return key.lower()
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
    action_page = 0
    try:
        payload = load_snapshot(path)
        action_page = clamp_page(action_page, payload.get("action_rows") or [], ACTION_PAGE_SIZE)
        with Live(build_view(payload, path, session_name, action_page), console=console, screen=True, refresh_per_second=1) as live:
            while True:
                payload = load_snapshot(path)
                action_page = clamp_page(action_page, payload.get("action_rows") or [], ACTION_PAGE_SIZE)
                live.update(build_view(payload, path, session_name, action_page, stopping), refresh=True)
                if stopping:
                    if session_name is None or not node_running(session_name):
                        return
                    time.sleep(refresh_sec)
                    continue
                key = read_key(refresh_sec)
                if key == "escape":
                    return
                if key == "up":
                    action_page = max(action_page - 1, 0)
                if key == "down":
                    action_page = min(action_page + 1, action_pages(payload.get("action_rows") or [], ACTION_PAGE_SIZE) - 1)
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
