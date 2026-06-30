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
SNAPSHOT_NAME = "pre_ipo_snapshot.json"
MONITOR_PAGES = 2


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
        ("lot", "lot", "right"),
        ("asset", "标的", "left"),
        ("action", "动作", "center"),
        ("status", "状态", "center"),
        ("qty", "qty", "right"),
        ("signal_edge", "信号edge", "right"),
        ("edge_slippage", "edge滑点", "right"),
        ("fill_slippage", "成交滑点", "right"),
        ("mean", "均值", "right"),
        ("std", "波动", "right"),
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
        ("unrealized_usdt", "未实现USDT"),
        ("realized_bps", "已实现bps"),
        ("unrealized_bps", "未实现bps"),
        ("total_bps", "总计bps"),
    )
    table.add_column("标的", justify="left", no_wrap=True)
    for _, label in metrics:
        table.add_column(label, justify="right", no_wrap=True)
    if not rows:
        table.add_row("-")
        return table
    for asset, values in rows.items():
        table.add_row(str(asset), *(str(values.get(key, "-")) for key, _ in metrics))
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
    table.add_column("交易所", justify="left", no_wrap=True)
    for _, label in metrics:
        table.add_column(label, justify="right", no_wrap=True)
    if not rows:
        table.add_row("-")
        return table
    for venue, values in rows.items():
        table.add_row(str(venue), *(str(values.get(key, "-")) for key, _ in metrics))
    return table


def build_view(payload: dict, path: Path, session_name: str | None, page: int = 0, stopping: bool = False) -> Group:
    market_tables = payload.get("market_tables") or {}
    assets = payload.get("assets") or sorted(market_tables)
    beijing_time = datetime.now(tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    if stopping:
        keys = "正在停止node..."
    else:
        keys = "↑/↓翻页 | Esc退出监控 | s停止node" if session_name else "↑/↓翻页 | Esc退出监控"
    parts = [Panel.fit(f"PRE IPO Live | Page {page + 1}/{MONITOR_PAGES} | 北京时间 {beijing_time} | {keys} | {path}", border_style="cyan")]
    if page == 1:
        parts.append(action_table(payload.get("action_rows") or []))
        return Group(*parts)
    tables = [market_table(str(asset), market_tables.get(str(asset), [])) for asset in assets]
    first_row = tables[:3]
    if first_row:
        parts.append(Columns(first_row, equal=True, expand=True))
    if len(tables) > 3:
        parts.extend(tables[3:])
    parts.append(Columns([risk_table(payload.get("risk") or {}), summary_table(payload.get("summary") or {})], equal=True, expand=True))
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
        ready, _, _ = select.select([fd], [], [], timeout_sec)
        if ready:
            key = os.read(fd, 1).decode(errors="ignore")
            if key == "\x1b":
                # 方向键在 Linux/tmux 下是 ESC 开头的多字节序列；SSH 延迟下 0.05s
                # 容易把 ↑ 的 ESC 误判成退出。
                ready, _, _ = select.select([fd], [], [], 0.2)
                if not ready:
                    return "escape"
                seq = os.read(fd, 1).decode(errors="ignore")
                deadline = time.monotonic() + 0.1
                while time.monotonic() < deadline:
                    ready, _, _ = select.select([fd], [], [], 0.02)
                    if not ready:
                        break
                    seq += os.read(fd, 1).decode(errors="ignore")
                if seq.startswith("[A"):
                    return "up"
                if seq.startswith("[B"):
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
    page = 0
    try:
        payload = load_snapshot(path)
        with Live(build_view(payload, path, session_name, page), console=console, screen=True, refresh_per_second=1) as live:
            while True:
                payload = load_snapshot(path)
                live.update(build_view(payload, path, session_name, page, stopping), refresh=True)
                if stopping:
                    if session_name is None or not node_running(session_name):
                        return
                    time.sleep(refresh_sec)
                    continue
                key = read_key(refresh_sec)
                if key == "escape":
                    return
                if key == "up":
                    page = max(page - 1, 0)
                if key == "down":
                    page = min(page + 1, MONITOR_PAGES - 1)
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

