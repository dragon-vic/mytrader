from __future__ import annotations

import json
import os
import socket
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

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from utils.arguments import EXTERNAL_COMMAND_DEFAULT_HOST
from utils.arguments import EXTERNAL_COMMAND_DEFAULT_PORT


BEIJING_TZ = timezone(timedelta(hours=8))
SNAPSHOT_NAME = "pre_ipo_snapshot.json"
PAGE_COUNT = 2


def snapshot_path(value: str) -> Path:
    path = Path(value)
    if path.is_dir() or path.suffix == "":
        return path / SNAPSHOT_NAME
    return path


def load_snapshot(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


# 向 external_command data client 发送一条控制命令。
def send_command(command: str) -> str:
    payload = {
        "command": command,
        "reason": "monitor",
        "source": "pre_ipo_monitor",
        "sent_ns": time.time_ns(),
    }
    data = json.dumps(payload).encode("utf-8") + b"\n"
    try:
        with socket.create_connection((EXTERNAL_COMMAND_DEFAULT_HOST, EXTERNAL_COMMAND_DEFAULT_PORT), timeout=2.0) as client:
            client.sendall(data)
    except OSError as exc:
        return f"{command}发送失败: {exc}"
    return f"已发送{command}"


def status_table(payload: dict, session_name: str | None, notice: str) -> Table:
    table = Table(title="状态", expand=True)
    table.add_column("项目", justify="left", no_wrap=True)
    table.add_column("值", justify="left")
    table.add_row("策略", str(payload.get("strategy", "-")))
    table.add_row("状态", str(payload.get("state", "-")))
    table.add_row("模式", str(payload.get("mode", "-")))
    table.add_row("库存", str(payload.get("inventory", "-")))
    table.add_row("北京时间", datetime.now(tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"))
    table.add_row("session", session_name or "-")
    if notice:
        table.add_row("消息", notice)
    return table


def edge_table(payload: dict) -> Table:
    table = Table(title="Edge", expand=True)
    table.add_column("指标", justify="left", no_wrap=True)
    table.add_column("值", justify="right", no_wrap=True)
    edge = payload.get("edge") or {}
    for key in ("long_bps", "long_mean_bps", "long_std_bps", "short_bps", "short_mean_bps", "short_std_bps"):
        table.add_row(key, str(edge.get(key, "-")))
    return table


def quotes_table(payload: dict) -> Table:
    table = Table(title="Quote", expand=True)
    for column in ("venue", "bid", "ask", "bid_size", "ask_size", "last 10m count"):
        table.add_column(column, justify="right" if column != "venue" else "left", no_wrap=True)
    quotes = payload.get("quotes") or {}
    counts = payload.get("quote_counts") or []
    if not quotes:
        table.add_row(*("-" for _ in table.columns))
        return table
    for venue, row in quotes.items():
        instrument = str(row.get("instrument", ""))
        count_sum = sum(int(item.get(instrument, 0)) for item in counts)
        table.add_row(
            str(venue),
            str(row.get("bid", "-")),
            str(row.get("ask", "-")),
            str(row.get("bid_size", "-")),
            str(row.get("ask_size", "-")),
            str(count_sum) if counts else "-",
        )
    return table


def pnl_table(payload: dict) -> Table:
    table = Table(title="PnL", expand=True)
    table.add_column("指标", justify="left", no_wrap=True)
    table.add_column("值", justify="right", no_wrap=True)
    pnl = payload.get("pnl") or {}
    for key in ("realized_usdt", "unrealized_usdt", "fee_usdt"):
        table.add_row(key, str(pnl.get(key, "-")))
    return table


def positions_table(payload: dict) -> Table:
    table = Table(title="持仓", expand=True)
    for column in ("venue", "instrument", "qty", "avg_px", "realized_usdt", "unrealized_usdt", "fee_usdt"):
        table.add_column(column, justify="right" if column not in {"venue", "instrument"} else "left", no_wrap=True)
    positions = payload.get("positions") or {}
    if not positions:
        table.add_row(*("-" for _ in table.columns))
        return table
    for venue, row in positions.items():
        table.add_row(
            str(venue),
            str(row.get("instrument", "-")),
            str(row.get("qty", "-")),
            str(row.get("avg_px", "-")),
            str(row.get("realized_usdt", "-")),
            str(row.get("unrealized_usdt", "-")),
            str(row.get("fee_usdt", "-")),
        )
    return table


def event_latency_ms(row: dict, venue: str) -> str:
    metadata = row.get("metadata") or {}
    start = metadata.get("signal_event_ns")
    end = metadata.get(f"{venue.lower()}_full_fill_event_ns")
    if start in (None, "-") or end in (None, "-"):
        return "-"
    return f"{(int(end) - int(start)) / 1_000_000:.2f}"


def actions_table(payload: dict) -> Table:
    table = Table(title="行动历史", expand=True)
    columns = (
        "time",
        "asset",
        "edge_side",
        "status",
        "qty",
        "signal_edge",
        "actual_edge",
        "edge_slippage",
        "fill_slippage",
        "bn_latency",
        "okx_latency",
    )
    left_columns = {"time", "asset", "edge_side", "status"}
    for column in columns:
        table.add_column(column, justify="left" if column in left_columns else "right", no_wrap=True)
    rows = payload.get("actions") or []
    if not rows:
        table.add_row(*("-" for _ in columns))
        return table
    for row in rows[:20]:
        values = []
        for column in columns:
            if column == "bn_latency":
                values.append(event_latency_ms(row, "bn"))
            elif column == "okx_latency":
                values.append(event_latency_ms(row, "okx"))
            else:
                values.append(str(row.get(column, "-")))
        table.add_row(*values)
    return table


def build_view(payload: dict, snapshot: Path, session_name: str | None, notice: str, page: int) -> Group:
    keys = "↑/↓翻页 | r减仓 | n正常 | s停止 | Esc退出"
    header = Panel.fit(f"PRE IPO Monitor | Page {page + 1}/{PAGE_COUNT} | {keys}", border_style="cyan")
    if page == 1:
        return Group(
            header,
            actions_table(payload),
        )
    return Group(
        header,
        Columns([status_table(payload, session_name, notice), edge_table(payload)], equal=True, expand=True),
        Columns([quotes_table(payload), pnl_table(payload)], equal=True, expand=True),
        positions_table(payload),
    )


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
        if not ready:
            return ""
        key = os.read(fd, 1).decode(errors="ignore")
        if key == "\x1b":
            ready, _, _ = select.select([fd], [], [], 0.15)
            if ready:
                seq = os.read(fd, 1).decode(errors="ignore")
                ready, _, _ = select.select([fd], [], [], 0.05)
                if ready:
                    seq += os.read(fd, 1).decode(errors="ignore")
                if seq == "[A":
                    return "up"
                if seq == "[B":
                    return "down"
                return ""
            return "escape"
        return key.lower()
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


def node_running(session_name: str | None) -> bool:
    if session_name is None or os.name == "nt":
        return True
    result = subprocess.run(
        ["tmux", "has-session", "-t", session_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def main(path: Path, session_name: str | None = None) -> None:
    console = Console()
    notice = ""
    page = 0
    try:
        with Live(console=console, screen=True, refresh_per_second=1) as live:
            while True:
                payload = load_snapshot(path)
                live.update(build_view(payload, path, session_name, notice, page), refresh=True)
                if not node_running(session_name):
                    return
                key = read_key(1.0)
                if key == "escape":
                    return
                if key == "up":
                    page = (page - 1) % PAGE_COUNT
                elif key == "down":
                    page = (page + 1) % PAGE_COUNT
                elif key == "r" and session_name:
                    notice = send_command("reduce")
                elif key == "n" and session_name:
                    notice = send_command("normal")
                elif key == "s" and session_name:
                    notice = send_command("stop")
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python monitor.py REPORT_DIR_OR_SNAPSHOT [TMUX_SESSION]")
    main(snapshot_path(sys.argv[1]), sys.argv[2] if len(sys.argv) > 2 else None)
