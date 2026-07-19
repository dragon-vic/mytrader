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

from utils.constants import EXTERNAL_COMMAND_DEFAULT_HOST
from utils.constants import EXTERNAL_COMMAND_DEFAULT_PORT


BEIJING_TZ = timezone(timedelta(hours=8))
ASSETS = ("ANTHROPIC", "OPENAI")
SNAPSHOT_FILE = "pre_ipo_snapshot.json"
PAGE_COUNT = 2
REFRESH_SEC = 60.0
NODE_CHECK_SEC = 1.0
KEY_POLL_SEC = 0.1


def load_snapshot(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_snapshots(report_dir: Path) -> tuple[dict[str, dict], dict]:
    snapshot = load_snapshot(report_dir / SNAPSHOT_FILE)
    strategies = snapshot.get("strategies") or {}
    return {asset: strategies.get(asset) or {} for asset in ASSETS}, snapshot


# 向 external_command data client 发送一条控制命令。
def send_command(command: str) -> str:
    payload = {
        "target": "ALL",
        "command": command,
        "reason": "monitor",
        "source": "pre_ipo_monitor",
        "sent_ns": time.time_ns(),
    }
    data = json.dumps(payload).encode("utf-8") + b"\n"
    try:
        with socket.create_connection(
            (EXTERNAL_COMMAND_DEFAULT_HOST, EXTERNAL_COMMAND_DEFAULT_PORT),
            timeout=2.0,
        ) as client:
            client.sendall(data)
    except OSError as exc:
        return f"{command}发送失败: {exc}"
    return f"已发送{command}"


def status_table(payloads: dict[str, dict]) -> Table:
    table = Table(title="状态", expand=True)
    table.add_column("项目", justify="left", no_wrap=True)
    for asset in ASSETS:
        table.add_column(asset, justify="right", no_wrap=True)
    for label, key in (
        ("状态", "state"),
        ("模式", "mode"),
        ("库存", "inventory"),
    ):
        table.add_row(
            label,
            *(str(payloads[asset].get(key, "-")) for asset in ASSETS),
        )
    return table


def edge_table(payloads: dict[str, dict]) -> Table:
    table = Table(title="Edge", expand=True)
    table.add_column("指标", justify="left", no_wrap=True)
    for asset in ASSETS:
        table.add_column(asset, justify="right", no_wrap=True)
    for key in (
        "long_bps",
        "long_mean_bps",
        "long_std_bps",
        "short_bps",
        "short_mean_bps",
        "short_std_bps",
    ):
        table.add_row(
            key,
            *(str((payloads[asset].get("edge") or {}).get(key, "-")) for asset in ASSETS),
        )
    return table


def accounts_table(payload: dict) -> Table:
    table = Table(title="Account USDT", expand=True)
    accounts = payload.get("accounts") or {}
    risk = payload.get("risk") or {}
    venues = list(accounts)

    table.add_column("metric", justify="left", no_wrap=True)

    if not accounts:
        table.add_column("value", justify="right", no_wrap=True)
        table.add_row("-", "-")
        return table

    for venue in venues:
        table.add_column(str(venue), justify="right", no_wrap=True)

    metrics = (
        ("total", accounts, "total_usdt"),
        ("free", accounts, "free_usdt"),
        ("locked", accounts, "locked_usdt"),
        ("strategy lock", risk, "locked_usdt"),
        ("reserved", risk, "reserved_usdt"),
        ("available", risk, "available_usdt"),
        ("unrealized", risk, "unrealized_usdt"),
        ("risk", risk, "risk_rate"),
    )

    for label, source, key in metrics:
        table.add_row(
            label,
            *(str((source.get(venue) or {}).get(key, "-")) for venue in venues),
        )

    return table


def quotes_table(payload: dict) -> Table:
    table = Table(title=f"{payload.get('asset', '-')} Quote", expand=True)
    for column in (
        "venue",
        "bid",
        "ask",
        "bid_size",
        "ask_size",
        "last 10m count",
    ):
        table.add_column(
            column,
            justify="right" if column != "venue" else "left",
            no_wrap=True,
        )

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


def positions_table(payload: dict) -> Table:
    table = Table(title=f"{payload.get('asset', '-')} 持仓", expand=True)

    for column in (
        "venue",
        "instrument",
        "qty",
        "avg_px",
        "realized_usdt",
        "unrealized_usdt",
        "fee_usdt",
    ):
        table.add_column(
            column,
            justify="right" if column not in {"venue", "instrument"} else "left",
            no_wrap=True,
        )

    positions = payload.get("positions") or {}

    if positions:
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

    pnl = payload.get("pnl") or {}
    table.add_row(
        "total",
        "-",
        "-",
        "-",
        str(pnl.get("realized_usdt", "-")),
        str(pnl.get("unrealized_usdt", "-")),
        str(pnl.get("fee_usdt", "-")),
    )

    return table


def event_latency_ms(row: dict, venue: str) -> str:
    metadata = row.get("metadata") or {}
    start = metadata.get("signal_event_ns")
    end = metadata.get(f"{venue.lower()}_full_fill_event_ns")

    if start in (None, "-") or end in (None, "-"):
        return "-"

    return f"{(int(end) - int(start)) / 1_000_000:.2f}"


def actions_table(payloads: dict[str, dict]) -> Table:
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
        table.add_column(
            column,
            justify="left" if column in left_columns else "right",
            no_wrap=True,
        )

    rows = [
        row
        for payload in payloads.values()
        for row in (payload.get("actions") or [])
    ]

    rows.sort(
        key=lambda row: int(
            (row.get("metadata") or {}).get("signal_event_ns", 0)
        ),
        reverse=True,
    )

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


def build_view(
    payloads: dict[str, dict],
    coordinator: dict,
    session_name: str | None,
    notice: str,
    page: int,
) -> Group:
    keys = "↑/↓翻页 | r减仓 | n正常 | s停止 | Esc退出"
    now = datetime.now(tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

    header_text = (
        f"北京时间 {now} | session {session_name or '-'}\n"
        f"Page {page + 1}/{PAGE_COUNT} | {keys}"
    )

    if notice:
        header_text += f"\n{notice}"

    header = Panel.fit(
        header_text,
        title="PRE IPO Monitor",
        border_style="cyan",
    )

    if page == 1:
        return Group(
            header,
            actions_table(payloads),
        )

    strategy_rows = list(payloads.values())

    return Group(
        header,

        # 第一行：Account、状态、Edge 三个表等宽排列。
        Columns(
            [
                accounts_table(coordinator),
                status_table(payloads),
                edge_table(payloads),
            ],
            equal=True,
            expand=True,
        ),

        Columns(
            [quotes_table(payload) for payload in strategy_rows],
            equal=True,
            expand=True,
        ),

        Columns(
            [positions_table(payload) for payload in strategy_rows],
            equal=True,
            expand=True,
        ),
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


def main(report_dir: Path, session_name: str | None = None) -> None:
    console = Console()
    notice = ""
    page = 0
    payloads = {asset: {} for asset in ASSETS}
    coordinator = {}
    last_refresh = 0.0
    last_node_check = 0.0
    terminal_size = console.size
    dirty = True

    try:
        with Live(
            console=console,
            screen=True,
            auto_refresh=False,
        ) as live:
            while True:
                now = time.monotonic()
                current_size = console.size

                if current_size != terminal_size:
                    terminal_size = current_size
                    dirty = True

                if now - last_refresh >= REFRESH_SEC:
                    payloads, coordinator = load_snapshots(report_dir)
                    last_refresh = now
                    dirty = True

                if dirty:
                    live.update(
                        build_view(
                            payloads,
                            coordinator,
                            session_name,
                            notice,
                            page,
                        ),
                        refresh=True,
                    )
                    dirty = False

                if now - last_node_check >= NODE_CHECK_SEC:
                    if not node_running(session_name):
                        return

                    last_node_check = now

                key = read_key(KEY_POLL_SEC)

                if key == "escape":
                    return

                if key == "up":
                    page = (page - 1) % PAGE_COUNT
                    dirty = True

                elif key == "down":
                    page = (page + 1) % PAGE_COUNT
                    dirty = True

                elif key == "r" and session_name:
                    notice = send_command("reduce")
                    dirty = True

                elif key == "n" and session_name:
                    notice = send_command("normal")
                    dirty = True

                elif key == "s" and session_name:
                    notice = send_command("stop")
                    dirty = True

    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python monitor.py REPORT_DIR_OR_SNAPSHOT [TMUX_SESSION]"
        )

    main(
        Path(sys.argv[1]),
        sys.argv[2] if len(sys.argv) > 2 else None,
    )
