from __future__ import annotations

import json
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
REPORT_ROOT = Path("strategies/preipoarb/report")
SNAPSHOT_NAME = "preipo_arb_snapshot.json"


def latest_snapshot_path() -> Path:
    snapshots = sorted(
        REPORT_ROOT.glob(f"live-*/{SNAPSHOT_NAME}"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if snapshots:
        return snapshots[0]
    return REPORT_ROOT / SNAPSHOT_NAME


def load_snapshot(path: Path) -> dict:
    if not path.exists():
        return {
            "rows": [],
            "market_tables": {},
            "position_rows": [],
            "state_rows": [],
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


def positions_table(rows: list[dict[str, str]]) -> Table:
    table = Table(title="Positions", expand=True)
    for column, justify in (
        ("asset", "left"),
        ("side", "left"),
        ("entry_buy", "left"),
        ("entry_sell", "left"),
        ("qty", "right"),
        ("entry_edge", "right"),
        ("entry_z", "right"),
        ("close_edge", "right"),
        ("current_z", "right"),
        ("hold", "right"),
    ):
        table.add_column(column, justify=justify, no_wrap=column not in {"entry_buy", "entry_sell"})
    if not rows:
        table.add_row("-", "-", "-", "-", "-", "-", "-", "-", "-", "-")
        return table
    for row in rows:
        table.add_row(
            str(row.get("asset", "-")),
            str(row.get("side", "-")),
            str(row.get("entry_buy", "-")),
            str(row.get("entry_sell", "-")),
            str(row.get("qty", "-")),
            str(row.get("entry_edge", "-")),
            str(row.get("entry_z", "-")),
            str(row.get("current_close_edge", "-")),
            str(row.get("current_z", "-")),
            str(row.get("hold", "-")),
        )
    return table


def state_table(rows: list[dict[str, str]]) -> Table:
    table = Table(title="State", expand=True)
    for column, justify in (
        ("asset", "left"),
        ("state", "left"),
        ("active", "center"),
        ("pending", "center"),
        ("position", "center"),
    ):
        table.add_column(column, justify=justify, no_wrap=True)
    if not rows:
        table.add_row("-", "waiting", "-", "-", "-")
        return table
    for row in rows:
        table.add_row(
            str(row.get("asset", "-")),
            str(row.get("state", "-")),
            str(row.get("active", "-")),
            str(row.get("pending", "-")),
            str(row.get("has_position", "-")),
        )
    return table


def build_view(payload: dict, path: Path) -> Group:
    market_tables = payload.get("market_tables") or {}
    assets = payload.get("assets") or sorted(market_tables)
    beijing_time = datetime.now(tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    parts = [Panel.fit(f"PREIPO Arbitrage Live | 北京时间 {beijing_time} | {path}", border_style="cyan")]
    tables = [market_table(str(asset), market_tables.get(str(asset), [])) for asset in assets]
    if len(tables) >= 2:
        parts.append(Columns(tables[:2], equal=True, expand=True))
        parts.extend(tables[2:])
    else:
        parts.extend(tables)
    parts.append(positions_table(payload.get("position_rows") or []))
    parts.append(state_table(payload.get("state_rows") or []))
    return Group(*parts)


def main(path: Path | None, refresh_sec: float) -> None:
    console = Console()
    try:
        current_path = path or latest_snapshot_path()
        with Live(build_view(load_snapshot(current_path), current_path), console=console, screen=True, refresh_per_second=1) as live:
            while True:
                current_path = path or latest_snapshot_path()
                live.update(build_view(load_snapshot(current_path), current_path), refresh=True)
                time.sleep(refresh_sec)
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main(None, 1.0)
