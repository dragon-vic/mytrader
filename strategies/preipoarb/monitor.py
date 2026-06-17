from __future__ import annotations

import json
import time
from pathlib import Path

from rich.console import Console
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table


def load_snapshot(path: Path) -> dict:
    if not path.exists():
        return {"rows": [], "market_tables": {}, "position_rows": [], "state_rows": [], "ts_ns": None}
    return json.loads(path.read_text(encoding="utf-8"))


def market_table(asset: str, rows: list[dict[str, str]]) -> Table:
    table = Table(title=f"{asset} Market", expand=True)
    for column, justify in (
        ("exchange", "left"),
        ("instrument", "left"),
        ("bid1", "right"),
        ("ask1", "right"),
        ("age", "right"),
        ("role", "center"),
        ("edge", "right"),
        ("mean", "right"),
        ("std", "right"),
        ("z", "right"),
        ("src", "left"),
        ("window", "right"),
    ):
        table.add_column(column, justify=justify, no_wrap=True)
    if not rows:
        table.add_row("-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-")
        return table
    for row in rows:
        table.add_row(
            str(row.get("exchange", "-")),
            str(row.get("instrument", "-")),
            str(row.get("bid1", "-")),
            str(row.get("ask1", "-")),
            str(row.get("age", "-")),
            str(row.get("role", "-")),
            str(row.get("edge", "-")),
            str(row.get("mean", "-")),
            str(row.get("std", "-")),
            str(row.get("z", "-")),
            str(row.get("source", "-")),
            str(row.get("window", "-")),
        )
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
    parts = [Panel.fit(f"PREIPO Arbitrage Live - {path}", border_style="cyan")]
    for asset in assets:
        parts.append(market_table(str(asset), market_tables.get(str(asset), [])))
    parts.append(positions_table(payload.get("position_rows") or []))
    parts.append(state_table(payload.get("state_rows") or []))
    return Group(*parts)


def main(path: Path, refresh_sec: float) -> None:
    console = Console()
    with Live(build_view(load_snapshot(path), path), console=console, screen=True, refresh_per_second=1) as live:
        while True:
            live.update(build_view(load_snapshot(path), path), refresh=True)
            time.sleep(refresh_sec)


if __name__ == "__main__":
    main(
        Path("reports/live/preipo_arb_snapshot.json"),
        1.0,
    )
