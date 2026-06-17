from __future__ import annotations

import json
import time
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.table import Table


def load_snapshot(path: Path) -> dict:
    if not path.exists():
        return {"rows": [], "ts_ns": None}
    return json.loads(path.read_text(encoding="utf-8"))


def build_table(payload: dict, path: Path) -> Table:
    table = Table(title=f"PREIPO Arbitrage Live - {path}")
    for column, justify in (
        ("asset", "left"),
        ("state", "left"),
        ("active", "center"),
        ("pending", "center"),
        ("edge", "right"),
        ("z", "right"),
        ("mean", "right"),
        ("std", "right"),
        ("src", "left"),
        ("samples", "right"),
        ("window", "right"),
        ("buy", "left"),
        ("sell", "left"),
        ("quotes", "left"),
    ):
        table.add_column(column, justify=justify, no_wrap=column not in {"quotes", "buy", "sell"})

    rows = payload.get("rows", [])
    if not rows:
        table.add_row("-", "waiting", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "snapshot not ready")
        return table

    for row in rows:
        table.add_row(
            str(row.get("asset", "-")),
            str(row.get("state", "-")),
            str(row.get("active", "-")),
            str(row.get("pending", "-")),
            str(row.get("edge", "-")),
            str(row.get("z", "-")),
            str(row.get("mean", "-")),
            str(row.get("std", "-")),
            str(row.get("source", "-")),
            str(row.get("samples", "-")),
            str(row.get("window", "-")),
            str(row.get("buy", "-")),
            str(row.get("sell", "-")),
            str(row.get("quotes", "-")),
        )
    return table


def main(path: Path, refresh_sec: float) -> None:
    console = Console()
    with Live(build_table(load_snapshot(path), path), console=console, screen=True, refresh_per_second=2) as live:
        while True:
            live.update(build_table(load_snapshot(path), path), refresh=True)
            time.sleep(refresh_sec)


if __name__ == "__main__":
    main(
        Path("reports/live/preipo_arb_snapshot.json"),
        1.0,
    )
