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
            "pair_rows": [],
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


def pair_history_table(rows: list[dict[str, str]]) -> Table:
    table = Table(title="持仓历史", expand=True)
    columns = (
        ("asset", "标的", "left"),
        ("lot", "pair", "right"),
        ("route", "方向", "left"),
        ("qty", "qty", "right"),
        ("entry_edge", "开仓edge", "right"),
        ("close_edge", "平仓edge", "right"),
        ("entry_mean", "开仓30m均值", "right"),
        ("entry_std", "开仓std", "right"),
        ("entry_jump", "偏离bps", "right"),
        ("status", "状态", "center"),
        ("opened_at", "开仓北京时间", "left"),
        ("hold_min", "持有分钟", "right"),
    )
    for key, label, justify in columns:
        table.add_column(label, justify=justify, no_wrap=key not in {"route", "opened_at"})
    if not rows:
        table.add_row(*("-" for _ in columns))
        return table
    for row in rows:
        table.add_row(
            *(str(row.get(key, "-")) for key, _, _ in columns),
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
    parts.append(pair_history_table(payload.get("pair_rows") or payload.get("position_rows") or []))
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
