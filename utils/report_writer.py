from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

import pandas as pd
from rich.console import Console
from rich.table import Table

from utils.config_loader import reports_dir, ROOT

REPORT_COLUMNS = {
    "orders": [
        "client_order_id",
        "instrument_id",
        "venue_order_id",
        "position_id",
        "type",
        "side",
        "quantity",
        "time_in_force",
        "filled_qty",
        "liquidity_side",
        "avg_px",
        "commissions",
        "status",
        "ts_init",
        "ts_last",
    ],
    "fills": [
        "ts_event",
        "instrument_id",
        "order_side",
        "order_type",
        "last_qty",
        "last_px",
        "currency",
        "commission",
        "liquidity_side",
        "client_order_id",
        "venue_order_id",
        "trade_id",
        "position_id",
    ],
    "positions": [
        "instrument_id",
        "entry",
        "side",
        "quantity",
        "peak_qty",
        "avg_px_open",
        "avg_px_close",
        "realized_pnl",
        "realized_return",
        "commissions",
        "ts_opened",
        "ts_closed",
        "duration_ns",
        "opening_order_id",
        "closing_order_id",
        "position_id",
    ],
}


# 返回当前 set 在 backtest/live 下的报告目录。
def run_reports_dir(settings: dict[str, Any], run_type: str):
    return ROOT / run_type / settings["project"]["reports_dir"]


# 只保留人工看交易结果需要的列。
def report_columns(name: str, df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()
    return df[[column for column in REPORT_COLUMNS[name] if column in df.columns]]


# 保存 NT trader 生成的订单、成交、持仓报告。
def write_trader_reports(trader, settings: dict[str, Any], run_type: str) -> None:
    output_dir = run_reports_dir(settings, run_type)
    output_dir.mkdir(parents=True, exist_ok=True)

    reports = {
        "orders": trader.generate_orders_report(),
        "fills": trader.generate_fills_report(),
        "positions": trader.generate_positions_report(),
    }
    for name, df in reports.items():
        if not df.empty:
            report_columns(name, df).to_csv(output_dir / f"{name}.csv", index=False)


# 保存回测结果 json。
def write_backtest_result(result, settings: dict[str, Any]) -> dict[str, Any]:
    output_dir = run_reports_dir(settings, "backtest")
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = asdict(result)
    (output_dir / "backtest_result.json").write_text(
        json.dumps(payload, indent=2, default=str),
        encoding="utf-8",
    )
    return payload


# 打印回测核心摘要。
def print_backtest_summary(payload: dict[str, Any]) -> None:
    table = Table(title="Backtest Summary")
    table.add_column("Metric")
    table.add_column("Value")
    for key in ("iterations", "total_events", "total_orders", "total_positions", "elapsed_time"):
        table.add_row(key, str(payload.get(key)))
    for currency, stats in payload.get("stats_pnls", {}).items():
        table.add_row(f"{currency} PnL", str(stats.get("PnL (total)")))
        table.add_row(f"{currency} Win Rate", str(stats.get("Win Rate")))
    Console().print(table)
