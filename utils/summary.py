from __future__ import annotations

import json
import time
from datetime import datetime
from io import StringIO
from math import sqrt
from pathlib import Path
from typing import Any

import pandas as pd
from rich.columns import Columns
from rich.console import Console
from rich.table import Table

from utils.constants import LOCAL_TZ
from utils.constants import ORDERS_FILE
from utils.constants import POSITIONS_FILE
from utils.constants import SUMMARY_FILE
from utils.reports import commissions_to_float
from utils.reports import money_to_float
from utils.reports import read_report_csv


def print_backtest_summary(
    payload: dict[str, Any],
    settings: dict[str, Any],
    output_dir: Path,
) -> float:
    started = time.perf_counter()
    elapsed_days = float(payload.get("elapsed_time") or 0) / 86400
    sections = [
        ("回测总览", ("指标", "数值"), backtest_overview_rows(payload, settings, output_dir)),
        ("参数", ("参数", "数值"), parameter_rows(settings)),
        ("仓位统计", ("指标", "数值"), trade_stats_rows(output_dir, elapsed_days)),
        (
            "标的统计",
            ("标的", "仓位数", "时间平均持仓", "平均最大持仓", "胜率", "净收益", "收益率", "平均收益", "最大盈利", "最大亏损", "手续费"),
            instrument_stats_rows(output_dir, elapsed_days),
        ),
        ("订单执行统计", ("指标", "数值"), order_stats_rows(output_dir, elapsed_days)),
    ]
    write_summary_json("回测摘要", sections, output_dir)
    print_summary_tables(Console(), sections)
    buffer = StringIO()
    print_summary_tables(Console(file=buffer, width=200), sections)
    (output_dir / "summary.txt").write_text(buffer.getvalue(), encoding="utf-8")
    return time.perf_counter() - started


def print_live_summary(settings: dict[str, Any], output_dir: Path) -> None:
    if not settings["reports"]["enabled"]:
        return
    elapsed_days = report_elapsed_days(output_dir)
    sections = [
        ("运行总览", ("指标", "数值"), live_overview_rows(settings, output_dir)),
        ("仓位统计", ("指标", "数值"), trade_stats_rows(output_dir, elapsed_days)),
        (
            "标的统计",
            ("标的", "仓位数", "平均持仓数量", "平均最大持仓", "胜率", "净收益", "收益率", "平均收益", "最大盈利", "最大亏损", "手续费"),
            instrument_stats_rows(output_dir, elapsed_days),
        ),
        ("订单执行统计", ("指标", "数值"), order_stats_rows(output_dir, elapsed_days)),
    ]
    write_summary_json("运行摘要", sections, output_dir)
    print_summary_tables(Console(), sections)


def write_summary_json(
    title: str,
    sections: list[tuple[str, tuple[str, ...], list[tuple[Any, ...]]]],
    output_dir: Path,
) -> None:
    payload = {
        "title": title,
        "generated_at": datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "sections": [
            {
                "title": section,
                "headers": list(headers),
                "rows": [[str(value) for value in row] for row in rows],
            }
            for section, headers, rows in sections
            if rows
        ],
    }
    (output_dir / SUMMARY_FILE).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def print_summary_tables(
    console: Console,
    sections: list[tuple[str, tuple[str, ...], list[tuple[Any, ...]]]],
) -> None:
    top = [
        summary_table(title, headers, rows)
        for title, headers, rows in sections
        if rows and title != "标的统计"
    ]
    if top:
        console.print(Columns(top, equal=True, expand=True))
    for title, headers, rows in sections:
        if title != "标的统计" or not rows:
            continue
        if console.width >= 160:
            console.print(summary_table(title, headers, rows))
            continue
        for row in rows:
            console.print(instrument_detail_table(headers, row))


def summary_table(title: str, headers: tuple[str, ...], rows: list[tuple[Any, ...]]) -> Table:
    table = Table(title=title)
    for index, header in enumerate(headers):
        table.add_column(str(header), justify="left" if index == 0 else "right")
    for row in rows:
        table.add_row(*(str(value) for value in row))
    return table


def instrument_detail_table(headers: tuple[str, ...], row: tuple[Any, ...]) -> Table:
    table = Table(title=f"标的统计：{row[0]}")
    table.add_column("指标")
    table.add_column("数值", justify="right")
    for header, value in zip(headers[1:], row[1:]):
        table.add_row(str(header), str(value))
    return table


def print_saved_summary(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    sections = [
        (
            str(section["title"]),
            tuple(str(header) for header in section["headers"]),
            [tuple(str(value) for value in row) for row in section["rows"]],
        )
        for section in payload["sections"]
    ]
    print_summary_tables(Console(), sections)


def backtest_overview_rows(
    payload: dict[str, Any],
    settings: dict[str, Any],
    output_dir: Path,
) -> list[tuple[str, str]]:
    data_interval = backtest_data_interval(settings)
    starting_balance = starting_balance_value(settings)
    return_stats = report_return_stats(output_dir, starting_balance)
    return [
        ("配置名", settings["project"]["config_name"]),
        ("策略名", settings["project"]["config_name"]),
        ("交易标的", traded_symbol_count(output_dir)),
        ("K线周期", data_interval),
        ("回测开始", format_timestamp_ns(payload.get("backtest_start"))),
        ("回测结束", format_timestamp_ns(payload.get("backtest_end"))),
        ("回测天数", format_number(float(payload.get("elapsed_time") or 0) / 86400)),
        ("初始资金", starting_balance_text(settings)),
        ("盈利因子", format_number(return_stats.get("profit_factor"))),
        ("Sharpe", format_number(return_stats.get("sharpe"))),
        ("Sortino", format_number(return_stats.get("sortino"))),
        ("迭代次数", format_int(payload.get("iterations"))),
        ("事件数", format_int(payload.get("total_events"))),
    ]


def parameter_rows(settings: dict[str, Any]) -> list[tuple[str, str]]:
    values = settings.get("runtime", {}).get("backtest_params") or {}
    return [(str(key), str(value)) for key, value in values.items()]


def backtest_data_interval(settings: dict[str, Any]) -> str:
    datasets = settings["backtest"].get("datasets", [])
    types = {dataset["type"] for dataset in datasets}
    labels = []
    if {"quote_ticks", "quote_tick_objects"} & types:
        labels.append("quote")
    if {"trade_ticks", "trade_tick_catalog"} & types:
        labels.append("tick")
    if "bars" in types:
        labels.extend(
            sorted(
                dataset["bar_type"]
                for dataset in datasets
                if dataset["type"] == "bars"
            ),
        )
    return "/".join(labels)


def live_overview_rows(settings: dict[str, Any], output_dir: Path) -> list[tuple[str, str]]:
    return [
        ("配置名", settings["project"]["config_name"]),
        ("策略名", settings["project"]["config_name"]),
        ("运行模式", settings["mode"]),
        ("报告目录", output_dir.name),
        ("交易标的", traded_symbol_count(output_dir)),
        ("净收益", format_number(net_pnl(output_dir))),
        ("生成时间", datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")),
    ]


def traded_symbol_count(output_dir: Path) -> str:
    orders = read_report_csv(output_dir, ORDERS_FILE)
    symbols: set[str] = set()
    if not orders.empty and "标的" in orders.columns and "已成交数量" in orders.columns:
        qty = pd.to_numeric(orders["已成交数量"], errors="coerce").fillna(0)
        symbols = {short_symbol(value) for value in orders.loc[qty > 0, "标的"].dropna()}
    if not symbols:
        positions = read_report_csv(output_dir, POSITIONS_FILE)
        if not positions.empty and "标的" in positions.columns:
            symbols = {short_symbol(value) for value in positions["标的"].dropna()}
    return f"{len(symbols)}个"


def short_symbol(value: Any) -> str:
    return str(value).split(".")[0].replace("USDT-PERP", "")


def trade_stats_rows(output_dir: Path, elapsed_days: float) -> list[tuple[str, str]]:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    avg_qty = average_position_qty(output_dir, elapsed_days)
    if positions.empty:
        return [
            ("完成仓位数", "0"),
            ("胜率", "0%"),
            ("净收益", "0"),
            ("平均仓位收益", "0"),
            ("总手续费", "0"),
            ("平均持仓数量", format_number(avg_qty)),
            ("平均持仓分钟", "0"),
            ("最长持仓分钟", "0"),
        ]

    pnl = positions["已实现盈亏"].map(money_to_float)
    fees = positions["手续费合计"].map(commissions_to_float)
    duration_min = pd.to_numeric(positions["持仓分钟"], errors="coerce")
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    return [
        ("净收益", format_number(net_pnl(output_dir))),
        ("总手续费", format_number(fees.sum())),
        ("完成仓位数", format_int(len(positions))),
        ("胜率", format_percent((pnl > 0).mean())),
        ("平均仓位收益", format_number(pnl.mean())),
        ("仓位收益中位数", format_number(pnl.median())),
        ("盈利仓位平均收益", format_number(wins.mean())),
        ("亏损仓位平均亏损", format_number(losses.mean())),
        ("最大盈利", format_number(pnl.max())),
        ("最大亏损", format_number(pnl.min())),
        ("平均持仓数量", format_number(avg_qty)),
        ("平均持仓分钟", format_number(duration_min.mean())),
        ("最长持仓分钟", format_number(duration_min.max())),
    ]


def average_position_qty(output_dir: Path, elapsed_days: float) -> float:
    return sum(average_position_qty_by_instrument(output_dir, elapsed_days).values())


def average_position_qty_by_instrument(output_dir: Path, elapsed_days: float) -> dict[str, float]:
    orders = read_report_csv(output_dir, ORDERS_FILE)
    if orders.empty:
        return {}
    required = {"成交时间", "标的", "方向", "已成交数量"}
    if not required.issubset(orders.columns):
        return {}

    data = orders.copy()
    data["order_time"] = pd.to_datetime(data["成交时间"], utc=True, errors="coerce")
    data["filled_qty_num"] = pd.to_numeric(data["已成交数量"], errors="coerce").fillna(0).abs()
    data = data[data["order_time"].notna() & (data["filled_qty_num"] > 0)].sort_values("order_time", kind="mergesort")
    if data.empty:
        return {}

    positions: dict[str, float] = {}
    weighted: dict[str, float] = {}
    prev_time = data.iloc[0]["order_time"]
    for row in data.itertuples(index=False):
        order_time = row.order_time
        elapsed_min = max((order_time - prev_time).total_seconds() / 60, 0.0)
        for instrument, qty in positions.items():
            weighted[instrument] = weighted.get(instrument, 0.0) + abs(qty) * elapsed_min

        instrument = str(getattr(row, "标的"))
        filled_qty = float(row.filled_qty_num)
        direction = 1.0 if str(getattr(row, "方向")).upper() == "BUY" else -1.0
        positions[instrument] = positions.get(instrument, 0.0) + direction * filled_qty
        if abs(positions[instrument]) < 1e-12:
            positions[instrument] = 0.0
        prev_time = order_time

    total_minutes = elapsed_days * 1440
    if total_minutes <= 0:
        total_minutes = (data.iloc[-1]["order_time"] - data.iloc[0]["order_time"]).total_seconds() / 60
        if total_minutes <= 0:
            return {}
    event_span = (data.iloc[-1]["order_time"] - data.iloc[0]["order_time"]).total_seconds() / 60
    if total_minutes > event_span:
        tail_min = total_minutes - event_span
        for instrument, qty in positions.items():
            weighted[instrument] = weighted.get(instrument, 0.0) + abs(qty) * tail_min
    return {instrument: value / total_minutes for instrument, value in weighted.items()}


def net_pnl(output_dir: Path) -> float:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    if positions.empty:
        return 0.0
    return positions["已实现盈亏"].map(money_to_float).sum()


def starting_balance_value(settings: dict[str, Any]) -> float:
    return sum(money_to_float(value) for value in starting_balance_values(settings))


def starting_balance_text(settings: dict[str, Any]) -> str:
    values = starting_balance_values(settings)
    currencies = {money_currency(value) for value in values}
    if len(currencies) == 1:
        return f"{format_number(sum(money_to_float(value) for value in values))} {currencies.pop()}"
    return ", ".join(str(value) for value in values)


def starting_balance_values(settings: dict[str, Any]) -> list[Any]:
    values = []
    for venue in settings["backtest"]["venues"].values():
        values.extend(venue["starting_balances"])
    return values


def money_currency(value: Any) -> str:
    parts = str(value).split()
    return parts[1] if len(parts) > 1 else ""


def report_return_stats(output_dir: Path, starting_balance: float) -> dict[str, float]:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    if positions.empty or starting_balance == 0:
        return {"profit_factor": 0.0, "sharpe": 0.0, "sortino": 0.0}
    pnl = positions["已实现盈亏"].map(money_to_float)
    wins = pnl[pnl > 0].sum()
    losses = -pnl[pnl < 0].sum()
    profit_factor = wins / losses if losses else 0.0

    close_time = pd.to_datetime(positions["平仓时间"], errors="coerce")
    daily = pnl.groupby(close_time.dt.date).sum() / starting_balance
    if len(daily) < 2:
        return {"profit_factor": profit_factor, "sharpe": 0.0, "sortino": 0.0}
    std = daily.std(ddof=1)
    sharpe = daily.mean() / std * sqrt(252) if std and not pd.isna(std) else 0.0
    downside = daily[daily < 0]
    downside_std = downside.std(ddof=1)
    valid_downside = len(downside) > 1 and downside_std and not pd.isna(downside_std)
    sortino = daily.mean() / downside_std * sqrt(252) if valid_downside else 0.0
    return {"profit_factor": profit_factor, "sharpe": sharpe, "sortino": sortino}


def instrument_stats_rows(output_dir: Path, elapsed_days: float) -> list[tuple[str, ...]]:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    if positions.empty:
        return [("无成交", "0", "0", "0", "0%", "0", "0%", "0", "0", "0", "0")]

    data = positions.copy()
    data["净收益"] = data["已实现盈亏"].map(money_to_float)
    data["手续费"] = data["手续费合计"].map(commissions_to_float)
    data["最大持仓"] = pd.to_numeric(data["数量"], errors="coerce").fillna(0).abs()
    data["开仓名义额"] = pd.to_numeric(data["数量"], errors="coerce") * pd.to_numeric(data["开仓均价"], errors="coerce")
    avg_qty = average_position_qty_by_instrument(output_dir, elapsed_days)
    rows = []
    for instrument, group in data.groupby("标的"):
        pnl = group["净收益"]
        notional = group["开仓名义额"].sum()
        rows.append((
            str(instrument),
            format_int(len(group)),
            format_number(avg_qty.get(str(instrument), 0.0)),
            format_number(group["最大持仓"].mean()),
            format_percent((pnl > 0).mean()),
            format_number(pnl.sum()),
            format_percent(pnl.sum() / notional if notional else 0),
            format_number(pnl.mean()),
            format_number(pnl.max()),
            format_number(pnl.min()),
            format_number(group["手续费"].sum()),
        ))
    return sorted(rows, key=lambda row: money_to_float(row[5]), reverse=True)


def order_stats_rows(output_dir: Path, elapsed_days: float = 0.0) -> list[tuple[str, str]]:
    orders = read_report_csv(output_dir, ORDERS_FILE)
    if orders.empty:
        return [
            ("订单总数", "0"),
            ("平均每日交易数", "0"),
            ("多单数量", "0"),
            ("空单数量", "0"),
            ("有成交订单数", "0"),
            ("已完成订单数", "0"),
            ("已取消订单数", "0"),
            ("本地拒单数", "0"),
            ("交易所拒单数", "0"),
        ]
    filled_qty = pd.to_numeric(orders["已成交数量"], errors="coerce").fillna(0)
    return [
        ("订单总数", format_int(len(orders))),
        ("平均每日交易数", format_number(len(orders) / elapsed_days if elapsed_days else 0)),
        ("多单数量", format_int((orders["方向"] == "BUY").sum())),
        ("空单数量", format_int((orders["方向"] == "SELL").sum())),
        ("有成交订单数", format_int((filled_qty > 0).sum())),
        ("已完成订单数", format_int((orders["订单状态"] == "FILLED").sum())),
        ("已取消订单数", format_int((orders["订单状态"] == "CANCELED").sum())),
        ("本地拒单数", format_int((orders["订单状态"] == "DENIED").sum())),
        ("交易所拒单数", format_int((orders["订单状态"] == "REJECTED").sum())),
    ]


def report_elapsed_days(output_dir: Path) -> float:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    if positions.empty:
        return 0.0
    opened = pd.to_datetime(positions["开仓时间"], utc=True, errors="coerce")
    closed = pd.to_datetime(positions["平仓时间"], utc=True, errors="coerce")
    start = opened.min()
    end = closed.max()
    if pd.isna(start) or pd.isna(end) or end <= start:
        return 0.0
    return (end - start).total_seconds() / 86400


def format_timestamp_ns(value) -> str:
    if value is None:
        return ""
    return pd.to_datetime(value, unit="ns", utc=True).tz_convert(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def format_number(value, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return ""
    number = round(float(value), 2)
    if number == 0:
        number = 0.0
    text = f"{number:.2f}".rstrip("0").rstrip(".")
    return f"{text}{suffix}"


def format_int(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{int(value)}"


def format_percent(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return format_number(float(value) * 100, "%")
