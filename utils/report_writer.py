from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from rich.columns import Columns
from rich.console import Console
from rich.table import Table

from utils.arguments import POSITIONS_FILE
from utils.arguments import SUMMARY_FILE
from utils.config_loader import ROOT


LOCAL_TZ = ZoneInfo("Asia/Shanghai")


# 返回当前运行的报告目录。
def run_reports_dir(settings: dict[str, Any], run_type: str) -> Path:
    run_name = settings.get("runtime", {}).get("run_name")
    if run_name:
        return ROOT / settings["project"]["reports_dir"] / run_name
    return ROOT / settings["project"]["reports_dir"] / f"{settings['project']['config_name']}-{run_type}"


# 创建当前运行的报告目录；每次运行使用新目录，不再清理旧文件。
def prepare_report_dir(settings: dict[str, Any], run_type: str) -> Path:
    reports_root = (ROOT / settings["project"]["reports_dir"]).resolve()
    output_dir = run_reports_dir(settings, run_type).resolve()
    output_dir.relative_to(reports_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


# 生成 PyO3 回测结果数据，报告只落人读的表格文件。
def write_backtest_result(result, engine) -> dict[str, Any]:
    return {
        "backtest_start": engine.backtest_start,
        "backtest_end": engine.backtest_end,
        "elapsed_time": result.elapsed_time_secs,
        "iterations": result.iterations,
        "total_events": result.total_events,
        "stats_pnls": result.stats_pnls,
        "stats_returns": result.stats_returns,
    }


# 打印回测核心摘要。
def print_backtest_summary(payload: dict[str, Any], settings: dict[str, Any]) -> None:
    output_dir = run_reports_dir(settings, "backtest")
    elapsed_days = float(payload.get("elapsed_time") or 0) / 86400
    write_summary_markdown(
        "回测摘要",
        [
            ("回测总览", ("指标", "数值"), backtest_overview_rows(payload, settings)),
            ("交易统计", ("指标", "数值"), trade_stats_rows(output_dir, elapsed_days)),
            (
                "标的统计",
                ("标的", "交易数", "胜率", "净收益", "收益率", "平均收益", "最大盈利", "最大亏损", "手续费"),
                instrument_stats_rows(output_dir),
            ),
            ("订单执行统计", ("指标", "数值"), order_stats_rows(output_dir)),
        ],
        output_dir,
    )
    console = Console()
    print_summary_tables(console, build_backtest_overview_table(payload, settings), output_dir, elapsed_days)


# 总览、交易统计、订单统计并排显示；标的统计较宽，单独放下面。
def print_summary_tables(console: Console, overview: Table, output_dir: Path, elapsed_days: float) -> None:
    console.print(
        Columns(
            (
                overview,
                build_trade_stats_table(output_dir, elapsed_days),
                build_order_stats_table(output_dir),
            ),
            equal=True,
            expand=True,
        )
    )
    console.print(build_instrument_stats_table(output_dir))


# 保存终端同款表格到 markdown 摘要。
def write_summary_markdown(
    title: str,
    sections: list[tuple[str, tuple[str, ...], list[tuple[str, ...]]]],
    output_dir: Path,
) -> None:
    lines = [f"# {title}", ""]
    for section, headers, rows in sections:
        if not rows:
            continue
        lines.extend([f"## {section}", "", markdown_table(headers, rows), ""])
    (output_dir / SUMMARY_FILE).write_text("\n".join(lines), encoding="utf-8-sig")


# 组装 markdown 表格。
def markdown_table(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


# 用 NT 回测结果组装总体概览表。
def build_backtest_overview_table(payload: dict[str, Any], settings: dict[str, Any]) -> Table:
    table = Table(title="回测总览")
    table.add_column("指标")
    table.add_column("数值", justify="right")
    for label, value in backtest_overview_rows(payload, settings):
        table.add_row(label, value)
    return table


# 用 NT 回测结果组装总体概览行。
def backtest_overview_rows(payload: dict[str, Any], settings: dict[str, Any]) -> list[tuple[str, str]]:
    output_dir = run_reports_dir(settings, "backtest")
    markets = settings["markets"]
    symbols = market_symbols(settings, "backtest")
    timeframes = ", ".join(sorted({market["timeframe"] for market in markets}))
    stats_pnls = payload.get("stats_pnls", {})
    currency = next(iter(stats_pnls), "")
    pnl_stats = stats_pnls.get(currency, {})
    return_stats = payload.get("stats_returns", {})

    return [
        ("配置名", settings["project"]["config_name"]),
        ("策略名", settings["strategy"]["name"]),
        ("markets", symbols),
        ("交易标的", traded_symbol_count(output_dir)),
        ("K线周期", timeframes),
        ("回测开始", format_timestamp_ns(payload.get("backtest_start"))),
        ("回测结束", format_timestamp_ns(payload.get("backtest_end"))),
        ("回测天数", format_number(float(payload.get("elapsed_time") or 0) / 86400)),
        ("初始资金", settings["backtest"]["starting_balance"]),
        ("总收益", format_number(pnl_stats.get("PnL (total)"), currency)),
        ("总收益率", format_number(pnl_stats.get("PnL% (total)"), "%")),
        ("盈利因子", format_number(return_stats.get("Profit Factor"))),
        ("Sharpe", format_number(return_stats.get("Sharpe Ratio (252 days)"))),
        ("Sortino", format_number(return_stats.get("Sortino Ratio (252 days)"))),
        ("迭代次数", format_int(payload.get("iterations"))),
        ("事件数", format_int(payload.get("total_events"))),
    ]


# 总览里按 yaml 写法显示标的；markets: all 不展开成长列表。
def market_symbols(settings: dict[str, Any], run_type: str) -> str:
    if settings.get("mode_markets") == "all" or settings.get(run_type, {}).get("markets") == "all" or settings.get("markets_all"):
        return "all"
    return ", ".join(market["instrument_symbol"] for market in settings["markets"])


# 从报告里提取真正有成交的交易对。
def traded_symbol_count(output_dir: Path) -> str:
    orders = read_report_csv(output_dir, "orders.csv")
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
    text = str(value)
    return text.split(".")[0].replace("USDT-PERP", "")


# 从持仓汇总表组装交易统计表。
def build_trade_stats_table(output_dir: Path, elapsed_days: float) -> Table | None:
    rows = trade_stats_rows(output_dir, elapsed_days)

    table = Table(title="交易统计")
    table.add_column("指标")
    table.add_column("数值", justify="right")
    for label, value in rows:
        table.add_row(label, value)
    return table


# 从持仓汇总表组装交易统计行。
def trade_stats_rows(output_dir: Path, elapsed_days: float) -> list[tuple[str, str]]:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    if positions.empty:
        return [
            ("已完成交易数", "0"),
            ("多单数量", "0"),
            ("空单数量", "0"),
            ("平均每日交易数", "0"),
            ("胜率", "0%"),
            ("净收益", "0"),
            ("平均单笔收益", "0"),
            ("总手续费", "0"),
            ("平均持仓分钟", "0"),
            ("最长持仓分钟", "0"),
        ]

    pnl = positions["已实现盈亏"].map(money_to_float)
    fees = positions["手续费合计"].map(commissions_to_float)
    duration_ns = pd.to_numeric(positions["持仓纳秒"], errors="coerce")
    duration_min = duration_ns / 60_000_000_000
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    gross_profit = wins.sum()
    return [
        ("已完成交易数", format_int(len(positions))),
        ("多单数量", format_int((positions["开仓方向"] == "BUY").sum())),
        ("空单数量", format_int((positions["开仓方向"] == "SELL").sum())),
        ("平均每日交易数", format_number(len(positions) / elapsed_days if elapsed_days else 0)),
        ("胜率", format_percent((pnl > 0).mean())),
        ("净收益", format_number(pnl.sum())),
        ("平均单笔收益", format_number(pnl.mean())),
        ("单笔收益中位数", format_number(pnl.median())),
        ("盈利单平均收益", format_number(wins.mean())),
        ("亏损单平均亏损", format_number(losses.mean())),
        ("最大盈利", format_number(pnl.max())),
        ("最大亏损", format_number(pnl.min())),
        ("总手续费", format_number(fees.sum())),
        ("手续费/毛利润", format_percent(fees.sum() / gross_profit if gross_profit else 0)),
        ("平均持仓分钟", format_number(duration_min.mean())),
        ("最长持仓分钟", format_number(duration_min.max())),
    ]


# 从持仓汇总表按标的聚合统计。
def build_instrument_stats_table(output_dir: Path) -> Table | None:
    rows = instrument_stats_rows(output_dir)

    table = Table(title="标的统计")
    for column, justify in (
        ("标的", "left"),
        ("交易数", "right"),
        ("胜率", "right"),
        ("净收益", "right"),
        ("收益率", "right"),
        ("平均收益", "right"),
        ("最大盈利", "right"),
        ("最大亏损", "right"),
        ("手续费", "right"),
    ):
        table.add_column(column, justify=justify)

    for row in rows:
        table.add_row(*row)
    return table


# 从持仓汇总表按标的聚合统计行。
def instrument_stats_rows(output_dir: Path) -> list[tuple[str, ...]]:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    if positions.empty:
        return [("无成交", "0", "0%", "0", "0%", "0", "0", "0", "0")]

    data = positions.copy()
    data["净收益"] = data["已实现盈亏"].map(money_to_float)
    data["手续费"] = data["手续费合计"].map(commissions_to_float)
    data["开仓名义额"] = pd.to_numeric(data["数量"], errors="coerce") * pd.to_numeric(data["开仓均价"], errors="coerce")
    rows = []
    for instrument, group in data.groupby("标的"):
        pnl = group["净收益"]
        notional = group["开仓名义额"].sum()
        rows.append((
            str(instrument),
            format_int(len(group)),
            format_percent((pnl > 0).mean()),
            format_number(pnl.sum()),
            format_percent(pnl.sum() / notional if notional else 0),
            format_number(pnl.mean()),
            format_number(pnl.max()),
            format_number(pnl.min()),
            format_number(group["手续费"].sum()),
        ))
    return sorted(rows, key=lambda row: money_to_float(row[3]), reverse=True)


# 从订单和成交表组装执行统计表。
def build_order_stats_table(output_dir: Path) -> Table | None:
    rows = order_stats_rows(output_dir)

    table = Table(title="订单执行统计")
    table.add_column("指标")
    table.add_column("数值", justify="right")
    for label, value in rows:
        table.add_row(label, value)
    return table


# 从订单和成交表组装执行统计行。
def order_stats_rows(output_dir: Path) -> list[tuple[str, str]]:
    orders = read_report_csv(output_dir, "orders.csv")
    if orders.empty:
        return [
            ("订单总数", "0"),
            ("有成交订单数", "0"),
            ("已完成订单数", "0"),
            ("已取消订单数", "0"),
            ("已拒绝订单数", "0"),
        ]
    filled_qty = pd.to_numeric(orders["已成交数量"], errors="coerce").fillna(0)
    return [
        ("订单总数", format_int(len(orders))),
        ("有成交订单数", format_int((filled_qty > 0).sum())),
        ("已完成订单数", format_int((orders["订单状态"] == "FILLED").sum())),
        ("已取消订单数", format_int((orders["订单状态"] == "CANCELED").sum())),
        ("已拒绝订单数", format_int((orders["订单状态"] == "REJECTED").sum())),
    ]


# 用持仓表估算运行覆盖天数。
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


# 读取报告 CSV，文件不存在表示本次没有生成这类报告。
def read_report_csv(output_dir: Path, filename: str) -> pd.DataFrame:
    path = output_dir / filename
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


# 报告里所有时间字段统一显示北京时间。
def localize_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    for column in data.columns:
        name = str(column).lower()
        if not is_time_column(str(column)):
            continue
        if pd.api.types.is_numeric_dtype(data[column]) and name.startswith("ts_"):
            values = pd.to_datetime(data[column], unit="ns", utc=True, errors="coerce")
        else:
            values = pd.to_datetime(data[column], utc=True, errors="coerce")
        mask = values.notna()
        if mask.any():
            localized = data[column].astype("object")
            localized.loc[mask] = values[mask].dt.tz_convert(LOCAL_TZ).dt.strftime("%Y-%m-%d %H:%M:%S")
            data[column] = localized
    return data


# 判断列名是否是报告时间字段。
def is_time_column(column: str) -> bool:
    name = column.lower()
    if name in {"time_in_force"}:
        return False
    return (
        name.startswith("ts_")
        or name.endswith("_time")
        or name.endswith("时间")
        or name in {"open_time", "close_time", "bar_time", "local_time", "income_time"}
    )


# 删除整列都没有有效值的列，避免 report 里出现全空字段。
def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    empty = df.isna() | df.astype(str).apply(lambda column: column.str.strip().isin(("", "nan", "None", "NaT")))
    return df.loc[:, ~empty.all(axis=0)]


# 把纳秒时间戳转成北京时间字符串。
def format_timestamp_ns(value) -> str:
    if value is None:
        return ""
    return pd.to_datetime(value, unit="ns", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")


# 终端表格里的数字统一保留两位小数。
def format_number(value, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return ""
    number = round(float(value), 2)
    if number == 0:
        number = 0.0
    text = f"{number:.2f}".rstrip("0").rstrip(".")
    return f"{text}{suffix}"


# 终端表格里的整数不显示小数位。
def format_int(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{int(value)}"


# NT 胜率这类字段是 0-1 小数，终端显示成百分比。
def format_percent(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return format_number(float(value) * 100, "%")


# 从 Money 字符串里提取数字部分。
def money_to_float(value) -> float:
    return float(str(value).replace("_", "").split()[0])


# 从 commissions 列表字符串里汇总手续费数字。
def commissions_to_float(value) -> float:
    text = str(value).strip()
    if text in ("", "nan", "None"):
        return 0.0
    numbers = []
    for part in text.replace("[", "").replace("]", "").replace("'", "").split(","):
        part = part.strip()
        if part:
            numbers.append(float(part.replace("_", "").split()[0]))
    return sum(numbers)
