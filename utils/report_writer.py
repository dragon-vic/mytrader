from __future__ import annotations

import re
import shutil
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from rich.console import Console
from rich.table import Table

from utils.arguments import LIVE_LOG_START_MARKER
from utils.arguments import LIVE_LOG_STOP_MARKER
from utils.arguments import LIVE_RESULT_FILES
from utils.arguments import LOGS_DIR
from utils.arguments import OBSOLETE_REPORT_FILES
from utils.arguments import POSITIONS_FILE
from utils.arguments import REPORT_COLUMNS
from utils.arguments import REPORT_FILES
from utils.arguments import SUMMARY_FILE
from utils.config_loader import ROOT
from utils.report_labels import to_chinese_columns


LOCAL_TZ = ZoneInfo("Asia/Shanghai")
LOG_UTC_PREFIX = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)(?P<rest>\s.*)$")


# 返回当前运行的报告目录。
def run_reports_dir(settings: dict[str, Any], run_type: str) -> Path:
    run_name = settings.get("runtime", {}).get("run_name")
    if run_name:
        return ROOT / settings["project"]["reports_dir"] / run_name
    return ROOT / settings["project"]["reports_dir"] / f"{settings['project']['config_name']}-{run_type}"


# 返回 live 日志目录。
def live_logs_dir() -> Path:
    return ROOT / LOGS_DIR


# 返回本次运行的 NT 原始日志文件名，不带后缀。
def live_raw_log_name(settings: dict[str, Any]) -> str:
    run_name = settings.get("runtime", {}).get("run_name")
    if run_name:
        return f"{run_name}-running"
    return f"{settings['project']['config_name']}-{settings['mode']}-running"


# 返回本次运行的 NT 原始日志路径。
def live_raw_log_path(settings: dict[str, Any]) -> Path:
    return live_logs_dir() / f"{live_raw_log_name(settings)}.log"


# 返回最终清洗日志路径，避免同一分钟重复运行时覆盖旧日志。
def final_live_log_path(settings: dict[str, Any]) -> Path:
    run_name = settings.get("runtime", {}).get("run_name")
    if run_name:
        return live_logs_dir() / f"{run_name}.log"
    end_time = datetime.now(LOCAL_TZ).strftime("%Y%m%d-%H%M")
    return live_logs_dir() / f"{settings['project']['config_name']}-{settings['mode']}-{end_time}.log"


# 每次运行前清空当前 set 的报告目录，避免旧文件混进新结果。
def prepare_report_dir(settings: dict[str, Any], run_type: str) -> Path:
    reports_root = (ROOT / settings["project"]["reports_dir"]).resolve()
    output_dir = run_reports_dir(settings, run_type).resolve()
    output_dir.relative_to(reports_root)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


# 只保留人工看交易结果需要的列。
def report_columns(name: str, df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()
    return df[[column for column in REPORT_COLUMNS[name] if column in df.columns]]


class TraderReportWriter:
    def __init__(self, output_dir: Path, clear_files: tuple[str, ...] = ()) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.clear_files(clear_files)

    # 从配置创建当前运行类型的报告整理器。
    @classmethod
    def from_settings(cls, settings: dict[str, Any], run_type: str):
        clear_files = (*LIVE_RESULT_FILES, *OBSOLETE_REPORT_FILES) if run_type == "live" else ()
        return cls(run_reports_dir(settings, run_type), clear_files=clear_files)

    # 清理本次运行会重新生成或已经废弃的报告文件。
    def clear_files(self, filenames: tuple[str, ...]) -> None:
        for filename in filenames:
            path = self.output_dir / filename
            if path.exists():
                path.unlink()

    # 保存 NT trader 在运行结束后生成的订单和持仓结果。
    def write_final_reports(self, trader, names=("orders", "positions")) -> None:
        self.clear_files(
            (
                "orders.csv",
                "orders_aggregate.csv",
                "fills.csv",
                "positions.csv",
                "positions_aggregate.csv",
                "accounts_aggregate.csv",
                "result.csv",
                "position_events.csv",
                "live_report.csv",
                "live_report_aggregate.csv",
                "summary.md",
                "summary_aggregate.md",
            )
        )

        report_fns = {
            "orders": trader.generate_orders_report,
            "positions": trader.generate_positions_report,
        }
        reports = {}
        for name in names:
            df = self.account_report(trader) if name == "accounts" else report_fns[name]()
            if not df.empty:
                reports[name] = report_columns(name, df)
                if name == "orders":
                    self.write_csv(self.format_orders(reports[name]), REPORT_FILES[name])
                elif name == "accounts":
                    self.write_csv(self.filter_nonzero_accounts(reports[name]), REPORT_FILES[name])
        self.write_position_events(trader)
        self.write_clean_reports(reports.get("positions", pd.DataFrame()))
        self.localize_runtime_csv("account_changes.csv")
        self.localize_runtime_csv("strategy_events.csv")
        self.localize_runtime_csv("funding_fees.csv")

    # 从 NT cache 的全部账户生成最终账户快照。
    def account_report(self, trader) -> pd.DataFrame:
        account = trader._cache.accounts()[0]
        return trader.generate_account_report(account_id=account.id)

    # 写出给人看的 CSV 时，最后一步统一改中文列名。
    def write_csv(self, df: pd.DataFrame, filename: str) -> None:
        data = localize_time_columns(df)
        drop_empty_columns(to_chinese_columns(data)).to_csv(
            self.output_dir / filename,
            index=False,
            encoding="utf-8-sig",
        )

    # 把运行中已经落盘的英文 CSV 在结束时改成中文表头。
    def localize_runtime_csv(self, filename: str) -> None:
        path = self.output_dir / filename
        if path.exists():
            df = pd.read_csv(path)
            self.write_csv(df, filename)

    # 订单表只保留成交时间，时间统一在 write_csv 出口转换。
    def format_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        return orders.copy()

    # 账户最终结果只保留有余额的币。
    def filter_nonzero_accounts(self, accounts: pd.DataFrame) -> pd.DataFrame:
        data = accounts.copy()
        if "currency" in data.columns:
            data = data.groupby("currency", as_index=False, sort=False).tail(1)
        balances = pd.DataFrame(
            {
                column: data[column].map(money_to_float) if column in data.columns else 0.0
                for column in ("total", "free", "locked")
            },
        )
        return data[balances.abs().sum(axis=1) > 0]

    # 从 NT cache 回写完整仓位事件，补齐停止阶段平仓事件。
    def write_position_events(self, trader) -> None:
        rows = []
        for position in trader._cache.positions():
            for index, event in enumerate(position.events):
                row = type(event).to_dict(event)
                row["ts_event"] = pd.to_datetime(event.ts_event, unit="ns", utc=True)
                if index == 0:
                    row["event_type"] = "PositionOpened"
                elif position.ts_closed and event.ts_event == position.ts_closed:
                    row["event_type"] = "PositionClosed"
                else:
                    row["event_type"] = "PositionChanged"
                row["adjustment_type"] = row.get("adjustment_type", "")
                row["quantity_change"] = row.get("quantity_change", "")
                row["pnl_change"] = row.get("pnl_change", "")
                row["reason"] = row.get("reason", "")
                row["event_side"] = row.get("side") or row.get("order_side") or row.get("entry", "")
                row["fill_quantity"] = row.get("last_qty", "")
                row["fill_price"] = row.get("last_px", "")
                rows.append({column: row.get(column) for column in REPORT_COLUMNS["position_events"]})
            for event in position.adjustments:
                row = type(event).to_dict(event)
                row["ts_event"] = pd.to_datetime(event.ts_event, unit="ns", utc=True)
                row["event_type"] = type(event).__name__
                row["event_side"] = ""
                row["fill_quantity"] = ""
                row["fill_price"] = ""
                rows.append({column: row.get(column) for column in REPORT_COLUMNS["position_events"]})
        if rows:
            self.write_csv(pd.DataFrame(rows), "position_events.csv")

    # 把 NT 原始日志行首 UTC 时间改成北京时间，保存后的 live 日志更方便直接阅读。
    def localize_live_log_line(self, line: str) -> str:
        match = LOG_UTC_PREFIX.match(line)
        if match is None:
            return line
        timestamp = pd.Timestamp(match.group("ts")).tz_convert(LOCAL_TZ)
        return f"{timestamp.isoformat()}{match.group('rest')}"

    # 生成更容易看的 positions.csv 和中文 summary.md。
    def write_clean_reports(self, positions: pd.DataFrame) -> None:
        trades = self.build_trades(positions)
        if not trades.empty:
            self.write_csv(trades, POSITIONS_FILE)

    # 按 position 维度合成交易表，并合并 funding 相关字段。
    def build_trades(self, positions: pd.DataFrame) -> pd.DataFrame:
        if positions.empty:
            return pd.DataFrame()

        trades = pd.DataFrame(
            {
                "open_time": pd.to_datetime(positions["ts_opened"], unit="ns", utc=True),
                "close_time": pd.to_datetime(positions["ts_closed"], unit="ns", utc=True),
                "instrument_id": positions["instrument_id"],
                "side": positions["entry"],
                "qty": positions["peak_qty"],
                "avg_open": positions["avg_px_open"],
                "avg_close": positions["avg_px_close"],
                "realized_pnl": positions["realized_pnl"].map(money_to_float),
                "realized_return": pd.to_numeric(positions["realized_return"], errors="coerce"),
                "commissions": positions["commissions"].map(commissions_to_float),
                "duration_min": pd.to_numeric(positions["duration_ns"], errors="coerce") / 60_000_000_000,
                "position_id": positions["position_id"],
            },
        )
        return self.merge_funding_decisions(trades)

    # 把 funding 策略事件里的资金费估算合并到交易表。
    def merge_funding_decisions(self, trades: pd.DataFrame) -> pd.DataFrame:
        decisions_path = self.output_dir / "strategy_events.csv"
        if not decisions_path.exists():
            trades["estimated_funding_income"] = 0.0
            trades["net_with_funding"] = trades["realized_pnl"]
            return trades

        decisions = pd.read_csv(decisions_path)
        opens = decisions[decisions["action"] == "OPEN"].copy()
        if opens.empty:
            trades["estimated_funding_income"] = 0.0
            trades["net_with_funding"] = trades["realized_pnl"]
            return trades

        opens["open_time"] = pd.to_datetime(opens["bar_time"], utc=True)
        trades["open_time_dt"] = pd.to_datetime(trades["open_time"], utc=True)
        merge_cols = ["open_time"]
        left_cols = ["open_time_dt"]
        if "instrument_id" in opens.columns:
            merge_cols.append("instrument_id")
            left_cols.append("instrument_id")
        merged = trades.merge(
            opens[
                [
                    *merge_cols,
                    "funding_time",
                    "funding_rate",
                    "estimated_funding_income",
                    "local_time",
                    "delta_to_funding_ms",
                    "adverse_entry_move",
                    "reason",
                ]
            ],
            left_on=left_cols,
            right_on=merge_cols,
            how="left",
            suffixes=("", "_funding"),
        )
        merged = merged.drop(columns=["open_time_dt", "open_time_funding"], errors="ignore")
        merged["estimated_funding_income"] = pd.to_numeric(
            merged["estimated_funding_income"],
            errors="coerce",
        ).fillna(0.0)
        merged = self.merge_actual_funding_income(merged)
        merged["funding_income"] = merged["actual_funding_income"]
        missing_actual = merged["funding_income"].isna() | (merged["funding_income"] == 0.0)
        merged.loc[missing_actual, "funding_income"] = merged.loc[missing_actual, "estimated_funding_income"]
        merged["net_with_funding"] = merged["realized_pnl"] + merged["funding_income"]
        return merged

    # 把交易所真实 funding fee 到账合并到交易表。
    def merge_actual_funding_income(self, trades: pd.DataFrame) -> pd.DataFrame:
        fees_path = self.output_dir / "funding_fees.csv"
        trades["actual_funding_income"] = 0.0
        trades["funding_income_time"] = ""
        trades["funding_income_delta_ms"] = ""
        if not fees_path.exists():
            return trades

        fees = pd.read_csv(fees_path)
        if fees.empty or "funding_time" not in fees.columns:
            return trades
        fees["funding_time_dt"] = pd.to_datetime(fees["funding_time"], utc=True)
        fees["actual_funding_income"] = pd.to_numeric(fees["income"], errors="coerce").fillna(0.0)
        trades["funding_time_dt"] = pd.to_datetime(trades["funding_time"], utc=True)
        merged = trades.merge(
            fees[
                [
                    "funding_time_dt",
                    "actual_funding_income",
                    "income_time",
                    "delta_ms",
                    "tran_id",
                ]
            ],
            on="funding_time_dt",
            how="left",
            suffixes=("", "_actual"),
        )
        merged["actual_funding_income"] = merged["actual_funding_income_actual"].fillna(
            merged["actual_funding_income"],
        )
        merged["funding_income_time"] = merged["income_time"].fillna("")
        merged["funding_income_delta_ms"] = merged["delta_ms"].fillna("")
        merged["funding_tran_id"] = merged["tran_id"].fillna("")
        return merged.drop(
            columns=["funding_time_dt", "actual_funding_income_actual", "income_time", "delta_ms", "tran_id"],
            errors="ignore",
        )

    # 保留 TradingNode RUNNING 到 STOPPING 之间的 live 日志。
    def write_clean_live_log(
        self,
        settings: dict[str, Any],
        start_marker: str = LIVE_LOG_START_MARKER,
        stop_marker: str = LIVE_LOG_STOP_MARKER,
    ) -> None:
        source = live_raw_log_path(settings)
        target = final_live_log_path(settings)
        if not source.exists():
            return

        lines = source.read_text(encoding="utf-8", errors="replace").splitlines()
        start = 0
        for index, line in enumerate(lines):
            if start_marker in line:
                start = index
                break
        end = len(lines)
        for index, line in enumerate(lines[start:], start=start):
            if stop_marker in line:
                end = index
                break
        localized_lines = [self.localize_live_log_line(line) for line in lines[start:end]]
        target.write_text("\n".join(localized_lines) + "\n", encoding="utf-8")
        source.unlink()


# 保存 NT trader 生成的订单、持仓报告。
def write_trader_reports(trader, settings: dict[str, Any], run_type: str) -> None:
    TraderReportWriter.from_settings(settings, run_type).write_final_reports(trader)


# 生成回测结果数据，报告只落人读的表格文件。
def write_backtest_result(result, settings: dict[str, Any]) -> dict[str, Any]:
    return asdict(result)


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
                ("标的", "交易数", "胜率", "净收益", "平均收益", "最大盈利", "最大亏损", "手续费"),
                instrument_stats_rows(output_dir),
            ),
            ("订单执行统计", ("指标", "数值"), order_stats_rows(output_dir)),
        ],
        output_dir,
    )
    console = Console()
    console.print(build_backtest_overview_table(payload, settings))
    print_common_tables(console, output_dir, elapsed_days)


# 打印 live/testnet 结束摘要。
def print_live_summary(settings: dict[str, Any]) -> None:
    output_dir = run_reports_dir(settings, "live")
    elapsed_days = report_elapsed_days(output_dir)
    write_summary_markdown(
        "运行摘要",
        [
            ("运行总览", ("指标", "数值"), live_overview_rows(settings, output_dir)),
            ("交易统计", ("指标", "数值"), trade_stats_rows(output_dir, elapsed_days)),
            (
                "标的统计",
                ("标的", "交易数", "胜率", "净收益", "平均收益", "最大盈利", "最大亏损", "手续费"),
                instrument_stats_rows(output_dir),
            ),
            ("订单执行统计", ("指标", "数值"), order_stats_rows(output_dir)),
        ],
        output_dir,
    )
    console = Console()
    console.print(build_live_overview_table(settings, output_dir))
    print_common_tables(console, output_dir, elapsed_days)


# 打印交易、标的和订单三张公共表。
def print_common_tables(console: Console, output_dir: Path, elapsed_days: float) -> None:
    for table in (
        build_trade_stats_table(output_dir, elapsed_days),
        build_instrument_stats_table(output_dir),
        build_order_stats_table(output_dir),
    ):
        if table is not None:
            console.print(table)


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
    markets = settings.get("markets") or [settings["market"]]
    symbols = ", ".join(market["instrument_symbol"] for market in markets)
    timeframes = ", ".join(sorted({market["timeframe"] for market in markets}))
    stats_pnls = payload.get("stats_pnls", {})
    currency = next(iter(stats_pnls), "")
    pnl_stats = stats_pnls.get(currency, {})
    return_stats = payload.get("stats_returns", {})

    return [
        ("配置名", settings["project"]["config_name"]),
        ("策略名", settings["strategy"]["name"]),
        ("标的", symbols),
        ("K线周期", timeframes),
        ("回测开始", format_timestamp_ns(payload.get("backtest_start"))),
        ("回测结束", format_timestamp_ns(payload.get("backtest_end"))),
        ("回测天数", format_number(float(payload.get("elapsed_time") or 0) / 86400)),
        ("初始资金", settings["backtest"]["starting_balance"]),
        ("总收益", format_number(pnl_stats.get("PnL (total)"), currency)),
        ("总收益率", format_number(pnl_stats.get("PnL% (total)"), "%")),
        ("胜率", format_percent(pnl_stats.get("Win Rate"))),
        ("最大盈利单", format_number(pnl_stats.get("Max Winner"), currency)),
        ("最大亏损单", format_number(pnl_stats.get("Max Loser"), currency)),
        ("期望收益", format_number(pnl_stats.get("Expectancy"), currency)),
        ("盈利因子", format_number(return_stats.get("Profit Factor"))),
        ("Sharpe", format_number(return_stats.get("Sharpe Ratio (252 days)"))),
        ("Sortino", format_number(return_stats.get("Sortino Ratio (252 days)"))),
        ("迭代次数", format_int(payload.get("iterations"))),
        ("事件数", format_int(payload.get("total_events"))),
        ("订单数", format_int(payload.get("total_orders"))),
        ("持仓数", format_int(payload.get("total_positions"))),
    ]


# 用 live/testnet 运行结果组装总体概览表。
def build_live_overview_table(settings: dict[str, Any], output_dir: Path) -> Table:
    table = Table(title="运行总览")
    table.add_column("指标")
    table.add_column("数值", justify="right")
    for label, value in live_overview_rows(settings, output_dir):
        table.add_row(label, value)
    return table


# 用 live/testnet 已落盘报告组装总体概览行。
def live_overview_rows(settings: dict[str, Any], output_dir: Path) -> list[tuple[str, str]]:
    markets = settings.get("markets") or [settings["market"]]
    symbols = ", ".join(market["instrument_symbol"] for market in markets)
    orders = read_report_csv(output_dir, "orders.csv")
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    return [
        ("配置名", settings["project"]["config_name"]),
        ("策略名", settings["strategy"]["name"]),
        ("运行模式", settings["mode"]),
        ("报告目录", output_dir.name),
        ("标的", symbols),
        ("生成时间", datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")),
        ("订单数", format_int(len(orders))),
        ("持仓数", format_int(len(positions))),
    ]


# 从持仓汇总表组装交易统计表。
def build_trade_stats_table(output_dir: Path, elapsed_days: float) -> Table | None:
    rows = trade_stats_rows(output_dir, elapsed_days)
    if not rows:
        return None

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
        return []

    pnl = positions["已实现盈亏"].map(money_to_float)
    fees = positions["手续费合计"].map(commissions_to_float)
    duration_min = pd.to_numeric(positions["持仓分钟"], errors="coerce")
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    gross_profit = wins.sum()
    return [
        ("已完成交易数", format_int(len(positions))),
        ("多单数量", format_int((positions["方向"] == "BUY").sum())),
        ("空单数量", format_int((positions["方向"] == "SELL").sum())),
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
    if not rows:
        return None

    table = Table(title="标的统计")
    for column, justify in (
        ("标的", "left"),
        ("交易数", "right"),
        ("胜率", "right"),
        ("净收益", "right"),
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
        return []

    data = positions.copy()
    data["净收益"] = data["已实现盈亏"].map(money_to_float)
    data["手续费"] = data["手续费合计"].map(commissions_to_float)
    rows = []
    for instrument, group in data.groupby("标的"):
        pnl = group["净收益"]
        rows.append((
            str(instrument),
            format_int(len(group)),
            format_percent((pnl > 0).mean()),
            format_number(pnl.sum()),
            format_number(pnl.mean()),
            format_number(pnl.max()),
            format_number(pnl.min()),
            format_number(group["手续费"].sum()),
        ))
    return rows


# 从订单和成交表组装执行统计表。
def build_order_stats_table(output_dir: Path) -> Table | None:
    rows = order_stats_rows(output_dir)
    if not rows:
        return None

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
        return []
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
