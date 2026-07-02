from __future__ import annotations

import json
import re
import time
from dataclasses import asdict
from datetime import datetime
from math import sqrt
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from rich.console import Console
from rich.table import Table

from utils.arguments import POSITIONS_FILE
from utils.arguments import REPORT_COLUMNS
from utils.arguments import REPORT_FILES
from utils.arguments import SUMMARY_FILE
from utils.funding_fees import add_funding_income
from utils.report_labels import to_chinese_columns


LOCAL_TZ = ZoneInfo("Asia/Shanghai")


# 返回当前策略自己的报告根目录。
def reports_root(settings: dict[str, Any]) -> Path:
    root = Path(settings["reports"]["root"])
    if root.is_absolute():
        return root
    return Path(settings["project"]["strategy_dir"]) / root


# 返回当前运行的报告目录。
def run_reports_dir(settings: dict[str, Any], run_type: str) -> Path:
    report_dir_name = settings.get("runtime", {}).get("report_dir_name")
    if report_dir_name:
        return reports_root(settings) / report_dir_name
    run_name = settings.get("runtime", {}).get("run_name")
    if run_name:
        return reports_root(settings) / run_name
    return reports_root(settings) / f"{run_type}-{settings['project']['config_name']}"


# 返回本次运行开始时间的 UTC ns，用于剔除 reconciliation 带入的旧订单。
def runtime_start_ns(settings: dict[str, Any]) -> int | None:
    started_at = settings.get("runtime", {}).get("started_at")
    if not started_at:
        return None
    dt = datetime.strptime(str(started_at), "%Y%m%d%H%M%S").replace(tzinfo=LOCAL_TZ)
    return pd.Timestamp(dt).value


def report_time(values: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(values):
        return pd.to_datetime(values, unit="ns", utc=True, errors="coerce")
    parsed = pd.to_datetime(values, errors="coerce")
    if getattr(parsed.dt, "tz", None) is None:
        return parsed.dt.tz_localize(LOCAL_TZ).dt.tz_convert("UTC")
    return parsed.dt.tz_convert("UTC")


# 返回 NT LoggingConfig 需要的文件日志参数。
def log_file_settings(settings: dict[str, Any], run_type: str) -> dict[str, Any]:
    logging = settings["logging"]
    return {
        "log_level_file": logging["log_level_file"],
        "log_directory": str(run_reports_dir(settings, run_type)),
        "log_file_name": logging["log_file_name"],
        "log_file_format": logging["log_file_format"],
        "log_file_max_size": logging["log_file_max_size"],
        "log_file_max_backup_count": logging["log_file_max_backup_count"],
        "clear_log_file": bool(logging["clear_log_file"]),
    }

# 创建当前运行的报告目录；每次运行使用新目录，不再清理旧文件。
def prepare_report_dir(settings: dict[str, Any], run_type: str) -> Path:
    root = reports_root(settings).resolve()
    output_dir = run_reports_dir(settings, run_type).resolve()
    # Windows 多进程下 resolve() 可能混用 \\?\ 扩展路径，先统一再校验目录边界。
    checked_root = Path(str(root).removeprefix("\\\\?\\"))
    checked_output = Path(str(output_dir).removeprefix("\\\\?\\"))
    checked_output.relative_to(checked_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


# 只保留人工看交易结果需要的列。
def report_columns(name: str, df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()
    return df[[column for column in REPORT_COLUMNS[name] if column in df.columns]]


class TraderReportWriter:
    def __init__(
        self,
        output_dir: Path,
        enabled: bool = True,
        run_start_ns: int | None = None,
        settings: dict[str, Any] | None = None,
    ) -> None:
        self.output_dir = output_dir
        self.enabled = enabled
        self.run_start_ns = run_start_ns
        self.settings = settings
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # 从配置创建当前运行类型的报告整理器。
    @classmethod
    def from_settings(cls, settings: dict[str, Any], run_type: str):
        return cls(
            run_reports_dir(settings, run_type),
            bool(settings["reports"]["enabled"]),
            runtime_start_ns(settings) if run_type in {"live", "testnet"} else None,
            settings,
        )

    # 保存 NT trader 在运行结束后生成的订单，并用订单流重建仓位表。
    def write_final_reports(self, trader, names=("orders",)) -> None:
        if not self.enabled:
            return
        report_fns = {
            "orders": trader.generate_orders_report,
        }
        reports = {}
        for name in names:
            df = self.account_report(trader) if name == "accounts" else report_fns[name]()
            if not df.empty:
                reports[name] = self.filter_current_run(name, report_columns(name, df))
                if name == "orders":
                    self.write_csv(self.format_orders(reports[name]), REPORT_FILES[name])
                elif name == "accounts":
                    self.write_csv(self.filter_nonzero_accounts(reports[name]), REPORT_FILES[name])
        self.write_positions_from_orders(reports.get("orders", pd.DataFrame()))
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

    # 最终报告只保留本次 node 启动后的交易记录。
    def filter_current_run(self, name: str, df: pd.DataFrame) -> pd.DataFrame:
        if self.run_start_ns is None or df.empty:
            return df
        column = {"orders": "ts_last"}.get(name)
        if column is None or column not in df.columns:
            return df
        ts = report_time(df[column])
        start = pd.to_datetime(self.run_start_ns, unit="ns", utc=True)
        return df[ts >= start].copy()

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

    # 从订单流重建完成仓位，避开交易所固定 position_id 覆盖历史的问题。
    def write_positions_from_orders(self, orders: pd.DataFrame) -> None:
        positions = self.build_positions_from_orders(orders)
        if not positions.empty:
            if self.settings is not None and self.settings["mode"] in {"live", "testnet"}:
                positions = add_funding_income(positions, self.settings)
            self.write_csv(positions, POSITIONS_FILE)

    # 同一标的和仓位 ID 的连续同向成交合成一个净仓位，归零时写一行。
    def build_positions_from_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        if orders.empty:
            return pd.DataFrame()

        data = orders.copy()
        data["filled_qty_num"] = pd.to_numeric(data["filled_qty"], errors="coerce").fillna(0.0)
        data["avg_px_num"] = pd.to_numeric(data["avg_px"], errors="coerce")
        data["fee_num"] = data["commissions"].map(commissions_to_float)
        data["order_time"] = report_time(data["ts_last"])
        data["source_seq"] = range(len(data))
        data["order_seq"] = [
            sequence if sequence is not None else source
            for sequence, source in zip(data["client_order_id"].map(order_sequence), data["source_seq"])
        ]
        data = data[(data["filled_qty_num"] > 0) & data["avg_px_num"].notna() & data["order_time"].notna()]
        if data.empty:
            return pd.DataFrame()

        rows = []
        keys = ["instrument_id", "position_id"]
        for (_, _), group in data.sort_values(["order_time", "order_seq"]).groupby(keys, dropna=False, sort=False):
            state = None
            for order in group.sort_values(["order_time", "order_seq"]).to_dict("records"):
                state = self.apply_order_to_position_state(state, order, rows)
            # 未平仓不写入完成仓位表；summary 只统计完成仓位。
        return pd.DataFrame(rows)

    # 将一笔订单应用到当前净仓位；如果反手，会拆成平旧仓和开新仓。
    def apply_order_to_position_state(self, state: dict[str, Any] | None, order: dict[str, Any], rows: list[dict[str, Any]]):
        side = str(order["side"])
        qty = float(order["filled_qty_num"])
        px = float(order["avg_px_num"])
        fee = float(order["fee_num"])
        remaining = qty
        remaining_fee = fee
        direction = 1 if side == "BUY" else -1

        while remaining > 1e-12:
            if state is None:
                return new_position_state(order, side, remaining, px, remaining_fee)

            state_dir = 1 if state["side"] == "BUY" else -1
            if state_dir == direction:
                add_to_position_state(state, order, remaining, px, remaining_fee)
                return state

            close_qty = min(state["qty"], remaining)
            close_fee = remaining_fee * close_qty / remaining if remaining else 0.0
            close_position_state(state, order, close_qty, px, close_fee)
            remaining -= close_qty
            remaining_fee -= close_fee
            if state["qty"] <= 1e-12:
                rows.append(finished_position_row(state, order))
                state = None
        return state


# 从客户端订单 ID 取递增序号；没有序号时保持原始相对顺序。
def order_sequence(value: Any) -> int | None:
    match = re.search(r"(\d+)$", str(value))
    return int(match.group(1)) if match else None


# 创建一个由订单流驱动的净仓位状态。
def new_position_state(order: dict[str, Any], side: str, qty: float, px: float, fee: float) -> dict[str, Any]:
    return {
        "open_time": order["order_time"],
        "close_time": None,
        "instrument_id": order["instrument_id"],
        "side": side,
        "qty": qty,
        "peak_qty": qty,
        "open_notional": qty * px,
        "close_notional": 0.0,
        "closed_open_notional": 0.0,
        "closed_qty": 0.0,
        "gross_pnl": 0.0,
        "fees": fee,
        "position_id": order.get("position_id", ""),
        "opening_order_id": order.get("client_order_id", ""),
        "closing_order_id": "",
    }


# 同向成交视为对当前净仓位加仓。
def add_to_position_state(state: dict[str, Any], order: dict[str, Any], qty: float, px: float, fee: float) -> None:
    state["qty"] += qty
    state["peak_qty"] = max(state["peak_qty"], state["qty"])
    state["open_notional"] += qty * px
    state["fees"] += fee


# 反向成交视为平仓，直到净仓位归零或订单剩余部分反手开新仓。
def close_position_state(state: dict[str, Any], order: dict[str, Any], qty: float, px: float, fee: float) -> None:
    avg_open = state["open_notional"] / state["qty"]
    gross = (px - avg_open) * qty if state["side"] == "BUY" else (avg_open - px) * qty
    state["qty"] -= qty
    state["open_notional"] = avg_open * state["qty"]
    state["close_notional"] += qty * px
    state["closed_open_notional"] += qty * avg_open
    state["closed_qty"] += qty
    state["gross_pnl"] += gross
    state["fees"] += fee
    state["close_time"] = order["order_time"]
    state["closing_order_id"] = order.get("client_order_id", "")


# 把已归零的净仓位转成 positions.csv 的一行。
def finished_position_row(state: dict[str, Any], order: dict[str, Any]) -> dict[str, Any]:
    avg_close = state["close_notional"] / state["closed_qty"] if state["closed_qty"] else 0.0
    avg_open = state["closed_open_notional"] / state["closed_qty"] if state["closed_qty"] else 0.0
    close_time = state["close_time"] or order["order_time"]
    duration = (close_time - state["open_time"]).total_seconds() / 60
    return {
        "open_time": state["open_time"],
        "close_time": close_time,
        "instrument_id": state["instrument_id"],
        "side": state["side"],
        "qty": state["peak_qty"],
        "avg_open": avg_open,
        "avg_close": avg_close,
        "realized_pnl": state["gross_pnl"] - state["fees"],
        "realized_return": state["gross_pnl"] / state["closed_open_notional"] if state["closed_open_notional"] else 0.0,
        "commissions": state["fees"],
        "duration_min": duration,
        "position_id": state["position_id"],
        "opening_order_id": state["opening_order_id"],
        "closing_order_id": state["closing_order_id"],
    }


# 保存 NT trader 生成的订单、持仓报告。
def write_trader_reports(trader, settings: dict[str, Any], run_type: str) -> None:
    TraderReportWriter.from_settings(settings, run_type).write_final_reports(trader)


# 生成回测结果数据，报告只落人读的表格文件。
def write_backtest_result(result, settings: dict[str, Any]) -> dict[str, Any]:
    return asdict(result)


# 打印回测核心摘要。
def print_backtest_summary(payload: dict[str, Any], settings: dict[str, Any]) -> float:
    started = time.perf_counter()
    output_dir = run_reports_dir(settings, "backtest")
    elapsed_days = float(payload.get("elapsed_time") or 0) / 86400
    sections = [
        ("回测总览", ("指标", "数值"), backtest_overview_rows(payload, settings)),
        ("参数", ("参数", "数值"), parameter_rows(settings)),
        ("仓位统计", ("指标", "数值"), trade_stats_rows(output_dir, elapsed_days)),
        (
            "标的统计",
            ("标的", "仓位数", "平均持仓数量", "平均最大持仓", "胜率", "净收益", "收益率", "平均收益", "最大盈利", "最大亏损", "手续费"),
            instrument_stats_rows(output_dir, elapsed_days),
        ),
        ("订单执行统计", ("指标", "数值"), order_stats_rows(output_dir, elapsed_days)),
    ]
    write_summary_json("回测摘要", sections, output_dir)
    console = Console(record=True)
    print_summary_tables(console, sections)
    (output_dir / "summary.txt").write_text(console.export_text(styles=False), encoding="utf-8")
    return time.perf_counter() - started


# 打印 live/testnet 结束摘要。
def print_live_summary(settings: dict[str, Any]) -> None:
    if not settings["reports"]["enabled"]:
        return
    output_dir = run_reports_dir(settings, "live")
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
    console = Console()
    print_summary_tables(console, sections)


# 保存结构化摘要，终端展示时再渲染成 Rich 表格。
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
    (output_dir / SUMMARY_FILE).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# 总览、仓位统计、订单统计并排显示；标的统计较宽，单独放下面。
def print_summary_tables(console: Console, sections: list[tuple[str, tuple[str, ...], list[tuple[Any, ...]]]]) -> None:
    tables = [summary_table(title, headers, rows) for title, headers, rows in sections if rows]
    top = [table for table in tables if table.title != "标的统计"]
    instruments = [table for table in tables if table.title == "标的统计"]
    if top:
        grid = Table.grid(expand=True)
        for _ in top:
            grid.add_column(ratio=1)
        grid.add_row(*top)
        console.print(grid)
    for table in instruments:
        console.print(table)


def summary_table(title: str, headers: tuple[str, ...], rows: list[tuple[Any, ...]]) -> Table:
    table = Table(title=title)
    for index, header in enumerate(headers):
        table.add_column(str(header), justify="left" if index == 0 else "right")
    for row in rows:
        table.add_row(*(str(value) for value in row))
    return table


# 回测总览的收益使用订单重建口径；NT multi-venue stats_pnls 只保留最后一个 venue。
def backtest_overview_rows(payload: dict[str, Any], settings: dict[str, Any]) -> list[tuple[str, str]]:
    output_dir = run_reports_dir(settings, "backtest")
    data_interval = backtest_data_interval(settings)
    starting_balance = starting_balance_value(settings)
    pnl = net_pnl(output_dir)
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
    if "bars" in types or not labels:
        labels.extend(sorted({market["timeframe"] for market in settings["markets"] if "timeframe" in market}))
    return "/".join(labels)


# 用 live/testnet 已落盘报告组装总体概览行。
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


# 总览里按 yaml 写法显示标的；markets: all 不展开成长列表。
def market_symbols(settings: dict[str, Any], run_type: str) -> str:
    if settings.get(run_type, {}).get("markets_all") or settings.get("markets_all"):
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


# 从订单重建的持仓汇总表组装仓位统计行。
def trade_stats_rows(output_dir: Path, elapsed_days: float) -> list[tuple[str, str]]:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    avg_qty = average_position_qty(output_dir, elapsed_days)
    if positions.empty:
        return [
            ("完成仓位数", "0"),
            ("胜率", "0%"),
            ("净收益", "0"),
            ("资金费收入", "0"),
            ("平均仓位收益", "0"),
            ("总手续费", "0"),
            ("平均持仓数量", format_number(avg_qty)),
            ("平均持仓分钟", "0"),
            ("最长持仓分钟", "0"),
        ]

    pnl = positions["已实现盈亏"].map(money_to_float)
    funding = report_funding_income(positions)
    total_pnl = pnl + funding
    fees = positions["手续费合计"].map(commissions_to_float)
    duration_min = pd.to_numeric(positions["持仓分钟"], errors="coerce")
    wins = total_pnl[total_pnl > 0]
    losses = total_pnl[total_pnl < 0]
    return [
        ("净收益", format_number(net_pnl(output_dir))),
        ("总手续费", format_number(fees.sum())),
        ("完成仓位数", format_int(len(positions))),
        ("胜率", format_percent((total_pnl > 0).mean())),
        ("资金费收入", format_number(funding.sum())),
        ("平均仓位收益", format_number(total_pnl.mean())),
        ("仓位收益中位数", format_number(total_pnl.median())),
        ("盈利仓位平均收益", format_number(wins.mean())),
        ("亏损仓位平均亏损", format_number(losses.mean())),
        ("最大盈利", format_number(total_pnl.max())),
        ("最大亏损", format_number(total_pnl.min())),
        ("平均持仓数量", format_number(avg_qty)),
        ("平均持仓分钟", format_number(duration_min.mean())),
        ("最长持仓分钟", format_number(duration_min.max())),
    ]


# 用订单成交事件重建每个标的净仓位，并按持续时间计算平均持仓数量。
def average_position_qty(output_dir: Path, elapsed_days: float) -> float:
    return sum(average_position_qty_by_instrument(output_dir, elapsed_days).values())


def average_position_qty_by_instrument(output_dir: Path, elapsed_days: float) -> dict[str, float]:
    orders = read_report_csv(output_dir, "orders.csv")
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
    weighted_qty_minutes: dict[str, float] = {}
    prev_time = data.iloc[0]["order_time"]
    for row in data.itertuples(index=False):
        order_time = row.order_time
        elapsed_min = max((order_time - prev_time).total_seconds() / 60, 0.0)
        for instrument, qty in positions.items():
            weighted_qty_minutes[instrument] = weighted_qty_minutes.get(instrument, 0.0) + abs(qty) * elapsed_min

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
    event_span_min = (data.iloc[-1]["order_time"] - data.iloc[0]["order_time"]).total_seconds() / 60
    if total_minutes > event_span_min:
        tail_min = total_minutes - event_span_min
        for instrument, qty in positions.items():
            weighted_qty_minutes[instrument] = weighted_qty_minutes.get(instrument, 0.0) + abs(qty) * tail_min
    return {
        instrument: weighted_qty / total_minutes
        for instrument, weighted_qty in weighted_qty_minutes.items()
    }


# 当前通用报告按订单重建出的完成仓位统计收益。
def net_pnl(output_dir: Path) -> float:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    if positions.empty:
        return 0.0
    return (positions["已实现盈亏"].map(money_to_float) + report_funding_income(positions)).sum()


# 回测配置里的初始资金是 Money 字符串，例如 "100000 USDT"。
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
    for venue in settings["backtest"].get("venues", []):
        values.extend(venue.get("starting_balances") or [venue["starting_balance"]])
    if values:
        return values
    return [settings["backtest"]["venue_account"]["starting_balance"]]


def money_currency(value: Any) -> str:
    parts = str(value).split()
    return parts[1] if len(parts) > 1 else ""


# 从完成仓位重算收益指标，避免 multi-venue 回测直接使用 NT 单 venue stats。
def report_return_stats(output_dir: Path, starting_balance: float) -> dict[str, float]:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    if positions.empty or starting_balance == 0:
        return {"profit_factor": 0.0, "sharpe": 0.0, "sortino": 0.0}
    pnl = positions["已实现盈亏"].map(money_to_float) + report_funding_income(positions)
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
    sortino = daily.mean() / downside_std * sqrt(252) if len(downside) > 1 and downside_std and not pd.isna(downside_std) else 0.0
    return {"profit_factor": profit_factor, "sharpe": sharpe, "sortino": sortino}


# positions.csv 可能来自旧报告；没有资金费列时按 0 处理。
def report_funding_income(positions: pd.DataFrame) -> pd.Series:
    if "资金费收入" not in positions.columns:
        return pd.Series(0.0, index=positions.index)
    return pd.to_numeric(positions["资金费收入"], errors="coerce").fillna(0.0)


# 从持仓汇总表按标的聚合统计行。
def instrument_stats_rows(output_dir: Path, elapsed_days: float) -> list[tuple[str, ...]]:
    positions = read_report_csv(output_dir, POSITIONS_FILE)
    if positions.empty:
        return [("无成交", "0", "0", "0", "0%", "0", "0%", "0", "0", "0", "0")]

    data = positions.copy()
    data["净收益"] = data["已实现盈亏"].map(money_to_float) + report_funding_income(data)
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


# 从订单和成交表组装执行统计行。
def order_stats_rows(output_dir: Path, elapsed_days: float = 0.0) -> list[tuple[str, str]]:
    orders = read_report_csv(output_dir, "orders.csv")
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
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


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
