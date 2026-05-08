from __future__ import annotations

import json
import shutil
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd
from rich.console import Console
from rich.table import Table

from utils.config_loader import ROOT
from utils.report_labels import to_chinese_columns


REPORT_COLUMNS = {
    "orders": [
        "ts_init",
        "ts_last",
        "instrument_id",
        "side",
        "quantity",
        "filled_qty",
        "avg_px",
        "commissions",
        "status",
        "position_id",
        "client_order_id",
    ],
    "fills": [
        "ts_event",
        "instrument_id",
        "order_side",
        "last_qty",
        "last_px",
        "commission",
        "position_id",
        "client_order_id",
    ],
    "positions": [
        "ts_opened",
        "ts_closed",
        "instrument_id",
        "entry",
        "quantity",
        "peak_qty",
        "avg_px_open",
        "avg_px_close",
        "realized_pnl",
        "realized_return",
        "commissions",
        "duration_ns",
        "position_id",
        "opening_order_id",
        "closing_order_id",
    ],
    "position_events": [
        "ts_event",
        "instrument_id",
        "event_side",
        "fill_quantity",
        "fill_price",
        "realized_pnl",
        "adjustment_type",
        "quantity_change",
        "pnl_change",
        "reason",
        "event_type",
        "account_id",
        "strategy_id",
        "position_id",
    ],
}

REPORT_FILES = {
    "orders": "orders_aggregate.csv",
    "fills": "fills.csv",
    "positions": "positions_aggregate.csv",
}

LIVE_REPORT_FILE = "live_report_aggregate.csv"
SUMMARY_FILE = "summary_aggregate.md"

LIVE_RESULT_FILES = (
    "fills.csv",
    "account_changes.csv",
    "position_events.csv",
    "orders.csv",
    "orders_aggregate.csv",
    "positions.csv",
    "positions_aggregate.csv",
    "live_report.csv",
    "live_report_aggregate.csv",
    "summary.md",
    "summary_aggregate.md",
    "live.log",
    "live_raw.log",
)

OBSOLETE_REPORT_FILES = (
    "account_states.csv",
    "trades.csv",
    "summary.csv",
    "fills_clean.csv",
    "funding_decisions.csv",
)

EMPTY_SUMMARY = {
    "trades": 0,
    "win_rate": 0.0,
    "realized_pnl": 0.0,
    "estimated_funding_income": 0.0,
    "actual_funding_income": 0.0,
    "net_with_funding": 0.0,
    "avg_trade_net": 0.0,
    "best_trade_net": 0.0,
    "worst_trade_net": 0.0,
    "gross_profit": 0.0,
    "gross_loss": 0.0,
    "profit_factor": "",
    "total_commissions": 0.0,
    "avg_duration_min": 0.0,
}

SUMMARY_LABELS = {
    "trades": "交易次数",
    "win_rate": "胜率",
    "realized_pnl": "已实现盈亏",
    "estimated_funding_income": "预估资金费收入",
    "actual_funding_income": "实际资金费收入",
    "net_with_funding": "含资金费净收益",
    "avg_trade_net": "单笔平均净收益",
    "best_trade_net": "最佳单笔净收益",
    "worst_trade_net": "最差单笔净收益",
    "gross_profit": "盈利交易合计",
    "gross_loss": "亏损交易合计",
    "profit_factor": "盈利因子",
    "total_commissions": "总手续费",
    "avg_duration_min": "平均持仓分钟",
}


# 返回当前 set 在 backtest/live 下的报告目录。
def run_reports_dir(settings: dict[str, Any], run_type: str) -> Path:
    return ROOT / settings["project"]["reports_dir"] / run_type / settings["project"]["config_name"]


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

    # 保存 NT trader 在运行结束后生成的订单、成交和持仓报告。
    def write_final_reports(self, trader, names=("orders", "fills", "positions")) -> None:
        self.clear_files(
            (
                "orders.csv",
                "orders_aggregate.csv",
                "fills.csv",
                "positions.csv",
                "positions_aggregate.csv",
                "position_events.csv",
                "live_report.csv",
                "live_report_aggregate.csv",
                "summary.md",
                "summary_aggregate.md",
            )
        )

        report_fns = {
            "orders": trader.generate_orders_report,
            "fills": trader.generate_fills_report,
            "positions": trader.generate_positions_report,
        }
        reports = {}
        for name in names:
            df = report_fns[name]()
            if not df.empty:
                reports[name] = report_columns(name, df)
                self.write_csv(reports[name], REPORT_FILES[name])
        self.write_position_events(trader)
        self.write_clean_reports(reports.get("positions", pd.DataFrame()))
        self.localize_runtime_csv("account_changes.csv")
        self.localize_runtime_csv("strategy_events.csv")
        self.localize_runtime_csv("funding_fees.csv")

    # 写出给人看的 CSV 时，最后一步统一改中文列名。
    def write_csv(self, df: pd.DataFrame, filename: str) -> None:
        to_chinese_columns(df).to_csv(self.output_dir / filename, index=False, encoding="utf-8-sig")

    # 把运行中已经落盘的英文 CSV 在结束时改成中文表头。
    def localize_runtime_csv(self, filename: str) -> None:
        path = self.output_dir / filename
        if path.exists():
            df = pd.read_csv(path)
            self.write_csv(df, filename)

    # 从 NT cache 回写完整仓位事件，补齐停止阶段平仓事件。
    def write_position_events(self, trader) -> None:
        rows = []
        for position in trader._cache.positions():
            for event in [*position.events, *position.adjustments]:
                row = type(event).to_dict(event)
                row["ts_event"] = pd.to_datetime(row["ts_event"], unit="ns", utc=True)
                row["event_type"] = row.get("type", type(event).__name__)
                row["adjustment_type"] = row.get("adjustment_type", "")
                row["quantity_change"] = row.get("quantity_change", "")
                row["pnl_change"] = row.get("pnl_change", "")
                row["reason"] = row.get("reason", "")
                row["event_side"] = row.get("side") or row.get("order_side") or row.get("entry", "")
                row["fill_quantity"] = row.get("last_qty", "")
                row["fill_price"] = row.get("last_px", "")
                rows.append({column: row.get(column) for column in REPORT_COLUMNS["position_events"]})
        if rows:
            self.write_csv(pd.DataFrame(rows), "position_events.csv")

    # 生成更容易看的 live_report 和中文 summary.md。
    def write_clean_reports(self, positions: pd.DataFrame) -> None:
        trades = self.build_trades(positions)
        if not trades.empty:
            self.write_csv(trades, LIVE_REPORT_FILE)
        summary = self.build_summary(trades)
        self.write_summary_markdown(summary)

    # 按 position 维度合成交易表，并合并 funding 相关字段。
    def build_trades(self, positions: pd.DataFrame) -> pd.DataFrame:
        if positions.empty:
            return pd.DataFrame()

        trades = pd.DataFrame(
            {
                "open_time": positions["ts_opened"],
                "close_time": positions["ts_closed"],
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
        merged = trades.merge(
            opens[
                [
                    "open_time",
                    "funding_time",
                    "funding_rate",
                    "estimated_funding_income",
                    "local_time",
                    "delta_to_funding_ms",
                    "adverse_entry_move",
                    "reason",
                ]
            ],
            left_on="open_time_dt",
            right_on="open_time",
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

    # 计算核心指标，作为 summary.md 的数据源。
    def build_summary(self, trades: pd.DataFrame) -> pd.DataFrame:
        if trades.empty:
            return pd.DataFrame([EMPTY_SUMMARY.copy()])

        net_col = "net_with_funding" if "net_with_funding" in trades.columns else "realized_pnl"
        gross_profit = trades.loc[trades[net_col] > 0, net_col].sum()
        gross_loss = trades.loc[trades[net_col] < 0, net_col].sum()
        summary = {
            "trades": len(trades),
            "win_rate": (trades[net_col] > 0).mean(),
            "realized_pnl": trades["realized_pnl"].sum(),
            "estimated_funding_income": trades.get("estimated_funding_income", pd.Series([0.0])).sum(),
            "actual_funding_income": trades.get("actual_funding_income", pd.Series([0.0])).sum(),
            "net_with_funding": trades[net_col].sum(),
            "avg_trade_net": trades[net_col].mean(),
            "best_trade_net": trades[net_col].max(),
            "worst_trade_net": trades[net_col].min(),
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": gross_profit / abs(gross_loss) if gross_loss < 0 else "",
            "total_commissions": trades["commissions"].sum(),
            "avg_duration_min": trades["duration_min"].mean(),
        }
        return pd.DataFrame([summary])

    # 输出中文 markdown 摘要。
    def write_summary_markdown(self, summary: pd.DataFrame) -> None:
        row = summary.iloc[0].to_dict()
        lines = ["# 交易运行摘要", ""]
        for key, label in SUMMARY_LABELS.items():
            lines.append(f"- {label}: {row.get(key, '')}")
        (self.output_dir / SUMMARY_FILE).write_text("\n".join(lines) + "\n", encoding="utf-8-sig")

    # 从第一条市场数据标记开始保留 live 日志。
    def write_clean_live_log(self, marker: str = "FIRST_MARKET_DATA") -> None:
        source = self.output_dir / "live_raw.log"
        target = self.output_dir / "live.log"
        if not source.exists():
            return

        lines = source.read_text(encoding="utf-8", errors="replace").splitlines()
        start = 0
        for index, line in enumerate(lines):
            if marker in line:
                start = index
                break
        target.write_text("\n".join(lines[start:]) + "\n", encoding="utf-8")


# 保存 NT trader 生成的订单、持仓报告。
def write_trader_reports(trader, settings: dict[str, Any], run_type: str) -> None:
    TraderReportWriter.from_settings(settings, run_type).write_final_reports(trader)


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
