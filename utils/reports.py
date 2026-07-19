from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from utils.constants import LOCAL_TZ
from utils.constants import ORDERS_FILE
from utils.constants import ORDER_COLUMNS
from utils.constants import POSITIONS_FILE


COLUMN_LABELS = {
    "ts_last": "成交时间",
    "strategy_id": "策略ID",
    "account_id": "账户ID",
    "instrument_id": "标的",
    "side": "方向",
    "quantity": "数量",
    "filled_qty": "已成交数量",
    "avg_px": "平均成交价",
    "commissions": "手续费合计",
    "status": "订单状态",
    "client_order_id": "客户端订单ID",
    "position_id": "仓位ID",
    "open_time": "开仓时间",
    "close_time": "平仓时间",
    "qty": "数量",
    "avg_open": "开仓均价",
    "avg_close": "平仓均价",
    "realized_pnl": "已实现盈亏",
    "realized_return": "已实现收益率",
    "duration_min": "持仓分钟",
    "opening_order_id": "开仓订单ID",
    "closing_order_id": "平仓订单ID",
}


def report_time(values: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(values):
        return pd.to_datetime(values, unit="ns", utc=True, errors="coerce")
    parsed = pd.to_datetime(values, errors="coerce")
    if getattr(parsed.dt, "tz", None) is None:
        return parsed.dt.tz_localize(LOCAL_TZ).dt.tz_convert("UTC")
    return parsed.dt.tz_convert("UTC")


def order_columns(df: pd.DataFrame) -> pd.DataFrame:
    data = df.reset_index()
    return data[[column for column in ORDER_COLUMNS if column in data.columns]]


class TraderReportWriter:
    def __init__(
        self,
        output_dir: Path,
        enabled: bool = True,
        run_start_ns: int | None = None,
    ) -> None:
        self.output_dir = output_dir
        self.enabled = enabled
        self.run_start_ns = run_start_ns

    # 保存 NT trader 的订单，并用订单流重建完成仓位。
    def write(self, trader) -> None:
        if not self.enabled:
            return
        orders = trader.generate_orders_report()
        if not orders.empty:
            orders = self.filter_current_run(order_columns(orders))
            self.write_csv(orders, ORDERS_FILE)
        self.write_positions(orders)

    def write_csv(self, df: pd.DataFrame, filename: str) -> None:
        data = localize_time_columns(df)
        drop_empty_columns(data.rename(columns=COLUMN_LABELS)).to_csv(
            self.output_dir / filename,
            index=False,
            encoding="utf-8-sig",
        )

    # Live 最终报告只保留本次 node 启动后的订单。
    def filter_current_run(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.run_start_ns is None or df.empty or "ts_last" not in df.columns:
            return df
        ts = report_time(df["ts_last"])
        start = pd.to_datetime(self.run_start_ns, unit="ns", utc=True)
        return df[ts >= start].copy()

    def write_positions(self, orders: pd.DataFrame) -> None:
        positions = self.build_positions(orders)
        if not positions.empty:
            self.write_csv(positions, POSITIONS_FILE)

    # 同一标的和仓位 ID 的连续成交合成净仓位，归零时写一行。
    def build_positions(self, orders: pd.DataFrame) -> pd.DataFrame:
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
        keys = [
            column
            for column in ("strategy_id", "account_id", "instrument_id", "position_id")
            if column in data.columns
        ]
        for _, group in data.sort_values(["order_time", "order_seq"]).groupby(keys, dropna=False, sort=False):
            state = None
            for order in group.sort_values(["order_time", "order_seq"]).to_dict("records"):
                state = self.apply_order(state, order, rows)
        return pd.DataFrame(rows)

    # 将订单应用到当前净仓位；反手时拆成平旧仓和开新仓。
    def apply_order(
        self,
        state: dict[str, Any] | None,
        order: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        side = str(order["side"])
        qty = float(order["filled_qty_num"])
        px = float(order["avg_px_num"])
        fee = float(order["fee_num"])
        remaining = qty
        remaining_fee = fee
        direction = 1 if side == "BUY" else -1

        while remaining > 1e-12:
            if state is None:
                return new_position(order, side, remaining, px, remaining_fee)

            state_dir = 1 if state["side"] == "BUY" else -1
            if state_dir == direction:
                add_position(state, remaining, px, remaining_fee)
                return state

            close_qty = min(state["qty"], remaining)
            close_fee = remaining_fee * close_qty / remaining if remaining else 0.0
            close_position(state, order, close_qty, px, close_fee)
            remaining -= close_qty
            remaining_fee -= close_fee
            if state["qty"] <= 1e-12:
                rows.append(finished_position(state, order))
                state = None
        return state


def order_sequence(value: Any) -> int | None:
    match = re.search(r"(\d+)$", str(value))
    return int(match.group(1)) if match else None


def new_position(order: dict[str, Any], side: str, qty: float, px: float, fee: float) -> dict[str, Any]:
    return {
        "open_time": order["order_time"],
        "close_time": None,
        "strategy_id": order.get("strategy_id", ""),
        "account_id": order.get("account_id", ""),
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


def add_position(state: dict[str, Any], qty: float, px: float, fee: float) -> None:
    state["qty"] += qty
    state["peak_qty"] = max(state["peak_qty"], state["qty"])
    state["open_notional"] += qty * px
    state["fees"] += fee


def close_position(state: dict[str, Any], order: dict[str, Any], qty: float, px: float, fee: float) -> None:
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


def finished_position(state: dict[str, Any], order: dict[str, Any]) -> dict[str, Any]:
    avg_close = state["close_notional"] / state["closed_qty"] if state["closed_qty"] else 0.0
    avg_open = state["closed_open_notional"] / state["closed_qty"] if state["closed_qty"] else 0.0
    close_time = state["close_time"] or order["order_time"]
    duration = (close_time - state["open_time"]).total_seconds() / 60
    return {
        "open_time": state["open_time"],
        "close_time": close_time,
        "strategy_id": state["strategy_id"],
        "account_id": state["account_id"],
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


def read_report_csv(output_dir: Path, filename: str) -> pd.DataFrame:
    path = output_dir / filename
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


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


def is_time_column(column: str) -> bool:
    name = column.lower()
    if name == "time_in_force":
        return False
    return (
        name.startswith("ts_")
        or name.endswith("_time")
        or name.endswith("时间")
        or name in {"open_time", "close_time", "bar_time", "local_time", "income_time"}
    )


def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    empty = df.isna() | df.astype(str).apply(lambda column: column.str.strip().isin(("", "nan", "None", "NaT")))
    return df.loc[:, ~empty.all(axis=0)]


def money_to_float(value) -> float:
    return float(str(value).replace("_", "").split()[0])


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
