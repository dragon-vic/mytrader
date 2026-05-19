from __future__ import annotations

import csv
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from utils.arguments import FILLS_FILE
from utils.arguments import ORDERS_FILE
from utils.arguments import POSITION_EVENTS_FILE
from utils.arguments import POSITIONS_FILE
from utils.arguments import REPORT_COLUMNS
from utils.report_labels import COLUMN_LABELS


LOCAL_TZ = ZoneInfo("Asia/Shanghai")


class DataRecorder:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir

    # 启动时创建报告目录。
    def start(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # 成交事件同时补 orders.csv 和 fills.csv。
    def on_order_filled(self, event: Any) -> None:
        row = self.event_row(event)
        self.append_row(FILLS_FILE, REPORT_COLUMNS["fills"], row)
        self.append_row(
            ORDERS_FILE,
            REPORT_COLUMNS["orders"],
            {
                "ts_last": row.get("ts_event"),
                "instrument_id": row.get("instrument_id"),
                "side": row.get("order_side"),
                "quantity": row.get("last_qty"),
                "filled_qty": row.get("last_qty"),
                "avg_px": row.get("last_px"),
                "commissions": row.get("commission"),
                "status": "FILLED",
                "position_id": row.get("position_id"),
                "client_order_id": row.get("client_order_id"),
            },
        )

    # 拒单事件逐条追加到 orders.csv。
    def on_order_rejected(self, event: Any) -> None:
        row = self.event_row(event)
        self.append_row(
            ORDERS_FILE,
            REPORT_COLUMNS["orders"],
            {
                "ts_last": row.get("ts_event"),
                "instrument_id": row.get("instrument_id"),
                "status": "REJECTED",
                "client_order_id": row.get("client_order_id"),
            },
        )

    # 撤单事件逐条追加到 orders.csv。
    def on_order_canceled(self, event: Any) -> None:
        row = self.event_row(event)
        self.append_row(
            ORDERS_FILE,
            REPORT_COLUMNS["orders"],
            {
                "ts_last": row.get("ts_event"),
                "instrument_id": row.get("instrument_id"),
                "status": "CANCELED",
                "client_order_id": row.get("client_order_id"),
            },
        )

    # 如果 PyO3 转发 position 事件，就按原 position_events.csv 格式落盘。
    def on_position_opened(self, event: Any) -> None:
        self.write_position_event("PositionOpened", event)

    def on_position_changed(self, event: Any) -> None:
        self.write_position_event("PositionChanged", event)

    def on_position_adjusted(self, event: Any) -> None:
        self.write_position_event("PositionAdjusted", event)

    def on_position_closed(self, event: Any) -> None:
        self.write_position_event("PositionClosed", event)
        self.append_row(POSITIONS_FILE, REPORT_COLUMNS["positions"], self.position_row(event))

    def write_position_event(self, event_type: str, event: Any) -> None:
        row = self.event_row(event)
        row["event_type"] = event_type
        row["event_side"] = row.get("side") or row.get("order_side") or row.get("entry")
        row["fill_quantity"] = row.get("last_qty")
        row["fill_price"] = row.get("last_px")
        self.append_row(POSITION_EVENTS_FILE, REPORT_COLUMNS["position_events"], row)

    def position_row(self, event: Any) -> dict[str, Any]:
        row = self.event_row(event)
        return {
            "ts_opened": row.get("ts_opened"),
            "ts_closed": row.get("ts_closed") or row.get("ts_event"),
            "instrument_id": row.get("instrument_id"),
            "entry": row.get("entry"),
            "quantity": row.get("quantity"),
            "peak_qty": row.get("peak_qty") or row.get("peak_quantity"),
            "avg_px_open": row.get("avg_px_open"),
            "avg_px_close": row.get("avg_px_close"),
            "realized_pnl": row.get("realized_pnl"),
            "realized_return": row.get("realized_return"),
            "commissions": row.get("commissions"),
            "duration_ns": row.get("duration_ns") or duration_ns(row),
            "position_id": row.get("position_id"),
            "opening_order_id": row.get("opening_order_id"),
            "closing_order_id": row.get("closing_order_id"),
        }

    def event_row(self, event: Any) -> dict[str, Any]:
        if hasattr(event, "to_dict"):
            return dict(event.to_dict())
        return {
            name: getattr(event, name)
            for name in dir(event)
            if not name.startswith("_") and not callable(getattr(event, name))
        }

    def append_row(self, filename: str, columns: list[str], row: dict[str, Any]) -> None:
        path = self.output_dir / filename
        exists = path.exists()
        labels = [COLUMN_LABELS.get(column, column) for column in columns]
        with path.open("a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=labels)
            if not exists:
                writer.writeheader()
            writer.writerow({
                COLUMN_LABELS.get(column, column): format_cell(column, row.get(column))
                for column in columns
            })


def duration_ns(row: dict[str, Any]) -> Any:
    opened = row.get("ts_opened")
    closed = row.get("ts_closed") or row.get("ts_event")
    if opened is None or closed is None:
        return ""
    return int(closed) - int(opened)


def format_cell(column: str, value: Any) -> str:
    if value is None:
        return ""
    if is_time_column(column):
        return format_time(value)
    if isinstance(value, list | tuple):
        return "[" + ", ".join(str(item) for item in value) + "]"
    return str(value)


def is_time_column(column: str) -> bool:
    return column.startswith("ts_") or column.endswith("_time")


def format_time(value: Any) -> str:
    timestamp = datetime.fromtimestamp(int(value) / 1_000_000_000, UTC)
    return timestamp.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
