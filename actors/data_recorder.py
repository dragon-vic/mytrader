from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from nautilus_trader.common.actor import Actor
from nautilus_trader.config import ActorConfig
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import PositionEvent


FILL_COLUMNS = [
    "ts_event",
    "instrument_id",
    "order_side",
    "last_qty",
    "last_px",
    "commission",
    "client_order_id",
    "position_id",
]

ACCOUNT_COLUMNS = [
    "ts_event",
    "account_id",
    "account_type",
    "base_currency",
    "currency",
    "total",
    "locked",
    "free",
    "is_reported",
    "event_type",
    "info_type",
    "info_reason",
    "info",
]

POSITION_COLUMNS = [
    "ts_event",
    "event_type",
    "instrument_id",
    "position_id",
    "side",
    "quantity",
    "last_qty",
    "last_px",
    "realized_pnl",
    "adjustment_type",
    "quantity_change",
    "pnl_change",
    "reason",
    "account_id",
    "strategy_id",
]


class DataRecorderConfig(ActorConfig, frozen=True):
    output_dir: str


class DataRecorder(Actor):
    def __init__(self, config: DataRecorderConfig) -> None:
        super().__init__(config)
        self.output_dir = Path(config.output_dir)

    # 启动时订阅所有 live 事件，后续由 actor 统一落盘。
    def on_start(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.msgbus.subscribe("events.order.*", self.handle_order_event)
        self.msgbus.subscribe("events.position.*", self.handle_position_event)
        self.msgbus.subscribe("events.account.*", self.handle_account_event)

    # 停止时退订 actor 自己注册的事件。
    def on_stop(self) -> None:
        self.msgbus.unsubscribe("events.order.*", self.handle_order_event)
        self.msgbus.unsubscribe("events.position.*", self.handle_position_event)
        self.msgbus.unsubscribe("events.account.*", self.handle_account_event)

    # 只把真实成交实时写入 fills.csv。
    def handle_order_event(self, event) -> None:
        if isinstance(event, OrderFilled):
            row = OrderFilled.to_dict(event)
            row["ts_event"] = pd.to_datetime(row["ts_event"], unit="ns", utc=True)
            self._append_csv("fills.csv", {column: row.get(column) for column in FILL_COLUMNS})

    # 把账户余额变化展开成一币种一行，并保留 info 字段用于判断 funding。
    def handle_account_event(self, event: AccountState) -> None:
        state = AccountState.to_dict(event)
        info = state.get("info") or {}
        info_type = info.get("type", "")
        info_reason = info.get("reason", "") or info.get("m", "")
        for balance in state.pop("balances", []):
            row = {
                **balance,
                **state,
                "ts_event": pd.to_datetime(state["ts_event"], unit="ns", utc=True),
                "event_type": state.get("type", ""),
                "info_type": info_type,
                "info_reason": info_reason,
                "info": json.dumps(info, ensure_ascii=False),
            }
            self._append_csv("account_changes.csv", {column: row.get(column) for column in ACCOUNT_COLUMNS})

    # 把 live 仓位事件落盘，方便和账户变化对照。
    def handle_position_event(self, event: PositionEvent) -> None:
        row = type(event).to_dict(event)
        row["ts_event"] = pd.to_datetime(row["ts_event"], unit="ns", utc=True)
        row["event_type"] = row.get("type", type(event).__name__)
        row["adjustment_type"] = row.get("adjustment_type", "")
        row["quantity_change"] = row.get("quantity_change", "")
        row["pnl_change"] = row.get("pnl_change", "")
        row["reason"] = row.get("reason", "")
        self._append_csv("position_events.csv", {column: row.get(column) for column in POSITION_COLUMNS})

    # 追加一行 CSV，写完立即关闭文件，方便运行中读取。
    def _append_csv(self, filename: str, row: dict) -> None:
        path = self.output_dir / filename
        pd.DataFrame([row]).to_csv(
            path,
            mode="a",
            header=not path.exists(),
            index=False,
        )
