from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from nautilus_trader.common.actor import Actor
from nautilus_trader.config import ActorConfig
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.events import PositionEvent

from utils.arguments import ACCOUNT_CHANGES_FILE
from utils.arguments import ACCOUNT_COLUMNS
from utils.arguments import EVENT_ACCOUNT_TOPIC
from utils.arguments import EVENT_POSITION_TOPIC
from utils.arguments import POSITION_COLUMNS
from utils.arguments import POSITION_EVENTS_FILE


class DataRecorderConfig(ActorConfig, frozen=True):
    output_dir: str


class DataRecorder(Actor):
    def __init__(self, config: DataRecorderConfig) -> None:
        super().__init__(config)
        self.output_dir = Path(config.output_dir)

    # 启动时订阅所有 live 事件，后续由 actor 统一落盘。
    def on_start(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.msgbus.subscribe(EVENT_POSITION_TOPIC, self.handle_position_event)
        self.msgbus.subscribe(EVENT_ACCOUNT_TOPIC, self.handle_account_event)
        self.log.info(f"data recoder 初始化完成")

    # 停止时退订 actor 自己注册的事件。
    def on_stop(self) -> None:
        self.msgbus.unsubscribe(EVENT_POSITION_TOPIC, self.handle_position_event)
        self.msgbus.unsubscribe(EVENT_ACCOUNT_TOPIC, self.handle_account_event)

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
                "ts_event": pd.to_datetime(event.ts_event, unit="ns", utc=True),
                "event_type": state.get("type", ""),
                "info_type": info_type,
                "info_reason": info_reason,
                "info": json.dumps(info, ensure_ascii=False),
            }
            self._append_csv(ACCOUNT_CHANGES_FILE, {column: row.get(column) for column in ACCOUNT_COLUMNS})

    # 把 live 仓位事件落盘，方便和账户变化对照。
    def handle_position_event(self, event: PositionEvent) -> None:
        row = type(event).to_dict(event)
        row["ts_event"] = pd.to_datetime(event.ts_event, unit="ns", utc=True)
        row["event_type"] = type(event).__name__
        row["adjustment_type"] = row.get("adjustment_type", "")
        row["quantity_change"] = row.get("quantity_change", "")
        row["pnl_change"] = row.get("pnl_change", "")
        row["reason"] = row.get("reason", "")
        row["event_side"] = row.get("side") or row.get("order_side") or row.get("entry", "")
        row["fill_quantity"] = row.get("last_qty", "")
        row["fill_price"] = row.get("last_px", "")
        self._append_csv(POSITION_EVENTS_FILE, {column: row.get(column) for column in POSITION_COLUMNS})

    # 追加一行 CSV，写完立即关闭文件，方便运行中读取。
    def _append_csv(self, filename: str, row: dict) -> None:
        path = self.output_dir / filename
        pd.DataFrame([row]).to_csv(
            path,
            mode="a",
            header=not path.exists(),
            index=False,
        )
