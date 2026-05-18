from __future__ import annotations

from nautilus_trader.common.actor import Actor
from nautilus_trader.config import ActorConfig
from nautilus_trader.model.events import AccountState

from utils.arguments import EVENT_ACCOUNT_TOPIC


class DataRecorderConfig(ActorConfig, frozen=True):
    pass


class DataRecorder(Actor):
    def __init__(self, config: DataRecorderConfig) -> None:
        super().__init__(config)
        self.seen_account_event_ids = set()

    # 启动时订阅账户事件，收到后只打运行日志。
    def on_start(self) -> None:
        self.msgbus.subscribe(EVENT_ACCOUNT_TOPIC, self.handle_account_event)
        self.log.info("DataRecorder started")

    # 停止时退订 actor 自己注册的事件。
    def on_stop(self) -> None:
        self.msgbus.unsubscribe(EVENT_ACCOUNT_TOPIC, self.handle_account_event)

    # 账户事件只打日志，不再落 CSV。
    def handle_account_event(self, event: AccountState) -> None:
        state = AccountState.to_dict(event)
        event_id = state.get("event_id")
        if event_id in self.seen_account_event_ids:
            return
        self.seen_account_event_ids.add(event_id)
        info = state.get("info") or {}
        info_type = info.get("type", "")
        info_reason = info.get("reason", "") or info.get("m", "")
        self.log.info(
            f"account_event account_id={state.get('account_id')} "
            f"event_id={event_id} event_type={state.get('type', '')} "
            f"info_type={info_type} info_reason={info_reason} "
            f"balances={len(state.get('balances', []))}",
        )
