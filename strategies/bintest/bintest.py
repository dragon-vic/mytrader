from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from adapters.external_command import ExternalCommand
from adapters.external_command import external_command_type
from adapters.external_signal import ExternalSignal
from adapters.external_signal import external_signal_type
from nautilus_trader.common.actor import Actor
from nautilus_trader.config import ActorConfig
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import EXTERNAL_COMMAND_CLIENT_NAME
from utils.arguments import EXTERNAL_SIGNAL_CLIENT_NAME
from utils.control_messages import NODE_STOP_TOPIC
from utils.control_messages import NodeStopRequest


STATUS_TOPIC = "bintest.status"
STOP_TIMER = "bintest.stop"


@dataclass(frozen=True)
class BintestStatus:
    phase: str
    counts: dict[str, int]


class BintestActorConfig(ActorConfig, frozen=True):
    status_path: str


class BintestActor(Actor):
    def __init__(self, config: BintestActorConfig) -> None:
        super().__init__(config)
        self.status_path = Path(config.status_path)
        self.statuses: list[BintestStatus] = []

    def on_start(self) -> None:
        self.msgbus.subscribe(STATUS_TOPIC, self._on_status)

    # Actor artifact 验证组件生命周期和报告目录路径注入。
    def on_stop(self) -> None:
        self.msgbus.unsubscribe(STATUS_TOPIC, self._on_status)
        payload = [
            {"phase": status.phase, "counts": status.counts}
            for status in self.statuses
        ]
        self.status_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _on_status(self, status: BintestStatus) -> None:
        if not isinstance(status, BintestStatus):
            raise TypeError(f"{STATUS_TOPIC} requires BintestStatus")
        self.statuses.append(status)
        self.log.info(f"bintest status phase={status.phase} counts={status.counts}")


class BintestConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    data_clients: dict[str, str]
    run_seconds: int


class BintestStrategy(Strategy):
    def __init__(self, config: BintestConfig) -> None:
        super().__init__(config)
        if config.run_seconds <= 0:
            raise ValueError("run_seconds must be positive")
        instruments = set(config.instrument_ids)
        if not {bar_type.instrument_id for bar_type in config.bar_types} <= instruments:
            raise ValueError("bar_types must only reference configured instruments")
        if set(config.data_clients) != {str(instrument_id) for instrument_id in instruments}:
            raise ValueError("data_clients must map every configured instrument")
        self.instrument_ids = list(config.instrument_ids)
        self.bar_types = list(config.bar_types)
        self.data_clients = {
            InstrumentId.from_str(instrument_id): ClientId(client_id)
            for instrument_id, client_id in config.data_clients.items()
        }
        self.run_seconds = config.run_seconds
        self.quote_counts = {str(instrument_id): 0 for instrument_id in self.instrument_ids}
        self.trade_counts = {str(instrument_id): 0 for instrument_id in self.instrument_ids}
        self.bar_counts = {str(bar_type): 0 for bar_type in self.bar_types}
        self.signal_count = 0
        self.command_count = 0
        self.timer_active = False

    # 只验证行情、外部数据和生命周期；策略没有任何下单路径。
    def on_start(self) -> None:
        missing = [
            str(instrument_id)
            for instrument_id in self.instrument_ids
            if self.cache.instrument(instrument_id) is None
        ]
        if missing:
            raise RuntimeError(f"bintest instruments missing from cache: {', '.join(missing)}")

        for instrument_id in self.instrument_ids:
            client_id = self.data_clients[instrument_id]
            self.subscribe_quote_ticks(instrument_id, client_id=client_id)
            self.subscribe_trade_ticks(instrument_id, client_id=client_id)
        for bar_type in self.bar_types:
            self.subscribe_bars(
                bar_type,
                client_id=self.data_clients[bar_type.instrument_id],
            )
        self.subscribe_data(
            external_signal_type(),
            client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME),
        )
        self.subscribe_data(
            external_command_type(),
            client_id=ClientId(EXTERNAL_COMMAND_CLIENT_NAME),
        )

        self.timer_active = True
        self.clock.set_time_alert_ns(
            STOP_TIMER,
            self.clock.timestamp_ns() + self.run_seconds * 1_000_000_000,
            callback=lambda _event: self._request_stop(),
        )
        accounts = [str(account.id) for account in self.cache.accounts()]
        self.log.info(
            f"bintest started instruments={list(self.quote_counts)} accounts={accounts} "
            f"run_seconds={self.run_seconds}",
        )
        self._publish_status("started")

    def on_quote_tick(self, tick: QuoteTick) -> None:
        key = str(tick.instrument_id)
        self.quote_counts[key] += 1
        if self.quote_counts[key] == 1:
            self.log.info(f"bintest first_quote instrument={key}")

    def on_trade_tick(self, tick: TradeTick) -> None:
        key = str(tick.instrument_id)
        self.trade_counts[key] += 1
        if self.trade_counts[key] == 1:
            self.log.info(f"bintest first_trade instrument={key}")

    def on_bar(self, bar: Bar) -> None:
        key = str(bar.bar_type)
        self.bar_counts[key] += 1
        if self.bar_counts[key] == 1:
            self.log.info(f"bintest first_bar bar_type={key}")

    def on_data(self, data) -> None:
        payload = data.data if isinstance(data, CustomData) else data
        if isinstance(payload, ExternalSignal):
            self.signal_count += 1
            self.log.info(
                f"bintest external_signal side={payload.side} instrument={payload.instrument_id}",
            )
        elif isinstance(payload, ExternalCommand):
            self.command_count += 1
            self.log.info(
                f"bintest external_command command={payload.command} source={payload.source}",
            )

    # 定时器通过 msgbus 请求框架停止整个 node。
    def _request_stop(self) -> None:
        self.timer_active = False
        self._publish_status("stopping")
        self.msgbus.publish(
            NODE_STOP_TOPIC,
            NodeStopRequest(source="bintest", reason="live smoke complete"),
        )

    def on_stop(self) -> None:
        if self.timer_active:
            self.clock.cancel_timer(STOP_TIMER)
            self.timer_active = False
        for instrument_id in self.instrument_ids:
            client_id = self.data_clients[instrument_id]
            self.unsubscribe_quote_ticks(instrument_id, client_id=client_id)
            self.unsubscribe_trade_ticks(instrument_id, client_id=client_id)
        for bar_type in self.bar_types:
            self.unsubscribe_bars(
                bar_type,
                client_id=self.data_clients[bar_type.instrument_id],
            )
        self.unsubscribe_data(
            external_signal_type(),
            client_id=ClientId(EXTERNAL_SIGNAL_CLIENT_NAME),
        )
        self.unsubscribe_data(
            external_command_type(),
            client_id=ClientId(EXTERNAL_COMMAND_CLIENT_NAME),
        )
        self._publish_status("stopped")
        self.log.info(f"bintest stopped counts={self._counts()}")

    def _counts(self) -> dict[str, int]:
        return {
            "quotes": sum(self.quote_counts.values()),
            "trades": sum(self.trade_counts.values()),
            "bars": sum(self.bar_counts.values()),
            "signals": self.signal_count,
            "commands": self.command_count,
        }

    def _publish_status(self, phase: str) -> None:
        self.msgbus.publish(STATUS_TOPIC, BintestStatus(phase=phase, counts=self._counts()))
