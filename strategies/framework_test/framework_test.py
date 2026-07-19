from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from adapters.external_command import ExternalCommand
from adapters.external_command import external_command_type
from nautilus_trader.common.actor import Actor
from nautilus_trader.config import ActorConfig
from nautilus_trader.config import NautilusConfig
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import EXTERNAL_COMMAND_CLIENT_NAME
from utils.control_messages import NODE_STOP_TOPIC
from utils.control_messages import NodeStopRequest


STATUS_TOPIC = "framework_test.status"
STOP_TIMER = "framework_test.stop"
COMPLETE_TIMER = "framework_test.complete"
BALANCE_BUFFER = Decimal("1.05")


@dataclass(frozen=True)
class FrameworkTestStatus:
    phase: str
    counts: dict[str, int]


@dataclass
class PendingOrder:
    action: str
    instrument_id: InstrumentId
    target_qty: Decimal
    filled_qty: Decimal = Decimal("0")


class FrameworkTestActorConfig(ActorConfig, frozen=True):
    status_path: str


class FrameworkTestActor(Actor):
    def __init__(self, config: FrameworkTestActorConfig) -> None:
        super().__init__(config)
        self.status_path = Path(config.status_path)
        self.statuses: list[FrameworkTestStatus] = []

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

    def _on_status(self, status: FrameworkTestStatus) -> None:
        if not isinstance(status, FrameworkTestStatus):
            raise TypeError(f"{STATUS_TOPIC} requires FrameworkTestStatus")
        self.statuses.append(status)
        self.log.info(f"framework_test status phase={status.phase} counts={status.counts}")


class RoundTripConfig(NautilusConfig, frozen=True):
    instrument_id: InstrumentId
    qty: Decimal


class FrameworkTestConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    data_clients: dict[str, str]
    round_trips: list[RoundTripConfig]
    run_seconds: int


class FrameworkTestStrategy(Strategy):
    def __init__(self, config: FrameworkTestConfig) -> None:
        super().__init__(config)
        if config.run_seconds <= 0:
            raise ValueError("run_seconds must be positive")
        instruments = set(config.instrument_ids)
        if not {bar_type.instrument_id for bar_type in config.bar_types} <= instruments:
            raise ValueError("bar_types must only reference configured instruments")
        if set(config.data_clients) != {str(instrument_id) for instrument_id in instruments}:
            raise ValueError("data_clients must map every configured instrument")
        round_trip_ids = [item.instrument_id for item in config.round_trips]
        if len(set(round_trip_ids)) != len(round_trip_ids):
            raise ValueError("round_trips must not contain duplicate instruments")
        if set(round_trip_ids) != instruments:
            raise ValueError("round_trips must cover every configured instrument")
        if any(item.qty <= 0 for item in config.round_trips):
            raise ValueError("round trip qty must be positive")
        self.instrument_ids = list(config.instrument_ids)
        self.bar_types = list(config.bar_types)
        self.data_clients = {
            InstrumentId.from_str(instrument_id): ClientId(client_id)
            for instrument_id, client_id in config.data_clients.items()
        }
        self.round_trips = list(config.round_trips)
        self.run_seconds = config.run_seconds
        self.quote_counts = {str(instrument_id): 0 for instrument_id in self.instrument_ids}
        self.trade_counts = {str(instrument_id): 0 for instrument_id in self.instrument_ids}
        self.bar_counts = {str(bar_type): 0 for bar_type in self.bar_types}
        self.command_count = 0
        self.round_trips_completed = 0
        self.current_round_trip = 0
        self.pending_order_id: str | None = None
        self.pending: PendingOrder | None = None
        self.trading_started = False
        self.failed = False
        self.stop_requested = False
        self.watchdog_active = False
        self.complete_timer_active = False

    # 验证行情、外部数据、真实成交和完整停止后处理流程。
    def on_start(self) -> None:
        missing = [
            str(instrument_id)
            for instrument_id in self.instrument_ids
            if self.cache.instrument(instrument_id) is None
        ]
        if missing:
            raise RuntimeError(
                f"framework_test instruments missing from cache: {', '.join(missing)}",
            )
        open_positions = [
            str(instrument_id)
            for instrument_id in self.instrument_ids
            if self.cache.positions_open(instrument_id=instrument_id)
        ]
        if open_positions:
            raise RuntimeError(
                f"framework_test requires flat instruments: {', '.join(open_positions)}",
            )
        open_orders = [
            str(instrument_id)
            for instrument_id in self.instrument_ids
            if self.cache.orders_open(instrument_id=instrument_id)
        ]
        if open_orders:
            raise RuntimeError(
                f"framework_test requires no open orders: {', '.join(open_orders)}",
            )

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
            external_command_type(),
            client_id=ClientId(EXTERNAL_COMMAND_CLIENT_NAME),
        )

        self.watchdog_active = True
        self.clock.set_time_alert_ns(
            STOP_TIMER,
            self.clock.timestamp_ns() + self.run_seconds * 1_000_000_000,
            callback=lambda _event: self._on_watchdog(),
        )
        accounts = [str(account.id) for account in self.cache.accounts()]
        self.log.info(
            f"framework_test started instruments={list(self.quote_counts)} accounts={accounts} "
            f"run_seconds={self.run_seconds}",
        )
        self._publish_status("started")

    def on_quote_tick(self, tick: QuoteTick) -> None:
        key = str(tick.instrument_id)
        self.quote_counts[key] += 1
        if self.quote_counts[key] == 1:
            self.log.info(f"framework_test first_quote instrument={key}")
        if not self.trading_started and all(self.quote_counts.values()):
            self.trading_started = True
            if not self._validate_opening_balances():
                return
            self._submit_open()

    def on_trade_tick(self, tick: TradeTick) -> None:
        key = str(tick.instrument_id)
        self.trade_counts[key] += 1
        if self.trade_counts[key] == 1:
            self.log.info(f"framework_test first_trade instrument={key}")

    def on_bar(self, bar: Bar) -> None:
        key = str(bar.bar_type)
        self.bar_counts[key] += 1
        if self.bar_counts[key] == 1:
            self.log.info(f"framework_test first_bar bar_type={key}")

    def on_data(self, data) -> None:
        payload = data.data if isinstance(data, CustomData) else data
        if isinstance(payload, ExternalCommand):
            self.command_count += 1
            self.log.info(
                f"framework_test external_command command={payload.command} "
                f"source={payload.source}",
            )

    def on_order_filled(self, event: OrderFilled) -> None:
        if self.pending is None or str(event.client_order_id) != self.pending_order_id:
            return
        self.pending.filled_qty += event.last_qty.as_decimal()
        if self.pending.filled_qty < self.pending.target_qty:
            return

        pending = self.pending
        self.pending = None
        self.pending_order_id = None
        if pending.action == "open":
            self._submit_close(pending.instrument_id, pending.filled_qty)
            return

        self.round_trips_completed += 1
        self.current_round_trip += 1
        self._publish_status("round_trip_completed")
        if self.current_round_trip < len(self.round_trips):
            self._submit_open()
            return
        self._schedule_stop()

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._handle_order_failure(str(event.client_order_id), "rejected")

    def on_order_denied(self, event: OrderDenied) -> None:
        self._handle_order_failure(str(event.client_order_id), "denied")

    def on_order_canceled(self, event: OrderCanceled) -> None:
        self._handle_order_failure(str(event.client_order_id), "canceled")

    def on_order_expired(self, event: OrderExpired) -> None:
        self._handle_order_failure(str(event.client_order_id), "expired")

    def on_stop(self) -> None:
        if self.watchdog_active:
            self.clock.cancel_timer(STOP_TIMER)
            self.watchdog_active = False
        if self.complete_timer_active:
            self.clock.cancel_timer(COMPLETE_TIMER)
            self.complete_timer_active = False
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
            external_command_type(),
            client_id=ClientId(EXTERNAL_COMMAND_CLIENT_NAME),
        )
        self._publish_status("stopped")
        open_positions = [
            str(position.instrument_id)
            for instrument_id in self.instrument_ids
            for position in self.cache.positions_open(instrument_id=instrument_id)
        ]
        if open_positions:
            self.log.error(f"framework_test stopped with open_positions={open_positions}")
        else:
            self.log.info(f"framework_test stopped counts={self._counts()}")

    def _submit_open(self) -> None:
        test = self.round_trips[self.current_round_trip]
        _, quantity, _ = self._order_inputs(test)

        order = self.order_factory.market(
            instrument_id=test.instrument_id,
            order_side=OrderSide.BUY,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )
        self.pending_order_id = str(order.client_order_id)
        self.pending = PendingOrder(
            action="open",
            instrument_id=test.instrument_id,
            target_qty=quantity.as_decimal(),
        )
        self._publish_status("opening")
        self.log.info(f"framework_test submit_open instrument={test.instrument_id} qty={quantity}")
        self.submit_order(order)

    def _submit_close(self, instrument_id: InstrumentId, filled_qty: Decimal) -> None:
        instrument = self.cache.instrument(instrument_id)
        if instrument is None:
            raise RuntimeError(f"instrument is not ready: {instrument_id}")
        quantity = instrument.make_qty(filled_qty)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=OrderSide.SELL,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
            reduce_only=True,
        )
        self.pending_order_id = str(order.client_order_id)
        self.pending = PendingOrder(
            action="close",
            instrument_id=instrument_id,
            target_qty=quantity.as_decimal(),
        )
        self._publish_status("closing")
        self.log.info(f"framework_test submit_close instrument={instrument_id} qty={quantity}")
        self.submit_order(order)

    def _validate_opening_balances(self) -> bool:
        for test in self.round_trips:
            instrument, quantity, quote = self._order_inputs(test)
            notional = instrument.notional_value(quantity, quote.ask_price)
            required = notional.as_decimal() * BALANCE_BUFFER
            account = self.cache.account_for_venue(test.instrument_id.venue)
            available = account.balance_free(notional.currency) if account is not None else None
            available_value = available.as_decimal() if available is not None else None
            if available_value is None or available_value < required:
                self.failed = True
                self._publish_status("balance_check_failed")
                self.log.error(
                    f"framework_test insufficient_balance instrument={test.instrument_id} "
                    f"available={available_value} required={required} "
                    f"currency={notional.currency}",
                )
                self._request_stop("opening balance check failed")
                return False
            self.log.info(
                f"framework_test balance_checked instrument={test.instrument_id} "
                f"available={available_value} required={required} "
                f"currency={notional.currency}",
            )
        self._publish_status("balance_checked")
        return True

    def _order_inputs(self, test: RoundTripConfig):
        instrument = self.cache.instrument(test.instrument_id)
        quote = self.cache.quote_tick(test.instrument_id)
        if instrument is None or quote is None:
            raise RuntimeError(f"instrument is not ready: {test.instrument_id}")
        quantity = instrument.make_qty(test.qty)
        if quantity.as_decimal() != test.qty:
            raise ValueError(f"qty is not aligned to instrument precision: {test}")
        if instrument.min_quantity is not None and quantity < instrument.min_quantity:
            raise ValueError(f"qty is below minimum quantity: {test}")
        if (
            instrument.min_notional is not None
            and instrument.notional_value(quantity, quote.ask_price) < instrument.min_notional
        ):
            raise ValueError(f"qty is below minimum notional: {test}")
        return instrument, quantity, quote

    def _schedule_stop(self) -> None:
        self._publish_status("completed")
        self.complete_timer_active = True
        self.clock.set_time_alert_ns(
            COMPLETE_TIMER,
            self.clock.timestamp_ns() + 2_000_000_000,
            callback=lambda _event: self._request_stop("round trips complete"),
        )

    def _on_watchdog(self) -> None:
        self.watchdog_active = False
        open_positions = any(
            self.cache.positions_open(instrument_id=instrument_id)
            for instrument_id in self.instrument_ids
        )
        if self.pending is not None or open_positions:
            self.failed = True
            self._publish_status("timeout")
            self.log.error(
                "framework_test timed out with pending order or open position; "
                "node remains running for manual intervention",
            )
            return
        self._request_stop("framework test timed out before trading")

    def _handle_order_failure(self, order_id: str, status: str) -> None:
        if self.pending is None or order_id != self.pending_order_id:
            return
        self.failed = True
        self._publish_status("failed")
        self.pending = None
        self.pending_order_id = None
        open_positions = any(
            self.cache.positions_open(instrument_id=instrument_id)
            for instrument_id in self.instrument_ids
        )
        if not open_positions:
            self.log.error(f"framework_test order_{status} order={order_id}; stopping safely")
            self._request_stop(f"order {status} without open position")
            return
        self.log.error(
            f"framework_test order_{status} order={order_id}; "
            "node remains running for manual intervention",
        )

    # 通过 msgbus 请求框架停止整个 node，再由统一生命周期执行后处理。
    def _request_stop(self, reason: str) -> None:
        if self.stop_requested:
            return
        self.stop_requested = True
        self.complete_timer_active = False
        self._publish_status("stopping")
        self.msgbus.publish(
            NODE_STOP_TOPIC,
            NodeStopRequest(source="framework_test", reason=reason),
        )

    def _counts(self) -> dict[str, int]:
        return {
            "quotes": sum(self.quote_counts.values()),
            "trades": sum(self.trade_counts.values()),
            "bars": sum(self.bar_counts.values()),
            "commands": self.command_count,
            "round_trips": self.round_trips_completed,
        }

    def _publish_status(self, phase: str) -> None:
        self.msgbus.publish(
            STATUS_TOPIC,
            FrameworkTestStatus(phase=phase, counts=self._counts()),
        )
