from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from adapters.external_json import ExternalJson
from adapters.external_json import external_json_type
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.orders import Order
from nautilus_trader.trading.strategy import Strategy

from strategies.agent_trading.contracts import DECISION_VALIDATOR
from strategies.agent_trading.lifecycle import load_market_universe
from strategies.agent_trading.lifecycle import load_schedule
from utils.constants import EXTERNAL_JSON_CLIENT_NAME
from utils.constants import PROJECT_ROOT

MINUTE_NS = 60_000_000_000
HOUR_NS = 60 * MINUTE_NS
PERCENT = Decimal("100")


@dataclass
class MarkBucket:
    total: Decimal = Decimal("0")
    count: int = 0


@dataclass
class PendingOrder:
    event_id: str
    instrument_id: InstrumentId
    target_qty: Decimal
    filled_qty: Decimal = Decimal("0")


class AgentTradingConfig(StrategyConfig, frozen=True):
    schedule_path: str
    market_universe_path: str
    data_client: str
    margin_usdt: Decimal
    leverage: Decimal
    min_remaining_pct: Decimal
    max_mark_age_sec: Decimal


class AgentTradingStrategy(Strategy):
    # 初始化 Agent 交易策略；具体交易参数在方案确认后加入。
    def __init__(self, config: AgentTradingConfig) -> None:
        super().__init__(config)
        schedule_path = self._project_path(config.schedule_path)
        universe_path = self._project_path(config.market_universe_path)
        batches = load_schedule(schedule_path)
        universe = load_market_universe(universe_path)

        self.data_client = ClientId(config.data_client)
        self.margin_usdt = config.margin_usdt
        self.leverage = config.leverage
        self.min_remaining = config.min_remaining_pct
        self.max_mark_age_ns = int(config.max_mark_age_sec * Decimal("1000000000"))
        if (
            self.margin_usdt <= 0
            or self.leverage <= 0
            or self.min_remaining < 0
            or self.max_mark_age_ns <= 0
        ):
            raise ValueError("agent trading risk and market parameters must be positive")

        self.instrument_ids = tuple(
            InstrumentId.from_str(item.instrument_id)
            for item in universe.instruments
            if item.venue == "BINANCE"
        )
        if not self.instrument_ids:
            raise ValueError("market universe has no Binance instruments")
        self.instrument_set = frozenset(self.instrument_ids)
        self.event_batch = {
            event.event_id: batch.batch_id
            for batch in batches
            for event in batch.events
        }
        self.windows = {
            batch.batch_id: (
                int(batch.watch_start_at.timestamp() * 1_000_000_000) - HOUR_NS,
                int(batch.watch_start_at.timestamp() * 1_000_000_000),
            )
            for batch in batches
        }
        if any(end_ns % MINUTE_NS for _start_ns, end_ns in self.windows.values()):
            raise ValueError("schedule watch_start_at must align to a whole UTC minute")
        self.mark_minutes: dict[
            str,
            dict[InstrumentId, dict[int, MarkBucket]],
        ] = {batch_id: {} for batch_id in self.windows}
        self.seen_events: set[str] = set()
        self.pending: dict[str, PendingOrder] = {}

    # 订阅外部 Agent 的通用 JSON 数据。
    def on_start(self) -> None:
        loaded = tuple(
            instrument_id
            for instrument_id in self.instrument_ids
            if self.cache.instrument(instrument_id) is not None
        )
        missing = set(self.instrument_ids) - set(loaded)
        if missing:
            preview = ",".join(str(value) for value in sorted(missing, key=str)[:5])
            self.log.warning(
                f"agent trading instruments unavailable count={len(missing)} first={preview}",
            )
        self.instrument_ids = loaded
        self.instrument_set = frozenset(loaded)
        if not self.instrument_ids:
            raise RuntimeError("agent trading has no loaded Binance instruments")
        if self._account() is None:
            raise RuntimeError("agent trading Binance account is not ready")
        for instrument_id in self.instrument_ids:
            self.subscribe_mark_prices(
                instrument_id,
                client_id=self.data_client,
            )
        self.subscribe_data(
            external_json_type(),
            client_id=ClientId(EXTERNAL_JSON_CLIENT_NAME),
        )
        self.log.info(
            f"agent_trading started instruments={len(self.instrument_ids)} "
            f"batches={len(self.windows)} margin_usdt={self.margin_usdt} "
            f"leverage={self.leverage}",
        )

    # 将每秒标记价格聚合到对应批次的分钟桶中。
    def on_mark_price(self, mark: MarkPriceUpdate) -> None:
        if mark.instrument_id not in self.instrument_set:
            return
        ts_event = int(mark.ts_event)
        minute_ns = ts_event // MINUTE_NS * MINUTE_NS
        price = mark.value.as_decimal()
        for batch_id, (start_ns, end_ns) in self.windows.items():
            if not start_ns <= ts_event < end_ns:
                continue
            instruments = self.mark_minutes[batch_id]
            minutes = instruments.setdefault(mark.instrument_id, {})
            bucket = minutes.setdefault(minute_ns, MarkBucket())
            bucket.total += price
            bucket.count += 1

    # 接收外部交易判断；无效消息只记录，不进入交易流程。
    def on_data(self, data) -> None:
        payload = data.data if isinstance(data, CustomData) else data
        if isinstance(payload, ExternalJson):
            message = json.loads(payload.payload)
            if not isinstance(message, dict):
                raise TypeError("agent message must be a JSON object")
            try:
                self._handle_agent_message(message)
            except (KeyError, TypeError, ValueError) as exc:
                self.log.error(f"agent_json_rejected error={type(exc).__name__}: {exc}")

    def on_stop(self) -> None:
        for instrument_id in self.instrument_ids:
            self.unsubscribe_mark_prices(
                instrument_id,
                client_id=self.data_client,
            )
        self.unsubscribe_data(
            external_json_type(),
            client_id=ClientId(EXTERNAL_JSON_CLIENT_NAME),
        )
        self.log.info(f"agent_trading stopped pending_orders={len(self.pending)}")

    def on_order_filled(self, event: OrderFilled) -> None:
        pending = self.pending.get(str(event.client_order_id))
        if pending is None:
            return
        pending.filled_qty += event.last_qty.as_decimal()
        if pending.filled_qty >= pending.target_qty:
            self.pending.pop(str(event.client_order_id))
            self.log.info(
                f"agent_order_filled event_id={pending.event_id} "
                f"instrument={pending.instrument_id} qty={pending.filled_qty}",
            )

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._order_failed(str(event.client_order_id), "rejected")

    def on_order_denied(self, event: OrderDenied) -> None:
        self._order_failed(str(event.client_order_id), "denied")

    def on_order_canceled(self, event: OrderCanceled) -> None:
        self._order_failed(str(event.client_order_id), "canceled")

    def on_order_expired(self, event: OrderExpired) -> None:
        self._order_failed(str(event.client_order_id), "expired")

    # 校验和标的检查完成后，进入市场判断函数并提交其生成的订单。
    def _handle_agent_message(self, payload: dict[str, Any]) -> None:
        self._check_decision(payload)
        self.log.info(
            f"agent_json_received payload="
            f"{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}",
        )
        event_id = payload["event_id"]
        if event_id not in self.event_batch:
            raise ValueError(f"event is not active in schedule: {event_id}")
        if event_id in self.seen_events:
            self.log.warning(f"agent_json_duplicate event_id={event_id}")
            return
        self.seen_events.add(event_id)

        if payload["decision"] == "HOLD":
            self.log.info(f"agent_decision_hold event_id={event_id}")
            return

        trades: list[dict[str, Any]] = []
        for trade in payload["trades"]:
            instrument_id = InstrumentId.from_str(trade["instrument_id"])
            if (
                instrument_id not in self.instrument_set
                or self.cache.instrument(instrument_id) is None
            ):
                self.log.warning(
                    f"agent_trade_ignored instrument_not_eligible={instrument_id}",
                )
                continue
            trades.append(trade)
        if not trades:
            self.log.warning(f"agent_decision_no_instruments event_id={event_id}")
            return

        checked = dict(payload)
        checked["trades"] = trades
        orders = self._judge_market(checked)
        for order in orders:
            self.pending[str(order.client_order_id)] = PendingOrder(
                event_id=event_id,
                instrument_id=order.instrument_id,
                target_qty=order.quantity.as_decimal(),
            )
        for order in orders:
            self.submit_order(order)

    # 根据财报前一小时标价均值和当前标价筛选并生成市场单。
    def _judge_market(self, payload: dict[str, Any]) -> list[Order]:
        event_id = payload["event_id"]
        batch_id = self.event_batch[event_id]
        start_ns, end_ns = self.windows[batch_id]
        account = self._account()
        if account is None:
            self.log.error(f"market_judgment_no_account event_id={event_id}")
            return []

        selected: list[tuple[dict[str, Any], Any, MarkPriceUpdate, Decimal]] = []
        for trade in payload["trades"]:
            instrument_id = InstrumentId.from_str(trade["instrument_id"])
            instrument = self.cache.instrument(instrument_id)
            minutes = self.mark_minutes[batch_id].get(instrument_id, {})
            expected_minutes = range(start_ns, end_ns, MINUTE_NS)
            if any(minute_ns not in minutes for minute_ns in expected_minutes):
                self.log.warning(
                    f"market_judgment_incomplete_baseline event_id={event_id} "
                    f"instrument={instrument_id} minutes={len(minutes)}",
                )
                continue
            reference = self._base_price(minutes, start_ns, end_ns)

            mark = self.cache.mark_price(instrument_id)
            if mark is None:
                self.log.warning(
                    f"market_judgment_no_mark event_id={event_id} instrument={instrument_id}",
                )
                continue
            age_ns = self.clock.timestamp_ns() - int(mark.ts_init)
            if not 0 <= age_ns <= self.max_mark_age_ns:
                self.log.warning(
                    f"market_judgment_stale_mark event_id={event_id} "
                    f"instrument={instrument_id} age_ns={age_ns}",
                )
                continue
            current = mark.value.as_decimal()
            expected = Decimal(str(trade["expected_move_pct"]))
            signal = trade["signal"]
            side = self._signal_side(signal)
            move, remaining = self._price_space(
                reference,
                current,
                side,
                expected,
            )
            self.log.info(
                f"market_judgment event_id={event_id} instrument={instrument_id} "
                f"signal={signal} reference={reference} current={current} "
                f"move_pct={move} expected_pct={expected} remaining_pct={remaining}",
            )
            if remaining < self.min_remaining:
                continue
            if self.cache.orders_open(instrument_id=instrument_id):
                self.log.warning(f"market_judgment_open_order instrument={instrument_id}")
                continue
            if self.cache.positions_open(instrument_id=instrument_id):
                self.log.warning(f"market_judgment_open_position instrument={instrument_id}")
                continue
            actual_leverage = account.leverage(instrument_id)
            if actual_leverage != self.leverage:
                self.log.error(
                    f"market_judgment_wrong_leverage instrument={instrument_id} "
                    f"expected={self.leverage} actual={actual_leverage}",
                )
                continue
            selected.append((trade, instrument, mark, remaining))

        if not selected:
            return []
        currency = selected[0][1].quote_currency
        available = account.balance_free(currency)
        free = available.as_decimal() if available is not None else Decimal("0")
        if free < self.margin_usdt:
            self.log.warning(
                f"market_judgment_insufficient_balance event_id={event_id} "
                f"available={free} required={self.margin_usdt} currency={currency}",
            )
            return []

        notional = self.margin_usdt * self.leverage / Decimal(len(selected))
        orders: list[Order] = []
        for trade, instrument, mark, _remaining in selected:
            if instrument.size_increment is None:
                self.log.warning(f"market_judgment_no_size_increment instrument={instrument.id}")
                continue
            size_step = instrument.size_increment.as_decimal()
            raw_qty = notional / mark.value.as_decimal()
            quantity = instrument.make_qty((raw_qty // size_step) * size_step)
            if quantity.as_decimal() <= 0:
                self.log.warning(f"market_judgment_zero_qty instrument={instrument.id}")
                continue
            if instrument.min_quantity is not None and quantity < instrument.min_quantity:
                self.log.warning(f"market_judgment_below_min_qty instrument={instrument.id}")
                continue
            actual_notional = instrument.notional_value(quantity, mark.value)
            if instrument.min_notional is not None and actual_notional < instrument.min_notional:
                self.log.warning(
                    f"market_judgment_below_min_notional instrument={instrument.id}",
                )
                continue
            side = self._signal_side(trade["signal"])
            order_side = OrderSide.BUY if side == "BUY" else OrderSide.SELL
            order = self.order_factory.market(
                instrument_id=instrument.id,
                order_side=order_side,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
            )
            orders.append(order)
            self.log.info(
                f"agent_order_ready event_id={event_id} instrument={instrument.id} "
                f"signal={trade['signal']} qty={quantity} notional={actual_notional}",
            )
        return orders

    def _account(self):
        return next(
            (
                account
                for account in self.cache.accounts()
                if str(account.id).upper().startswith("BINANCE")
            ),
            None,
        )

    def _order_failed(self, order_id: str, status: str) -> None:
        pending = self.pending.pop(order_id, None)
        if pending is not None:
            self.log.error(
                f"agent_order_{status} event_id={pending.event_id} "
                f"instrument={pending.instrument_id} order={order_id}",
            )

    # 对60个分钟均值再次等权平均，得到批次财报前基准价。
    @staticmethod
    def _base_price(
        minutes: dict[int, MarkBucket],
        start_ns: int,
        end_ns: int,
    ) -> Decimal:
        means = [
            minutes[minute_ns].total / minutes[minute_ns].count
            for minute_ns in range(start_ns, end_ns, MINUTE_NS)
        ]
        return sum(means, Decimal("0")) / Decimal(len(means))

    # 按交易方向计算市场已走幅度和剩余预期空间。
    @staticmethod
    def _price_space(
        reference: Decimal,
        current: Decimal,
        side: str,
        expected: Decimal,
    ) -> tuple[Decimal, Decimal]:
        move = (current / reference - Decimal("1")) * PERCENT
        if side == "SELL":
            move = -move
        return move, expected - move

    @staticmethod
    def _project_path(value: str) -> Path:
        path = (PROJECT_ROOT / value).resolve()
        path.relative_to(PROJECT_ROOT.resolve())
        return path

    # v3 signal 已包含方向，NT 不再要求 Agent 重复输出 side。
    @staticmethod
    def _signal_side(signal: str) -> str:
        return "BUY" if signal.endswith("_BUY") else "SELL"

    # 检查 NT 执行流程依赖的交易指令字段。
    @staticmethod
    def _check_decision(payload: dict[str, Any]) -> None:
        error = next(iter(DECISION_VALIDATOR.iter_errors(payload)), None)
        if error is not None:
            raise ValueError(f"trade decision schema mismatch: {error.message}")
        trades = payload["trades"]
        if (payload["decision"] == "HOLD") != (not trades):
            raise ValueError("HOLD must have no trades and TRADE must have trades")

        instruments = [trade["instrument_id"] for trade in trades]
        if len(instruments) != len(set(instruments)):
            raise ValueError("trade instruments must be unique")
