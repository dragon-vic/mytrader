from __future__ import annotations

import json
import time
from collections import deque
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from adapters.external_command import ExternalCommand
from adapters.external_command import external_command_type
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import QuoteTick
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
from strategies.pre_ipo.anthropic_core import ASSET
from strategies.pre_ipo.anthropic_core import BPS
from strategies.pre_ipo.anthropic_core import BINANCE_EXIT_SLIPPAGE_BPS
from strategies.pre_ipo.anthropic_core import LONG_EDGE
from strategies.pre_ipo.anthropic_core import MINUTE_NS
from strategies.pre_ipo.anthropic_core import OKX_EXIT_SLIPPAGE_BPS
from strategies.pre_ipo.anthropic_core import SHORT_EDGE
from strategies.pre_ipo.anthropic_core import STATE_FLAT
from strategies.pre_ipo.anthropic_core import STATE_LONG
from strategies.pre_ipo.anthropic_core import STATE_PENDING
from strategies.pre_ipo.anthropic_core import STATE_SHORT
from strategies.pre_ipo.anthropic_core import STATE_UNBALANCE
from strategies.pre_ipo.anthropic_core import EdgePair
from strategies.pre_ipo.anthropic_core import PendingLeg
from strategies.pre_ipo.anthropic_core import PendingPair
from strategies.pre_ipo.anthropic_core import PositionState
from utils.arguments import EXTERNAL_COMMAND_CLIENT_NAME
from utils.arguments import NODE_STOP_TOPIC


BEIJING_TZ = timezone(timedelta(hours=8))
COLLECTOR_COLUMNS = ("ts_local_ns", "ts_exchange_ms", "venue", "symbol", "bid", "ask", "bid_size", "ask_size")


class AnthropicArbConfig(StrategyConfig, frozen=True):
    instruments: list[str]
    window_minutes: Decimal
    snapshot_path: str
    okx_multiplier: Decimal
    entry_bps: Decimal
    exit_bps: Decimal
    std_mult: Decimal
    long_max_bps: Decimal
    short_min_bps: Decimal
    max_position: Decimal
    qty: Decimal
    margin_leverage: Decimal
    margin_buffer: Decimal


class AnthropicArbStrategy(Strategy):
    def __init__(self, config: AnthropicArbConfig) -> None:
        super().__init__(config)
        instruments = [InstrumentId.from_str(value) for value in config.instruments]
        self.binance_id = instruments[0]
        self.okx_id = instruments[1]
        self.instrument_ids = [self.binance_id, self.okx_id]
        self.quotes: dict[InstrumentId, QuoteTick] = {}
        self.housekeeping_interval_ns = 1_000_000_000
        self.snapshot_path = Path(config.snapshot_path)
        self.edge = EdgePair(
            window_ns=int(config.window_minutes * Decimal(MINUTE_NS)),
            okx_price_multiplier=config.okx_multiplier,
            long_mean_bps=Decimal("0"),
            short_mean_bps=Decimal("0"),
            long_std_bps=Decimal("0"),
            short_std_bps=Decimal("0"),
            entry_bps=config.entry_bps,
            exit_bps=config.exit_bps,
            std_mult=config.std_mult,
            long_max_bps=config.long_max_bps,
            short_min_bps=config.short_min_bps,
        )
        self.qty = config.qty
        self.okx_qty_multiplier = config.okx_multiplier
        self.max_position = config.max_position
        self.margin_leverage = config.margin_leverage
        self.margin_buffer = config.margin_buffer
        self.trade_state = STATE_FLAT
        self.mode = "normal"
        self.fail_count = 0
        self.pending: PendingPair | None = None
        self.positions = {
            self.binance_id: PositionState(self.binance_id),
            self.okx_id: PositionState(self.okx_id),
        }
        self.housekeeping_seq = 0
        self.action_rows: deque[dict[str, str]] = deque(maxlen=200)

    # 策略启动入口：检查空仓、初始化窗口、订阅 quote 和外部命令。
    def on_start(self) -> None:
        self._warm_initial_window()
        self._check_startup()
        for instrument_id in self.instrument_ids:
            self.subscribe_quote_ticks(instrument_id)
        self.subscribe_data(external_command_type(), client_id=ClientId(EXTERNAL_COMMAND_CLIENT_NAME))
        self._schedule_housekeeping()
        self._write_snapshot(self.clock.timestamp_ns())

    # 策略停止入口：stop mode 已负责减仓，这里只取消订阅。
    def on_stop(self) -> None:
        self.unsubscribe_data(external_command_type(), client_id=ClientId(EXTERNAL_COMMAND_CLIENT_NAME))
        for instrument_id in self.instrument_ids:
            self.unsubscribe_quote_ticks(instrument_id)

    # 外部命令入口：monitor 通过 external_command 控制 normal/reduce/stop。
    def on_data(self, data) -> None:
        if isinstance(data, ExternalCommand):
            command = data
        elif isinstance(data, CustomData) and isinstance(data.data, ExternalCommand):
            command = data.data
        else:
            return
        name = command.command.strip().lower()
        if name == "stop":
            self.log.warning(f"external_command_stop source={command.source} reason={command.reason}")
            if self.trade_state == STATE_FLAT:
                self.msgbus.publish(NODE_STOP_TOPIC, {"source": "anthropic_arb", "reason": "external_stop_flat"})
                return
            self.mode = "stop"
            return
        if name == "reduce":
            self.mode = "reduce"
            self.log.warning(f"mode_reduce source={command.source}")
            return
        if name == "normal":
            self.mode = "normal"
            self.log.warning(f"mode_normal source={command.source}")
            return
        self.log.warning(f"external_command_ignored command={command.command} source={command.source}")

    # quote 主入口：更新 edge，生成 signal，检查通过后提交双腿订单。
    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.quotes[tick.instrument_id] = tick
        self.edge.record_quote(tick, self.binance_id, self.okx_id)
        self.edge.update(self.quotes[self.binance_id], self.quotes[self.okx_id])
        if self.trade_state not in {STATE_PENDING, STATE_UNBALANCE}:
            signal = self.edge.signal(self.trade_state)
            signal = self._checked_signal(signal)
            if signal is not None:
                self._submit_signal(signal, tick)

    # 成交事件入口：先更新仓位账本，再推进 pending 生命周期。
    def on_order_filled(self, event: OrderFilled) -> None:
        order_id = str(event.client_order_id)
        event_qty = self._event_qty(event)
        event_px = self._event_px(event)
        self.positions[event.instrument_id].apply_fill(event.order_side, event_qty, event_px, self._event_fee(event))
        if self.pending is None or not self.pending.has_order(order_id):
            return
        self.pending.record_fill(
            order_id,
            event_qty,
            event_px,
            int(event.ts_event),
        )
        self._resolve_pending_if_done()

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def on_order_denied(self, event: OrderDenied) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def on_order_canceled(self, event: OrderCanceled) -> None:
        self._mark_order_failed(str(event.client_order_id))

    def on_order_expired(self, event: OrderExpired) -> None:
        self._mark_order_failed(str(event.client_order_id))

    # 启动检查集中在这里，避免策略接管外部仓位或缺少交易前置数据。
    def _check_startup(self) -> None:
        for instrument_id in self.instrument_ids:
            if self.cache.instrument(instrument_id) is None:
                raise RuntimeError(f"startup_instrument_missing instrument={instrument_id}")
            positions = self.cache.positions_open(instrument_id=instrument_id)
            if positions:
                raise RuntimeError(f"start_position_not_empty instrument={instrument_id} positions={len(positions)}")
            venue = self._venue(instrument_id)
            if self._account_for_venue(venue) is None:
                raise RuntimeError(f"startup_account_missing venue={venue} instrument={instrument_id}")
            if instrument_id not in self.quotes:
                raise RuntimeError(f"startup_quote_missing instrument={instrument_id}")
        if self.edge.long_bps is None or self.edge.short_bps is None:
            raise RuntimeError("startup_edge_missing")

    # 从 NT fill 事件取成交数量。
    def _event_qty(self, event: OrderFilled) -> Decimal:
        return Decimal(str(event.last_qty))

    # 从 NT fill 事件取成交价。
    def _event_px(self, event: OrderFilled) -> Decimal:
        return Decimal(str(event.last_px).split()[0].replace("_", ""))

    # 从 NT fill 事件取手续费。
    def _event_fee(self, event: OrderFilled) -> Decimal:
        commission = getattr(event, "commission", None)
        if commission is None:
            return Decimal("0")
        return Decimal(str(commission).split()[0].replace("_", ""))

    # signal 生成后的所有交易前检查集中在这里。
    def _checked_signal(self, signal: str | None) -> str | None:
        if self.mode == "suspend":
            return None
        if self.mode == "stop":
            if self.trade_state == STATE_FLAT:
                self.mode = "normal"
                self.msgbus.publish(NODE_STOP_TOPIC, {"source": "anthropic_arb", "reason": "stop_mode_flat"})
                return None
            if self.qty == 0:
                return None
            signal = "short" if self.trade_state == STATE_LONG else "long"
        if signal is None:
            return None
        if self.qty == 0:
            return None
        if self.mode == "reduce":
            if self.trade_state == STATE_LONG and signal != "short":
                return None
            if self.trade_state == STATE_SHORT and signal != "long":
                return None
            if self.trade_state == STATE_FLAT:
                return None
        before_inventory = self._inventory()
        trade_qty = self._trade_qty(signal, before_inventory)
        if trade_qty == 0:
            return None
        after_inventory = before_inventory + trade_qty if signal == "long" else before_inventory - trade_qty
        if abs(after_inventory) > self.max_position:
            self.log.warning(
                f"signal_rejected_max_position signal={signal} before={before_inventory} "
                f"after={after_inventory} max_position={self.max_position}",
            )
            return None
        opens_inventory = (
            after_inventory != 0
            and (
                before_inventory == 0
                or (before_inventory > 0) != (after_inventory > 0)
                or abs(after_inventory) > abs(before_inventory)
            )
        )
        if opens_inventory and not self._balance_allowed(signal):
            return None
        return signal

    # stop/reduce 模式只减仓，不允许一次反向 signal 翻仓。
    def _trade_qty(self, signal: str, before_inventory: Decimal) -> Decimal:
        if self.mode in {"stop", "reduce"} and before_inventory != 0:
            if before_inventory > 0 and signal == "short":
                return min(self.qty, before_inventory)
            if before_inventory < 0 and signal == "long":
                return min(self.qty, abs(before_inventory))
        return self.qty

    # 开仓或加仓前做保证金余额检查。
    def _balance_allowed(self, signal: str) -> bool:
        for instrument_id, side, qty in self._signal_legs(signal):
            account = self._account_for_venue(self._venue(instrument_id))
            if account is None:
                self.log.warning(f"signal_rejected_balance signal={signal} venue={self._venue(instrument_id)} reason=no_account")
                return False
            quote = self.quotes[instrument_id]
            price = Decimal(str(quote.ask_price if side == OrderSide.BUY else quote.bid_price))
            required = price * qty / self.margin_leverage * self.margin_buffer
            money = account.balance_free(USDT)
            free = Decimal(str(money.as_decimal())) if hasattr(money, "as_decimal") else Decimal(str(money))
            if free < required:
                self.log.warning(
                    f"signal_rejected_balance signal={signal} venue={self._venue(instrument_id)} "
                    f"free={free} required={required}",
                )
                return False
        return True

    # 把 long/short signal 转为两条实际订单腿。
    def _signal_legs(self, signal: str, base_qty: Decimal | None = None) -> list[tuple[InstrumentId, OrderSide, Decimal]]:
        qty = self.qty if base_qty is None else base_qty
        if signal == "long":
            return [
                (self.okx_id, OrderSide.BUY, qty * self.okx_qty_multiplier),
                (self.binance_id, OrderSide.SELL, qty),
            ]
        return [
            (self.binance_id, OrderSide.BUY, qty),
            (self.okx_id, OrderSide.SELL, qty * self.okx_qty_multiplier),
        ]

    # 注册下一次 housekeeping 定时任务。
    def _schedule_housekeeping(self) -> None:
        self.housekeeping_seq += 1
        self.clock.set_time_alert_ns(
            f"anthropic_arb_housekeeping_{self.housekeeping_seq}",
            self.clock.timestamp_ns() + self.housekeeping_interval_ns,
            callback=lambda _event: self._on_housekeeping(),
            allow_past=True,
        )

    # 低频维护任务：更新分钟窗口、mark 仓位浮盈亏、写 snapshot。
    def _on_housekeeping(self) -> None:
        now_ns = self.clock.timestamp_ns()
        self.edge.fill_to(now_ns, self.binance_id, self.okx_id)
        for instrument_id, position in self.positions.items():
            quote = self.quotes[instrument_id]
            slippage_bps = BINANCE_EXIT_SLIPPAGE_BPS if instrument_id == self.binance_id else OKX_EXIT_SLIPPAGE_BPS
            position.mark(Decimal(str(quote.bid_price)), Decimal(str(quote.ask_price)), slippage_bps)
        self._write_snapshot(now_ns)
        self._schedule_housekeeping()

    # 生成策略私有 snapshot；monitor 会按新 schema 单独适配。
    def _build_snapshot(self, now_ns: int) -> dict[str, object]:
        quotes = {
            self._venue(instrument_id): {
                "instrument": str(instrument_id),
                "bid": self._fmt(Decimal(str(quote.bid_price))),
                "ask": self._fmt(Decimal(str(quote.ask_price))),
                "bid_size": self._fmt(Decimal(str(quote.bid_size))),
                "ask_size": self._fmt(Decimal(str(quote.ask_size))),
                "ts_event": str(quote.ts_event),
                "age_ms": self._fmt((now_ns - int(quote.ts_event)) / 1_000_000),
            }
            for instrument_id, quote in self.quotes.items()
        }
        positions = {
            self._venue(instrument_id): {
                "instrument": str(instrument_id),
                "qty": self._fmt(position.signed_qty),
                "avg_px": self._fmt(position.avg_px),
                "realized_usdt": self._fmt(position.realized_pnl),
                "unrealized_usdt": self._fmt(position.unrealized_pnl),
                "fee_usdt": self._fmt(position.fee),
            }
            for instrument_id, position in self.positions.items()
        }
        pending = None
        if self.pending is not None:
            actual_edge = self._actual_edge_bps(self.pending)
            edge_slip = self._edge_slippage_bps(self.pending, actual_edge)
            pending = {
                "signal": self.pending.signal,
                "edge_side": self.pending.edge_side,
                "signal_edge_bps": self._fmt(self.pending.signal_edge_bps),
                "mean_bps": self._fmt(self.pending.mean_bps),
                "signal_event_ns": str(self.pending.signal_event_ns),
                "signal_ts_ns": str(self.pending.signal_ts_ns),
                "edge_slip": self._fmt(edge_slip),
                "fill_slip": self._fmt(self._fill_slippage_bps(self.pending)),
                "age_sec": self._fmt(max((now_ns - self.pending.created_ns) / 1_000_000_000, 0.0)),
                "orders": [
                    {
                        "order_id": leg.order_id,
                        "kind": "main",
                        "instrument": str(leg.instrument_id),
                        "side": str(leg.side),
                        "filled_qty": self._fmt(leg.filled_qty),
                        "target_qty": self._fmt(leg.target_qty),
                        "submit_ns": str(leg.submit_ns) if leg.submit_ns is not None else "-",
                        "fill_event_ns": str(leg.fill_event_ns) if leg.fill_event_ns is not None else "-",
                        "full_fill_event_ns": str(leg.full_fill_event_ns) if leg.full_fill_event_ns is not None else "-",
                        "failed": leg.failed,
                    }
                    for leg in self.pending.legs.values()
                ] + [
                    {
                        "order_id": leg.order_id,
                        "kind": "repair",
                        "instrument": str(leg.instrument_id),
                        "side": str(leg.side),
                        "filled_qty": self._fmt(leg.filled_qty),
                        "target_qty": self._fmt(leg.target_qty),
                        "submit_ns": str(leg.submit_ns) if leg.submit_ns is not None else "-",
                        "fill_event_ns": str(leg.fill_event_ns) if leg.fill_event_ns is not None else "-",
                        "full_fill_event_ns": str(leg.full_fill_event_ns) if leg.full_fill_event_ns is not None else "-",
                        "failed": leg.failed,
                    }
                    for leg in self.pending.repairs.values()
                ],
            }
        return {
            "strategy": "anthropic_arb",
            "asset": ASSET,
            "time_ns": now_ns,
            "state": self.trade_state,
            "mode": self.mode,
            "inventory": self._fmt(self._inventory()),
            "quotes": quotes,
            "edge": {
                "long_bps": self._fmt(self.edge.long_bps),
                "long_mean_bps": self._fmt(self.edge.long_mean_bps),
                "long_std_bps": self._fmt(self.edge.long_std_bps),
                "short_bps": self._fmt(self.edge.short_bps),
                "short_mean_bps": self._fmt(self.edge.short_mean_bps),
                "short_std_bps": self._fmt(self.edge.short_std_bps),
            },
            "positions": positions,
            "pnl": {
                "realized_usdt": self._fmt(sum((position.realized_pnl for position in self.positions.values()), Decimal("0"))),
                "unrealized_usdt": self._fmt(sum((position.unrealized_pnl for position in self.positions.values()), Decimal("0"))),
                "fee_usdt": self._fmt(sum((position.fee for position in self.positions.values()), Decimal("0"))),
            },
            "quote_counts": list(self.edge.minute_counts),
            "pending": pending,
            "actions": list(self.action_rows),
        }

    # 原子写 snapshot，避免 monitor 读到半截 JSON。
    def _write_snapshot(self, now_ns: int) -> None:
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        data = self._build_snapshot(now_ns)
        tmp = self.snapshot_path.with_suffix(f"{self.snapshot_path.suffix}.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.snapshot_path)

    # 提交双腿市价单；submit 前先写 pending，避免同步事件早于状态。
    def _submit_signal(self, signal: str, tick: QuoteTick) -> None:
        before_inventory = self._inventory()
        base_qty = self._trade_qty(signal, before_inventory)
        self.trade_state = STATE_PENDING
        signal_event_ns = int(tick.ts_event)
        signal_ts_ns = int(tick.ts_init)
        if signal == "long":
            buy_id, sell_id = self.okx_id, self.binance_id
            buy_qty, sell_qty = base_qty * self.okx_qty_multiplier, base_qty
            edge_side = LONG_EDGE
            signal_edge = self.edge.long_bps
            mean_bps = self.edge.long_mean_bps
            after_inventory = before_inventory + base_qty
        else:
            buy_id, sell_id = self.binance_id, self.okx_id
            buy_qty, sell_qty = base_qty, base_qty * self.okx_qty_multiplier
            edge_side = SHORT_EDGE
            signal_edge = self.edge.short_bps
            mean_bps = self.edge.short_mean_bps
            after_inventory = before_inventory - base_qty

        buy_quantity = self.cache.instrument(buy_id).make_qty(buy_qty)
        sell_quantity = self.cache.instrument(sell_id).make_qty(sell_qty)
        buy_order = self.order_factory.market(
            instrument_id=buy_id,
            order_side=OrderSide.BUY,
            quantity=buy_quantity,
            time_in_force=TimeInForce.GTC,
        )
        sell_order = self.order_factory.market(
            instrument_id=sell_id,
            order_side=OrderSide.SELL,
            quantity=sell_quantity,
            time_in_force=TimeInForce.GTC,
        )
        legs = {
            str(buy_order.client_order_id): PendingLeg(
                order_id=str(buy_order.client_order_id),
                instrument_id=buy_id,
                side=OrderSide.BUY,
                target_qty=Decimal(str(buy_quantity)),
                quote_px=Decimal(str(self.quotes[buy_id].ask_price)),
                submit_ns=self.clock.timestamp_ns(),
            ),
            str(sell_order.client_order_id): PendingLeg(
                order_id=str(sell_order.client_order_id),
                instrument_id=sell_id,
                side=OrderSide.SELL,
                target_qty=Decimal(str(sell_quantity)),
                quote_px=Decimal(str(self.quotes[sell_id].bid_price)),
                submit_ns=self.clock.timestamp_ns(),
            ),
        }
        self.pending = PendingPair(
            legs=legs,
            signal=signal,
            edge_side=edge_side,
            signal_edge_bps=signal_edge,
            mean_bps=mean_bps,
            signal_event_ns=signal_event_ns,
            signal_ts_ns=signal_ts_ns,
            created_ns=self.clock.timestamp_ns(),
            before_inventory=before_inventory,
            after_inventory=after_inventory,
        )
        self.submit_order(buy_order)
        self.submit_order(sell_order)

    # 按 Binance 腿库存同步 flat/long/short 状态。
    def _sync_state_from_inventory(self) -> None:
        positions = self.cache.positions_open(instrument_id=self.binance_id, strategy_id=self.id)
        if not positions:
            self.trade_state = STATE_FLAT
            return
        position = positions[0]
        self.trade_state = STATE_SHORT if bool(position.is_long) else STATE_LONG

    # 记录 pending 订单失败，等两腿都有最终反馈后统一处理。
    def _mark_order_failed(self, order_id: str) -> None:
        if self.pending is not None and self.pending.has_order(order_id):
            leg = self.pending.leg(order_id)
            self.log.error(
                f"pending_order_failed order={order_id} instrument={leg.instrument_id} side={leg.side} "
                f"filled={leg.filled_qty} target={leg.target_qty}",
            )
            self.pending.record_failed(order_id)
            self._resolve_pending_if_done()

    # pending 生命周期收口：成功、全失败、单腿失败补反向单。
    def _resolve_pending_if_done(self) -> None:
        pending = self.pending
        if pending is None or not pending.is_done():
            return
        if pending.is_complete():
            self._finish_record("filled")
            return
        if pending.has_repairs():
            failed_repairs = [leg for leg in pending.repairs.values() if leg.failed]
            if failed_repairs:
                self.mode = "suspend"
                failed_text = ",".join(leg.order_id for leg in failed_repairs)
                self.log.error(
                    f"repair_order_failed_suspend orders={failed_text}",
                )
                self._finish_record("repair_unbalance")
                return
            bn_qty = self._net_qty(self.binance_id)
            okx_qty = self._net_qty(self.okx_id)
            if bn_qty + okx_qty / self.okx_qty_multiplier == Decimal("0"):
                self._finish_record("repaired")
            else:
                self.mode = "suspend"
                self.log.error(f"repair_finished_still_unbalanced bn_qty={bn_qty} okx_qty={okx_qty}")
                self._finish_record("repair_unbalance")
            return
        if pending.is_all_failed():
            self.log.warning(f"pending_pair_all_failed orders={','.join(pending.legs)}")
            self.fail_count += 1
            if any(leg.filled_qty > 0 for leg in pending.legs.values()):
                self.trade_state = STATE_UNBALANCE
                self._submit_repair_orders()
            else:
                self._finish_record("failed")
            return
        self.log.error(f"pending_pair_one_leg_failed_enter_unbalance orders={','.join(pending.legs)}")
        self.fail_count += 1
        self.trade_state = STATE_UNBALANCE
        self._submit_repair_orders()

    # 按主订单已成交数量生成反向修复单，把账户退回下单前状态。
    def _submit_repair_orders(self) -> None:
        orders = []
        repairs = {}
        for leg in self.pending.legs.values():
            if leg.filled_qty <= 0:
                continue
            side = OrderSide.SELL if leg.side == OrderSide.BUY else OrderSide.BUY
            quantity = self.cache.instrument(leg.instrument_id).make_qty(leg.filled_qty)
            order = self.order_factory.market(
                instrument_id=leg.instrument_id,
                order_side=side,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
            )
            order_id = str(order.client_order_id)
            repairs[order_id] = PendingLeg(
                order_id=order_id,
                instrument_id=leg.instrument_id,
                side=side,
                target_qty=Decimal(str(quantity)),
                quote_px=Decimal(str(self.quotes[leg.instrument_id].ask_price if side == OrderSide.BUY else self.quotes[leg.instrument_id].bid_price)),
                submit_ns=self.clock.timestamp_ns(),
            )
            orders.append(order)
        if not repairs:
            self.mode = "suspend"
            self.log.error(f"repair_no_filled_qty orders={','.join(self.pending.legs)}")
            self._finish_record("repair_unbalance")
            return
        self.pending.repairs = repairs
        for order in orders:
            self.submit_order(order)

    # 从 NT cache 读取本策略当前 instrument 净持仓。
    def _net_qty(self, instrument_id: InstrumentId) -> Decimal:
        positions = self.cache.positions_open(instrument_id=instrument_id, strategy_id=self.id)
        if not positions:
            return Decimal("0")
        position = positions[0]
        qty = Decimal(str(position.quantity))
        return qty if bool(position.is_long) else -qty

    # 启动时用 collector 真实 bid/ask 初始化 6h 分钟窗口。
    def _warm_initial_window(self) -> None:
        end_ns = time.time_ns()
        end_minute_ns = end_ns // MINUTE_NS * MINUTE_NS
        start_minute_ns = end_minute_ns - (int(self.config.window_minutes) - 1) * MINUTE_NS
        read_start_ns = start_minute_ns // (60 * MINUTE_NS) * (60 * MINUTE_NS)
        paths = self._collector_quote_files(read_start_ns, end_ns)
        rows = self._load_collector_quotes(paths, read_start_ns, end_ns)
        self.edge.warm_from_rows(rows, start_minute_ns, end_minute_ns, self.binance_id, self.okx_id)
        self._seed_initial_quotes(rows)
        self.log.info(
            f"initial_window asset={ASSET} rows={len(rows)} long_mean={self.edge.long_mean_bps:.2f} "
            f"short_mean={self.edge.short_mean_bps:.2f}",
        )

    # 找到 collector 在窗口内的 merged/raw quote 文件。
    def _collector_quote_files(self, start_ns: int, end_ns: int) -> list[Path]:
        base_dir = Path(__file__).resolve().parent / "collector" / "bidask1-live"
        merged_dir = base_dir / "quote_merged"
        raw_dir = base_dir / "quote_raw"
        paths: list[Path] = []
        for key in self._collector_hour_keys(start_ns, end_ns):
            merged = merged_dir / ASSET / f"bidask1-{key}.parquet"
            if merged.exists():
                paths.append(merged)
            hour_dir = raw_dir / ASSET / key
            if hour_dir.exists():
                paths.extend(sorted(hour_dir.glob("*.parquet")))
        return sorted(set(paths), key=lambda path: str(path))

    # collector 文件按北京时间小时分桶。
    def _collector_hour_keys(self, start_ns: int, end_ns: int) -> list[str]:
        start = datetime.fromtimestamp(start_ns / 1_000_000_000, BEIJING_TZ).replace(minute=0, second=0, microsecond=0)
        end = datetime.fromtimestamp(end_ns / 1_000_000_000, BEIJING_TZ).replace(minute=0, second=0, microsecond=0)
        keys = []
        current = start
        while current <= end:
            keys.append(current.strftime("%Y%m%d%H"))
            current += timedelta(hours=1)
        return keys

    # 读取并裁剪 collector quote 行。
    def _load_collector_quotes(self, paths: list[Path], start_ns: int, end_ns: int) -> list[dict[str, object]]:
        if not paths:
            raise RuntimeError("no bidask1 collector parquet files found for initial window")
        dataset = ds.dataset([str(path) for path in paths], format="parquet")
        filt = (
            (pc.field("ts_local_ns") >= pa.scalar(start_ns, pa.int64()))
            & (pc.field("ts_local_ns") <= pa.scalar(end_ns, pa.int64()))
            & pc.field("symbol").isin([ASSET])
        )
        table = dataset.to_table(columns=list(COLLECTOR_COLUMNS), filter=filt)
        rows = table.to_pylist()
        if not rows:
            raise RuntimeError("no bidask1 collector rows found for initial window")
        return sorted(rows, key=lambda row: int(row["ts_local_ns"]))

    # 用 warmup 最后一条 bid/ask 构造初始 QuoteTick。
    def _seed_initial_quotes(self, rows: list[dict[str, object]]) -> None:
        latest: dict[str, dict[str, object]] = {}
        for row in rows:
            latest[str(row["venue"]).upper()] = row
        ts_init = self.clock.timestamp_ns()
        for instrument_id in self.instrument_ids:
            venue = self._venue(instrument_id)
            row = latest[venue]
            instrument = self.cache.instrument(instrument_id)
            bid_size = Decimal(str(row["bid_size"]))
            ask_size = Decimal(str(row["ask_size"]))
            ts_event = int(row["ts_exchange_ms"]) * 1_000_000 if int(row["ts_exchange_ms"]) > 0 else int(row["ts_local_ns"])
            self.quotes[instrument_id] = QuoteTick(
                instrument_id=instrument_id,
                bid_price=instrument.make_price(Decimal(str(row["bid"]))),
                ask_price=instrument.make_price(Decimal(str(row["ask"]))),
                bid_size=instrument.make_qty(bid_size if bid_size > 0 else self.qty),
                ask_size=instrument.make_qty(ask_size if ask_size > 0 else self.qty),
                ts_event=ts_event,
                ts_init=ts_init,
            )
        self.edge.update(self.quotes[self.binance_id], self.quotes[self.okx_id])

    # 统一用 Binance 腿方向表示策略库存：正数 long，负数 short。
    def _inventory(self) -> Decimal:
        return -self._net_qty(self.binance_id)

    # pending 生命周期结束时统一固化阶段性数据并清理状态。
    def _finish_record(self, status: str) -> None:
        row = self._action_row(self.pending, status)
        self.action_rows.appendleft(row)
        self.pending = None
        if status == "repair_unbalance":
            self.trade_state = STATE_UNBALANCE
        else:
            self._sync_state_from_inventory()
        if status == "filled":
            self.fail_count = 0
        elif self.fail_count > 2:
            self.mode = "suspend"

    # 记录一笔 pending 完成后的动作摘要。
    def _action_row(self, pending: PendingPair, status: str) -> dict[str, str]:
        actual_edge = self._actual_edge_bps(pending)
        edge_slippage = self._edge_slippage_bps(pending, actual_edge)
        latencies = {"BINANCE": "-", "OKX": "-"}
        for leg in pending.legs.values():
            venue = self._venue(leg.instrument_id)
            if leg.full_fill_event_ns is not None:
                latencies[venue] = self._fmt((leg.full_fill_event_ns - pending.signal_event_ns) / 1_000_000)
        ts_sec = pending.created_ns / 1_000_000_000
        return {
            "created_ns": str(pending.created_ns),
            "signal_event_ns": str(pending.signal_event_ns),
            "signal_ts_ns": str(pending.signal_ts_ns),
            "asset": ASSET,
            "action": self._display_action(pending.before_inventory, pending.after_inventory),
            "edge_side": pending.edge_side,
            "status": status,
            "qty": self._fmt(abs(self._inventory())),
            "signal_edge": self._fmt(pending.signal_edge_bps),
            "actual_edge": self._fmt(actual_edge),
            "edge_slippage": self._fmt(edge_slippage),
            "fill_slippage": "-",
            "mean": self._fmt(pending.mean_bps),
            "std": "-",
            "bn_latency": latencies["BINANCE"],
            "okx_latency": latencies["OKX"],
            "time": datetime.fromtimestamp(ts_sec, tz=timezone.utc).astimezone(BEIJING_TZ).strftime("%m-%d %H:%M:%S"),
        }

    # 用双腿成交均价计算实际成交 edge。
    def _actual_edge_bps(self, pending: PendingPair) -> Decimal | None:
        buy_id = self.okx_id if pending.signal == "long" else self.binance_id
        sell_id = self.binance_id if pending.signal == "long" else self.okx_id
        buy_avg = pending.avg_px(buy_id)
        sell_avg = pending.avg_px(sell_id)
        if buy_avg is None or sell_avg is None:
            return None
        if buy_id == self.okx_id:
            buy_avg *= self.edge.okx_price_multiplier
        if sell_id == self.okx_id:
            sell_avg *= self.edge.okx_price_multiplier
        if pending.edge_side == SHORT_EDGE:
            return (sell_avg - buy_avg) / buy_avg * BPS
        return (buy_avg - sell_avg) / sell_avg * BPS

    # 计算实际成交 edge 相对 signal edge 的滑点。
    def _edge_slippage_bps(self, pending: PendingPair, actual_edge: Decimal | None) -> Decimal | None:
        if actual_edge is None:
            return None
        if pending.edge_side == SHORT_EDGE:
            return actual_edge - pending.signal_edge_bps
        return pending.signal_edge_bps - actual_edge

    # 计算两条腿成交均价相对触发 bid/ask 的滑点合计。
    def _fill_slippage_bps(self, pending: PendingPair) -> Decimal | None:
        total = Decimal("0")
        seen = False
        for leg in pending.legs.values():
            if leg.filled_qty <= 0:
                continue
            avg_px = leg.filled_notional / leg.filled_qty
            if leg.side == OrderSide.BUY:
                total += (avg_px - leg.quote_px) / leg.quote_px * BPS
            else:
                total += (leg.quote_px - avg_px) / leg.quote_px * BPS
            seen = True
        return total if seen else None

    # 按 venue 找账户，用于下单前余额检查。
    def _account_for_venue(self, venue: str):
        venue_text = venue.upper()
        for account in self.cache.accounts():
            if str(account.id).upper().startswith(venue_text):
                return account
        return None

    # InstrumentId 的 venue 标准化为大写字符串。
    def _venue(self, instrument_id: InstrumentId) -> str:
        return str(instrument_id.venue).upper()

    # 根据库存变化显示 open/add/reduce/close。
    def _display_action(self, before: Decimal, after: Decimal) -> str:
        if before == 0 and after != 0:
            return "open"
        if before != 0 and after == 0:
            return "close"
        if abs(after) > abs(before):
            return "add"
        if abs(after) < abs(before):
            return "reduce"
        return "-"

    # snapshot 数值格式化；缺失值显示为短横线。
    def _fmt(self, value: object, suffix: str = "") -> str:
        if value is None:
            return "-"
        return f"{Decimal(str(value)):.2f}{suffix}"
