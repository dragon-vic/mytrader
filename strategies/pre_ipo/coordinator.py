from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from adapters.external_command import ExternalCommand
from adapters.external_command import external_command_type
from nautilus_trader.common.actor import Actor
from nautilus_trader.config import ActorConfig
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.identifiers import ClientId
from strategies.pre_ipo.pre_ipo_core import MINUTE_NS
from strategies.pre_ipo.pre_ipo_core import STATE_FLAT
from utils.arguments import EXTERNAL_COMMAND_CLIENT_NAME
from utils.control_messages import NODE_STOP_TOPIC
from utils.control_messages import NodeStopRequest


BIND_ENDPOINT = "PreIpoCoordinator.bind"


@dataclass
class StrategyBindRequest:
    strategy: object
    coordinator: PreIpoCoordinatorActor | None = None


@dataclass(frozen=True)
class MarginReservation:
    lock_amounts: dict[str, Decimal]
    required_amounts: dict[str, Decimal]


class PreIpoCoordinatorConfig(ActorConfig, frozen=True):
    assets: list[str]
    max_lock: Decimal
    reduce_risk_rate: Decimal
    stop_risk_rate: Decimal
    snapshot_path: str


class PreIpoCoordinatorActor(Actor):
    def __init__(self, config: PreIpoCoordinatorConfig) -> None:
        super().__init__(config)
        self.assets = [asset.upper() for asset in config.assets]
        self.max_lock = config.max_lock
        self.reduce_risk_rate = config.reduce_risk_rate
        self.stop_risk_rate = config.stop_risk_rate
        self.snapshot_path = Path(config.snapshot_path)
        self.strategies: dict[str, object] = {}
        self.reservations: dict[str, MarginReservation] = {}
        self.reservation_seq = 0
        self.housekeeping_seq = 0
        self.node_stop_requested = False
        self.node_stop_published = False

    # Actor 先于策略启动，先注册绑定入口并接收 monitor 命令。
    def on_start(self) -> None:
        self.msgbus.register(BIND_ENDPOINT, self._bind_strategy)
        self.subscribe_data(external_command_type(), client_id=ClientId(EXTERNAL_COMMAND_CLIENT_NAME))

    def on_stop(self) -> None:
        self.unsubscribe_data(external_command_type(), client_id=ClientId(EXTERNAL_COMMAND_CLIENT_NAME))
        self.msgbus.deregister(BIND_ENDPOINT, self._bind_strategy)

    # 策略在 on_start 完成自身初始化后绑定到唯一的协调器。
    def _bind_strategy(self, request: StrategyBindRequest) -> None:
        strategy = request.strategy
        asset = strategy.asset.upper()
        if asset not in self.assets:
            raise RuntimeError(f"coordinator_unexpected_strategy asset={asset}")
        if asset in self.strategies:
            raise RuntimeError(f"coordinator_duplicate_strategy asset={asset}")
        self.strategies[asset] = strategy
        request.coordinator = self
        self.log.info(f"coordinator_strategy_bound asset={asset} strategy_id={strategy.id}")
        if set(self.strategies) == set(self.assets):
            now_ns = self.clock.timestamp_ns()
            metrics = self._collect_metrics()
            self._check_risk(metrics)
            self._write_snapshot(now_ns, metrics)
            self._schedule_housekeeping()

    # 外部命令由 Actor 统一路由，策略不再直接订阅 TCP command。
    def on_data(self, data) -> None:
        if isinstance(data, ExternalCommand):
            command = data
        elif isinstance(data, CustomData) and isinstance(data.data, ExternalCommand):
            command = data.data
        else:
            return
        if set(self.strategies) != set(self.assets):
            self.log.warning("coordinator_command_ignored reason=strategies_not_ready")
            return
        target = command.target.strip().upper()
        targets = list(self.strategies.values()) if target == "ALL" else [self.strategies[target]]
        name = command.command.strip().lower()
        if name == "stop" and target == "ALL":
            self.node_stop_requested = True
        for strategy in targets:
            strategy.handle_command(name, command.source, command.reason)
        self._check_node_stop()

    # submit 前同步预留两边保证金；同一事件循环保证检查和写入原子执行。
    def reserve(self, strategy, lock_amounts: dict[str, Decimal]) -> str | None:
        asset = strategy.asset.upper()
        if self.strategies.get(asset) is not strategy:
            raise RuntimeError(f"coordinator_strategy_not_bound asset={asset}")
        metrics = self._collect_metrics()
        required_amounts = {
            venue: amount * strategy.margin_buffer
            for venue, amount in lock_amounts.items()
        }
        for venue, new_lock in lock_amounts.items():
            current_lock = metrics[venue]["locked_usdt"]
            projected_lock = current_lock + self._reserved_lock_usdt(venue) + new_lock
            if projected_lock > self.max_lock:
                self.log.warning(
                    f"margin_reservation_rejected asset={asset} venue={venue} reason=max_lock "
                    f"projected={projected_lock} max_lock={self.max_lock}",
                )
                return None
            required = required_amounts[venue]
            available = metrics[venue]["available_usdt"]
            if available is None:
                self.log.warning(
                    f"margin_reservation_rejected asset={asset} venue={venue} reason=balance_missing",
                )
                return None
            if available < required:
                self.log.warning(
                    f"margin_reservation_rejected asset={asset} venue={venue} "
                    f"available={available} required={required}",
                )
                return None
        self.reservation_seq += 1
        reservation_id = f"{asset}-{self.reservation_seq}"
        self.reservations[reservation_id] = MarginReservation(
            lock_amounts=dict(lock_amounts),
            required_amounts=required_amounts,
        )
        return reservation_id

    def release(self, reservation_id: str | None) -> None:
        if reservation_id is not None:
            self.reservations.pop(reservation_id)

    # 策略仓位完成变化后立即检查全局停止条件。
    def position_changed(self) -> None:
        self._check_node_stop()

    def _run_housekeeping(self) -> None:
        now_ns = self.clock.timestamp_ns()
        for strategy in self.strategies.values():
            strategy.run_housekeeping(now_ns)
        metrics = self._collect_metrics()
        self._check_risk(metrics)
        self._write_snapshot(now_ns, metrics)
        self._schedule_housekeeping()

    def _schedule_housekeeping(self) -> None:
        self.housekeeping_seq += 1
        now_ns = self.clock.timestamp_ns()
        next_minute_ns = (now_ns // MINUTE_NS + 1) * MINUTE_NS
        self.clock.set_time_alert_ns(
            f"pre_ipo_coordinator_housekeeping_{self.housekeeping_seq}",
            next_minute_ns,
            callback=lambda _event: self._run_housekeeping(),
            allow_past=True,
        )

    # 汇总两个策略在每个 venue 的估算保证金和未实现盈亏。
    def _aggregate_metrics(self) -> dict[str, dict[str, Decimal | None]]:
        totals = {
            "BINANCE": {"locked_usdt": Decimal("0"), "unrealized_usdt": Decimal("0")},
            "OKX": {"locked_usdt": Decimal("0"), "unrealized_usdt": Decimal("0")},
        }
        for strategy in self.strategies.values():
            for venue, values in strategy.metrics.venues.items():
                for key, value in (
                    ("locked_usdt", values.locked_usdt),
                    ("unrealized_usdt", values.unrealized_usdt),
                ):
                    current = totals[venue][key]
                    totals[venue][key] = None if value is None or current is None else current + value
        return totals

    # 每轮只读取一次账户字段，并与策略缓存合成 Actor 使用的唯一指标集。
    def _collect_metrics(self) -> dict[str, dict[str, Decimal | None]]:
        metrics = self._aggregate_metrics()
        for venue, values in metrics.items():
            account = self._account_for_venue(venue)
            total = self._money_decimal(account.balance_total(USDT))
            free = self._money_decimal(account.balance_free(USDT))
            account_locked = self._money_decimal(account.balance_locked(USDT))
            reserved = self._reserved_required_usdt(venue)
            strategy_locked = values["locked_usdt"]
            available = None if total is None or strategy_locked is None else total - strategy_locked - reserved
            unrealized = values["unrealized_usdt"]
            risk_rate = None
            if total is not None and unrealized is not None:
                loss = max(-unrealized, Decimal("0"))
                risk_rate = loss / total if total > 0 else Decimal("1" if loss > 0 else "0")
            values.update({
                "account_id": str(account.id),
                "account_total_usdt": total,
                "account_free_usdt": free,
                "account_locked_usdt": account_locked,
                "reserved_usdt": reserved,
                "available_usdt": available,
                "risk_rate": risk_rate,
            })
        return metrics

    # 账户级风险超过阈值时同时控制两个策略。
    def _check_risk(self, metrics: dict[str, dict[str, Decimal | None]]) -> None:
        max_rate = Decimal("0")
        for values in metrics.values():
            risk_rate = values["risk_rate"]
            if risk_rate is not None:
                max_rate = max(max_rate, risk_rate)
        if max_rate >= self.stop_risk_rate and not self.node_stop_requested:
            self.node_stop_requested = True
            self.log.error(f"account_risk_stop risk_rate={max_rate * Decimal('100'):.2f}%")
            for strategy in self.strategies.values():
                strategy.handle_command("stop", "coordinator", "account_risk")
        elif max_rate >= self.reduce_risk_rate:
            for strategy in self.strategies.values():
                if strategy.mode == "normal":
                    strategy.handle_command("reduce", "coordinator", "account_risk")
        self._check_node_stop()

    def _check_node_stop(self) -> None:
        if (
            self.node_stop_requested
            and not self.node_stop_published
            and set(self.strategies) == set(self.assets)
            and all(strategy.trade_state == STATE_FLAT for strategy in self.strategies.values())
        ):
            self.node_stop_published = True
            self.msgbus.publish(
                NODE_STOP_TOPIC,
                NodeStopRequest(source="pre_ipo_coordinator", reason="all_flat"),
            )

    def _reserved_lock_usdt(self, venue: str) -> Decimal:
        return sum(
            (reservation.lock_amounts.get(venue, Decimal("0")) for reservation in self.reservations.values()),
            Decimal("0"),
        )

    def _reserved_required_usdt(self, venue: str) -> Decimal:
        return sum(
            (reservation.required_amounts.get(venue, Decimal("0")) for reservation in self.reservations.values()),
            Decimal("0"),
        )

    def _account_for_venue(self, venue: str):
        venue_text = venue.upper()
        return next(account for account in self.cache.accounts() if str(account.id).upper().startswith(venue_text))

    def _money_decimal(self, money) -> Decimal | None:
        return None if money is None else money.as_decimal()

    def _fmt(self, value: object, suffix: str = "") -> str:
        if value is None:
            return "-"
        return f"{Decimal(str(value)):.2f}{suffix}"

    def _write_snapshot(self, now_ns: int, metrics: dict[str, dict[str, Decimal | None]]) -> None:
        accounts = {}
        risk = {}
        for venue, values in metrics.items():
            total = values["account_total_usdt"]
            free = values["account_free_usdt"]
            locked = values["account_locked_usdt"]
            strategy_locked = values["locked_usdt"]
            reserved = values["reserved_usdt"]
            available = values["available_usdt"]
            accounts[venue] = {
                "account_id": values["account_id"],
                "total_usdt": self._fmt(total),
                "free_usdt": self._fmt(free),
                "locked_usdt": self._fmt(locked),
            }
            risk[venue] = {
                "locked_usdt": self._fmt(strategy_locked),
                "reserved_usdt": self._fmt(reserved),
                "available_usdt": self._fmt(available),
                "unrealized_usdt": self._fmt(values["unrealized_usdt"]),
                "risk_rate": self._fmt(values["risk_rate"] * Decimal("100"), "%")
                if values["risk_rate"] is not None else "-",
            }
        data = {
            "time_ns": now_ns,
            "accounts": accounts,
            "risk": risk,
            "strategies": {
                asset: strategy.snapshot(now_ns)
                for asset, strategy in self.strategies.items()
            },
        }
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.snapshot_path.with_suffix(f"{self.snapshot_path.suffix}.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.snapshot_path)
