from __future__ import annotations

import csv
from decimal import Decimal
from decimal import ROUND_CEILING
from pathlib import Path

import pandas as pd
from actors.data_recorder import DataRecorder
from nautilus_trader.core.nautilus_pyo3 import BarType
from nautilus_trader.core.nautilus_pyo3 import ClientOrderId
from nautilus_trader.core.nautilus_pyo3 import InstrumentId
from nautilus_trader.core.nautilus_pyo3 import LogColor
from nautilus_trader.core.nautilus_pyo3 import MarketOrder
from nautilus_trader.core.nautilus_pyo3 import OrderSide
from nautilus_trader.core.nautilus_pyo3 import Strategy
from nautilus_trader.core.nautilus_pyo3 import StrategyConfig
from nautilus_trader.core.nautilus_pyo3 import TimeEvent
from nautilus_trader.core.nautilus_pyo3 import TimeInForce
from nautilus_trader.core.nautilus_pyo3 import TradeTick
from nautilus_trader.core.nautilus_pyo3 import UUID4


class BacktestfundingConfig(StrategyConfig):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    trade_notional: Decimal
    min_rate_bps: Decimal
    trade_symbols: list[str] | str
    exclude_symbols: list[str]
    events_path: str
    entry_before_ms: int
    exit_after_ms: int
    event_log_path: str

    def __new__(
        cls,
        instrument_ids: list[InstrumentId],
        bar_types: list[BarType],
        trade_notional: Decimal,
        min_rate_bps: Decimal,
        trade_symbols: list[str] | str,
        exclude_symbols: list[str],
        events_path: str,
        entry_before_ms: int,
        exit_after_ms: int,
        event_log_path: str,
    ):
        config = super().__new__(cls)
        config.instrument_ids = [
            InstrumentId.from_str(value) if isinstance(value, str) else value
            for value in instrument_ids
        ]
        config.bar_types = [
            BarType.from_str(value) if isinstance(value, str) else value
            for value in bar_types
        ]
        config.trade_notional = Decimal(str(trade_notional))
        config.min_rate_bps = Decimal(str(min_rate_bps))
        config.trade_symbols = trade_symbols
        config.exclude_symbols = exclude_symbols
        config.events_path = events_path
        config.entry_before_ms = entry_before_ms
        config.exit_after_ms = exit_after_ms
        config.event_log_path = event_log_path
        return config


class Backtestfunding(Strategy):
    def __init__(self, config: BacktestfundingConfig) -> None:
        super().__init__(config)
        self.config = config
        self.events: dict[str, dict] = {}
        self.open_events: dict[InstrumentId, str] = {}
        self.last_px: dict[InstrumentId, Decimal] = {}
        self.log_path = Path(config.event_log_path)
        self.recorder = DataRecorder(self.log_path.parent)
        self.min_rate_bps = Decimal(str(config.min_rate_bps))
        self.entry_before_ms = int(config.entry_before_ms)
        self.exit_after_ms = int(config.exit_after_ms)
        self.trade = None if config.trade_symbols == "all" else {
            self._base_symbol(symbol) for symbol in config.trade_symbols
        }
        self.exclude = {self._base_symbol(symbol) for symbol in config.exclude_symbols}

    # 启动时加载 funding 事件，并注册 t-1/t+1 定时器。
    def on_start(self) -> None:
        self._load_events()
        self.recorder.start()
        self._init_log()
        for instrument_id in self.config.instrument_ids:
            self.subscribe_trades(instrument_id)
        current_ns = self.clock.timestamp_ns()
        active_events = {
            event_id: row
            for event_id, row in self.events.items()
            if int(row["entry_time_ms"]) * 1_000_000 > current_ns
            and int(row["exit_time_ms"]) * 1_000_000 > current_ns
        }
        skipped = len(self.events) - len(active_events)
        self.events = active_events
        for event_id, row in self.events.items():
            self.clock.set_time_alert_ns(
                f"backtestfunding_entry:{event_id}",
                int(row["entry_time_ms"]) * 1_000_000,
                callback=self._on_time,
                allow_past=False,
            )
            self.clock.set_time_alert_ns(
                f"backtestfunding_exit:{event_id}",
                int(row["exit_time_ms"]) * 1_000_000,
                callback=self._on_time,
                allow_past=False,
            )
        self.log.info(
            f"backtestfunding启动，事件{len(self.events)}个，跳过过期事件{skipped}个，交易对{len(self.config.instrument_ids)}个",
            LogColor.NORMAL,
        )

    # trade tick 只用于估算市价单数量。
    def on_trade(self, tick: TradeTick) -> None:
        self.last_px[tick.instrument_id] = Decimal(str(tick.price))

    # 定时开仓和平仓。
    def _on_time(self, event: TimeEvent) -> None:
        action, event_id = event.name.split(":", 1)
        if action.endswith("entry"):
            self._entry(event_id)
        elif action.endswith("exit"):
            self._exit(event_id)

    def _entry(self, event_id: str) -> None:
        row = self.events[event_id]
        instrument_id = row["instrument_id"]
        if instrument_id in self.open_events:
            self._write(row, "skip_not_flat", None)
            return
        price = self.last_px.get(instrument_id)
        if price is None:
            self._write(row, "skip_no_tick", None)
            return
        instrument = self.cache.instrument(instrument_id)
        order = self._market_order(instrument_id, row["order_side"], self._qty(instrument, price))
        self.submit_order(order)
        self.open_events[instrument_id] = event_id
        self._write(row, "OPEN", price)

    def _exit(self, event_id: str) -> None:
        row = self.events[event_id]
        instrument_id = row["instrument_id"]
        if self.open_events.get(instrument_id) != event_id:
            self._write(row, "skip_no_position", self.last_px.get(instrument_id))
            return
        self.close_all_positions(instrument_id)
        self.open_events.pop(instrument_id, None)
        self._write(row, "CLOSE", self.last_px.get(instrument_id))

    def on_stop(self) -> None:
        for instrument_id in self.config.instrument_ids:
            self.cancel_all_orders(instrument_id)
            self.close_all_positions(instrument_id)
            self.unsubscribe_trades(instrument_id)

    def on_order_filled(self, event) -> None:
        self.recorder.on_order_filled(event)

    def on_order_canceled(self, event) -> None:
        self.recorder.on_order_canceled(event)

    def on_order_rejected(self, event) -> None:
        self.recorder.on_order_rejected(event)

    def on_position_opened(self, event) -> None:
        self.recorder.on_position_opened(event)

    def on_position_changed(self, event) -> None:
        self.recorder.on_position_changed(event)

    def on_position_adjusted(self, event) -> None:
        self.recorder.on_position_adjusted(event)

    def on_position_closed(self, event) -> None:
        self.recorder.on_position_closed(event)

    def on_reset(self) -> None:
        self.open_events.clear()
        self.last_px.clear()

    def _load_events(self) -> None:
        if not self.config.events_path:
            raise RuntimeError("backtestfunding 当前只支持回测，请在 backtest.events_path 配置历史 funding 事件文件。")
        path = Path(self.config.events_path)
        df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
        by_base = {
            self._base_symbol(str(instrument_id)): instrument_id
            for instrument_id in self.config.instrument_ids
        }
        if self.trade is None:
            allowed = set(self.config.instrument_ids)
        else:
            missing = sorted(self.trade - set(by_base))
            if missing:
                raise RuntimeError(f"trade_symbols not loaded: {','.join(missing)}")
            allowed = {by_base[symbol] for symbol in self.trade}
        missing_exclude = sorted(self.exclude - set(by_base))
        if missing_exclude:
            raise RuntimeError(f"exclude_symbols not loaded: {','.join(missing_exclude)}")
        allowed -= {by_base[symbol] for symbol in self.exclude}
        if not allowed:
            raise RuntimeError("trade_symbols is empty after exclude_symbols")

        by_node: dict[int, dict] = {}
        for row in df.to_dict("records"):
            base = self._base_symbol(row["symbol"])
            instrument_id = (
                InstrumentId.from_str(row["instrument_id"])
                if "instrument_id" in row
                else by_base.get(base)
            )
            if instrument_id is None or instrument_id not in allowed:
                continue
            if base in self.exclude:
                continue
            abs_rate_bps = Decimal(str(row["abs_rate_bps"]))
            if abs_rate_bps <= self.min_rate_bps:
                continue
            node_ms = int(row["funding_time"]) // 60_000 * 60_000
            current = by_node.get(node_ms)
            if current is None or abs_rate_bps > current["_abs_rate_bps"]:
                by_node[node_ms] = {**row, "_abs_rate_bps": abs_rate_bps}

        for row in by_node.values():
            base = self._base_symbol(row["symbol"])
            instrument_id = (
                InstrumentId.from_str(row["instrument_id"])
                if "instrument_id" in row
                else by_base[base]
            )
            rate = Decimal(str(row["rate"]))
            side = OrderSide.SELL if rate > 0 else OrderSide.BUY
            event_id = f"{row['symbol']}:{int(row['funding_time'])}"
            row.pop("_abs_rate_bps")
            fund_ms = int(row["funding_time"])
            self.events[event_id] = {
                **row,
                "event_id": event_id,
                "instrument_id": instrument_id,
                "rate": rate,
                "side": "SELL" if side == OrderSide.SELL else "BUY",
                "order_side": side,
                "entry_time_ms": row.get("entry_time_ms", fund_ms - self.entry_before_ms),
                "exit_time_ms": row.get("exit_time_ms", fund_ms + self.exit_after_ms),
                "funding_gain": self.config.trade_notional * abs(rate),
            }

    def _base_symbol(self, symbol: str) -> str:
        return symbol.upper().replace("-PERP.BINANCE", "").replace("USDT", "").replace("/", "")

    def _market_order(self, instrument_id: InstrumentId, side: OrderSide, quantity):
        ts_init = self.clock.timestamp_ns()
        return MarketOrder(
            trader_id=self.trader_id,
            strategy_id=self.strategy_id,
            instrument_id=instrument_id,
            client_order_id=ClientOrderId(f"O-{ts_init}"),
            order_side=side,
            quantity=quantity,
            init_id=UUID4(),
            ts_init=ts_init,
            time_in_force=TimeInForce.GTC,
            reduce_only=False,
            quote_quantity=False,
        )

    def _qty(self, instrument, price: Decimal):
        raw = self.config.trade_notional / price
        step = Decimal(str(instrument.size_increment))
        if step > 0:
            raw = (raw / step).to_integral_value(rounding=ROUND_CEILING) * step
        return instrument.make_qty(raw)

    def _init_log(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "event_id",
                    "symbol",
                    "instrument_id",
                    "funding_time",
                    "action",
                    "bar_time",
                    "side",
                    "funding_rate",
                    "rate_bps",
                    "notional",
                    "estimated_funding_income",
                    "local_time",
                    "delta_to_funding_ms",
                    "adverse_entry_move",
                    "reason",
                    "ref_price",
                ],
            )

    def _write(self, row: dict, action: str, price: Decimal | None) -> None:
        bar_ms = int(row["entry_time_ms"] if action == "OPEN" else row["exit_time_ms"])
        fund_ms = int(row["funding_time"])
        funding_time = pd.to_datetime(fund_ms, unit="ms", utc=True).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        bar_time = pd.to_datetime(bar_ms, unit="ms", utc=True).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        local_time = pd.to_datetime(bar_ms, unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime(
            "%Y-%m-%dT%H:%M:%S.%f%z",
        )
        with self.log_path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    row["event_id"],
                    row["symbol"],
                    row["instrument_id"],
                    funding_time,
                    action,
                    bar_time,
                    row["side"],
                    row["rate"],
                    f"{Decimal(str(row['rate_bps'])):.4f}",
                    self.config.trade_notional,
                    row["funding_gain"],
                    local_time,
                    bar_ms - fund_ms,
                    "",
                    "",
                    price or "",
                ],
            )
