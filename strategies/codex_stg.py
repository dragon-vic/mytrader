from __future__ import annotations

import csv
from decimal import Decimal
from decimal import ROUND_CEILING
from pathlib import Path

import pandas as pd
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class CodexStgConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    signal_path: str
    trade_notional: Decimal
    min_score_bps: Decimal
    close_positions_on_stop: bool
    event_log_path: str


class CodexStg(Strategy):
    def __init__(self, config: CodexStgConfig) -> None:
        super().__init__(config)
        self.signals: dict[str, dict] = {}
        self.open_signals: dict[InstrumentId, str] = {}
        self.last_px: dict[InstrumentId, Decimal] = {}
        self.log_path = Path(config.event_log_path)
        self.min_score_bps = Decimal(str(config.min_score_bps))

    # 加载外部研究信号，注册对应 tick 和开平仓定时器。
    def on_start(self) -> None:
        self._load_signals()
        self._init_log()
        for instrument_id in self.config.instrument_ids:
            self.subscribe_trade_ticks(instrument_id)
        for event_id, row in self.signals.items():
            self.clock.set_time_alert_ns(
                f"codex_entry:{event_id}",
                int(row["entry_time_ms"]) * 1_000_000,
                callback=self._on_time,
                allow_past=False,
            )
            self.clock.set_time_alert_ns(
                f"codex_exit:{event_id}",
                int(row["exit_time_ms"]) * 1_000_000,
                callback=self._on_time,
                allow_past=False,
            )
        self.log.info(f"codex_stg启动，信号{len(self.signals)}个，交易对{len(self.config.instrument_ids)}个")

    # tick 用于记录最近价格并估算市价单数量。
    def on_trade_tick(self, tick: TradeTick) -> None:
        self.last_px[tick.instrument_id] = Decimal(str(tick.price))

    # 定时器分发开仓和平仓动作。
    def _on_time(self, event: TimeEvent) -> None:
        action, event_id = event.name.split(":", 1)
        if action.endswith("entry"):
            self._entry(event_id)
        elif action.endswith("exit"):
            self._exit(event_id)

    def _entry(self, event_id: str) -> None:
        row = self.signals[event_id]
        instrument_id = row["instrument_id"]
        if not self.portfolio.is_flat(instrument_id):
            self._write(row, "skip_not_flat", self.last_px.get(instrument_id))
            return
        price = self.last_px.get(instrument_id)
        if price is None:
            self._write(row, "skip_no_tick", None)
            return
        instrument = self.cache.instrument(instrument_id)
        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=row["order_side"],
            quantity=self._qty(instrument, price),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
        self.open_signals[instrument_id] = event_id
        self._write(row, "OPEN", price)

    def _exit(self, event_id: str) -> None:
        row = self.signals[event_id]
        instrument_id = row["instrument_id"]
        if self.open_signals.get(instrument_id) != event_id:
            self._write(row, "skip_no_position", self.last_px.get(instrument_id))
            return
        self.close_all_positions(instrument_id)
        self.open_signals.pop(instrument_id, None)
        self._write(row, "CLOSE", self.last_px.get(instrument_id))

    def on_stop(self) -> None:
        for instrument_id in self.config.instrument_ids:
            self.cancel_all_orders(instrument_id)
            if self.config.close_positions_on_stop:
                self.close_all_positions(instrument_id)
            self.unsubscribe_trade_ticks(instrument_id)

    def on_reset(self) -> None:
        self.signals.clear()
        self.open_signals.clear()
        self.last_px.clear()

    def _load_signals(self) -> None:
        path = Path(self.config.signal_path)
        df = pd.read_parquet(path)
        by_id = {instrument_id: instrument_id for instrument_id in self.config.instrument_ids}
        for row in df.to_dict("records"):
            score = Decimal(str(row["score_bps"]))
            if score < self.min_score_bps:
                continue
            instrument_id = InstrumentId.from_str(str(row["instrument_id"]))
            if instrument_id not in by_id:
                raise RuntimeError(f"signal instrument not loaded: {instrument_id}")
            side = OrderSide.BUY if str(row["side"]).upper() == "BUY" else OrderSide.SELL
            event_id = str(row["event_id"])
            self.signals[event_id] = {
                **row,
                "event_id": event_id,
                "instrument_id": instrument_id,
                "order_side": side,
                "score_bps": score,
                "funding_gain": self.config.trade_notional * Decimal(str(row["abs_rate_bps"])) / Decimal("10000"),
            }
        if not self.signals:
            raise RuntimeError("codex_stg loaded no signals")

    def _qty(self, instrument: Instrument, price: Decimal):
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
                    "local_time",
                    "side",
                    "rate_bps",
                    "abs_rate_bps",
                    "score_bps",
                    "predicted_cost_bps",
                    "notional",
                    "estimated_funding_income",
                    "ref_price",
                ],
            )

    def _write(self, row: dict, action: str, price: Decimal | None) -> None:
        event_ms = int(row["entry_time_ms"] if action == "OPEN" else row["exit_time_ms"])
        local_time = pd.to_datetime(event_ms, unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime(
            "%Y-%m-%d %H:%M:%S.%f",
        )
        funding_time = pd.to_datetime(int(row["funding_time"]), unit="ms", utc=True).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        with self.log_path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    row["event_id"],
                    row["symbol"],
                    row["instrument_id"],
                    funding_time,
                    action,
                    local_time,
                    row["side"],
                    f"{Decimal(str(row['rate_bps'])):.4f}",
                    f"{Decimal(str(row['abs_rate_bps'])):.4f}",
                    f"{Decimal(str(row['score_bps'])):.4f}",
                    f"{Decimal(str(row['predicted_cost_bps'])):.4f}",
                    self.config.trade_notional,
                    row["funding_gain"],
                    price or "",
                ],
            )
