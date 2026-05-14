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


class MaxfundingConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    trade_notional: Decimal
    min_rate_bps: Decimal
    whitelist_symbols: list[str]
    events_path: str = ""
    exclude_symbols: list[str] = []
    event_log_path: str = "auto"


class Maxfunding(Strategy):
    def __init__(self, config: MaxfundingConfig) -> None:
        super().__init__(config)
        self.events: dict[str, dict] = {}
        self.open_events: dict[InstrumentId, str] = {}
        self.last_px: dict[InstrumentId, Decimal] = {}
        self.log_path = Path(config.event_log_path)
        self.min_rate_bps = Decimal(str(config.min_rate_bps))
        self.whitelist = {self._base_symbol(symbol) for symbol in config.whitelist_symbols}
        self.exclude = {self._base_symbol(symbol) for symbol in config.exclude_symbols}

    # 启动时加载 funding 事件，并注册 t-1/t+1 定时器。
    def on_start(self) -> None:
        self._load_events()
        self._init_log()
        for instrument_id in self.config.instrument_ids:
            self.subscribe_trade_ticks(instrument_id)
        for event_id, row in self.events.items():
            self.clock.set_time_alert_ns(
                f"maxfunding_entry:{event_id}",
                int(row["entry_time_ms"]) * 1_000_000,
                callback=self._on_time,
                allow_past=False,
            )
            self.clock.set_time_alert_ns(
                f"maxfunding_exit:{event_id}",
                int(row["exit_time_ms"]) * 1_000_000,
                callback=self._on_time,
                allow_past=False,
            )
        self.log.info(f"maxfunding启动，事件{len(self.events)}个，交易对{len(self.config.instrument_ids)}个")

    # trade tick 只用于估算市价单数量。
    def on_trade_tick(self, tick: TradeTick) -> None:
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
        if not self.portfolio.is_flat(instrument_id):
            self._write(row, "skip_not_flat", None)
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
            self.unsubscribe_trade_ticks(instrument_id)

    def on_reset(self) -> None:
        self.open_events.clear()
        self.last_px.clear()

    def _load_events(self) -> None:
        if not self.config.events_path:
            raise RuntimeError("maxfunding 当前只支持回测，请在 backtest.events_path 配置历史 funding 事件文件。")
        path = Path(self.config.events_path)
        df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
        allowed = set(self.config.instrument_ids)
        by_node: dict[int, dict] = {}
        for row in df.to_dict("records"):
            instrument_id = InstrumentId.from_str(row["instrument_id"])
            if instrument_id not in allowed:
                continue
            base = self._base_symbol(row["symbol"])
            if base in self.exclude or base not in self.whitelist:
                continue
            abs_rate_bps = Decimal(str(row["abs_rate_bps"]))
            if abs_rate_bps <= self.min_rate_bps:
                continue
            node_ms = int(row["funding_time"]) // 14_400_000 * 14_400_000
            current = by_node.get(node_ms)
            if current is None or abs_rate_bps > current["_abs_rate_bps"]:
                by_node[node_ms] = {**row, "_abs_rate_bps": abs_rate_bps}

        for row in by_node.values():
            instrument_id = InstrumentId.from_str(row["instrument_id"])
            rate = Decimal(str(row["rate"]))
            side = OrderSide.SELL if row["side"] == "SELL" else OrderSide.BUY
            event_id = f"{row['symbol']}:{int(row['funding_time'])}"
            row.pop("_abs_rate_bps")
            self.events[event_id] = {
                **row,
                "event_id": event_id,
                "instrument_id": instrument_id,
                "rate": rate,
                "order_side": side,
                "funding_gain": self.config.trade_notional * abs(rate),
            }

    def _base_symbol(self, symbol: str) -> str:
        return symbol.upper().replace("-PERP.BINANCE", "").replace("USDT", "")

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
