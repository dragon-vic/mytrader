from __future__ import annotations

import csv
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from nautilus_trader.model.data import BarType
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


class FundingConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    trade_size: Decimal
    bar_type: BarType
    entry_side: str = "BUY"           # "BUY" 或 "SELL"，开仓方向
    open_delay_seconds: int = 5         # 启动后先平仓，几秒后再开仓
    funding_time_ms: int | None = None  # 下一次 funding 准点，毫秒时间戳
    event_log_path: str = "reports/live/funding_event_probe_events.csv"


class Funding(Strategy):
    def __init__(self, config: FundingConfig) -> None:
        super().__init__(config)
        self.instrument = None
        self.open_submitted = False
        self.event_log_path = Path(config.event_log_path)

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)

        if self.instrument is None:
            self.log.error(f"Instrument not found: {self.config.instrument_id}")
            self.stop()
            return

        self.event_log_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.event_log_path.exists():
            with self.event_log_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "local_ts_ns",
                    "event_type",
                    "is_possible_funding",
                    "funding_diff_sec",
                    "event_text",
                ])

        self.log.info("FundingEventProbe started")
        self.log.info(f"instrument_id={self.config.instrument_id}")
        self.log.info(f"quantity={self.config.trade_size}")
        self.log.info(f"entry_side={self.config.entry_side}")
        self.log.info(f"funding_time_ms={self.config.funding_time_ms}")

        # 先撤挂单，再平已有仓位。
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)

        # 给撤单/平仓一点时间，然后再开测试仓。
        self.clock.set_time_alert(
            name="open_probe_position",
            alert_time=self.clock.utc_now() + timedelta(seconds=self.config.open_delay_seconds),
            callback=self._open_probe_position,
        )

    def _open_probe_position(self, event) -> None:
        if self.open_submitted:
            return

        side_text = self.config.entry_side.upper()

        if side_text == "BUY":
            side = OrderSide.BUY
        elif side_text == "SELL":
            side = OrderSide.SELL
        else:
            self.log.error(f"Invalid entry_side: {self.config.entry_side}")
            return

        qty = self.instrument.make_qty(self.config.trade_size)

        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=qty,
        )

        self.submit_order(order)
        self.open_submitted = True

        self.log.info(f"Submitted probe market order: side={side}, qty={qty}")
        self._record_event("PROBE_OPEN_SUBMITTED", order)

    def _record_event(self, event_type: str, event: Any) -> None:
        text = str(event)
        lower = text.lower()

        is_possible_funding = (
            "funding" in lower
            or "funding_fee" in lower
            or "accountstate" in lower
            or "account_state" in lower
            or "account update" in lower
        )

        local_ts_ns = self.clock.timestamp_ns()

        funding_diff_sec = ""
        if self.config.funding_time_ms is not None:
            funding_diff_sec = (self.clock.timestamp_ms() - self.config.funding_time_ms) / 1000

        with self.event_log_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                local_ts_ns,
                event_type,
                is_possible_funding,
                funding_diff_sec,
                text,
            ])

        if is_possible_funding:
            self.log.warning(
                f"POSSIBLE_FUNDING_EVENT type={event_type}, "
                f"funding_diff_sec={funding_diff_sec}, event={text}"
            )
        else:
            self.log.info(f"EVENT type={event_type}, event={text}")

    def on_order_event(self, event) -> None:
        self._record_event(type(event).__name__, event)

    def on_position_event(self, event) -> None:
        self._record_event(type(event).__name__, event)

    def on_event(self, event) -> None:
        self._record_event(type(event).__name__, event)

    # 如果你的 NT 版本会调用这个 handler，就能单独抓 AccountState。
    # 如果不会调用，也不影响 on_event 的通用记录。
    def on_account_state(self, event) -> None:
        self._record_event(type(event).__name__, event)

