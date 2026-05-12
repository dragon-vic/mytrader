from __future__ import annotations

from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_CEILING

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import FundingRateUpdate
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import EVENT_ACCOUNT_TOPIC


class WatchConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_notional: Decimal = Decimal("5.5")
    trade_mode: str = "print"
    min_abs_funding_rate: Decimal = Decimal("0.0050")
    entry_window_seconds: float = 0.5
    close_positions_on_stop: bool = True


class Watch(Strategy):
    def __init__(self, config: WatchConfig) -> None:
        super().__init__(config)

        self.funding_time_ns = None
        self.already_submit = False
        self.already_print = False
        self.client_id = ClientId("BINANCE")

        self.trade_mode = config.trade_mode.lower()
        self.trade_notional = Decimal(str(config.trade_notional))
        self.min_abs_funding_rate = Decimal(str(config.min_abs_funding_rate))

        self.instruments: dict[InstrumentId, Instrument] = {}
        self.mark_prices: dict[InstrumentId, Decimal] = {}

        # funding 前最后 1 秒内，abs(rate) > threshold 的 instruments。
        # value 保留原始 funding rate，正负号用于决定下单方向。
        self.highrate: dict[InstrumentId, Decimal] = {}

        # 本轮已经开仓的 instruments，避免同一轮重复开仓。
        self.opened_instrument_ids: set[InstrumentId] = set()

        self.seen_account_event_ids: set[str] = set()

    def on_start(self) -> None:
        if self.trade_mode not in {"print", "trade"}:
            raise RuntimeError(f"Invalid trade_mode: {self.trade_mode}")

        self._load_all_perp_instruments()

        for instrument_id in self.instruments:
            self.subscribe_funding_rates(instrument_id, client_id=self.client_id)

            # 只有真实下单时才需要价格。
            # Binance U 本位 futures 下单接口需要 base quantity，不支持直接传 5.5 USDT 名义金额。
            if self.trade_mode == "trade":
                self.subscribe_mark_prices(instrument_id, client_id=self.client_id)

        # 只用 config 里的 TRUMP tick 作为最后一秒触发器。
        self.subscribe_trade_ticks(self.config.instrument_id)

        # 只关注精确 account topic。
        self.msgbus.subscribe(EVENT_ACCOUNT_TOPIC, self.handle_account_event)
        self.update_funding_time()

        self.log.info(
            "Watch started: "
            f"mode={self.trade_mode}, "
            f"heartbeat={self.config.instrument_id}, "
            f"instruments={len(self.instruments)}, "
            f"trade_notional={self.trade_notional}, "
            f"min_abs_funding_rate={self.min_abs_funding_rate}, "
            f"entry_window={self.config.entry_window_seconds}s"
        )

    def on_funding_rate(self, funding_rate: FundingRateUpdate) -> None:
        if self.already_submit and self.trade_mode == "print" and not self.already_print:
            self._print_highrate()
            self.already_print = True
            self.update_all()
            return
        funding_time_ns = funding_rate.next_funding_ns
        if funding_time_ns != self.funding_time_ns:
            return

        if not self._is_entry_window(self.config.entry_window_seconds+2):
            return

        instrument_id = funding_rate.instrument_id
        rate = Decimal(str(funding_rate.rate))

        # 绝对值大于万 50 才记录。
        # rate > 0 后面开 SELL；rate < 0 后面开 BUY。
        if abs(rate) <= self.min_abs_funding_rate:
            self.highrate.pop(instrument_id, None)
            return

        self.highrate[instrument_id] = rate

        self.log.info(
            "Watch highrate detected: "
            f"instrument={instrument_id}, "
            f"rate={rate}, "
            f"abs_rate={abs(rate)}, "
            f"threshold={self.min_abs_funding_rate}, "
            f"funding_time={self._iso(funding_time_ns)}"
        )

    def on_mark_price(self, mark_price) -> None:
        if self.already_submit:
            return
        if not self._is_entry_window(self.config.entry_window_seconds+2):
            return
        instrument_id = getattr(mark_price, "instrument_id", None)

        price = (
            getattr(mark_price, "value", None)
            or getattr(mark_price, "price", None)
            or getattr(mark_price, "mark", None)
            or getattr(mark_price, "mark_price", None)
        )

        if instrument_id is None or price is None:
            self.log.warning(f"Watch mark price ignored: {mark_price}")
            return

        self.mark_prices[instrument_id] = Decimal(str(price))

    def on_trade_tick(self, tick: TradeTick) -> None:
        if self.already_submit :
            return

        if not self._is_entry_window(self.config.entry_window_seconds) or not self.highrate:
            return
        if self.trade_mode == "trade":
            for instrument_id, rate in list(self.highrate.items()):
                if instrument_id in self.opened_instrument_ids:
                    continue

                instrument = self.instruments.get(instrument_id)
                if instrument is None:
                    self.log.warning(f"Watch skip entry: no instrument for {instrument_id}")
                    continue

                mark_price = self.mark_prices.get(instrument_id)
                if mark_price is None:
                    self.log.warning(f"Watch skip entry: no mark price for {instrument_id}")
                    continue

                side = OrderSide.SELL if rate > 0 else OrderSide.BUY
                self.submit_entry(
                    instrument=instrument,
                    mark_price=mark_price,
                    funding_rate=rate,
                    side=side,
                    funding_time_ns=self.funding_time_ns,
                )
        self.already_submit = True

    def handle_account_event(self, event: AccountState) -> None:
        now_ns = self.clock.timestamp_ns()
        if now_ns < self.funding_time_ns:
            return

        event_id = self._account_event_id(event)
        if event_id is not None:
            if event_id in self.seen_account_event_ids:
                return
            self.seen_account_event_ids.add(event_id)

        close_ids = list(self.opened_instrument_ids)

        for instrument_id in close_ids:
            if self.trade_mode == "trade":
                self.close_all_positions(instrument_id)
                self.already_submit = False

        self.log.info(
            "Watch close submitted: "
            f"funding_time={self._iso(self.funding_time_ns)}, "
            f"now={self._iso(now_ns)}, "
            f"instruments={[str(x) for x in close_ids]}"
        )
        self.update_all()

    def update_all(self):
        self.highrate.clear()
        self.mark_prices.clear()
        self.opened_instrument_ids.clear()
        self.funding_time_ns+=4 * 60 * 60 * 1_000_000_000
        self.already_submit = False
        self.already_print = False

    def _print_highrate(self) -> None:
        rows = sorted(
            self.highrate.items(),
            key=lambda item: abs(item[1]),
            reverse=True,
        )

        self.log.info(
            "Watch highrate snapshot: "
            f"count={len(rows)}, "
            f"items={[(str(instrument_id), str(rate)) for instrument_id, rate in rows]}"
        )

    def _quantity_from_notional(self, instrument: Instrument, price: Decimal):
        raw_quantity = self.trade_notional / price

        increment = Decimal(str(instrument.size_increment))
        if increment > 0:
            steps = (raw_quantity / increment).to_integral_value(rounding=ROUND_CEILING)
            raw_quantity = steps * increment

        return instrument.make_qty(raw_quantity)

    def _is_entry_window(self,entry_window_seconds) -> bool:
        now_ns = self.clock.timestamp_ns()
        window_ns =entry_window_seconds * 1_000_000_000
        return self.funding_time_ns - window_ns <= now_ns < self.funding_time_ns

    def _account_event_id(self, event: AccountState) -> str | None:
        if isinstance(event, AccountState):
            event_id = AccountState.to_dict(event).get("event_id")
            return None if event_id is None else str(event_id)
        event_id = getattr(event, "event_id", None)
        return None if event_id is None else str(event_id)

    def _iso(self, ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()

    def update_funding_time(self) -> None:
        now_ns = self.clock.timestamp_ns()
        four_hours_ns = 4 * 60 * 60 * 1_000_000_000

        # 最近的下一个 4h UTC 准点：00:00 / 04:00 / 08:00 / 12:00 / 16:00 / 20:00
        self.funding_time_ns = ((now_ns // four_hours_ns) + 1) * four_hours_ns

        self.log.info(
            "Watch next funding time updated: "
            f"now={self._iso(now_ns)}, "
            f"next_funding_time={self._iso(self.funding_time_ns)}"
        )

    def _load_all_perp_instruments(self) -> None:
        instruments = [
            instrument
            for instrument in self.cache.instruments()
            if instrument is not None and "-PERP." in str(instrument.id)
        ]

        self.instruments = {
            instrument.id: instrument
            for instrument in instruments
        }

        if not self.instruments:
            raise RuntimeError("No perpetual instruments found in cache")

    # 保留下单函数。trade 模式才会调用。
    def submit_entry(self,
        instrument: Instrument,
        mark_price: Decimal,
        funding_rate: Decimal,
        side: OrderSide,
        funding_time_ns: int,) -> None:
        quantity = self._quantity_from_notional(instrument, mark_price)

        order = self.order_factory.market(
            instrument_id=instrument.id,
            order_side=side,
            quantity=quantity,
            time_in_force=TimeInForce.GTC,
        )

        self.submit_order(order)
        self.opened_instrument_ids.add(instrument.id)

        self.log.info(
            "Watch entry submitted: "
            f"instrument={instrument.id}, "
            f"funding_time={self._iso(funding_time_ns)}, "
            f"rate={funding_rate}, "
            f"side={'BUY' if side == OrderSide.BUY else 'SELL'}, "
            f"mark_price={mark_price}, "
            f"notional={self.trade_notional}, "
            f"quantity={quantity}"
        )

    def on_stop(self) -> None:
        for instrument_id in self.instruments:
            self.cancel_all_orders(instrument_id)

        if self.config.close_positions_on_stop and self.trade_mode == "trade":
            for instrument_id in self.opened_instrument_ids:
                self.close_all_positions(instrument_id)

        self.unsubscribe_trade_ticks(self.config.instrument_id)

        for instrument_id in self.instruments:
            self.unsubscribe_funding_rates(instrument_id, client_id=self.client_id)

            if self.trade_mode == "trade":
                self.unsubscribe_mark_prices(instrument_id, client_id=self.client_id)

        self.msgbus.unsubscribe(EVENT_ACCOUNT_TOPIC, self.handle_account_event)

        self.log.info("Watch stopped.")

    def on_reset(self) -> None:
        self.mark_prices.clear()
        self.highrate.clear()
        self.opened_instrument_ids.clear()
        self.seen_account_event_ids.clear()
        self.log.info("Watch reset.")