from __future__ import annotations

import csv
import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode

import requests
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


@dataclass(frozen=True)
class FundingEvent:
    ts_ns: int
    rate: Decimal
    interval_hours: int


class FundingConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal
    funding_csv_path: str = ""
    min_abs_funding_rate: Decimal = Decimal("0.0015")
    max_adverse_entry_move: Decimal = Decimal("0.005")
    funding_interval_hours: int = 0
    entry_minutes_before: int = 1
    entry_milliseconds_before: int = 0
    exit_minutes_after: int = 1
    exit_milliseconds_after: int = 0
    use_trade_ticks: bool = False
    request_bars: bool = True
    warmup_days: int = 1
    allow_long: bool = True
    allow_short: bool = True
    close_positions_on_stop: bool = True
    event_log_path: str = "auto"
    use_live_funding: bool = False
    funding_api_base_url: str = "https://fapi.binance.com"
    funding_refresh_seconds: int = 60
    funding_income_poll_seconds: int = 3
    funding_income_lookback_minutes: int = 5
    api_key: str = ""
    api_secret: str = ""
    proxy_url: str = ""


class Funding(Strategy):
    def __init__(self, config: FundingConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument | None = None
        self.csv_events: list[FundingEvent] = []
        self.csv_event_index = 0
        self.live_event: FundingEvent | None = None
        self.last_live_refresh_ns = 0
        self.active_event: FundingEvent | None = None
        self.active_side: OrderSide | None = None
        self.pending_funding_event: FundingEvent | None = None
        self.last_funding_income_poll_ns = 0
        self.recorded_funding_income_ids: set[str] = set()
        self.seen_market_data = False
        self.event_log_path = Path(config.event_log_path)
        self.funding_fees_path = self.event_log_path.with_name("funding_fees.csv")
        self.min_abs_funding_rate = Decimal(str(config.min_abs_funding_rate))
        self.max_adverse_entry_move = Decimal(str(config.max_adverse_entry_move))
        self.proxies = {"http": config.proxy_url, "https": config.proxy_url} if config.proxy_url else None

    # 加载资金费来源，订阅 bar，后续只围绕 funding 时间点交易。
    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            raise RuntimeError(f"Instrument not found: {self.config.instrument_id}")

        self._init_event_log()
        self._init_funding_fee_log()
        if self.config.use_live_funding:
            self._refresh_live_funding(force=True)
        else:
            self.csv_events = self._load_funding_events(Path(self.config.funding_csv_path))

        if self.config.request_bars and not self.config.use_trade_ticks:
            self.request_bars(
                self.config.bar_type,
                start=self._clock.utc_now() - timedelta(days=self.config.warmup_days),
            )
        if self.config.use_trade_ticks:
            self.subscribe_trade_ticks(self.config.instrument_id)
        else:
            self.subscribe_bars(self.config.bar_type)
        self.log.info(
            f"Funding started: live={self.config.use_live_funding}, "
            f"min_abs_rate={self.min_abs_funding_rate}, "
            f"entry_before={self._entry_offset_ns() / 1_000_000}ms, "
            f"exit_after={self._exit_offset_ns() / 1_000_000}ms, "
            f"use_trade_ticks={self.config.use_trade_ticks}"
        )

    # 每根 bar 检查是否接近下一次 funding。
    def on_bar(self, bar: Bar) -> None:
        self._log_first_market_data("bar", bar.ts_event)
        now_ns = bar.ts_event
        self._poll_pending_funding_income(now_ns)

        if self.active_event is not None:
            if now_ns >= self._exit_time_ns(self.active_event):
                self._exit_after_funding(bar)
            return

        if self.config.use_live_funding:
            self._refresh_live_funding_if_due(now_ns)

        self._skip_expired_events(now_ns)
        event = self._next_event()
        if event is None or now_ns < self._entry_time_ns(event):
            return
        if now_ns >= event.ts_ns:
            self._record_decision(bar, event, "SKIP_LATE", "", "missed entry window")
            self._consume_event()
            return

        side = self._entry_side(event)
        if abs(event.rate) < self.min_abs_funding_rate:
            self._record_decision(bar, event, "SKIP_SMALL_RATE", "", "funding rate too small")
            self._consume_event()
            return
        if side == OrderSide.BUY and not self.config.allow_long:
            self._record_decision(bar, event, "SKIP_LONG_DISABLED", "BUY", "long disabled")
            self._consume_event()
            return
        if side == OrderSide.SELL and not self.config.allow_short:
            self._record_decision(bar, event, "SKIP_SHORT_DISABLED", "SELL", "short disabled")
            self._consume_event()
            return
        if self._adverse_entry_move(bar, side) > self.max_adverse_entry_move:
            self._record_decision(
                bar,
                event,
                "SKIP_ADVERSE_MOVE",
                self._side_name(side),
                "entry bar moved against side",
            )
            self._consume_event()
            return

        self._open_for_funding(bar, event, side)

    # 每个成交 tick 检查是否到达 funding 前后的毫秒级开平仓时间。
    def on_trade_tick(self, tick: TradeTick) -> None:
        self._log_first_market_data("trade_tick", tick.ts_event)
        now_ns = self.clock.timestamp_ns()
        self._poll_pending_funding_income(now_ns)

        if self.active_event is not None:
            if now_ns >= self._exit_time_ns(self.active_event):
                self._exit_after_funding(tick)
            return

        if self.config.use_live_funding:
            self._refresh_live_funding_if_due(now_ns)

        self._skip_expired_events(now_ns)
        event = self._next_event()
        if event is None or now_ns < self._entry_time_ns(event):
            return
        if now_ns >= event.ts_ns:
            self._record_decision(tick, event, "SKIP_LATE", "", "missed tick entry window")
            self._consume_event()
            return

        side = self._entry_side(event)
        if abs(event.rate) < self.min_abs_funding_rate:
            self._record_decision(tick, event, "SKIP_SMALL_RATE", "", "funding rate too small")
            self._consume_event()
            return
        if side == OrderSide.BUY and not self.config.allow_long:
            self._record_decision(tick, event, "SKIP_LONG_DISABLED", "BUY", "long disabled")
            self._consume_event()
            return
        if side == OrderSide.SELL and not self.config.allow_short:
            self._record_decision(tick, event, "SKIP_SHORT_DISABLED", "SELL", "short disabled")
            self._consume_event()
            return

        self._open_for_funding(tick, event, side)

    # 正资金费做空收钱，负资金费做多收钱。
    def _entry_side(self, event: FundingEvent) -> OrderSide:
        return OrderSide.SELL if event.rate > 0 else OrderSide.BUY

    # 在 funding 前提交市价单，优先保证拿到资金费。
    def _open_for_funding(self, market_data: Bar | TradeTick, event: FundingEvent, side: OrderSide) -> None:
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
        self.active_event = event
        self.active_side = side
        self._consume_event()
        self._record_decision(market_data, event, "OPEN", self._side_name(side), "rate passed threshold")

    # funding 后尽快平仓，减少价格方向暴露。
    def _exit_after_funding(self, market_data: Bar | TradeTick) -> None:
        self.close_all_positions(self.config.instrument_id)
        self._record_decision(market_data, self.active_event, "CLOSE", "", "after funding")
        self.pending_funding_event = self.active_event
        self.active_event = None
        self.active_side = None

    # 获取下一次待处理 funding 事件。
    def _next_event(self) -> FundingEvent | None:
        if self.config.use_live_funding:
            return self.live_event
        if self.csv_event_index >= len(self.csv_events):
            return None
        return self.csv_events[self.csv_event_index]

    # 消费当前 funding 事件，避免重复交易。
    def _consume_event(self) -> None:
        if self.config.use_live_funding:
            self.live_event = None
        else:
            self.csv_event_index += 1

    # 跳过已经错过平仓窗口的旧 funding 事件。
    def _skip_expired_events(self, now_ns: int) -> None:
        if self.config.use_live_funding:
            if self.live_event is not None and now_ns > self._exit_time_ns(self.live_event):
                self.live_event = None
            return
        while (
            self.csv_event_index < len(self.csv_events)
            and now_ns > self._exit_time_ns(self.csv_events[self.csv_event_index])
        ):
            self.csv_event_index += 1

    # 到刷新间隔后更新实盘下一次 funding 信息。
    def _refresh_live_funding_if_due(self, now_ns: int) -> None:
        elapsed_ns = now_ns - self.last_live_refresh_ns
        if self.live_event is None or elapsed_ns >= self.config.funding_refresh_seconds * 1_000_000_000:
            self._refresh_live_funding(force=False)

    # 从 Binance REST 刷新下一次 funding 时间和当前预测 rate。
    def _refresh_live_funding(self, force: bool) -> None:
        symbol = self._binance_symbol()
        response = requests.get(
            f"{self.config.funding_api_base_url}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            proxies=self.proxies,
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        interval_hours = self._live_interval_hours(symbol)
        event = FundingEvent(
            ts_ns=round(int(payload["nextFundingTime"]) / 60_000) * 60_000 * 1_000_000,
            rate=Decimal(str(payload["lastFundingRate"])),
            interval_hours=interval_hours,
        )
        self.last_live_refresh_ns = self.clock.timestamp_ns()
        if force or self.live_event is None or self.live_event != event:
            self.live_event = event
            self.log.info(
                f"Live funding refreshed: symbol={symbol}, "
                f"time={self._iso(event.ts_ns)}, rate={event.rate}, interval={interval_hours}h"
            )

    # 查询 Binance 资金费到账记录，发现真实到账后写日志和 CSV。
    def _poll_pending_funding_income(self, now_ns: int) -> None:
        event = self.pending_funding_event
        if not self.config.use_live_funding or event is None or now_ns < event.ts_ns:
            return
        elapsed_ns = now_ns - self.last_funding_income_poll_ns
        if elapsed_ns < self.config.funding_income_poll_seconds * 1_000_000_000:
            return

        self.last_funding_income_poll_ns = now_ns
        records = self._query_funding_income(event, now_ns)
        for record in records:
            income_id = str(record["tranId"])
            if income_id in self.recorded_funding_income_ids:
                continue
            self.recorded_funding_income_ids.add(income_id)
            self._record_funding_income(event, record)
            self.pending_funding_event = None

    # 调 Binance income 接口读取 funding fee 入账。
    def _query_funding_income(self, event: FundingEvent, now_ns: int) -> list[dict]:
        start_ms = event.ts_ns // 1_000_000 - self.config.funding_income_lookback_minutes * 60_000
        end_ms = now_ns // 1_000_000 + 60_000
        payload = {
            "symbol": self._binance_symbol(),
            "incomeType": "FUNDING_FEE",
            "startTime": start_ms,
            "endTime": end_ms,
            "timestamp": self.clock.timestamp_ms(),
        }
        query = urlencode(payload)
        signature = hmac.new(
            self.config.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        response = requests.get(
            f"{self.config.funding_api_base_url}/fapi/v1/income?{query}&signature={signature}",
            headers={"X-MBX-APIKEY": self.config.api_key},
            proxies=self.proxies,
            timeout=10,
        )
        response.raise_for_status()
        return [
            item
            for item in response.json()
            if item["symbol"] == self._binance_symbol() and Decimal(str(item["income"])) != 0
        ]

    # 记录真实到账资金费，INFO 日志会进 live.log。
    def _record_funding_income(self, event: FundingEvent, record: dict) -> None:
        income_time_ns = int(record["time"]) * 1_000_000
        delta_ms = (income_time_ns - event.ts_ns) / 1_000_000
        self.log.info(
            "FUNDING_RECEIVED "
            f"symbol={record['symbol']} income={record['income']} asset={record['asset']} "
            f"funding_time={self._iso(event.ts_ns)} income_time={self._iso(income_time_ns)} "
            f"delta_ms={delta_ms} tran_id={record['tranId']}"
        )
        with self.funding_fees_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    self._iso(event.ts_ns),
                    self._iso(income_time_ns),
                    delta_ms,
                    record["symbol"],
                    record["income"],
                    record["asset"],
                    record["incomeType"],
                    record.get("info", ""),
                    record["tranId"],
                    record.get("tradeId", ""),
                ],
            )

    # 查询当前 symbol 的 funding 间隔，普通合约默认 8h。
    def _live_interval_hours(self, symbol: str) -> int:
        if self.config.funding_interval_hours > 0:
            return self.config.funding_interval_hours
        response = requests.get(
            f"{self.config.funding_api_base_url}/fapi/v1/fundingInfo",
            proxies=self.proxies,
            timeout=10,
        )
        response.raise_for_status()
        for item in response.json():
            if item["symbol"] == symbol:
                return int(item["fundingIntervalHours"])
        return 8

    # 从 NT instrument id 推导 Binance 原生 symbol。
    def _binance_symbol(self) -> str:
        return str(self.config.instrument_id).split(".")[0].replace("-PERP", "")

    # 计算计划开仓时间。
    def _entry_time_ns(self, event: FundingEvent) -> int:
        return event.ts_ns - self._entry_offset_ns()

    # 计算计划平仓时间。
    def _exit_time_ns(self, event: FundingEvent) -> int:
        return event.ts_ns + self._exit_offset_ns()

    # 返回开仓提前量，优先使用毫秒配置。
    def _entry_offset_ns(self) -> int:
        if self.config.entry_milliseconds_before > 0:
            return self.config.entry_milliseconds_before * 1_000_000
        return self.config.entry_minutes_before * 60 * 1_000_000_000

    # 返回平仓延后量，优先使用毫秒配置。
    def _exit_offset_ns(self) -> int:
        if self.config.exit_milliseconds_after > 0:
            return self.config.exit_milliseconds_after * 1_000_000
        return self.config.exit_minutes_after * 60 * 1_000_000_000

    # 计算入场 bar 对计划方向的不利波动。
    def _adverse_entry_move(self, bar: Bar, side: OrderSide) -> Decimal:
        move = (Decimal(str(bar.close)) - Decimal(str(bar.open))) / Decimal(str(bar.open))
        if side == OrderSide.SELL:
            return move
        return -move

    # 从 CSV 读取历史 funding rate，并推断 1h/4h/8h 间隔。
    def _load_funding_events(self, path: Path) -> list[FundingEvent]:
        with path.open("r", newline="", encoding="utf-8") as f:
            rows = sorted(csv.DictReader(f), key=lambda row: int(row["fundingTime"]))

        events = []
        previous_ms = None
        for row in rows:
            ts_ms = round(int(row["fundingTime"]) / 60_000) * 60_000
            interval_hours = 8 if previous_ms is None else max(1, round((ts_ms - previous_ms) / 3_600_000))
            events.append(
                FundingEvent(
                    ts_ns=ts_ms * 1_000_000,
                    rate=Decimal(row["fundingRate"]),
                    interval_hours=interval_hours,
                ),
            )
            previous_ms = ts_ms

        if not events:
            raise RuntimeError(f"No funding events loaded from {path}")
        return events

    # 初始化策略自己的 decision 日志。
    def _init_event_log(self) -> None:
        self.event_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.event_log_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "bar_time",
                    "funding_time",
                    "interval_hours",
                    "funding_rate",
                    "action",
                    "side",
                    "bar_open",
                    "bar_close",
                    "quantity",
                    "notional",
                    "estimated_funding_income",
                    "adverse_entry_move",
                    "local_time",
                    "delta_to_funding_ms",
                    "reason",
                ],
            )

    # 初始化真实 funding fee 到账日志。
    def _init_funding_fee_log(self) -> None:
        self.funding_fees_path.parent.mkdir(parents=True, exist_ok=True)
        with self.funding_fees_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "funding_time",
                    "income_time",
                    "delta_ms",
                    "symbol",
                    "income",
                    "asset",
                    "income_type",
                    "info",
                    "tran_id",
                    "trade_id",
                ],
            )

    # 记录每次资金费事件的交易或跳过原因。
    def _record_decision(
        self,
        market_data: Bar | TradeTick,
        event: FundingEvent | None,
        action: str,
        side: str,
        reason: str,
    ) -> None:
        quantity = ""
        notional = ""
        funding_income = ""
        adverse_move = ""
        open_price, close_price = self._market_prices(market_data)
        local_ts_ns = self.clock.timestamp_ns()
        delta_to_funding_ms = ""
        if event is not None:
            delta_to_funding_ms = (local_ts_ns - event.ts_ns) / 1_000_000
        if event is not None and action == "OPEN":
            quantity = Decimal(str(self.config.trade_size))
            notional = quantity * close_price
            funding_income = abs(event.rate) * notional
            if isinstance(market_data, Bar):
                adverse_move = self._adverse_entry_move(market_data, self.active_side)

        with self.event_log_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    self._iso(market_data.ts_event),
                    "" if event is None else self._iso(event.ts_ns),
                    "" if event is None else event.interval_hours,
                    "" if event is None else event.rate,
                    action,
                    side,
                    open_price,
                    close_price,
                    quantity,
                    notional,
                    funding_income,
                    adverse_move,
                    self._iso(local_ts_ns),
                    delta_to_funding_ms,
                    reason,
                ],
            )

    # 返回记录日志用的开盘/成交参考价，tick 模式下两列都写 tick price。
    def _market_prices(self, market_data: Bar | TradeTick) -> tuple[Decimal, Decimal]:
        if isinstance(market_data, TradeTick):
            price = Decimal(str(market_data.price))
            return price, price
        return Decimal(str(market_data.open)), Decimal(str(market_data.close))

    # 记录第一条市场数据，live 日志清洗从这里开始。
    def _log_first_market_data(self, data_kind: str, ts_ns: int) -> None:
        if not self.seen_market_data:
            self.log.info(f"FIRST_MARKET_DATA kind={data_kind} ts={self._iso(ts_ns)}")
            self.seen_market_data = True

    # 把 NT 订单方向转成简短文本。
    def _side_name(self, side: OrderSide) -> str:
        return "BUY" if side == OrderSide.BUY else "SELL"

    # 把 NT 纳秒时间戳转成 UTC 字符串。
    def _iso(self, ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()

    # 停止时撤单并按配置平仓。
    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        if self.config.close_positions_on_stop:
            self.close_all_positions(self.config.instrument_id)
        if self.config.use_trade_ticks:
            self.unsubscribe_trade_ticks(self.config.instrument_id)
        else:
            self.unsubscribe_bars(self.config.bar_type)
