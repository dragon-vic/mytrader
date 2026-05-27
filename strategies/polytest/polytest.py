from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import datetime
from datetime import timezone
from pathlib import Path
from threading import Lock
from zoneinfo import ZoneInfo

import pandas as pd
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import NODE_STOP_TOPIC
from utils.config_loader import ROOT


LOCAL_TZ = ZoneInfo("Asia/Shanghai")
POLYMARKET_VENUE = Venue("POLYMARKET")
POLY_TRADE_COLUMNS = [
    "window_start",
    "window_end",
    "outcome",
    "ts_event_ns",
    "time",
    "price",
    "size",
    "side",
]
POLY_QUOTE_COLUMNS = [
    "window_start",
    "window_end",
    "outcome",
    "ts_event_ns",
    "time",
    "bid_price",
    "bid_size",
    "ask_price",
    "ask_size",
]
BINANCE_TICK_COLUMNS = [
    "ts_event_ns",
    "time",
    "price",
    "size",
]
POLY_TRADE_DEDUP = ["window_start", "outcome", "ts_event_ns", "price", "size", "side"]
POLY_QUOTE_DEDUP = ["window_start", "outcome", "ts_event_ns", "bid_price", "bid_size", "ask_price", "ask_size"]
BINANCE_TICK_DEDUP = ["ts_event_ns", "price", "size"]


class PolyTestConfig(StrategyConfig, frozen=True):
    max_ticks: int
    timeout_sec: int
    flush_sec: int
    poly_trade_path: str
    poly_quote_path: str
    binance_tick_path: str
    btc_spot_instrument_id: str
    event_windows: dict[str, dict[str, int | str]]


class PolyTestStrategy(Strategy):
    def __init__(self, config: PolyTestConfig) -> None:
        super().__init__(config)
        self.row_count = 0
        self.stopped = False
        self.boundary_count = 0
        self.flush_count = 0
        self.poly_trade_path = self._output_path(config.poly_trade_path)
        self.poly_quote_path = self._output_path(config.poly_quote_path)
        self.binance_tick_path = self._output_path(config.binance_tick_path)
        self.btc_spot_instrument_id = InstrumentId.from_str(config.btc_spot_instrument_id)
        self.btc_spot_subscribed = False
        self.trade_subs: set[InstrumentId] = set()
        self.quote_subs: set[InstrumentId] = set()
        self.instruments: dict[InstrumentId, BinaryOption] = {}
        self.poly_trade_buffers: dict[InstrumentId, list[dict[str, object]]] = {}
        self.poly_quote_buffers: dict[InstrumentId, list[dict[str, object]]] = {}
        self.binance_buffer: list[dict[str, object]] = []
        self.event_windows = config.event_windows
        self.buffer_lock = Lock()
        self.write_lock = Lock()

    # 启动后订阅 Binance BTC 成交和当前 BTC 5m Up/Down 成交/盘口。
    def on_start(self) -> None:
        for path in (self.poly_trade_path, self.poly_quote_path, self.binance_tick_path):
            path.parent.mkdir(parents=True, exist_ok=True)
        self._log_account()
        self.subscribe_trade_ticks(self.btc_spot_instrument_id)
        self.btc_spot_subscribed = True
        self._sync_windows()
        self._schedule_boundary()
        self._schedule_flush()
        if self.config.timeout_sec > 0:
            self.clock.set_time_alert_ns(
                "polytest_timeout",
                self.clock.timestamp_ns() + int(self.config.timeout_sec * 1_000_000_000),
                callback=self._on_timeout,
            )
        self._write_status("running")
        self.log.info(
            "polytest启动，订阅BTC 5m Up/Down成交和盘口，"
            f"trade_subs={len(self.trade_subs)} quote_subs={len(self.quote_subs)}",
        )

    # 收到 Polymarket instrument 批量响应后补订阅当前窗口。
    def on_instruments(self, instruments: list) -> None:
        self._refresh_subscriptions(instruments)

    # 收到单个 instrument 响应后补订阅当前窗口。
    def on_instrument(self, instrument) -> None:
        self._refresh_subscriptions([instrument])

    # 收到成交 tick：Binance 单独落盘；Polymarket 写当前窗口 Up/Down 成交。
    def on_trade_tick(self, tick: TradeTick) -> None:
        if tick.instrument_id == self.btc_spot_instrument_id:
            with self.buffer_lock:
                self.binance_buffer.append(self._binance_row(tick))
            self.row_count += 1
            self._stop_if_done()
            return
        if tick.instrument_id not in self.trade_subs:
            return
        if not self._window_active(tick.instrument_id, self.clock.timestamp_ns()):
            return
        instrument = self._instrument(tick.instrument_id)
        if instrument is None:
            return
        row = self._poly_trade_row(tick, instrument)
        with self.buffer_lock:
            self.poly_trade_buffers.setdefault(tick.instrument_id, []).append(row)
        self.row_count += 1
        self._stop_if_done()

    # 收到盘口 tick：写当前窗口 Up/Down ask1/bid1。
    def on_quote_tick(self, tick: QuoteTick) -> None:
        if tick.instrument_id not in self.quote_subs:
            return
        if not self._window_active(tick.instrument_id, self.clock.timestamp_ns()):
            return
        instrument = self._instrument(tick.instrument_id)
        if instrument is None:
            return
        row = self._poly_quote_row(tick, instrument)
        with self.buffer_lock:
            self.poly_quote_buffers.setdefault(tick.instrument_id, []).append(row)
        self.row_count += 1
        self._stop_if_done()

    # 打印启动时已经加载到账户和持仓摘要。
    def _log_account(self) -> None:
        accounts = list(self.cache.accounts())
        positions = list(self.cache.positions_open())
        if not accounts:
            self.log.info(f"账户摘要 accounts=0 open_positions={len(positions)}，exec 账户可能尚未完成加载")
            return
        for account in accounts:
            self.log.info(
                f"账户摘要 account={account.id} type={account.type} "
                f"base={account.base_currency} open_positions={len(positions)} "
                f"balances={account.balances_total()}",
            )

    # 在 5 分钟窗口边界准点切换订阅。
    def _on_boundary(self, _event: TimeEvent) -> None:
        self._sync_windows()
        self._schedule_boundary()

    def _sync_windows(self) -> None:
        self._flush_expired_buffers()
        self._subscribe_ids(InstrumentId.from_str(instrument_id) for instrument_id in self.event_windows)
        self._refresh_subscriptions()

    def _schedule_boundary(self) -> None:
        if self.stopped:
            return
        next_ns = self._next_boundary_ns(self.clock.timestamp_ns())
        if next_ns is None:
            return
        self.boundary_count += 1
        self.clock.set_time_alert_ns(
            f"polytest_boundary_{self.boundary_count}",
            next_ns,
            callback=self._on_boundary,
        )

    def _next_boundary_ns(self, now_ns: int) -> int | None:
        boundaries = []
        for window in self.event_windows.values():
            for key in ("event_start_ns", "event_end_ns"):
                value = int(window[key])
                if value > now_ns:
                    boundaries.append(value)
        return min(boundaries) if boundaries else None

    # 定时写出未到期 buffer，避免运行中数据只留在内存。
    def _on_flush(self, _event: TimeEvent) -> None:
        self._flush_all_buffers()
        self._write_status("running")
        self._schedule_flush()

    def _schedule_flush(self) -> None:
        if self.config.flush_sec <= 0 or self.stopped:
            return
        self.flush_count += 1
        self.clock.set_time_alert_ns(
            f"polytest_flush_{self.flush_count}",
            self.clock.timestamp_ns() + int(self.config.flush_sec * 1_000_000_000),
            callback=self._on_flush,
        )

    # provider 刷新后，当前窗口 Up/Down token 会在这里补订阅成交和盘口。
    def _refresh_subscriptions(self, instruments: Iterable | None = None) -> None:
        now_ns = self.clock.timestamp_ns()
        source = instruments if instruments is not None else self.cache.instruments(venue=POLYMARKET_VENUE)
        for instrument in source:
            if not self._is_poly_option(instrument):
                continue
            if not self._window_known(instrument.id):
                continue
            self.instruments[instrument.id] = instrument
            if not self._window_active(instrument.id, now_ns):
                continue
            self._subscribe_poly(instrument.id)

    def _subscribe_ids(self, instrument_ids: Iterable[InstrumentId]) -> None:
        now_ns = self.clock.timestamp_ns()
        for instrument_id in instrument_ids:
            if not self._window_active(instrument_id, now_ns):
                continue
            self._subscribe_poly(instrument_id)

    def _subscribe_poly(self, instrument_id: InstrumentId) -> int:
        added = 0
        if instrument_id not in self.trade_subs:
            self.subscribe_trade_ticks(instrument_id)
            self.trade_subs.add(instrument_id)
            added += 1
        if instrument_id not in self.quote_subs:
            self.subscribe_quote_ticks(instrument_id)
            self.quote_subs.add(instrument_id)
            added += 1
        if added:
            window = self._window(instrument_id)
            self.log.info(
                f"订阅BTC 5m {self._window_label(window)} "
                f"outcome={self._window_outcome(instrument_id)} "
                f"trade={instrument_id in self.trade_subs} quote={instrument_id in self.quote_subs}",
            )
        return added

    def _unsubscribe_poly(self, instrument_id: InstrumentId) -> None:
        if instrument_id in self.trade_subs:
            self.unsubscribe_trade_ticks(instrument_id)
            self.trade_subs.remove(instrument_id)
        if instrument_id in self.quote_subs:
            self.unsubscribe_quote_ticks(instrument_id)
            self.quote_subs.remove(instrument_id)

    def _is_poly_option(self, instrument) -> bool:
        return isinstance(instrument, BinaryOption) and str(instrument.outcome).lower() in {"up", "down"}

    def _instrument(self, instrument_id: InstrumentId) -> BinaryOption | None:
        instrument = self.instruments.get(instrument_id)
        if instrument is None:
            cached = self.cache.instrument(instrument_id)
            if self._is_poly_option(cached):
                instrument = cached
                self.instruments[instrument_id] = instrument
        return instrument

    # 到期的 event buffer 统一落盘并取消订阅。
    def _flush_expired_buffers(self) -> None:
        now_ns = self.clock.timestamp_ns()
        instrument_ids = set(self.instruments) | self.trade_subs | self.quote_subs
        expired_ids = []
        for instrument_id in list(instrument_ids):
            window = self._window(instrument_id)
            if window and int(window["event_end_ns"]) <= now_ns:
                expired_ids.append(instrument_id)
        if not expired_ids:
            return
        trade_rows, quote_rows = self._drain_instruments(expired_ids)
        for instrument_id in expired_ids:
            self._unsubscribe_poly(instrument_id)
            self.instruments.pop(instrument_id, None)
        self._write_poly_rows(trade_rows, quote_rows, "过期BTC 5m")

    def _flush_all_buffers(self) -> None:
        binance_rows, trade_rows, quote_rows = self._drain_all_buffers()
        if binance_rows:
            self._write_rows(self.binance_tick_path, binance_rows, BINANCE_TICK_COLUMNS, BINANCE_TICK_DEDUP)
            self.log.info(f"落盘Binance BTC tick rows={len(binance_rows)}")
        self._write_poly_rows(trade_rows, quote_rows, "BTC 5m")

    def _drain_all_buffers(self) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
        with self.buffer_lock:
            binance_rows = self.binance_buffer
            trade_rows = [row for rows in self.poly_trade_buffers.values() for row in rows]
            quote_rows = [row for rows in self.poly_quote_buffers.values() for row in rows]
            self.binance_buffer = []
            self.poly_trade_buffers = {}
            self.poly_quote_buffers = {}
        return binance_rows, trade_rows, quote_rows

    def _drain_instruments(
        self,
        instrument_ids: list[InstrumentId],
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        with self.buffer_lock:
            trade_rows = []
            quote_rows = []
            for instrument_id in instrument_ids:
                trade_rows.extend(self.poly_trade_buffers.pop(instrument_id, []))
                quote_rows.extend(self.poly_quote_buffers.pop(instrument_id, []))
        return trade_rows, quote_rows

    def _write_poly_rows(self, trade_rows: list[dict[str, object]], quote_rows: list[dict[str, object]], label: str) -> None:
        if trade_rows:
            self._write_rows(self.poly_trade_path, trade_rows, POLY_TRADE_COLUMNS, POLY_TRADE_DEDUP)
        if quote_rows:
            self._write_rows(self.poly_quote_path, quote_rows, POLY_QUOTE_COLUMNS, POLY_QUOTE_DEDUP)
        if trade_rows or quote_rows:
            self.log.info(f"落盘{label} rows trade={len(trade_rows)} quote={len(quote_rows)}")

    # 合并已有 parquet 后整体去重，写临时文件再替换。
    def _write_rows(
        self,
        path: Path,
        rows: list[dict[str, object]],
        columns: list[str],
        dedup_columns: list[str],
    ) -> None:
        with self.write_lock:
            new_df = pd.DataFrame(rows, columns=columns)
            if path.exists():
                old_df = pd.read_parquet(path)
                merged = pd.concat([old_df, new_df], ignore_index=True)
            else:
                merged = new_df
            merged = (
                merged.drop_duplicates(subset=dedup_columns, keep="last")
                .sort_values([column for column in ("window_start", "outcome", "ts_event_ns") if column in columns])
                .reset_index(drop=True)
            )
            merged = merged.reindex(columns=columns)
            tmp = path.with_name(path.name + ".tmp.parquet")
            merged.to_parquet(tmp, index=False)
            os.replace(tmp, path)

    # 构造 Polymarket 成交 tick 的落盘行。
    def _poly_trade_row(self, tick: TradeTick, instrument: BinaryOption) -> dict[str, object]:
        window = self._window(tick.instrument_id)
        return {
            **self._window_columns(window),
            "outcome": str(instrument.outcome),
            "ts_event_ns": int(tick.ts_event),
            "time": self._local_time(tick.ts_event),
            "price": float(str(tick.price)),
            "size": float(str(tick.size)),
            "side": self._side_label(tick.aggressor_side),
        }

    # 构造 Polymarket ask1/bid1 的落盘行。
    def _poly_quote_row(self, tick: QuoteTick, instrument: BinaryOption) -> dict[str, object]:
        window = self._window(tick.instrument_id)
        return {
            **self._window_columns(window),
            "outcome": str(instrument.outcome),
            "ts_event_ns": int(tick.ts_event),
            "time": self._local_time(tick.ts_event),
            "bid_price": float(str(tick.bid_price)),
            "bid_size": float(str(tick.bid_size)),
            "ask_price": float(str(tick.ask_price)),
            "ask_size": float(str(tick.ask_size)),
        }

    # 构造 Binance BTCUSDT spot trade tick 的落盘行。
    def _binance_row(self, tick: TradeTick) -> dict[str, object]:
        return {
            "ts_event_ns": int(tick.ts_event),
            "time": self._local_time(tick.ts_event),
            "price": float(str(tick.price)),
            "size": float(str(tick.size)),
        }

    def _window_columns(self, window: dict[str, int | str] | None) -> dict[str, object]:
        if window is None:
            return {"window_start": "", "window_end": ""}
        return {
            "window_start": self._local_time(int(window["event_start_ns"])),
            "window_end": self._local_time(int(window["event_end_ns"])),
        }

    def _window_label(self, window: dict[str, int | str] | None) -> str:
        if window is None:
            return "未知窗口"
        return (
            f"{self._local_time(int(window['event_start_ns']))}"
            f"~{self._local_time(int(window['event_end_ns']))}"
        )

    def _window_outcome(self, instrument_id: InstrumentId) -> str:
        window = self._window(instrument_id)
        if window and "outcome" in window:
            return str(window["outcome"])
        instrument = self._instrument(instrument_id)
        return str(instrument.outcome) if instrument else ""

    def _condition_token(self, instrument_id: InstrumentId) -> tuple[str, str]:
        symbol = str(instrument_id).split(".", 1)[0]
        condition_id, token_id = symbol.split("-", 1)
        return condition_id, token_id

    # 把 NT 纳秒时间戳转成北京时间。
    def _local_time(self, timestamp_ns: int) -> str:
        timestamp = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc)
        return timestamp.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")

    def _window(self, instrument_id: InstrumentId) -> dict[str, int | str] | None:
        return self.event_windows.get(str(instrument_id))

    def _window_known(self, instrument_id: InstrumentId) -> bool:
        return self._window(instrument_id) is not None

    def _window_active(self, instrument_id: InstrumentId, now_ns: int) -> bool:
        window = self._window(instrument_id)
        return (
            window is not None
            and int(window["event_start_ns"]) <= now_ns < int(window["event_end_ns"])
        )

    # 把 NT aggressor side 转成人读中文。
    def _side_label(self, side) -> str:
        text = str(side).upper()
        if "BUY" in text:
            return "买"
        if "SELL" in text:
            return "卖"
        return "未知"

    def _short_id(self, instrument_id: InstrumentId) -> str:
        condition_id, token_id = self._condition_token(instrument_id)
        return f"{condition_id[:10]}-{token_id[:10]}"

    def _output_path(self, value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else ROOT / path

    def _write_status(self, status: str) -> None:
        path = self.poly_trade_path.with_name("polytest_status.md")
        text = "\n".join(
            [
                "# polytest status",
                f"status: {status}",
                f"row_count: {self.row_count}",
                f"boundary_count: {self.boundary_count}",
                f"trade_subs: {len(self.trade_subs)}",
                f"quote_subs: {len(self.quote_subs)}",
                f"poly_trade_path: {self.poly_trade_path}",
                f"poly_quote_path: {self.poly_quote_path}",
                f"binance_tick_path: {self.binance_tick_path}",
                f"updated: {datetime.now(LOCAL_TZ).isoformat()}",
            ],
        )
        path.write_text(text + "\n", encoding="utf-8")

    # 超时也停止，方便需要短测时临时打开 timeout_sec。
    def _on_timeout(self, _event: TimeEvent) -> None:
        self.log.info(f"polytest超时停止，rows={self.row_count}")
        self._request_stop()

    # 达到 tick 数量就请求 live node 停止；max_ticks<=0 表示持续运行。
    def _stop_if_done(self) -> None:
        if self.config.max_ticks > 0 and self.row_count >= self.config.max_ticks:
            self._request_stop()

    # 通过 live.py 注册的控制 topic 停止 node。
    def _request_stop(self) -> None:
        if self.stopped:
            return
        self.stopped = True
        self.msgbus.publish(NODE_STOP_TOPIC, {"source": "polytest"})

    # 停止时取消订阅，并强制落盘尚未到期的 buffer。
    def on_stop(self) -> None:
        if self.btc_spot_subscribed:
            self.unsubscribe_trade_ticks(self.btc_spot_instrument_id)
        for instrument_id in list(self.trade_subs | self.quote_subs):
            self._unsubscribe_poly(instrument_id)
        self._flush_all_buffers()
        self._write_status("stopped")
