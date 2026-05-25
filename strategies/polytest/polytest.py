from __future__ import annotations

import os
import time
from collections import deque
from collections.abc import Iterable
from datetime import datetime
from datetime import timezone
from pathlib import Path
from threading import Lock
from threading import Thread
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import NODE_STOP_TOPIC
from utils.config_loader import ROOT


LOCAL_TZ = ZoneInfo("Asia/Shanghai")
POLYMARKET_VENUE = Venue("POLYMARKET")
POLY_TRADES_URL = "https://data-api.polymarket.com/trades"
BINANCE_SYMBOL = "BTCUSDT"
REQUEST_TIMEOUT_SECONDS = 10
OUTPUT_COLUMNS = [
    "source",
    "北京时间",
    "event_start_utc",
    "event_end_utc",
    "ts_event_ns",
    "ts_init_ns",
    "instrument_id",
    "condition_id",
    "token_id",
    "outcome",
    "price",
    "size",
    "side",
    "trade_id",
    "expiration_utc",
    "btc_spot_price",
    "btc_spot_ts_event_ns",
    "btc_spot_age_ms",
    "binance_time",
    "binance_price",
    "binance_size",
    "binance_trade_id",
    "binance_offset_ms",
]
DEDUP_COLUMNS = ["instrument_id", "trade_id", "ts_event_ns"]


class PolyTestConfig(StrategyConfig, frozen=True):
    max_ticks: int
    timeout_sec: int
    scan_sec: int
    flush_sec: int
    tick_log_path: str
    btc_spot_instrument_id: str
    binance_rest_url: str
    history_enabled: bool
    history_window_count: int
    event_windows: dict[str, dict[str, int | str]]


class PolyTestStrategy(Strategy):
    def __init__(self, config: PolyTestConfig) -> None:
        super().__init__(config)
        self.trade_count = 0
        self.stopped = False
        self.scan_count = 0
        self.flush_count = 0
        self.output_path = self._output_path(config.tick_log_path)
        self.subscribed: set[InstrumentId] = set()
        self.btc_spot_instrument_id = InstrumentId.from_str(config.btc_spot_instrument_id)
        self.btc_spot_subscribed = False
        self.btc_spot_price: float | None = None
        self.btc_spot_ts_event_ns: int | None = None
        self.btc_ticks: deque[dict[str, object]] = deque(maxlen=10000)
        self.instruments: dict[InstrumentId, BinaryOption] = {}
        self.buffers: dict[InstrumentId, list[dict[str, object]]] = {}
        self.event_windows = config.event_windows
        self.history_started: set[InstrumentId] = set()
        self.write_lock = Lock()

    # 启动后订阅 Binance BTC 和当前 BTC 5m Up 成交。
    def on_start(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_account()
        self.subscribe_trade_ticks(self.btc_spot_instrument_id)
        self.btc_spot_subscribed = True
        self._subscribe_ids(InstrumentId.from_str(instrument_id) for instrument_id in self.event_windows)
        self._refresh_subscriptions()
        self._flush_expired_buffers()
        self._schedule_scan()
        self._schedule_flush()
        if self.config.timeout_sec > 0:
            self.clock.set_time_alert_ns(
                "polytest_timeout",
                self.clock.timestamp_ns() + int(self.config.timeout_sec * 1_000_000_000),
                callback=self._on_timeout,
            )
        self._write_status("running")
        self.log.info(
            f"polytest启动，订阅BTC 5m Up成交，当前订阅数={len(self.subscribed)}，落盘={self.output_path}",
        )

    # 收到 Polymarket instrument 批量响应后订阅 Up token。
    def on_instruments(self, instruments: list) -> None:
        self._refresh_subscriptions(instruments)

    # 收到单个 instrument 响应后订阅 Up token。
    def on_instrument(self, instrument) -> None:
        self._refresh_subscriptions([instrument])

    # 收到成交后进入当前 event buffer，定时、过期或停止时落盘。
    def on_trade_tick(self, tick: TradeTick) -> None:
        if tick.instrument_id == self.btc_spot_instrument_id:
            self._record_btc_tick(tick)
            return
        if not self._window_active(tick.instrument_id, self.clock.timestamp_ns()):
            return
        instrument = self._instrument(tick.instrument_id)
        if instrument is None:
            return

        self.trade_count += 1
        self.buffers.setdefault(tick.instrument_id, []).append(self._row(tick, instrument, "live"))
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

    # 定时扫描新加载的 BTC 5m Up instrument，并处理到期落盘。
    def _on_scan(self, _event: TimeEvent) -> None:
        self._refresh_subscriptions()
        self._flush_expired_buffers()
        self._schedule_scan()

    def _schedule_scan(self) -> None:
        if self.config.scan_sec <= 0 or self.stopped:
            return
        self.scan_count += 1
        self.clock.set_time_alert_ns(
            f"polytest_scan_{self.scan_count}",
            self.clock.timestamp_ns() + int(self.config.scan_sec * 1_000_000_000),
            callback=self._on_scan,
        )

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

    # provider 刷新后，当前窗口 Up token 会在这里补订阅，并启动历史抓取。
    def _refresh_subscriptions(self, instruments: Iterable | None = None) -> None:
        now_ns = self.clock.timestamp_ns()
        added = 0
        source = instruments if instruments is not None else self.cache.instruments(venue=POLYMARKET_VENUE)
        for instrument in source:
            if not self._is_up_option(instrument):
                continue
            if not self._window_known(instrument.id):
                continue
            self.instruments[instrument.id] = instrument
            self._start_history(instrument.id, instrument, now_ns)
            if not self._window_active(instrument.id, now_ns):
                continue
            if instrument.id in self.subscribed:
                continue
            self.subscribe_trade_ticks(instrument.id)
            self.subscribed.add(instrument.id)
            added += 1
        if added:
            self.log.info(f"新增订阅BTC 5m Up instrument={added}，总订阅数={len(self.subscribed)}")

    def _subscribe_ids(self, instrument_ids: Iterable[InstrumentId]) -> None:
        now_ns = self.clock.timestamp_ns()
        added = 0
        for instrument_id in instrument_ids:
            if not self._window_active(instrument_id, now_ns):
                continue
            if instrument_id in self.subscribed:
                continue
            self.subscribe_trade_ticks(instrument_id)
            self.subscribed.add(instrument_id)
            added += 1
        if added:
            self.log.info(f"按配置订阅BTC 5m Up instrument={added}，总订阅数={len(self.subscribed)}")

    def _is_up_option(self, instrument) -> bool:
        return isinstance(instrument, BinaryOption) and str(instrument.outcome).lower() == "up"

    def _instrument(self, instrument_id: InstrumentId) -> BinaryOption | None:
        instrument = self.instruments.get(instrument_id)
        if instrument is None:
            cached = self.cache.instrument(instrument_id)
            if self._is_up_option(cached):
                instrument = cached
                self.instruments[instrument_id] = instrument
        return instrument

    # 到期的 event buffer 统一落盘。
    def _flush_expired_buffers(self) -> None:
        now_ns = self.clock.timestamp_ns()
        for instrument_id in list(self.instruments):
            window = self._window(instrument_id)
            if window and int(window["event_end_ns"]) <= now_ns:
                self._flush_instrument(instrument_id)
                if instrument_id in self.subscribed:
                    self.unsubscribe_trade_ticks(instrument_id)
                    self.subscribed.remove(instrument_id)
                self.instruments.pop(instrument_id, None)

    def _flush_all_buffers(self) -> None:
        for instrument_id in list(self.buffers):
            self._flush_instrument(instrument_id)

    def _flush_instrument(self, instrument_id: InstrumentId) -> None:
        rows = self.buffers.pop(instrument_id, [])
        if not rows:
            return
        self._write_rows(rows)
        self.log.info(f"落盘BTC 5m Up tick rows={len(rows)} instrument={self._short_id(instrument_id)}")

    # 合并已有 parquet 后整体去重，写临时文件再替换。
    def _write_rows(self, rows: list[dict[str, object]]) -> None:
        with self.write_lock:
            new_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
            if self.output_path.exists():
                old_df = pd.read_parquet(self.output_path)
                merged = pd.concat([old_df, new_df], ignore_index=True)
            else:
                merged = new_df
            merged = (
                merged.drop_duplicates(subset=DEDUP_COLUMNS, keep="last")
                .sort_values(["ts_event_ns", "instrument_id", "trade_id"])
                .reset_index(drop=True)
            )
            merged = merged.reindex(columns=OUTPUT_COLUMNS)
            tmp = self.output_path.with_name(self.output_path.name + ".tmp.parquet")
            merged.to_parquet(tmp, index=False)
            os.replace(tmp, self.output_path)

    # 构造成交 tick 的落盘行。
    def _row(self, tick: TradeTick, instrument: BinaryOption, source: str) -> dict[str, object]:
        condition_id, token_id = self._condition_token(tick.instrument_id)
        window = self._window(tick.instrument_id)
        btc = self._nearest_btc_tick(int(tick.ts_event))
        return {
            "source": source,
            "北京时间": self._local_time(tick.ts_event),
            "event_start_utc": self._utc_time(window["event_start_ns"]) if window else "",
            "event_end_utc": self._utc_time(window["event_end_ns"]) if window else "",
            "ts_event_ns": int(tick.ts_event),
            "ts_init_ns": int(tick.ts_init),
            "instrument_id": str(tick.instrument_id),
            "condition_id": condition_id,
            "token_id": token_id,
            "outcome": str(instrument.outcome),
            "price": float(str(tick.price)),
            "size": float(str(tick.size)),
            "side": self._side_label(tick.aggressor_side),
            "trade_id": str(tick.trade_id),
            "expiration_utc": str(instrument.expiration_utc),
            **self._btc_columns(int(tick.ts_event), btc),
        }

    def _row_from_raw(
        self,
        trade: dict[str, object],
        instrument_id: InstrumentId,
        instrument: BinaryOption,
        btc_ticks: list[dict[str, object]],
    ) -> dict[str, object]:
        condition_id, token_id = self._condition_token(instrument_id)
        ts_event_ns = int(float(trade["timestamp"]) * 1_000_000_000)
        window = self._window(instrument_id)
        btc = self._nearest_from_list(ts_event_ns, btc_ticks)
        return {
            "source": "history",
            "北京时间": self._local_time(ts_event_ns),
            "event_start_utc": self._utc_time(window["event_start_ns"]) if window else "",
            "event_end_utc": self._utc_time(window["event_end_ns"]) if window else "",
            "ts_event_ns": ts_event_ns,
            "ts_init_ns": ts_event_ns,
            "instrument_id": str(instrument_id),
            "condition_id": condition_id,
            "token_id": token_id,
            "outcome": str(instrument.outcome),
            "price": float(trade["price"]),
            "size": float(trade["size"]),
            "side": self._side_label(trade.get("side")),
            "trade_id": self._history_trade_id(trade),
            "expiration_utc": str(instrument.expiration_utc),
            **self._btc_columns(ts_event_ns, btc),
        }

    def _condition_token(self, instrument_id: InstrumentId) -> tuple[str, str]:
        symbol = str(instrument_id).split(".", 1)[0]
        condition_id, token_id = symbol.split("-", 1)
        return condition_id, token_id

    def _history_trade_id(self, trade: dict[str, object]) -> str:
        tx = str(trade.get("transactionHash") or "")
        return "-".join(
            [
                tx[-16:] or "nohash",
                str(trade.get("side") or ""),
                str(trade.get("price") or ""),
                str(trade.get("size") or ""),
                str(trade.get("timestamp") or ""),
            ],
        )

    # 把 NT 纳秒时间戳转成北京时间。
    def _local_time(self, timestamp_ns: int) -> str:
        timestamp = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc)
        return timestamp.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")

    # 把纳秒时间戳转成 UTC 字符串。
    def _utc_time(self, timestamp_ns: int | str) -> str:
        timestamp = datetime.fromtimestamp(int(timestamp_ns) / 1_000_000_000, tz=timezone.utc)
        return timestamp.isoformat()

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

    def _window_recent_or_active(self, instrument_id: InstrumentId, now_ns: int) -> bool:
        window = self._window(instrument_id)
        if window is None:
            return False
        lookback_ns = max(1, self.config.history_window_count) * 300 * 1_000_000_000
        return int(window["event_end_ns"]) >= now_ns - lookback_ns and int(window["event_start_ns"]) <= now_ns

    def _btc_spot_age_ms(self, ts_event_ns: int) -> int | None:
        if self.btc_spot_ts_event_ns is None:
            return None
        return int((int(ts_event_ns) - self.btc_spot_ts_event_ns) / 1_000_000)

    def _record_btc_tick(self, tick: TradeTick) -> None:
        row = {
            "ts_event_ns": int(tick.ts_event),
            "price": float(str(tick.price)),
            "size": float(str(tick.size)),
            "trade_id": str(tick.trade_id),
        }
        self.btc_ticks.append(row)
        self.btc_spot_price = float(row["price"])
        self.btc_spot_ts_event_ns = int(row["ts_event_ns"])

    def _nearest_btc_tick(self, ts_event_ns: int) -> dict[str, object] | None:
        if not self.btc_ticks:
            return None
        return min(self.btc_ticks, key=lambda row: abs(int(row["ts_event_ns"]) - ts_event_ns))

    def _nearest_from_list(
        self,
        ts_event_ns: int,
        rows: list[dict[str, object]],
    ) -> dict[str, object] | None:
        if not rows:
            return None
        return min(rows, key=lambda row: abs(int(row["ts_event_ns"]) - ts_event_ns))

    def _btc_columns(self, ts_event_ns: int, btc: dict[str, object] | None) -> dict[str, object]:
        if btc is None:
            return {
                "btc_spot_price": self.btc_spot_price,
                "btc_spot_ts_event_ns": self.btc_spot_ts_event_ns,
                "btc_spot_age_ms": self._btc_spot_age_ms(ts_event_ns),
                "binance_time": "",
                "binance_price": None,
                "binance_size": None,
                "binance_trade_id": "",
                "binance_offset_ms": None,
            }
        btc_ts = int(btc["ts_event_ns"])
        return {
            "btc_spot_price": float(btc["price"]),
            "btc_spot_ts_event_ns": btc_ts,
            "btc_spot_age_ms": int((int(ts_event_ns) - btc_ts) / 1_000_000),
            "binance_time": self._local_time(btc_ts),
            "binance_price": float(btc["price"]),
            "binance_size": float(btc["size"]),
            "binance_trade_id": str(btc["trade_id"]),
            "binance_offset_ms": int((int(ts_event_ns) - btc_ts) / 1_000_000),
        }

    # 后台抓 Polymarket 历史成交，并用同窗口 Binance aggTrades 做最近时间对齐。
    def _start_history(
        self,
        instrument_id: InstrumentId,
        instrument: BinaryOption,
        now_ns: int,
    ) -> None:
        if not self.config.history_enabled or instrument_id in self.history_started:
            return
        if not self._window_recent_or_active(instrument_id, now_ns):
            return
        self.history_started.add(instrument_id)
        Thread(target=self._load_history, args=(instrument_id, instrument, now_ns), daemon=True).start()

    def _load_history(
        self,
        instrument_id: InstrumentId,
        instrument: BinaryOption,
        now_ns: int,
    ) -> None:
        try:
            condition_id, token_id = self._condition_token(instrument_id)
            window = self._window(instrument_id)
            if window is None:
                return
            start_ns = int(window["event_start_ns"])
            end_ns = min(int(window["event_end_ns"]), now_ns)
            poly = self._fetch_poly_trades(condition_id, token_id, start_ns, end_ns)
            btc = self._fetch_binance_trades(start_ns, end_ns)
            rows = [self._row_from_raw(trade, instrument_id, instrument, btc) for trade in poly]
            if rows:
                self._write_rows(rows)
            self.log.info(
                f"历史tick完成 poly_rows={len(rows)} binance_rows={len(btc)} "
                f"instrument={self._short_id(instrument_id)}",
            )
        except Exception as exc:
            self.log.error(f"历史tick失败 instrument={instrument_id}: {type(exc).__name__}: {exc}")

    def _fetch_poly_trades(
        self,
        condition_id: str,
        token_id: str,
        start_ns: int,
        end_ns: int,
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        offset = 0
        start_s = int(start_ns / 1_000_000_000)
        end_s = int(end_ns / 1_000_000_000)
        session = requests.Session()
        while offset <= 10_000:
            data = self._request_json(
                session,
                POLY_TRADES_URL,
                {"market": condition_id, "limit": 10000, "offset": offset},
            )
            if not data:
                break
            for trade in data:
                if str(trade.get("asset")) != token_id:
                    continue
                ts = int(trade["timestamp"])
                if start_s <= ts <= end_s:
                    rows.append(trade)
            if len(data) < 10000:
                break
            offset += len(data)
        rows.sort(key=lambda row: (int(row["timestamp"]), self._history_trade_id(row)))
        return rows

    def _fetch_binance_trades(self, start_ns: int, end_ns: int) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        start_ms = int(start_ns / 1_000_000)
        end_ms = int(end_ns / 1_000_000)
        current = start_ms
        session = requests.Session()
        while current <= end_ms:
            data = self._request_json(
                session,
                f"{self.config.binance_rest_url.rstrip('/')}/api/v3/aggTrades",
                {
                    "symbol": BINANCE_SYMBOL,
                    "startTime": current,
                    "endTime": end_ms,
                    "limit": 1000,
                },
            )
            if not data:
                break
            for trade in data:
                rows.append(
                    {
                        "ts_event_ns": int(trade["T"]) * 1_000_000,
                        "price": float(trade["p"]),
                        "size": float(trade["q"]),
                        "trade_id": str(trade["a"]),
                    },
                )
            last_ms = int(data[-1]["T"])
            if last_ms < current:
                break
            current = last_ms + 1
            if len(data) < 1000:
                break
            time.sleep(0.1)
        rows.sort(key=lambda row: int(row["ts_event_ns"]))
        return rows

    def _request_json(
        self,
        session: requests.Session,
        url: str,
        params: dict[str, object],
    ):
        for attempt in range(3):
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 429 and attempt < 2:
                time.sleep(attempt + 1)
                continue
            response.raise_for_status()
            return response.json()
        return []

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
        path = self.output_path.with_name("polytest_status.md")
        text = "\n".join(
            [
                "# polytest status",
                f"status: {status}",
                f"trade_count: {self.trade_count}",
                f"subscribed: {len(self.subscribed)}",
                f"history_started: {len(self.history_started)}",
                f"output: {self.output_path}",
                f"updated: {datetime.now(LOCAL_TZ).isoformat()}",
            ],
        )
        path.write_text(text + "\n", encoding="utf-8")

    # 超时也停止，方便需要短测时临时打开 timeout_sec。
    def _on_timeout(self, _event: TimeEvent) -> None:
        self.log.info(f"polytest超时停止，trade_ticks={self.trade_count}")
        self._request_stop()

    # 达到 tick 数量就请求 live node 停止；max_ticks<=0 表示持续运行。
    def _stop_if_done(self) -> None:
        if self.config.max_ticks > 0 and self.trade_count >= self.config.max_ticks:
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
        for instrument_id in self.subscribed:
            self.unsubscribe_trade_ticks(instrument_id)
        self._flush_all_buffers()
        self._write_status("stopped")
