from __future__ import annotations

import os
from datetime import datetime
from datetime import timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import NODE_STOP_TOPIC
from utils.config_loader import ROOT


LOCAL_TZ = ZoneInfo("Asia/Shanghai")
POLYMARKET_VENUE = Venue("POLYMARKET")
OUTPUT_COLUMNS = [
    "北京时间",
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
]
DEDUP_COLUMNS = ["instrument_id", "trade_id", "ts_event_ns"]


class PolyTestConfig(StrategyConfig, frozen=True):
    instrument_ids: list[InstrumentId]
    bar_types: list[BarType]
    max_ticks: int
    timeout_sec: int
    scan_sec: int
    tick_log_path: str


class PolyTestStrategy(Strategy):
    def __init__(self, config: PolyTestConfig) -> None:
        super().__init__(config)
        self.trade_count = 0
        self.stopped = False
        self.scan_count = 0
        self.output_path = self._output_path(config.tick_log_path)
        self.subscribed: set[InstrumentId] = set()
        self.instruments: dict[InstrumentId, BinaryOption] = {}
        self.buffers: dict[InstrumentId, list[dict[str, object]]] = {}

    # 启动后从 provider 已加载的 Polymarket instruments 中订阅 BTC 5m Up 成交。
    def on_start(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_account()
        self._refresh_subscriptions()
        self._flush_expired_buffers()
        self._schedule_scan()
        if self.config.timeout_sec > 0:
            self.clock.set_time_alert_ns(
                "polytest_timeout",
                self.clock.timestamp_ns() + int(self.config.timeout_sec * 1_000_000_000),
                callback=self._on_timeout,
            )
        self.log.info(
            f"polytest启动，订阅BTC 5m Up成交，当前订阅数={len(self.subscribed)}，落盘={self.output_path}",
        )

    # 收到成交后先进入当前 event buffer，event 结束时统一落盘。
    def on_trade_tick(self, tick: TradeTick) -> None:
        instrument = self._instrument(tick.instrument_id)
        if instrument is None:
            return

        self.trade_count += 1
        self.buffers.setdefault(tick.instrument_id, []).append(self._row(tick, instrument))
        if self.trade_count <= 5 or self.trade_count % 100 == 0:
            self.log.info(
                f"成交 n={self.trade_count} market={self._short_id(tick.instrument_id)} "
                f"price={tick.price} size={tick.size} side={self._side_label(tick.aggressor_side)}",
            )
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

    # provider 刷新后，新增 Up token 会在这里补订阅。
    def _refresh_subscriptions(self) -> None:
        now_ns = self.clock.timestamp_ns()
        added = 0
        for instrument in self.cache.instruments(venue=POLYMARKET_VENUE):
            if not self._is_up_option(instrument):
                continue
            if int(instrument.expiration_ns) <= now_ns:
                continue
            self.instruments[instrument.id] = instrument
            if instrument.id in self.subscribed:
                continue
            self.subscribe_trade_ticks(instrument.id)
            self.subscribed.add(instrument.id)
            added += 1
        if added:
            self.log.info(f"新增订阅BTC 5m Up instrument={added}，总订阅数={len(self.subscribed)}")

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
        for instrument_id, instrument in list(self.instruments.items()):
            if int(instrument.expiration_ns) <= now_ns:
                self._flush_instrument(instrument_id)

    def _flush_instrument(self, instrument_id: InstrumentId) -> None:
        rows = self.buffers.pop(instrument_id, [])
        if not rows:
            return
        self._write_rows(rows)
        self.log.info(f"落盘BTC 5m Up tick rows={len(rows)} instrument={self._short_id(instrument_id)}")

    # 合并已有 parquet 后整体去重，写临时文件再替换。
    def _write_rows(self, rows: list[dict[str, object]]) -> None:
        new_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
        old_df = pd.read_parquet(self.output_path) if self.output_path.exists() else pd.DataFrame(columns=OUTPUT_COLUMNS)
        merged = pd.concat([old_df, new_df], ignore_index=True)
        merged = (
            merged.drop_duplicates(subset=DEDUP_COLUMNS, keep="last")
            .sort_values(["ts_event_ns", "instrument_id", "trade_id"])
            .reset_index(drop=True)
        )
        tmp = self.output_path.with_name(self.output_path.name + ".tmp.parquet")
        merged.to_parquet(tmp, index=False)
        os.replace(tmp, self.output_path)

    # 构造成交 tick 的落盘行。
    def _row(self, tick: TradeTick, instrument: BinaryOption) -> dict[str, object]:
        condition_id, token_id = self._condition_token(tick.instrument_id)
        return {
            "北京时间": self._local_time(tick.ts_event),
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
        }

    def _condition_token(self, instrument_id: InstrumentId) -> tuple[str, str]:
        symbol = str(instrument_id).split(".", 1)[0]
        condition_id, token_id = symbol.split("-", 1)
        return condition_id, token_id

    # 把 NT 纳秒时间戳转成北京时间。
    def _local_time(self, timestamp_ns: int) -> str:
        timestamp = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc)
        return timestamp.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")

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
        for instrument_id in self.subscribed:
            self.unsubscribe_trade_ticks(instrument_id)
        for instrument_id in list(self.buffers):
            self._flush_instrument(instrument_id)
