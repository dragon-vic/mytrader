from __future__ import annotations

import os
import json
from html import escape
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
from nautilus_trader.trading.strategy import Strategy

from utils.arguments import NODE_STOP_TOPIC
from utils.config_loader import ROOT
from strategies.worldcup_price.history import fetch_events
from strategies.worldcup_price.history import fetch_trades
from strategies.worldcup_price.polymarket_worldcup import next_match_yes_windows
from strategies.worldcup_price.webview import ViewServer


LOCAL_TZ = ZoneInfo("Asia/Shanghai")
TICK_COLUMNS = [
    "event_slug",
    "event_title",
    "market_slug",
    "market_question",
    "label",
    "instrument_id",
    "condition_id",
    "token_id",
    "record_type",
    "ts_event_ns",
    "time",
    "price",
    "size",
    "side",
    "bid_price",
    "bid_size",
    "ask_price",
    "ask_size",
    "source",
    "trade_id",
]
TICK_DEDUP = [
    "instrument_id",
    "record_type",
    "ts_event_ns",
    "price",
    "size",
    "side",
    "bid_price",
    "bid_size",
    "ask_price",
    "ask_size",
    "trade_id",
]
CHART_COLORS = ["#7cc7ff", "#ffcd66", "#83e377"]


class WorldcupPriceConfig(StrategyConfig, frozen=True):
    max_ticks: int
    timeout_sec: int
    flush_sec: int
    tick_rows: int
    html_refresh_ms: int
    html_host: str
    html_port: int
    backfill_on_start: bool
    backfill_from_start: bool
    backfill_limit: int
    backfill_max_pages: int
    backfill_pause_ms: int
    event_refresh_sec: int
    persist_ticks: bool
    tick_path: str
    proxy_url: str


class WorldcupPriceStrategy(Strategy):
    def __init__(self, config: WorldcupPriceConfig) -> None:
        super().__init__(config)
        self.tick_path = self._output_path(config.tick_path)
        self.status_path = self.tick_path.with_name("worldcup_price_status.md")
        self.targets: dict[str, dict[str, int | str]] = {}
        self.id_labels: dict[str, str] = {}
        self.trade_subs: set[InstrumentId] = set()
        self.quote_subs: set[InstrumentId] = set()
        self.buffer: list[dict[str, object]] = []
        self.persist_df = pd.DataFrame(columns=TICK_COLUMNS)
        self.latest_quote: dict[str, dict[str, float | str]] = {}
        self.latest_trade: dict[str, dict[str, float | str]] = {}
        self.trade_history: dict[str, list[dict[str, float | int | str]]] = {}
        self.recent_trades: list[dict[str, float | int | str]] = []
        self.match_events: list[dict[str, object]] = []
        self.quote_counts: dict[str, int] = {}
        self.trade_counts: dict[str, int] = {}
        self.row_count = 0
        self.backfill_rows = 0
        self.flush_count = 0
        self.event_count = 0
        self.view_server: ViewServer | None = None
        self.stopped = False
        self.buffer_lock = Lock()
        self.write_lock = Lock()
        self.state_lock = Lock()

    # 启动时发现下一场世界杯三个 Yes token，并订阅成交和盘口。
    def on_start(self) -> None:
        self.tick_path.parent.mkdir(parents=True, exist_ok=True)
        self.targets = next_match_yes_windows(self.config.proxy_url or None)
        self.id_labels = {instrument_id: str(target["label"]) for instrument_id, target in self.targets.items()}
        self._load_existing()
        self._fetch_match_events()
        self._backfill_startup()
        self._write_status("running")
        self._start_view()
        self._subscribe_targets()
        self._schedule_flush()
        self._schedule_events()
        if self.config.timeout_sec > 0:
            self.clock.set_time_alert_ns(
                "worldcup_price_timeout",
                self.clock.timestamp_ns() + int(self.config.timeout_sec * 1_000_000_000),
                callback=self._on_timeout,
            )
        self.log.info(
            "worldcup_price启动，"
            f"event={self._event_title()} targets={len(self.targets)} "
            f"backfill_rows={self.backfill_rows} "
            f"html={self._view_url()} "
            f"persist={self.config.persist_ticks}",
        )

    # provider 加载 instrument 后补订阅，避免启动时 instrument 尚未进 cache。
    def on_instruments(self, _instruments: list) -> None:
        self._subscribe_targets()

    def on_instrument(self, _instrument) -> None:
        self._subscribe_targets()

    def on_trade_tick(self, tick: TradeTick) -> None:
        if str(tick.instrument_id) not in self.targets:
            return
        price = float(str(tick.price))
        size = float(str(tick.size))
        label = self.id_labels[str(tick.instrument_id)]
        with self.state_lock:
            self.latest_trade[label] = {
                "price": price,
                "size": size,
                "time": self._local_time(tick.ts_event),
                "ts_event_ns": int(tick.ts_event),
                "side": self._side_label(tick.aggressor_side),
            }
            trade = {
                "label": label,
                "price": price,
                "size": size,
                "time": self._local_time(tick.ts_event),
                "ts_event_ns": int(tick.ts_event),
                "side": self._side_label(tick.aggressor_side),
            }
            self.trade_history.setdefault(label, []).append(trade)
            self.recent_trades.append(trade)
            self._trim_history(label)
            self.trade_counts[label] = self.trade_counts.get(label, 0) + 1
        if self.config.persist_ticks:
            row = self._trade_row(tick)
            with self.buffer_lock:
                self.buffer.append(row)
        self.row_count += 1
        self._stop_if_done()

    def on_quote_tick(self, tick: QuoteTick) -> None:
        if str(tick.instrument_id) not in self.targets:
            return
        bid = float(str(tick.bid_price))
        ask = float(str(tick.ask_price))
        mid = (bid + ask) / 2
        label = self.id_labels[str(tick.instrument_id)]
        with self.state_lock:
            self.latest_quote[label] = {
                "bid": bid,
                "bid_size": float(str(tick.bid_size)),
                "ask": ask,
                "ask_size": float(str(tick.ask_size)),
                "mid": mid,
                "spread": ask - bid,
                "time": self._local_time(tick.ts_event),
                "ts_event_ns": int(tick.ts_event),
            }
            self.quote_counts[label] = self.quote_counts.get(label, 0) + 1
        if self.config.persist_ticks:
            row = self._quote_row(tick)
            with self.buffer_lock:
                self.buffer.append(row)
        self.row_count += 1
        self._stop_if_done()

    def _subscribe_targets(self) -> None:
        for raw_id in self.targets:
            instrument_id = InstrumentId.from_str(raw_id)
            if instrument_id not in self.trade_subs:
                self.subscribe_trade_ticks(instrument_id)
                self.trade_subs.add(instrument_id)
            if instrument_id not in self.quote_subs:
                self.subscribe_quote_ticks(instrument_id)
                self.quote_subs.add(instrument_id)
        if self.targets:
            self.log.info(f"已订阅 World Cup YES tokens trade={len(self.trade_subs)} quote={len(self.quote_subs)}")

    def _unsubscribe_targets(self) -> None:
        for instrument_id in list(self.trade_subs):
            self.unsubscribe_trade_ticks(instrument_id)
            self.trade_subs.remove(instrument_id)
        for instrument_id in list(self.quote_subs):
            self.unsubscribe_quote_ticks(instrument_id)
            self.quote_subs.remove(instrument_id)

    def _on_flush(self, _event: TimeEvent) -> None:
        self._flush()
        self._write_status("running")
        self._schedule_flush()

    def _schedule_flush(self) -> None:
        if not self.config.persist_ticks or self.config.flush_sec <= 0 or self.stopped:
            return
        self.flush_count += 1
        self.clock.set_time_alert_ns(
            f"worldcup_price_flush_{self.flush_count}",
            self.clock.timestamp_ns() + int(self.config.flush_sec * 1_000_000_000),
            callback=self._on_flush,
        )

    def _on_events(self, _event: TimeEvent) -> None:
        self._fetch_match_events()
        self._schedule_events()

    def _schedule_events(self) -> None:
        if self.config.event_refresh_sec <= 0 or self.stopped:
            return
        self.event_count += 1
        self.clock.set_time_alert_ns(
            f"worldcup_price_events_{self.event_count}",
            self.clock.timestamp_ns() + int(self.config.event_refresh_sec * 1_000_000_000),
            callback=self._on_events,
        )

    def _on_timeout(self, _event: TimeEvent) -> None:
        self.log.info(f"worldcup_price超时停止 rows={self.row_count}")
        self._request_stop()

    def _stop_if_done(self) -> None:
        if self.config.max_ticks > 0 and self.row_count >= self.config.max_ticks:
            self._request_stop()

    def _request_stop(self) -> None:
        if self.stopped:
            return
        self.stopped = True
        self.msgbus.publish(NODE_STOP_TOPIC, {"source": "worldcup_price"})

    def _flush(self) -> None:
        if not self.config.persist_ticks:
            return
        with self.buffer_lock:
            rows = self.buffer
            self.buffer = []
        if not rows:
            return
        self._write_rows(rows)
        self.log.info(f"落盘 World Cup Polymarket tick rows={len(rows)} path={self.tick_path}")

    # 合并旧 parquet 后去重，避免定时 flush 或重启带来重复行。
    def _write_rows(self, rows: list[dict[str, object]]) -> None:
        with self.write_lock:
            new_df = pd.DataFrame(rows, columns=TICK_COLUMNS)
            if self.persist_df.empty and self.tick_path.exists():
                self.persist_df = pd.read_parquet(self.tick_path).reindex(columns=TICK_COLUMNS)
            if self.persist_df.empty:
                merged = new_df
            else:
                merged = pd.concat([self.persist_df, new_df], ignore_index=True)
            merged = (
                merged.drop_duplicates(subset=TICK_DEDUP, keep="last")
                .sort_values(["ts_event_ns", "instrument_id", "record_type"])
                .reset_index(drop=True)
            )
            tmp = self.tick_path.with_name(self.tick_path.name + ".tmp.parquet")
            merged.to_parquet(tmp, index=False)
            os.replace(tmp, self.tick_path)
            self.persist_df = merged

    def _load_existing(self) -> None:
        if not self.tick_path.exists():
            return
        frame = pd.read_parquet(self.tick_path).reindex(columns=TICK_COLUMNS)
        self.persist_df = frame
        frame = frame[frame["instrument_id"].isin(set(self.targets))]
        rows = frame.to_dict("records")
        self._ingest_rows(rows)
        self.row_count = len(rows)

    def _backfill_startup(self) -> None:
        if not self.config.backfill_on_start:
            return
        event_start_ns = self._event_start_ns(int(datetime.now(timezone.utc).timestamp() * 1_000_000_000))
        since_ns = event_start_ns - 1 if self.config.backfill_from_start else self._last_trade_ns()
        if since_ns <= 0:
            since_ns = event_start_ns - 1
        try:
            rows = fetch_trades(
                self.targets,
                since_ns,
                self.config.backfill_limit,
                self.config.backfill_max_pages,
                self.config.backfill_pause_ms,
                self.config.proxy_url,
            )
        except Exception as exc:
            self.log.warning(f"历史 tick 回补失败，继续实时订阅: {exc}")
            return
        if not rows:
            return
        self._write_rows(rows)
        self._ingest_rows(rows)
        self.backfill_rows = len(rows)
        self.row_count += len(rows)

    def _fetch_match_events(self) -> None:
        if not self.targets:
            return
        start_ns = self._event_start_ns(int(datetime.now(timezone.utc).timestamp() * 1_000_000_000))
        try:
            self.match_events = fetch_events(self._event_title(), start_ns, self.config.proxy_url)
        except Exception as exc:
            self.log.warning(f"比赛事件拉取失败，HTML 暂不显示事件: {exc}")

    def _ingest_rows(self, rows: list[dict[str, object]]) -> None:
        with self.state_lock:
            for row in rows:
                if row.get("record_type") != "trade":
                    continue
                label = str(row["label"])
                trade = {
                    "label": label,
                    "price": float(row["price"]),
                    "size": float(row["size"]),
                    "time": str(row["time"]),
                    "ts_event_ns": int(row["ts_event_ns"]),
                    "side": str(row.get("side") or ""),
                }
                self.latest_trade[label] = trade
                self.trade_history.setdefault(label, []).append(trade)
                self.recent_trades.append(trade)
                self.trade_counts[label] = self.trade_counts.get(label, 0) + 1
            for label in list(self.trade_history):
                self.trade_history[label].sort(key=lambda trade: int(trade["ts_event_ns"]))
            self.recent_trades.sort(key=lambda trade: int(trade["ts_event_ns"]))
            tape_limit = max(30, self.config.tick_rows * 3)
            if len(self.recent_trades) > tape_limit:
                del self.recent_trades[:-tape_limit]

    def _last_trade_ns(self) -> int:
        latest = 0
        if self.tick_path.exists():
            frame = pd.read_parquet(self.tick_path, columns=["instrument_id", "record_type", "ts_event_ns"])
            frame = frame[frame["instrument_id"].isin(set(self.targets))]
            trades = frame[frame["record_type"] == "trade"]
            if not trades.empty:
                latest = max(latest, int(trades["ts_event_ns"].max()))
        for rows in self.trade_history.values():
            if rows:
                latest = max(latest, int(rows[-1]["ts_event_ns"]))
        return latest

    def _start_view(self) -> None:
        if self.config.html_refresh_ms <= 0:
            return
        self.view_server = ViewServer(
            self.config.html_host,
            self.config.html_port,
            self._html_doc,
            self._json_response,
        )
        self.view_server.start()

    def _view_url(self) -> str:
        if self.view_server:
            return self.view_server.url
        if self.config.html_refresh_ms <= 0:
            return "disabled"
        return f"http://{self.config.html_host}:{self.config.html_port}/"

    def _json_response(self) -> str:
        with self.state_lock:
            return self._json_doc(int(datetime.now(timezone.utc).timestamp() * 1_000_000_000))

    def _trim_history(self, label: str) -> None:
        tape_limit = max(30, self.config.tick_rows * 3)
        if len(self.recent_trades) > tape_limit:
            del self.recent_trades[:-tape_limit]

    def _json_doc(self, now_ns: int) -> str:
        title = self._event_title() or "waiting for event"
        labels = sorted(set(self.id_labels.values()) | set(self.latest_quote) | set(self.latest_trade))
        start_ns = max(self._event_start_ns(now_ns), now_ns - 600_000_000_000)
        updated = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        return json.dumps(
            {
                "title": title,
                "meta": (
                    f"updated {updated} | window {self._html_time(start_ns)}-{self._html_time(now_ns)} | "
                    f"now {self._html_time(now_ns)} | ticks {self.row_count} | "
                    f"backfill {self.backfill_rows} | events {len(self.match_events)}"
                ),
                "quotes": "\n".join(self._html_quote_row(label, now_ns) for label in labels),
                "charts": self._html_chart(labels, start_ns, now_ns),
                "events": "\n".join(self._html_event_row(event) for event in self.match_events),
                "tape": "\n".join(self._html_tape_row(trade, now_ns) for trade in reversed(self.recent_trades[-self.config.tick_rows :])),
            },
            ensure_ascii=False,
        )

    def _html_doc(self) -> str:
        title = self._event_title() or "waiting for event"
        interval_ms = max(250, self.config.html_refresh_ms)
        return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{escape(title)} Polymarket realtime</title>
  <style>
    body {{ margin: 0; background: #0d1117; color: #d6dee6; font: 13px/1.35 Consolas, Menlo, monospace; }}
    header {{ position: sticky; top: 0; z-index: 2; background: #111820; border-bottom: 1px solid #263241; padding: 10px 14px; }}
    h1 {{ font-size: 15px; margin: 0 0 6px; font-weight: 600; }}
    .meta {{ color: #8b9bab; display: flex; gap: 18px; flex-wrap: wrap; }}
    main {{ padding: 12px 14px 28px; }}
    table {{ border-collapse: collapse; min-width: 980px; }}
    th, td {{ padding: 4px 8px; border-bottom: 1px solid #202a35; text-align: right; white-space: nowrap; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ color: #8b9bab; font-weight: 500; }}
    .panel {{ margin-top: 14px; border-top: 1px solid #263241; padding-top: 10px; }}
    .chart-wrap {{ overflow: hidden; background: #0a0f14; border: 1px solid #263241; }}
    .chart-title {{ margin: 0 0 6px; color: #f0f3f6; }}
    .empty {{ color: #8b9bab; padding: 16px; }}
    svg {{ display: block; }}
    .tick-chart {{ width: 100%; height: 440px; }}
    .point {{ stroke: #0a0f14; stroke-width: 1; }}
    .grid {{ stroke: #1e2935; stroke-width: 1; }}
    .event-line {{ stroke: #f0c36a; stroke-width: 1; opacity: .85; }}
    .event-label {{ fill: #f0c36a; font-size: 11px; }}
    .axis {{ fill: #8b9bab; font-size: 11px; }}
    .vol {{ fill: #3d6f9f; opacity: .8; }}
    .price-tag {{ font-size: 12px; font-weight: 600; }}
    .legend {{ color: #8b9bab; margin: 6px 0 0; }}
    .legend span {{ display: inline-flex; align-items: center; gap: 6px; margin-right: 18px; }}
    .swatch {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%; }}
  </style>
</head>
<body>
  <header>
    <h1 id="title">{escape(title)} | World Cup Polymarket YES realtime</h1>
    <div id="meta" class="meta">waiting for data...</div>
  </header>
  <main>
    <table>
      <thead><tr><th>market</th><th>bid</th><th>bid size</th><th>ask</th><th>ask size</th><th>sprd</th><th>mid</th><th>last</th><th>last size</th><th>side</th><th>q cnt</th><th>t cnt</th><th>age</th></tr></thead>
      <tbody id="quotes"></tbody>
    </table>
    <div id="charts"></div>
    <div class="panel">
      <div class="chart-title">Match events</div>
      <table>
        <thead><tr><th>time</th><th>minute</th><th>kind</th><th>team</th><th>text</th></tr></thead>
        <tbody id="events"></tbody>
      </table>
    </div>
    <div class="panel">
      <div class="chart-title">Recent trade ticks</div>
      <table>
        <thead><tr><th>time</th><th>market</th><th>price</th><th>size</th><th>side</th><th>age</th></tr></thead>
        <tbody id="tape"></tbody>
      </table>
    </div>
  </main>
  <script>
    const refreshMs = {interval_ms};

    async function refreshData() {{
      const response = await fetch('/data.json', {{ cache: 'no-store' }});
      const data = await response.json();
      document.getElementById('title').textContent = data.title + ' | World Cup Polymarket YES realtime';
      document.getElementById('meta').textContent = data.meta;
      document.getElementById('quotes').innerHTML = data.quotes;
      document.getElementById('charts').innerHTML = data.charts;
      document.getElementById('events').innerHTML = data.events;
      document.getElementById('tape').innerHTML = data.tape;
    }}

    refreshData();
    setInterval(refreshData, refreshMs);
  </script>
</body>
</html>
"""

    def _html_quote_row(self, label: str, now_ns: int) -> str:
        quote = self.latest_quote.get(label, {})
        trade = self.latest_trade.get(label, {})
        latest_ns = max(int(quote.get("ts_event_ns", 0)), int(trade.get("ts_event_ns", 0)))
        cells = [
            label,
            self._fmt_price(quote.get("bid")),
            self._fmt_size(quote.get("bid_size")),
            self._fmt_price(quote.get("ask")),
            self._fmt_size(quote.get("ask_size")),
            self._fmt_price(quote.get("spread")),
            self._fmt_price(quote.get("mid")),
            self._fmt_price(trade.get("price")),
            self._fmt_size(trade.get("size")),
            str(trade.get("side", "-")),
            str(self.quote_counts.get(label, 0)),
            str(self.trade_counts.get(label, 0)),
            self._fmt_age(now_ns, latest_ns),
        ]
        return "<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in cells) + "</tr>"

    def _html_event_row(self, event: dict[str, object]) -> str:
        cells = [
            self._html_time(int(event["ts_event_ns"])),
            str(event.get("minute") or ""),
            str(event.get("kind") or ""),
            str(event.get("team") or ""),
            str(event.get("text") or ""),
        ]
        return "<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in cells) + "</tr>"

    def _html_tape_row(self, trade: dict[str, float | int | str], now_ns: int) -> str:
        cells = [
            str(trade["time"])[-15:-3],
            str(trade["label"]),
            f"{float(trade['price']):.4f}",
            self._fmt_size(trade["size"]),
            str(trade["side"]),
            self._fmt_age(now_ns, int(trade["ts_event_ns"])),
        ]
        return "<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in cells) + "</tr>"

    def _html_chart(self, labels: list[str], start_ns: int, now_ns: int) -> str:
        series: dict[str, list[dict[str, float | int | str]]] = {}
        all_trades: list[dict[str, float | int | str]] = []
        for label in labels:
            trades = [
                trade for trade in self.trade_history.get(label, [])
                if start_ns <= int(trade["ts_event_ns"]) <= now_ns
            ]
            if trades:
                series[label] = trades
                all_trades.extend(trades)
        if not all_trades:
            return '<div class="panel"><div class="chart-title">YES tick window</div><div class="empty">waiting for trade ticks</div></div>'

        width = 1200
        chart_h = 285
        vol_h = 105
        pad_l = 22
        pad_r = 150
        pad_t = 24
        gap = 22
        pad_b = 34
        height = pad_t + chart_h + gap + vol_h + pad_b
        inner_w = width - pad_l - pad_r
        window_sec = max(1, int((now_ns - start_ns) / 1_000_000_000))
        low = min(float(trade["price"]) for trade in all_trades)
        high = max(float(trade["price"]) for trade in all_trades)
        if high == low:
            low -= 0.005
            high += 0.005
        notionals = sorted(float(trade["price"]) * float(trade["size"]) for trade in all_trades)
        cap_idx = min(len(notionals) - 1, int(len(notionals) * 0.95))
        notional_cap = max(1.0, notionals[cap_idx])

        point_parts = []
        legend_parts = []
        latest = []
        for idx, (label, trades) in enumerate(series.items()):
            color = CHART_COLORS[idx % len(CHART_COLORS)]
            legend_parts.append(
                f'<span><i class="swatch" style="background:{color}"></i>{escape(label)}</span>'
            )
            for trade in trades:
                notional = float(trade["price"]) * float(trade["size"])
                radius = self._tick_radius(notional, notional_cap)
                x = self._svg_x(int(trade["ts_event_ns"]), start_ns, pad_l, inner_w, window_sec)
                y = self._svg_y(float(trade["price"]), low, high, pad_t, chart_h)
                tip = escape(
                    f"{label} {str(trade['time'])[-15:-3]} "
                    f"price={float(trade['price']):.4f} size={float(trade['size']):.2f} notional={notional:.2f}"
                )
                point_parts.append(
                    f'<circle class="point" cx="{x:.1f}" cy="{y:.1f}" r="{radius:.1f}" fill="{color}"><title>{tip}</title></circle>'
                )
            last = trades[-1]
            latest.append((
                label,
                color,
                float(last["price"]),
                self._svg_y(float(last["price"]), low, high, pad_t, chart_h),
            ))

        vol_parts = self._svg_volumes(all_trades, start_ns, pad_l, pad_t + chart_h + gap, vol_h, inner_w, window_sec)
        markers = self._svg_events(start_ns, now_ns, pad_l, pad_t, chart_h + gap + vol_h, inner_w, window_sec)
        grid = self._svg_grid(start_ns, now_ns, pad_l, pad_t, chart_h, vol_h, gap, inner_w, width, pad_r, low, high)
        tags = self._svg_price_tags(latest, width - pad_r + 12, pad_t, chart_h)
        return f"""
    <div class="panel">
      <div class="chart-title">YES trade ticks | 10m window | ticks={len(all_trades)} | combined volume below</div>
      <div class="chart-wrap">
        <svg class="tick-chart" viewBox="0 0 {width} {height}" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
          {grid}
          {vol_parts}
          {markers}
          {"".join(point_parts)}
          {tags}
        </svg>
      </div>
      <div class="legend">{"".join(legend_parts)}<span>蓝柱：三个 YES 合并成交量，5 秒一桶</span></div>
    </div>"""

    def _svg_y(self, price: float, low: float, high: float, top: int, height: int) -> float:
        return top + (high - price) / (high - low) * height

    def _svg_x(self, timestamp_ns: int, start_ns: int, pad_l: int, inner_w: int, window_sec: int) -> float:
        sec = max(0, min(window_sec, (timestamp_ns - start_ns) / 1_000_000_000))
        return pad_l + sec / window_sec * inner_w

    def _tick_radius(self, notional: float, cap: float) -> float:
        ratio = min(1.0, max(0.0, notional / cap))
        return 2.4 + ratio ** 0.5 * 5.8

    def _svg_price_tags(
        self,
        latest: list[tuple[str, str, float, float]],
        x: int,
        pad_t: int,
        chart_h: int,
    ) -> str:
        if not latest:
            return ""
        rows = sorted(latest, key=lambda row: row[3])
        min_gap = 18
        placed: list[tuple[str, str, float, float]] = []
        for label, color, price, y in rows:
            if placed:
                y = max(y, placed[-1][3] + min_gap)
            placed.append((label, color, price, y))
        overflow = placed[-1][3] - (pad_t + chart_h)
        if overflow > 0:
            placed = [(label, color, price, y - overflow) for label, color, price, y in placed]
        parts = []
        for label, color, price, y in placed:
            y = max(pad_t + 10, min(pad_t + chart_h, y))
            parts.append(f'<line x1="{x - 10}" x2="{x - 2}" y1="{y:.1f}" y2="{y:.1f}" stroke="{color}" stroke-width="2"/>')
            parts.append(f'<text class="price-tag" x="{x}" y="{y + 4:.1f}" fill="{color}">{escape(label)} {price:.4f}</text>')
        return "".join(parts)

    def _svg_volumes(
        self,
        trades: list[dict[str, float | int | str]],
        start_ns: int,
        pad_l: int,
        top: int,
        height: int,
        inner_w: int,
        window_sec: int,
    ) -> str:
        volumes: dict[int, float] = {}
        bucket_sec = 5
        bucket_ns = bucket_sec * 1_000_000_000
        for trade in trades:
            bucket_ns_start = int(trade["ts_event_ns"]) // bucket_ns * bucket_ns
            volumes[bucket_ns_start] = volumes.get(bucket_ns_start, 0.0) + float(trade["size"])
        if not volumes:
            return ""
        max_vol = max(volumes.values())
        bar_w = max(2, inner_w * bucket_sec / window_sec - 1)
        parts = []
        for bucket_ns_start, volume in sorted(volumes.items()):
            vh = max(1, volume / max_vol * height)
            x = self._svg_x(bucket_ns_start + bucket_ns // 2, start_ns, pad_l, inner_w, window_sec) - bar_w / 2
            y = top + height - vh
            parts.append(f'<rect class="vol" x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{vh:.1f}"/>')
        parts.append(f'<text class="axis" x="{pad_l + inner_w + 8}" y="{top + 11}">max vol {max_vol:.0f}</text>')
        return "".join(parts)

    def _svg_events(
        self,
        start_ns: int,
        now_ns: int,
        pad_l: int,
        pad_t: int,
        height: int,
        inner_w: int,
        window_sec: int,
    ) -> str:
        parts = []
        displayed = 0
        for event in self.match_events:
            timestamp_ns = int(event["ts_event_ns"])
            if timestamp_ns < start_ns or timestamp_ns > now_ns:
                continue
            x = self._svg_x(timestamp_ns, start_ns, pad_l, inner_w, window_sec)
            kind = escape(str(event.get("kind") or "event"))
            minute = escape(str(event.get("minute") or self._html_time(timestamp_ns)[-8:-3]))
            label_y = pad_t + 14 + displayed % 4 * 14
            parts.append(f'<line class="event-line" x1="{x:.1f}" x2="{x:.1f}" y1="{pad_t}" y2="{pad_t + height}"/>')
            parts.append(f'<text class="event-label" x="{x + 4:.1f}" y="{label_y}">{minute} {kind}</text>')
            displayed += 1
        return "".join(parts)

    def _svg_grid(
        self,
        start_ns: int,
        now_ns: int,
        pad_l: int,
        pad_t: int,
        chart_h: int,
        vol_h: int,
        gap: int,
        inner_w: int,
        width: int,
        pad_r: int,
        low: float,
        high: float,
    ) -> str:
        right = pad_l + inner_w
        mid = (low + high) / 2
        parts = [
            f'<line class="grid" x1="{pad_l}" x2="{right}" y1="{pad_t}" y2="{pad_t}"/>',
            f'<line class="grid" x1="{pad_l}" x2="{right}" y1="{pad_t + chart_h / 2:.1f}" y2="{pad_t + chart_h / 2:.1f}"/>',
            f'<line class="grid" x1="{pad_l}" x2="{right}" y1="{pad_t + chart_h}" y2="{pad_t + chart_h}"/>',
            f'<line class="grid" x1="{right}" x2="{right}" y1="{pad_t}" y2="{pad_t + chart_h + gap + vol_h}"/>',
            f'<text class="axis" x="{width - pad_r + 70}" y="{pad_t + 4}">{high:.4f}</text>',
            f'<text class="axis" x="{width - pad_r + 70}" y="{pad_t + chart_h / 2 + 4:.1f}">{mid:.4f}</text>',
            f'<text class="axis" x="{width - pad_r + 70}" y="{pad_t + chart_h}">{low:.4f}</text>',
            f'<text class="axis" x="{width - pad_r + 70}" y="{pad_t + chart_h + gap + 12}">Vol</text>',
        ]
        total_sec = max(1, int((now_ns - start_ns) / 1_000_000_000))
        tick_step = 60
        for sec in range(0, total_sec + tick_step, tick_step):
            x = pad_l + sec / total_sec * inner_w
            if x > right:
                break
            tick_ns = start_ns + sec * 1_000_000_000
            label = self._html_time(tick_ns)[-8:-3]
            parts.append(f'<line class="grid" x1="{x}" x2="{x}" y1="{pad_t}" y2="{pad_t + chart_h + gap + vol_h}"/>')
            parts.append(f'<text class="axis" x="{x + 3}" y="{pad_t + chart_h + gap + vol_h + 18}">{label}</text>')
        return "\n".join(parts)

    def _event_start_ns(self, now_ns: int) -> int:
        starts = [int(target["event_start_ns"]) for target in self.targets.values() if "event_start_ns" in target]
        return min(starts) if starts else now_ns

    def _html_time(self, timestamp_ns: int) -> str:
        timestamp = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc)
        return timestamp.astimezone(LOCAL_TZ).strftime("%H:%M:%S")

    def _fmt_price(self, value) -> str:
        if value is None or value == "":
            return "-"
        return f"{float(value):.4f}"

    def _fmt_size(self, value) -> str:
        if value is None or value == "":
            return "-"
        number = float(value)
        if number >= 1_000_000:
            return f"{number / 1_000_000:.2f}m"
        if number >= 1000:
            return f"{number / 1000:.1f}k"
        return f"{number:.2f}"

    def _fmt_age(self, now_ns: int, latest_ns: int) -> str:
        if latest_ns <= 0:
            return "-"
        age_ms = max(0, (now_ns - latest_ns) // 1_000_000)
        if age_ms < 1000:
            return f"{age_ms}ms"
        return f"{age_ms / 1000:.1f}s"

    def _trade_row(self, tick: TradeTick) -> dict[str, object]:
        return {
            **self._target_columns(tick.instrument_id),
            "record_type": "trade",
            "ts_event_ns": int(tick.ts_event),
            "time": self._local_time(tick.ts_event),
            "price": float(str(tick.price)),
            "size": float(str(tick.size)),
            "side": self._side_label(tick.aggressor_side),
            "bid_price": None,
            "bid_size": None,
            "ask_price": None,
            "ask_size": None,
            "source": "live",
            "trade_id": str(getattr(tick, "trade_id", "")),
        }

    def _quote_row(self, tick: QuoteTick) -> dict[str, object]:
        return {
            **self._target_columns(tick.instrument_id),
            "record_type": "quote",
            "ts_event_ns": int(tick.ts_event),
            "time": self._local_time(tick.ts_event),
            "price": None,
            "size": None,
            "side": "",
            "bid_price": float(str(tick.bid_price)),
            "bid_size": float(str(tick.bid_size)),
            "ask_price": float(str(tick.ask_price)),
            "ask_size": float(str(tick.ask_size)),
            "source": "live",
            "trade_id": "",
        }

    def _target_columns(self, instrument_id: InstrumentId) -> dict[str, object]:
        target = self.targets[str(instrument_id)]
        return {
            "event_slug": target["event_slug"],
            "event_title": target["event_title"],
            "market_slug": target["market_slug"],
            "market_question": target["market_question"],
            "label": target["label"],
            "instrument_id": str(instrument_id),
            "condition_id": target["condition_id"],
            "token_id": target["token_id"],
        }

    def _write_status(self, status: str) -> None:
        if not self.config.persist_ticks:
            return
        lines = [
            "# worldcup_price status",
            f"status: {status}",
            f"event: {self._event_title()}",
            f"row_count: {self.row_count}",
            f"trade_subs: {len(self.trade_subs)}",
            f"quote_subs: {len(self.quote_subs)}",
            f"tick_path: {self.tick_path}",
            f"updated: {datetime.now(LOCAL_TZ).isoformat()}",
            "",
        ]
        for instrument_id, target in self.targets.items():
            lines.append(f"- {target['label']}: {instrument_id}")
        self.status_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _event_title(self) -> str:
        if not self.targets:
            return ""
        return str(next(iter(self.targets.values()))["event_title"])

    def _local_time(self, timestamp_ns: int) -> str:
        timestamp = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc)
        return timestamp.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")

    def _side_label(self, side) -> str:
        text = str(side).upper()
        if "BUY" in text:
            return "买"
        if "SELL" in text:
            return "卖"
        return "未知"

    def _output_path(self, value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else ROOT / path

    def on_stop(self) -> None:
        self._unsubscribe_targets()
        self._flush()
        self._write_status("stopped")
        if self.view_server:
            self.view_server.stop()
            self.view_server = None
