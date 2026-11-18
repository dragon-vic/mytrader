from __future__ import annotations

import asyncio
import json
import os
import random
import shlex
import signal
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests
import websockets


LOCAL_TZ = ZoneInfo("Asia/Shanghai")

ASSET = "ANTHROPIC"
BINANCE_SYMBOL = "ANTHROPICUSDT"
OKX_SYMBOL = "ANTHROPIC-USDT-SWAP"
DEPTH = 50

FLUSH_SEC = 30
COMPACT_SEC = 300
METRICS_SEC = 60
MAX_ROWS = 10_000
BACKOFF_INITIAL_SEC = 5.0
BACKOFF_MAX_SEC = 60.0

STRATEGY_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = Path(__file__).resolve().parents[3]
BASE_DIR = STRATEGY_DIR / "collector" / "orderbook-depth-live"
RAW_DIR = BASE_DIR / "depth_raw"
MERGED_DIR = BASE_DIR / "depth_merged"
LOG_PATH = BASE_DIR / "collector.log"
TMUX_SESSION = "preipo_depth_collector"
TMUX_CHILD_ENV = "PREIPO_DEPTH_TMUX_CHILD"

DEPTH_SCHEMA = pa.schema([
    ("ts_local_ns", pa.int64()),
    ("ts_exchange_ms", pa.int64()),
    ("venue", pa.string()),
    ("symbol", pa.string()),
    ("raw_symbol", pa.string()),
    ("depth", pa.int32()),
    ("bids_px", pa.list_(pa.float64())),
    ("bids_sz", pa.list_(pa.float64())),
    ("asks_px", pa.list_(pa.float64())),
    ("asks_sz", pa.list_(pa.float64())),
    ("bid_usdt_20", pa.float64()),
    ("ask_usdt_20", pa.float64()),
    ("bid_usdt_50", pa.float64()),
    ("ask_usdt_50", pa.float64()),
    ("sequence", pa.string()),
])


@dataclass
class StreamStats:
    name: str
    connects: int = 0
    errors: int = 0
    messages: int = 0
    connected_at: float | None = None
    last_msg_at: float | None = None
    backoff_sec: float = BACKOFF_INITIAL_SEC


class Book:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}

    def reset(self, bids: list[list[str]], asks: list[list[str]]) -> None:
        self.bids = levels_to_dict(bids)
        self.asks = levels_to_dict(asks)

    def apply(self, bids: list[list[str]], asks: list[list[str]]) -> None:
        apply_side(self.bids, bids)
        apply_side(self.asks, asks)

    def top(self, depth: int) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        bids = sorted(self.bids.items(), key=lambda item: item[0], reverse=True)[:depth]
        asks = sorted(self.asks.items(), key=lambda item: item[0])[:depth]
        return bids, asks


# 采集 ANTHROPIC 在 Binance/OKX 的 top50 orderbook，按小时写 parquet。
async def main(duration_sec: int) -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MERGED_DIR.mkdir(parents=True, exist_ok=True)
    stop_at = time.monotonic() + duration_sec if duration_sec > 0 else None
    stop = asyncio.Event()
    queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=50_000)
    stats = {
        name: StreamStats(name)
        for name in ("binance_depth", "okx_depth")
    }

    def request_stop(*_args) -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, request_stop)

    write_log(f"start duration_sec={duration_sec} base_dir={BASE_DIR}")
    tasks = [
        asyncio.create_task(collect_binance(queue, stop, stop_at, stats["binance_depth"])),
        asyncio.create_task(collect_okx(queue, stop, stop_at, stats["okx_depth"])),
    ]
    writer = asyncio.create_task(write_chunks(queue, stop, stop_at))
    compactor = asyncio.create_task(compact_loop(stop, stop_at))
    metrics = asyncio.create_task(metrics_loop(queue, stats, stop, stop_at))
    try:
        while not stop.is_set() and not expired(stop_at):
            await asyncio.sleep(1)
        stop.set()
    finally:
        stop.set()
        for task in [*tasks, compactor, metrics]:
            task.cancel()
        await asyncio.gather(*tasks, compactor, metrics, return_exceptions=True)
        try:
            await asyncio.wait_for(writer, timeout=30)
        except asyncio.TimeoutError:
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)
        compact_hours(include_current=True)
        write_log("finished")


# Binance 50 档需要 snapshot + diff stream 重建；partial depth WS 最多只有 20 档。
async def collect_binance(
    queue: asyncio.Queue[dict],
    stop: asyncio.Event,
    stop_at: float | None,
    stats: StreamStats,
) -> None:
    url = f"wss://fstream.binance.com/ws/{BINANCE_SYMBOL.lower()}@depth@100ms"
    while not stop.is_set() and not expired(stop_at):
        book = Book()
        try:
            write_log("binance depth connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                mark_connected(stats)
                write_log("binance depth connected")
                first_raw = await asyncio.wait_for(ws.recv(), timeout=10)
                buffer = [json.loads(first_raw)]
                mark_message(stats)
                snapshot_task = asyncio.create_task(asyncio.to_thread(fetch_binance_snapshot))
                while not snapshot_task.done() and not stop.is_set() and not expired(stop_at):
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=0.1)
                        buffer.append(json.loads(raw))
                        mark_message(stats)
                    except asyncio.TimeoutError:
                        pass
                snapshot = await snapshot_task
                last_id = int(snapshot["lastUpdateId"])
                book.reset(snapshot["bids"], snapshot["asks"])
                ready = False
                for event in buffer:
                    last_id, ready = apply_binance_event(book, event, last_id, ready)
                    if ready:
                        await emit(queue, "BINANCE", BINANCE_SYMBOL, book, event_time_ms(event), str(last_id))
                async for raw in ws:
                    mark_message(stats)
                    event = json.loads(raw)
                    last_id, ready = apply_binance_event(book, event, last_id, ready)
                    if ready:
                        await emit(queue, "BINANCE", BINANCE_SYMBOL, book, event_time_ms(event), str(last_id))
                    if stop.is_set() or expired(stop_at):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats.errors += 1
            write_log(f"binance depth error {type(exc).__name__}: {exc}")
            await sleep_backoff(stats, stop, stop_at)


async def collect_okx(
    queue: asyncio.Queue[dict],
    stop: asyncio.Event,
    stop_at: float | None,
    stats: StreamStats,
) -> None:
    url = "wss://ws.okx.com:8443/ws/v5/public"
    args = [{"channel": "books", "instId": OKX_SYMBOL}]
    while not stop.is_set() and not expired(stop_at):
        book = Book()
        try:
            write_log("okx depth connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                mark_connected(stats)
                write_log("okx depth connected")
                async for raw in ws:
                    mark_message(stats)
                    if raw == "ping":
                        await ws.send("pong")
                        continue
                    payload = json.loads(raw)
                    if "event" in payload:
                        write_log(f"okx event {payload}")
                        continue
                    action = payload.get("action", "")
                    for item in payload.get("data", []):
                        if action == "snapshot":
                            book.reset(item["bids"], item["asks"])
                        else:
                            book.apply(item["bids"], item["asks"])
                        await emit(queue, "OKX", OKX_SYMBOL, book, int(item["ts"]), str(item.get("seqId", "")))
                    if stop.is_set() or expired(stop_at):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats.errors += 1
            write_log(f"okx depth error {type(exc).__name__}: {exc}")
            await sleep_backoff(stats, stop, stop_at)


def fetch_binance_snapshot() -> dict:
    response = requests.get(
        "https://fapi.binance.com/fapi/v1/depth",
        params={"symbol": BINANCE_SYMBOL, "limit": 1000},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def apply_binance_event(book: Book, event: dict, last_id: int, ready: bool) -> tuple[int, bool]:
    first_id = int(event["U"])
    final_id = int(event["u"])
    if final_id <= last_id:
        return last_id, ready
    if not ready:
        prev_id = int(event["pu"]) if "pu" in event else None
        if prev_id == last_id:
            pass
        elif first_id <= last_id <= final_id:
            pass
        elif first_id > last_id:
            raise RuntimeError(f"binance depth sequence gap U={first_id} last={last_id}")
        else:
            return last_id, ready
    elif "pu" in event and int(event["pu"]) != last_id:
        raise RuntimeError(f"binance depth sequence gap pu={event['pu']} last={last_id}")
    book.apply(event.get("b", []), event.get("a", []))
    return final_id, True


async def emit(
    queue: asyncio.Queue[dict],
    venue: str,
    raw_symbol: str,
    book: Book,
    ts_exchange_ms: int,
    sequence: str,
) -> None:
    bids, asks = book.top(DEPTH)
    await queue.put({
        "ts_local_ns": time.time_ns(),
        "ts_exchange_ms": ts_exchange_ms,
        "venue": venue,
        "symbol": ASSET,
        "raw_symbol": raw_symbol,
        "depth": DEPTH,
        "bids_px": [price for price, _size in bids],
        "bids_sz": [size for _price, size in bids],
        "asks_px": [price for price, _size in asks],
        "asks_sz": [size for _price, size in asks],
        "bid_usdt_20": side_notional(bids[:20]),
        "ask_usdt_20": side_notional(asks[:20]),
        "bid_usdt_50": side_notional(bids),
        "ask_usdt_50": side_notional(asks),
        "sequence": sequence,
    })


async def write_chunks(queue: asyncio.Queue[dict], stop: asyncio.Event, stop_at: float | None) -> None:
    rows = []
    last_write = time.monotonic()
    while not stop.is_set() or not queue.empty() or rows:
        try:
            row = await asyncio.wait_for(queue.get(), timeout=1.0)
            rows.append(row)
        except asyncio.TimeoutError:
            pass
        now = time.monotonic()
        if rows and (len(rows) >= MAX_ROWS or now - last_write >= FLUSH_SEC or (stop.is_set() and queue.empty())):
            write_parts(rows, RAW_DIR)
            write_log(f"wrote depth rows={len(rows)} queue={queue.qsize()}")
            rows = []
            last_write = now
        if expired(stop_at):
            stop.set()


def write_parts(rows: list[dict], raw_dir: Path) -> None:
    by_part: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        key = (str(row["symbol"]).upper(), hour_key(int(row["ts_local_ns"])))
        by_part.setdefault(key, []).append(row)
    for (asset, hour), part_rows in by_part.items():
        out_dir = raw_dir / asset / hour
        out_dir.mkdir(parents=True, exist_ok=True)
        data = {name: [row.get(name) for row in part_rows] for name in DEPTH_SCHEMA.names}
        table = pa.Table.from_pydict(data, schema=DEPTH_SCHEMA)
        path = out_dir / f"part-{time.time_ns()}.parquet"
        pq.write_table(table, path, compression="zstd")


async def compact_loop(stop: asyncio.Event, stop_at: float | None) -> None:
    while not stop.is_set() and not expired(stop_at):
        try:
            await asyncio.wait_for(stop.wait(), timeout=COMPACT_SEC)
        except asyncio.TimeoutError:
            pass
        if stop.is_set() or expired(stop_at):
            break
        try:
            compact_hours(include_current=False)
        except Exception as exc:
            write_log(f"compact error {type(exc).__name__}: {exc}")


def compact_hours(include_current: bool) -> None:
    current = current_hour_key()
    for asset_dir in sorted(path for path in RAW_DIR.glob("*") if path.is_dir()):
        asset = asset_dir.name.upper()
        for hour_dir in sorted(path for path in asset_dir.glob("*") if path.is_dir()):
            key = hour_dir.name
            if not include_current and key >= current:
                continue
            files = sorted(hour_dir.glob("*.parquet"))
            if not files:
                hour_dir.rmdir()
                continue
            out_dir = MERGED_DIR / asset
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"depth-{key}.parquet"
            inputs = [str(path) for path in files]
            if out_path.exists():
                inputs.insert(0, str(out_path))
            table = ds.dataset(inputs, format="parquet").to_table(columns=DEPTH_SCHEMA.names)
            table = table.sort_by([("ts_local_ns", "ascending"), ("venue", "ascending"), ("sequence", "ascending")])
            tmp_path = out_path.with_suffix(".parquet.tmp")
            pq.write_table(table, tmp_path, compression="zstd", row_group_size=100_000)
            tmp_path.replace(out_path)
            for path in files:
                path.unlink()
            hour_dir.rmdir()
            write_log(f"compacted depth asset={asset} hour={key} files={len(files)} rows={table.num_rows} out={out_path.name}")


async def metrics_loop(
    queue: asyncio.Queue[dict],
    stats: dict[str, StreamStats],
    stop: asyncio.Event,
    stop_at: float | None,
) -> None:
    while not stop.is_set() and not expired(stop_at):
        try:
            await asyncio.wait_for(stop.wait(), timeout=METRICS_SEC)
        except asyncio.TimeoutError:
            pass
        if stop.is_set() or expired(stop_at):
            break
        now = time.monotonic()
        parts = [
            f"rss_mb={current_rss_mb()}",
            f"queue={queue.qsize()}",
            f"raw_files={raw_file_count(RAW_DIR)}",
        ]
        for item in stats.values():
            last_age = int(now - item.last_msg_at) if item.last_msg_at is not None else -1
            connected_age = int(now - item.connected_at) if item.connected_at is not None else -1
            parts.append(
                f"{item.name}:connects={item.connects},errors={item.errors},msgs={item.messages},"
                f"last_age={last_age}s,connected_age={connected_age}s,backoff={item.backoff_sec:.0f}s"
            )
        write_log("metrics " + " ".join(parts))


def levels_to_dict(levels: list[list[str]]) -> dict[float, float]:
    return {float(price): float(size) for price, size, *_rest in levels if float(size) > 0}


def apply_side(side: dict[float, float], levels: list[list[str]]) -> None:
    for price_text, size_text, *_rest in levels:
        price = float(price_text)
        size = float(size_text)
        if size <= 0:
            side.pop(price, None)
        else:
            side[price] = size


def side_notional(levels: list[tuple[float, float]]) -> float:
    return sum(price * size for price, size in levels)


def event_time_ms(event: dict) -> int:
    return int(event.get("E") or event.get("T") or 0)


def hour_key(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, LOCAL_TZ).strftime("%Y%m%d%H")


def current_hour_key() -> str:
    return datetime.now(LOCAL_TZ).strftime("%Y%m%d%H")


def expired(stop_at: float | None) -> bool:
    return stop_at is not None and time.monotonic() >= stop_at


def mark_connected(stats: StreamStats) -> None:
    now = time.monotonic()
    stats.connects += 1
    stats.connected_at = now


def mark_message(stats: StreamStats) -> None:
    now = time.monotonic()
    stats.messages += 1
    stats.last_msg_at = now
    stats.backoff_sec = BACKOFF_INITIAL_SEC


async def sleep_backoff(stats: StreamStats, stop: asyncio.Event, stop_at: float | None) -> None:
    delay = stats.backoff_sec + random.uniform(0, min(1.0, stats.backoff_sec * 0.2))
    write_log(f"{stats.name} reconnect backoff={delay:.1f}s")
    stats.backoff_sec = min(stats.backoff_sec * 2, BACKOFF_MAX_SEC)
    end = time.monotonic() + delay
    while not stop.is_set() and time.monotonic() < end and not expired(stop_at):
        await asyncio.sleep(0.2)


def current_rss_mb() -> int:
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) // 1024
    except OSError:
        return -1
    return -1


def raw_file_count(raw_dir: Path) -> int:
    return sum(1 for path in raw_dir.glob("*/*/*.parquet") if path.is_file())


def write_log(text: str) -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {text}"
    print(line, flush=True)
    with LOG_PATH.open("a", encoding="utf-8") as file:
        file.write(line + "\n")


def launch_tmux_if_needed(args: list[str]) -> bool:
    if os.environ.get(TMUX_CHILD_ENV) == "1" or os.environ.get("TMUX"):
        return False

    tmux = shutil.which("tmux")
    if tmux is None:
        print("tmux not found; running collector in foreground.", flush=True)
        return False

    if subprocess.run([tmux, "has-session", "-t", TMUX_SESSION], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        print(f"tmux session already exists: {TMUX_SESSION}", flush=True)
        print(f"attach: tmux attach -t {TMUX_SESSION}", flush=True)
        return True

    script = Path(__file__).resolve()
    run_args = args if args else ["0"]
    command = " ".join(shlex.quote(part) for part in [sys.executable, "-u", str(script), *run_args])
    command = f"{TMUX_CHILD_ENV}=1 exec {command}"
    subprocess.run([tmux, "new-session", "-d", "-s", TMUX_SESSION, "-c", str(PROJECT_DIR), command], check=True)
    print(f"started tmux session: {TMUX_SESSION}", flush=True)
    print(f"attach: tmux attach -t {TMUX_SESSION}", flush=True)
    return True


if __name__ == "__main__":
    # 0 表示持续采集；临时测试可传秒数。
    if launch_tmux_if_needed(sys.argv[1:]):
        raise SystemExit(0)
    seconds = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    asyncio.run(main(seconds))
