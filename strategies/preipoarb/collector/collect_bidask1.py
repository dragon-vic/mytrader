from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import websockets


LOCAL_TZ = ZoneInfo("Asia/Shanghai")

BINANCE_SYMBOLS = ("OPENAIUSDT", "ANTHROPICUSDT")
OKX_SYMBOLS = ("OPENAI-USDT-SWAP", "ANTHROPIC-USDT-SWAP")

QUOTE_FLUSH_SEC = 30
# Trade tick 量明显小于 quote，单独放慢落盘节奏，减少小 parquet 文件数量。
TRADE_FLUSH_SEC = 900
COMPACT_SEC = 300
MAX_ROWS = 50_000

STRATEGY_DIR = Path(__file__).resolve().parents[1]
BASE_DIR = STRATEGY_DIR / "research" / "bidask1-live"
# 策略 warmup 和本地合并脚本已依赖 raw/merged quote 路径，trade tick 只能放旁边新目录。
QUOTE_RAW_DIR = BASE_DIR / "raw"
QUOTE_MERGED_DIR = BASE_DIR / "merged"
TRADE_RAW_DIR = BASE_DIR / "trade_raw"
TRADE_MERGED_DIR = BASE_DIR / "trade_merged"
LOG_PATH = BASE_DIR / "collector.log"

QUOTE_SCHEMA = pa.schema([
    ("ts_local_ns", pa.int64()),
    ("ts_exchange_ms", pa.int64()),
    ("venue", pa.string()),
    ("symbol", pa.string()),
    ("raw_symbol", pa.string()),
    ("bid", pa.float64()),
    ("ask", pa.float64()),
    ("bid_size", pa.float64()),
    ("ask_size", pa.float64()),
    ("sequence", pa.string()),
])

TRADE_SCHEMA = pa.schema([
    ("ts_local_ns", pa.int64()),
    ("ts_exchange_ms", pa.int64()),
    ("venue", pa.string()),
    ("symbol", pa.string()),
    ("raw_symbol", pa.string()),
    ("price", pa.float64()),
    ("size", pa.float64()),
    ("side", pa.string()),
    ("trade_id", pa.string()),
    ("sequence", pa.string()),
])


# 长期采集四个 preipo 标的的 bid/ask1 和成交 tick，并按小时合并 parquet。
async def main(duration_sec: int) -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    QUOTE_RAW_DIR.mkdir(parents=True, exist_ok=True)
    QUOTE_MERGED_DIR.mkdir(parents=True, exist_ok=True)
    TRADE_RAW_DIR.mkdir(parents=True, exist_ok=True)
    TRADE_MERGED_DIR.mkdir(parents=True, exist_ok=True)
    stop_at = time.monotonic() + duration_sec if duration_sec > 0 else None
    stop = asyncio.Event()
    quote_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=100_000)
    trade_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=100_000)

    def request_stop(*_args) -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, request_stop)

    write_log(f"start duration_sec={duration_sec} base_dir={BASE_DIR}")
    collectors = [
        asyncio.create_task(collect_binance_quotes(quote_queue, stop, stop_at)),
        asyncio.create_task(collect_okx_quotes(quote_queue, stop, stop_at)),
        asyncio.create_task(collect_binance_trades(trade_queue, stop, stop_at)),
        asyncio.create_task(collect_okx_trades(trade_queue, stop, stop_at)),
    ]
    writers = [
        asyncio.create_task(write_chunks("quote", quote_queue, QUOTE_RAW_DIR, QUOTE_SCHEMA, QUOTE_FLUSH_SEC, stop, stop_at)),
        asyncio.create_task(write_chunks("trade", trade_queue, TRADE_RAW_DIR, TRADE_SCHEMA, TRADE_FLUSH_SEC, stop, stop_at)),
    ]
    compactor = asyncio.create_task(compact_loop(stop, stop_at))
    try:
        while not stop.is_set() and not expired(stop_at):
            await asyncio.sleep(1)
        stop.set()
    finally:
        stop.set()
        for task in [*collectors, compactor]:
            task.cancel()
        await asyncio.gather(*collectors, compactor, return_exceptions=True)
        for writer in writers:
            try:
                await asyncio.wait_for(writer, timeout=30)
            except asyncio.TimeoutError:
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)
        compact_hours(include_current=True)
        write_log("finished")


# Binance futures bookTicker 是变更推送，URL 里一次订阅两个标的。
async def collect_binance_quotes(queue: asyncio.Queue[dict], stop: asyncio.Event, stop_at: float | None) -> None:
    streams = "/".join(f"{symbol.lower()}@bookTicker" for symbol in BINANCE_SYMBOLS)
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    while not stop.is_set() and not expired(stop_at):
        try:
            write_log("binance quote connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                write_log("binance quote connected")
                async for raw in ws:
                    data = json.loads(raw)["data"]
                    await queue.put({
                        "ts_local_ns": time.time_ns(),
                        "ts_exchange_ms": int(data.get("E") or data.get("T") or 0),
                        "venue": "BINANCE",
                        "symbol": asset_symbol(data["s"]),
                        "raw_symbol": data["s"],
                        "bid": float(data["b"]),
                        "ask": float(data["a"]),
                        "bid_size": float(data["B"]),
                        "ask_size": float(data["A"]),
                        "sequence": str(data.get("u", "")),
                    })
                    if stop.is_set() or expired(stop_at):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            write_log(f"binance quote error {type(exc).__name__}: {exc}")
            await sleep_backoff(stop, stop_at)


# Binance futures trade tick 量小于 quote burst，和 quote 分队列写盘避免互相阻塞。
async def collect_binance_trades(queue: asyncio.Queue[dict], stop: asyncio.Event, stop_at: float | None) -> None:
    streams = "/".join(f"{symbol.lower()}@trade" for symbol in BINANCE_SYMBOLS)
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    while not stop.is_set() and not expired(stop_at):
        try:
            write_log("binance trade connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                write_log("binance trade connected")
                async for raw in ws:
                    data = json.loads(raw)["data"]
                    await queue.put({
                        "ts_local_ns": time.time_ns(),
                        "ts_exchange_ms": int(data.get("T") or data.get("E") or 0),
                        "venue": "BINANCE",
                        "symbol": asset_symbol(data["s"]),
                        "raw_symbol": data["s"],
                        "price": float(data["p"]),
                        "size": float(data["q"]),
                        "side": "SELL" if bool(data.get("m")) else "BUY",
                        "trade_id": str(data.get("t", "")),
                        "sequence": str(data.get("t", "")),
                    })
                    if stop.is_set() or expired(stop_at):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            write_log(f"binance trade error {type(exc).__name__}: {exc}")
            await sleep_backoff(stop, stop_at)


# OKX bbo-tbt 推送最优一档，公共频道不需要鉴权。
async def collect_okx_quotes(queue: asyncio.Queue[dict], stop: asyncio.Event, stop_at: float | None) -> None:
    url = "wss://ws.okx.com:8443/ws/v5/public"
    args = [{"channel": "bbo-tbt", "instId": symbol} for symbol in OKX_SYMBOLS]
    while not stop.is_set() and not expired(stop_at):
        try:
            write_log("okx quote connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                write_log("okx quote connected")
                async for raw in ws:
                    if raw == "ping":
                        await ws.send("pong")
                        continue
                    payload = json.loads(raw)
                    if "event" in payload:
                        write_log(f"okx event {payload}")
                        continue
                    raw_symbol = payload["arg"]["instId"]
                    for item in payload.get("data", []):
                        bid = item["bids"][0]
                        ask = item["asks"][0]
                        await queue.put({
                            "ts_local_ns": time.time_ns(),
                            "ts_exchange_ms": int(item["ts"]),
                            "venue": "OKX",
                            "symbol": raw_symbol.replace("-USDT-SWAP", ""),
                            "raw_symbol": raw_symbol,
                            "bid": float(bid[0]),
                            "ask": float(ask[0]),
                            "bid_size": float(bid[1]),
                            "ask_size": float(ask[1]),
                            "sequence": "",
                        })
                    if stop.is_set() or expired(stop_at):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            write_log(f"okx quote error {type(exc).__name__}: {exc}")
            await sleep_backoff(stop, stop_at)


# OKX trades 频道推送逐笔成交，和 Binance trade 统一成 taker side。
async def collect_okx_trades(queue: asyncio.Queue[dict], stop: asyncio.Event, stop_at: float | None) -> None:
    url = "wss://ws.okx.com:8443/ws/v5/public"
    args = [{"channel": "trades", "instId": symbol} for symbol in OKX_SYMBOLS]
    while not stop.is_set() and not expired(stop_at):
        try:
            write_log("okx trade connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                write_log("okx trade connected")
                async for raw in ws:
                    if raw == "ping":
                        await ws.send("pong")
                        continue
                    payload = json.loads(raw)
                    if "event" in payload:
                        write_log(f"okx event {payload}")
                        continue
                    raw_symbol = payload["arg"]["instId"]
                    for item in payload.get("data", []):
                        trade_id = str(item.get("tradeId", ""))
                        await queue.put({
                            "ts_local_ns": time.time_ns(),
                            "ts_exchange_ms": int(item["ts"]),
                            "venue": "OKX",
                            "symbol": raw_symbol.replace("-USDT-SWAP", ""),
                            "raw_symbol": raw_symbol,
                            "price": float(item["px"]),
                            "size": float(item["sz"]),
                            "side": str(item.get("side", "")).upper(),
                            "trade_id": trade_id,
                            "sequence": trade_id,
                        })
                    if stop.is_set() or expired(stop_at):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            write_log(f"okx trade error {type(exc).__name__}: {exc}")
            await sleep_backoff(stop, stop_at)


# 小批量写 parquet，按北京时间小时分桶，方便后台合并。
async def write_chunks(
    label: str,
    queue: asyncio.Queue[dict],
    raw_dir: Path,
    schema: pa.Schema,
    flush_sec: int,
    stop: asyncio.Event,
    stop_at: float | None,
) -> None:
    rows = []
    last_write = time.monotonic()
    while not stop.is_set() or not queue.empty() or rows:
        try:
            row = await asyncio.wait_for(queue.get(), timeout=1.0)
            rows.append(row)
        except asyncio.TimeoutError:
            pass
        now = time.monotonic()
        if rows and (len(rows) >= MAX_ROWS or now - last_write >= flush_sec or (stop.is_set() and queue.empty())):
            write_parts(rows, raw_dir, schema)
            write_log(f"wrote {label} rows={len(rows)} queue={queue.qsize()}")
            rows = []
            last_write = now
        if expired(stop_at):
            stop.set()


def write_parts(rows: list[dict], raw_dir: Path, schema: pa.Schema) -> None:
    by_hour: dict[str, list[dict]] = {}
    for row in rows:
        by_hour.setdefault(hour_key(int(row["ts_local_ns"])), []).append(row)
    for key, hour_rows in by_hour.items():
        out_dir = raw_dir / key
        out_dir.mkdir(parents=True, exist_ok=True)
        data = {name: [row.get(name) for row in hour_rows] for name in schema.names}
        table = pa.Table.from_pydict(data, schema=schema)
        path = out_dir / f"part-{time.time_ns()}.parquet"
        pq.write_table(table, path, compression="zstd")


# 周期性合并已经结束的小时，降低小文件数量和拉取成本。
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
    compact_one(
        "quote",
        QUOTE_RAW_DIR,
        QUOTE_MERGED_DIR,
        QUOTE_SCHEMA,
        "bidask1",
        [("ts_local_ns", "ascending"), ("venue", "ascending"), ("symbol", "ascending")],
        include_current,
    )
    compact_one(
        "trade",
        TRADE_RAW_DIR,
        TRADE_MERGED_DIR,
        TRADE_SCHEMA,
        "trades",
        [("ts_local_ns", "ascending"), ("venue", "ascending"), ("symbol", "ascending"), ("trade_id", "ascending")],
        include_current,
    )


def compact_one(
    label: str,
    raw_dir: Path,
    merged_dir: Path,
    schema: pa.Schema,
    prefix: str,
    sort_by: list[tuple[str, str]],
    include_current: bool,
) -> None:
    current = current_hour_key()
    for hour_dir in sorted(path for path in raw_dir.glob("*") if path.is_dir()):
        key = hour_dir.name
        if not include_current and key >= current:
            continue
        files = sorted(hour_dir.glob("*.parquet"))
        if not files:
            hour_dir.rmdir()
            continue
        out_path = merged_dir / f"{prefix}-{key}.parquet"
        inputs = [str(path) for path in files]
        if out_path.exists():
            inputs.insert(0, str(out_path))
        table = ds.dataset(inputs, format="parquet").to_table(columns=schema.names)
        table = table.sort_by(sort_by)
        tmp_path = out_path.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp_path, compression="zstd", row_group_size=100_000)
        tmp_path.replace(out_path)
        for path in files:
            path.unlink()
        hour_dir.rmdir()
        write_log(f"compacted {label} hour={key} files={len(files)} rows={table.num_rows} out={out_path.name}")


def asset_symbol(raw_symbol: str) -> str:
    return raw_symbol.replace("USDT", "")


def hour_key(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, LOCAL_TZ).strftime("%Y%m%d%H")


def current_hour_key() -> str:
    return datetime.now(LOCAL_TZ).strftime("%Y%m%d%H")


def expired(stop_at: float | None) -> bool:
    return stop_at is not None and time.monotonic() >= stop_at


async def sleep_backoff(stop: asyncio.Event, stop_at: float | None) -> None:
    end = time.monotonic() + 5
    while not stop.is_set() and time.monotonic() < end and not expired(stop_at):
        await asyncio.sleep(0.2)


def write_log(text: str) -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {text}"
    print(line, flush=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


if __name__ == "__main__":
    # 0 表示持续采集；临时测试可传秒数，例如：
    # python strategies/preipoarb/collector/collect_bidask1.py 120
    seconds = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    asyncio.run(main(seconds))
