# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import csv
import datetime as dt
import json
import statistics
import time
from pathlib import Path
from typing import Any

import aiohttp
import websockets


# =========================
# Config
# =========================

REST_URL = "https://fapi.binance.com/fapi/v1/time"
WS_URL = "wss://ws-fapi.binance.com/ws-fapi/v1?returnRateLimits=false"

REST_COUNT = 500
WS_COUNT = 500
INTERVAL_SECONDS = 0.05

# If True, the script will also run a continuous REST latency watch.
# This is useful around funding time. If you only want a quick test, keep it False.
RUN_WATCH = False
WATCH_SECONDS = 180
WATCH_INTERVAL_SECONDS = 0.1

OUTPUT_DIR = Path("latency_results")


# =========================
# Helpers
# =========================

def now_str() -> str:
    return dt.datetime.now().isoformat(timespec="milliseconds")


def percentile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")

    xs = sorted(values)
    idx = int((len(xs) - 1) * q)
    return xs[idx]


def calc_stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "count": 0,
            "avg": float("nan"),
            "p50": float("nan"),
            "p90": float("nan"),
            "p95": float("nan"),
            "p99": float("nan"),
            "max": float("nan"),
            "over_200ms": 0,
            "over_500ms": 0,
            "over_1000ms": 0,
        }

    return {
        "count": len(values),
        "avg": statistics.mean(values),
        "p50": percentile(values, 0.50),
        "p90": percentile(values, 0.90),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values),
        "over_200ms": sum(1 for x in values if x >= 200),
        "over_500ms": sum(1 for x in values if x >= 500),
        "over_1000ms": sum(1 for x in values if x >= 1000),
    }


def print_stats(title: str, values: list[float]) -> None:
    s = calc_stats(values)

    print()
    print("=" * 60)
    print(title)
    print("=" * 60)
    print(f"count       : {int(s['count'])}")
    print(f"avg         : {s['avg']:.2f} ms")
    print(f"p50         : {s['p50']:.2f} ms")
    print(f"p90         : {s['p90']:.2f} ms")
    print(f"p95         : {s['p95']:.2f} ms")
    print(f"p99         : {s['p99']:.2f} ms")
    print(f"max         : {s['max']:.2f} ms")
    print(f">=200ms     : {int(s['over_200ms'])}")
    print(f">=500ms     : {int(s['over_500ms'])}")
    print(f">=1000ms    : {int(s['over_1000ms'])}")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path: Path, rest_values: list[float], ws_values: list[float], watch_values: list[float]) -> None:
    lines: list[str] = []

    def add_block(name: str, values: list[float]) -> None:
        s = calc_stats(values)
        lines.append(f"[{name}]")
        lines.append(f"count={int(s['count'])}")
        lines.append(f"avg_ms={s['avg']:.2f}")
        lines.append(f"p50_ms={s['p50']:.2f}")
        lines.append(f"p90_ms={s['p90']:.2f}")
        lines.append(f"p95_ms={s['p95']:.2f}")
        lines.append(f"p99_ms={s['p99']:.2f}")
        lines.append(f"max_ms={s['max']:.2f}")
        lines.append(f"over_200ms={int(s['over_200ms'])}")
        lines.append(f"over_500ms={int(s['over_500ms'])}")
        lines.append(f"over_1000ms={int(s['over_1000ms'])}")
        lines.append("")

    add_block("REST_KEEP_ALIVE", rest_values)
    add_block("WS_API", ws_values)

    if watch_values:
        add_block("REST_WATCH", watch_values)

    lines.append("[INTERPRETATION]")
    lines.append(make_interpretation(rest_values, ws_values, watch_values))

    path.write_text("\n".join(lines), encoding="utf-8")


def make_interpretation(rest_values: list[float], ws_values: list[float], watch_values: list[float]) -> str:
    rest = calc_stats(rest_values)
    ws = calc_stats(ws_values)
    watch = calc_stats(watch_values) if watch_values else None

    parts: list[str] = []

    if rest["p99"] < 200 and ws["p99"] < 200:
        parts.append("REST and WS baseline p99 are both below 200 ms. The VPS network path looks acceptable in normal conditions.")
    elif rest["p99"] >= 500 and ws["p99"] < 200:
        parts.append("REST has high tail latency while WS is stable. WS order placement is likely worth testing first.")
    elif rest["p99"] < 200 and ws["p99"] >= 500:
        parts.append("WS has high tail latency while REST is stable. The WS path or endpoint may be worse from this VPS.")
    elif rest["p99"] >= 500 and ws["p99"] >= 500:
        parts.append("Both REST and WS have high tail latency. This VPS route is likely unstable or congested.")
    else:
        parts.append("Baseline latency is not extreme, but tail latency is not very clean. Compare with another region before changing trading logic.")

    if rest["over_1000ms"] > 0:
        parts.append("REST baseline contains >=1000 ms spikes. This is bad for funding-time trading.")
    if ws["over_1000ms"] > 0:
        parts.append("WS baseline contains >=1000 ms spikes. WS may not solve the delay from this VPS.")

    if watch:
        if watch["over_1000ms"] > 0 or watch["p99"] >= 500:
            parts.append("The continuous watch contains large spikes. If this was run near funding time, the VPS-to-Binance path or Binance API edge is unstable in that window.")
        else:
            parts.append("The continuous watch has no large spikes. If real orders still take seconds, the delay is more likely inside Binance order processing rather than the basic API path.")

    return " ".join(parts)


# =========================
# Tests
# =========================

async def measure_rest() -> tuple[list[dict[str, Any]], list[float]]:
    rows: list[dict[str, Any]] = []
    values: list[float] = []

    timeout = aiohttp.ClientTimeout(total=5)
    connector = aiohttp.TCPConnector(
        limit=1,
        ttl_dns_cache=300,
        keepalive_timeout=60,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for i in range(REST_COUNT):
            t0 = time.perf_counter_ns()
            ok = 1
            status = None
            error = ""

            try:
                async with session.get(REST_URL) as resp:
                    status = resp.status
                    await resp.text()
            except Exception as e:
                ok = 0
                error = repr(e)

            latency_ms = (time.perf_counter_ns() - t0) / 1_000_000

            rows.append({
                "i": i,
                "local_time": now_str(),
                "ok": ok,
                "status": status,
                "latency_ms": round(latency_ms, 3),
                "error": error,
            })

            if ok:
                values.append(latency_ms)

            print(f"REST {i + 1}/{REST_COUNT} latency_ms={latency_ms:.2f} ok={ok} status={status}", flush=True)
            await asyncio.sleep(INTERVAL_SECONDS)

    return rows, values


async def measure_ws() -> tuple[list[dict[str, Any]], list[float]]:
    rows: list[dict[str, Any]] = []
    values: list[float] = []

    connect_t0 = time.perf_counter_ns()

    async with websockets.connect(
        WS_URL,
        ping_interval=None,
        open_timeout=10,
        close_timeout=3,
        max_queue=None,
    ) as ws:
        connect_ms = (time.perf_counter_ns() - connect_t0) / 1_000_000
        print(f"WS connected. connect_ms={connect_ms:.2f}", flush=True)

        for i in range(WS_COUNT):
            req = {
                "id": str(i),
                "method": "time",
                "params": {},
            }

            t0 = time.perf_counter_ns()
            ok = 1
            status = None
            error = ""

            try:
                await ws.send(json.dumps(req, separators=(",", ":")))
                raw = await ws.recv()
                msg = json.loads(raw)
                status = msg.get("status")
                if status != 200:
                    ok = 0
                    error = raw[:500]
            except Exception as e:
                ok = 0
                error = repr(e)

            latency_ms = (time.perf_counter_ns() - t0) / 1_000_000

            rows.append({
                "i": i,
                "local_time": now_str(),
                "ok": ok,
                "status": status,
                "latency_ms": round(latency_ms, 3),
                "error": error,
            })

            if ok:
                values.append(latency_ms)

            print(f"WS   {i + 1}/{WS_COUNT} latency_ms={latency_ms:.2f} ok={ok} status={status}", flush=True)
            await asyncio.sleep(INTERVAL_SECONDS)

    return rows, values


async def watch_rest() -> tuple[list[dict[str, Any]], list[float]]:
    rows: list[dict[str, Any]] = []
    values: list[float] = []

    timeout = aiohttp.ClientTimeout(total=5)
    connector = aiohttp.TCPConnector(
        limit=1,
        ttl_dns_cache=300,
        keepalive_timeout=60,
        enable_cleanup_closed=True,
    )

    end_time = time.time() + WATCH_SECONDS

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        i = 0

        while time.time() < end_time:
            t0 = time.perf_counter_ns()
            ok = 1
            status = None
            error = ""

            try:
                async with session.get(REST_URL) as resp:
                    status = resp.status
                    await resp.text()
            except Exception as e:
                ok = 0
                error = repr(e)

            latency_ms = (time.perf_counter_ns() - t0) / 1_000_000

            rows.append({
                "i": i,
                "local_time": now_str(),
                "ok": ok,
                "status": status,
                "latency_ms": round(latency_ms, 3),
                "error": error,
            })

            if ok:
                values.append(latency_ms)

            print(f"WATCH {i} latency_ms={latency_ms:.2f} ok={ok} status={status}", flush=True)

            i += 1
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    return rows, values


# =========================
# Main
# =========================

async def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Start Binance latency test")
    print(f"REST_URL={REST_URL}")
    print(f"WS_URL={WS_URL}")
    print(f"REST_COUNT={REST_COUNT}, WS_COUNT={WS_COUNT}, INTERVAL_SECONDS={INTERVAL_SECONDS}")
    print(f"RUN_WATCH={RUN_WATCH}, WATCH_SECONDS={WATCH_SECONDS}")
    print()

    rest_rows, rest_values = await measure_rest()
    write_csv(OUTPUT_DIR / "rest_latency.csv", rest_rows)
    print_stats("REST keep-alive latency", rest_values)

    ws_rows, ws_values = await measure_ws()
    write_csv(OUTPUT_DIR / "ws_latency.csv", ws_rows)
    print_stats("WS API latency", ws_values)

    watch_values: list[float] = []

    if RUN_WATCH:
        watch_rows, watch_values = await watch_rest()
        write_csv(OUTPUT_DIR / "funding_rest_watch.csv", watch_rows)
        print_stats("REST continuous watch latency", watch_values)

    write_summary(OUTPUT_DIR / "summary.txt", rest_values, ws_values, watch_values)

    print()
    print("=" * 60)
    print("Auto analysis")
    print("=" * 60)
    print(make_interpretation(rest_values, ws_values, watch_values))
    print()
    print(f"Files saved under: {OUTPUT_DIR.resolve()}")
    print("Generated files:")
    print("  rest_latency.csv")
    print("  ws_latency.csv")
    if RUN_WATCH:
        print("  funding_rest_watch.csv")
    print("  summary.txt")


if __name__ == "__main__":
    asyncio.run(main())