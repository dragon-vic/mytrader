from __future__ import annotations

import json
import random
import time
from datetime import datetime
from datetime import timezone
from pathlib import Path

import pandas as pd
import requests


API_URL = "https://fapi.binance.com"
THRESHOLD_BPS = 30
WINDOW_BEFORE_MS = 3000
WINDOW_AFTER_MS = 3000
BATCH_SIZE = 40
BATCH_PAUSE_SEC = 120
BATCH_PAUSE_JITTER_SEC = 30
EVENT_SLEEP_SEC = 3.4
EVENT_SLEEP_JITTER_SEC = 0.8
PAGE_SLEEP_SEC = 1.2
PAGE_SLEEP_JITTER_SEC = 0.5
REQUEST_TIMEOUT_SEC = 20

FUNDING_PATH = Path("data/funding/all_6m/funding_all_6m.parquet")
OUT_DIR = Path("data/ticks/funding_6m")
RAW_DIR = OUT_DIR / "raw_agg_trades_30bps"
COMBINED_PATH = OUT_DIR / "events_abs30_tminus3_tplus3_agg_trades.parquet"
META_PATH = OUT_DIR / "fetch_meta_abs30.json"


# 先写临时文件，再替换正式文件，避免中断时留下半个 parquet。
def write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    temp_path = path.with_suffix(".tmp.parquet")
    frame.to_parquet(temp_path, index=False)
    temp_path.replace(path)


# 读取 funding 事件，并只保留本轮要补的阈值。
def load_events() -> pd.DataFrame:
    events = pd.read_parquet(FUNDING_PATH)
    events["funding_time"] = events["funding_time"].astype("int64")
    events["funding_utc"] = pd.to_datetime(events["funding_utc"], utc=True)
    events = events[events["abs_rate_bps"] >= THRESHOLD_BPS].copy()
    events["event_key"] = events["symbol"] + "_" + events["funding_time"].astype(str)
    return events.sort_values(
        ["funding_time", "abs_rate_bps", "symbol"],
        ascending=[True, False, True],
    )


# 读已有 tick，并回收上次中断时已写 raw、还没合并的事件。
def load_existing() -> tuple[pd.DataFrame, set[str]]:
    if COMBINED_PATH.exists():
        ticks = pd.read_parquet(COMBINED_PATH)
        ticks["event_key"] = ticks["event_key"].astype(str)
    else:
        ticks = pd.DataFrame()

    done = set(ticks["event_key"].unique()) if not ticks.empty else set()
    recovered: list[pd.DataFrame] = []
    for path in RAW_DIR.rglob("*.parquet"):
        frame = pd.read_parquet(path)
        frame["event_key"] = frame["event_key"].astype(str)
        key = frame["event_key"].iloc[0]
        if key not in done:
            recovered.append(frame)
            done.add(key)

    if not recovered:
        return ticks, done

    frames = [ticks] if not ticks.empty else []
    frames.extend(recovered)
    merged = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(["event_key", "agg_trade_id"])
        .sort_values(["funding_time", "symbol", "timestamp_ms", "agg_trade_id"])
    )
    write_parquet_atomic(merged, COMBINED_PATH)
    print(f"recovered raw-only events={len(recovered)}", flush=True)
    return merged, set(merged["event_key"].unique())


# 一个事件可能超过 1000 笔 aggTrade，分页拉完完整窗口。
def fetch_window(
    session: requests.Session,
    symbol: str,
    funding_time: int,
) -> list[dict]:
    rows: list[dict] = []
    cursor = funding_time - WINDOW_BEFORE_MS
    end_ms = funding_time + WINDOW_AFTER_MS
    while cursor <= end_ms:
        response = session.get(
            f"{API_URL}/fapi/v1/aggTrades",
            params={
                "symbol": symbol,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            },
            timeout=REQUEST_TIMEOUT_SEC,
        )
        if response.status_code in (418, 429):
            raise RuntimeError(f"{response.status_code} {response.text[:160]}")
        response.raise_for_status()
        page = response.json()
        if not page:
            break
        rows.extend(page)
        last_ts = max(int(item["T"]) for item in page)
        if last_ts < cursor or len(page) < 1000:
            break
        cursor = last_ts + 1
        time.sleep(PAGE_SLEEP_SEC + random.random() * PAGE_SLEEP_JITTER_SEC)
    return rows


# 统一成和现有回测 tick 文件一致的列。
def normalize_rows(row, rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows).rename(
        columns={
            "a": "agg_trade_id",
            "p": "price",
            "q": "quantity",
            "T": "timestamp_ms",
            "m": "buyer_maker",
        },
    )
    frame["timestamp_ms"] = frame["timestamp_ms"].astype("int64")
    frame["timestamp"] = pd.to_datetime(frame["timestamp_ms"], unit="ms", utc=True)
    frame["symbol"] = row.symbol
    frame["funding_time"] = int(row.funding_time)
    frame["funding_utc"] = row.funding_utc.isoformat()
    frame["rate_bps"] = float(row.rate_bps)
    frame["abs_rate_bps"] = float(row.abs_rate_bps)
    frame["side"] = "SELL" if row.rate_bps > 0 else "BUY"
    frame["event_key"] = row.event_key
    frame["price"] = frame["price"].astype(float)
    frame["quantity"] = frame["quantity"].astype(float)
    frame["trade_id"] = frame["agg_trade_id"].astype(str)
    columns = [
        "event_key",
        "timestamp",
        "timestamp_ms",
        "symbol",
        "funding_time",
        "funding_utc",
        "rate_bps",
        "abs_rate_bps",
        "side",
        "agg_trade_id",
        "price",
        "quantity",
        "buyer_maker",
        "trade_id",
    ]
    return frame[columns].drop_duplicates(["event_key", "agg_trade_id"])


# 每次只跑一小批，限流时立刻停，避免把 429 撞成 418。
def run_batch(events: pd.DataFrame) -> tuple[int, bool]:
    existing, done = load_existing()
    pending = events[~events["event_key"].isin(done)].head(BATCH_SIZE)
    print(
        f"threshold={THRESHOLD_BPS}bps batch={len(pending)} "
        f"done_before={len(done)} target={len(events)}",
        flush=True,
    )
    if pending.empty:
        return 0, False

    session = requests.Session()
    new_frames: list[pd.DataFrame] = []
    errors: list[dict] = []
    stopped_on_limit = False

    for index, row in enumerate(pending.itertuples(index=False), 1):
        try:
            rows = fetch_window(session, row.symbol, int(row.funding_time))
        except Exception as exc:
            text = str(exc)
            errors.append(
                {
                    "event_key": row.event_key,
                    "symbol": row.symbol,
                    "funding_time": int(row.funding_time),
                    "error": text,
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                },
            )
            print(f"error {index}/{len(pending)} {row.event_key} {text}", flush=True)
            if "429" in text or "418" in text:
                stopped_on_limit = True
                break
            time.sleep(5)
            continue

        if rows:
            frame = normalize_rows(row, rows)
            symbol_dir = RAW_DIR / row.symbol.lower()
            symbol_dir.mkdir(parents=True, exist_ok=True)
            write_parquet_atomic(
                frame,
                symbol_dir / f"{row.symbol.lower()}_{int(row.funding_time)}.parquet",
            )
            new_frames.append(frame)
            print(f"ok {index}/{len(pending)} {row.event_key} rows={len(frame)}", flush=True)
        else:
            print(f"empty {index}/{len(pending)} {row.event_key}", flush=True)

        time.sleep(EVENT_SLEEP_SEC + random.random() * EVENT_SLEEP_JITTER_SEC)

    frames = [existing] if not existing.empty else []
    frames.extend(new_frames)
    merged = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(["event_key", "agg_trade_id"])
        .sort_values(["funding_time", "symbol", "timestamp_ms", "agg_trade_id"])
        if frames
        else pd.DataFrame()
    )
    write_parquet_atomic(merged, COMBINED_PATH)

    meta = json.loads(META_PATH.read_text(encoding="utf-8")) if META_PATH.exists() else {}
    meta_errors = meta.get("errors", [])
    meta_errors.extend(errors)
    meta.update(
        {
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "last_mode": f"abs{THRESHOLD_BPS}_batch{BATCH_SIZE}_direct_slow",
            "batch_requested": int(len(pending)),
            "batch_new_events": int(len(new_frames)),
            "batch_errors": errors,
            "stopped_on_limit": stopped_on_limit,
            "events_done": int(merged["event_key"].nunique()) if not merged.empty else 0,
            "trades_saved": int(len(merged)),
            "errors": meta_errors,
            "combined_path": str(COMBINED_PATH.resolve()),
        },
    )
    META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"done_after={merged['event_key'].nunique() if not merged.empty else 0} "
        f"rows={len(merged)} new_events={len(new_frames)} "
        f"stopped_on_limit={stopped_on_limit} errors={len(errors)}",
        flush=True,
    )
    return len(new_frames), stopped_on_limit


# 自动按批次续拉；可随时 Ctrl+C 手动停止。
def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    events = load_events()
    while True:
        new_events, stopped_on_limit = run_batch(events)
        if stopped_on_limit:
            print("stopped after rate limit response", flush=True)
            return
        if new_events == 0:
            print("all requested events are done", flush=True)
            return
        pause = BATCH_PAUSE_SEC + random.random() * BATCH_PAUSE_JITTER_SEC
        print(f"batch pause {pause:.1f}s", flush=True)
        time.sleep(pause)


if __name__ == "__main__":
    main()
