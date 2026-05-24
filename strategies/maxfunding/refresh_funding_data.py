from __future__ import annotations

import os
import platform
import random
import time
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv


STRATEGY_DIR = Path(__file__).resolve().parent
DATA_DIR = STRATEGY_DIR / "data"
ROOT = STRATEGY_DIR.parents[1]
BASE_URL = "https://fapi.binance.com"
START = "20250101"
FUNDING_PATH = DATA_DIR / "ALL-Funding-20250101.parquet"
TICK_PATH = DATA_DIR / "ALL-PERP-FundingEventTicks-20250101.parquet"
WINDOW_BEFORE_MS = 3000
WINDOW_AFTER_MS = 3000
FUNDING_SLEEP_SECONDS = 0.61
EVENT_SLEEP_SEC = 0.2
EVENT_SLEEP_JITTER_SEC = 0.2
PAGE_SLEEP_SEC = 0.2
PAGE_SLEEP_JITTER_SEC = 0.2
REQUEST_TIMEOUT_SEC = 20
WEIGHT_SOFT_LIMIT = 2000
WEIGHT_RESET_BUFFER_SEC = 1.0
BEIJING_TZ = "Asia/Shanghai"
TICK_STATUS_COL = "tick_status"
TICK_STATUS_EMPTY = "empty"
TICK_STATUS_HAS_TICK = "has_tick"


class RateLimitError(RuntimeError):
    pass


# 构建本策略后处理用的 Binance session。
def new_session() -> requests.Session:
    load_dotenv(ROOT / ".env")
    session = requests.Session()
    if platform.system() == "Windows":
        proxy_url = os.environ.get("PROXY_URL")
        if proxy_url:
            session.proxies.update({"http": proxy_url, "https": proxy_url})
    return session


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def start_ms() -> int:
    return int(datetime.strptime(START, "%Y%m%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def time_label(ms: Any) -> str:
    if ms is None or pd.isna(ms):
        return "无"
    return pd.to_datetime(int(ms), unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S")


def to_bj(ms_series: pd.Series) -> pd.Series:
    return pd.to_datetime(ms_series, unit="ms", utc=True).dt.tz_convert(BEIJING_TZ)


def bj_text(dt_series: pd.Series) -> pd.Series:
    return dt_series.dt.strftime("%Y-%m-%d %H:%M")


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temp, index=False)
    os.replace(temp, path)


def request_json(session: requests.Session, path: str, params: dict[str, Any] | None = None) -> Any:
    response = session.get(f"{BASE_URL}{path}", params=params or {}, timeout=REQUEST_TIMEOUT_SEC)
    data = response.json()
    if response.status_code in (418, 429):
        raise RateLimitError(f"rate limit: status={response.status_code}, body={str(data)[:300]}")
    if response.status_code >= 400:
        raise RuntimeError(f"http error: status={response.status_code}, body={str(data)[:300]}")
    if isinstance(data, dict) and data.get("code") in {-1003, -1015}:
        raise RateLimitError(f"rate limit: {data}")
    return data


def normalize_funding(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["funding_time"] = pd.to_numeric(frame["funding_time"], errors="coerce").astype("int64")
    raw_rate = (
        pd.to_numeric(frame["funding_rate"], errors="coerce")
        if "funding_rate" in frame.columns
        else pd.Series(pd.NA, index=frame.index, dtype="float64")
    )
    rate = (
        pd.to_numeric(frame["rate"], errors="coerce")
        if "rate" in frame.columns
        else pd.Series(pd.NA, index=frame.index, dtype="float64")
    )
    frame["rate"] = rate.fillna(raw_rate).astype("float64")
    frame["rate_bps"] = (
        pd.to_numeric(frame["rate_bps"], errors="coerce")
        if "rate_bps" in frame.columns
        else pd.Series(pd.NA, index=frame.index, dtype="float64")
    ).fillna(frame["rate"] * 10000.0)
    frame["abs_rate_bps"] = (
        pd.to_numeric(frame["abs_rate_bps"], errors="coerce")
        if "abs_rate_bps" in frame.columns
        else pd.Series(pd.NA, index=frame.index, dtype="float64")
    ).fillna(frame["rate_bps"].abs())
    if "funding_utc" in frame.columns:
        utc = pd.to_datetime(frame["funding_utc"], utc=True, errors="coerce")
    else:
        utc = pd.Series(pd.NaT, index=frame.index)
    frame["funding_utc"] = utc.fillna(pd.to_datetime(frame["funding_time"], unit="ms", utc=True))
    if "side" not in frame.columns:
        frame["side"] = pd.NA
    missing_side = frame["side"].isna()
    frame.loc[missing_side, "side"] = frame.loc[missing_side, "rate_bps"].map(
        lambda value: "SELL" if value > 0 else "BUY",
    )
    if TICK_STATUS_COL not in frame.columns:
        frame[TICK_STATUS_COL] = pd.NA
    low_rate = frame["abs_rate_bps"] < 30
    frame.loc[low_rate & frame[TICK_STATUS_COL].isna(), TICK_STATUS_COL] = TICK_STATUS_EMPTY
    return frame.drop_duplicates(["symbol", "funding_time"], keep="last").sort_values(["symbol", "funding_time"]).reset_index(drop=True)


def get_symbols(session: requests.Session) -> list[str]:
    data = request_json(session, "/fapi/v1/exchangeInfo")
    return sorted(
        item["symbol"]
        for item in data.get("symbols", [])
        if item.get("contractType") == "PERPETUAL"
        and item.get("quoteAsset") == "USDT"
        and item.get("status") == "TRADING"
    )


def fetch_funding_range(session: requests.Session, symbol: str, since_ms: int, until_ms: int) -> pd.DataFrame:
    frames = []
    current = since_ms
    while current <= until_ms:
        rows = request_json(
            session,
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "startTime": current, "endTime": until_ms, "limit": 1000},
        )
        if not rows:
            break
        frame = pd.DataFrame(rows).rename(
            columns={"fundingTime": "funding_time", "fundingRate": "funding_rate", "markPrice": "mark_price"},
        )
        frame["funding_time"] = pd.to_numeric(frame["funding_time"], errors="coerce").astype("int64")
        frame["funding_rate"] = pd.to_numeric(frame["funding_rate"], errors="coerce")
        frame["mark_price"] = pd.to_numeric(frame["mark_price"], errors="coerce")
        frame["funding_time_bj"] = to_bj(frame["funding_time"])
        frame["funding_time_bj_text"] = bj_text(frame["funding_time_bj"])
        frames.append(frame[["symbol", "funding_time", "funding_time_bj", "funding_time_bj_text", "funding_rate", "mark_price"]])
        current = int(frame["funding_time"].max()) + 1
        if len(rows) < 1000:
            break
        time.sleep(FUNDING_SLEEP_SECONDS)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# 增量刷新策略目录内 funding 总文件。
def refresh_funding() -> pd.DataFrame:
    session = new_session()
    old = normalize_funding(read_parquet(FUNDING_PATH))
    until_ms = now_ms()
    frames = [old] if not old.empty else []
    total_new = 0
    symbols = get_symbols(session)
    print(
        f"funding刷新开始，交易所symbol数：{len(symbols)}，本地行数：{len(old)}，"
        f"本地最大时间：{time_label(old['funding_time'].max()) if not old.empty else '无'}",
        flush=True,
    )
    for index, symbol in enumerate(symbols, 1):
        exist = old[old["symbol"] == symbol] if not old.empty and "symbol" in old.columns else pd.DataFrame()
        ranges: list[tuple[int, int]] = []
        if exist.empty:
            ranges.append((start_ms(), until_ms))
        else:
            latest = int(exist["funding_time"].max())
            if latest < until_ms:
                ranges.append((latest + 1, until_ms))
        if not ranges:
            print(f"funding进度：{index}/{len(symbols)}，{symbol} 已是最新", flush=True)
            continue
        print(
            f"funding进度：{index}/{len(symbols)}，{symbol}，"
            f"补 {time_label(ranges[0][0])} -> {time_label(ranges[-1][1])}",
            flush=True,
        )
        symbol_new = 0
        for since_ms, end_ms in ranges:
            new = fetch_funding_range(session, symbol, since_ms, end_ms)
            if not new.empty:
                symbol_new += len(new)
                total_new += len(new)
                frames.append(new)
        print(f"funding完成：{index}/{len(symbols)}，{symbol}，新增：{symbol_new}", flush=True)
    merged = normalize_funding(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
    write_parquet(merged, FUNDING_PATH)
    print(f"funding刷新完成，新增：{total_new}，总行数：{len(merged)}，文件：{FUNDING_PATH}", flush=True)
    return merged


def event_key(symbol: str, funding_time: int) -> str:
    return f"{symbol}_{int(funding_time)}"


def event_frame(events: pd.DataFrame, funding: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    rows = []
    funding = normalize_funding(funding)
    for event in events.itertuples(index=False):
        symbol = str(event.symbol).upper()
        target = int(pd.Timestamp(event.funding_time).timestamp() * 1000)
        subset = funding[funding["symbol"].astype(str).str.upper().eq(symbol)].copy()
        if subset.empty:
            rows.append({"symbol": symbol, "funding_time": target, "event_key": event_key(symbol, target)})
            continue
        subset["delta"] = (subset["funding_time"].astype("int64") - target).abs()
        row = subset.sort_values("delta").iloc[0].to_dict()
        if int(row["delta"]) > 10:
            row = {"symbol": symbol, "funding_time": target}
        row["event_key"] = event_key(symbol, int(row["funding_time"]))
        rows.append(row)
    out = pd.DataFrame(rows)
    if not out.empty:
        out["funding_time"] = out["funding_time"].astype("int64")
    return out


def high_funding_events(funding: pd.DataFrame) -> pd.DataFrame:
    funding = normalize_funding(funding)
    if funding.empty:
        return pd.DataFrame()
    events = funding[funding["abs_rate_bps"].astype(float) >= 30.0].copy()
    if TICK_STATUS_COL in events.columns:
        events = events[events[TICK_STATUS_COL].fillna("") != TICK_STATUS_EMPTY].copy()
    events["event_key"] = events["symbol"].astype(str) + "_" + events["funding_time"].astype("int64").astype(str)
    return events.sort_values(
        ["funding_time", "abs_rate_bps", "symbol"],
        ascending=[False, False, True],
    )


def load_existing_ticks() -> tuple[pd.DataFrame, set[str]]:
    ticks = read_parquet(TICK_PATH)
    if ticks.empty:
        return ticks, set()
    ticks["event_key"] = ticks["event_key"].astype(str)
    return ticks, set(ticks["event_key"].unique())


def used_weight_1m(response: requests.Response) -> int | None:
    value = response.headers.get("X-MBX-USED-WEIGHT-1M")
    return int(value) if value is not None else None


def wait_for_weight_reset(weight: int | None) -> None:
    if weight is None or weight < WEIGHT_SOFT_LIMIT:
        return
    pause = 60 - (time.time() % 60) + WEIGHT_RESET_BUFFER_SEC
    print(f"used_weight_1m：{weight}，等待下一分钟窗口：{pause:.1f}s", flush=True)
    time.sleep(pause)


def fetch_tick_window(session: requests.Session, symbol: str, funding_time: int) -> tuple[list[dict], int | None]:
    rows: list[dict] = []
    max_weight: int | None = None
    cursor = funding_time - WINDOW_BEFORE_MS
    end_ms = funding_time + WINDOW_AFTER_MS
    while cursor <= end_ms:
        response = session.get(
            f"{BASE_URL}/fapi/v1/aggTrades",
            params={"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": 1000},
            timeout=REQUEST_TIMEOUT_SEC,
        )
        weight = used_weight_1m(response)
        if weight is not None:
            max_weight = weight if max_weight is None else max(max_weight, weight)
        if response.status_code in (418, 429):
            raise RateLimitError(f"{response.status_code} used_weight_1m={weight} {response.text[:160]}")
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        rows.extend(batch)
        last_ts = max(int(item["T"]) for item in batch)
        if len(batch) < 1000 or last_ts >= end_ms:
            break
        cursor = last_ts + 1
        time.sleep(PAGE_SLEEP_SEC + random.random() * PAGE_SLEEP_JITTER_SEC)
    return rows, max_weight


def normalize_ticks(row: Any, rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows).rename(
        columns={"a": "agg_trade_id", "p": "price", "q": "quantity", "T": "timestamp_ms", "m": "buyer_maker"},
    )
    frame["timestamp_ms"] = frame["timestamp_ms"].astype("int64")
    frame["timestamp"] = pd.to_datetime(frame["timestamp_ms"], unit="ms", utc=True)
    frame["symbol"] = row.symbol
    frame["funding_time"] = int(row.funding_time)
    frame["funding_utc"] = pd.to_datetime(int(row.funding_time), unit="ms", utc=True).isoformat()
    frame["rate_bps"] = float(getattr(row, "rate_bps", 0.0))
    frame["abs_rate_bps"] = float(getattr(row, "abs_rate_bps", abs(frame["rate_bps"].iloc[0])))
    frame["side"] = "SELL" if frame["rate_bps"].iloc[0] > 0 else "BUY"
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


def mark_tick_status(funding: pd.DataFrame, keys: set[str], status: str) -> pd.DataFrame:
    if not keys or funding.empty:
        return funding
    funding = funding.copy()
    if TICK_STATUS_COL not in funding.columns:
        funding[TICK_STATUS_COL] = pd.NA
    current_keys = funding["symbol"].astype(str) + "_" + funding["funding_time"].astype("int64").astype(str)
    funding.loc[current_keys.isin(keys), TICK_STATUS_COL] = status
    return funding


# 补齐所有 30bps 以上 funding 事件附近 tick，模型训练和测量共用。
def refresh_event_ticks(funding: pd.DataFrame) -> pd.DataFrame:
    requests_df = high_funding_events(funding)
    existing, done = load_existing_ticks()
    pending = requests_df[~requests_df["event_key"].astype(str).isin(done)] if not requests_df.empty else pd.DataFrame()
    print(
        f"tick刷新开始，30bps事件数：{len(requests_df)}，已完成事件数：{len(done)}，"
        f"待补事件数：{len(pending)}",
        flush=True,
    )
    if pending.empty:
        print(f"event tick 已是最新，事件数：{len(requests_df)}，文件：{TICK_PATH}", flush=True)
        return existing

    session = new_session()
    frames = [existing] if not existing.empty else []
    has_tick: set[str] = set()
    empty_tick: set[str] = set()
    for index, row in enumerate(pending.itertuples(index=False), 1):
        rows, weight = fetch_tick_window(session, str(row.symbol), int(row.funding_time))
        if rows:
            frame = normalize_ticks(row, rows)
            frames.append(frame)
            has_tick.add(str(row.event_key))
            print(f"tick完成，进度：{index}/{len(pending)}，事件：{row.event_key}，行数：{len(frame)}", flush=True)
        else:
            empty_tick.add(str(row.event_key))
            print(f"tick为空，进度：{index}/{len(pending)}，事件：{row.event_key}", flush=True)
        wait_for_weight_reset(weight)
        time.sleep(EVENT_SLEEP_SEC + random.random() * EVENT_SLEEP_JITTER_SEC)

    merged = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(["event_key", "agg_trade_id"])
        .sort_values(["funding_time", "symbol", "timestamp_ms", "agg_trade_id"])
        if frames
        else pd.DataFrame()
    )
    write_parquet(merged, TICK_PATH)
    funding = mark_tick_status(funding, has_tick, TICK_STATUS_HAS_TICK)
    funding = mark_tick_status(funding, empty_tick, TICK_STATUS_EMPTY)
    write_parquet(normalize_funding(funding), FUNDING_PATH)
    print(f"event tick刷新完成，新增事件：{len(has_tick)}，空事件：{len(empty_tick)}，文件：{TICK_PATH}", flush=True)
    return merged


# 只刷新策略本地行情数据，不处理策略 events。
def main() -> None:
    print("开始刷新 MaxFunding 本地数据", flush=True)
    print("步骤1：刷新 ALL-Funding", flush=True)
    funding = refresh_funding()
    print("步骤1完成：ALL-Funding 已刷新", flush=True)
    print("步骤2：刷新 30bps+ funding tick", flush=True)
    refresh_event_ticks(funding)
    print("步骤2完成：ALL-PERP-FundingEventTicks 已刷新", flush=True)
    print("MaxFunding 本地数据刷新完成", flush=True)


if __name__ == "__main__":
    main()
