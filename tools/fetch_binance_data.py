from __future__ import annotations

import io
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Literal

import pandas as pd
import requests

Kind = Literal["funding", "ticks", "kline"]
Market = Literal["spot", "um"]

SPOT_API = "https://api.binance.com"
UM_API = "https://fapi.binance.com"
VISION = "https://data.binance.vision"
BEIJING_TZ = "Asia/Shanghai"

FUNDING_SLEEP_SECONDS = 0.61
KLINE_UM_SLEEP_SECONDS = 0.12
KLINE_SPOT_SLEEP_SECONDS = 0.04
TICKS_SLEEP_SECONDS = 0.02
TICKS_PROGRESS_EVERY_DAYS = 30
FUNDING_PROGRESS_EVERY_SYMBOLS = 25

INTERVAL_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
}

FUNDING_COLUMNS = ["symbol", "funding_time", "funding_time_bj", "funding_time_bj_text", "funding_rate", "mark_price"]
KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]
AGG_COLUMNS_7 = ["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker"]
AGG_COLUMNS_8 = AGG_COLUMNS_7 + ["is_best_match"]


class RateLimitError(RuntimeError):
    pass


@dataclass(frozen=True)
class FetchConfig:
    kind: Kind
    start: str
    symbols: list[str] | None = None
    interval: str | None = None
    market: Market = "um"
    end: str | None = None


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_yyyymmdd(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d").replace(tzinfo=timezone.utc)


def yyyymmdd_to_date(value: str) -> datetime.date:
    return parse_yyyymmdd(value).date()


def dt_to_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def utc_today() -> datetime.date:
    return datetime.now(timezone.utc).date()


def to_bj(ms_series: pd.Series) -> pd.Series:
    return pd.to_datetime(ms_series, unit="ms", utc=True).dt.tz_convert(BEIJING_TZ)


def bj_text(dt_series: pd.Series, interval: str | None = None, millisecond: bool = False) -> pd.Series:
    if millisecond:
        return dt_series.dt.strftime("%Y-%m-%d %H:%M:%S.%f").str.slice(0, 23)
    if interval is None:
        return dt_series.dt.strftime("%Y-%m-%d %H:%M")
    interval = interval.lower()
    if interval.endswith("s"):
        fmt = "%Y-%m-%d %H:%M:%S"
    elif interval.endswith("m") or interval.endswith("h"):
        fmt = "%Y-%m-%d %H:%M"
    else:
        fmt = "%Y-%m-%d"
    return dt_series.dt.strftime(fmt)


def one_bj_text(ms: int | None, interval: str | None = None) -> str:
    if ms is None:
        return "None"
    s = pd.Series([pd.to_datetime(ms, unit="ms", utc=True).tz_convert(BEIJING_TZ)])
    return str(bj_text(s, interval).iloc[0])


def normalize_market(value: str) -> Market:
    value = value.lower().strip()
    if value in {"um", "u", "usdtm", "usdt-m", "perp", "futures"}:
        return "um"
    if value == "spot":
        return "spot"
    raise ValueError("market must be 'spot' or 'um'")


def market_label(market: Market) -> str:
    return "PERP" if market == "um" else "SPOT"


def normalize_symbol(symbol: str) -> str:
    value = symbol.upper().strip().replace("-PERP", "").replace("-SPOT", "")
    return value if value.endswith("USDT") else f"{value}USDT"


def normalize_symbols(symbols: Iterable[str] | None) -> list[str]:
    result: list[str] = []
    for symbol in symbols or []:
        value = normalize_symbol(symbol)
        if value not in result:
            result.append(value)
    return result


def normalize_interval(interval: str | None) -> str:
    if not interval:
        raise ValueError("kline requires interval")
    value = interval.lower().strip()
    if value not in INTERVAL_MS:
        raise ValueError(f"unsupported interval: {interval}")
    return value


def request_json(session: requests.Session, url: str, params: dict[str, Any] | None = None, timeout: int = 30) -> Any:
    resp = session.get(url, params=params, timeout=timeout)
    if resp.status_code in {418, 429}:
        raise RateLimitError(f"rate limit: status={resp.status_code}, body={resp.text[:300]}")
    if resp.status_code >= 400:
        raise RuntimeError(f"http error: status={resp.status_code}, url={resp.url}, body={resp.text[:300]}")
    data = resp.json()
    if isinstance(data, dict) and data.get("code") in {-1003, -1015}:
        raise RateLimitError(f"rate limit: {data}")
    return data


def download_bytes(session: requests.Session, url: str, timeout: int = 120) -> bytes | None:
    resp = session.get(url, timeout=timeout)
    if resp.status_code in {418, 429}:
        raise RateLimitError(f"rate limit: status={resp.status_code}, url={url}, body={resp.text[:300]}")
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise RuntimeError(f"http error: status={resp.status_code}, url={url}, body={resp.text[:300]}")
    return resp.content


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


def merge_write(path: Path, new_df: pd.DataFrame, dedup_cols: list[str], sort_cols: list[str]) -> pd.DataFrame:
    old_df = read_parquet(path)
    merged = new_df if old_df.empty else pd.concat([old_df, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=dedup_cols, keep="last").sort_values(sort_cols).reset_index(drop=True)
    write_parquet(merged, path)
    return merged


def row_count(path: Path) -> int:
    return 0 if not path.exists() else len(pd.read_parquet(path))


def estimate_rows(start_ms: int, end_ms: int, step_ms: int) -> int:
    if start_ms > end_ms:
        return 0
    return int((end_ms - start_ms) // step_ms) + 1


def funding_path(start: str) -> Path:
    return project_root() / "data" / f"ALL-Funding-{start}.parquet"


def ticks_path(symbol: str, market: Market, start: str) -> Path:
    return project_root() / "data" / f"{symbol}-{market_label(market)}-Ticks-{start}.parquet"


def kline_path(symbol: str, market: Market, interval: str, start: str) -> Path:
    return project_root() / "data" / f"{symbol}-{market_label(market)}-{interval.upper()}-{start}.parquet"


def get_um_symbols(session: requests.Session) -> list[str]:
    data = request_json(session, f"{UM_API}/fapi/v1/exchangeInfo")
    symbols = []
    for item in data.get("symbols", []):
        if item.get("contractType") == "PERPETUAL" and item.get("quoteAsset") == "USDT" and item.get("status") == "TRADING":
            symbols.append(item["symbol"])
    return sorted(symbols)


def fetch_funding(config: FetchConfig) -> None:
    session = requests.Session()
    path = funding_path(config.start)
    start_ms = dt_to_ms(parse_yyyymmdd(config.start))
    end_ms = now_ms()
    old_df = read_parquet(path)
    if old_df.empty:
        old_df = pd.DataFrame(columns=FUNDING_COLUMNS)

    symbols = get_um_symbols(session)
    total_new = 0
    print(f"[START] funding: symbols={len(symbols)}, start={config.start}, output={path}")
    print(f"[FILE] existing={path.exists()}, rows={len(old_df)}")

    for i, symbol in enumerate(symbols, 1):
        exist = old_df[old_df["symbol"] == symbol] if not old_df.empty and "symbol" in old_df.columns else pd.DataFrame()
        current = start_ms if exist.empty else int(exist["funding_time"].max()) + 1
        if current > end_ms:
            if i == 1 or i % FUNDING_PROGRESS_EVERY_SYMBOLS == 0 or i == len(symbols):
                print(f"[PROGRESS] funding {i}/{len(symbols)} {symbol}: up_to_date")
            continue

        approx = estimate_rows(current, end_ms, 8 * 60 * 60 * 1000)
        print(f"[FETCH] funding {i}/{len(symbols)} {symbol}: from_bj={one_bj_text(current)}, estimate_rows≈{approx}")
        symbol_new = 0

        while current <= end_ms:
            rows = request_json(session, f"{UM_API}/fapi/v1/fundingRate", {"symbol": symbol, "startTime": current, "endTime": end_ms, "limit": 1000})
            if not rows:
                break
            df = pd.DataFrame(rows).rename(columns={"fundingTime": "funding_time", "fundingRate": "funding_rate", "markPrice": "mark_price"})
            df["funding_time"] = pd.to_numeric(df["funding_time"], errors="coerce").astype("Int64")
            df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")
            df["mark_price"] = pd.to_numeric(df["mark_price"], errors="coerce")
            df["funding_time_bj"] = to_bj(df["funding_time"])
            df["funding_time_bj_text"] = bj_text(df["funding_time_bj"])
            df = df[FUNDING_COLUMNS]
            old_df = merge_write(path, df, ["symbol", "funding_time"], ["symbol", "funding_time"])
            symbol_new += len(df)
            current = int(df["funding_time"].max()) + 1
            if len(rows) < 1000:
                break
            time.sleep(FUNDING_SLEEP_SECONDS)

        total_new += symbol_new
        print(f"[DONE] funding {i}/{len(symbols)} {symbol}: new_rows={symbol_new}, total_new_rows={total_new}")

    print(f"[END] funding: new_rows={total_new}, final_rows={row_count(path)}, output={path}")


def agg_url(symbol: str, market: Market, day: datetime.date) -> str:
    day_text = day.strftime("%Y-%m-%d")
    if market == "um":
        return f"{VISION}/data/futures/um/daily/aggTrades/{symbol}/{symbol}-aggTrades-{day_text}.zip"
    return f"{VISION}/data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{day_text}.zip"


def read_agg_zip(content: bytes, symbol: str, market: Market) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = [name for name in zf.namelist() if name.endswith(".csv")]
        if not names:
            return pd.DataFrame()
        with zf.open(names[0]) as f:
            df = pd.read_csv(f, header=None)

    if df.empty:
        return df
    if not str(df.iloc[0, 0]).replace(".", "", 1).isdigit():
        df = df.iloc[1:].reset_index(drop=True)

    if df.shape[1] == 7:
        df.columns = AGG_COLUMNS_7
    elif df.shape[1] >= 8:
        df = df.iloc[:, :8]
        df.columns = AGG_COLUMNS_8
    else:
        raise RuntimeError(f"unexpected aggTrades columns: {df.shape[1]}")

    df.insert(0, "symbol", symbol)
    df.insert(1, "market", market_label(market))
    for col in ["agg_trade_id", "first_trade_id", "last_trade_id", "transact_time"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["price", "quantity"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["is_buyer_maker"] = df["is_buyer_maker"].astype(str).str.lower().isin(["true", "1"])
    if "is_best_match" in df.columns:
        df["is_best_match"] = df["is_best_match"].astype(str).str.lower().isin(["true", "1"])

    ts = pd.to_numeric(df["transact_time"], errors="coerce")
    if pd.notna(ts.dropna().median()) and ts.dropna().median() > 10_000_000_000_000:
        df["transact_time_ms"] = (ts // 1000).astype("Int64")
    else:
        df["transact_time_ms"] = ts.astype("Int64")
    df["transact_time_bj"] = to_bj(df["transact_time_ms"])
    df["transact_time_bj_text"] = bj_text(df["transact_time_bj"], millisecond=True)
    return df.dropna(subset=["agg_trade_id", "transact_time_ms"]).sort_values(["transact_time_ms", "agg_trade_id"]).reset_index(drop=True)


def latest_tick_utc_day(path: Path) -> datetime.date | None:
    df = read_parquet(path)
    if df.empty or "transact_time_ms" not in df.columns:
        return None
    latest_ms = pd.to_numeric(df["transact_time_ms"], errors="coerce").max()
    if pd.isna(latest_ms):
        return None
    return pd.to_datetime(int(latest_ms), unit="ms", utc=True).date()


def fetch_ticks(config: FetchConfig) -> None:
    session = requests.Session()
    market = normalize_market(config.market)
    symbols = normalize_symbols(config.symbols)
    if not symbols:
        raise ValueError("ticks requires symbols")
    end_day = yyyymmdd_to_date(config.end) if config.end else utc_today() - timedelta(days=1)

    for symbol in symbols:
        path = ticks_path(symbol, market, config.start)
        latest_day = latest_tick_utc_day(path)
        current_day = yyyymmdd_to_date(config.start) if latest_day is None else latest_day + timedelta(days=1)
        total_days = max((end_day - current_day).days + 1, 0)
        done_days = 0
        total_new = 0
        print(f"[START] ticks {symbol} {market_label(market)}: from={current_day}, to={end_day}, days={total_days}, output={path}")
        print(f"[FILE] existing={path.exists()}, rows={row_count(path)}, latest_utc_day={latest_day}")

        while current_day <= end_day:
            content = download_bytes(session, agg_url(symbol, market, current_day))
            done_days += 1
            if content is None:
                if done_days == 1 or done_days % TICKS_PROGRESS_EVERY_DAYS == 0 or current_day == end_day:
                    print(f"[PROGRESS] ticks {symbol}: day={current_day}, file_not_found, done_days={done_days}/{total_days}")
                current_day += timedelta(days=1)
                continue

            df = read_agg_zip(content, symbol, market)
            if not df.empty:
                merge_write(path, df, ["symbol", "market", "agg_trade_id"], ["transact_time_ms", "agg_trade_id"])
                total_new += len(df)
            if done_days == 1 or done_days % TICKS_PROGRESS_EVERY_DAYS == 0 or current_day == end_day:
                print(f"[PROGRESS] ticks {symbol}: day={current_day}, done_days={done_days}/{total_days}, new_rows={total_new}")
            current_day += timedelta(days=1)
            time.sleep(TICKS_SLEEP_SECONDS)

        print(f"[END] ticks {symbol}: new_rows={total_new}, final_rows={row_count(path)}, output={path}")

    print("[END] ticks: all symbols done")


def kline_url(market: Market) -> str:
    return f"{UM_API}/fapi/v1/klines" if market == "um" else f"{SPOT_API}/api/v3/klines"


def latest_kline_open(path: Path) -> int | None:
    df = read_parquet(path)
    if df.empty or "open_time" not in df.columns:
        return None
    value = pd.to_numeric(df["open_time"], errors="coerce").max()
    return None if pd.isna(value) else int(value)


def parse_klines(rows: list[list[Any]], symbol: str, market: Market, interval: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=KLINE_COLUMNS)
    df.insert(0, "symbol", symbol)
    df.insert(1, "market", market_label(market))
    for col in ["open_time", "close_time", "trade_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["open_time_bj"] = to_bj(df["open_time"])
    df["close_time_bj"] = to_bj(df["close_time"])
    df["open_time_bj_text"] = bj_text(df["open_time_bj"], interval)
    df["close_time_bj_text"] = bj_text(df["close_time_bj"], interval)
    return df.drop(columns=["ignore"]).sort_values("open_time").reset_index(drop=True)


def check_kline_integrity(path: Path, interval: str) -> None:
    df = read_parquet(path).sort_values("open_time").reset_index(drop=True)
    if df.empty:
        print(f"[CHECK] kline empty: {path}")
        return
    dup_count = int(df["open_time"].duplicated().sum())
    if dup_count:
        raise ValueError(f"kline integrity failed: duplicate open_time count={dup_count}, file={path}")
    expected = INTERVAL_MS[interval]
    bad = pd.to_numeric(df["open_time"], errors="coerce").diff().dropna()
    bad = bad[bad != expected]
    if not bad.empty:
        examples = []
        for idx in bad.index[:10]:
            prev_ms = int(df.loc[idx - 1, "open_time"])
            curr_ms = int(df.loc[idx, "open_time"])
            examples.append({"prev_bj": one_bj_text(prev_ms, interval), "curr_bj": one_bj_text(curr_ms, interval), "diff_ms": curr_ms - prev_ms})
        raise ValueError(f"kline integrity failed: expected_ms={expected}, bad_gap_count={len(bad)}, examples={examples}, file={path}")
    print(f"[CHECK] kline OK: {path.name}, rows={len(df)}, interval={interval}")


def fetch_kline(config: FetchConfig) -> None:
    session = requests.Session()
    market = normalize_market(config.market)
    interval = normalize_interval(config.interval)
    symbols = normalize_symbols(config.symbols)
    if not symbols:
        raise ValueError("kline requires symbols")
    start_ms = dt_to_ms(parse_yyyymmdd(config.start))
    end_ms = dt_to_ms(parse_yyyymmdd(config.end)) if config.end else now_ms()
    step_ms = INTERVAL_MS[interval]
    sleep_seconds = KLINE_UM_SLEEP_SECONDS if market == "um" else KLINE_SPOT_SLEEP_SECONDS

    for symbol in symbols:
        path = kline_path(symbol, market, interval, config.start)
        latest_open = latest_kline_open(path)
        current = start_ms if latest_open is None else latest_open + step_ms
        approx = estimate_rows(current, end_ms, step_ms)
        total_new = 0
        print(f"[START] kline {symbol} {market_label(market)} {interval}: from_bj={one_bj_text(current, interval)}, to_bj={one_bj_text(end_ms, interval)}, estimate_rows≈{approx}, output={path}")
        print(f"[FILE] existing={path.exists()}, rows={row_count(path)}, latest_bj={one_bj_text(latest_open, interval)}")

        while current <= end_ms:
            rows = request_json(session, kline_url(market), {"symbol": symbol, "interval": interval, "startTime": current, "endTime": end_ms, "limit": 1000})
            if not rows:
                break
            df = parse_klines(rows, symbol, market, interval)
            if df.empty:
                break
            merge_write(path, df, ["symbol", "market", "open_time"], ["open_time"])
            total_new += len(df)
            latest = int(df["open_time"].max())
            current = latest + step_ms
            print(f"[PROGRESS] kline {symbol}: batch_rows={len(df)}, new_rows={total_new}, latest_bj={one_bj_text(latest, interval)}")
            if len(rows) < 1000:
                break
            time.sleep(sleep_seconds)

        check_kline_integrity(path, interval)
        print(f"[END] kline {symbol}: new_rows={total_new}, final_rows={row_count(path)}, output={path}")

    print("[END] kline: all symbols done")


def run(config: FetchConfig) -> None:
    try:
        if config.kind == "funding":
            fetch_funding(config)
        elif config.kind == "ticks":
            fetch_ticks(config)
        elif config.kind == "kline":
            fetch_kline(config)
        else:
            raise ValueError("kind must be funding, ticks, or kline")
    except RateLimitError as exc:
        print("[STOP] rate limit detected; stop immediately")
        print(str(exc))


def main() -> None:
    config = FetchConfig(
        kind="kline",          # funding / ticks / kline
        start="20250101",     # YYYYMMDD
        end=None,              # None: funding/kline 拉到最新，ticks 拉到昨天
        symbols=["BTC", "ETH"],
        interval="1h",        # kline only: 1m / 1h / 1d ...
        market="um",          # spot / um；um = U 本位 USDⓈ-M Futures
    )
    run(config)


if __name__ == "__main__":
    main()
