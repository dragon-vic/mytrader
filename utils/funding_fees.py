from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from datetime import UTC
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests
from dotenv import load_dotenv

from utils.config_loader import ROOT
from utils.config_loader import proxy_url


BINANCE_URLS = {
    "testnet": "https://testnet.binancefuture.com",
    "live": "https://fapi.binance.com",
}
BINANCE_KEYS = {
    "testnet": ("BINANCE_FUTURES_TESTNET_API_KEY", "BINANCE_FUTURES_TESTNET_API_SECRET"),
    "live": ("BINANCE_FUTURES_API_KEY", "BINANCE_FUTURES_API_SECRET"),
}
OKX_URL = "https://www.okx.com"
OKX_FUNDING_TYPE = "8"


# 给重建仓位补实际资金费收入，收入为正，支出为负。
def add_funding_income(positions: pd.DataFrame, settings: dict[str, Any]) -> pd.DataFrame:
    if positions.empty:
        return positions

    result = positions.copy()
    result["funding_income"] = 0.0
    fees = fetch_funding_fees(result, settings)
    if fees.empty:
        return result

    result["_pos_index"] = range(len(result))
    result["_symbol"] = result["instrument_id"].map(exchange_symbol)
    result["_venue"] = result["instrument_id"].map(exchange_venue)
    result["_start_ms"] = result[["open_time", "close_time"]].min(axis=1).map(timestamp_ms)
    result["_end_ms"] = result[["open_time", "close_time"]].max(axis=1).map(timestamp_ms)
    result["_notional"] = pd.to_numeric(result["qty"], errors="coerce").abs() * pd.to_numeric(
        result["avg_open"],
        errors="coerce",
    ).abs()

    for row in fees.to_dict("records"):
        mask = (
            result["_venue"].eq(row["venue"])
            & result["_symbol"].eq(row["symbol"])
            & result["_start_ms"].le(row["time_ms"])
            & result["_end_ms"].ge(row["time_ms"])
        )
        matches = result.loc[mask]
        if matches.empty:
            continue
        weights = matches["_notional"].fillna(0.0)
        if weights.sum() <= 0:
            weights = pd.Series(1.0, index=matches.index)
        result.loc[matches.index, "funding_income"] += float(row["income"]) * weights / weights.sum()

    return result.drop(columns=["_pos_index", "_symbol", "_venue", "_start_ms", "_end_ms", "_notional"])


# 读取仓位涉及交易所的资金费流水。
def fetch_funding_fees(positions: pd.DataFrame, settings: dict[str, Any]) -> pd.DataFrame:
    start_ms = int(positions[["open_time", "close_time"]].min(axis=1).map(timestamp_ms).min())
    end_ms = int(positions[["open_time", "close_time"]].max(axis=1).map(timestamp_ms).max())
    symbols = (
        positions.assign(
            venue=positions["instrument_id"].map(exchange_venue),
            symbol=positions["instrument_id"].map(exchange_symbol),
        )[["venue", "symbol"]]
        .dropna()
        .drop_duplicates()
    )

    rows: list[dict[str, Any]] = []
    for item in symbols.to_dict("records"):
        venue = item["venue"]
        symbol = item["symbol"]
        try:
            if venue == "BINANCE":
                rows.extend(fetch_binance_funding(settings, symbol, start_ms, end_ms))
            elif venue == "OKX":
                rows.extend(fetch_okx_funding(settings, symbol, start_ms, end_ms))
            else:
                print(f"funding流水跳过：暂不支持交易所 {venue} symbol={symbol}", flush=True)
        except Exception as exc:
            print(f"funding流水获取失败：venue={venue} symbol={symbol} reason={exc}", flush=True)
    return pd.DataFrame(rows)


# 查询 Binance U 本位 funding fee 实际到账流水。
def fetch_binance_funding(settings: dict[str, Any], symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    load_dotenv(ROOT / ".env")
    key_name, secret_name = BINANCE_KEYS[settings["mode"]]
    session = requests.Session()
    session.headers.update({"X-MBX-APIKEY": os.environ[key_name]})
    apply_proxy(session, settings)
    secret = os.environ[secret_name].encode()
    rows: list[dict[str, Any]] = []
    for chunk_start, chunk_end in time_chunks(start_ms, end_ms, 7 * 24 * 60 * 60 * 1000):
        cursor = chunk_start
        while cursor <= chunk_end:
            data = binance_signed_get(
                session,
                secret,
                settings["mode"],
                "/fapi/v1/income",
                {
                    "symbol": symbol,
                    "incomeType": "FUNDING_FEE",
                    "startTime": cursor,
                    "endTime": chunk_end,
                    "limit": 1000,
                },
            )
            if not data:
                break
            batch = sorted(data, key=lambda item: int(item["time"]))
            for item in batch:
                rows.append(
                    {
                        "venue": "BINANCE",
                        "symbol": str(item["symbol"]),
                        "time_ms": int(item["time"]),
                        "income": float(item["income"]),
                        "tran_id": str(item.get("tranId", "")),
                    },
                )
            if len(batch) < 1000:
                break
            cursor = int(batch[-1]["time"]) + 1
            time.sleep(0.05)
    return rows


def binance_signed_get(
    session: requests.Session,
    secret: bytes,
    mode: str,
    path: str,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    payload = dict(params)
    payload["timestamp"] = int(time.time() * 1000)
    payload["recvWindow"] = 10000
    query = urlencode(payload)
    sig = hmac.new(secret, query.encode(), hashlib.sha256).hexdigest()
    response = session.get(f"{BINANCE_URLS[mode]}{path}?{query}&signature={sig}", timeout=20)
    data = response.json()
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code}: {data}")
    return data


# 查询 OKX 账户账单里的资金费流水。
def fetch_okx_funding(settings: dict[str, Any], symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    load_dotenv(ROOT / ".env")
    session = requests.Session()
    apply_proxy(session, settings)
    rows: list[dict[str, Any]] = []
    for chunk_start, chunk_end in time_chunks(start_ms, end_ms, 30 * 24 * 60 * 60 * 1000):
        data = okx_signed_get(
            session,
            settings,
            "/api/v5/account/bills-archive",
            {
                "instId": symbol,
                "type": OKX_FUNDING_TYPE,
                "begin": chunk_start,
                "end": chunk_end,
                "limit": 100,
            },
        )
        for item in data:
            income = item.get("balChg") or item.get("pnl") or "0"
            rows.append(
                {
                    "venue": "OKX",
                    "symbol": str(item.get("instId", symbol)),
                    "time_ms": int(item["ts"]),
                    "income": float(income),
                    "tran_id": str(item.get("billId", "")),
                },
            )
    return rows


def okx_signed_get(
    session: requests.Session,
    settings: dict[str, Any],
    path: str,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    query = urlencode(params)
    request_path = f"{path}?{query}"
    timestamp = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    secret = os.environ["OKX_API_SECRET"].encode()
    prehash = f"{timestamp}GET{request_path}"
    sign = base64.b64encode(hmac.new(secret, prehash.encode(), hashlib.sha256).digest()).decode()
    headers = {
        "OK-ACCESS-KEY": os.environ["OKX_API_KEY"],
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": os.environ["OKX_API_PASSPHRASE"],
    }
    if settings["mode"] == "testnet":
        headers["x-simulated-trading"] = "1"
    response = session.get(f"{OKX_URL}{request_path}", headers=headers, timeout=20)
    data = response.json()
    if response.status_code >= 400 or data.get("code") != "0":
        raise RuntimeError(f"HTTP {response.status_code}: {data}")
    return data.get("data", [])


def apply_proxy(session: requests.Session, settings: dict[str, Any]) -> None:
    url = proxy_url(settings)
    if url:
        session.proxies.update({"http": url, "https": url})


def time_chunks(start_ms: int, end_ms: int, step_ms: int):
    cursor = int(start_ms)
    while cursor <= end_ms:
        chunk_end = min(cursor + step_ms - 1, int(end_ms))
        yield cursor, chunk_end
        cursor = chunk_end + 1


def exchange_venue(instrument_id: Any) -> str:
    return str(instrument_id).rsplit(".", 1)[-1].upper()


def exchange_symbol(instrument_id: Any) -> str:
    text = str(instrument_id)
    venue = exchange_venue(text)
    symbol = text.rsplit(".", 1)[0]
    if venue == "BINANCE":
        return symbol.replace("-PERP", "")
    return symbol


def timestamp_ms(value: Any) -> int:
    ts = pd.to_datetime(value, utc=True)
    return int(ts.timestamp() * 1000)
