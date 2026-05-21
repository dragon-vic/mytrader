from __future__ import annotations

import hashlib
import hmac
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
BASE_URL = "https://fapi.binance.com"
REQUEST_DELAY_SEC = 0.35
TARGET_QUOTES = ("USDT", "USDC", "BTC")


# 读取 .env 里的 Windows 代理。
def load_proxy() -> dict[str, str] | None:
    if platform.system() != "Windows":
        return None
    proxy_url = os.environ.get("PROXY_URL")
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


# 构建带实盘 U 本位 API key 的 session。
def build_session() -> requests.Session:
    load_dotenv(ROOT / ".env")
    session = requests.Session()
    session.headers.update({"X-MBX-APIKEY": os.environ["BINANCE_FUTURES_API_KEY"]})
    proxies = load_proxy()
    if proxies is not None:
        session.proxies.update(proxies)
    return session


# 发 Binance 签名请求。
def signed_request(
    session: requests.Session,
    secret: bytes,
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    payload = dict(params or {})
    payload["timestamp"] = int(time.time() * 1000)
    payload["recvWindow"] = 10000
    query = urlencode(payload)
    sig = hmac.new(secret, query.encode(), hashlib.sha256).hexdigest()
    url = f"{BASE_URL}{path}?{query}&signature={sig}"
    response = session.request(method, url, timeout=20)
    data = response.json()
    if response.status_code in (418, 429):
        raise RuntimeError(f"RATE_LIMIT HTTP {response.status_code}: {data}")
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code}: {data}")
    return data


# 读取当前可交易 U 本位永续合约。
def trading_symbols(session: requests.Session) -> set[str]:
    response = session.get(f"{BASE_URL}/fapi/v1/exchangeInfo", timeout=30)
    response.raise_for_status()
    info = response.json()
    return {
        item["symbol"]
        for item in info["symbols"]
        if item.get("status") == "TRADING"
        and item.get("contractType") == "PERPETUAL"
    }


# 读取账户侧当前 symbol 杠杆。
def current_leverages(
    session: requests.Session,
    secret: bytes,
    symbols: set[str],
) -> dict[str, int]:
    configs = signed_request(session, secret, "GET", "/fapi/v1/symbolConfig")
    result = {}
    for item in configs:
        symbol = item.get("symbol")
        if symbol in symbols:
            result[symbol] = int(item["leverage"])
    return result


# 把 PHB、PHB/USDT、PHBUSDT、PHBUSDT-PERP.BINANCE 统一成 Binance raw symbol。
def resolve_symbol(raw: str, symbols: set[str]) -> str:
    symbol = raw.upper().strip()
    symbol = symbol.replace("-PERP.BINANCE", "").replace("/", "")
    if symbol in symbols:
        return symbol
    if not any(symbol.endswith(quote) for quote in TARGET_QUOTES):
        usdt_symbol = f"{symbol}USDT"
        if usdt_symbol in symbols:
            return usdt_symbol
    raise ValueError(f"Unknown USD-M perpetual symbol: {raw}")


# 改一个 symbol 的初始杠杆。
def change_one(
    session: requests.Session,
    secret: bytes,
    symbol: str,
    leverage: int,
) -> dict[str, Any]:
    return signed_request(
        session,
        secret,
        "POST",
        "/fapi/v1/leverage",
        {"symbol": symbol, "leverage": leverage},
    )


# 打印用法并退出。
def usage() -> None:
    print("Usage:")
    print("  D:\\app\\miniconda\\envs\\nt\\python.exe change_leverage.py all 1")
    print("  D:\\app\\miniconda\\envs\\nt\\python.exe change_leverage.py PHB 1")
    raise SystemExit(2)


def main() -> None:
    if len(sys.argv) != 3:
        usage()

    target = sys.argv[1]
    leverage = int(sys.argv[2])
    if leverage < 1 or leverage > 125:
        raise ValueError("leverage must be between 1 and 125")

    session = build_session()
    secret = os.environ["BINANCE_FUTURES_API_SECRET"].encode()
    symbols = trading_symbols(session)
    current = current_leverages(session, secret, symbols)

    if target.lower() == "all":
        targets = sorted(symbol for symbol, value in current.items() if value != leverage)
    else:
        symbol = resolve_symbol(target, symbols)
        targets = [] if current[symbol] == leverage else [symbol]

    print(
        f"symbols={len(symbols)} current={len(current)} "
        f"target={target} leverage={leverage} need_change={len(targets)}",
        flush=True,
    )

    ok = []
    failed = []
    for index, symbol in enumerate(targets, 1):
        try:
            data = change_one(session, secret, symbol, leverage)
            ok.append(symbol)
            print(
                f"OK {index}/{len(targets)} {symbol} "
                f"{current[symbol]}x->{data.get('leverage')}x "
                f"maxNotional={data.get('maxNotionalValue')}",
                flush=True,
            )
        except RuntimeError as exc:
            msg = str(exc)
            failed.append((symbol, msg))
            print(f"FAIL {index}/{len(targets)} {symbol} {msg}", flush=True)
            if msg.startswith("RATE_LIMIT"):
                print("STOP_ON_RATE_LIMIT", flush=True)
                break
        time.sleep(REQUEST_DELAY_SEC)

    skipped = len(current) - len(targets) if target.lower() == "all" else int(not targets)
    print(
        f"SUMMARY changed={len(ok)} failed={len(failed)} "
        f"skipped_already_target={skipped}",
        flush=True,
    )
    if failed:
        print(f"FAILED_SYMBOLS {failed}", flush=True)


if __name__ == "__main__":
    main()
