# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
import websocket
from dotenv import load_dotenv


GAMMA_HOST = "https://gamma-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DEFAULT_EVENT_SLUG = "what-price-will-hyperliquid-hit-in-may-576"
DEFAULT_MARKET_SLUG = "will-hyperliquid-reach-52-in-may"

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def log(message: str) -> None:
    print(message, flush=True)


def now_utc_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def request_json(url: str, params: dict[str, Any] | None = None) -> Any:
    for i in range(3):
        try:
            resp = requests.get(url, params=params, timeout=20)

            if resp.status_code == 429:
                wait_seconds = 1 + i
                log(f"请求过快，等待 {wait_seconds} 秒后重试：{url}")
                time.sleep(wait_seconds)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as e:
            if i == 2:
                raise

            wait_seconds = 1 + i
            log(f"请求失败，等待 {wait_seconds} 秒后重试：{type(e).__name__}: {e}")
            time.sleep(wait_seconds)

    raise RuntimeError("请求失败")


def parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []

        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except json.JSONDecodeError:
            return [value]

    return [value]


def get_event(event_slug: str) -> dict[str, Any]:
    log(f"正在读取 event：{event_slug}")

    data = request_json(f"{GAMMA_HOST}/events", {"slug": event_slug})

    if not isinstance(data, list) or not data:
        raise RuntimeError(f"没有找到 event：{event_slug}")

    event = data[0]

    log(f"已找到 event：{event.get('title')}")
    log(f"event_id：{event.get('id')}")
    log(f"event_slug：{event.get('slug')}")

    return event


def get_target_market(event: dict[str, Any], market_slug: str) -> dict[str, Any]:
    markets = event.get("markets") or []

    if not isinstance(markets, list):
        raise RuntimeError("event 中 markets 不是列表")

    for market in markets:
        if market.get("slug") == market_slug:
            log(f"已找到目标 market：{market.get('question') or market.get('title')}")
            log(f"market_slug：{market.get('slug')}")
            log(f"condition_id：{market.get('conditionId')}")
            return market

    log("没有找到指定 market。当前 event 下可用 market：")
    for market in markets:
        log(f"- {market.get('slug')} | {market.get('question') or market.get('title')}")

    raise RuntimeError(f"没有找到 market：{market_slug}")


def extract_tokens(market: dict[str, Any]) -> list[dict[str, Any]]:
    outcomes = parse_json_list(market.get("outcomes"))

    token_ids = parse_json_list(
        market.get("clobTokenIds")
        or market.get("clobTokenIDs")
        or market.get("clob_token_ids")
    )

    prices = parse_json_list(market.get("outcomePrices"))

    rows: list[dict[str, Any]] = []

    for i, token_id in enumerate(token_ids):
        rows.append(
            {
                "token_id": str(token_id),
                "outcome": outcomes[i] if i < len(outcomes) else "",
                "price": prices[i] if i < len(prices) else "",
            }
        )

    if not rows:
        raise RuntimeError("这个 market 没有解析出 clobTokenIds")

    log("已解析订阅 token：")
    for row in rows:
        log(f"- outcome={row['outcome']} token_id={row['token_id']} 当前价格={row['price']}")

    return rows


def build_token_map(tokens: list[dict[str, Any]]) -> dict[str, str]:
    token_map: dict[str, str] = {}

    for row in tokens:
        token_map[str(row["token_id"])] = str(row.get("outcome") or "")

    return token_map


def get_event_type(data: dict[str, Any]) -> str:
    return str(data.get("event_type") or data.get("eventType") or data.get("type") or "")


def get_asset_id(data: dict[str, Any]) -> str:
    return str(
        data.get("asset_id")
        or data.get("asset")
        or data.get("token_id")
        or ""
    )


def print_trade_tick(data: dict[str, Any], token_map: dict[str, str]) -> None:
    asset_id = get_asset_id(data)
    outcome = token_map.get(asset_id, "")

    log(
        f"[{now_utc_text()}] 成交tick "
        f"outcome={outcome} "
        f"token={asset_id} "
        f"side={data.get('side')} "
        f"price={data.get('price')} "
        f"size={data.get('size')} "
        f"fee_rate_bps={data.get('fee_rate_bps')}"
    )


def print_book_event(data: dict[str, Any], token_map: dict[str, str]) -> None:
    asset_id = get_asset_id(data)
    outcome = token_map.get(asset_id, "")

    bids = data.get("bids") or []
    asks = data.get("asks") or []

    best_bid = bids[0] if bids else None
    best_ask = asks[0] if asks else None

    log(
        f"[{now_utc_text()}] 盘口快照 "
        f"outcome={outcome} "
        f"token={asset_id} "
        f"best_bid={best_bid} "
        f"best_ask={best_ask} "
        f"bids={len(bids)} "
        f"asks={len(asks)}"
    )


def print_price_change_event(data: dict[str, Any], token_map: dict[str, str]) -> None:
    changes = data.get("price_changes") or []

    if not isinstance(changes, list):
        changes = [changes]

    for change in changes:
        if not isinstance(change, dict):
            log(f"[{now_utc_text()}] 价格变动 原始数据={change}")
            continue

        asset_id = str(
            change.get("asset_id")
            or change.get("asset")
            or data.get("asset_id")
            or data.get("asset")
            or ""
        )

        outcome = token_map.get(asset_id, "")

        log(
            f"[{now_utc_text()}] 价格变动 "
            f"outcome={outcome} "
            f"token={asset_id} "
            f"side={change.get('side')} "
            f"price={change.get('price')} "
            f"size={change.get('size')} "
            f"best_bid={change.get('best_bid')} "
            f"best_ask={change.get('best_ask')}"
        )


def print_raw_message(data: Any) -> None:
    text = json.dumps(data, ensure_ascii=False)
    if len(text) > 1200:
        text = text[:1200] + "...(已截断)"
    log(f"[{now_utc_text()}] 原始消息：{text}")


def handle_message(raw_message: str, token_map: dict[str, str], verbose: bool) -> None:
    try:
        data = json.loads(raw_message)
    except json.JSONDecodeError:
        if verbose:
            log(f"[{now_utc_text()}] 非JSON消息：{raw_message}")
        return

    if isinstance(data, list):
        for item in data:
            handle_message(json.dumps(item, ensure_ascii=False), token_map, verbose)
        return

    if not isinstance(data, dict):
        if verbose:
            print_raw_message(data)
        return

    event_type = get_event_type(data)

    if event_type == "last_trade_price":
        print_trade_tick(data, token_map)
        return

    if not verbose:
        return

    if event_type == "book":
        print_book_event(data, token_map)
    elif event_type == "price_change":
        print_price_change_event(data, token_map)
    elif event_type == "tick_size_change":
        log(f"[{now_utc_text()}] tick_size变化：{json.dumps(data, ensure_ascii=False)}")
    else:
        print_raw_message(data)


def parse_proxy_url(proxy_url: str) -> dict[str, Any]:
    parsed = urlparse(proxy_url)

    if not parsed.hostname or not parsed.port:
        raise RuntimeError(f"代理地址无法解析：{proxy_url}")

    proxy_type = parsed.scheme.lower()

    if proxy_type in {"http", "https"}:
        ws_proxy_type = "http"
    elif proxy_type in {"socks5", "socks5h"}:
        ws_proxy_type = "socks5"
    elif proxy_type in {"socks4", "socks4a"}:
        ws_proxy_type = "socks4"
    else:
        raise RuntimeError(f"暂不支持的代理类型：{proxy_type}")

    options: dict[str, Any] = {
        "proxy_type": ws_proxy_type,
        "http_proxy_host": parsed.hostname,
        "http_proxy_port": parsed.port,
    }

    if parsed.username:
        options["http_proxy_auth"] = (
            parsed.username,
            parsed.password or "",
        )

    return options


def get_proxy_options(cli_proxy: str | None) -> dict[str, Any]:
    proxy_url = (
        cli_proxy
        or os.getenv("WS_PROXY")
        or os.getenv("HTTPS_PROXY")
        or os.getenv("HTTP_PROXY")
        or ""
    ).strip()

    if not proxy_url:
        log("未使用代理")
        return {}

    log(f"使用 WebSocket 代理：{proxy_url}")
    return parse_proxy_url(proxy_url)


def connect_once(
    token_ids: list[str],
    token_map: dict[str, str],
    proxy_options: dict[str, Any],
    verbose: bool,
) -> None:
    subscribe_message = {
        "type": "market",
        "assets_ids": token_ids,
    }

    log("正在连接 WebSocket")

    ws = websocket.create_connection(
        WS_URL,
        timeout=20,
        enable_multithread=True,
        **proxy_options,
    )

    try:
        log("WebSocket 已连接")

        ws.send(json.dumps(subscribe_message))
        log(f"订阅请求已发送：{json.dumps(subscribe_message, ensure_ascii=False)}")
        log("开始监听。默认只打印成交tick；如果长时间没有输出，说明这段时间没有成交。")

        last_ping_time = time.time()

        while True:
            if time.time() - last_ping_time >= 15:
                ws.ping()
                last_ping_time = time.time()

            raw_message = ws.recv()

            if raw_message is None:
                raise RuntimeError("收到空消息")

            handle_message(raw_message, token_map, verbose)

    finally:
        try:
            ws.close()
        except Exception:
            pass


def run_ws(tokens: list[dict[str, Any]], proxy: str | None, verbose: bool) -> None:
    token_ids = [str(row["token_id"]) for row in tokens]
    token_map = build_token_map(tokens)

    log("准备连接 Polymarket market WebSocket")
    log(f"订阅 token 数量：{len(token_ids)}")
    for token_id in token_ids:
        log(f"- token={token_id} outcome={token_map.get(token_id, '')}")

    proxy_options = get_proxy_options(proxy)

    while True:
        try:
            connect_once(
                token_ids=token_ids,
                token_map=token_map,
                proxy_options=proxy_options,
                verbose=verbose,
            )

        except KeyboardInterrupt:
            log("用户中断，程序退出")
            return

        except Exception as e:
            log(f"WebSocket 断开或异常：{type(e).__name__}: {e}")
            log("3 秒后重连")
            time.sleep(3)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-slug", default=DEFAULT_EVENT_SLUG)
    parser.add_argument("--market-slug", default=DEFAULT_MARKET_SLUG)
    parser.add_argument("--proxy", default=None, help="例如 http://127.0.0.1:7890")
    parser.add_argument("--verbose", action="store_true", help="打印盘口、价格变动和原始消息")
    args = parser.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")

    log("开始订阅 Polymarket 实时 tick")
    log(f"event_slug：{args.event_slug}")
    log(f"market_slug：{args.market_slug}")

    event = get_event(args.event_slug)
    market = get_target_market(event, args.market_slug)
    tokens = extract_tokens(market)

    run_ws(
        tokens=tokens,
        proxy=args.proxy,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()