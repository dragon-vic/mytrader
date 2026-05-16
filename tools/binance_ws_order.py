from __future__ import annotations

import argparse
import asyncio
import hmac
import hashlib
import json
import os
import time
import urllib.parse
import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import aiohttp
import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
LIVE_REST = "https://fapi.binance.com"
LIVE_WS = "wss://ws-fapi.binance.com/ws-fapi/v1"
TESTNET_REST = "https://testnet.binancefuture.com"
TESTNET_WS = "wss://testnet.binancefuture.com/ws-fapi/v1"


# 生成 Binance HMAC 签名，签名前参数按 key 排序。
def sign(params: dict[str, Any], secret: str) -> str:
    query = urllib.parse.urlencode(sorted(params.items()))
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


# 用 Binance serverTime 校准本机时间。
def server_offset(rest_url: str) -> int:
    response = requests.get(f"{rest_url}/fapi/v1/time", timeout=10)
    response.raise_for_status()
    server_ms = int(response.json()["serverTime"])
    return server_ms - int(time.time() * 1000)


def local_ms() -> int:
    return int(time.time() * 1000)


def bj(ms: int | str) -> str:
    ts = int(ms) / 1000
    zone = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(zone).isoformat(timespec="milliseconds")


def env_urls(environment: str) -> tuple[str, str]:
    if environment == "LIVE":
        return LIVE_REST, LIVE_WS
    if environment == "TESTNET":
        return TESTNET_REST, TESTNET_WS
    raise ValueError(f"unsupported environment: {environment}")


def order_params(args: argparse.Namespace, api_key: str, offset_ms: int) -> dict[str, Any]:
    params: dict[str, Any] = {
        "apiKey": api_key,
        "symbol": args.symbol,
        "side": args.side,
        "type": args.order_type,
        "quantity": args.quantity,
        "newClientOrderId": args.client_order_id,
        "newOrderRespType": args.response_type,
        "recvWindow": args.recv_window,
        "timestamp": local_ms() + offset_ms,
    }
    if args.reduce_only:
        params["reduceOnly"] = "true"
    if args.position_side:
        params["positionSide"] = args.position_side
    if args.order_type == "LIMIT":
        params["price"] = args.price
        params["timeInForce"] = args.time_in_force
    return params


def check_args(args: argparse.Namespace) -> None:
    if args.order_type == "LIMIT":
        if args.price is None:
            raise ValueError("--price is required for LIMIT")
        if args.time_in_force is None:
            raise ValueError("--time-in-force is required for LIMIT")
    if args.order_type == "MARKET" and (args.price is not None or args.time_in_force is not None):
        raise ValueError("MARKET does not accept --price or --time-in-force")
    if not args.yes:
        raise RuntimeError("refusing to send live order without --yes")


async def place_ws(args: argparse.Namespace) -> dict[str, Any]:
    load_dotenv(ROOT / ".env")
    check_args(args)
    api_key = os.environ[args.api_key_env]
    api_secret = os.environ[args.api_secret_env]
    rest_url, ws_url = env_urls(args.environment)
    offset_ms = server_offset(rest_url)
    params = order_params(args, api_key, offset_ms)
    params["signature"] = sign(params, api_secret)
    request = {
        "id": str(uuid.uuid4()),
        "method": "order.place",
        "params": params,
    }

    send_ms = local_ms()
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url, heartbeat=20) as ws:
            await ws.send_str(json.dumps(request, separators=(",", ":")))
            message = await ws.receive(timeout=10)
    recv_ms = local_ms()

    if message.type != aiohttp.WSMsgType.TEXT:
        raise RuntimeError(f"unexpected websocket message type: {message.type}")

    response = json.loads(message.data)
    result = response.get("result") or {}
    order_id = result.get("orderId")
    queried = query_order(rest_url, api_key, api_secret, args.symbol, args.client_order_id, offset_ms)
    return {
        "environment": args.environment,
        "symbol": args.symbol,
        "client_order_id": args.client_order_id,
        "local_send_ms": send_ms,
        "local_recv_ms": recv_ms,
        "local_rtt_ms": recv_ms - send_ms,
        "server_offset_ms": offset_ms,
        "ws_status": response.get("status"),
        "ws_error": response.get("error"),
        "ws_order_id": order_id,
        "ws_result": result,
        "query_order": queried,
    }


def query_order(
    rest_url: str,
    api_key: str,
    api_secret: str,
    symbol: str,
    client_order_id: str,
    offset_ms: int,
) -> dict[str, Any]:
    params = {
        "symbol": symbol,
        "origClientOrderId": client_order_id,
        "recvWindow": 10_000,
        "timestamp": local_ms() + offset_ms,
    }
    params["signature"] = sign(params, api_secret)
    response = requests.get(
        f"{rest_url}/fapi/v1/order",
        headers={"X-MBX-APIKEY": api_key},
        params=params,
        timeout=10,
    )
    data = response.json()
    if response.status_code != 200:
        return {"status_code": response.status_code, "body": data}
    return {
        "status_code": response.status_code,
        "orderId": data.get("orderId"),
        "status": data.get("status"),
        "type": data.get("type"),
        "side": data.get("side"),
        "avgPrice": data.get("avgPrice"),
        "origQty": data.get("origQty"),
        "executedQty": data.get("executedQty"),
        "time": data.get("time"),
        "time_bj": bj(data["time"]) if data.get("time") else "",
        "updateTime": data.get("updateTime"),
        "update_time_bj": bj(data["updateTime"]) if data.get("updateTime") else "",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Place one Binance USD-M Futures order through WS API.")
    parser.add_argument("--environment", choices=["LIVE", "TESTNET"], required=True)
    parser.add_argument("--api-key-env", required=True)
    parser.add_argument("--api-secret-env", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", choices=["BUY", "SELL"], required=True)
    parser.add_argument("--order-type", choices=["MARKET", "LIMIT"], required=True)
    parser.add_argument("--quantity", required=True)
    parser.add_argument("--client-order-id", required=True)
    parser.add_argument("--response-type", choices=["ACK", "RESULT"], required=True)
    parser.add_argument("--recv-window", type=int, required=True)
    parser.add_argument("--price")
    parser.add_argument("--time-in-force", choices=["GTC", "IOC", "FOK", "GTX"])
    parser.add_argument("--position-side", choices=["BOTH", "LONG", "SHORT"])
    parser.add_argument("--reduce-only", action="store_true")
    parser.add_argument("--yes", action="store_true")
    return parser.parse_args()


def main() -> None:
    result = asyncio.run(place_ws(parse_args()))
    queried = result["query_order"]
    print(f"ws_status {result['ws_status']}")
    print(f"local_rtt_ms {result['local_rtt_ms']}")
    print(f"server_offset_ms {result['server_offset_ms']}")
    print(f"order_id {queried.get('orderId')}")
    print(f"order_status {queried.get('status')}")
    print(f"order_time {queried.get('time')} {queried.get('time_bj')}")
    print(f"order_update_time {queried.get('updateTime')} {queried.get('update_time_bj')}")
    if result["ws_error"]:
        print("ws_error")
        print(json.dumps(result["ws_error"], ensure_ascii=False, indent=2))
    print("ws_result")
    print(json.dumps(result["ws_result"], ensure_ascii=False, indent=2))
    if queried.get("status_code") != 200:
        print("query_order")
        print(json.dumps(queried, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
