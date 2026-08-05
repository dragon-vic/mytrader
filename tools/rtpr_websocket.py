from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import websockets
from dotenv import load_dotenv
from websockets.exceptions import ConnectionClosed


ROOT = Path(__file__).resolve().parents[1]
WS_ENDPOINT = "wss://ws.rtpr.io/ws-alerts"
INITIAL_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 60


def _api_key() -> str:
    load_dotenv(ROOT / ".env")
    value = os.environ.get("RTPR_API_KEY", "").strip()
    if not value:
        raise RuntimeError("RTPR_API_KEY is missing from .env")
    return value


def _log(message: str, **values: Any) -> None:
    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "message": message,
        **values,
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)


async def _consume_once(api_key: str) -> None:
    endpoint = f"{WS_ENDPOINT}?apiKey={api_key}"
    async with websockets.connect(
        endpoint,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
    ) as websocket:
        _log("connected")
        async for raw in websocket:
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                _log("ignored_non_json_message")
                continue

            if message.get("type") == "ping":
                await websocket.send(json.dumps({"type": "pong"}))
                continue

            if message.get("type") == "alert":
                _log(
                    "alert",
                    ticker=message.get("ticker"),
                    article_published_at=message.get("article_published_at"),
                    article_url=message.get("article_url"),
                    alert_kind=message.get("alert_kind", "rule_match"),
                    rules=message.get("rules"),
                    impact_score=message.get("impact_score"),
                    impact_direction=message.get("impact_direction"),
                )
                continue

            _log("message", payload=message)


async def run() -> None:
    api_key = _api_key()
    backoff = INITIAL_BACKOFF_SECONDS
    while True:
        try:
            await _consume_once(api_key)
            backoff = INITIAL_BACKOFF_SECONDS
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, OSError, TimeoutError) as exc:
            _log(
                "connection_failed",
                error_type=type(exc).__name__,
                error=str(exc),
                retry_in_seconds=backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        _log("stopped")
