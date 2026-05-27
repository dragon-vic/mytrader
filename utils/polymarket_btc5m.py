from __future__ import annotations

import time
from datetime import datetime
from datetime import timezone
from typing import Any

import requests


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
SLUG_PREFIX = "btc-updown-5m"
WINDOW_SECONDS = 300
PAST_WINDOWS = 2
FUTURE_HOURS = 12
OUTCOMES = ("Up", "Down")
REQUEST_TIMEOUT_SECONDS = 15
SSL_RETRIES = 2


# 返回当前到未来一段时间的 BTC 5m event slug。
def build_event_slugs() -> list[str]:
    now = int(datetime.now(timezone.utc).timestamp())
    current = now // WINDOW_SECONDS * WINDOW_SECONDS
    future_windows = FUTURE_HOURS * 3600 // WINDOW_SECONDS
    starts = range(current - PAST_WINDOWS * WINDOW_SECONDS, current + future_windows * WINDOW_SECONDS + 1, WINDOW_SECONDS)
    return [f"{SLUG_PREFIX}-{start}" for start in starts]


# 解析 Gamma event 里的 Up/Down token 和 5m 窗口时间。
def up_down_instrument_windows(proxy_url: str | None = None) -> dict[str, dict[str, int | str]]:
    session = requests.Session()
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})

    windows: dict[str, dict[str, int | str]] = {}
    for slug in build_event_slugs():
        start = slug_start(slug)
        end = start + WINDOW_SECONDS
        event = fetch_event(session, slug)
        if event is None:
            continue
        for market in event.get("markets", []):
            condition_id = market.get("conditionId")
            for outcome, token_id in outcome_token_ids(market).items():
                if not condition_id or not token_id:
                    continue
                instrument_id = f"{condition_id}-{token_id}.POLYMARKET"
                windows[instrument_id] = {
                    "slug": slug,
                    "outcome": outcome,
                    "event_start_ns": start * 1_000_000_000,
                    "event_end_ns": end * 1_000_000_000,
                }
    return windows


def up_instrument_windows(proxy_url: str | None = None) -> dict[str, dict[str, int | str]]:
    return {
        instrument_id: window
        for instrument_id, window in up_down_instrument_windows(proxy_url).items()
        if str(window["outcome"]).lower() == "up"
    }


# BTC 5m slug 末尾就是窗口开始秒级时间戳。
def slug_start(slug: str) -> int:
    return int(slug.rsplit("-", 1)[1])


# 读取单个 Gamma event，SSL EOF 时短重试。
def fetch_event(session: requests.Session, slug: str) -> dict[str, Any] | None:
    for attempt in range(SSL_RETRIES + 1):
        try:
            response = session.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()
            return data[0] if data else None
        except requests.exceptions.SSLError:
            if attempt == SSL_RETRIES:
                raise
            time.sleep(attempt + 1)
    return None


# Gamma market 中 outcomes 和 clobTokenIds 是 JSON 字符串或列表。
def outcome_token_ids(market: dict[str, Any]) -> dict[str, str]:
    outcomes = parse_list(market.get("outcomes"))
    token_ids = parse_list(market.get("clobTokenIds"))
    tokens: dict[str, str] = {}
    for outcome, token_id in zip(outcomes, token_ids, strict=False):
        text = str(outcome)
        if text in OUTCOMES:
            tokens[text] = str(token_id)
    return tokens


def parse_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value:
        return []

    import json

    data = json.loads(value)
    return data if isinstance(data, list) else []
