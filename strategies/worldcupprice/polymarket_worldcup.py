from __future__ import annotations

import json
import re
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

import requests


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
MATCH_SLUG_RE = re.compile(r"^fifwc-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}-\d{2}$")
REQUEST_TIMEOUT_SECONDS = 15
PAGE_LIMIT = 100
MAX_PAGES = 8
LIVE_MATCH_PAD = timedelta(hours=2, minutes=15)
MATCH_END_PAD = timedelta(hours=4)


# 返回下一场世界杯 full-time result event slug，供 Polymarket provider 加载 instrument。
def build_event_slugs() -> list[str]:
    return [next_match_event()["slug"]]


# 解析下一场世界杯三个 Yes token 的 InstrumentId 和比赛元数据。
def next_match_yes_windows(proxy_url: str | None = None) -> dict[str, dict[str, int | str]]:
    event = next_match_event(proxy_url)
    start = parse_time(event["startTime"])
    end = start + MATCH_END_PAD
    windows: dict[str, dict[str, int | str]] = {}

    for market in event.get("markets", []):
        condition_id = market.get("conditionId")
        yes_token = yes_token_id(market)
        if not condition_id or not yes_token:
            continue
        instrument_id = f"{condition_id}-{yes_token}.POLYMARKET"
        windows[instrument_id] = {
            "event_slug": event["slug"],
            "event_title": event["title"],
            "market_slug": market.get("slug", ""),
            "market_question": market.get("question", ""),
            "label": yes_label(market),
            "condition_id": condition_id,
            "token_id": yes_token,
            "event_start_ns": int(start.timestamp() * 1_000_000_000),
            "event_end_ns": int(end.timestamp() * 1_000_000_000),
        }

    if len(windows) != 3:
        raise ValueError(f"expected 3 World Cup YES markets, got {len(windows)} for {event['slug']}")
    return windows


def next_match_event(proxy_url: str | None = None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    events = [
        event
        for event in fetch_events(proxy_url)
        if is_match_event(event)
        and event.get("active") is True
        and event.get("closed") is False
        and parse_time(event["startTime"]) + LIVE_MATCH_PAD >= now
    ]
    if not events:
        raise RuntimeError("no active upcoming FIFA World Cup match event found on Polymarket Gamma")
    events.sort(key=lambda event: parse_time(event["startTime"]))
    return events[0]


def fetch_events(proxy_url: str | None = None) -> list[dict[str, Any]]:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})

    events: list[dict[str, Any]] = []
    for page in range(MAX_PAGES):
        response = session.get(
            GAMMA_EVENTS_URL,
            params={
                "limit": PAGE_LIMIT,
                "offset": page * PAGE_LIMIT,
                "tag_slug": "fifa-world-cup",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        events.extend(data)
        time.sleep(0.05)
    return events


def is_match_event(event: dict[str, Any]) -> bool:
    slug = str(event.get("slug", ""))
    return bool(MATCH_SLUG_RE.match(slug)) and event.get("startTime")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def yes_token_id(market: dict[str, Any]) -> str:
    for outcome, token_id in zip(parse_list(market.get("outcomes")), parse_list(market.get("clobTokenIds")), strict=False):
        if str(outcome).lower() == "yes":
            return str(token_id)
    return ""


def yes_label(market: dict[str, Any]) -> str:
    slug = str(market.get("slug", ""))
    if slug.endswith("-draw"):
        return "Draw Yes"

    question = str(market.get("question", ""))
    match = re.match(r"Will (.+?) win\b", question)
    if match:
        return f"{match.group(1)} Yes"
    return f"{slug.rsplit('-', 1)[-1].upper()} Yes"


def parse_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value:
        return []
    data = json.loads(value)
    return data if isinstance(data, list) else []
