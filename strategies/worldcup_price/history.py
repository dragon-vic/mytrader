from __future__ import annotations

import time
from datetime import datetime
from datetime import timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests


POLY_TRADES_URL = "https://data-api.polymarket.com/trades"
ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard"
ESPN_SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/summary"
REQUEST_TIMEOUT_SECONDS = 20
LOCAL_TZ = ZoneInfo("Asia/Shanghai")


# 拉取 Polymarket public data API 上能拿到的全市场成交，再过滤成三个 Yes token。
def fetch_trades(
    targets: dict[str, dict[str, int | str]],
    since_ns: int,
    limit: int,
    max_pages: int,
    pause_ms: int,
    proxy_url: str,
) -> list[dict[str, object]]:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})

    rows: list[dict[str, object]] = []
    for instrument_id, target in targets.items():
        rows.extend(fetch_market(session, instrument_id, target, since_ns, limit, max_pages, pause_ms))
    rows.sort(key=lambda row: (int(row["ts_event_ns"]), str(row["instrument_id"]), str(row["trade_id"])))
    return rows


def fetch_market(
    session: requests.Session,
    instrument_id: str,
    target: dict[str, int | str],
    since_ns: int,
    limit: int,
    max_pages: int,
    pause_ms: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    condition_id = str(target["condition_id"])
    token_id = str(target["token_id"])
    for page in range(max_pages):
        response = session.get(
            POLY_TRADES_URL,
            params={
                "market": condition_id,
                "limit": limit,
                "offset": page * limit,
                "takerOnly": "false",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code >= 400 and page > 0:
            break
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        oldest_ns = None
        for trade in data:
            if str(trade.get("asset")) != token_id:
                continue
            timestamp_ns = int(float(trade["timestamp"]) * 1_000_000_000)
            oldest_ns = timestamp_ns if oldest_ns is None else min(oldest_ns, timestamp_ns)
            if timestamp_ns <= since_ns:
                continue
            trade_id = str(trade.get("transactionHash") or f"{timestamp_ns}-{trade.get('price')}-{trade.get('size')}")
            key = f"{instrument_id}:{trade_id}"
            if key in seen:
                continue
            seen.add(key)
            rows.append(public_row(instrument_id, target, trade, timestamp_ns, trade_id))
        if oldest_ns is not None and oldest_ns <= since_ns:
            break
        if len(data) < limit:
            break
        if pause_ms > 0:
            time.sleep(pause_ms / 1000)
    return rows


def public_row(
    instrument_id: str,
    target: dict[str, int | str],
    trade: dict[str, Any],
    timestamp_ns: int,
    trade_id: str,
) -> dict[str, object]:
    return {
        "event_slug": target["event_slug"],
        "event_title": target["event_title"],
        "market_slug": target["market_slug"],
        "market_question": target["market_question"],
        "label": target["label"],
        "instrument_id": instrument_id,
        "condition_id": target["condition_id"],
        "token_id": target["token_id"],
        "record_type": "trade",
        "ts_event_ns": timestamp_ns,
        "time": local_time(timestamp_ns),
        "price": float(trade["price"]),
        "size": float(trade["size"]),
        "side": side_label(trade.get("side")),
        "bid_price": None,
        "bid_size": None,
        "ask_price": None,
        "ask_size": None,
        "source": "public",
        "trade_id": trade_id,
    }


# 从 ESPN summary 拉比赛关键事件，返回 wallclock 对齐后的时间点。
def fetch_events(
    event_title: str,
    event_start_ns: int,
    proxy_url: str,
) -> list[dict[str, object]]:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})
    event_id = espn_event_id(session, event_title, event_start_ns)
    if not event_id:
        return []
    response = session.get(ESPN_SUMMARY_URL, params={"event": event_id}, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    summary = response.json()
    events: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in summary.get("commentary", []):
        play = item.get("play") or {}
        kind = event_kind(play, item)
        if not kind:
            continue
        timestamp_ns = event_time_ns(play, item, event_start_ns)
        text = str(item.get("text") or play.get("text") or "")
        key = f"{timestamp_ns}:{kind}:{text}"
        if key in seen:
            continue
        seen.add(key)
        events.append({
            "ts_event_ns": timestamp_ns,
            "minute": str((item.get("time") or {}).get("displayValue") or (play.get("clock") or {}).get("displayValue") or ""),
            "kind": kind,
            "team": str((play.get("team") or {}).get("displayName") or ""),
            "text": text,
        })
    events.sort(key=lambda event: int(event["ts_event_ns"]))
    return events


def espn_event_id(session: requests.Session, event_title: str, event_start_ns: int) -> str:
    date = datetime.fromtimestamp(event_start_ns / 1_000_000_000, tz=timezone.utc).strftime("%Y%m%d")
    response = session.get(ESPN_SCOREBOARD_URL, params={"dates": date, "limit": 100}, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    names = set(normalized_title(event_title).split())
    best_id = ""
    best_score = 0
    for event in response.json().get("events", []):
        candidate = normalized_title(str(event.get("name") or event.get("shortName") or ""))
        score = len(names & set(candidate.split()))
        if score > best_score:
            best_id = str(event.get("id") or "")
            best_score = score
    return best_id if best_score >= 2 else ""


def normalized_title(value: str) -> str:
    return (
        value.lower()
        .replace(" vs. ", " ")
        .replace(" at ", " ")
        .replace(".", "")
        .replace("-", " ")
    )


def event_kind(play: dict[str, Any], item: dict[str, Any]) -> str:
    raw = str((play.get("type") or {}).get("type") or (play.get("type") or {}).get("text") or item.get("text") or "").lower()
    text = str(item.get("text") or play.get("text") or "").lower()
    if raw == "goal" or text.startswith("goal!"):
        return "goal"
    if "shot" in raw or "shot" in text:
        return "shot"
    if "penalty" in raw or "penalty" in text:
        return "penalty"
    if "yellow" in raw or "red card" in text or "yellow card" in text:
        return "card"
    if "halftime" in text or "half ends" in text:
        return "half"
    if "kickoff" in raw or "begins" in text:
        return "start"
    if "substitution" in raw or "substitution" in text:
        return "sub"
    if "foul" in raw:
        return "foul"
    if "corner" in raw or "corner" in text:
        return "corner"
    return ""


def event_time_ns(play: dict[str, Any], item: dict[str, Any], event_start_ns: int) -> int:
    wallclock = play.get("wallclock")
    if wallclock:
        return int(datetime.fromisoformat(str(wallclock).replace("Z", "+00:00")).timestamp() * 1_000_000_000)
    seconds = float((item.get("time") or {}).get("value") or (play.get("clock") or {}).get("value") or 0.0)
    return event_start_ns + int(seconds * 1_000_000_000)


def local_time(timestamp_ns: int) -> str:
    return datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc).astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")


def side_label(side: object) -> str:
    text = str(side).upper()
    if "BUY" in text:
        return "买"
    if "SELL" in text:
        return "卖"
    return "未知"
