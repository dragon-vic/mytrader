from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

from strategies.agent_trading.watch.watch_data_models import WatchPlan


SESSIONS = {"BMO", "AMC"}
VENUES = {"BINANCE", "HYPERLIQUID"}
RESEARCH_MINUTES_PER_EVENT = 40
TRADE_SIGNALS = {
    "STRONG_BUY",
    "MEDIUM_BUY",
    "WEAK_BUY",
    "WEAK_SELL",
    "MEDIUM_SELL",
    "STRONG_SELL",
}


@dataclass(frozen=True)
class EventSpec:
    event_id: str
    company: str
    ticker: str
    scope: str
    research_hints: tuple[str, ...]
    watch_plan: WatchPlan


@dataclass(frozen=True)
class BatchPlan:
    batch_id: str
    session: str
    watch_start_at: datetime
    watch_end_at: datetime
    events: tuple[EventSpec, ...]

    @property
    def research_duration(self) -> timedelta:
        return timedelta(minutes=RESEARCH_MINUTES_PER_EVENT * len(self.events))

    @property
    def research_start_at(self) -> datetime:
        return self.watch_start_at - self.research_duration

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BatchPlan:
        _keys(
            payload,
            {"batch_id", "session", "watch_start_at", "watch_end_at", "events"},
            "batch",
        )
        watch_start_at = _time(payload["watch_start_at"], "watch_start_at")
        watch_end_at = _time(payload["watch_end_at"], "watch_end_at")
        events = tuple(
            _event(_dict(item, "events[]"), watch_start_at, watch_end_at)
            for item in _list(payload["events"], "events")
        )
        plan = cls(
            batch_id=_text(payload["batch_id"], "batch_id"),
            session=_text(payload["session"], "session"),
            watch_start_at=watch_start_at,
            watch_end_at=watch_end_at,
            events=events,
        )
        plan._validate()
        return plan

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "session": self.session,
            "watch_start_at": self.watch_start_at.isoformat(),
            "watch_end_at": self.watch_end_at.isoformat(),
            "events": [
                {
                    "event_id": event.event_id,
                    "company": event.company,
                    "ticker": event.ticker,
                    "scope": event.scope,
                    "research_hints": list(event.research_hints),
                    "watch": event.watch_plan.to_watch_dict(),
                }
                for event in self.events
            ],
        }

    def _validate(self) -> None:
        if self.session not in SESSIONS:
            raise ValueError(f"unsupported earnings session: {self.session}")
        if self.watch_end_at <= self.watch_start_at:
            raise ValueError("watch_end_at must be later than watch_start_at")
        if not self.events:
            raise ValueError("batch events must not be empty")
        event_ids = [event.event_id for event in self.events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("batch event_id values must be unique")


# 静态计划由人工按周或按月维护，运行时不再调用规划 Agent。
def load_schedule(path: Path) -> tuple[BatchPlan, ...]:
    payload = _dict(json.loads(path.read_text(encoding="utf-8")), "schedule")
    _keys(payload, {"schedule_id", "timezone", "active_scope", "batches"}, "schedule")
    _text(payload["schedule_id"], "schedule_id")
    active_scope = _text(payload["active_scope"], "active_scope")
    if payload["timezone"] != "UTC":
        raise ValueError("schedule timezone must be UTC")

    all_batches = tuple(
        BatchPlan.from_dict(_dict(item, "batches[]"))
        for item in _list(payload["batches"], "batches")
    )
    if not all_batches:
        raise ValueError("schedule batches must not be empty")

    batch_ids = [batch.batch_id for batch in all_batches]
    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("schedule batch_id values must be unique")
    event_ids = [event.event_id for batch in all_batches for event in batch.events]
    if len(event_ids) != len(set(event_ids)):
        raise ValueError("schedule event_id values must be unique")
    if list(all_batches) != sorted(all_batches, key=lambda batch: batch.watch_start_at):
        raise ValueError("schedule batches must be ordered by watch_start_at")

    # 当前只把计划中明确标记的事件交给预研和 watcher。
    selected: list[BatchPlan] = []
    for batch in all_batches:
        events = tuple(event for event in batch.events if event.scope == active_scope)
        if events:
            selected.append(replace(batch, events=events))
    batches = tuple(selected)
    if not batches:
        raise ValueError(f"schedule has no events for active_scope: {active_scope}")
    return batches


@dataclass(frozen=True)
class MarketInstrument:
    symbol: str
    instrument_id: str
    venue: str
    market_symbol: str
    is_index: bool


@dataclass(frozen=True)
class MarketUniverse:
    as_of: datetime
    instruments: tuple[MarketInstrument, ...]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> MarketUniverse:
        _keys(payload, {"as_of", "instruments"}, "market_universe")
        parsed_instruments = tuple(
            _instrument(_dict(item, "instruments[]"))
            for item in _list(payload["instruments"], "instruments")
        )
        if not parsed_instruments:
            raise ValueError("market universe must not be empty")
        ids = [item.instrument_id for item in parsed_instruments]
        if len(ids) != len(set(ids)):
            raise ValueError("market instrument_id values must be unique")
        instruments = tuple(item for item in parsed_instruments if not item.is_index)
        if not instruments:
            raise ValueError("market universe has no eligible non-index instruments")
        return cls(
            as_of=_time(payload["as_of"], "market_universe.as_of"),
            instruments=instruments,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "instruments": [
                {
                    "symbol": item.symbol,
                    "instrument_id": item.instrument_id,
                    "venue": item.venue,
                    "market_symbol": item.market_symbol,
                    "is_index": False,
                }
                for item in self.instruments
                if not item.is_index
            ],
        }

    # 校验预研候选来自实时市场列表，并执行 Binance 优先规则。
    def validate_candidate(self, candidate: dict[str, Any]) -> None:
        symbol = candidate["symbol"]
        instrument_id = candidate["instrument_id"]
        selected = next(
            (
                item
                for item in self.instruments
                if item.instrument_id == instrument_id and not item.is_index
            ),
            None,
        )
        if selected is None or selected.symbol != symbol:
            raise ValueError(
                f"candidate is not an eligible non-index instrument: {instrument_id}",
            )
        venues = {
            item.venue
            for item in self.instruments
            if item.symbol == symbol and not item.is_index
        }
        preferred = "BINANCE" if "BINANCE" in venues else "HYPERLIQUID"
        if selected.venue != preferred:
            raise ValueError(
                f"candidate must prefer {preferred} for {symbol}: {instrument_id}",
            )

# 周度交易范围提前生成并随排期加载，所有批次复用同一份。
def load_market_universe(path: Path) -> MarketUniverse:
    payload = _dict(json.loads(path.read_text(encoding="utf-8")), "market_universe")
    return MarketUniverse.from_dict(payload)


# JSON schema 约束形状；这里只检查事件、市场和跨字段关系。
def validate_research(
    event_id: str,
    payload: dict[str, Any],
    market_universe: MarketUniverse,
) -> None:
    if payload.get("event_id") != event_id:
        raise ValueError(
            f"research event_id mismatch: {payload.get('event_id')} != {event_id}",
        )
    candidates = payload["trade_candidates"]
    instrument_ids: list[str] = []
    for candidate in candidates:
        market_universe.validate_candidate(candidate)
        moves = _moves(candidate)
        for side in ("BUY", "SELL"):
            values = [
                moves[f"{strength}_{side}"]
                for strength in ("WEAK", "MEDIUM", "STRONG")
            ]
            if values != sorted(values) or len(set(values)) != 3:
                raise ValueError(
                    f"candidate {side} impact must increase from weak to strong",
                )
        instrument_ids.append(candidate["instrument_id"])
    if len(instrument_ids) != len(set(instrument_ids)):
        raise ValueError("trade candidate instruments must be unique")


def validate_decision(
    event_id: str,
    payload: dict[str, Any],
    research: dict[str, Any],
) -> None:
    if payload.get("event_id") != event_id:
        raise ValueError(
            f"decision event_id mismatch: {payload.get('event_id')} != {event_id}",
        )
    if research.get("event_id") != event_id:
        raise ValueError(
            f"research event_id mismatch: {research.get('event_id')} != {event_id}",
        )
    trades = payload["trades"]
    if (payload.get("decision") == "HOLD") != (not trades):
        raise ValueError("HOLD must have no trades and TRADE must have trades")
    candidates: dict[str, dict[str, Any]] = {}
    for candidate in research["trade_candidates"]:
        candidates[candidate["instrument_id"]] = candidate
    selected: list[str] = []
    for trade in trades:
        instrument_id = trade["instrument_id"]
        if instrument_id not in candidates:
            raise ValueError(
                f"analysis selected an instrument absent from pre-research: {instrument_id}",
            )
        selected.append(instrument_id)
        signal = trade["signal"]
        expected_move = float(trade["expected_move_pct"])
        mapped_move = _moves(candidates[instrument_id])[signal]
        if expected_move != mapped_move:
            raise ValueError(
                "trade expected_move_pct does not match pre-research "
                f"outcomes.{signal}: {expected_move} != {mapped_move}",
            )
    if len(selected) != len(set(selected)):
        raise ValueError("decision instruments must be unique")


def _event(
    payload: dict[str, Any],
    watch_start_at: datetime,
    watch_end_at: datetime,
) -> EventSpec:
    _keys(
        payload,
        {"event_id", "company", "ticker", "scope", "research_hints", "watch"},
        "events[]",
    )
    research_hints = tuple(
        _text(item, "events[].research_hints[]")
        for item in _list(payload["research_hints"], "events[].research_hints")
    )
    if not research_hints:
        raise ValueError("events[].research_hints must not be empty")
    event_id = _text(payload["event_id"], "events[].event_id")
    return EventSpec(
        event_id=event_id,
        company=_text(payload["company"], "events[].company"),
        ticker=_text(payload["ticker"], "events[].ticker"),
        scope=_text(payload["scope"], "events[].scope"),
        research_hints=research_hints,
        watch_plan=WatchPlan.from_watch_dict(
            event_id,
            watch_start_at,
            watch_end_at,
            _dict(payload["watch"], "events[].watch"),
        ),
    )


def _instrument(payload: dict[str, Any]) -> MarketInstrument:
    _keys(
        payload,
        {"symbol", "instrument_id", "venue", "market_symbol", "is_index"},
        "instruments[]",
    )
    venue = _text(payload["venue"], "instruments[].venue")
    if venue not in VENUES:
        raise ValueError(f"unsupported market venue: {venue}")
    is_index = payload["is_index"]
    if not isinstance(is_index, bool):
        raise TypeError("instruments[].is_index must be a boolean")
    return MarketInstrument(
        symbol=_text(payload["symbol"], "instruments[].symbol"),
        instrument_id=_text(payload["instrument_id"], "instruments[].instrument_id"),
        venue=venue,
        market_symbol=_text(payload["market_symbol"], "instruments[].market_symbol"),
        is_index=is_index,
    )


def _time(value: Any, name: str) -> datetime:
    parsed = datetime.fromisoformat(_text(value, name).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include timezone")
    return parsed.astimezone(UTC)


def _keys(payload: dict[str, Any], expected: set[str], name: str) -> None:
    if set(payload) != expected:
        raise ValueError(
            f"{name} fields must be {sorted(expected)}, got {sorted(payload)}",
        )


def _dict(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    return value


def _list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return value


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{name} must be non-empty text")
    return value.strip()


def _moves(candidate: dict[str, Any]) -> dict[str, float]:
    return {
        signal: float(candidate["outcomes"][signal]["expected_move_pct"])
        for signal in TRADE_SIGNALS
    }
