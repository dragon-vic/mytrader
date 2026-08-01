from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any


SESSIONS = {"BMO", "AMC"}
VENUES = {"BINANCE", "HYPERLIQUID"}


@dataclass(frozen=True)
class EventSpec:
    event_id: str
    company: str
    ticker: str
    expected_at: datetime
    relevance_reason: str


@dataclass(frozen=True)
class BatchPlan:
    batch_id: str
    session: str
    watch_start_at: datetime
    watch_end_at: datetime
    events: tuple[EventSpec, ...]

    @property
    def research_start_at(self) -> datetime:
        return self.watch_start_at - timedelta(hours=4)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BatchPlan:
        _keys(
            payload,
            {"batch_id", "session", "watch_start_at", "watch_end_at", "events"},
            "batch",
        )
        events = tuple(_event(_dict(item, "events[]")) for item in _list(payload["events"], "events"))
        plan = cls(
            batch_id=_text(payload["batch_id"], "batch_id"),
            session=_text(payload["session"], "session"),
            watch_start_at=_time(payload["watch_start_at"], "watch_start_at"),
            watch_end_at=_time(payload["watch_end_at"], "watch_end_at"),
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
                    "expected_at": event.expected_at.isoformat(),
                    "relevance_reason": event.relevance_reason,
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


@dataclass(frozen=True)
class MarketInstrument:
    symbol: str
    instrument_id: str
    venue: str
    market_symbol: str


@dataclass(frozen=True)
class MarketUniverse:
    as_of: datetime
    instruments: tuple[MarketInstrument, ...]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> MarketUniverse:
        _keys(payload, {"as_of", "instruments"}, "market_universe")
        instruments = tuple(
            _instrument(_dict(item, "instruments[]"))
            for item in _list(payload["instruments"], "instruments")
        )
        if not instruments:
            raise ValueError("market universe must not be empty")
        ids = [item.instrument_id for item in instruments]
        if len(ids) != len(set(ids)):
            raise ValueError("market instrument_id values must be unique")
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
                }
                for item in self.instruments
            ],
        }

    # 校验预研候选来自实时市场列表，并执行 Binance 优先规则。
    def validate_candidate(self, candidate: dict[str, Any]) -> None:
        symbol = _text(candidate["symbol"], "trade_candidates[].symbol")
        instrument_id = _text(
            candidate["instrument_id"],
            "trade_candidates[].instrument_id",
        )
        selected = next(
            (item for item in self.instruments if item.instrument_id == instrument_id),
            None,
        )
        if selected is None or selected.symbol != symbol:
            raise ValueError(f"candidate instrument is not in market universe: {instrument_id}")
        venues = {item.venue for item in self.instruments if item.symbol == symbol}
        preferred = "BINANCE" if "BINANCE" in venues else "HYPERLIQUID"
        if selected.venue != preferred:
            raise ValueError(
                f"candidate must prefer {preferred} for {symbol}: {instrument_id}",
            )

    def get(self, instrument_id: str) -> MarketInstrument:
        selected = next(
            (item for item in self.instruments if item.instrument_id == instrument_id),
            None,
        )
        if selected is None:
            raise ValueError(f"instrument is not in market universe: {instrument_id}")
        return selected


# JSON schema 约束形状；这里额外约束 schema 无法表达的事件、规则和市场关系。
def validate_research(
    event_id: str,
    payload: dict[str, Any],
    market_universe: MarketUniverse,
) -> None:
    if payload.get("event_id") != event_id:
        raise ValueError(
            f"research event_id mismatch: {payload.get('event_id')} != {event_id}",
        )
    if not isinstance(payload.get("research_complete"), bool):
        raise TypeError("research_complete must be boolean")
    rules = _list(payload.get("decision_rules"), "decision_rules")
    rule_ids = [_text(_dict(rule, "decision_rules[]").get("id"), "rule.id") for rule in rules]
    if len(rule_ids) != len(set(rule_ids)):
        raise ValueError("decision rule ids must be unique")

    candidates = _list(payload.get("trade_candidates"), "trade_candidates")
    if len(candidates) > 3:
        raise ValueError("trade_candidates must contain at most 3 items")
    instrument_ids: list[str] = []
    for raw_candidate in candidates:
        candidate = _dict(raw_candidate, "trade_candidates[]")
        market_universe.validate_candidate(candidate)
        instrument_id = _text(candidate.get("instrument_id"), "candidate.instrument_id")
        instrument_ids.append(instrument_id)
        unknown_rules = set(_list(candidate.get("relevant_rule_ids"), "candidate.relevant_rule_ids")) - set(rule_ids)
        if unknown_rules:
            raise ValueError(
                f"candidate references unknown rules: {sorted(unknown_rules)}",
            )
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
    trades = _list(payload.get("trades"), "trades")
    if len(trades) > 3:
        raise ValueError("decision trades must contain at most 3 items")
    decision = payload.get("decision")
    if (decision == "HOLD") != (not trades):
        raise ValueError("HOLD must have no trades and TRADE must have at least one trade")

    candidates = {
        _text(_dict(item, "trade_candidates[]").get("instrument_id"), "candidate.instrument_id")
        for item in _list(research.get("trade_candidates"), "trade_candidates")
    }
    rule_ids = {
        _text(_dict(item, "decision_rules[]").get("id"), "rule.id")
        for item in _list(research.get("decision_rules"), "decision_rules")
    }
    selected: list[str] = []
    for raw_trade in trades:
        trade = _dict(raw_trade, "trades[]")
        instrument_id = _text(trade.get("instrument_id"), "trade.instrument_id")
        if instrument_id not in candidates:
            raise ValueError(
                f"analysis selected an instrument absent from pre-research: {instrument_id}",
            )
        selected.append(instrument_id)
        unknown_rules = set(_list(trade.get("rule_ids"), "trade.rule_ids")) - rule_ids
        if unknown_rules:
            raise ValueError(f"trade references unknown rules: {sorted(unknown_rules)}")
    if len(selected) != len(set(selected)):
        raise ValueError("decision instruments must be unique")

    unknown_applied = set(_list(payload.get("applied_rules"), "applied_rules")) - rule_ids
    if unknown_applied:
        raise ValueError(f"decision applied unknown rules: {sorted(unknown_applied)}")
    for raw_deviation in _list(payload.get("rule_deviations"), "rule_deviations"):
        deviation = _dict(raw_deviation, "rule_deviations[]")
        rule_id = _text(deviation.get("rule_id"), "rule_deviations[].rule_id")
        if rule_id not in rule_ids:
            raise ValueError(f"decision deviated from unknown rule: {rule_id}")


def _event(payload: dict[str, Any]) -> EventSpec:
    _keys(
        payload,
        {"event_id", "company", "ticker", "expected_at", "relevance_reason"},
        "events[]",
    )
    return EventSpec(
        event_id=_text(payload["event_id"], "events[].event_id"),
        company=_text(payload["company"], "events[].company"),
        ticker=_text(payload["ticker"], "events[].ticker"),
        expected_at=_time(payload["expected_at"], "events[].expected_at"),
        relevance_reason=_text(payload["relevance_reason"], "events[].relevance_reason"),
    )


def _instrument(payload: dict[str, Any]) -> MarketInstrument:
    _keys(
        payload,
        {"symbol", "instrument_id", "venue", "market_symbol"},
        "instruments[]",
    )
    venue = _text(payload["venue"], "instruments[].venue")
    if venue not in VENUES:
        raise ValueError(f"unsupported market venue: {venue}")
    return MarketInstrument(
        symbol=_text(payload["symbol"], "instruments[].symbol"),
        instrument_id=_text(payload["instrument_id"], "instruments[].instrument_id"),
        venue=venue,
        market_symbol=_text(payload["market_symbol"], "instruments[].market_symbol"),
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
