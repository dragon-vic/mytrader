from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Awaitable
from typing import Callable
from urllib.parse import urlsplit

from strategies.agent_trading.watch.watch_trace import WatchTrace


SOURCE_FORMATS = {"feed", "html", "q4_json"}
EARNINGS_FORMS = {"8-K", "10-Q", "10-K", "6-K", "20-F", "40-F"}


@dataclass(frozen=True)
class SecPlan:
    cik: str
    forms: tuple[str, ...]


@dataclass(frozen=True)
class NewsSource:
    url: str
    format: str
    title_phrases: tuple[str, ...]
    exclude_phrases: tuple[str, ...]
    content_terms: tuple[str, ...]
    user_agent: str | None = None


@dataclass(frozen=True)
class WatchPlan:
    event_id: str
    start_at: datetime
    end_at: datetime
    sec: SecPlan
    news_sources: tuple[NewsSource, ...]

    # 将受限 JSON 配置转为 Watcher 使用的完整计划。
    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> WatchPlan:
        _require_keys(
            payload,
            {"event_id", "start_at", "end_at", "sec", "news_release"},
            "watch_plan",
        )
        sec_raw = _require_dict(payload["sec"], "sec")
        _require_keys(sec_raw, {"cik", "forms"}, "sec")
        news_raw = _require_dict(payload["news_release"], "news_release")
        _require_keys(news_raw, {"sources"}, "news_release")

        sources = tuple(
            _parse_source(_require_dict(item, "news_release.sources[]"))
            for item in _require_list(news_raw["sources"], "news_release.sources")
        )
        if not sources:
            raise ValueError("news_release.sources must not be empty")

        plan = cls(
            event_id=_require_text(payload["event_id"], "event_id"),
            start_at=_parse_time(payload["start_at"], "start_at"),
            end_at=_parse_time(payload["end_at"], "end_at"),
            sec=SecPlan(
                cik=_require_text(sec_raw["cik"], "sec.cik"),
                forms=_text_tuple(sec_raw["forms"], "sec.forms"),
            ),
            news_sources=sources,
        )
        plan._validate()
        return plan

    @classmethod
    def from_watch_dict(
        cls,
        event_id: str,
        start_at: datetime,
        end_at: datetime,
        payload: dict[str, Any],
    ) -> WatchPlan:
        _require_keys(payload, {"sec", "news_release"}, "watch")
        return cls.from_dict(
            {
                "event_id": event_id,
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
                "sec": payload["sec"],
                "news_release": payload["news_release"],
            },
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "start_at": self.start_at.isoformat(),
            "end_at": self.end_at.isoformat(),
            "sec": {
                "cik": self.sec.cik,
                "forms": list(self.sec.forms),
            },
            "news_release": {
                "sources": [
                    _source_dict(source)
                    for source in self.news_sources
                ],
            },
        }

    def to_watch_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        return {
            "sec": payload["sec"],
            "news_release": payload["news_release"],
        }

    def _validate(self) -> None:
        if self.end_at <= self.start_at:
            raise ValueError("end_at must be later than start_at")
        if len(self.sec.cik) != 10 or not self.sec.cik.isdigit():
            raise ValueError("sec.cik must contain 10 digits")
        if not self.sec.forms:
            raise ValueError("sec.forms must not be empty")
        unsupported = set(self.sec.forms) - EARNINGS_FORMS
        if unsupported:
            raise ValueError(f"unsupported earnings forms: {sorted(unsupported)}")
        for source in self.news_sources:
            parsed = urlsplit(source.url)
            if parsed.scheme != "https" or not parsed.netloc:
                raise ValueError(f"news source must use https: {source.url}")
            if source.format not in SOURCE_FORMATS:
                raise ValueError(f"unsupported news source format: {source.format}")
            if not source.title_phrases:
                raise ValueError("news source title_phrases must not be empty")
            if not source.content_terms:
                raise ValueError("news source content_terms must not be empty")


@dataclass(frozen=True)
class DisclosureFile:
    document_type: str
    description: str
    source_url: str
    content_type: str
    content_format: str
    size_bytes: int
    sha256: str
    raw_path: str
    analysis_path: str
    processing_status: str
    processing_error: str | None


@dataclass(frozen=True)
class DisclosurePackage:
    event_id: str
    source: str
    form: str | None
    accession: str | None
    items: tuple[str, ...]
    origin_url: str
    published_at: str | None
    detected_ns: int
    files: tuple[DisclosureFile, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source": self.source,
            "form": self.form,
            "accession": self.accession,
            "items": list(self.items),
            "origin_url": self.origin_url,
            "published_at": self.published_at,
            "detected_ns": self.detected_ns,
            "files": [
                {
                    "document_type": item.document_type,
                    "description": item.description,
                    "source_url": item.source_url,
                    "content_type": item.content_type,
                    "content_format": item.content_format,
                    "size_bytes": item.size_bytes,
                    "sha256": item.sha256,
                    "raw_path": item.raw_path,
                    "analysis_path": item.analysis_path,
                    "processing_status": item.processing_status,
                    "processing_error": item.processing_error,
                }
                for item in self.files
            ],
        }


ReadyHandler = Callable[[DisclosurePackage], Awaitable[bool]]
FailHandler = Callable[[Exception], None]
HealthHandler = Callable[[str, Exception | None], None]


@dataclass
class WatchTarget:
    plan: WatchPlan
    analysis_input_dir: Path
    watch_dir: Path
    ready: ReadyHandler
    fail: FailHandler
    health: HealthHandler
    trace: WatchTrace


def _parse_source(payload: dict[str, Any]) -> NewsSource:
    required = {"url", "format", "title_phrases", "exclude_phrases", "content_terms"}
    actual = set(payload)
    if not required <= actual or actual - required - {"user_agent"}:
        raise ValueError(
            "news_release.sources[] fields must contain "
            f"{sorted(required)} with optional user_agent, got {sorted(actual)}",
        )
    return NewsSource(
        url=_require_text(payload["url"], "news source url"),
        format=_require_text(payload["format"], "news source format"),
        title_phrases=_text_tuple(
            payload["title_phrases"],
            "news source title_phrases",
        ),
        exclude_phrases=_text_tuple(
            payload["exclude_phrases"],
            "news source exclude_phrases",
        ),
        content_terms=_text_tuple(
            payload["content_terms"],
            "news source content_terms",
        ),
        user_agent=(
            _require_text(payload["user_agent"], "news source user_agent")
            if "user_agent" in payload
            else None
        ),
    )


def _source_dict(source: NewsSource) -> dict[str, Any]:
    payload = {
        "url": source.url,
        "format": source.format,
        "title_phrases": list(source.title_phrases),
        "exclude_phrases": list(source.exclude_phrases),
        "content_terms": list(source.content_terms),
    }
    if source.user_agent is not None:
        payload["user_agent"] = source.user_agent
    return payload


def _parse_time(value: Any, name: str) -> datetime:
    text = _require_text(value, name)
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def _require_keys(payload: dict[str, Any], keys: set[str], name: str) -> None:
    actual = set(payload)
    if actual != keys:
        raise ValueError(
            f"{name} fields must be {sorted(keys)}, got {sorted(actual)}",
        )


def _require_dict(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    return value


def _require_list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return value


def _require_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{name} must be a non-empty string")
    return value.strip()


def _text_tuple(value: Any, name: str) -> tuple[str, ...]:
    items = _require_list(value, name)
    return tuple(_require_text(item, f"{name}[]") for item in items)
