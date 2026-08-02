from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qsl
from urllib.parse import unquote
from urllib.parse import urlencode
from urllib.parse import urljoin
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import aiohttp

from strategies.agent_trading.watch.models import DisclosurePackage
from strategies.agent_trading.watch.models import NewsSource
from strategies.agent_trading.watch.models import WatchPlan
from strategies.agent_trading.watch.models import WatchTarget
from strategies.agent_trading.watch.processor import DisclosureProcessor


LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class _HttpBody:
    data: bytes
    content_type: str
    url: str


@dataclass(frozen=True)
class _FeedEntry:
    item_id: str
    title: str
    url: str
    published: datetime | None
    inline_body: bytes | None = None
    body_lookup_url: str | None = None


@dataclass(frozen=True)
class _Link:
    title: str
    url: str


class NewsReleaseWatcher:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        poll_seconds: float,
        processor: DisclosureProcessor,
    ) -> None:
        self.session = session
        self.poll_seconds = poll_seconds
        self.processor = processor
        self.tasks: dict[str, set[asyncio.Task]] = {}
        self.limits: dict[str, _HostLimiter] = {}

    def add(self, target: WatchTarget) -> None:
        event_id = target.plan.event_id
        if event_id in self.tasks:
            raise ValueError(f"news watch already exists: {event_id}")
        self.tasks[event_id] = {
            asyncio.create_task(
                self._run(target, source),
                name=f"agent-trading-news-{event_id}",
            )
            for source in target.plan.news_sources
        }

    async def remove(self, event_id: str) -> None:
        tasks = self.tasks.pop(event_id, set())
        current = asyncio.current_task()
        waiting = []
        for task in tasks:
            if task is current:
                continue
            task.cancel()
            waiting.append(task)
        if waiting:
            await asyncio.gather(*waiting, return_exceptions=True)

    async def close(self) -> None:
        for event_id in tuple(self.tasks):
            await self.remove(event_id)

    # 每个新闻源独立维护已见条目，重启后继续去重。
    async def _run(self, target: WatchTarget, source: NewsSource) -> None:
        state_path = _seen_path(target.internal_dir, source)
        seen = _load_seen(state_path, source)
        try:
            while True:
                try:
                    listing = await self._get(source.url, source.user_agent)
                    entries = _parse_news(source, listing.data)
                    if not entries:
                        raise ValueError(f"news listing has no entries: {source.url}")
                    if seen is None:
                        seen = {_entry_id(entry) for entry in entries}
                        _save_seen(state_path, source, seen)
                        await asyncio.sleep(self.poll_seconds)
                        continue

                    changed = False
                    for entry in entries:
                        entry_id = _entry_id(entry)
                        if entry_id in seen:
                            continue
                        if not _matches_entry(source, target.plan, entry):
                            seen.add(entry_id)
                            changed = True
                            continue
                        package = await self._download(target, source, entry)
                        seen.add(entry_id)
                        changed = True
                        if not _matches_content(source, target.analysis_dir, package):
                            continue
                        _save_seen(state_path, source, seen)
                        if await target.ready(package):
                            return
                    if changed:
                        _save_seen(state_path, source, seen)
                except (aiohttp.ClientError, TimeoutError) as exc:
                    LOG.warning("news polling failed url=%s error=%s", source.url, exc)
                    await asyncio.sleep(self.poll_seconds)
                    continue
                await asyncio.sleep(self.poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            target.fail(exc)

    async def _download(
        self,
        target: WatchTarget,
        source: NewsSource,
        entry: _FeedEntry,
    ) -> DisclosurePackage:
        detected_ns = time.time_ns()
        if entry.inline_body is not None:
            body = _HttpBody(entry.inline_body, "text/html; charset=utf-8", entry.url)
        elif entry.body_lookup_url is not None:
            lookup = await self._get(entry.body_lookup_url, source.user_agent)
            full_entry = next(
                (
                    candidate
                    for candidate in _parse_q4_json(entry.body_lookup_url, lookup.data)
                    if candidate.item_id == entry.item_id
                ),
                None,
            )
            if full_entry is None or full_entry.inline_body is None:
                raise ValueError(f"Q4 press-release body is missing: {entry.item_id}")
            body = _HttpBody(
                full_entry.inline_body,
                "text/html; charset=utf-8",
                entry.url,
            )
        else:
            body = await self._get(entry.url, source.user_agent)
        folder = target.internal_dir / "disclosure" / "news_release" / "raw"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / _file_name(body.url, "release.html")
        path.write_bytes(body.data)
        processed = await asyncio.to_thread(
            self.processor.process,
            target.analysis_dir,
            path,
            "NEWS_RELEASE",
            entry.title,
            body.url,
            body.content_type,
        )
        return DisclosurePackage(
            event_id=target.plan.event_id,
            source="news_release",
            form=None,
            accession=None,
            items=(),
            origin_url=body.url,
            published_at=entry.published.isoformat() if entry.published else None,
            detected_ns=detected_ns,
            files=(processed,),
        )

    async def _get(self, url: str, user_agent: str | None = None) -> _HttpBody:
        host = urlsplit(url).netloc.casefold()
        limit = self.limits.setdefault(host, _HostLimiter(self.poll_seconds))
        await limit.wait()
        headers = {"User-Agent": user_agent} if user_agent else None
        async with self.session.get(url, headers=headers) as response:
            response.raise_for_status()
            return _HttpBody(
                data=await response.read(),
                content_type=response.headers.get("Content-Type", ""),
                url=str(response.url),
            )


class _HostLimiter:
    def __init__(self, interval: float) -> None:
        self.interval = interval
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    # 同一新闻主机上的所有事件共享请求间隔。
    async def wait(self) -> None:
        async with self.lock:
            now = asyncio.get_running_loop().time()
            if now < self.next_at:
                await asyncio.sleep(self.next_at - now)
            self.next_at = asyncio.get_running_loop().time() + self.interval


class _LinkParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.href = ""
        self.text: list[str] = []
        self.links: list[_Link] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href", "")
        if href:
            self.href = urljoin(self.base_url, href)
            self.text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.href:
            self.links.append(_Link(" ".join(self.text).strip(), self.href))
            self.href = ""
            self.text = []

    def handle_data(self, data: str) -> None:
        if self.href:
            text = data.strip()
            if text:
                self.text.append(text)


# 将 RSS/Atom 与 HTML 列表统一成带稳定 ID 的新闻条目。
def _parse_news(source: NewsSource, data: bytes) -> tuple[_FeedEntry, ...]:
    if source.format == "feed":
        return _parse_news_feed(source.url, data)
    if source.format == "q4_json":
        return _parse_q4_json(source.url, data)
    if source.format != "html":
        raise ValueError(f"unsupported news source format: {source.format}")

    parser = _LinkParser(source.url)
    parser.feed(data.decode("utf-8", errors="replace"))
    return tuple(
        _FeedEntry(link.url, link.title, link.url, None)
        for link in parser.links
    )


# 新条目先通过事件时间和标题包含/排除规则。
def _matches_entry(
    source: NewsSource,
    plan: WatchPlan,
    entry: _FeedEntry,
) -> bool:
    title = " ".join(entry.title.casefold().split())
    phrases = tuple(phrase.casefold() for phrase in source.title_phrases)
    excluded = tuple(phrase.casefold() for phrase in source.exclude_phrases)
    if not any(phrase in title for phrase in phrases):
        return False
    if any(phrase in title for phrase in excluded):
        return False
    if entry.published is None:
        return True
    published = entry.published.astimezone(UTC)
    return plan.start_at <= published <= plan.end_at


# 下载后的正文必须包含计划指定的财务确认词。
def _matches_content(
    source: NewsSource,
    analysis_dir: Path,
    package: DisclosurePackage,
) -> bool:
    processed = package.files[0]
    if processed.processing_status != "processed":
        return False
    path = analysis_dir / processed.analysis_path
    text = " ".join(path.read_text(encoding="utf-8").casefold().split())
    return all(term.casefold() in text for term in source.content_terms)


def _entry_id(entry: _FeedEntry) -> str:
    return entry.item_id or entry.url


# 每个事件、每个新闻源单独保存 seen_ids，避免并发写同一文件。
def _seen_path(internal_dir: Path, source: NewsSource) -> Path:
    source_id = hashlib.sha256(source.url.encode("utf-8")).hexdigest()[:16]
    return internal_dir / "watch" / "news_seen" / f"{source_id}.json"


def _load_seen(path: Path, source: NewsSource) -> set[str] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("source_url") != source.url:
        raise ValueError(f"news state source mismatch: {source.url}")
    values = payload.get("seen_ids")
    if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
        raise TypeError("news state seen_ids must be an array of strings")
    return set(values)


def _save_seen(path: Path, source: NewsSource, seen: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(
            {
                "source_url": source.url,
                "updated_at": datetime.now(UTC).isoformat(),
                "seen_ids": sorted(seen),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def _parse_news_feed(base_url: str, data: bytes) -> tuple[_FeedEntry, ...]:
    root = ET.fromstring(data)
    items = list(_children(root, "entry"))
    if not items:
        channel = _child(root, "channel")
        items = list(_children(channel, "item"))
    entries: list[_FeedEntry] = []
    for item in items:
        url = _alternate_link(item) or _child_text(item, "link")
        item_id = _child_text(item, "id") or _child_text(item, "guid") or url
        date_text = (
            _child_text(item, "published")
            or _child_text(item, "updated")
            or _child_text(item, "pubDate")
        )
        if url:
            absolute_url = urljoin(base_url, url)
            entries.append(
                _FeedEntry(
                    item_id=item_id,
                    title=_child_text(item, "title"),
                    url=absolute_url,
                    published=_parse_date(date_text) if date_text else None,
                ),
            )
    return tuple(entries)


def _parse_q4_json(base_url: str, data: bytes) -> tuple[_FeedEntry, ...]:
    payload = json.loads(data.decode("utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Q4 press-release response must be an object")
    items = payload.get("GetPressReleaseListResult")
    if not isinstance(items, list):
        raise TypeError("Q4 press-release result must be an array")

    entries: list[_FeedEntry] = []
    for item in items:
        if not isinstance(item, dict):
            raise TypeError("Q4 press-release item must be an object")
        press_release_id = item.get("PressReleaseId")
        title = item.get("Headline")
        url = item.get("LinkToDetailPage")
        body = item.get("Body")
        if not isinstance(press_release_id, (int, str)):
            raise TypeError("Q4 PressReleaseId must be an integer or string")
        if not isinstance(title, str) or not title.strip():
            raise TypeError("Q4 Headline must be non-empty text")
        if not isinstance(url, str) or not url.strip():
            raise TypeError("Q4 LinkToDetailPage must be non-empty text")
        if body is not None and not isinstance(body, str):
            raise TypeError("Q4 Body must be text or null")
        inline_body = body.encode("utf-8") if body and body.strip() else None
        entries.append(
            _FeedEntry(
                item_id=f"q4:{press_release_id}",
                title=title.strip(),
                url=urljoin(base_url, url.strip()),
                published=None,
                inline_body=inline_body,
                body_lookup_url=None if inline_body else _q4_body_url(base_url),
            ),
        )
    return tuple(entries)


def _q4_body_url(url: str) -> str:
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["bodyType"] = "1"
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment),
    )


def _children(element: ET.Element | None, name: str) -> tuple[ET.Element, ...]:
    if element is None:
        return ()
    return tuple(child for child in element if _local_name(child.tag) == name)


def _child(element: ET.Element, name: str) -> ET.Element | None:
    return next(
        (child for child in element if _local_name(child.tag) == name),
        None,
    )


def _child_text(element: ET.Element, name: str) -> str:
    child = _child(element, name)
    return child.text.strip() if child is not None and child.text else ""


def _alternate_link(element: ET.Element) -> str:
    for link in _children(element, "link"):
        if link.attrib.get("rel", "alternate") == "alternate":
            return link.attrib.get("href", "")
    return ""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_date(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include timezone: {value}")
    return parsed


def _file_name(url: str, default: str) -> str:
    name = Path(unquote(urlsplit(url).path)).name or default
    clean = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return clean or default
