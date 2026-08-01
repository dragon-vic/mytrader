from __future__ import annotations

import asyncio
import logging
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote
from urllib.parse import urljoin
from urllib.parse import urlsplit

import aiohttp

from strategies.agent_trading.watch.models import DisclosurePackage
from strategies.agent_trading.watch.models import NewsSource
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

    async def _run(self, target: WatchTarget, source: NewsSource) -> None:
        try:
            while True:
                try:
                    listing = await self._get(source.url)
                    entry = _find_news(source, listing.data)
                    if entry is not None:
                        package = await self._download(target, entry)
                        if await target.ready(package):
                            return
                except aiohttp.ClientError as exc:
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
        entry: _FeedEntry,
    ) -> DisclosurePackage:
        detected_ns = time.time_ns()
        body = await self._get(entry.url)
        folder = target.event_dir / "disclosure" / "news_release" / "raw"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / _file_name(body.url, "release.html")
        path.write_bytes(body.data)
        processed = await asyncio.to_thread(
            self.processor.process,
            target.event_dir,
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

    async def _get(self, url: str) -> _HttpBody:
        async with self.session.get(url) as response:
            response.raise_for_status()
            return _HttpBody(
                data=await response.read(),
                content_type=response.headers.get("Content-Type", ""),
                url=str(response.url),
            )


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


def _find_news(source: NewsSource, data: bytes) -> _FeedEntry | None:
    terms = tuple(term.casefold() for term in source.title_terms)
    if source.format == "feed":
        entries = _parse_news_feed(data)
        baseline = next(
            (
                index
                for index, entry in enumerate(entries)
                if source.last_seen in {entry.item_id, entry.url}
            ),
            None,
        )
        if baseline is None:
            raise ValueError(f"news baseline was not found: {source.last_seen}")
        for entry in entries[:baseline]:
            if all(term in entry.title.casefold() for term in terms):
                return entry
        return None

    parser = _LinkParser(source.url)
    parser.feed(data.decode("utf-8", errors="replace"))
    baseline_found = False
    for link in parser.links:
        if link.url == source.last_seen:
            baseline_found = True
            break
        if all(term in link.title.casefold() for term in terms):
            return _FeedEntry("", link.title, link.url, None)
    if not baseline_found:
        raise ValueError(f"news baseline was not found: {source.last_seen}")
    return None


def _parse_news_feed(data: bytes) -> tuple[_FeedEntry, ...]:
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
            entries.append(
                _FeedEntry(
                    item_id=item_id,
                    title=_child_text(item, "title"),
                    url=url,
                    published=_parse_date(date_text) if date_text else None,
                ),
            )
    return tuple(entries)


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
