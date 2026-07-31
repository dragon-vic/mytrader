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

from strategies.agent_trading.watch.models import DisclosureFile
from strategies.agent_trading.watch.models import DisclosurePackage
from strategies.agent_trading.watch.models import WatchPlan
from strategies.agent_trading.watch.models import WatchTarget
from strategies.agent_trading.watch.processor import DisclosureProcessor


SEC_FEED_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&count=100&output=atom"
)
CIK_PATTERN = re.compile(r"\((\d{10})\)")
ACCESSION_PREFIX = "urn:tag:sec.gov,2008:accession-number="
LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class _HttpBody:
    data: bytes
    content_type: str
    url: str


@dataclass(frozen=True)
class _SecEntry:
    cik: str
    form: str
    accession: str
    index_url: str
    updated: datetime


@dataclass(frozen=True)
class _FilingRow:
    document_type: str
    source_url: str


class SecWatcher:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        poll_seconds: float,
        feed_url: str,
        processor: DisclosureProcessor,
    ) -> None:
        self.session = session
        self.poll_seconds = poll_seconds
        self.feed_url = feed_url
        self.processor = processor
        self.targets: dict[str, WatchTarget] = {}
        self.seen: set[tuple[str, str]] = set()
        self.downloads: dict[tuple[str, str], asyncio.Task] = {}
        self.changed = asyncio.Event()
        self.closed = False
        self.limiter = _RateLimiter(requests_per_second=8)
        self.task = asyncio.create_task(self._run(), name="agent-trading-sec-watcher")

    def add(self, target: WatchTarget) -> None:
        event_id = target.plan.event_id
        if event_id in self.targets:
            raise ValueError(f"SEC watch already exists: {event_id}")
        self.targets[event_id] = target
        self.changed.set()

    def remove(self, event_id: str) -> None:
        self.targets.pop(event_id, None)
        self.seen = {item for item in self.seen if item[0] != event_id}
        for key, task in tuple(self.downloads.items()):
            if key[0] == event_id:
                task.cancel()
        self.changed.set()

    async def close(self) -> None:
        self.closed = True
        self.changed.set()
        await self.task
        tasks = tuple(self.downloads.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self) -> None:
        try:
            while not self.closed:
                busy = {key[0] for key in self.downloads}
                if not any(event_id not in busy for event_id in self.targets):
                    await self.changed.wait()
                    self.changed.clear()
                    continue
                try:
                    body = await self._get(self.feed_url)
                except aiohttp.ClientError as exc:
                    LOG.warning("SEC polling failed: %s", exc)
                else:
                    await self._process(_parse_sec_feed(body.data))
                try:
                    await asyncio.wait_for(self.changed.wait(), self.poll_seconds)
                except TimeoutError:
                    pass
                self.changed.clear()
        except Exception as exc:
            for target in tuple(self.targets.values()):
                target.fail(exc)
            self.targets.clear()
            raise

    async def _process(self, entries: tuple[_SecEntry, ...]) -> None:
        for entry in entries:
            for event_id, target in tuple(self.targets.items()):
                key = (event_id, entry.accession)
                if key in self.seen or not _sec_matches(entry, target.plan):
                    continue
                self.seen.add(key)
                self.downloads[key] = asyncio.create_task(
                    self._finish(key, target, entry),
                    name=f"agent-trading-sec-download-{event_id}",
                )

    # 下载和预处理独立运行，不能阻塞共享 SEC feed 的下一次轮询。
    async def _finish(
        self,
        key: tuple[str, str],
        target: WatchTarget,
        entry: _SecEntry,
    ) -> None:
        try:
            package = await self._download(target, entry)
            if await target.ready(package):
                self.targets.pop(target.plan.event_id, None)
        except asyncio.CancelledError:
            raise
        except aiohttp.ClientError as exc:
            self.seen.discard(key)
            LOG.warning(
                "SEC document download failed accession=%s error=%s",
                entry.accession,
                exc,
            )
        except Exception as exc:
            target.fail(exc)
        finally:
            self.downloads.pop(key, None)
            self.changed.set()

    async def _download(
        self,
        target: WatchTarget,
        entry: _SecEntry,
    ) -> DisclosurePackage:
        detected_ns = time.time_ns()
        index = await self._get(entry.index_url)
        rows = _parse_filing_index(index.data)
        prefixes = tuple(item.upper() for item in target.plan.sec.exhibits)
        selected = [
            row
            for row in rows
            if row.document_type.upper() == entry.form.upper()
            or row.document_type.upper().startswith(prefixes)
        ]
        if not selected:
            raise ValueError(f"no selected SEC documents: {entry.index_url}")

        folder = target.event_dir / "disclosure" / "sec" / "raw"
        folder.mkdir(parents=True, exist_ok=True)
        files: list[DisclosureFile] = []
        for row in selected:
            url = urljoin(entry.index_url, row.source_url)
            body = await self._get(url)
            path = folder / _file_name(body.url, "document.html")
            path.write_bytes(body.data)
            processed = await asyncio.to_thread(
                self.processor.process,
                target.event_dir,
                path,
                row.document_type,
                body.url,
                body.content_type,
            )
            files.append(processed)
        return DisclosurePackage(
            event_id=target.plan.event_id,
            source="sec",
            origin_url=entry.index_url,
            published_at=entry.updated.isoformat(),
            detected_ns=detected_ns,
            files=tuple(files),
        )

    async def _get(self, url: str) -> _HttpBody:
        await self.limiter.wait()
        async with self.session.get(url) as response:
            response.raise_for_status()
            return _HttpBody(
                data=await response.read(),
                content_type=response.headers.get("Content-Type", ""),
                url=str(response.url),
            )


class _RateLimiter:
    def __init__(self, requests_per_second: int) -> None:
        self.interval = 1 / requests_per_second
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = asyncio.get_running_loop().time()
            if now < self.next_at:
                await asyncio.sleep(self.next_at - now)
            self.next_at = asyncio.get_running_loop().time() + self.interval


class _FilingParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.cell_text: list[str] = []
        self.cell_href = ""
        self.row: list[tuple[str, str]] = []
        self.rows: list[_FilingRow] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        values = dict(attrs)
        if tag == "table" and "tableFile" in values.get("class", "").split():
            self.in_table = True
        elif self.in_table and tag == "tr":
            self.in_row = True
            self.row = []
        elif self.in_row and tag == "td":
            self.in_cell = True
            self.cell_text = []
            self.cell_href = ""
        elif self.in_cell and tag == "a":
            self.cell_href = values.get("href", "")

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self.in_cell:
            self.row.append(("".join(self.cell_text).strip(), self.cell_href))
            self.in_cell = False
        elif tag == "tr" and self.in_row:
            if len(self.row) >= 4 and self.row[2][1] and self.row[3][0]:
                self.rows.append(
                    _FilingRow(
                        document_type=self.row[3][0],
                        source_url=self.row[2][1],
                    ),
                )
            self.in_row = False
        elif tag == "table" and self.in_table:
            self.in_table = False

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.cell_text.append(data)


def _parse_sec_feed(data: bytes) -> tuple[_SecEntry, ...]:
    root = ET.fromstring(data)
    entries: list[_SecEntry] = []
    for item in _children(root, "entry"):
        title = _child_text(item, "title")
        cik_match = CIK_PATTERN.search(title)
        category = _child(item, "category")
        link = _alternate_link(item)
        item_id = _child_text(item, "id")
        if cik_match is None or category is None or not link:
            continue
        if not item_id.startswith(ACCESSION_PREFIX):
            continue
        entries.append(
            _SecEntry(
                cik=cik_match.group(1),
                form=category.attrib["term"],
                accession=item_id.removeprefix(ACCESSION_PREFIX),
                index_url=link,
                updated=_parse_date(_child_text(item, "updated")),
            ),
        )
    return tuple(entries)


def _parse_filing_index(data: bytes) -> tuple[_FilingRow, ...]:
    parser = _FilingParser()
    parser.feed(data.decode("utf-8", errors="replace"))
    return tuple(parser.rows)


def _sec_matches(entry: _SecEntry, plan: WatchPlan) -> bool:
    return (
        entry.cik == plan.sec.cik
        and entry.form.upper() in {item.upper() for item in plan.sec.forms}
        and plan.start_at <= entry.updated <= plan.end_at
    )


def _children(element: ET.Element, name: str) -> tuple[ET.Element, ...]:
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
