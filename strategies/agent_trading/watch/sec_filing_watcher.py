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
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlsplit

import aiohttp

from strategies.agent_trading.watch.watch_data_models import DisclosureFile
from strategies.agent_trading.watch.watch_data_models import DisclosurePackage
from strategies.agent_trading.watch.watch_data_models import WatchPlan
from strategies.agent_trading.watch.watch_data_models import WatchTarget
from strategies.agent_trading.watch.watch_trace import cache_metadata
from strategies.agent_trading.watch.watch_trace import fresh_url
from strategies.agent_trading.watch.disclosure_preprocessor import DisclosureProcessor


SEC_FEED_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&count=100&output=atom"
)
CIK_PATTERN = re.compile(r"\((\d{10})\)")
ACCESSION_PREFIX = "urn:tag:sec.gov,2008:accession-number="
LOG = logging.getLogger(__name__)
MAX_RETRY_SECONDS = 10.0
RETRY_ERRORS = (
    aiohttp.ClientError,
    TimeoutError,
    UnicodeDecodeError,
    KeyError,
    TypeError,
    ValueError,
    ET.ParseError,
)


@dataclass(frozen=True)
class _HttpBody:
    data: bytes
    content_type: str
    url: str
    cache: dict[str, str]


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
    description: str
    source_url: str


@dataclass(frozen=True)
class _FilingIndex:
    items: tuple[str, ...]
    rows: tuple[_FilingRow, ...]


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
        self.timeout_failures = 0
        self.limiter = _RateLimiter(requests_per_second=8)
        self.task = asyncio.create_task(self._run(), name="agent-trading-sec-watcher")

    def add(self, target: WatchTarget) -> None:
        event_id = target.plan.event_id
        if event_id in self.targets:
            raise ValueError(f"SEC watch already exists: {event_id}")
        self.targets[event_id] = target
        self.changed.set()

    async def remove(self, event_id: str) -> None:
        self.targets.pop(event_id, None)
        self.seen = {item for item in self.seen if item[0] != event_id}
        waiting = []
        for key, task in tuple(self.downloads.items()):
            if key[0] == event_id:
                task.cancel()
                waiting.append(task)
        if waiting:
            await asyncio.gather(*waiting, return_exceptions=True)
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
        retry_seconds = self.poll_seconds
        while not self.closed:
            busy = {key[0] for key in self.downloads}
            if not any(event_id not in busy for event_id in self.targets):
                await self.changed.wait()
                self.changed.clear()
                continue
            try:
                started = time.perf_counter()
                body = await self._get(fresh_url(self.feed_url))
                for target in tuple(self.targets.values()):
                    target.trace.record(
                        "sec",
                        "poll_response",
                        duration_ms=round((time.perf_counter() - started) * 1000, 3),
                        url=body.url,
                        size_bytes=len(body.data),
                        cache=body.cache,
                    )
                await self._process(_parse_sec_feed(body.data))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                for target in tuple(self.targets.values()):
                    target.trace.record(
                        "sec",
                        "poll_failed",
                        error=f"{type(exc).__name__}: {exc}",
                    )
                self._health(exc)
                limited = isinstance(exc, aiohttp.ClientResponseError) and exc.status == 429
                delay = retry_seconds if limited else self.poll_seconds
                if limited:
                    retry_after = exc.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = max(
                                self.poll_seconds,
                                min(float(retry_after), MAX_RETRY_SECONDS),
                            )
                        except (TypeError, ValueError):
                            pass
                log = LOG.warning if isinstance(exc, RETRY_ERRORS) else LOG.exception
                log(
                    "SEC polling failed url=%s error_type=%s error=%r "
                    "retry_in=%.1fs",
                    self.feed_url,
                    type(exc).__name__,
                    exc,
                    delay,
                )
                await self._wait(delay)
                retry_seconds = min(delay * 2, MAX_RETRY_SECONDS) if limited else self.poll_seconds
                continue
            self._health(None)
            retry_seconds = self.poll_seconds
            await self._wait(self.poll_seconds)

    async def _wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.changed.wait(), seconds)
        except TimeoutError:
            pass
        self.changed.clear()

    def _health(self, exc: Exception | None) -> None:
        for event_id, target in tuple(self.targets.items()):
            self._target_health(event_id, target, exc)

    def _target_health(
        self,
        event_id: str,
        target: WatchTarget,
        exc: Exception | None,
    ) -> bool:
        try:
            target.health("sec", exc)
            return True
        except Exception as health_exc:
            LOG.exception(
                "SEC health callback failed event_id=%s error_type=%s error=%r",
                event_id,
                type(health_exc).__name__,
                health_exc,
            )
            self.targets.pop(event_id, None)
            self._fail(event_id, target, health_exc)
            return False

    @staticmethod
    def _fail(event_id: str, target: WatchTarget, exc: Exception) -> None:
        try:
            target.fail(exc)
        except Exception as fail_exc:
            LOG.exception(
                "SEC failure callback failed event_id=%s error_type=%s error=%r",
                event_id,
                type(fail_exc).__name__,
                fail_exc,
            )

    async def _process(self, entries: tuple[_SecEntry, ...]) -> None:
        for entry in entries:
            for event_id, target in tuple(self.targets.items()):
                try:
                    key = (event_id, entry.accession)
                    if key in self.seen or not _sec_matches(entry, target.plan):
                        continue
                    self.seen.add(key)
                    self.downloads[key] = asyncio.create_task(
                        self._finish(key, target, entry),
                        name=f"agent-trading-sec-download-{event_id}",
                    )
                except Exception as exc:
                    LOG.exception(
                        "SEC target failed event_id=%s accession=%s "
                        "error_type=%s error=%r",
                        event_id,
                        entry.accession,
                        type(exc).__name__,
                        exc,
                    )
                    self.targets.pop(event_id, None)
                    self._fail(event_id, target, exc)
                    continue

    # 下载和预处理独立运行，不能阻塞共享 SEC feed 的下一次轮询。
    async def _finish(
        self,
        key: tuple[str, str],
        target: WatchTarget,
        entry: _SecEntry,
    ) -> None:
        try:
            package = await self._download(target, entry)
            if package is None:
                return
            if not self._target_health(target.plan.event_id, target, None):
                return
            if await target.ready(package):
                self.targets.pop(target.plan.event_id, None)
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, TimeoutError) as exc:
            self.seen.discard(key)
            self._target_health(target.plan.event_id, target, exc)
            LOG.warning(
                "SEC document download failed accession=%s url=%s "
                "error_type=%s error=%r",
                entry.accession,
                entry.index_url,
                type(exc).__name__,
                exc,
            )
        except (KeyError, TypeError, ValueError, RuntimeError) as exc:
            # 单份外部申报异常不能终止整个事件，新闻稿来源仍可继续触发分析。
            self._target_health(target.plan.event_id, target, exc)
            LOG.exception(
                "SEC document processing failed accession=%s url=%s "
                "error_type=%s error=%r",
                entry.accession,
                entry.index_url,
                type(exc).__name__,
                exc,
            )
        except Exception as exc:
            self._target_health(target.plan.event_id, target, exc)
            LOG.exception(
                "SEC event failed event_id=%s accession=%s error_type=%s error=%r",
                target.plan.event_id,
                entry.accession,
                type(exc).__name__,
                exc,
            )
            self._fail(target.plan.event_id, target, exc)
        finally:
            self.downloads.pop(key, None)
            self.changed.set()

    async def _download(
        self,
        target: WatchTarget,
        entry: _SecEntry,
    ) -> DisclosurePackage | None:
        detected_ns = time.time_ns()
        target.trace.record("sec", "disclosure_detected", accession=entry.accession)
        index = await self._get(entry.index_url)
        filing = _parse_filing_index(index.data)
        if not _is_earnings_filing(entry.form, filing.items):
            LOG.info(
                "ignored non-earnings filing form=%s accession=%s items=%s",
                entry.form,
                entry.accession,
                filing.items,
            )
            return None
        selected = _select_rows(entry.form, filing.rows)
        if not selected:
            raise ValueError(f"no selected SEC documents: {entry.index_url}")

        folder = target.watch_dir / "raw" / "sec"
        folder.mkdir(parents=True, exist_ok=True)
        files: list[DisclosureFile] = []
        for row in selected:
            url = _document_url(entry.index_url, row.source_url)
            body = await self._get(url)
            path = folder / _file_name(body.url, "document.html")
            path.write_bytes(body.data)
            processed = await asyncio.to_thread(
                self.processor.process,
                target.analysis_input_dir,
                path,
                "sec",
                row.document_type,
                row.description,
                body.url,
                body.content_type,
            )
            if processed.processing_status == "failed":
                raise RuntimeError(
                    f"SEC document preprocessing failed: {processed.source_url}",
                )
            files.append(processed)
        target.trace.record(
            "sec",
            "disclosure_processed",
            accession=entry.accession,
            file_count=len(files),
        )
        return DisclosurePackage(
            event_id=target.plan.event_id,
            source="sec",
            form=entry.form,
            accession=entry.accession,
            items=filing.items,
            origin_url=entry.index_url,
            published_at=entry.updated.isoformat(),
            detected_ns=detected_ns,
            files=tuple(files),
        )

    async def _get(self, url: str) -> _HttpBody:
        await self.limiter.wait()
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                body = _HttpBody(
                    data=await response.read(),
                    content_type=response.headers.get("Content-Type", ""),
                    url=str(response.url),
                    cache=cache_metadata(response.headers),
                )
        except TimeoutError:
            self.timeout_failures += 1
            if self.timeout_failures >= 2:
                self._clear_dns(url)
                self.timeout_failures = 0
            raise
        self.timeout_failures = 0
        return body

    def _clear_dns(self, url: str) -> None:
        connector = self.session.connector
        if not isinstance(connector, aiohttp.TCPConnector):
            return
        parsed = urlsplit(url)
        host = parsed.hostname
        if host is None:
            return
        port = parsed.port or 443
        connector.clear_dns_cache(host, port)
        LOG.warning("SEC DNS cache cleared host=%s port=%s", host, port)


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
        self.page_text: list[str] = []

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
                        description=self.row[1][0],
                        source_url=self.row[2][1],
                    ),
                )
            self.in_row = False
        elif tag == "table" and self.in_table:
            self.in_table = False

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if text:
            self.page_text.append(text)
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


def _parse_filing_index(data: bytes) -> _FilingIndex:
    parser = _FilingParser()
    parser.feed(data.decode("utf-8", errors="replace"))
    text = " ".join(parser.page_text)
    items = tuple(dict.fromkeys(re.findall(r"\bItem\s+(\d+\.\d+)\s*:", text)))
    return _FilingIndex(items=items, rows=tuple(parser.rows))


def _is_earnings_filing(form: str, items: tuple[str, ...]) -> bool:
    return form.upper() != "8-K" or "2.02" in items


def _select_rows(form: str, rows: tuple[_FilingRow, ...]) -> tuple[_FilingRow, ...]:
    selected = []
    for row in rows:
        kind = row.document_type.upper()
        description = row.description.casefold()
        if (
            kind == form.upper()
            or kind.startswith(("EX-13", "EX-99"))
            or any(
                term in description
                for term in (
                    "earnings release",
                    "financial results",
                    "financial supplement",
                    "cfo commentary",
                    "shareholder letter",
                )
            )
        ):
            selected.append(row)
    return tuple(selected)


# SEC index 的 iXBRL 链接先指向 viewer，doc 参数才是真实申报文件。
def _document_url(index_url: str, source_url: str) -> str:
    url = urljoin(index_url, source_url)
    parsed = urlsplit(url)
    document = parse_qs(parsed.query).get("doc")
    return urljoin(index_url, document[0]) if document else url


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
    parsed = urlsplit(url)
    path = parse_qs(parsed.query).get("doc", [parsed.path])[0]
    name = Path(unquote(path)).name or default
    clean = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return clean or default
