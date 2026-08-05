from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
import re
from datetime import UTC
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote
from urllib.parse import urlsplit

import aiohttp
import websockets
from dotenv import load_dotenv
from websockets.exceptions import ConnectionClosed
from websockets.exceptions import WebSocketException

from strategies.agent_trading.watch.disclosure_preprocessor import DisclosureProcessor
from strategies.agent_trading.watch.watch_data_models import DisclosurePackage
from strategies.agent_trading.watch.watch_data_models import WatchPlan
from strategies.agent_trading.watch.watch_data_models import WatchTarget


LOG = logging.getLogger(__name__)
WS_ENDPOINT = "wss://ws.rtpr.io/ws-alerts"
INITIAL_BACKOFF_SECONDS = 2.0
MAX_BACKOFF_SECONDS = 60.0


class RtprWebSocketWatcher:
    """One shared RTPR stream which dispatches matching articles to events."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        processor: DisclosureProcessor,
        api_key: str | None = None,
    ) -> None:
        load_dotenv(Path(__file__).resolve().parents[3] / ".env")
        self.session = session
        self.processor = processor
        self.api_key = (api_key or os.environ.get("RTPR_API_KEY", "")).strip()
        self.targets: dict[str, tuple[WatchTarget, str]] = {}
        self.seen_urls: set[str] = set()
        self.closed = False
        self.task: asyncio.Task | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def start(self) -> None:
        if not self.enabled:
            LOG.warning("RTPR_API_KEY is missing; RTPR WebSocket is disabled")
            return
        if self.task is not None:
            raise RuntimeError("RTPR WebSocket watcher is already started")
        self.task = asyncio.create_task(
            self._run(),
            name="agent-trading-rtpr-websocket",
        )

    def add(self, target: WatchTarget, ticker: str) -> None:
        if not self.enabled:
            return
        event_id = target.plan.event_id
        if event_id in self.targets:
            raise ValueError(f"RTPR watch already exists: {event_id}")
        self.targets[event_id] = (target, ticker.casefold())

    async def remove(self, event_id: str) -> None:
        self.targets.pop(event_id, None)

    async def close(self) -> None:
        self.closed = True
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None

    async def _run(self) -> None:
        backoff = INITIAL_BACKOFF_SECONDS
        while not self.closed:
            try:
                await self._consume_once()
                if self.closed:
                    return
                LOG.warning(
                    "RTPR WebSocket closed; reconnect_in=%.1fs",
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OSError, TimeoutError, WebSocketException) as exc:
                LOG.warning(
                    "RTPR WebSocket failed error_type=%s error=%r "
                    "reconnect_in=%.1fs",
                    type(exc).__name__,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

    async def _consume_once(self) -> None:
        endpoint = f"{WS_ENDPOINT}?apiKey={self.api_key}"
        async with websockets.connect(
            endpoint,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
        ) as websocket:
            LOG.info("RTPR WebSocket connected")
            for target, _ in tuple(self.targets.values()):
                target.trace.record("rtpr", "connected")
            async for raw in websocket:
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    LOG.warning("ignoring non-JSON RTPR WebSocket message")
                    continue
                if message.get("type") == "ping":
                    await websocket.send(json.dumps({"type": "pong"}))
                    continue
                if message.get("type") != "alert":
                    continue
                for target, _ in tuple(self.targets.values()):
                    target.trace.record(
                        "rtpr",
                        "alert_received",
                        ticker=message.get("ticker"),
                        article_url=message.get("article_url"),
                    )
                await self._dispatch(message)

    async def _dispatch(self, message: dict) -> None:
        ticker = str(message.get("ticker", "")).casefold()
        article_url = message.get("article_url")
        if not ticker or not isinstance(article_url, str) or not article_url:
            return
        if article_url in self.seen_urls:
            return
        now = datetime.now(UTC)
        published = _timestamp(message.get("article_published_at")) or now
        candidates = [
            target
            for target, target_ticker in self.targets.values()
            if target_ticker == ticker
        ]
        if not candidates:
            return
        for target in candidates:
            try:
                package = await self._download(target, article_url, published)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                target.health("rtpr", exc)
                LOG.warning(
                    "RTPR article processing failed event_id=%s url=%s "
                    "error_type=%s error=%r",
                    target.plan.event_id,
                    article_url,
                    type(exc).__name__,
                    exc,
                )
                continue
            if not _matches_plan(target, package):
                continue
            self.seen_urls.add(article_url)
            target.health("rtpr", None)
            if await target.ready(package):
                return
        self.seen_urls.add(article_url)

    async def _download(
        self,
        target: WatchTarget,
        article_url: str,
        published: datetime,
    ) -> DisclosurePackage:
        headers = {"X-API-Key": self.api_key}
        async with self.session.get(article_url, headers=headers) as response:
            response.raise_for_status()
            data = await response.read()
            content_type = response.headers.get("Content-Type", "")
            source_url = str(response.url)
        folder = target.watch_dir / "raw" / "rtpr"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / _file_name(source_url, "article.html")
        path.write_bytes(data)
        processed = await asyncio.to_thread(
            self.processor.process,
            target.analysis_input_dir,
            path,
            "rtpr",
            "NEWS_RELEASE",
            f"RTPR {target.plan.event_id}",
            source_url,
            content_type,
        )
        if processed.processing_status == "failed":
            raise ValueError(f"RTPR preprocessing failed: {source_url}")
        target.trace.record(
            "rtpr",
            "disclosure_processed",
            article_url=source_url,
            size_bytes=len(data),
        )
        return DisclosurePackage(
            event_id=target.plan.event_id,
            source="rtpr",
            form=None,
            accession=None,
            items=(),
            origin_url=source_url,
            published_at=published.isoformat(),
            detected_ns=time.time_ns(),
            files=(processed,),
        )


def _matches_plan(target: WatchTarget, package: DisclosurePackage) -> bool:
    processed = package.files[0]
    path = target.analysis_input_dir / processed.analysis_path
    text = " ".join(path.read_text(encoding="utf-8").casefold().split())
    for source in target.plan.news_sources:
        phrases = tuple(" ".join(item.casefold().split()) for item in source.title_phrases)
        excludes = tuple(" ".join(item.casefold().split()) for item in source.exclude_phrases)
        if any(phrase in text for phrase in phrases) and not any(
            phrase in text for phrase in excludes
        ) and all(term.casefold() in text for term in source.content_terms):
            return True
    return False


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _file_name(url: str, default: str) -> str:
    name = Path(unquote(urlsplit(url).path)).name or default
    clean = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"{clean[:80]}-{digest}.html"
