from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC
from datetime import datetime
from pathlib import Path

import aiohttp

from strategies.agent_trading.watch.watch_data_models import DisclosurePackage
from strategies.agent_trading.watch.watch_data_models import WatchPlan
from strategies.agent_trading.watch.watch_data_models import WatchTarget
from strategies.agent_trading.watch.news_release_watcher import NewsReleaseWatcher
from strategies.agent_trading.watch.rtpr_websocket_watcher import RtprWebSocketWatcher
from strategies.agent_trading.watch.disclosure_preprocessor import DisclosureProcessor
from strategies.agent_trading.watch.sec_filing_watcher import SEC_FEED_URL
from strategies.agent_trading.watch.sec_filing_watcher import SecWatcher
from strategies.agent_trading.watch.watch_trace import WatchTrace


LOG = logging.getLogger(__name__)


class DisclosureTimeoutError(TimeoutError):
    def __init__(self, event_id: str, source_status: dict[str, dict]) -> None:
        self.source_status = source_status
        super().__init__(f"watch window ended without disclosure: {event_id}")


class DisclosureWatcher:
    def __init__(
        self,
        user_agent: str,
        poll_seconds: float = 0.5,
        sec_feed_url: str = SEC_FEED_URL,
    ) -> None:
        if not user_agent.strip():
            raise ValueError("user_agent must not be empty")
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        self.user_agent = user_agent
        self.poll_seconds = poll_seconds
        self.sec_feed_url = sec_feed_url
        self.session: aiohttp.ClientSession | None = None
        self.sec: SecWatcher | None = None
        self.news: NewsReleaseWatcher | None = None
        self.rtpr: RtprWebSocketWatcher | None = None

    async def __aenter__(self) -> DisclosureWatcher:
        await self.start()
        return self

    async def __aexit__(self, *_args) -> None:
        await self.close()

    async def start(self) -> None:
        if self.session is not None:
            raise RuntimeError("DisclosureWatcher is already started")
        timeout = aiohttp.ClientTimeout(total=15)
        # 本地 Windows Python 的 aiodns 不兼容 WSL DNS 代理，统一使用系统解析。
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        self.session = aiohttp.ClientSession(
            headers={
                "User-Agent": self.user_agent,
                "Cache-Control": "no-cache, no-store, max-age=0",
                "Pragma": "no-cache",
            },
            timeout=timeout,
            connector=connector,
        )
        processor = DisclosureProcessor()
        self.sec = SecWatcher(
            session=self.session,
            poll_seconds=self.poll_seconds,
            feed_url=self.sec_feed_url,
            processor=processor,
        )
        self.news = NewsReleaseWatcher(
            session=self.session,
            poll_seconds=self.poll_seconds,
            processor=processor,
        )
        self.rtpr = RtprWebSocketWatcher(
            session=self.session,
            processor=processor,
        )
        self.rtpr.start()

    async def close(self) -> None:
        if (
            self.session is None
            or self.sec is None
            or self.news is None
            or self.rtpr is None
        ):
            return
        try:
            await self.rtpr.close()
            await self.news.close()
            await self.sec.close()
        finally:
            await self.session.close()
            self.session = None
            self.sec = None
            self.news = None
            self.rtpr = None

    # SEC filing 或官方新闻稿中，第一个完整处理成功的来源触发分析。
    async def watch(
        self,
        plan: WatchPlan,
        analysis_input_dir: Path,
        watch_dir: Path,
    ) -> DisclosurePackage:
        if self.sec is None or self.news is None:
            raise RuntimeError("DisclosureWatcher is not started")
        now = datetime.now(UTC)
        if now < plan.start_at:
            await asyncio.sleep((plan.start_at - now).total_seconds())
        remaining = (plan.end_at - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise TimeoutError(f"watch window already ended: {plan.event_id}")

        analysis_input_dir = analysis_input_dir.resolve()
        watch_dir = watch_dir.resolve()
        analysis_input_dir.mkdir(parents=True, exist_ok=True)
        watch_dir.mkdir(parents=True, exist_ok=True)
        trace = WatchTrace(plan.event_id, watch_dir)
        trace.record(
            "watch",
            "started",
            start_at=plan.start_at.isoformat(),
            end_at=plan.end_at.isoformat(),
        )
        result = asyncio.get_running_loop().create_future()
        source_status = {
            "sec": self._source_status(),
            "rtpr": self._source_status(),
            **{
                f"news_release:{source.url}": self._source_status()
                for source in plan.news_sources
            },
        }

        async def ready(package: DisclosurePackage) -> bool:
            if result.done():
                return False
            result.set_result(package)
            return True

        def fail(exc: Exception) -> None:
            if not result.done():
                result.set_exception(exc)

        def health(source: str, exc: Exception | None) -> None:
            status = source_status[source]
            now = datetime.now(UTC).isoformat()
            if exc is None:
                status["consecutive_failures"] = 0
                status["last_success_at"] = now
                return
            status["consecutive_failures"] += 1
            status["last_error"] = f"{type(exc).__name__}: {exc!r}"
            status["last_error_at"] = now

        def news_fail(exc: Exception) -> None:
            LOG.warning(
                "news watch failed event_id=%s error_type=%s error=%r",
                plan.event_id,
                type(exc).__name__,
                exc,
            )

        def rtpr_fail(exc: Exception) -> None:
            LOG.warning(
                "RTPR watch failed event_id=%s error_type=%s error=%r",
                plan.event_id,
                type(exc).__name__,
                exc,
            )

        sec_target = WatchTarget(plan, analysis_input_dir, watch_dir, ready, fail, health, trace)
        news_target = WatchTarget(
            plan,
            analysis_input_dir,
            watch_dir,
            ready,
            news_fail,
            health,
            trace,
        )
        ticker = self._event_ticker(analysis_input_dir / "event.json")
        package = None
        try:
            self.sec.add(sec_target)
            self.news.add(news_target)
            self.rtpr.add(
                WatchTarget(plan, analysis_input_dir, watch_dir, ready, rtpr_fail, health, trace),
                ticker,
            )
            try:
                package = await asyncio.wait_for(result, timeout=remaining)
            except TimeoutError as exc:
                if result.done() and not result.cancelled():
                    raise
                raise DisclosureTimeoutError(plan.event_id, source_status) from exc
            self._write_manifest(analysis_input_dir / "report.json", package)
            trace.record(package.source, "winner", origin_url=package.origin_url)
            return package
        except Exception as exc:
            trace.record("watch", "failed", error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            await self.sec.remove(plan.event_id)
            await self.news.remove(plan.event_id)
            await self.rtpr.remove(plan.event_id)
            trace.write_summary(
                {
                    "event_id": plan.event_id,
                    "finished_at": datetime.now(UTC).isoformat(),
                    "winner": package.source if package is not None else None,
                    "source_status": source_status,
                },
            )

    @staticmethod
    def _source_status() -> dict:
        return {
            "consecutive_failures": 0,
            "last_error": None,
            "last_error_at": None,
            "last_success_at": None,
        }

    @staticmethod
    def _write_manifest(path: Path, package: DisclosurePackage) -> None:
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(package.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(path)

    @staticmethod
    def _event_ticker(path: Path) -> str:
        payload = json.loads(path.read_text(encoding="utf-8"))
        metadata = payload.get("metadata")
        ticker = metadata.get("ticker") if isinstance(metadata, dict) else None
        if not isinstance(ticker, str) or not ticker.strip():
            raise ValueError(f"event metadata ticker is missing: {path}")
        return ticker.strip()
