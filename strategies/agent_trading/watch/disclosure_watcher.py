from __future__ import annotations

import asyncio
import json
import logging
import shutil
from datetime import UTC
from datetime import datetime
from pathlib import Path

import aiohttp

from strategies.agent_trading.watch.watch_data_models import DisclosurePackage
from strategies.agent_trading.watch.watch_data_models import WatchPlan
from strategies.agent_trading.watch.watch_data_models import WatchTarget
from strategies.agent_trading.watch.news_release_watcher import NewsReleaseWatcher
from strategies.agent_trading.watch.disclosure_preprocessor import DisclosureProcessor
from strategies.agent_trading.watch.sec_filing_watcher import SEC_FEED_URL
from strategies.agent_trading.watch.sec_filing_watcher import SecWatcher


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
            headers={"User-Agent": self.user_agent},
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

    async def close(self) -> None:
        if self.session is None or self.sec is None or self.news is None:
            return
        try:
            await self.news.close()
            await self.sec.close()
        finally:
            await self.session.close()
            self.session = None
            self.sec = None
            self.news = None

    # SEC filing 或官方新闻稿中，第一个完整处理成功的来源触发分析。
    async def watch(
        self,
        plan: WatchPlan,
        context_dir: Path,
        internal_dir: Path,
    ) -> DisclosurePackage:
        if self.sec is None or self.news is None:
            raise RuntimeError("DisclosureWatcher is not started")
        now = datetime.now(UTC)
        if now < plan.start_at:
            await asyncio.sleep((plan.start_at - now).total_seconds())
        remaining = (plan.end_at - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise TimeoutError(f"watch window already ended: {plan.event_id}")

        context_dir = context_dir.resolve()
        internal_dir = internal_dir.resolve()
        context_dir.mkdir(parents=True, exist_ok=True)
        internal_dir.mkdir(parents=True, exist_ok=True)
        result = asyncio.get_running_loop().create_future()
        source_status = {
            "sec": self._source_status(),
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

        sec_target = WatchTarget(plan, context_dir, internal_dir, ready, fail, health)
        news_target = WatchTarget(
            plan,
            context_dir,
            internal_dir,
            ready,
            news_fail,
            health,
        )
        package = None
        try:
            self.sec.add(sec_target)
            self.news.add(news_target)
            try:
                package = await asyncio.wait_for(result, timeout=remaining)
            except TimeoutError as exc:
                if result.done() and not result.cancelled():
                    raise
                raise DisclosureTimeoutError(plan.event_id, source_status) from exc
            self._write_manifest(context_dir / "report.json", package)
            return package
        finally:
            await self.sec.remove(plan.event_id)
            await self.news.remove(plan.event_id)
            if package is not None:
                self._discard_loser(context_dir, internal_dir, package.source)

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
    def _discard_loser(context_dir: Path, internal_dir: Path, winner: str) -> None:
        loser = "news_release" if winner == "sec" else "sec"
        for root in (context_dir, internal_dir):
            disclosure_dir = (root / "disclosure").resolve()
            loser_dir = (disclosure_dir / loser).resolve()
            loser_dir.relative_to(disclosure_dir)
            if loser_dir.exists():
                shutil.rmtree(loser_dir)
