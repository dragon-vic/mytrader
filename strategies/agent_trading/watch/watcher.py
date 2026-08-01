from __future__ import annotations

import asyncio
import json
import logging
import shutil
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from pathlib import Path

import aiohttp

from strategies.agent_trading.watch.models import DisclosurePackage
from strategies.agent_trading.watch.models import WatchPlan
from strategies.agent_trading.watch.models import WatchTarget
from strategies.agent_trading.watch.news import NewsReleaseWatcher
from strategies.agent_trading.watch.processor import DisclosureProcessor
from strategies.agent_trading.watch.sec import SEC_FEED_URL
from strategies.agent_trading.watch.sec import SecWatcher


LOG = logging.getLogger(__name__)


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

    # 新闻稿可以提前下载，但只有完整 SEC filing 才能触发一次分析。
    async def watch(self, plan: WatchPlan, context_dir: Path) -> DisclosurePackage:
        if self.sec is None or self.news is None:
            raise RuntimeError("DisclosureWatcher is not started")
        now = datetime.now(UTC)
        if now < plan.start_at:
            await asyncio.sleep((plan.start_at - now).total_seconds())
        remaining = (plan.end_at - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise TimeoutError(f"watch window already ended: {plan.event_id}")

        context_dir = context_dir.resolve()
        context_dir.mkdir(parents=True, exist_ok=True)
        result = asyncio.get_running_loop().create_future()
        news_package = None

        async def sec_ready(package: DisclosurePackage) -> bool:
            if result.done():
                return False
            result.set_result(package)
            return True

        async def news_ready(package: DisclosurePackage) -> bool:
            nonlocal news_package
            if news_package is None:
                news_package = package
            return True

        def fail(exc: Exception) -> None:
            if not result.done():
                result.set_exception(exc)

        def news_fail(exc: Exception) -> None:
            LOG.warning("news watch failed event_id=%s error=%s", plan.event_id, exc)

        sec_target = WatchTarget(plan, context_dir, sec_ready, fail)
        news_target = WatchTarget(plan, context_dir, news_ready, news_fail)
        package = None
        try:
            self.sec.add(sec_target)
            self.news.add(news_target)
            package = await asyncio.wait_for(result, timeout=remaining)
            if news_package is not None and not any(
                item.document_type.upper().startswith("EX-99")
                for item in package.files
            ):
                package = replace(
                    package,
                    files=package.files + news_package.files,
                )
            self._write_manifest(context_dir / "report.json", package)
            return package
        finally:
            await self.sec.remove(plan.event_id)
            await self.news.remove(plan.event_id)
            if package is None or not any(
                item.document_type == "NEWS_RELEASE" for item in package.files
            ):
                self._discard_news(context_dir)

    @staticmethod
    def _write_manifest(path: Path, package: DisclosurePackage) -> None:
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(package.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(path)

    @staticmethod
    def _discard_news(context_dir: Path) -> None:
        disclosure_dir = (context_dir / "disclosure").resolve()
        news_dir = (disclosure_dir / "news_release").resolve()
        news_dir.relative_to(disclosure_dir)
        if news_dir.exists():
            shutil.rmtree(news_dir)
