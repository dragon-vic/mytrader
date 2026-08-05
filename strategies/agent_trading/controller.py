from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from strategies.agent_trading.agents import AnalysisAgent
from strategies.agent_trading.agents import CodexRunner
from strategies.agent_trading.agents import ResearchDigestAgent
from strategies.agent_trading.agents import load_codex_profiles
from strategies.agent_trading.agents import ResearchAgent
from strategies.agent_trading.agents import ResearchOutcome
from strategies.agent_trading.event_store import EventStore
from strategies.agent_trading.lifecycle import BatchPlan
from strategies.agent_trading.lifecycle import MarketUniverse
from strategies.agent_trading.lifecycle import load_market_universe
from strategies.agent_trading.lifecycle import load_schedule
from strategies.agent_trading.lifecycle import validate_decision
from strategies.agent_trading.lifecycle import validate_research
from strategies.agent_trading.watch import DisclosurePackage
from strategies.agent_trading.watch import DisclosureTimeoutError
from strategies.agent_trading.watch import DisclosureWatcher
from strategies.agent_trading.watch import WatchPlan
from utils.config import load_settings


SEC_USER_AGENT = "nt_quant-agent-trading/1.0 victorice@yeah.net"
STRATEGY_ROOT = Path(__file__).resolve().parent
RESOURCES_DIR = STRATEGY_ROOT / "resources"
PROMPTS_DIR = RESOURCES_DIR / "prompts"
SCHEMAS_DIR = RESOURCES_DIR / "schemas"
SCHEDULE_PATH = RESOURCES_DIR / "schedules" / "2026-08-03_2026-08-07.json"
MARKET_UNIVERSE_PATH = SCHEDULE_PATH.with_name(
    f"{SCHEDULE_PATH.stem}_market_universe.json",
)
ANALYSIS_PROMPT = PROMPTS_DIR / "analysis.md"
RESEARCH_SCHEMA = SCHEMAS_DIR / "research.json"
DECISION_SCHEMA = SCHEMAS_DIR / "decision.json"
DIGEST_PROMPT = PROMPTS_DIR / "research_digest.md"
LOG = logging.getLogger(__name__)
FINISHED_EVENT_STATES = {"decision_ready", "decision_sent"}


# 外部 Agent 和 NT 共用的 JSON Schema 校验器集中在 controller，避免重复定义。
def _validator(name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMAS_DIR / name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


RESEARCH_VALIDATOR = _validator("research.json")
DECISION_VALIDATOR = _validator("decision.json")


class AgentController:
    def __init__(
        self,
        host: str,
        port: int,
        event_store: EventStore,
        research_agent: ResearchAgent,
        analysis_agent: AnalysisAgent,
        disclosure_watcher: DisclosureWatcher,
        research_digest_agent: ResearchDigestAgent | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.event_store = event_store
        self.research_agent = research_agent
        self.analysis_agent = analysis_agent
        self.disclosure_watcher = disclosure_watcher
        self.research_digest_agent = research_digest_agent

    # 分析结束时建立一次连接，将单条交易 JSON 发送进 NT 后关闭。
    async def send(self, payload: dict[str, Any]) -> None:
        line = (
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            .encode("utf-8")
            + b"\n"
        )
        _, writer = await asyncio.open_connection(self.host, self.port)
        try:
            writer.write(line)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def wait_until(self, moment: datetime) -> None:
        delay = (moment - datetime.now(UTC)).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)

    # batch真正启动时才创建目录；已有event配置保持不变，只补新增event。
    async def prepare_batch(
        self,
        batch: BatchPlan,
        market_universe: MarketUniverse,
    ) -> tuple[BatchPlan, dict[str, WatchPlan]]:
        stored_batch = self.event_store.create_batch(
            batch.batch_id,
            batch.to_dict(),
            market_universe.to_dict(),
        )
        batch = BatchPlan.from_dict(stored_batch)
        for event in batch.events:
            self.event_store.create_event(
                batch.batch_id,
                event.event_id,
                {
                    "batch_id": batch.batch_id,
                    "company": event.company,
                    "ticker": event.ticker,
                    "scope": event.scope,
                    "research_hints": list(event.research_hints),
                },
                event.watch_plan.to_dict(),
            )
        reused: set[str] = set()
        for event in batch.events:
            if self._reuse_batch_research(
                batch.batch_id,
                event.event_id,
                market_universe,
            ):
                reused.add(event.event_id)
        pending = tuple(
            event for event in batch.events if event.event_id not in reused
        )
        self.event_store.update_batch(
            batch.batch_id,
            "researching",
            reused_research=sorted(reused),
        )
        outcomes: dict[str, ResearchOutcome] = {}
        if pending:
            outcomes = await self.research_agent.run_batch(
                batch=replace(batch, events=pending),
                batch_dir=self.event_store.batch_paths(batch.batch_id).root,
                deadline=batch.watch_start_at,
            )

        plans: dict[str, WatchPlan] = {}
        for event in batch.events:
            ready = event.event_id in reused
            if not ready:
                ready = self._save_research_outcome(
                    batch.batch_id,
                    event.event_id,
                    outcomes[event.event_id],
                    market_universe,
                )
            state = self.event_store.load(batch.batch_id, event.event_id)
            if ready and state["state"] not in FINISHED_EVENT_STATES:
                plans[event.event_id] = event.watch_plan
        self.event_store.update_batch(
            batch.batch_id,
            "research_finished",
            ready_events=sorted(plans),
        )
        return batch, plans

    # 到达预研启动点后复核 batch 产物，并整理到分析事件目录。
    def _reuse_batch_research(
        self,
        batch_id: str,
        event_id: str,
        market_universe: MarketUniverse,
    ) -> bool:
        paths = self.event_store.event_paths(batch_id, event_id)
        if not paths.research.exists():
            return False
        research = self._load_object(paths.research, "event research")
        if research.get("research_complete") is not True:
            return False
        state = self.event_store.load(batch_id, event_id)
        return self._save_research_outcome(
            batch_id,
            event_id,
            ResearchOutcome(event_id=event_id, research=research, error=None),
            market_universe,
            preserve_state=state.get("research_complete") is True,
        )

    # 静态计划中的批次按财报数量动态等待，互不因其他批次失败而取消。
    async def run_schedule(
        self,
        batches: tuple[BatchPlan, ...],
        market_universe: MarketUniverse,
    ) -> None:
        active = tuple(
            batch
            for batch in batches
            if batch.watch_end_at > datetime.now(UTC)
        )
        tasks = {
            batch.batch_id: asyncio.create_task(
                self.run_batch(batch, market_universe),
                name=f"agent-trading-batch-{batch.batch_id}",
            )
            for batch in active
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        errors = [result for result in results if isinstance(result, Exception)]
        if errors:
            raise ExceptionGroup("agent trading batches failed", errors)

    def _save_research_outcome(
        self,
        batch_id: str,
        event_id: str,
        outcome: ResearchOutcome,
        market_universe: MarketUniverse,
        preserve_state: bool = False,
    ) -> bool:
        if not outcome.ready:
            self.event_store.update(
                batch_id,
                event_id,
                "research_incomplete",
                research_complete=False,
                research_error=outcome.error or "research_complete is false",
            )
            return False

        assert outcome.research is not None
        full = outcome.research
        try:
            RESEARCH_VALIDATOR.validate(full)
            validate_research(event_id, full, market_universe)
        except ValidationError as exc:
            error = f"ValidationError: {exc.message}"
        except (KeyError, TypeError, ValueError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        else:
            error = None
        if error is not None:
            self.event_store.update(
                batch_id,
                event_id,
                "research_incomplete",
                research_complete=False,
                research_error=error,
            )
            return False

        # 完整预研留在event中审计，分析目录只接收执行所需材料。
        paths = self.event_store.save_analysis_input(
            batch_id,
            event_id,
            {
                "event_id": event_id,
                "trade_candidates": full["trade_candidates"],
            },
            full["analysis_brief"],
        )
        if not preserve_state:
            self.event_store.update(
                batch_id,
                event_id,
                "research_ready",
                research_complete=True,
                research_error=None,
                research_path=paths.research.relative_to(paths.root).as_posix(),
                analysis_brief_path=(
                    paths.analysis_brief.relative_to(paths.root).as_posix()
                ),
            )
        return True

    # 所有公司的监听并发运行；一家公司完成或失败不会停止其余目标。
    async def run_batch(
        self,
        batch: BatchPlan,
        market_universe: MarketUniverse,
    ) -> None:
        try:
            await self.wait_until(batch.research_start_at)
            batch, plans = await self.prepare_batch(batch, market_universe)
            digest_task = None
            # 只有整批公司的预研都 ready 才发汇总邮件，避免把半成品误当成完整结论。
            if (
                self.research_digest_agent is not None
                and plans
                and len(plans) == len(batch.events)
            ):
                digest_task = asyncio.create_task(
                    self._run_research_digest(batch, tuple(plans)),
                    name=f"agent-trading-research-digest-{batch.batch_id}",
                )
            tasks = [
                asyncio.create_task(
                    self.run_event(batch.batch_id, event_id),
                    name=f"agent-trading-event-{event_id}",
                )
                for event_id in plans
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            errors = [result for result in results if isinstance(result, Exception)]
            if errors:
                raise ExceptionGroup("agent trading events failed", errors)
        except Exception as exc:
            LOG.exception(
                "agent trading batch failed batch_id=%s error_type=%s error=%r",
                batch.batch_id,
                type(exc).__name__,
                exc,
            )
            raise

    async def _run_research_digest(
        self,
        batch: BatchPlan,
        event_ids: tuple[str, ...],
    ) -> None:
        paths = self.event_store.batch_paths(batch.batch_id)
        try:
            await self.research_digest_agent.run(
                paths.root,
                event_ids,
            )
        except Exception as exc:
            LOG.exception(
                "research digest failed batch_id=%s error_type=%s error=%r",
                batch.batch_id,
                type(exc).__name__,
                exc,
            )

    async def run_event(
        self,
        batch_id: str,
        event_id: str,
    ) -> None:
        try:
            await self.wait_report(batch_id, event_id)
            decision = await self.run_analysis(batch_id, event_id)
            await self.send(decision)
            self.event_store.update(batch_id, event_id, "decision_sent")
        except Exception as exc:
            LOG.exception(
                "agent trading event failed event_id=%s error_type=%s error=%r",
                event_id,
                type(exc).__name__,
                exc,
            )
            details: dict[str, Any] = {}
            if isinstance(exc, DisclosureTimeoutError):
                details["source_status"] = exc.source_status
            self.event_store.update(
                batch_id,
                event_id,
                "failed",
                error=f"{type(exc).__name__}: {exc}",
                **details,
            )
            raise

    async def wait_report(
        self,
        batch_id: str,
        event_id: str,
    ) -> DisclosurePackage:
        paths = self.event_store.event_paths(batch_id, event_id)
        plan = WatchPlan.from_dict(
            self._load_object(paths.watch_plan, "watch plan"),
        )
        if plan.event_id != event_id:
            raise ValueError(
                f"stored watch plan event_id mismatch: {plan.event_id} != {event_id}",
            )
        self.event_store.update(batch_id, event_id, "watching_disclosure")
        package = await self.disclosure_watcher.watch(
            plan,
            paths.analysis_input,
            paths.watch,
        )
        self.event_store.update(
            batch_id,
            event_id,
            "report_ready",
            report_path=paths.report.relative_to(paths.root).as_posix(),
            report_source=package.source,
            report_detected_ns=package.detected_ns,
        )
        return package

    async def run_analysis(self, batch_id: str, event_id: str) -> dict[str, Any]:
        paths = self.event_store.event_paths(batch_id, event_id)
        self.event_store.update(batch_id, event_id, "analyzing")
        await self.analysis_agent.run(
            ANALYSIS_PROMPT.read_text(encoding="utf-8"),
            paths.analysis_input,
            paths.decision,
            DECISION_SCHEMA,
        )
        decision = self._load_object(paths.decision, "decision")
        DECISION_VALIDATOR.validate(decision)
        research = self._load_object(paths.analysis_research, "research")
        validate_decision(event_id, decision, research)
        self.event_store.update(
            batch_id,
            event_id,
            "decision_ready",
            decision_path=paths.decision.relative_to(paths.root).as_posix(),
        )
        return decision

    @staticmethod
    def _load_object(path: Path, name: str) -> dict[str, Any]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError(f"{name} must be a JSON object")
        return payload


async def main() -> None:
    settings = load_settings("agent_trading", "live")
    profiles = load_codex_profiles(settings)
    runner = CodexRunner()
    batches = load_schedule(SCHEDULE_PATH)
    market_universe = load_market_universe(MARKET_UNIVERSE_PATH)
    research_agent = ResearchAgent(
        runner,
        PROMPTS_DIR,
        SCHEMAS_DIR,
        profile=profiles.research,
    )
    research_digest_agent = ResearchDigestAgent(
        runner,
        DIGEST_PROMPT,
        profile=profiles.digest,
    )
    async with DisclosureWatcher(
        user_agent=SEC_USER_AGENT,
        poll_seconds=0.5,
    ) as watcher:
        controller = AgentController(
            host="127.0.0.1",
            port=9003,
            event_store=EventStore(STRATEGY_ROOT),
            research_agent=research_agent,
            analysis_agent=AnalysisAgent(runner, profile=profiles.analysis),
            research_digest_agent=research_digest_agent,
            disclosure_watcher=watcher,
        )
        await controller.run_schedule(batches, market_universe)


if __name__ == "__main__":
    asyncio.run(main())
