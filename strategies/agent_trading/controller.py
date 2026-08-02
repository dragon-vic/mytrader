from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from jsonschema.exceptions import ValidationError

from strategies.agent_trading.agents import AnalysisAgent
from strategies.agent_trading.agents import CodexRunner
from strategies.agent_trading.agents import ResearchAgent
from strategies.agent_trading.agents import ResearchOutcome
from strategies.agent_trading.contracts import DECISION_VALIDATOR
from strategies.agent_trading.contracts import RESEARCH_VALIDATOR
from strategies.agent_trading.event_store import EventStore
from strategies.agent_trading.lifecycle import BatchPlan
from strategies.agent_trading.lifecycle import MarketUniverse
from strategies.agent_trading.lifecycle import load_market_universe
from strategies.agent_trading.lifecycle import load_schedule
from strategies.agent_trading.lifecycle import validate_decision
from strategies.agent_trading.lifecycle import validate_research
from strategies.agent_trading.watch import DisclosurePackage
from strategies.agent_trading.watch import DisclosureWatcher
from strategies.agent_trading.watch import WatchPlan


SEC_USER_AGENT = "nt_quant-agent-trading/1.0 victorice@yeah.net"
STRATEGY_ROOT = Path(__file__).resolve().parent
PROMPTS_DIR = STRATEGY_ROOT / "prompts"
SCHEMAS_DIR = STRATEGY_ROOT / "schemas"
SCHEDULE_PATH = STRATEGY_ROOT / "schedules" / "2026-08-03_2026-08-07.json"
MARKET_UNIVERSE_PATH = SCHEDULE_PATH.with_name(
    f"{SCHEDULE_PATH.stem}_market_universe.json",
)
ANALYSIS_PROMPT = PROMPTS_DIR / "analysis.md"
RESEARCH_SCHEMA = SCHEMAS_DIR / "research.json"
DECISION_SCHEMA = SCHEMAS_DIR / "decision.json"


class AgentController:
    def __init__(
        self,
        host: str,
        port: int,
        event_store: EventStore,
        research_agent: ResearchAgent,
        analysis_agent: AnalysisAgent,
        disclosure_watcher: DisclosureWatcher,
    ) -> None:
        self.host = host
        self.port = port
        self.event_store = event_store
        self.research_agent = research_agent
        self.analysis_agent = analysis_agent
        self.disclosure_watcher = disclosure_watcher

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

    # 预设 watcher 计划先落盘；批次研究在监听前四小时独立启动。
    async def prepare_batch(
        self,
        batch: BatchPlan,
        market_universe: MarketUniverse,
    ) -> dict[str, WatchPlan]:
        self.event_store.create_batch(
            batch.batch_id,
            batch.to_dict(),
            market_universe.to_dict(),
        )
        for event in batch.events:
            self.event_store.create(
                event.event_id,
                {
                    "batch_id": batch.batch_id,
                    "company": event.company,
                    "ticker": event.ticker,
                    "scope": event.scope,
                    "research_hints": list(event.research_hints),
                },
            )
            self.event_store.save_plan(event.event_id, event.watch_plan.to_dict())
        self.event_store.update_batch(
            batch.batch_id,
            "waiting_for_research",
            research_start_at=batch.research_start_at.isoformat(),
        )
        await self.wait_until(batch.research_start_at)

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
        statuses: dict[str, str] = {}
        for event in batch.events:
            ready = event.event_id in reused
            if not ready:
                ready = self._save_research_outcome(
                    event.event_id,
                    outcomes[event.event_id],
                    market_universe,
                )
            if ready:
                plans[event.event_id] = event.watch_plan
            state = self.event_store.load(event.event_id)
            statuses[event.event_id] = str(state["state"])
        self.event_store.update_batch(
            batch.batch_id,
            "research_finished",
            companies=statuses,
        )
        return plans

    # 到达预研启动点后复核 batch 产物，并整理到分析事件目录。
    def _reuse_batch_research(
        self,
        batch_id: str,
        event_id: str,
        market_universe: MarketUniverse,
    ) -> bool:
        batch_state = self.event_store.load_batch(batch_id)
        companies = batch_state.get("companies", {})
        if not isinstance(companies, dict):
            raise TypeError(f"batch companies must be a JSON object: {batch_id}")
        if companies.get(event_id) != "research_ready":
            return False
        research_path = (
            self.event_store.batch_paths(batch_id).work
            / event_id
            / "research.json"
        )
        research = self._load_object(research_path, "batch research")
        return self._save_research_outcome(
            event_id,
            ResearchOutcome(event_id=event_id, research=research, error=None),
            market_universe,
        )

    # 静态计划中的批次各自等待 T-4h，互不因其他批次失败而取消。
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
        event_id: str,
        outcome: ResearchOutcome,
        market_universe: MarketUniverse,
    ) -> bool:
        if not outcome.ready:
            self.event_store.update(
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
                event_id,
                "research_incomplete",
                research_complete=False,
                research_error=error,
            )
            return False

        # 分析只接收执行所需的候选表，完整报告留在 batch work 目录审计。
        self.event_store.save_brief(event_id, full["analysis_brief"])
        self.event_store.save_research(
            event_id,
            {
                "event_id": event_id,
                "trade_candidates": full["trade_candidates"],
            },
        )
        self.event_store.update(
            event_id,
            "research_ready",
            research_complete=True,
            research_error=None,
            research_path=str(self.event_store.paths(event_id).research),
            analysis_brief_path=str(self.event_store.paths(event_id).analysis_brief),
        )
        return True

    # 所有公司的监听并发运行；一家公司完成或失败不会停止其余目标。
    async def run_batch(
        self,
        batch: BatchPlan,
        market_universe: MarketUniverse,
    ) -> None:
        plans = await self.prepare_batch(batch, market_universe)
        tasks = [
            asyncio.create_task(
                self.run_event(event_id),
                name=f"agent-trading-event-{event_id}",
            )
            for event_id in plans
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [result for result in results if isinstance(result, Exception)]
        if errors:
            raise ExceptionGroup("agent trading events failed", errors)

    async def run_event(
        self,
        event_id: str,
    ) -> None:
        try:
            await self.wait_report(event_id)
            decision = await self.run_analysis(event_id)
            await self.send(decision)
            self.event_store.update(event_id, "decision_sent")
        except Exception as exc:
            self.event_store.update(
                event_id,
                "failed",
                error=f"{type(exc).__name__}: {exc}",
            )
            raise

    async def wait_report(self, event_id: str) -> DisclosurePackage:
        paths = self.event_store.paths(event_id)
        plan = WatchPlan.from_dict(self.event_store.load_plan(event_id))
        if plan.event_id != event_id:
            raise ValueError(
                f"stored watch plan event_id mismatch: {plan.event_id} != {event_id}",
            )
        self.event_store.update(event_id, "watching_disclosure")
        package = await self.disclosure_watcher.watch(
            plan,
            paths.analysis_input,
            paths.internal,
        )
        self.event_store.update(
            event_id,
            "report_ready",
            report_path=str(paths.report),
            report_source=package.source,
            report_detected_ns=package.detected_ns,
        )
        return package

    async def run_analysis(self, event_id: str) -> dict[str, Any]:
        paths = self.event_store.paths(event_id)
        self.event_store.update(event_id, "analyzing")
        await self.analysis_agent.run(
            ANALYSIS_PROMPT.read_text(encoding="utf-8"),
            paths.analysis_input,
            paths.decision,
            DECISION_SCHEMA,
        )
        decision = self._load_object(paths.decision, "decision")
        DECISION_VALIDATOR.validate(decision)
        research = self._load_object(paths.research, "research")
        validate_decision(event_id, decision, research)
        self.event_store.update(
            event_id,
            "decision_ready",
            decision_path=str(paths.decision),
        )
        return decision

    @staticmethod
    def _load_object(path: Path, name: str) -> dict[str, Any]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError(f"{name} must be a JSON object")
        return payload


async def main() -> None:
    runner = CodexRunner()
    batches = load_schedule(SCHEDULE_PATH)
    market_universe = load_market_universe(MARKET_UNIVERSE_PATH)
    research_agent = ResearchAgent(runner, PROMPTS_DIR, SCHEMAS_DIR)
    async with DisclosureWatcher(
        user_agent=SEC_USER_AGENT,
        poll_seconds=0.5,
    ) as watcher:
        controller = AgentController(
            host="127.0.0.1",
            port=9003,
            event_store=EventStore(STRATEGY_ROOT),
            research_agent=research_agent,
            analysis_agent=AnalysisAgent(runner),
            disclosure_watcher=watcher,
        )
        await controller.run_schedule(batches, market_universe)


if __name__ == "__main__":
    asyncio.run(main())
