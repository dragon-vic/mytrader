from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

from strategies.agent_trading.agents import AnalysisAgent
from strategies.agent_trading.agents import CodexRunner
from strategies.agent_trading.agents import ResearchAgent
from strategies.agent_trading.agents import ResearchOutcome
from strategies.agent_trading.agents import ScheduleAgent
from strategies.agent_trading.event_store import EventStore
from strategies.agent_trading.lifecycle import BatchPlan
from strategies.agent_trading.lifecycle import MarketUniverse
from strategies.agent_trading.lifecycle import validate_decision
from strategies.agent_trading.lifecycle import validate_research
from strategies.agent_trading.market import MarketSnapshotter
from strategies.agent_trading.market import RestMarketSnapshotter
from strategies.agent_trading.watch import DisclosurePackage
from strategies.agent_trading.watch import DisclosureWatcher
from strategies.agent_trading.watch import WatchPlan


SEC_USER_AGENT = "nt_quant-agent-trading/1.0 victorice@yeah.net"
STRATEGY_ROOT = Path(__file__).resolve().parent
PROMPTS_DIR = STRATEGY_ROOT / "prompts"
SCHEMAS_DIR = STRATEGY_ROOT / "schemas"
ANALYSIS_PROMPT = PROMPTS_DIR / "analysis.md"
DECISION_SCHEMA = SCHEMAS_DIR / "decision.json"


class AgentController:
    def __init__(
        self,
        host: str,
        port: int,
        event_store: EventStore,
        schedule_agent: ScheduleAgent,
        research_agent: ResearchAgent,
        analysis_agent: AnalysisAgent,
        disclosure_watcher: DisclosureWatcher,
        market_snapshotter: MarketSnapshotter,
    ) -> None:
        self.host = host
        self.port = port
        self.event_store = event_store
        self.schedule_agent = schedule_agent
        self.research_agent = research_agent
        self.analysis_agent = analysis_agent
        self.disclosure_watcher = disclosure_watcher
        self.market_snapshotter = market_snapshotter
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.send_lock = asyncio.Lock()

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def close(self) -> None:
        if self.writer is None:
            return
        self.writer.close()
        await self.writer.wait_closed()
        self.reader = None
        self.writer = None

    async def send(self, payload: dict[str, Any]) -> None:
        if self.writer is None:
            raise RuntimeError("AgentController is not connected")
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        async with self.send_lock:
            self.writer.write(line)
            await self.writer.drain()

    async def receive(self) -> dict[str, Any]:
        if self.reader is None:
            raise RuntimeError("AgentController is not connected")
        line = await self.reader.readline()
        if not line:
            raise ConnectionError("ExternalJson connection closed")
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise TypeError("payload must be a JSON object")
        return payload

    # ScheduleAgent 只生成下一个盘前或盘后批次，不承担深度公司研究。
    async def build_schedule(self, work_dir: Path, output_path: Path) -> BatchPlan:
        return await self.schedule_agent.run(work_dir, output_path)

    async def wait_until(self, timestamp_ns: int) -> None:
        delay = (timestamp_ns - time.time_ns()) / 1_000_000_000
        if delay > 0:
            await asyncio.sleep(delay)

    # 批次研究在监听前四小时启动；公司最终材料和完整性状态分别落盘。
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
                    "expected_at": event.expected_at.isoformat(),
                    "relevance_reason": event.relevance_reason,
                },
            )
        self.event_store.update_batch(
            batch.batch_id,
            "waiting_for_research",
            research_start_at=batch.research_start_at.isoformat(),
        )
        await self.wait_until(int(batch.research_start_at.timestamp() * 1_000_000_000))
        self.event_store.update_batch(batch.batch_id, "researching")
        outcomes = await self.research_agent.run_batch(
            batch=batch,
            batch_dir=self.event_store.batch_paths(batch.batch_id).root,
            deadline=batch.watch_start_at,
        )

        plans: dict[str, WatchPlan] = {}
        statuses: dict[str, str] = {}
        for event in batch.events:
            outcome = outcomes[event.event_id]
            plan = self._save_research_outcome(
                event.event_id,
                outcome,
                market_universe,
            )
            if plan is not None:
                plans[event.event_id] = plan
            state = self.event_store.load(event.event_id)
            statuses[event.event_id] = str(state["state"])
        self.event_store.update_batch(
            batch.batch_id,
            "research_finished",
            companies=statuses,
        )
        return plans

    def _save_research_outcome(
        self,
        event_id: str,
        outcome: ResearchOutcome,
        market_universe: MarketUniverse,
    ) -> WatchPlan | None:
        if outcome.research is None:
            self.event_store.update(
                event_id,
                "research_incomplete",
                research_complete=False,
                research_error=outcome.error,
            )
            return None

        full = outcome.research
        report = full.get("research_report")
        brief = full.get("analysis_brief")
        if not isinstance(report, str) or not isinstance(brief, str):
            self.event_store.update(
                event_id,
                "research_incomplete",
                research_complete=False,
                research_error="research report or analysis brief is missing",
            )
            return None

        structured = {
            key: value
            for key, value in full.items()
            if key not in {"research_report", "analysis_brief"}
        }
        self.event_store.save_report(event_id, report)
        self.event_store.save_brief(event_id, brief)
        self.event_store.save_research(event_id, structured)

        plan: WatchPlan | None = None
        local_error: str | None = None
        try:
            validate_research(event_id, full, market_universe)
        except (KeyError, TypeError, ValueError) as exc:
            local_error = f"{type(exc).__name__}: {exc}"
        try:
            watch_payload = full.get("watch_plan")
            if not isinstance(watch_payload, dict):
                raise TypeError("research watch_plan must be a JSON object")
            plan = self.set_watch_plan(event_id, watch_payload)
        except (KeyError, TypeError, ValueError) as exc:
            watch_error = f"{type(exc).__name__}: {exc}"
            local_error = f"{local_error}; {watch_error}" if local_error else watch_error

        complete = outcome.ready and local_error is None
        self.event_store.update(
            event_id,
            "research_ready" if complete else "research_incomplete",
            research_complete=complete,
            research_error=local_error or outcome.error,
            research_path=str(self.event_store.paths(event_id).research),
            analysis_brief_path=str(self.event_store.paths(event_id).analysis_brief),
        )
        return plan

    def set_watch_plan(self, event_id: str, payload: dict[str, Any]) -> WatchPlan:
        plan = WatchPlan.from_dict(payload)
        if plan.event_id != event_id:
            raise ValueError(
                f"watch plan event_id mismatch: {plan.event_id} != {event_id}",
            )
        self.event_store.save_plan(event_id, plan.to_dict())
        return plan

    # 所有公司的监听并发运行；一家公司完成或失败不会停止其余目标。
    async def run_batch(
        self,
        batch: BatchPlan,
        market_universe: MarketUniverse,
    ) -> None:
        plans = await self.prepare_batch(batch, market_universe)
        tasks = [
            asyncio.create_task(
                self.run_event(event_id, market_universe),
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
        market_universe: MarketUniverse,
    ) -> None:
        try:
            await self.wait_report(event_id)
            state = self.event_store.load(event_id)
            if not state.get("research_complete", False):
                self.event_store.update(event_id, "report_saved_research_incomplete")
                return
            await self.capture_market(event_id, market_universe)
            await self.run_analysis(event_id)
            await self.send_decision(event_id)
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
        package = await self.disclosure_watcher.watch(plan, paths.context)
        self.event_store.update(
            event_id,
            "report_ready",
            report_path=str(paths.report),
            report_source=package.source,
            report_detected_ns=package.detected_ns,
        )
        return package

    async def capture_market(
        self,
        event_id: str,
        market_universe: MarketUniverse,
    ) -> dict[str, Any]:
        research = self._load_object(
            self.event_store.paths(event_id).research,
            "research",
        )
        candidate_ids = [
            item["instrument_id"]
            for item in research["trade_candidates"]
        ]
        instruments = tuple(market_universe.get(value) for value in candidate_ids)
        snapshot = await self.market_snapshotter.capture(instruments)
        self.event_store.save_snapshot(event_id, snapshot)
        self.event_store.update(event_id, "market_snapshot_ready")
        return snapshot

    async def run_analysis(self, event_id: str) -> str:
        paths = self.event_store.paths(event_id)
        self.event_store.update(event_id, "analyzing")
        result = await self.analysis_agent.run(
            ANALYSIS_PROMPT.read_text(encoding="utf-8"),
            paths.root,
            paths.decision,
            DECISION_SCHEMA,
        )
        decision = self._load_object(paths.decision, "decision")
        research = self._load_object(paths.research, "research")
        validate_decision(event_id, decision, research)
        self.event_store.update(
            event_id,
            "decision_ready",
            decision_path=str(paths.decision),
        )
        return result

    async def send_decision(self, event_id: str) -> None:
        payload = self._load_object(
            self.event_store.paths(event_id).decision,
            "decision",
        )
        await self.send(payload)
        self.event_store.update(event_id, "decision_sent")

    async def receive_forever(self) -> None:
        while True:
            payload = await self.receive()
            print(json.dumps(payload, ensure_ascii=False), flush=True)

    @staticmethod
    def _load_object(path: Path, name: str) -> dict[str, Any]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError(f"{name} must be a JSON object")
        return payload


async def main() -> None:
    runner = CodexRunner()
    schedule_agent = ScheduleAgent(runner, PROMPTS_DIR, SCHEMAS_DIR)
    research_agent = ResearchAgent(runner, PROMPTS_DIR, SCHEMAS_DIR)
    async with DisclosureWatcher(
        user_agent=SEC_USER_AGENT,
        poll_seconds=0.5,
    ) as watcher, RestMarketSnapshotter() as snapshotter:
        controller = AgentController(
            host="127.0.0.1",
            port=9003,
            event_store=EventStore(STRATEGY_ROOT),
            schedule_agent=schedule_agent,
            research_agent=research_agent,
            analysis_agent=AnalysisAgent(runner),
            disclosure_watcher=watcher,
            market_snapshotter=snapshotter,
        )
        await controller.connect()
        try:
            await controller.receive_forever()
        finally:
            await controller.close()


if __name__ == "__main__":
    asyncio.run(main())
