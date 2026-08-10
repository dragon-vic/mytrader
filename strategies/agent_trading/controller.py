from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from strategies.agent_trading.agents import AnalysisAgent
from strategies.agent_trading.agents import CodexProfile
from strategies.agent_trading.agents import ResearchAgent
from strategies.agent_trading.agents import ResearchOutcome
from strategies.agent_trading.event_store import EventStore
from strategies.agent_trading.lifecycle import BatchPlan
from strategies.agent_trading.lifecycle import EventSpec
from strategies.agent_trading.lifecycle import MarketUniverse
from strategies.agent_trading.lifecycle import load_event_plan
from strategies.agent_trading.lifecycle import load_event_schedule
from strategies.agent_trading.lifecycle import load_market_universe
from strategies.agent_trading.lifecycle import validate_decision
from strategies.agent_trading.watch import DisclosurePackage
from strategies.agent_trading.watch import DisclosureTimeoutError
from strategies.agent_trading.watch import DisclosureWatcher
from strategies.agent_trading.watch import WatchPlan
from tools.codex_agent import CodexRunner


SEC_USER_AGENT = "nt_quant-agent-trading/1.0 victorice@yeah.net"
STRATEGY_ROOT = Path(__file__).resolve().parent
RESOURCES_DIR = STRATEGY_ROOT / "resources"
PROMPTS_DIR = RESOURCES_DIR / "prompts"
SCHEMAS_DIR = RESOURCES_DIR / "schemas"
SCHEDULE_PATH = RESOURCES_DIR / "schedules" / "2026-08.json"
MARKET_UNIVERSE_PATH = SCHEDULE_PATH.with_name(
    f"{SCHEDULE_PATH.stem}_market_universe.json",
)
ANALYSIS_PROMPT = PROMPTS_DIR / "analysis.md"
DECISION_SCHEMA = SCHEMAS_DIR / "decision.json"
EMAIL_TOOL = STRATEGY_ROOT.parents[1] / "tools" / "send_email.py"
LOG = logging.getLogger(__name__)
FINISHED_EVENT_STATES = {"decision_ready", "decision_sent"}
SCHEDULE_RELOAD_SECONDS = 60.0
LIFECYCLE_POLL_SECONDS = 1.0

# Codex 是 controller 的外部依赖，不从 NT 的 live_config.yaml 读取参数。
RESEARCH_PROFILE = CodexProfile(
    model="gpt-5.6-sol",
    reasoning_effort="xhigh",
    subagent_threads=3,
    subagent_model="gpt-5.6-terra",
    subagent_reasoning_effort="high",
)
ANALYSIS_PROFILE = CodexProfile(
    model="gpt-5.6-sol",
    reasoning_effort="medium",
    service_tier="fast",
)


# 外部 Agent 和 NT 共用的 JSON Schema 校验器集中在 controller，避免重复定义。
def _validator(name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMAS_DIR / name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


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
    ) -> None:
        self.host = host
        self.port = port
        self.event_store = event_store
        self.research_agent = research_agent
        self.analysis_agent = analysis_agent
        self.disclosure_watcher = disclosure_watcher
        self.batch_locks: dict[str, asyncio.Lock] = {}
        self.research_lock = asyncio.Lock()

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

    # Event 到达生命周期节点时才建立运行快照；已有快照保持不变。
    async def _prepare_event(
        self,
        batch: BatchPlan,
        event: EventSpec,
        market_universe: MarketUniverse,
    ) -> None:
        lock = self.batch_locks.setdefault(batch.batch_id, asyncio.Lock())
        async with lock:
            self.event_store.create_batch(
                batch.batch_id,
                batch.to_dict(),
                market_universe.to_dict(),
            )
            self.event_store.update_batch(batch.batch_id, "running")
        self.event_store.create_event(
            batch.batch_id,
            event.event_id,
            {
                "batch_id": batch.batch_id,
                "company": event.company,
                "ticker": event.ticker,
                "scope": event.scope,
                "confirmed": event.confirmed,
                "research_hints": list(event.research_hints),
            },
            event.watch_plan.to_dict(),
        )

    # 已有预研只复用，不触发邮件；watch 开始时还会再次读取和校验。
    def _reuse_event_research(
        self,
        batch_id: str,
        event_id: str,
    ) -> bool:
        paths = self.event_store.event_paths(batch_id, event_id)
        if not paths.research.exists():
            return False
        try:
            state = self.event_store.load(batch_id, event_id)
            memo = paths.research.read_text(encoding="utf-8")
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError):
            return False
        return (
            state.get("research_complete") is True
            and isinstance(state.get("research_session_id"), str)
            and bool(state["research_session_id"].strip())
            and bool(memo.strip())
        )

    # 持续重读现有 schedule；只有到点且校验通过的 event 才启动。
    async def run_schedule(
        self,
        schedule_path: Path,
        market_universe_path: Path,
    ) -> None:
        tasks: dict[str, asyncio.Task[None]] = {}
        reported: set[str] = set()
        event_errors: dict[str, str] = {}
        schedule_errors: set[str] = set()
        schedule_error: str | None = None
        while True:
            try:
                snapshot = load_event_schedule(schedule_path, datetime.now(UTC))
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                if error != schedule_error:
                    LOG.exception("dynamic schedule reload failed error=%s", error)
                    schedule_error = error
                await asyncio.sleep(SCHEDULE_RELOAD_SECONDS)
                continue
            schedule_error = None
            current_errors = set(snapshot.errors)
            for error in sorted(current_errors - schedule_errors):
                LOG.error("dynamic schedule entry rejected error=%s", error)
            schedule_errors = current_errors

            for scheduled in snapshot.events:
                if scheduled.event_id in tasks:
                    continue
                try:
                    batch, event = load_event_plan(
                        schedule_path,
                        scheduled.batch_id,
                        scheduled.event_id,
                    )
                    market_universe = load_market_universe(market_universe_path)
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    if event_errors.get(scheduled.event_id) != error:
                        LOG.error(
                            "dynamic event rejected event_id=%s error=%s",
                            scheduled.event_id,
                            error,
                        )
                        event_errors[scheduled.event_id] = error
                    continue
                event_errors.pop(scheduled.event_id, None)
                tasks[scheduled.event_id] = asyncio.create_task(
                    self.run_event(
                        batch,
                        event,
                        market_universe,
                    ),
                    name=f"agent-trading-event-{scheduled.event_id}",
                )

            for event_id, task in tasks.items():
                if not task.done() or event_id in reported:
                    continue
                reported.add(event_id)
                if task.cancelled():
                    LOG.warning("dynamic event task cancelled event_id=%s", event_id)
                    continue
                error = task.exception()
                if error is not None:
                    LOG.error(
                        "dynamic event task failed event_id=%s "
                        "error_type=%s error=%r",
                        event_id,
                        type(error).__name__,
                        error,
                    )
            await asyncio.sleep(SCHEDULE_RELOAD_SECONDS)

    def _save_research_outcome(
        self,
        batch_id: str,
        event_id: str,
        outcome: ResearchOutcome,
    ) -> bool:
        if not outcome.ready:
            self.event_store.update(
                batch_id,
                event_id,
                "research_incomplete",
                research_complete=False,
                research_error=outcome.error or "research memo or session id is missing",
            )
            return False

        if outcome.event_id != event_id:
            raise ValueError(
                f"research event_id mismatch: {outcome.event_id} != {event_id}",
            )

        paths = self.event_store.event_paths(batch_id, event_id)
        self.event_store.update(
            batch_id,
            event_id,
            "research_ready",
            research_complete=True,
            research_error=None,
            research_session_id=outcome.session_id,
            research_completed_at=datetime.now(UTC).isoformat(),
            research_path=paths.research.relative_to(paths.root).as_posix(),
        )
        return True

    def _finalize_analysis_input(
        self,
        batch_id: str,
        event_id: str,
    ) -> bool:
        paths = self.event_store.event_paths(batch_id, event_id)
        try:
            state = self.event_store.load(batch_id, event_id)
            memo = paths.research.read_text(encoding="utf-8")
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        else:
            session_id = state.get("research_session_id")
            if state.get("research_complete") is not True:
                error = "research_complete is false"
            elif not isinstance(session_id, str) or not session_id.strip():
                error = "research_session_id is missing"
            elif not memo.strip():
                error = "research memo is empty"
            elif not paths.analysis_event.exists():
                error = "analysis event input is missing"
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

        self.event_store.update(
            batch_id,
            event_id,
            "analysis_input_ready",
            research_complete=True,
            research_error=None,
        )
        return True

    async def _wait_for_watch_start(
        self,
        batch_id: str,
        event_id: str,
    ) -> None:
        plan_path = self.event_store.event_paths(batch_id, event_id).watch_plan
        while True:
            plan = WatchPlan.from_dict(self._load_object(plan_path, "watch plan"))
            now = datetime.now(UTC)
            if now >= plan.end_at:
                raise TimeoutError(f"event watch window expired: {event_id}")
            if now >= plan.start_at:
                return
            await asyncio.sleep(LIFECYCLE_POLL_SECONDS)

    async def run_event(
        self,
        batch: BatchPlan,
        event: EventSpec,
        market_universe: MarketUniverse,
    ) -> None:
        event_id = event.event_id
        email_task: asyncio.Task[None] | None = None
        try:
            paths = self.event_store.event_paths(batch.batch_id, event_id)
            if paths.state.exists():
                state = self.event_store.load(batch.batch_id, event_id)
                if state["state"] in FINISHED_EVENT_STATES:
                    return
            await self._prepare_event(batch, event, market_universe)
            ready = self._reuse_event_research(
                batch.batch_id,
                event_id,
            )
            if not ready:
                async with self.research_lock:
                    ready = self._reuse_event_research(
                        batch.batch_id,
                        event_id,
                    )
                    if not ready:
                        self.event_store.update(
                            batch.batch_id,
                            event_id,
                            "researching",
                        )
                        outcome = await self.research_agent.run_event(
                            event_id=event_id,
                            batch_dir=self.event_store.batch_paths(batch.batch_id).root,
                            deadline=batch.watch_start_at,
                        )
                        ready = self._save_research_outcome(
                            batch.batch_id,
                            event_id,
                            outcome,
                        )
                if ready:
                    email_task = asyncio.create_task(
                        self._send_research_email(batch, event),
                        name=f"agent-trading-research-email-{event_id}",
                    )

            await self._wait_for_watch_start(
                batch.batch_id,
                event_id,
            )
            if not self._finalize_analysis_input(
                batch.batch_id,
                event_id,
            ):
                LOG.error(
                    "event analysis input incomplete event_id=%s",
                    event_id,
                )
                return
            await self._run_disclosure_and_analysis(
                batch.batch_id,
                event_id,
            )
        except Exception as exc:
            LOG.exception(
                "agent trading event failed event_id=%s error_type=%s error=%r",
                event_id,
                type(exc).__name__,
                exc,
            )
            try:
                self.event_store.update(
                    batch.batch_id,
                    event_id,
                    "failed",
                    error=f"{type(exc).__name__}: {exc}",
                )
            except Exception as state_exc:
                LOG.error(
                    "event failure state unavailable event_id=%s "
                    "error_type=%s error=%r",
                    event_id,
                    type(state_exc).__name__,
                    state_exc,
                )
        finally:
            if email_task is not None:
                await asyncio.gather(email_task, return_exceptions=True)

    async def _send_research_email(
        self,
        batch: BatchPlan,
        event: EventSpec,
    ) -> None:
        input_path = self.event_store.event_paths(
            batch.batch_id,
            event.event_id,
        ).research
        custom_prompt = (
            f"这是 `{batch.batch_id}` 批次中 `{event.event_id}` "
            f"（{event.company}/{event.ticker}）的单个事件预研。"
            "邮件只总结这个 event，不等待或引用同批次其他 event。"
            "邮件主题必须包含 batch id 和 event id，并明确这是财报发布前预研。"
        )
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(EMAIL_TOOL),
                str(input_path),
                "--prompt",
                custom_prompt,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _stdout, stderr = await process.communicate()
            if process.returncode != 0:
                detail = stderr.decode("utf-8", errors="replace").strip()
                raise RuntimeError(
                    f"send_email.py failed with code {process.returncode}: "
                    f"{detail or 'no diagnostic output'}",
                )
        except Exception as exc:
            LOG.exception(
                "research email failed batch_id=%s event_id=%s "
                "error_type=%s error=%r",
                batch.batch_id,
                event.event_id,
                type(exc).__name__,
                exc,
            )

    async def _run_disclosure_and_analysis(
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
        state = self.event_store.load(batch_id, event_id)
        session_id = state.get("research_session_id")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError(f"research session id is missing: {event_id}")
        self.event_store.update(batch_id, event_id, "analyzing")
        await self.analysis_agent.run(
            ANALYSIS_PROMPT.read_text(encoding="utf-8"),
            paths.analysis_input,
            paths.decision,
            session_id,
            DECISION_SCHEMA,
        )
        decision = self._load_object(paths.decision, "decision")
        DECISION_VALIDATOR.validate(decision)
        market_universe = MarketUniverse.from_dict(
            self._load_object(
                self.event_store.batch_paths(batch_id).market_universe,
                "batch market universe",
            ),
        )
        validate_decision(event_id, decision, market_universe)
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
    runner = CodexRunner()
    research_agent = ResearchAgent(
        runner,
        PROMPTS_DIR,
        profile=RESEARCH_PROFILE,
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
            analysis_agent=AnalysisAgent(runner, profile=ANALYSIS_PROFILE),
            disclosure_watcher=watcher,
        )
        await controller.run_schedule(SCHEDULE_PATH, MARKET_UNIVERSE_PATH)


if __name__ == "__main__":
    asyncio.run(main())
