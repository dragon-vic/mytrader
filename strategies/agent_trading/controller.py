from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from strategies.agent_trading.agents import (
    AnalysisAgent,
    CodexProfile,
    ResearchAgent,
    ResearchOutcome,
)
from strategies.agent_trading.event_store import (
    EventState,
    EventStore,
    FailureStage,
    ResearchHandoff,
)
from strategies.agent_trading.lifecycle import (
    EventSpec,
    MarketUniverse,
    load_event_plan,
    load_event_schedule,
    load_market_universe,
    validate_decision,
)
from strategies.agent_trading.watch import (
    DisclosurePackage,
    DisclosureTimeoutError,
    DisclosureWatcher,
    WatchPlan,
)
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

    # Event 到达生命周期节点时建立自己的输入和共享快照；不建立 group 状态。
    def _prepare_event(
        self,
        group_id: str,
        event: EventSpec,
        market_universe: MarketUniverse,
    ) -> None:
        self.event_store.ensure_event_group(
            group_id,
            market_universe.to_dict(),
        )
        self.event_store.create_event(
            group_id,
            event.event_id,
            {
                "schedule_group_id": group_id,
                "company": event.company,
                "ticker": event.ticker,
                "scope": event.scope,
                "active": event.active,
                "confirmed": event.confirmed,
                "research_hints": list(event.research_hints),
            },
            event.watch_plan.to_dict(),
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
                    event = load_event_plan(
                        schedule_path,
                        scheduled.batch_id,
                        scheduled.event_id,
                    )
                    market_universe = load_market_universe(market_universe_path)
                except Exception as exc:  # noqa: BLE001 - 单个 event 不能阻塞 schedule
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
                        scheduled.batch_id,
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
        group_id: str,
        event_id: str,
        outcome: ResearchOutcome,
    ) -> ResearchHandoff | None:
        if not outcome.ready:
            self.event_store.update(
                group_id,
                event_id,
                EventState.FAILED,
                failed_stage=FailureStage.RESEARCH,
                error=outcome.error or "research memo is missing",
            )
            return None

        if outcome.event_id != event_id:
            raise ValueError(
                f"research event_id mismatch: {outcome.event_id} != {event_id}",
            )

        paths = self.event_store.event_paths(group_id, event_id)
        values: dict[str, Any] = {
            "research_session_id": outcome.session_id,
            "research_completed_at": datetime.now(UTC).isoformat(),
            "research_path": paths.research.relative_to(paths.root).as_posix(),
        }
        self.event_store.update(
            group_id,
            event_id,
            EventState.RESEARCH_READY,
            **values,
        )
        return self.event_store.load_research_handoff(group_id, event_id)

    def _prepare_analysis_input(
        self,
        group_id: str,
        event_id: str,
    ) -> bool:
        paths = self.event_store.event_paths(group_id, event_id)
        if not paths.analysis_event.is_file():
            self.event_store.update(
                group_id,
                event_id,
                EventState.FAILED,
                failed_stage=FailureStage.LIFECYCLE,
                error="analysis event input is missing",
            )
            return False

        self.event_store.update(
            group_id,
            event_id,
            EventState.ANALYSIS_INPUT_READY,
        )
        return True

    async def _wait_for_watch_start(
        self,
        group_id: str,
        event_id: str,
    ) -> None:
        plan_path = self.event_store.event_paths(group_id, event_id).watch_plan
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
        group_id: str,
        event: EventSpec,
        market_universe: MarketUniverse,
    ) -> None:
        event_id = event.event_id
        email_task: asyncio.Task[None] | None = None
        try:
            paths = self.event_store.event_paths(group_id, event_id)
            if (
                paths.state.exists()
                and self.event_store.load_state(group_id, event_id).is_finished
            ):
                return
            self._prepare_event(group_id, event, market_universe)
            research = self.event_store.load_research_handoff(
                group_id,
                event_id,
            )
            generated_research = False
            if research is None:
                async with self.research_lock:
                    research = self.event_store.load_research_handoff(
                        group_id,
                        event_id,
                    )
                    if research is None:
                        self.event_store.update(
                            group_id,
                            event_id,
                            EventState.RESEARCHING,
                        )
                        outcome = await self.research_agent.run_event(
                            event_id=event_id,
                            group_dir=self.event_store.event_group_paths(group_id).root,
                            deadline=event.watch_plan.start_at,
                        )
                        research = self._save_research_outcome(
                            group_id,
                            event_id,
                            outcome,
                        )
                        generated_research = research is not None
                if generated_research:
                    email_task = asyncio.create_task(
                        self._send_research_email(group_id, event),
                        name=f"agent-trading-research-email-{event_id}",
                    )

            if research is None:
                LOG.error("event research incomplete event_id=%s", event_id)
                return

            await self._wait_for_watch_start(
                group_id,
                event_id,
            )
            if not self._prepare_analysis_input(
                group_id,
                event_id,
            ):
                LOG.error(
                    "event analysis input incomplete event_id=%s",
                    event_id,
                )
                return
            await self._run_disclosure_and_analysis(
                group_id,
                event_id,
                research,
            )
        except Exception as exc:
            LOG.exception(
                "agent trading event failed event_id=%s error_type=%s",
                event_id,
                type(exc).__name__,
            )
            try:
                self.event_store.update(
                    group_id,
                    event_id,
                    EventState.FAILED,
                    failed_stage=FailureStage.LIFECYCLE,
                    error=f"{type(exc).__name__}: {exc}",
                )
            except Exception as state_exc:  # noqa: BLE001 - 原始错误后尽力记录状态
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
        group_id: str,
        event: EventSpec,
    ) -> None:
        input_path = self.event_store.event_paths(
            group_id,
            event.event_id,
        ).research
        custom_prompt = (
            f"这是 `{event.event_id}`（{event.company}/{event.ticker}）的事件预研。"
            "邮件只总结这个 event，不等待或引用其他 event。"
            "邮件主题必须包含 event id，并明确这是财报发布前预研。"
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
                "research email failed group_id=%s event_id=%s "
                "error_type=%s",
                group_id,
                event.event_id,
                type(exc).__name__,
            )

    async def _run_disclosure_and_analysis(
        self,
        group_id: str,
        event_id: str,
        research: ResearchHandoff,
    ) -> None:
        try:
            await self.wait_report(group_id, event_id)
            await self._run_analysis_and_send(group_id, event_id, research)
        except Exception as exc:
            LOG.exception(
                "agent trading event failed event_id=%s error_type=%s",
                event_id,
                type(exc).__name__,
            )
            details: dict[str, Any] = {}
            if isinstance(exc, DisclosureTimeoutError):
                details["source_status"] = exc.source_status
            state = self.event_store.load_state(group_id, event_id)
            failed_stage = (
                FailureStage.ANALYSIS
                if state
                in {
                    EventState.REPORT_READY,
                    EventState.ANALYZING,
                    EventState.DECISION_READY,
                }
                else FailureStage.WATCH
            )
            self.event_store.update(
                group_id,
                event_id,
                EventState.FAILED,
                failed_stage=failed_stage,
                error=f"{type(exc).__name__}: {exc}",
                **details,
            )

    async def _run_analysis_and_send(
        self,
        group_id: str,
        event_id: str,
        research: ResearchHandoff,
    ) -> None:
        decision = await self.run_analysis(group_id, event_id, research)
        await self.send(decision)
        self.event_store.update(group_id, event_id, EventState.DECISION_SENT)

    async def wait_report(
        self,
        group_id: str,
        event_id: str,
    ) -> DisclosurePackage:
        paths = self.event_store.event_paths(group_id, event_id)
        plan = WatchPlan.from_dict(
            self._load_object(paths.watch_plan, "watch plan"),
        )
        if plan.event_id != event_id:
            raise ValueError(
                f"stored watch plan event_id mismatch: {plan.event_id} != {event_id}",
            )
        self.event_store.update(
            group_id,
            event_id,
            EventState.WATCHING_DISCLOSURE,
        )
        package = await self.disclosure_watcher.watch(
            plan,
            paths.analysis_input,
            paths.watch,
        )
        self.event_store.update(
            group_id,
            event_id,
            EventState.REPORT_READY,
            report_path=paths.report.relative_to(paths.root).as_posix(),
            report_source=package.source,
            report_detected_ns=package.detected_ns,
        )
        return package

    async def run_analysis(
        self,
        group_id: str,
        event_id: str,
        research: ResearchHandoff,
    ) -> dict[str, Any]:
        paths = self.event_store.event_paths(group_id, event_id)
        self.event_store.update(group_id, event_id, EventState.ANALYZING)
        await self.analysis_agent.run(
            ANALYSIS_PROMPT.read_text(encoding="utf-8"),
            paths.analysis_input,
            paths.decision,
            research,
            DECISION_SCHEMA,
        )
        decision = self._load_object(paths.decision, "decision")
        DECISION_VALIDATOR.validate(decision)
        market_universe = MarketUniverse.from_dict(
            self._load_object(
                self.event_store.event_group_paths(group_id).market_universe,
                "event market universe",
            ),
        )
        validate_decision(event_id, decision, market_universe)
        self.event_store.update(
            group_id,
            event_id,
            EventState.DECISION_READY,
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
