from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path

from tools.codex_agent import AgentRequest
from tools.codex_agent import CodexRunner
from tools.codex_agent import DEFAULT_MODEL


@dataclass(frozen=True)
class CodexProfile:
    """One agent's Codex model, reasoning and service-tier settings."""

    model: str = DEFAULT_MODEL
    reasoning_effort: str | None = None
    service_tier: str | None = None
    subagent_threads: int | None = None
    subagent_model: str | None = None
    subagent_reasoning_effort: str | None = None


@dataclass(frozen=True)
class ResearchOutcome:
    event_id: str
    memo: str | None
    session_id: str | None
    error: str | None

    @property
    def ready(self) -> bool:
        return bool(self.memo and self.memo.strip() and self.session_id)


class ResearchAgent:
    def __init__(
        self,
        runner: CodexRunner,
        prompts_dir: Path,
        profile: CodexProfile | None = None,
    ) -> None:
        self.runner = runner
        self.profile = profile or CodexProfile(
            reasoning_effort="xhigh",
            subagent_threads=3,
        )
        self.company_prompt = prompts_dir / "research_company.md"

    # 单个 event 独立运行；controller 负责并发调度不同 event。
    async def run_event(
        self,
        event_id: str,
        batch_dir: Path,
        deadline: datetime,
    ) -> ResearchOutcome:
        try:
            return await self._run_company(
                event_id,
                batch_dir,
                deadline,
            )
        except Exception as exc:
            return ResearchOutcome(
                event_id=event_id,
                memo=None,
                session_id=None,
                error=f"{type(exc).__name__}: {exc}",
            )

    async def _run_company(
        self,
        event_id: str,
        batch_dir: Path,
        deadline: datetime,
    ) -> ResearchOutcome:
        work = batch_dir / "events" / event_id / "research_output"
        work.mkdir(parents=True, exist_ok=True)
        research_path = work / "research.md"
        metrics_path = _metrics_path(research_path)
        prompt = (
            self.company_prompt.read_text(encoding="utf-8")
            + f"\n\nAssigned event id: `{event_id}`."
        )
        research_path.unlink(missing_ok=True)
        metrics_path.unlink(missing_ok=True)
        result = await self._before_deadline(
            self.runner.run(
                AgentRequest(
                    prompt=prompt,
                    work_dir=batch_dir,
                    output_path=research_path,
                    metrics_path=metrics_path,
                    web_search=True,
                    model=self.profile.model,
                    reasoning_effort=self.profile.reasoning_effort,
                    service_tier=self.profile.service_tier,
                    subagent_threads=self.profile.subagent_threads,
                    subagent_model=self.profile.subagent_model,
                    subagent_reasoning_effort=self.profile.subagent_reasoning_effort,
                    ephemeral=False,
                ),
            ),
            deadline,
        )
        memo = result.message.strip()
        if not result.thread_id:
            raise RuntimeError("research agent returned no reusable session id")
        return ResearchOutcome(event_id, memo, result.thread_id, None)

    @staticmethod
    async def _before_deadline(awaitable, deadline: datetime):
        remaining = (deadline - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            if hasattr(awaitable, "close"):
                awaitable.close()
            raise TimeoutError("research deadline reached")
        return await asyncio.wait_for(awaitable, timeout=remaining)


class AnalysisAgent:
    def __init__(
        self,
        runner: CodexRunner,
        profile: CodexProfile | None = None,
    ) -> None:
        self.runner = runner
        self.profile = profile or CodexProfile(
            reasoning_effort="medium",
            service_tier="fast",
        )

    # 分析只读取打包后的输入目录，完成后将结果移到analysis_output。
    async def run(
        self,
        prompt: str,
        input_dir: Path,
        output_path: Path,
        session_id: str,
        schema_path: Path | None = None,
    ) -> None:
        temporary = input_dir / ".decision.output.json"
        temporary.unlink(missing_ok=True)
        temp_metrics = _metrics_path(temporary)
        temp_metrics.unlink(missing_ok=True)
        try:
            await self.runner.run(
                AgentRequest(
                    prompt=prompt,
                    work_dir=input_dir,
                    output_path=temporary,
                    metrics_path=temp_metrics,
                    schema_path=schema_path,
                    session_id=session_id,
                    ephemeral=False,
                    web_search=False,
                    model=self.profile.model,
                    reasoning_effort=self.profile.reasoning_effort,
                    service_tier=self.profile.service_tier,
                    subagent_threads=self.profile.subagent_threads,
                    subagent_model=self.profile.subagent_model,
                    subagent_reasoning_effort=self.profile.subagent_reasoning_effort,
                ),
            )
        finally:
            if temp_metrics.exists():
                output_path.parent.mkdir(parents=True, exist_ok=True)
                temp_metrics.replace(_metrics_path(output_path))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary.replace(output_path)


def _metrics_path(output_path: Path) -> Path:
    return output_path.with_name(f"{output_path.stem}.metrics.json")
