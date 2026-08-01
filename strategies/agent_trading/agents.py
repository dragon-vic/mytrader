from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from strategies.agent_trading.lifecycle import BatchPlan


REASONING_EFFORTS = {"minimal", "low", "medium", "high", "xhigh"}


class CodexRunner:
    def __init__(
        self,
        executable: str | Path = "codex",
    ) -> None:
        self.executable = str(executable)

    # 在独立进程中运行一次 Codex，并返回最终输出文件内容。
    async def run(
        self,
        prompt: str,
        work_dir: Path,
        output_path: Path,
        schema_path: Path | None = None,
        web_search: bool = False,
        reasoning_effort: str | None = None,
        service_tier: str | None = None,
        subagent_threads: int | None = None,
        subagent_effort: str | None = None,
    ) -> str:
        if reasoning_effort is not None and reasoning_effort not in REASONING_EFFORTS:
            raise ValueError(f"unsupported Codex reasoning effort: {reasoning_effort}")
        if service_tier is not None and not service_tier.strip():
            raise ValueError("Codex service tier must not be empty")
        if subagent_threads is not None and subagent_threads <= 0:
            raise ValueError("Codex subagent thread count must be positive")
        if subagent_effort is not None and subagent_effort not in REASONING_EFFORTS:
            raise ValueError(f"unsupported subagent reasoning effort: {subagent_effort}")
        if subagent_effort is not None and subagent_threads is None:
            raise ValueError("subagent effort requires subagent threads")
        work_dir = work_dir.resolve()
        output_path = output_path.resolve()
        output_path.relative_to(work_dir)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        args = [self.executable]
        if web_search:
            args.append("--search")
        args.extend(
            [
                "exec",
                "--json",
                "--ephemeral",
                "--sandbox",
                "read-only",
                "--output-last-message",
                str(output_path),
                "-C",
                str(work_dir),
            ],
        )
        if schema_path is not None:
            args.extend(["--output-schema", str(schema_path.resolve())])
        if reasoning_effort is not None:
            args.extend(["-c", f'model_reasoning_effort="{reasoning_effort}"'])
        if service_tier is not None:
            args.extend(["-c", f'service_tier="{service_tier}"'])
        if subagent_threads is not None:
            args.extend(
                [
                    "-c",
                    "features.multi_agent=true",
                    "-c",
                    f"agents.max_concurrent_threads_per_session={subagent_threads}",
                ],
            )
        if subagent_effort is not None:
            args.extend(
                [
                    "-c",
                    f'agents.default_subagent_reasoning_effort="{subagent_effort}"',
                ],
            )
        args.append("-")

        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await process.communicate(prompt.encode("utf-8"))
        except asyncio.CancelledError:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            await process.wait()
            raise
        if process.returncode != 0:
            detail = _codex_diagnostic(stdout, stderr)
            raise RuntimeError(f"codex exec failed with code {process.returncode}: {detail}")
        return output_path.read_text(encoding="utf-8")


class ScheduleAgent:
    def __init__(
        self,
        runner: CodexRunner,
        prompts_dir: Path,
        schemas_dir: Path,
    ) -> None:
        self.runner = runner
        self.prompt = prompts_dir / "schedule.md"
        self.schema = schemas_dir / "schedule.json"

    async def run(self, work_dir: Path, output_path: Path) -> BatchPlan:
        result = await self.runner.run(
            prompt=self.prompt.read_text(encoding="utf-8"),
            work_dir=work_dir,
            output_path=output_path,
            schema_path=self.schema,
            web_search=True,
            reasoning_effort="medium",
            service_tier="fast",
        )
        return BatchPlan.from_dict(_json_object(result, "schedule result"))


@dataclass(frozen=True)
class ResearchOutcome:
    event_id: str
    research: dict[str, Any] | None
    error: str | None

    @property
    def ready(self) -> bool:
        return self.research is not None and self.research.get("research_complete") is True


class ResearchAgent:
    def __init__(
        self,
        runner: CodexRunner,
        prompts_dir: Path,
        schemas_dir: Path,
    ) -> None:
        self.runner = runner
        self.plan_prompt = prompts_dir / "research_plan.md"
        self.company_prompt = prompts_dir / "research_company.md"
        self.research_schema = schemas_dir / "research.json"

    # 一个批次只做一次统筹规划；各公司研究彼此并行且互不取消。
    async def run_batch(
        self,
        batch: BatchPlan,
        batch_dir: Path,
        deadline: datetime,
    ) -> dict[str, ResearchOutcome]:
        as_of = datetime.now(UTC)
        plan_output = batch_dir / "context" / "research_plan.md"
        await self._before_deadline(
            self.runner.run(
                prompt=self.plan_prompt.read_text(encoding="utf-8"),
                work_dir=batch_dir,
                output_path=plan_output,
                web_search=False,
                reasoning_effort="xhigh",
            ),
            deadline,
        )

        tasks = {
            event.event_id: asyncio.create_task(
                self._run_company(event.event_id, batch_dir, as_of, deadline),
                name=f"agent-trading-research-{event.event_id}",
            )
            for event in batch.events
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        outcomes: dict[str, ResearchOutcome] = {}
        for event_id, result in zip(tasks, results, strict=True):
            if isinstance(result, BaseException):
                outcomes[event_id] = ResearchOutcome(
                    event_id=event_id,
                    research=None,
                    error=f"{type(result).__name__}: {result}",
                )
            else:
                outcomes[event_id] = result
        return outcomes

    async def _run_company(
        self,
        event_id: str,
        batch_dir: Path,
        as_of: datetime,
        deadline: datetime,
    ) -> ResearchOutcome:
        work = batch_dir / "work" / event_id
        work.mkdir(parents=True, exist_ok=True)
        research_path: Path | None = None
        research: dict[str, Any] | None = None
        base_prompt = (
            self.company_prompt.read_text(encoding="utf-8")
            + f"\n\nAssigned event id: `{event_id}`."
            + f"\nHard information cutoff (`as_of`): `{as_of.isoformat()}`."
        )
        try:
            attempt = 0
            while True:
                next_path = work / f"research-{attempt}.json"
                prompt = base_prompt
                if research_path is not None:
                    prompt += (
                        "\n\nThis is a continuation of unfinished pre-research, not a new task."
                        f"\nRead the latest research at `{research_path.relative_to(batch_dir).as_posix()}`."
                        "\nContinue every unresolved material question recorded in that report and re-run the required subagent debate for disputed or newly researched areas. Preserve supported work and return one complete replacement JSON. Keep `research_complete` false until no material research gap remains."
                    )
                result = await self._before_deadline(
                    self.runner.run(
                        prompt=prompt,
                        work_dir=batch_dir,
                        output_path=next_path,
                        schema_path=self.research_schema,
                        web_search=True,
                        reasoning_effort="xhigh",
                        subagent_threads=3,
                        subagent_effort="xhigh",
                    ),
                    deadline,
                )
                research = _json_object(result, "research result")
                complete = research.get("research_complete")
                if not isinstance(complete, bool):
                    raise TypeError("research_complete must be boolean")
                research_path = next_path
                if complete:
                    return ResearchOutcome(event_id, research, None)
                attempt += 1
        except TimeoutError:
            return ResearchOutcome(
                event_id=event_id,
                research=research,
                error="research deadline reached before research_complete became true",
            )

    @staticmethod
    async def _before_deadline(awaitable, deadline: datetime):
        remaining = (deadline - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            if hasattr(awaitable, "close"):
                awaitable.close()
            raise TimeoutError("research deadline reached")
        return await asyncio.wait_for(awaitable, timeout=remaining)


class AnalysisAgent:
    def __init__(self, runner: CodexRunner) -> None:
        self.runner = runner

    # 分析只读取事件目录，避免引入披露后的外部信息。
    async def run(
        self,
        prompt: str,
        event_dir: Path,
        output_path: Path,
        schema_path: Path | None = None,
    ) -> str:
        return await self.runner.run(
            prompt=prompt,
            work_dir=event_dir,
            output_path=output_path,
            schema_path=schema_path,
            web_search=False,
            reasoning_effort="medium",
            service_tier="fast",
        )


def _codex_diagnostic(stdout: bytes, stderr: bytes) -> str:
    errors: list[Any] = []
    for line in stdout.decode("utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") in {"error", "turn.failed"}:
            errors.append(event.get("error") or event)
    parts = [stderr.decode("utf-8", errors="replace").strip()]
    parts.extend(
        json.dumps(error, ensure_ascii=False) if isinstance(error, dict) else str(error)
        for error in errors
    )
    detail = "\n".join(part for part in parts if part)
    return detail or "no diagnostic output"


def _json_object(text: str, name: str) -> dict[str, Any]:
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise TypeError(f"{name} must be a JSON object")
    return payload
