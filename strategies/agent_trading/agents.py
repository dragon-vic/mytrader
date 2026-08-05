from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from strategies.agent_trading.lifecycle import BatchPlan


REASONING_EFFORTS = {"minimal", "low", "medium", "high", "xhigh"}
DEFAULT_MODEL = "gpt-5.6-sol"


@dataclass(frozen=True)
class CodexProfile:
    """One agent's Codex model, reasoning and service-tier settings."""

    model: str = DEFAULT_MODEL
    reasoning_effort: str | None = None
    service_tier: str | None = None
    subagent_threads: int | None = None

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any] | None,
        location: str,
    ) -> "CodexProfile":
        if value is None:
            return cls()
        if not isinstance(value, Mapping):
            raise TypeError(f"{location} must be a mapping")
        allowed = {"model", "reasoning_effort", "service_tier", "subagent_threads"}
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ValueError(f"{location} has unsupported keys: {', '.join(unknown)}")
        model = str(value.get("model", DEFAULT_MODEL)).strip()
        if not model:
            raise ValueError(f"{location}.model must not be empty")
        reasoning_effort = value.get("reasoning_effort")
        if reasoning_effort is not None:
            reasoning_effort = str(reasoning_effort)
            if reasoning_effort not in REASONING_EFFORTS:
                raise ValueError(
                    f"{location}.reasoning_effort must be one of "
                    f"{', '.join(sorted(REASONING_EFFORTS))}",
                )
        service_tier = value.get("service_tier")
        if service_tier is not None:
            service_tier = str(service_tier).strip()
            if not service_tier:
                raise ValueError(f"{location}.service_tier must not be empty")
        subagent_threads = value.get("subagent_threads")
        if subagent_threads is not None:
            subagent_threads = int(subagent_threads)
            if subagent_threads <= 0:
                raise ValueError(f"{location}.subagent_threads must be positive")
        return cls(model, reasoning_effort, service_tier, subagent_threads)


@dataclass(frozen=True)
class CodexProfiles:
    research: CodexProfile
    analysis: CodexProfile
    digest: CodexProfile


def load_codex_profiles(settings: Mapping[str, Any]) -> CodexProfiles:
    """Read per-agent Codex profiles from the loaded strategy settings."""
    value = settings.get("codex", {})
    if not isinstance(value, Mapping):
        raise TypeError("codex must be a mapping")
    return CodexProfiles(
        research=CodexProfile.from_mapping(value.get("research"), "codex.research"),
        analysis=CodexProfile.from_mapping(value.get("analysis"), "codex.analysis"),
        digest=CodexProfile.from_mapping(value.get("digest"), "codex.digest"),
    )


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
        output_path: Path | None,
        schema_path: Path | None = None,
        web_search: bool = False,
        model: str = DEFAULT_MODEL,
        reasoning_effort: str | None = None,
        service_tier: str | None = None,
        subagent_threads: int | None = None,
    ) -> str:
        if not model.strip():
            raise ValueError("Codex model must not be empty")
        if reasoning_effort is not None and reasoning_effort not in REASONING_EFFORTS:
            raise ValueError(f"unsupported Codex reasoning effort: {reasoning_effort}")
        if service_tier is not None and not service_tier.strip():
            raise ValueError("Codex service tier must not be empty")
        if subagent_threads is not None and subagent_threads <= 0:
            raise ValueError("Codex subagent thread count must be positive")
        work_dir = work_dir.resolve()
        if output_path is not None:
            output_path = output_path.resolve()
            output_path.relative_to(work_dir)
            output_path.parent.mkdir(parents=True, exist_ok=True)
        started_at = datetime.now(UTC)
        started = time.perf_counter()

        args = [self.executable]
        if web_search:
            args.append("--search")
        args.extend(
            [
                "exec",
                "--model",
                model,
                "--json",
                "--ephemeral",
                "--sandbox",
                # AWS 的受限沙箱无法创建网络命名空间，会在读取本地材料前失败。
                "danger-full-access",
            ],
        )
        if output_path is not None:
            args.extend(["--output-last-message", str(output_path)])
        args.extend(["-C", str(work_dir)])
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
                    f"agents.max_threads={subagent_threads}",
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
            _save_metrics(
                output_path,
                started_at,
                time.perf_counter() - started,
                "cancelled",
                b"",
            )
            raise
        _save_metrics(
            output_path,
            started_at,
            time.perf_counter() - started,
            "completed" if process.returncode == 0 else "failed",
            stdout,
        )
        if process.returncode != 0:
            detail = _codex_diagnostic(stdout, stderr)
            raise RuntimeError(f"codex exec failed with code {process.returncode}: {detail}")
        if output_path is not None:
            return output_path.read_text(encoding="utf-8")
        return _last_agent_message(stdout)


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
        profile: CodexProfile | None = None,
    ) -> None:
        self.runner = runner
        self.profile = profile or CodexProfile(
            reasoning_effort="xhigh",
            subagent_threads=3,
        )
        self.company_prompt = prompts_dir / "research_company.md"
        self.research_schema = schemas_dir / "research.json"

    # 静态批次内的公司预研直接并行，彼此失败不会互相取消。
    async def run_batch(
        self,
        batch: BatchPlan,
        batch_dir: Path,
        deadline: datetime,
    ) -> dict[str, ResearchOutcome]:
        as_of = datetime.now(UTC)
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
        work = batch_dir / "events" / event_id / "research_output"
        work.mkdir(parents=True, exist_ok=True)
        started_at = datetime.now(UTC)
        started = time.perf_counter()
        status = "failed"
        research_path = work / "research.json"
        metrics_path = _metrics_path(research_path)
        runs: list[dict[str, Any]] = []
        research: dict[str, Any] | None = None
        base_prompt = (
            self.company_prompt.read_text(encoding="utf-8")
            + f"\n\nAssigned event id: `{event_id}`."
            + f"\nHard information cutoff (`as_of`): `{as_of.isoformat()}`."
        )
        try:
            attempt = 0
            while True:
                prompt = base_prompt
                if attempt or research_path.exists():
                    prompt += (
                        "\n\nThis is a continuation of unfinished pre-research, not a new task."
                        f"\nRead the latest research at `{research_path.relative_to(batch_dir).as_posix()}`."
                        "\nContinue every unresolved material question recorded in that report and re-run the required subagent debate for disputed or newly researched areas. Preserve supported work and return one complete replacement JSON. Keep `research_complete` false until no material research gap remains."
                    )
                metrics_path.unlink(missing_ok=True)
                try:
                    result = await self._before_deadline(
                        self.runner.run(
                            prompt=prompt,
                            work_dir=batch_dir,
                            output_path=research_path,
                            schema_path=self.research_schema,
                            web_search=True,
                            model=self.profile.model,
                            reasoning_effort=self.profile.reasoning_effort,
                            service_tier=self.profile.service_tier,
                            subagent_threads=self.profile.subagent_threads,
                        ),
                        deadline,
                    )
                finally:
                    if metrics_path.exists():
                        runs.append(json.loads(metrics_path.read_text(encoding="utf-8")))
                research = _json_object(result, "research result")
                if research["research_complete"]:
                    status = "completed"
                    return ResearchOutcome(event_id, research, None)
                attempt += 1
        except TimeoutError:
            status = "timeout"
            return ResearchOutcome(
                event_id=event_id,
                research=research,
                error="research deadline reached before research_complete became true",
            )
        finally:
            _save_research_metrics(
                work,
                event_id,
                started_at,
                time.perf_counter() - started,
                status,
                runs,
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
        schema_path: Path | None = None,
    ) -> None:
        temporary = input_dir / ".decision.output.json"
        temporary.unlink(missing_ok=True)
        temp_metrics = _metrics_path(temporary)
        temp_metrics.unlink(missing_ok=True)
        try:
            await self.runner.run(
                prompt=prompt,
                work_dir=input_dir,
                output_path=temporary,
                schema_path=schema_path,
                web_search=False,
                model=self.profile.model,
                reasoning_effort=self.profile.reasoning_effort,
                service_tier=self.profile.service_tier,
                subagent_threads=self.profile.subagent_threads,
            )
        finally:
            if temp_metrics.exists():
                output_path.parent.mkdir(parents=True, exist_ok=True)
                temp_metrics.replace(_metrics_path(output_path))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary.replace(output_path)


class ResearchDigestAgent:
    """Ask Codex to summarize completed research and send the email itself."""

    def __init__(
        self,
        runner: CodexRunner,
        prompt_path: Path,
        profile: CodexProfile | None = None,
    ) -> None:
        self.runner = runner
        self.prompt_path = prompt_path
        self.profile = profile or CodexProfile(reasoning_effort="high")

    async def run(
        self,
        batch_dir: Path,
        event_ids: tuple[str, ...],
    ) -> None:
        files = [
            "batch.json",
            "market_universe.json",
            *[
                relative
                for event_id in event_ids
                for relative in (
                    f"events/{event_id}/research_output/research.json",
                    f"events/{event_id}/research_output/research.metrics.json",
                )
            ],
        ]
        prompt = self.prompt_path.read_text(encoding="utf-8")
        prompt += (
            "\n\nAssigned batch directory: `.`\n"
            "Read only these files (some optional metrics files may be absent):\n"
            + "\n".join(f"- `{relative}`" for relative in files)
            + "\nComplete the email send before returning."
        )
        await self.runner.run(
            prompt=prompt,
            work_dir=batch_dir,
            output_path=None,
            schema_path=None,
            web_search=False,
            model=self.profile.model,
            reasoning_effort=self.profile.reasoning_effort,
            service_tier=self.profile.service_tier,
            subagent_threads=self.profile.subagent_threads,
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


def _last_agent_message(stdout: bytes) -> str:
    messages: list[str] = []
    for line in stdout.decode("utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        item = event.get("item") or {}
        if item.get("type") == "agent_message" and item.get("text"):
            messages.append(str(item["text"]))
    if not messages:
        raise RuntimeError("codex exec returned no agent message")
    return messages[-1]


# 把每次 Codex 调用的耗时和官方 JSONL usage 写到输出旁边。
def _save_metrics(
    output_path: Path | None,
    started_at: datetime,
    elapsed_seconds: float,
    status: str,
    stdout: bytes,
) -> None:
    if output_path is None:
        return
    turns: list[dict[str, int]] = []
    thread_ids: list[str] = []
    for line in stdout.decode("utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "thread.started" and isinstance(event.get("thread_id"), str):
            thread_ids.append(event["thread_id"])
        usage = event.get("usage")
        if event.get("type") != "turn.completed" or not isinstance(usage, dict):
            continue
        turns.append(
            {
                key: int(usage.get(key, 0))
                for key in (
                    "input_tokens",
                    "cached_input_tokens",
                    "output_tokens",
                    "reasoning_output_tokens",
                )
            },
        )
    usage = {
        key: sum(turn[key] for turn in turns)
        for key in (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_output_tokens",
        )
    }
    usage["total_tokens"] = usage["input_tokens"] + usage["output_tokens"]
    path = _metrics_path(output_path)
    _write_json(
        path,
        {
            "output_path": str(output_path),
            "status": status,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "thread_ids": thread_ids,
            "turns": turns,
            "usage": usage,
        },
    )


# 汇总一次公司预研的多次补充研究调用，保留总墙钟耗时和总 token。
def _save_research_metrics(
    work: Path,
    event_id: str,
    started_at: datetime,
    elapsed_seconds: float,
    status: str,
    runs: list[dict[str, Any]],
) -> None:
    keys = (
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    usage = {
        key: sum(int(run["usage"][key]) for run in runs)
        for key in keys
    }
    _write_json(
        work / "research.metrics.json",
        {
            "event_id": event_id,
            "status": status,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "attempts": len(runs),
            "usage": usage,
            "runs": runs,
        },
    )


def _metrics_path(output_path: Path) -> Path:
    return output_path.with_name(f"{output_path.stem}.metrics.json")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def _json_object(text: str, name: str) -> dict[str, Any]:
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise TypeError(f"{name} must be a JSON object")
    return payload
