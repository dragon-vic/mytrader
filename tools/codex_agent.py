from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any


REASONING_EFFORTS = {"minimal", "low", "medium", "high", "xhigh", "max"}
APPROVAL_POLICIES = {"untrusted", "on-request", "never"}
SANDBOX_MODES = {"read-only", "workspace-write", "danger-full-access"}
DEFAULT_MODEL = "gpt-5.6-sol"
USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
)


@dataclass(frozen=True)
class AgentRequest:
    prompt: str
    work_dir: Path
    model: str = DEFAULT_MODEL
    reasoning_effort: str | None = None
    service_tier: str | None = None
    web_search: bool = False
    output_path: Path | None = None
    metrics_path: Path | None = None
    schema_path: Path | None = None
    session_id: str | None = None
    ephemeral: bool = True
    sandbox: str = "danger-full-access"
    approval_policy: str | None = None
    dangerously_bypass_approvals_and_sandbox: bool = False
    subagent_threads: int | None = None
    subagent_model: str | None = None
    subagent_reasoning_effort: str | None = None
    config_overrides: tuple[str, ...] = ()
    timeout_seconds: float | None = None


@dataclass(frozen=True)
class AgentResult:
    message: str
    thread_id: str | None
    usage: dict[str, int] | None
    output_path: Path | None
    metrics_path: Path | None


@dataclass(frozen=True)
class _ParsedOutput:
    message: str
    thread_id: str | None
    thread_ids: tuple[str, ...]
    usage: dict[str, int] | None
    turns: tuple[dict[str, int], ...]
    diagnostic: str


class CodexRunner:
    """执行一次 Codex CLI 调用并解析通用结果。"""

    def __init__(self, executable: str | Path | None = None) -> None:
        self.executable = self._resolve_executable(executable)

    async def run(self, request: AgentRequest) -> AgentResult:
        """执行 Agent 请求并返回最终消息、线程 ID 和 usage。"""
        work_dir, output_path, metrics_path, schema_path = self._validate_request(request)
        command = self._build_command(request, work_dir, output_path, schema_path)
        started_at = datetime.now(UTC)
        started = time.perf_counter()
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        prompt = None if request.session_id else request.prompt.encode("utf-8")
        try:
            communicate = process.communicate(prompt)
            if request.timeout_seconds is None:
                stdout, stderr = await communicate
            else:
                stdout, stderr = await asyncio.wait_for(
                    communicate,
                    timeout=request.timeout_seconds,
                )
        except asyncio.TimeoutError as exc:
            await self._terminate(process)
            _write_metrics(
                metrics_path,
                output_path,
                started_at,
                time.perf_counter() - started,
                "timeout",
                (),
                None,
            )
            raise TimeoutError("Codex agent timed out") from exc
        except asyncio.CancelledError:
            await self._terminate(process)
            _write_metrics(
                metrics_path,
                output_path,
                started_at,
                time.perf_counter() - started,
                "cancelled",
                (),
                None,
            )
            raise

        parsed = self._parse_output(stdout, stderr)
        status = "completed" if process.returncode == 0 else "failed"
        _write_metrics(
            metrics_path,
            output_path,
            started_at,
            time.perf_counter() - started,
            status,
            parsed.thread_ids,
            parsed.turns,
        )
        if process.returncode != 0:
            detail = parsed.diagnostic or f"codex exited with code {process.returncode}"
            raise RuntimeError(
                f"codex exec failed with code {process.returncode}: {detail}",
            )

        message = parsed.message
        if output_path is not None:
            message = output_path.read_text(encoding="utf-8")
        if not message:
            raise RuntimeError("codex exec returned no final message")
        return AgentResult(
            message=message,
            thread_id=parsed.thread_id,
            usage=parsed.usage,
            output_path=output_path,
            metrics_path=metrics_path,
        )

    def run_sync(self, request: AgentRequest) -> AgentResult:
        """在没有运行事件循环的同步调用方中执行 Agent 请求。"""
        return asyncio.run(self.run(request))

    @staticmethod
    def _resolve_executable(executable: str | Path | None) -> str:
        if executable is not None:
            return str(executable)
        configured = os.environ.get("CODEX_EXECUTABLE", "").strip()
        if configured:
            return configured
        local_executable = Path.home() / ".local" / "bin" / "codex"
        if local_executable.is_file():
            return str(local_executable)
        return shutil.which("codex") or "codex"

    @staticmethod
    def _validate_request(
        request: AgentRequest,
    ) -> tuple[Path, Path | None, Path | None, Path | None]:
        if not request.prompt.strip():
            raise ValueError("Agent prompt must not be empty")
        if not request.model.strip():
            raise ValueError("Agent model must not be empty")
        if request.reasoning_effort is not None and request.reasoning_effort not in REASONING_EFFORTS:
            raise ValueError(
                f"unsupported reasoning effort: {request.reasoning_effort}",
            )
        if request.approval_policy is not None and request.approval_policy not in APPROVAL_POLICIES:
            raise ValueError(
                f"unsupported approval policy: {request.approval_policy}",
            )
        if request.sandbox not in SANDBOX_MODES:
            raise ValueError(f"unsupported sandbox mode: {request.sandbox}")
        if request.subagent_threads is not None and request.subagent_threads <= 0:
            raise ValueError("subagent thread count must be positive")
        if request.subagent_model is not None and not request.subagent_model.strip():
            raise ValueError("subagent model must not be empty")
        if (
            request.subagent_reasoning_effort is not None
            and request.subagent_reasoning_effort not in REASONING_EFFORTS
        ):
            raise ValueError(
                "unsupported subagent reasoning effort: "
                f"{request.subagent_reasoning_effort}",
            )
        if request.timeout_seconds is not None and request.timeout_seconds <= 0:
            raise ValueError("timeout must be positive")
        if any(not value.strip() for value in request.config_overrides):
            raise ValueError("Codex config overrides must not be empty")

        work_dir = request.work_dir.resolve()
        if not work_dir.is_dir():
            raise ValueError(f"Codex work directory does not exist: {work_dir}")
        output_path = _prepare_output_path(request.output_path)
        metrics_path = _prepare_output_path(request.metrics_path)
        schema_path = request.schema_path.resolve() if request.schema_path else None
        if schema_path is not None and not schema_path.is_file():
            raise ValueError(f"Codex schema does not exist: {schema_path}")
        return work_dir, output_path, metrics_path, schema_path

    def _build_command(
        self,
        request: AgentRequest,
        work_dir: Path,
        output_path: Path | None,
        schema_path: Path | None,
    ) -> list[str]:
        command = [self.executable]
        if request.web_search:
            command.append("--search")
        command.extend(["exec"])
        if request.session_id:
            command.append("resume")
        command.extend(["--model", request.model, "--json"])
        if request.ephemeral and request.session_id is None:
            command.append("--ephemeral")
        if output_path is not None:
            command.extend(["--output-last-message", str(output_path)])
        command.extend(["-C", str(work_dir)])
        if schema_path is not None:
            command.extend(["--output-schema", str(schema_path)])
        if request.reasoning_effort is not None:
            command.extend(
                ["-c", f'model_reasoning_effort="{request.reasoning_effort}"'],
            )
        if request.service_tier is not None:
            command.extend(["-c", f'service_tier="{request.service_tier}"'])
        subagent_role: dict[str, str] = {}
        if request.subagent_model is not None:
            subagent_role["model"] = request.subagent_model
        if request.subagent_reasoning_effort is not None:
            subagent_role["reasoning_effort"] = request.subagent_reasoning_effort
        if subagent_role:
            role_fields = ",".join(
                f"{key}={json.dumps(value)}" for key, value in subagent_role.items()
            )
            command.extend(["-c", f"agents.default={{{role_fields}}}"])
        if request.subagent_threads is not None:
            command.extend(["-c", "features.multi_agent=true"])
        for override in request.config_overrides:
            command.extend(["-c", override])
        if request.dangerously_bypass_approvals_and_sandbox:
            command.append("--dangerously-bypass-approvals-and-sandbox")
        else:
            if request.approval_policy is not None:
                command.extend(["--ask-for-approval", request.approval_policy])
            command.extend(["--sandbox", request.sandbox])
        if request.session_id:
            command.extend([request.session_id, request.prompt])
        else:
            command.append("-")
        return command

    @staticmethod
    async def _terminate(process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=3)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()

    @staticmethod
    def _parse_output(stdout: bytes, stderr: bytes) -> _ParsedOutput:
        messages: list[str] = []
        errors: list[str] = []
        thread_id: str | None = None
        thread_ids: list[str] = []
        turns: list[dict[str, int]] = []
        for line in stdout.decode("utf-8", errors="replace").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "thread.started":
                raw_thread_id = event.get("thread_id")
                if isinstance(raw_thread_id, str):
                    thread_ids.append(raw_thread_id)
                    thread_id = raw_thread_id
            if event.get("type") in {"error", "turn.failed"}:
                error = event.get("error") or event
                errors.append(_format_error(error))
            if event.get("type") == "turn.completed":
                usage = event.get("usage")
                if isinstance(usage, dict):
                    turns.append(
                        {
                            field: int(usage.get(field, 0))
                            for field in USAGE_FIELDS
                        },
                    )
            item = event.get("item") or {}
            if item.get("type") == "agent_message" and item.get("text"):
                messages.append(str(item["text"]))
        usage = None
        if turns:
            usage = {
                field: sum(turn[field] for turn in turns)
                for field in USAGE_FIELDS
            }
            usage["total_tokens"] = usage["input_tokens"] + usage["output_tokens"]
        diagnostic_parts = [stderr.decode("utf-8", errors="replace").strip()]
        diagnostic_parts.extend(error for error in errors if error)
        return _ParsedOutput(
            message=messages[-1] if messages else "",
            thread_id=thread_id,
            thread_ids=tuple(thread_ids),
            usage=usage,
            turns=tuple(turns),
            diagnostic="\n".join(part for part in diagnostic_parts if part),
        )


def _prepare_output_path(path: Path | None) -> Path | None:
    if path is None:
        return None
    output_path = path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path


# 按调用方要求保存一次 Agent 的运行指标。
def _write_metrics(
    metrics_path: Path | None,
    output_path: Path | None,
    started_at: datetime,
    elapsed_seconds: float,
    status: str,
    thread_ids: tuple[str, ...],
    turns: tuple[dict[str, int], ...] | None,
) -> None:
    if metrics_path is None:
        return
    turn_list = list(turns or ())
    usage = {
        field: sum(turn[field] for turn in turn_list)
        for field in USAGE_FIELDS
    }
    usage["total_tokens"] = usage["input_tokens"] + usage["output_tokens"]
    payload = {
        "output_path": str(output_path) if output_path is not None else None,
        "status": status,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "thread_ids": list(thread_ids),
        "turns": turn_list,
        "usage": usage,
    }
    temporary = metrics_path.with_suffix(f"{metrics_path.suffix}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(metrics_path)


def _format_error(error: Any) -> str:
    if isinstance(error, dict):
        return json.dumps(error, ensure_ascii=False)
    return str(error)


__all__ = ["AgentRequest", "AgentResult", "CodexRunner"]
