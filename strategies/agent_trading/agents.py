from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any


class CodexRunner:
    def __init__(
        self,
        executable: str | Path = Path.home() / ".local" / "bin" / "codex",
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
    ) -> str:
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
        args.append("-")

        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate(prompt.encode("utf-8"))
        if process.returncode != 0:
            detail = _codex_diagnostic(stdout, stderr)
            raise RuntimeError(f"codex exec failed with code {process.returncode}: {detail}")
        return output_path.read_text(encoding="utf-8")


class ResearchAgent:
    def __init__(self, runner: CodexRunner) -> None:
        self.runner = runner

    # 预研结论属于最终分析材料，固定写入 context。
    async def run(
        self,
        prompt: str,
        event_dir: Path,
        output_path: Path,
    ) -> str:
        return await self.runner.run(
            prompt=prompt,
            work_dir=event_dir,
            output_path=output_path,
            web_search=True,
        )


class AnalysisAgent:
    def __init__(self, runner: CodexRunner) -> None:
        self.runner = runner

    # 分析 Agent 的输出由后续确定的 JSON Schema 约束。
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
            web_search=True,
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
