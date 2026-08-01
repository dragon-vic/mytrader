from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


EVENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(frozen=True)
class EventPaths:
    root: Path
    context: Path
    result: Path
    state: Path
    event: Path
    research: Path
    research_report: Path
    analysis_brief: Path
    watch_plan: Path
    report: Path
    market_snapshot: Path
    decision: Path


@dataclass(frozen=True)
class BatchPaths:
    root: Path
    context: Path
    work: Path
    state: Path
    batch: Path
    market_universe: Path
    research_plan: Path


class EventStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    # 每个财报事件拥有独立目录，避免并发事件互相覆盖。
    def paths(self, event_id: str) -> EventPaths:
        self._validate_id(event_id, "event_id")
        root = (self.root / "events" / event_id).resolve()
        root.relative_to(self.root)
        root.mkdir(parents=True, exist_ok=True)
        context = root / "context"
        result = root / "result"
        context.mkdir(exist_ok=True)
        result.mkdir(exist_ok=True)
        return EventPaths(
            root=root,
            context=context,
            result=result,
            state=root / "state.json",
            event=context / "event.json",
            research=context / "research.json",
            research_report=context / "research_report.md",
            analysis_brief=context / "analysis_brief.md",
            watch_plan=context / "watch_plan.json",
            report=context / "report.json",
            market_snapshot=context / "market_snapshot.json",
            decision=result / "decision.json",
        )

    # 每个盘前或盘后批次共享研究计划，但公司材料仍写入独立事件目录。
    def batch_paths(self, batch_id: str) -> BatchPaths:
        self._validate_id(batch_id, "batch_id")
        root = (self.root / "batches" / batch_id).resolve()
        root.relative_to(self.root)
        context = root / "context"
        work = root / "work"
        context.mkdir(parents=True, exist_ok=True)
        work.mkdir(exist_ok=True)
        return BatchPaths(
            root=root,
            context=context,
            work=work,
            state=root / "state.json",
            batch=context / "batch.json",
            market_universe=context / "market_universe.json",
            research_plan=context / "research_plan.md",
        )

    def create_batch(
        self,
        batch_id: str,
        batch: dict[str, Any],
        market_universe: dict[str, Any],
    ) -> BatchPaths:
        paths = self.batch_paths(batch_id)
        if paths.state.exists():
            raise FileExistsError(paths.state)
        self._write(paths.batch, batch)
        self._write(paths.market_universe, market_universe)
        self._write(
            paths.state,
            {
                "batch_id": batch_id,
                "state": "created",
                "updated_ns": time.time_ns(),
            },
        )
        return paths

    def update_batch(self, batch_id: str, state: str, **values: Any) -> dict[str, Any]:
        paths = self.batch_paths(batch_id)
        payload = json.loads(paths.state.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("batch state must be a JSON object")
        payload.update(values)
        payload["state"] = state
        payload["updated_ns"] = time.time_ns()
        self._write(paths.state, payload)
        return payload

    def create(self, event_id: str, metadata: dict[str, Any]) -> EventPaths:
        paths = self.paths(event_id)
        if paths.state.exists():
            raise FileExistsError(paths.state)
        self._write(
            paths.event,
            {
                "event_id": event_id,
                "metadata": metadata,
            },
        )
        self._write(
            paths.state,
            {
                "event_id": event_id,
                "state": "created",
                "updated_ns": time.time_ns(),
            },
        )
        return paths

    def load(self, event_id: str) -> dict[str, Any]:
        payload = json.loads(self.paths(event_id).state.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("event state must be a JSON object")
        return payload

    def update(self, event_id: str, state: str, **values: Any) -> dict[str, Any]:
        payload = self.load(event_id)
        payload.update(values)
        payload["state"] = state
        payload["updated_ns"] = time.time_ns()
        self._write(self.paths(event_id).state, payload)
        return payload

    def save_research(self, event_id: str, payload: dict[str, Any]) -> Path:
        path = self.paths(event_id).research
        self._write(path, payload)
        return path

    def save_snapshot(self, event_id: str, payload: dict[str, Any]) -> Path:
        path = self.paths(event_id).market_snapshot
        self._write(path, payload)
        return path

    def save_plan(self, event_id: str, payload: dict[str, Any]) -> Path:
        path = self.paths(event_id).watch_plan
        self._write(path, payload)
        return path

    def load_plan(self, event_id: str) -> dict[str, Any]:
        payload = json.loads(
            self.paths(event_id).watch_plan.read_text(encoding="utf-8"),
        )
        if not isinstance(payload, dict):
            raise TypeError("watch plan must be a JSON object")
        return payload

    # 将每次预研生成的分析指引独立保存，避免动态内容进入固定提示词。
    def save_brief(self, event_id: str, text: str) -> Path:
        if not isinstance(text, str) or not text.strip():
            raise TypeError("analysis_brief must be non-empty text")
        path = self.paths(event_id).analysis_brief
        temporary = path.with_suffix(".md.tmp")
        temporary.write_text(text.strip() + "\n", encoding="utf-8")
        temporary.replace(path)
        return path

    def save_report(self, event_id: str, text: str) -> Path:
        path = self.paths(event_id).research_report
        self._write_text(path, text, "research_report")
        return path

    @staticmethod
    def _write_text(path: Path, text: str, name: str) -> None:
        if not isinstance(text, str) or not text.strip():
            raise TypeError(f"{name} must be non-empty text")
        temporary = path.with_suffix(f"{path.suffix}.tmp")
        temporary.write_text(text.strip() + "\n", encoding="utf-8")
        temporary.replace(path)

    @staticmethod
    def _validate_id(value: str, name: str) -> None:
        if not EVENT_ID_PATTERN.fullmatch(value):
            raise ValueError(f"invalid {name}: {value!r}")

    # 临时文件与目标文件位于同一目录，replace 保证状态切换是原子的。
    @staticmethod
    def _write(path: Path, payload: dict[str, Any]) -> None:
        temporary = path.with_suffix(f"{path.suffix}.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(path)
