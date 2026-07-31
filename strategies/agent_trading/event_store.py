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
    watch_plan: Path
    report: Path
    decision: Path


class EventStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    # 每个财报事件拥有独立目录，避免并发事件互相覆盖。
    def paths(self, event_id: str) -> EventPaths:
        if not EVENT_ID_PATTERN.fullmatch(event_id):
            raise ValueError(f"invalid event_id: {event_id!r}")
        root = (self.root / event_id).resolve()
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
            research=context / "research.md",
            watch_plan=context / "watch_plan.json",
            report=context / "report.json",
            decision=result / "decision.json",
        )

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

    # 临时文件与目标文件位于同一目录，replace 保证状态切换是原子的。
    @staticmethod
    def _write(path: Path, payload: dict[str, Any]) -> None:
        temporary = path.with_suffix(f"{path.suffix}.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(path)
