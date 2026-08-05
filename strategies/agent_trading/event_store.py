from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(frozen=True)
class EventPaths:
    root: Path
    state: Path
    event: Path
    research_output: Path
    research: Path
    research_metrics: Path
    watch: Path
    watch_plan: Path
    analysis_input: Path
    analysis_event: Path
    analysis_research: Path
    analysis_brief: Path
    report: Path
    analysis_output: Path
    decision: Path
    decision_metrics: Path


@dataclass(frozen=True)
class BatchPaths:
    root: Path
    state: Path
    batch: Path
    market_universe: Path
    events: Path


class EventStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def batch_paths(self, batch_id: str) -> BatchPaths:
        self._validate_id(batch_id, "batch_id")
        root = (self.root / "batches" / batch_id).resolve()
        root.relative_to(self.root)
        events = root / "events"
        events.mkdir(parents=True, exist_ok=True)
        return BatchPaths(
            root=root,
            state=root / "state.json",
            batch=root / "batch.json",
            market_universe=root / "market_universe.json",
            events=events,
        )

    # event始终属于一个batch，不再维护顶层events目录。
    def event_paths(self, batch_id: str, event_id: str) -> EventPaths:
        self._validate_id(batch_id, "batch_id")
        self._validate_id(event_id, "event_id")
        batch = self.batch_paths(batch_id)
        root = (batch.events / event_id).resolve()
        root.relative_to(batch.events.resolve())
        research_output = root / "research_output"
        watch = root / "watch"
        analysis_input = root / "analysis_input"
        analysis_output = root / "analysis_output"
        for path in (research_output, watch, analysis_input, analysis_output):
            path.mkdir(parents=True, exist_ok=True)
        return EventPaths(
            root=root,
            state=root / "state.json",
            event=root / "event.json",
            research_output=research_output,
            research=research_output / "research.json",
            research_metrics=research_output / "research.metrics.json",
            watch=watch,
            watch_plan=watch / "plan.json",
            analysis_input=analysis_input,
            analysis_event=analysis_input / "event.json",
            analysis_research=analysis_input / "research.json",
            analysis_brief=analysis_input / "analysis_brief.md",
            report=analysis_input / "report.json",
            analysis_output=analysis_output,
            decision=analysis_output / "decision.json",
            decision_metrics=analysis_output / "decision.metrics.json",
        )

    # 已有batch配置保持不变，只追加新的event定义。
    def create_batch(
        self,
        batch_id: str,
        batch: dict[str, Any],
        market_universe: dict[str, Any],
    ) -> dict[str, Any]:
        paths = self.batch_paths(batch_id)
        if paths.batch.exists():
            stored = self._read(paths.batch, "batch")
            if stored.get("batch_id") != batch_id:
                raise ValueError(f"stored batch_id mismatch: {batch_id}")
            merged = self._append_events(stored, batch)
            if merged != stored:
                self._write(paths.batch, merged)
        else:
            merged = batch
            self._write(paths.batch, merged)

        if not paths.market_universe.exists():
            self._write(paths.market_universe, market_universe)
        if not paths.state.exists():
            self._write(
                paths.state,
                {
                    "batch_id": batch_id,
                    "state": "created",
                    "updated_ns": time.time_ns(),
                },
            )
        return merged

    def create_event(
        self,
        batch_id: str,
        event_id: str,
        metadata: dict[str, Any],
        watch_plan: dict[str, Any],
    ) -> EventPaths:
        paths = self.event_paths(batch_id, event_id)
        if not paths.event.exists():
            self._write(
                paths.event,
                {
                    "event_id": event_id,
                    "metadata": metadata,
                },
            )
        else:
            stored = self._read(paths.event, "event")
            if stored.get("event_id") != event_id:
                raise ValueError(f"stored event_id mismatch: {event_id}")
        if not paths.watch_plan.exists():
            self._write(paths.watch_plan, watch_plan)
        if not paths.state.exists():
            self._write(
                paths.state,
                {
                    "event_id": event_id,
                    "state": "created",
                    "updated_ns": time.time_ns(),
                },
            )
        return paths

    def load_batch(self, batch_id: str) -> dict[str, Any]:
        return self._read(self.batch_paths(batch_id).state, "batch state")

    def update_batch(self, batch_id: str, state: str, **values: Any) -> dict[str, Any]:
        paths = self.batch_paths(batch_id)
        payload = self._read(paths.state, "batch state")
        payload.update(values)
        payload["state"] = state
        payload["updated_ns"] = time.time_ns()
        self._write(paths.state, payload)
        return payload

    def load(self, batch_id: str, event_id: str) -> dict[str, Any]:
        return self._read(
            self.event_paths(batch_id, event_id).state,
            "event state",
        )

    def update(
        self,
        batch_id: str,
        event_id: str,
        state: str,
        **values: Any,
    ) -> dict[str, Any]:
        paths = self.event_paths(batch_id, event_id)
        payload = self._read(paths.state, "event state")
        payload.update(values)
        payload["state"] = state
        payload["updated_ns"] = time.time_ns()
        self._write(paths.state, payload)
        return payload

    def save_analysis_input(
        self,
        batch_id: str,
        event_id: str,
        research: dict[str, Any],
        brief: str,
    ) -> EventPaths:
        if not isinstance(brief, str) or not brief.strip():
            raise TypeError("analysis_brief must be non-empty text")
        paths = self.event_paths(batch_id, event_id)
        self._write(paths.analysis_event, self._read(paths.event, "event"))
        self._write(paths.analysis_research, research)
        temporary = paths.analysis_brief.with_suffix(".md.tmp")
        temporary.write_text(brief.strip() + "\n", encoding="utf-8")
        temporary.replace(paths.analysis_brief)
        return paths

    @staticmethod
    def _append_events(
        stored: dict[str, Any],
        incoming: dict[str, Any],
    ) -> dict[str, Any]:
        existing_events = stored.get("events")
        new_events = incoming.get("events")
        if not isinstance(existing_events, list) or not isinstance(new_events, list):
            raise TypeError("batch events must be arrays")
        existing_ids = {
            item.get("event_id")
            for item in existing_events
            if isinstance(item, dict)
        }
        additions = [
            item
            for item in new_events
            if isinstance(item, dict) and item.get("event_id") not in existing_ids
        ]
        if not additions:
            return stored
        merged = dict(stored)
        merged["events"] = [*existing_events, *additions]
        return merged

    @staticmethod
    def _validate_id(value: str, name: str) -> None:
        if not ID_PATTERN.fullmatch(value):
            raise ValueError(f"invalid {name}: {value!r}")

    @staticmethod
    def _read(path: Path, name: str) -> dict[str, Any]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError(f"{name} must be a JSON object")
        return payload

    @staticmethod
    def _write(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(f"{path.suffix}.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(path)
