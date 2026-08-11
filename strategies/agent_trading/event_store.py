from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


class EventState(StrEnum):
    CREATED = "created"
    RESEARCHING = "researching"
    RESEARCH_READY = "research_ready"
    ANALYSIS_INPUT_READY = "analysis_input_ready"
    WATCHING_DISCLOSURE = "watching_disclosure"
    REPORT_READY = "report_ready"
    ANALYZING = "analyzing"
    DECISION_READY = "decision_ready"
    DECISION_SENT = "decision_sent"
    FAILED = "failed"

    @property
    def is_finished(self) -> bool:
        return self in {EventState.DECISION_READY, EventState.DECISION_SENT}


class FailureStage(StrEnum):
    RESEARCH = "research"
    WATCH = "watch"
    ANALYSIS = "analysis"
    LIFECYCLE = "lifecycle"


@dataclass(frozen=True)
class ResearchHandoff:
    session_id: str | None
    memo: str | None

    def __post_init__(self) -> None:
        if (self.session_id is None) == (self.memo is None):
            raise ValueError("research handoff requires exactly one context source")
        if self.session_id is not None and not self.session_id.strip():
            raise ValueError("research handoff session id must not be empty")
        if self.memo is not None and not self.memo.strip():
            raise ValueError("research handoff memo must not be empty")


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
    report: Path
    analysis_output: Path
    decision: Path
    decision_metrics: Path


@dataclass(frozen=True)
class EventGroupPaths:
    root: Path
    market_universe: Path
    events: Path


class EventStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    # 同一时间附近的 event 共用目录和市场快照；这里没有 group 生命周期状态。
    def event_group_paths(self, group_id: str) -> EventGroupPaths:
        self._validate_id(group_id, "group_id")
        root = (self.root / "batches" / group_id).resolve()
        root.relative_to(self.root)
        events = root / "events"
        events.mkdir(parents=True, exist_ok=True)
        return EventGroupPaths(
            root=root,
            market_universe=root / "market_universe.json",
            events=events,
        )

    # event 始终归属于一个 schedule group，但所有生命周期数据都在 event 内。
    def event_paths(self, group_id: str, event_id: str) -> EventPaths:
        self._validate_id(group_id, "group_id")
        self._validate_id(event_id, "event_id")
        group = self.event_group_paths(group_id)
        root = (group.events / event_id).resolve()
        root.relative_to(group.events.resolve())
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
            research=research_output / "research.md",
            research_metrics=research_output / "research.metrics.json",
            watch=watch,
            watch_plan=watch / "plan.json",
            analysis_input=analysis_input,
            analysis_event=analysis_input / "event.json",
            report=analysis_input / "report.json",
            analysis_output=analysis_output,
            decision=analysis_output / "decision.json",
            decision_metrics=analysis_output / "decision.metrics.json",
        )

    # 建立共享快照；不建立、不读取、不更新 group 状态。
    def ensure_event_group(
        self,
        group_id: str,
        market_universe: dict[str, Any],
    ) -> None:
        paths = self.event_group_paths(group_id)
        if not paths.market_universe.exists():
            self._write(paths.market_universe, market_universe)

    def create_event(
        self,
        group_id: str,
        event_id: str,
        metadata: dict[str, Any],
        watch_plan: dict[str, Any],
    ) -> EventPaths:
        paths = self.event_paths(group_id, event_id)
        if paths.event.exists():
            stored = self._read(paths.event, "event")
            if stored.get("event_id") != event_id:
                raise ValueError(f"stored event_id mismatch: {event_id}")
        else:
            stored = {
                "event_id": event_id,
                "metadata": metadata,
            }
            self._write(paths.event, stored)
        if not paths.analysis_event.exists():
            self._write(paths.analysis_event, stored)
        if not paths.watch_plan.exists():
            self._write(paths.watch_plan, watch_plan)
        if not paths.state.exists():
            self._write(
                paths.state,
                {
                    "event_id": event_id,
                    "state": EventState.CREATED,
                },
            )
        return paths

    def load(self, group_id: str, event_id: str) -> dict[str, Any]:
        return self._read(
            self.event_paths(group_id, event_id).state,
            "event state",
        )

    def load_state(self, group_id: str, event_id: str) -> EventState:
        payload = self.load(group_id, event_id)
        try:
            return EventState(payload.get("state"))
        except ValueError as exc:
            raise ValueError(
                f"invalid event state for {event_id}: {payload.get('state')!r}",
            ) from exc

    # research.md 是正式交接物；session_id 只决定分析是否恢复原会话。
    def load_research_handoff(
        self,
        group_id: str,
        event_id: str,
    ) -> ResearchHandoff | None:
        if self.load_state(group_id, event_id) is not EventState.RESEARCH_READY:
            return None
        paths = self.event_paths(group_id, event_id)
        if not paths.research.is_file():
            return None
        memo = paths.research.read_text(encoding="utf-8").strip()
        if not memo:
            return None
        payload = self.load(group_id, event_id)
        session_id = payload.get("research_session_id")
        if session_id is not None:
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError(
                    f"invalid research_session_id for {event_id}",
                )
            session_id = session_id.strip()
        return ResearchHandoff(
            session_id=session_id,
            memo=memo if session_id is None else None,
        )

    def update(
        self,
        group_id: str,
        event_id: str,
        state: EventState,
        **values: Any,
    ) -> dict[str, Any]:
        paths = self.event_paths(group_id, event_id)
        payload = self._read(paths.state, "event state")
        payload.pop("research_complete", None)
        payload.pop("research_error", None)
        if state is EventState.FAILED:
            failed_stage = values.get("failed_stage")
            error = values.get("error")
            if not isinstance(failed_stage, FailureStage):
                raise ValueError("failed event state requires failed_stage")
            if not isinstance(error, str) or not error.strip():
                raise ValueError("failed event state requires error")
        else:
            payload.pop("failed_stage", None)
            payload.pop("error", None)
        payload.update(values)
        payload["state"] = state.value
        payload["updated_at"] = datetime.now(UTC).isoformat()
        self._write(paths.state, payload)
        return payload

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
