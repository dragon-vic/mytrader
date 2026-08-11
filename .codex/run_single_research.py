from __future__ import annotations

import asyncio
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategies.agent_trading.agents import ResearchAgent
from strategies.agent_trading.controller import (
    PROMPTS_DIR,
    RESEARCH_PROFILE,
    RESOURCES_DIR,
    SCHEDULE_PATH,
)
from strategies.agent_trading.event_store import EventState, EventStore, FailureStage
from strategies.agent_trading.lifecycle import load_event_plan, load_market_universe
from tools.codex_agent import CodexRunner


async def run(event_id: str) -> None:
    schedule = json.loads(SCHEDULE_PATH.read_text(encoding="utf-8"))
    matches = [
        batch["batch_id"]
        for batch in schedule["batches"]
        for event in batch["events"]
        if event["event_id"] == event_id
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one schedule group for {event_id}, got {matches}")

    group_id = matches[0]
    event = load_event_plan(SCHEDULE_PATH, group_id, event_id)
    universe_path = SCHEDULE_PATH.with_name(
        f"{SCHEDULE_PATH.stem}_market_universe.json"
    )
    market_universe = load_market_universe(universe_path)
    strategy_root = RESOURCES_DIR.parent
    store = EventStore(strategy_root)
    store.ensure_event_group(group_id, market_universe.to_dict())
    paths = store.create_event(
        group_id,
        event_id,
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

    if store.load_research_handoff(group_id, event_id) is not None:
        print(json.dumps({"event_id": event_id, "status": "reused"}))
        return

    store.update(
        group_id,
        event_id,
        EventState.RESEARCHING,
    )
    agent = ResearchAgent(
        CodexRunner(),
        PROMPTS_DIR,
        RESEARCH_PROFILE,
    )
    outcome = await agent.run_event(
        event_id,
        store.event_group_paths(group_id).root,
        datetime.now(UTC) + timedelta(minutes=40),
    )
    if not outcome.ready:
        store.update(
            group_id,
            event_id,
            EventState.FAILED,
            failed_stage=FailureStage.RESEARCH,
            error=outcome.error or "research memo is missing",
        )
        print(json.dumps({"event_id": event_id, "status": "failed"}))
        return

    store.update(
        group_id,
        event_id,
        EventState.RESEARCH_READY,
        research_session_id=outcome.session_id,
        research_completed_at=datetime.now(UTC).isoformat(),
        research_path=paths.research.relative_to(paths.root).as_posix(),
    )
    print(json.dumps({"event_id": event_id, "status": "completed"}))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: run_single_research.py EVENT_ID")
    asyncio.run(run(sys.argv[1]))
