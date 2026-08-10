from __future__ import annotations

import asyncio
import json
import sys
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategies.agent_trading.agents import ResearchAgent
from strategies.agent_trading.controller import PROMPTS_DIR
from strategies.agent_trading.controller import RESEARCH_PROFILE
from strategies.agent_trading.controller import RESOURCES_DIR
from strategies.agent_trading.controller import SCHEDULE_PATH
from strategies.agent_trading.event_store import EventStore
from strategies.agent_trading.lifecycle import BatchPlan
from strategies.agent_trading.lifecycle import load_market_universe
from tools.codex_agent import CodexRunner


async def run(event_id: str) -> None:
    schedule = json.loads(SCHEDULE_PATH.read_text(encoding="utf-8"))
    matches = [
        batch
        for batch in schedule["batches"]
        for event in batch["events"]
        if event["event_id"] == event_id
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one batch for {event_id}, got {matches}")

    batch_payload = matches[0]
    batch = BatchPlan.from_dict(batch_payload)
    batch_id = batch.batch_id
    event = next(item for item in batch.events if item.event_id == event_id)
    universe_path = SCHEDULE_PATH.with_name(
        f"{SCHEDULE_PATH.stem}_market_universe.json"
    )
    market_universe = load_market_universe(universe_path)
    strategy_root = RESOURCES_DIR.parent
    store = EventStore(strategy_root)
    store.create_batch(batch_id, batch.to_dict(), market_universe.to_dict())
    store.update_batch(batch_id, "running")
    paths = store.create_event(
        batch_id,
        event_id,
        {
            "batch_id": batch_id,
            "company": event.company,
            "ticker": event.ticker,
            "scope": event.scope,
            "confirmed": event.confirmed,
            "research_hints": list(event.research_hints),
        },
        event.watch_plan.to_dict(),
    )

    if paths.research.exists():
        state = store.load(batch_id, event_id)
        session_id = state.get("research_session_id")
        if (
            state.get("research_complete") is True
            and isinstance(session_id, str)
            and session_id.strip()
            and paths.research.read_text(encoding="utf-8").strip()
        ):
            print(json.dumps({"event_id": event_id, "status": "reused"}))
            return

    store.update(
        batch_id,
        event_id,
        "researching",
        research_complete=False,
        research_error=None,
    )
    agent = ResearchAgent(
        CodexRunner(),
        PROMPTS_DIR,
        RESEARCH_PROFILE,
    )
    outcome = await agent.run_event(
        event_id,
        store.batch_paths(batch_id).root,
        datetime.now(UTC) + timedelta(minutes=40),
    )
    if not outcome.ready:
        store.update(
            batch_id,
            event_id,
            "research_incomplete",
            research_complete=False,
            research_error=outcome.error or "research memo or session id is missing",
        )
        print(json.dumps({"event_id": event_id, "status": "incomplete"}))
        return

    store.update(
        batch_id,
        event_id,
        "research_ready",
        research_complete=True,
        research_error=None,
        research_session_id=outcome.session_id,
        research_completed_at=datetime.now(UTC).isoformat(),
        research_path=paths.research.relative_to(paths.root).as_posix(),
    )
    print(json.dumps({"event_id": event_id, "status": "completed"}))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: run_single_research.py EVENT_ID")
    asyncio.run(run(sys.argv[1]))
