from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

from strategies.agent_trading.agents import AnalysisAgent
from strategies.agent_trading.agents import CodexRunner
from strategies.agent_trading.agents import ResearchAgent
from strategies.agent_trading.event_store import EventStore
from strategies.agent_trading.watch import DisclosurePackage
from strategies.agent_trading.watch import DisclosureWatcher
from strategies.agent_trading.watch import WatchPlan


SEC_USER_AGENT = "nt_quant-agent-trading/1.0 victorice@yeah.net"


class AgentController:
    def __init__(
        self,
        host: str,
        port: int,
        event_store: EventStore,
        research_agent: ResearchAgent,
        analysis_agent: AnalysisAgent,
        disclosure_watcher: DisclosureWatcher,
    ) -> None:
        self.host = host
        self.port = port
        self.event_store = event_store
        self.research_agent = research_agent
        self.analysis_agent = analysis_agent
        self.disclosure_watcher = disclosure_watcher
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    # 连接 NT 的 ExternalJson data client。
    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    # 关闭与 NT 的双向连接。
    async def close(self) -> None:
        if self.writer is None:
            return
        self.writer.close()
        await self.writer.wait_closed()
        self.reader = None
        self.writer = None

    # 向 NT 发送一个 JSON object。
    async def send(self, payload: dict[str, Any]) -> None:
        if self.writer is None:
            raise RuntimeError("AgentController is not connected")
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        self.writer.write(line)
        await self.writer.drain()

    # 等待 NT 返回一个 JSON object。
    async def receive(self) -> dict[str, Any]:
        if self.reader is None:
            raise RuntimeError("AgentController is not connected")
        line = await self.reader.readline()
        if not line:
            raise ConnectionError("ExternalJson connection closed")
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise TypeError("payload must be a JSON object")
        return payload

    # 登记一个待研究的财报事件。
    def create_event(self, event_id: str, metadata: dict[str, Any]) -> None:
        self.event_store.create(event_id, metadata)

    # 外部进程使用系统时间调度研究，不占用 NT 的时钟和事件循环。
    async def wait_until(self, timestamp_ns: int) -> None:
        delay = (timestamp_ns - time.time_ns()) / 1_000_000_000
        if delay > 0:
            await asyncio.sleep(delay)

    # 调用预研 Agent；下一次研究时间将在预研 schema 确定后由这里接管。
    async def run_research(self, event_id: str, prompt: str) -> str:
        paths = self.event_store.paths(event_id)
        self.event_store.update(event_id, "researching")
        result = await self.research_agent.run(prompt, paths.root, paths.research)
        self.event_store.update(
            event_id,
            "research_ready",
            research_path=str(paths.research),
        )
        return result

    # 校验并持久化预研 Agent 给 Watcher 的受限配置。
    def set_watch_plan(self, event_id: str, payload: dict[str, Any]) -> WatchPlan:
        plan = WatchPlan.from_dict(payload)
        if plan.event_id != event_id:
            raise ValueError(
                f"watch plan event_id mismatch: {plan.event_id} != {event_id}",
            )
        path = self.event_store.save_plan(event_id, plan.to_dict())
        self.event_store.update(
            event_id,
            "watch_plan_ready",
            watch_plan_path=str(path),
        )
        return plan

    # 两个信息源谁先完成核心披露下载，谁就唤醒 Controller。
    async def wait_report(self, event_id: str) -> DisclosurePackage:
        paths = self.event_store.paths(event_id)
        plan = WatchPlan.from_dict(self.event_store.load_plan(event_id))
        if plan.event_id != event_id:
            raise ValueError(
                f"stored watch plan event_id mismatch: {plan.event_id} != {event_id}",
            )
        self.event_store.update(event_id, "watching_disclosure")
        package = await self.disclosure_watcher.watch(plan, paths.context)
        self.event_store.update(
            event_id,
            "report_ready",
            report_path=str(paths.report),
            report_source=package.source,
            report_detected_ns=package.detected_ns,
        )
        return package

    # 调用最终分析 Agent，输出待发往 NT 的 JSON 决策。
    async def run_analysis(
        self,
        event_id: str,
        prompt: str,
        schema_path: Path | None = None,
    ) -> str:
        paths = self.event_store.paths(event_id)
        self.event_store.update(event_id, "analyzing")
        result = await self.analysis_agent.run(
            prompt,
            paths.root,
            paths.decision,
            schema_path,
        )
        self.event_store.update(
            event_id,
            "decision_ready",
            decision_path=str(paths.decision),
        )
        return result

    # 决策 schema 确认后仍保持原样透传，不在 Controller 内改写交易含义。
    async def send_decision(self, event_id: str) -> None:
        paths = self.event_store.paths(event_id)
        payload = json.loads(paths.decision.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("decision must be a JSON object")
        await self.send(payload)
        self.event_store.update(event_id, "decision_sent")

    # 单独接收 NT 的订单、成交和仓位回报，生命周期处理随后接入。
    async def receive_forever(self) -> None:
        while True:
            payload = await self.receive()
            print(json.dumps(payload, ensure_ascii=False), flush=True)


async def main() -> None:
    strategy_root = Path(__file__).resolve().parent
    runner = CodexRunner()
    async with DisclosureWatcher(
        user_agent=SEC_USER_AGENT,
        poll_seconds=0.5,
    ) as watcher:
        controller = AgentController(
            host="127.0.0.1",
            port=9003,
            event_store=EventStore(strategy_root / "events"),
            research_agent=ResearchAgent(runner),
            analysis_agent=AnalysisAgent(runner),
            disclosure_watcher=watcher,
        )
        await controller.connect()
        try:
            await controller.receive_forever()
        finally:
            await controller.close()


if __name__ == "__main__":
    asyncio.run(main())
