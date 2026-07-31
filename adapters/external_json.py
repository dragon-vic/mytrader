import asyncio
import json
from typing import Any

from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.core.data import Data
from nautilus_trader.data.messages import RequestData
from nautilus_trader.data.messages import SubscribeData
from nautilus_trader.data.messages import UnsubscribeData
from nautilus_trader.live.data_client import LiveDataClient
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.model.custom import customdataclass
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import DataType
from nautilus_trader.model.identifiers import ClientId

from adapters.common import LiveContext
from utils.constants import EXTERNAL_JSON_CLIENT_NAME
from utils.constants import EXTERNAL_JSON_DEFAULT_HOST
from utils.constants import EXTERNAL_JSON_DEFAULT_PORT
from utils.constants import EXTERNAL_JSON_SEND_TOPIC


@customdataclass
class ExternalJson(Data):
    payload: str = "{}"


# 构建外部 JSON 的 NT data type。
def external_json_type() -> DataType:
    return DataType(ExternalJson)


class ExternalJsonDataClientConfig(LiveDataClientConfig, frozen=True):
    host: str = EXTERNAL_JSON_DEFAULT_HOST
    port: int = EXTERNAL_JSON_DEFAULT_PORT


class ExternalJsonDataClient(LiveDataClient):
    """Bidirectional newline-delimited JSON over TCP."""

    def __init__(self, loop, msgbus, cache, clock, config, name=None):
        super().__init__(
            loop=loop,
            client_id=ClientId(name or EXTERNAL_JSON_CLIENT_NAME),
            venue=None,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        self._config = config
        self._server = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._subscribed = False
        self._bus_subscribed = False

    async def _connect(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            self._config.host,
            self._config.port,
        )
        self._msgbus.subscribe(EXTERNAL_JSON_SEND_TOPIC, self._send_json)
        self._bus_subscribed = True
        self._log.info(f"Listening external JSON on {self._config.host}:{self._config.port}")

    async def _disconnect(self) -> None:
        if self._bus_subscribed:
            self._msgbus.unsubscribe(EXTERNAL_JSON_SEND_TOPIC, self._send_json)
            self._bus_subscribed = False
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        writers = tuple(self._writers)
        self._writers.clear()
        for writer in writers:
            writer.close()
        for writer in writers:
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _subscribe(self, command: SubscribeData) -> None:
        self._subscribed = True
        self._log.info(f"Subscribed: {command.data_type}")

    async def _unsubscribe(self, command: UnsubscribeData) -> None:
        self._subscribed = False
        self._log.info(f"Unsubscribed: {command.data_type}")

    async def _request(self, request: RequestData) -> None:
        self._log.error("ExternalJsonDataClient does not support request_data")

    # 一个 TCP 连接可以连续双向传输多条 JSON。
    async def _handle_client(self, reader, writer) -> None:
        self._writers.add(writer)
        try:
            while line := await reader.readline():
                self._handle_line(line)
        except (ConnectionError, OSError) as exc:
            self._log.warning(f"External JSON connection closed: {type(exc).__name__}")
        finally:
            self._writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    # 只接受 JSON object，业务字段由订阅方解释。
    def _handle_line(self, line: bytes) -> None:
        try:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise TypeError("payload must be a JSON object")
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            self._log.warning(f"Dropped invalid external JSON: {exc}")
            return

        ts_now = self._clock.timestamp_ns()
        data = ExternalJson(
            ts_now,
            ts_now,
            payload=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )
        if self._subscribed:
            self._handle_data(CustomData(data_type=external_json_type(), data=data))

    # NT 组件向该 topic 发布 dict，客户端异步广播给外部连接。
    def _send_json(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            raise TypeError(f"{EXTERNAL_JSON_SEND_TOPIC} requires dict")
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        self.create_task(self._broadcast(line), log_msg="external_json_send")

    async def _broadcast(self, line: bytes) -> None:
        if not self._writers:
            self._log.warning("Dropped outbound external JSON: no client connected")
            return
        for writer in tuple(self._writers):
            try:
                writer.write(line)
                await writer.drain()
            except (ConnectionError, OSError) as exc:
                self._writers.discard(writer)
                writer.close()
                self._log.warning(f"External JSON send failed: {type(exc).__name__}")


class ExternalJsonLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        return ExternalJsonDataClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
            name=name,
        )


def normalize_client(_cfg: dict[str, Any]) -> None:
    pass


# 构建双向外部 JSON live data client 配置。
def build_data_client(_context: LiveContext, cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        ExternalJsonDataClientConfig(
            host=cfg["host"],
            port=int(cfg["port"]),
        ),
        ExternalJsonLiveDataClientFactory,
    )
