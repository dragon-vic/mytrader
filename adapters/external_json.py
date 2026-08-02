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
    """Receives one JSON object per TCP connection."""

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
        self._subscribed = False

    async def _connect(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            self._config.host,
            self._config.port,
        )
        self._log.info(f"Listening external JSON on {self._config.host}:{self._config.port}")

    async def _disconnect(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _subscribe(self, command: SubscribeData) -> None:
        self._subscribed = True
        self._log.info(f"Subscribed: {command.data_type}")

    async def _unsubscribe(self, command: UnsubscribeData) -> None:
        self._subscribed = False
        self._log.info(f"Unsubscribed: {command.data_type}")

    async def _request(self, request: RequestData) -> None:
        self._log.error("ExternalJsonDataClient does not support request_data")

    # 外部进程建立短连接，每次只发送一条 JSON。
    async def _handle_client(self, reader, writer) -> None:
        try:
            self._handle_line(await reader.readline())
        except (ConnectionError, OSError) as exc:
            self._log.warning(f"External JSON connection closed: {type(exc).__name__}")
        finally:
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


# 构建单向外部 JSON live data client 配置。
def build_data_client(_context: LiveContext, cfg: dict[str, Any]):
    return (
        cfg["client_id"],
        ExternalJsonDataClientConfig(
            host=cfg["host"],
            port=int(cfg["port"]),
        ),
        ExternalJsonLiveDataClientFactory,
    )
