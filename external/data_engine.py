import asyncio
import json

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
from nautilus_trader.model.identifiers import InstrumentId


EXTERNAL_SIGNAL_CLIENT_NAME = "EXTERNAL_SIGNAL"


@customdataclass
class ExternalSignal(Data):
    instrument_id: InstrumentId = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    side: str = "BUY"
    sent_ns: int = 0


# 构建外部信号的 NT data type。
def external_signal_type() -> DataType:
    return DataType(ExternalSignal)


class ExternalSignalDataClientConfig(LiveDataClientConfig, frozen=True):
    host: str = "127.0.0.1"
    port: int = 9001


class ExternalSignalDataClient(LiveDataClient):
    def __init__(self, loop, msgbus, cache, clock, config, name=None):
        super().__init__(
            loop=loop,
            client_id=ClientId(name or EXTERNAL_SIGNAL_CLIENT_NAME),
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
        self._log.info(f"Listening external signals on {self._config.host}:{self._config.port}")

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
        self._log.error("ExternalSignalDataClient does not support request_data")

    async def _handle_client(self, reader, writer) -> None:
        line = await reader.readline()
        payload = json.loads(line)

        if self._subscribed:
            sent_ns = int(payload["sent_ns"])
            signal = ExternalSignal(
                sent_ns,
                sent_ns,
                instrument_id=InstrumentId.from_str(payload["instrument_id"]),
                side=payload["side"],
                sent_ns=sent_ns,
            )
            self._handle_data(CustomData(data_type=external_signal_type(), data=signal))

        writer.close()
        await writer.wait_closed()


class ExternalSignalLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        return ExternalSignalDataClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
            name=name,
        )
