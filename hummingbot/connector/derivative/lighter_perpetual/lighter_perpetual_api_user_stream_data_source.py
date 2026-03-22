import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils as web_utils
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative import (
        LighterPerpetualDerivative,
    )


class LighterPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            auth: AuthBase,
            trading_pairs: List[str],
            connector: 'LighterPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__()
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._connector = connector
        self._trading_pairs: List[str] = trading_pairs
        self._ws_assistant: Optional[WSAssistant] = None

    @property
    def last_recv_time(self) -> float:
        if self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return 0

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._get_ws_assistant()
        url = web_utils.wss_url(self._domain)
        await ws.connect(ws_url=url, ping_timeout=self.HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            account_index = self._connector.lighter_account_index
            auth_token = self._auth.get_auth_token()

            # Subscribe to account_all_orders (order updates) — requires auth token
            orders_payload = {
                "type": "subscribe",
                "channel": f"{CONSTANTS.WS_ACCOUNT_ALL_ORDERS_CHANNEL}/{account_index}",
                "auth": auth_token,
            }
            await websocket_assistant.send(WSJSONRequest(payload=orders_payload))

            # Subscribe to account_all (positions, assets, trades) — no auth required
            account_payload = {
                "type": "subscribe",
                "channel": f"{CONSTANTS.WS_ACCOUNT_ALL_CHANNEL}/{account_index}",
            }
            await websocket_assistant.send(WSJSONRequest(payload=account_payload))

            # Subscribe to user_stats (balance updates) — requires auth token
            stats_payload = {
                "type": "subscribe",
                "channel": f"{CONSTANTS.WS_USER_STATS_CHANNEL}/{account_index}",
                "auth": auth_token,
            }
            await websocket_assistant.send(WSJSONRequest(payload=stats_payload))

            self.logger().info("Subscribed to private order, trade, position, and balance channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to user streams...")
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        msg_type = event_message.get("type", "")
        channel = event_message.get("channel", "")

        # Handle ping
        if msg_type == "ping":
            return

        # Handle connected
        if msg_type == "connected":
            return

        # Handle subscription confirmations
        if msg_type.startswith("subscribed/"):
            return

        # Handle errors
        if event_message.get("error") is not None:
            err_msg = event_message.get("error", {})
            if isinstance(err_msg, dict):
                err_msg = err_msg.get("message", str(err_msg))
            raise IOError({
                "label": "WSS_ERROR",
                "message": f"Error received via websocket - {err_msg}."
            })

        # Route user stream messages
        if any(ch in channel for ch in [
            CONSTANTS.WS_ACCOUNT_ALL_ORDERS_CHANNEL,
            CONSTANTS.WS_ACCOUNT_ALL_CHANNEL,
            CONSTANTS.WS_USER_STATS_CHANNEL,
        ]):
            queue.put_nowait(event_message)

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        keepalive_task = asyncio.ensure_future(self._keepalive_loop(websocket_assistant))
        try:
            while True:
                try:
                    await super()._process_websocket_messages(
                        websocket_assistant=websocket_assistant,
                        queue=queue)
                except asyncio.TimeoutError:
                    pong_request = WSJSONRequest(payload={"type": "pong"})
                    await websocket_assistant.send(pong_request)
        finally:
            keepalive_task.cancel()

    async def _keepalive_loop(self, ws: WSAssistant):
        """Send a frame every 90s to stay under the 2-minute keepalive cutoff."""
        try:
            while True:
                await asyncio.sleep(CONSTANTS.WS_KEEPALIVE_INTERVAL)
                pong_request = WSJSONRequest(payload={"type": "pong"})
                await ws.send(pong_request)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger().debug(f"Keepalive error: {e}")
