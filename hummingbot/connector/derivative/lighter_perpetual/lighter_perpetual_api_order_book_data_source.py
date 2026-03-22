import asyncio
import json
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_utils import (
    market_id_to_pair,
    pair_to_market_id,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative import (
        LighterPerpetualDerivative,
    )


class LighterPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            trading_pairs: List[str],
            connector: 'LighterPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._trading_pairs: List[str] = trading_pairs
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._funding_info_messages_queue_key = "funding_info"
        self._snapshot_messages_queue_key = "order_book_snapshot"
        self._last_ob_nonce: Dict[int, int] = {}  # market_id → last nonce

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        market_id = pair_to_market_id(trading_pair)
        response = await self._connector._api_get(
            path_url=CONSTANTS.ORDER_BOOK_DETAILS_URL,
            params={"market_id": market_id, "filter": "perp"},
        )
        details = response.get("order_book_details", [])
        for detail in details:
            if detail.get("market_id") == market_id:
                return FundingInfo(
                    trading_pair=trading_pair,
                    index_price=Decimal(str(detail.get("index_price", detail.get("last_trade_price", 0)))),
                    mark_price=Decimal(str(detail.get("mark_price", detail.get("last_trade_price", 0)))),
                    next_funding_utc_timestamp=self._next_funding_time(),
                    rate=Decimal(str(detail.get("funding_rate", "0"))),
                )
        # Fallback placeholder
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal("0"),
            mark_price=Decimal("0"),
            next_funding_utc_timestamp=self._next_funding_time(),
            rate=Decimal("0"),
        )

    async def listen_for_funding_info(self, output: asyncio.Queue):
        message_queue = self._message_queue[self._funding_info_messages_queue_key]
        while True:
            try:
                funding_info_event = await message_queue.get()
                await self._parse_funding_info_message(funding_info_event, output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public funding info updates from exchange")
                await self._sleep(5)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        market_id = pair_to_market_id(trading_pair)
        data = await self._connector._api_get(
            path_url=CONSTANTS.ORDER_BOOK_URL,
            params={"market_id": market_id},
        )
        return data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response = await self._request_order_book_snapshot(trading_pair)
        timestamp = time.time()
        bids = [[float(b["price"]), float(b["size"])] for b in snapshot_response.get("bids", [])]
        asks = [[float(a["price"]), float(a["size"])] for a in snapshot_response.get("asks", [])]
        snapshot_msg = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair,
            "update_id": int(timestamp * 1e3),
            "bids": bids,
            "asks": asks,
        }, timestamp=timestamp)
        return snapshot_msg

    async def _connected_websocket_assistant(self) -> WSAssistant:
        url = web_utils.wss_url(self._domain)
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                market_id = pair_to_market_id(trading_pair)

                # Subscribe to order book channel
                ob_payload = {
                    "type": "subscribe",
                    "channel": f"{CONSTANTS.WS_ORDER_BOOK_CHANNEL}/{market_id}",
                }
                await ws.send(WSJSONRequest(payload=ob_payload))

            self.logger().info("Subscribed to public order book channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book data streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        msg_type = event_message.get("type", "")
        msg_channel = event_message.get("channel", "")

        if msg_type == "ping":
            return ""

        if CONSTANTS.WS_ORDER_BOOK_CHANNEL in msg_channel:
            channel = self._snapshot_messages_queue_key
        elif CONSTANTS.WS_MARKET_STATS_CHANNEL in msg_channel:
            channel = self._funding_info_messages_queue_key
        return channel

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        channel = raw_message.get("channel", "")
        if CONSTANTS.WS_ORDER_BOOK_CHANNEL not in channel:
            return

        market_id_str = channel.split(":")[1] if ":" in channel else ""
        if not market_id_str:
            return
        market_id = int(market_id_str)

        ob_data = raw_message.get("order_book", {})

        # Check begin_nonce continuity
        begin_nonce = ob_data.get("begin_nonce")
        last_nonce = self._last_ob_nonce.get(market_id)
        if last_nonce is not None and begin_nonce is not None and begin_nonce != last_nonce:
            self.logger().warning(
                f"Order book nonce gap for market {market_id}: "
                f"expected begin_nonce={last_nonce}, got {begin_nonce}. Re-snapshotting."
            )
            self._last_ob_nonce.pop(market_id, None)
            return

        if ob_data.get("nonce") is not None:
            self._last_ob_nonce[market_id] = ob_data["nonce"]

        try:
            trading_pair = market_id_to_pair(market_id)
        except KeyError:
            return

        timestamp = time.time()
        bids = [[float(b["price"]), float(b["size"])] for b in ob_data.get("bids", [])]
        asks = [[float(a["price"]), float(a["size"])] for a in ob_data.get("asks", [])]

        order_book_message = OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": trading_pair,
            "update_id": raw_message.get("offset", int(timestamp * 1e3)),
            "bids": bids,
            "asks": asks,
        }, timestamp=timestamp)
        message_queue.put_nowait(order_book_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # On Lighter, the first message after subscribe is a full snapshot.
        # We treat subscribed/order_book as snapshot, update/order_book as diff.
        channel = raw_message.get("channel", "")
        msg_type = raw_message.get("type", "")

        if CONSTANTS.WS_ORDER_BOOK_CHANNEL not in channel:
            return

        market_id_str = channel.split(":")[1] if ":" in channel else ""
        if not market_id_str:
            return
        market_id = int(market_id_str)

        ob_data = raw_message.get("order_book", {})
        if ob_data.get("nonce") is not None:
            self._last_ob_nonce[market_id] = ob_data["nonce"]

        try:
            trading_pair = market_id_to_pair(market_id)
        except KeyError:
            return

        timestamp = time.time()
        bids = [[float(b["price"]), float(b["size"])] for b in ob_data.get("bids", [])]
        asks = [[float(a["price"]), float(a["size"])] for a in ob_data.get("asks", [])]

        order_book_message = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair,
            "update_id": raw_message.get("offset", int(timestamp * 1e3)),
            "bids": bids,
            "asks": asks,
        }, timestamp=timestamp)
        message_queue.put_nowait(order_book_message)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # Lighter does not have a separate trades WS channel for the order book data source.
        # Trade data comes through account_all channel for the user stream.
        pass

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        try:
            channel = raw_message.get("channel", "")
            if CONSTANTS.WS_MARKET_STATS_CHANNEL not in channel:
                return

            market_id_str = channel.split(":")[1] if ":" in channel else ""
            if not market_id_str:
                return
            market_id = int(market_id_str)

            try:
                trading_pair = market_id_to_pair(market_id)
            except KeyError:
                return

            if trading_pair not in self._trading_pairs:
                return

            stats = raw_message.get("stats", {})
            funding_info = FundingInfoUpdate(
                trading_pair=trading_pair,
                index_price=Decimal(str(stats.get("index_price", "0"))),
                mark_price=Decimal(str(stats.get("mark_price", "0"))),
                next_funding_utc_timestamp=self._next_funding_time(),
                rate=Decimal(str(stats.get("funding_rate", "0"))),
            )
            message_queue.put_nowait(funding_info)
        except Exception as e:
            self.logger().debug(f"Error parsing funding info message: {e}")

    def _next_funding_time(self) -> int:
        """Funding settlement occurs every 1 hour."""
        return int(((time.time() // 3600) + 1) * 3600)

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        # Start keepalive task alongside message processing
        keepalive_task = asyncio.ensure_future(self._keepalive_loop(websocket_assistant))
        try:
            while True:
                try:
                    await super()._process_websocket_messages(
                        websocket_assistant=websocket_assistant,
                        queue=queue)
                except asyncio.TimeoutError:
                    # Send pong on timeout to keep connection alive
                    pong_request = WSJSONRequest(payload={"type": "pong"})
                    await websocket_assistant.send(pong_request)
        finally:
            keepalive_task.cancel()

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            return False
        try:
            market_id = pair_to_market_id(trading_pair)
            ob_payload = {
                "type": "subscribe",
                "channel": f"{CONSTANTS.WS_ORDER_BOOK_CHANNEL}/{market_id}",
            }
            await self._ws_assistant.send(WSJSONRequest(payload=ob_payload))
            self.logger().info(f"Subscribed to {trading_pair} order book channel")
            return True
        except Exception as e:
            self.logger().error(f"Error subscribing to {trading_pair}: {e}")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            return False
        try:
            market_id = pair_to_market_id(trading_pair)
            payload = {
                "type": "unsubscribe",
                "channel": f"{CONSTANTS.WS_ORDER_BOOK_CHANNEL}/{market_id}",
            }
            await self._ws_assistant.send(WSJSONRequest(payload=payload))
            self.logger().info(f"Unsubscribed from {trading_pair} order book channel")
            return True
        except Exception as e:
            self.logger().error(f"Error unsubscribing from {trading_pair}: {e}")
            return False

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
