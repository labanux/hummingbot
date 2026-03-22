import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_user_stream_data_source import (
    LighterPerpetualUserStreamDataSource,
)


class TestLighterPerpetualUserStreamDataSource(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.auth = MagicMock()
        self.auth.get_auth_token.return_value = "test_auth_token"
        self.connector = MagicMock()
        self.connector.lighter_account_index = 100
        self.api_factory = MagicMock()
        self.domain = CONSTANTS.TESTNET_DOMAIN

        self.data_source = LighterPerpetualUserStreamDataSource(
            auth=self.auth,
            trading_pairs=["ETH-USD"],
            connector=self.connector,
            api_factory=self.api_factory,
            domain=self.domain,
        )

    # --- last_recv_time ---

    def test_last_recv_time_no_ws(self):
        self.data_source._ws_assistant = None
        self.assertEqual(0, self.data_source.last_recv_time)

    def test_last_recv_time_with_ws(self):
        mock_ws = MagicMock()
        mock_ws.last_recv_time = 1234567890.0
        self.data_source._ws_assistant = mock_ws
        self.assertEqual(1234567890.0, self.data_source.last_recv_time)

    # --- _get_ws_assistant ---

    async def test_get_ws_assistant_lazy_init(self):
        mock_ws = AsyncMock()
        self.api_factory.get_ws_assistant = AsyncMock(return_value=mock_ws)
        self.assertIsNone(self.data_source._ws_assistant)
        result = await self.data_source._get_ws_assistant()
        self.assertIs(mock_ws, result)
        self.assertIs(mock_ws, self.data_source._ws_assistant)

    async def test_get_ws_assistant_caches(self):
        mock_ws = AsyncMock()
        self.api_factory.get_ws_assistant = AsyncMock(return_value=mock_ws)
        await self.data_source._get_ws_assistant()
        await self.data_source._get_ws_assistant()
        # Should only create once
        self.assertEqual(1, self.api_factory.get_ws_assistant.call_count)

    # --- _connected_websocket_assistant ---

    async def test_connected_websocket_assistant(self):
        mock_ws = AsyncMock()
        self.api_factory.get_ws_assistant = AsyncMock(return_value=mock_ws)
        result = await self.data_source._connected_websocket_assistant()
        self.assertIs(mock_ws, result)
        mock_ws.connect.assert_called_once()
        call_kwargs = mock_ws.connect.call_args[1]
        self.assertIn("ws_url", call_kwargs)
        self.assertIn("ping_timeout", call_kwargs)

    # --- _subscribe_channels ---

    async def test_subscribe_channels_sends_three_payloads(self):
        mock_ws = AsyncMock()
        await self.data_source._subscribe_channels(mock_ws)
        self.assertEqual(3, mock_ws.send.call_count)

    async def test_subscribe_channels_orders_has_auth(self):
        mock_ws = AsyncMock()
        await self.data_source._subscribe_channels(mock_ws)
        # First call: account_all_orders with auth
        orders_payload = mock_ws.send.call_args_list[0][0][0].payload
        self.assertEqual("subscribe", orders_payload["type"])
        self.assertIn("account_all_orders/100", orders_payload["channel"])
        self.assertEqual("test_auth_token", orders_payload["auth"])

    async def test_subscribe_channels_account_all_no_auth(self):
        mock_ws = AsyncMock()
        await self.data_source._subscribe_channels(mock_ws)
        # Second call: account_all without auth
        account_payload = mock_ws.send.call_args_list[1][0][0].payload
        self.assertEqual("subscribe", account_payload["type"])
        self.assertIn("account_all/100", account_payload["channel"])
        self.assertNotIn("auth", account_payload)

    async def test_subscribe_channels_user_stats_has_auth(self):
        mock_ws = AsyncMock()
        await self.data_source._subscribe_channels(mock_ws)
        # Third call: user_stats with auth
        stats_payload = mock_ws.send.call_args_list[2][0][0].payload
        self.assertEqual("subscribe", stats_payload["type"])
        self.assertIn("user_stats/100", stats_payload["channel"])
        self.assertEqual("test_auth_token", stats_payload["auth"])

    async def test_subscribe_channels_exception_propagates(self):
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = Exception("Connection failed")
        with self.assertRaises(Exception):
            await self.data_source._subscribe_channels(mock_ws)

    async def test_subscribe_channels_cancelled_error_propagates(self):
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_ws)

    # --- _process_event_message ---

    async def test_process_event_message_ignores_ping(self):
        queue = asyncio.Queue()
        await self.data_source._process_event_message({"type": "ping"}, queue)
        self.assertEqual(0, queue.qsize())

    async def test_process_event_message_ignores_connected(self):
        queue = asyncio.Queue()
        await self.data_source._process_event_message({"type": "connected"}, queue)
        self.assertEqual(0, queue.qsize())

    async def test_process_event_message_ignores_subscribed(self):
        queue = asyncio.Queue()
        await self.data_source._process_event_message(
            {"type": "subscribed/account_all", "channel": "account_all:100"}, queue
        )
        self.assertEqual(0, queue.qsize())

    async def test_process_event_message_raises_on_error_dict(self):
        queue = asyncio.Queue()
        with self.assertRaises(IOError):
            await self.data_source._process_event_message(
                {"type": "error", "error": {"message": "Auth failed"}}, queue
            )

    async def test_process_event_message_raises_on_error_string(self):
        queue = asyncio.Queue()
        with self.assertRaises(IOError):
            await self.data_source._process_event_message(
                {"type": "error", "error": "Something went wrong"}, queue
            )

    async def test_process_event_message_routes_account_all_orders(self):
        queue = asyncio.Queue()
        msg = {
            "type": "update/account_all_orders",
            "channel": "account_all_orders:100",
            "orders": {"0": [{"order_index": 1}]},
        }
        await self.data_source._process_event_message(msg, queue)
        self.assertEqual(1, queue.qsize())
        self.assertEqual(msg, queue.get_nowait())

    async def test_process_event_message_routes_account_all(self):
        queue = asyncio.Queue()
        msg = {
            "type": "update/account_all",
            "channel": "account_all:100",
            "positions": {},
        }
        await self.data_source._process_event_message(msg, queue)
        self.assertEqual(1, queue.qsize())

    async def test_process_event_message_routes_user_stats(self):
        queue = asyncio.Queue()
        msg = {
            "type": "update/user_stats",
            "channel": "user_stats:100",
            "stats": {"collateral": "10000"},
        }
        await self.data_source._process_event_message(msg, queue)
        self.assertEqual(1, queue.qsize())

    async def test_process_event_message_ignores_unknown_channel(self):
        queue = asyncio.Queue()
        msg = {
            "type": "update",
            "channel": "unknown_channel:0",
            "data": {},
        }
        await self.data_source._process_event_message(msg, queue)
        self.assertEqual(0, queue.qsize())
