import asyncio
import time
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_order_book_data_source import (
    LighterPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_utils import (
    MarketDecimals,
    _market_decimals,
    _market_id_to_pair,
    _pair_to_market_id,
)
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class TestLighterPerpetualAPIOrderBookDataSource(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.trading_pairs = ["ETH-USD"]
        self.connector = MagicMock()
        self.api_factory = MagicMock()
        self.domain = CONSTANTS.TESTNET_DOMAIN

        # Pre-populate market maps
        _pair_to_market_id.clear()
        _market_id_to_pair.clear()
        _market_decimals.clear()
        _pair_to_market_id["ETH-USD"] = 0
        _market_id_to_pair[0] = "ETH-USD"
        _market_decimals[0] = MarketDecimals(price_decimals=2, size_decimals=4, quote_decimals=6)

        self.data_source = LighterPerpetualAPIOrderBookDataSource(
            trading_pairs=self.trading_pairs,
            connector=self.connector,
            api_factory=self.api_factory,
            domain=self.domain,
        )

    def tearDown(self):
        _pair_to_market_id.clear()
        _market_id_to_pair.clear()
        _market_decimals.clear()

    # --- get_last_traded_prices ---

    async def test_get_last_traded_prices_delegates_to_connector(self):
        self.connector.get_last_traded_prices = AsyncMock(return_value={"ETH-USD": 2100.0})
        result = await self.data_source.get_last_traded_prices(self.trading_pairs)
        self.assertEqual({"ETH-USD": 2100.0}, result)
        self.connector.get_last_traded_prices.assert_called_once_with(trading_pairs=self.trading_pairs)

    # --- get_funding_info ---

    async def test_get_funding_info_returns_funding_info(self):
        self.connector._api_get = AsyncMock(return_value={
            "order_book_details": [
                {
                    "market_id": 0,
                    "index_price": "2100.50",
                    "mark_price": "2101.00",
                    "funding_rate": "0.0001",
                    "last_trade_price": "2100.00",
                },
            ],
        })
        result = await self.data_source.get_funding_info("ETH-USD")
        self.assertIsInstance(result, FundingInfo)
        self.assertEqual("ETH-USD", result.trading_pair)
        self.assertEqual(Decimal("2100.50"), result.index_price)
        self.assertEqual(Decimal("2101.00"), result.mark_price)
        self.assertEqual(Decimal("0.0001"), result.rate)

    async def test_get_funding_info_fallback_to_last_trade_price(self):
        self.connector._api_get = AsyncMock(return_value={
            "order_book_details": [
                {
                    "market_id": 0,
                    "last_trade_price": "2000.00",
                    "funding_rate": "0",
                },
            ],
        })
        result = await self.data_source.get_funding_info("ETH-USD")
        self.assertEqual(Decimal("2000.00"), result.index_price)
        self.assertEqual(Decimal("2000.00"), result.mark_price)

    async def test_get_funding_info_fallback_placeholder(self):
        self.connector._api_get = AsyncMock(return_value={
            "order_book_details": [
                {"market_id": 999},  # Different market
            ],
        })
        result = await self.data_source.get_funding_info("ETH-USD")
        self.assertEqual(Decimal("0"), result.index_price)
        self.assertEqual(Decimal("0"), result.mark_price)

    # --- _request_order_book_snapshot ---

    async def test_request_order_book_snapshot(self):
        expected = {"bids": [{"price": "2100", "remaining_base_amount": "1"}], "asks": []}
        self.connector._api_get = AsyncMock(return_value=expected)
        result = await self.data_source._request_order_book_snapshot("ETH-USD")
        self.assertEqual(expected, result)
        self.connector._api_get.assert_called_once_with(
            path_url=CONSTANTS.ORDER_BOOK_ORDERS_URL,
            params={"market_id": 0, "limit": 100},
        )

    # --- _order_book_snapshot ---

    async def test_order_book_snapshot_message(self):
        self.connector._api_get = AsyncMock(return_value={
            "bids": [{"price": "2100.00", "remaining_base_amount": "1.5"}],
            "asks": [{"price": "2101.00", "remaining_base_amount": "2.0"}],
        })
        msg = await self.data_source._order_book_snapshot("ETH-USD")
        self.assertIsInstance(msg, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual("ETH-USD", msg.trading_pair)
        self.assertEqual(1, len(msg.bids))
        self.assertEqual(2100.0, msg.bids[0].price)
        self.assertEqual(1.5, msg.bids[0].amount)
        self.assertEqual(1, len(msg.asks))
        self.assertEqual(2101.0, msg.asks[0].price)
        self.assertEqual(2.0, msg.asks[0].amount)

    # --- _channel_originating_message ---

    def test_channel_routing_order_book_update(self):
        event = {"type": "update", "channel": "order_book:0"}
        result = self.data_source._channel_originating_message(event)
        self.assertEqual(self.data_source._diff_messages_queue_key, result)

    def test_channel_routing_order_book_subscribed(self):
        event = {"type": "subscribed/order_book", "channel": "order_book:0"}
        result = self.data_source._channel_originating_message(event)
        self.assertEqual(self.data_source._snapshot_messages_queue_key, result)

    def test_channel_routing_market_stats(self):
        event = {"type": "update", "channel": "market_stats:0"}
        result = self.data_source._channel_originating_message(event)
        self.assertEqual(self.data_source._funding_info_messages_queue_key, result)

    def test_channel_routing_ping(self):
        event = {"type": "ping", "channel": ""}
        result = self.data_source._channel_originating_message(event)
        self.assertEqual("", result)

    def test_channel_routing_unknown(self):
        event = {"type": "update", "channel": "unknown:0"}
        result = self.data_source._channel_originating_message(event)
        self.assertEqual("", result)

    # --- _parse_order_book_diff_message ---

    async def test_parse_order_book_diff_message(self):
        queue = asyncio.Queue()
        raw_message = {
            "type": "update",
            "channel": "order_book:0",
            "offset": 12345,
            "order_book": {
                "begin_nonce": None,
                "nonce": 100,
                "bids": [{"price": "2100.00", "size": "1.0"}],
                "asks": [{"price": "2101.00", "size": "2.0"}],
            },
        }
        await self.data_source._parse_order_book_diff_message(raw_message, queue)
        self.assertEqual(1, queue.qsize())
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual("ETH-USD", msg.trading_pair)
        self.assertEqual(12345, msg.update_id)

    async def test_parse_order_book_diff_skips_wrong_channel(self):
        queue = asyncio.Queue()
        raw_message = {"type": "update", "channel": "unknown:0", "order_book": {}}
        await self.data_source._parse_order_book_diff_message(raw_message, queue)
        self.assertEqual(0, queue.qsize())

    async def test_parse_order_book_diff_skips_no_market_id(self):
        queue = asyncio.Queue()
        raw_message = {"type": "update", "channel": "order_book", "order_book": {}}
        await self.data_source._parse_order_book_diff_message(raw_message, queue)
        self.assertEqual(0, queue.qsize())

    async def test_parse_order_book_diff_nonce_gap_triggers_resnapshot(self):
        queue = asyncio.Queue()
        # Set last nonce for market 0
        self.data_source._last_ob_nonce[0] = 50
        raw_message = {
            "type": "update",
            "channel": "order_book:0",
            "order_book": {
                "begin_nonce": 60,  # Gap: expected 50, got 60
                "nonce": 70,
                "bids": [],
                "asks": [],
            },
        }
        await self.data_source._parse_order_book_diff_message(raw_message, queue)
        # Should skip (re-snapshot needed) — nothing in queue
        self.assertEqual(0, queue.qsize())
        # last_ob_nonce should be cleared for this market
        self.assertNotIn(0, self.data_source._last_ob_nonce)

    async def test_parse_order_book_diff_nonce_continuity_ok(self):
        queue = asyncio.Queue()
        self.data_source._last_ob_nonce[0] = 50
        raw_message = {
            "type": "update",
            "channel": "order_book:0",
            "order_book": {
                "begin_nonce": 50,  # Matches last nonce
                "nonce": 60,
                "bids": [],
                "asks": [],
            },
        }
        await self.data_source._parse_order_book_diff_message(raw_message, queue)
        self.assertEqual(1, queue.qsize())
        self.assertEqual(60, self.data_source._last_ob_nonce[0])

    async def test_parse_order_book_diff_unknown_market_id(self):
        queue = asyncio.Queue()
        raw_message = {
            "type": "update",
            "channel": "order_book:999",
            "order_book": {"nonce": 1, "bids": [], "asks": []},
        }
        await self.data_source._parse_order_book_diff_message(raw_message, queue)
        self.assertEqual(0, queue.qsize())  # KeyError caught — skipped

    # --- _parse_order_book_snapshot_message ---

    async def test_parse_order_book_snapshot_message(self):
        queue = asyncio.Queue()
        raw_message = {
            "type": "subscribed",
            "channel": "order_book:0",
            "offset": 500,
            "order_book": {
                "nonce": 42,
                "bids": [{"price": "2050.00", "size": "3.0"}],
                "asks": [{"price": "2060.00", "size": "1.5"}],
            },
        }
        await self.data_source._parse_order_book_snapshot_message(raw_message, queue)
        self.assertEqual(1, queue.qsize())
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual("ETH-USD", msg.trading_pair)
        self.assertEqual(42, self.data_source._last_ob_nonce[0])

    async def test_parse_order_book_snapshot_skips_wrong_channel(self):
        queue = asyncio.Queue()
        raw_message = {"type": "subscribed", "channel": "unknown:0", "order_book": {}}
        await self.data_source._parse_order_book_snapshot_message(raw_message, queue)
        self.assertEqual(0, queue.qsize())

    # --- _parse_trade_message ---

    async def test_parse_trade_message_is_noop(self):
        queue = asyncio.Queue()
        await self.data_source._parse_trade_message({}, queue)
        self.assertEqual(0, queue.qsize())

    # --- _parse_funding_info_message ---

    async def test_parse_funding_info_message(self):
        queue = asyncio.Queue()
        raw_message = {
            "type": "update",
            "channel": "market_stats:0",
            "stats": {
                "index_price": "2100.50",
                "mark_price": "2101.00",
                "funding_rate": "0.0001",
            },
        }
        await self.data_source._parse_funding_info_message(raw_message, queue)
        self.assertEqual(1, queue.qsize())
        info = queue.get_nowait()
        self.assertIsInstance(info, FundingInfoUpdate)
        self.assertEqual("ETH-USD", info.trading_pair)
        self.assertEqual(Decimal("2100.50"), info.index_price)
        self.assertEqual(Decimal("2101.00"), info.mark_price)
        self.assertEqual(Decimal("0.0001"), info.rate)

    async def test_parse_funding_info_message_skips_wrong_channel(self):
        queue = asyncio.Queue()
        raw_message = {"type": "update", "channel": "unknown:0", "stats": {}}
        await self.data_source._parse_funding_info_message(raw_message, queue)
        self.assertEqual(0, queue.qsize())

    async def test_parse_funding_info_message_skips_untracked_pair(self):
        queue = asyncio.Queue()
        _pair_to_market_id["BTC-USD"] = 1
        _market_id_to_pair[1] = "BTC-USD"
        raw_message = {
            "type": "update",
            "channel": "market_stats:1",
            "stats": {"index_price": "50000", "mark_price": "50000", "funding_rate": "0"},
        }
        # BTC-USD is not in self.trading_pairs
        await self.data_source._parse_funding_info_message(raw_message, queue)
        self.assertEqual(0, queue.qsize())

    # --- _next_funding_time ---

    def test_next_funding_time(self):
        result = self.data_source._next_funding_time()
        now = time.time()
        # Should be within the next hour
        self.assertGreater(result, now)
        self.assertLessEqual(result, now + 3600)
        # Should be on the hour boundary
        self.assertEqual(0, result % 3600)

    # --- _subscribe_channels ---

    async def test_subscribe_channels(self):
        mock_ws = AsyncMock()
        await self.data_source._subscribe_channels(mock_ws)
        # Should send one subscribe message per trading pair for order_book
        self.assertEqual(1, mock_ws.send.call_count)
        payload = mock_ws.send.call_args[0][0].payload
        self.assertEqual("subscribe", payload["type"])
        self.assertIn("order_book/0", payload["channel"])

    async def test_subscribe_channels_multiple_pairs(self):
        _pair_to_market_id["BTC-USD"] = 1
        _market_id_to_pair[1] = "BTC-USD"
        ds = LighterPerpetualAPIOrderBookDataSource(
            trading_pairs=["ETH-USD", "BTC-USD"],
            connector=self.connector,
            api_factory=self.api_factory,
            domain=self.domain,
        )
        mock_ws = AsyncMock()
        await ds._subscribe_channels(mock_ws)
        self.assertEqual(2, mock_ws.send.call_count)

    # --- subscribe_to_trading_pair / unsubscribe_from_trading_pair ---

    async def test_subscribe_to_trading_pair_no_ws(self):
        self.data_source._ws_assistant = None
        result = await self.data_source.subscribe_to_trading_pair("ETH-USD")
        self.assertFalse(result)

    async def test_subscribe_to_trading_pair_success(self):
        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws
        result = await self.data_source.subscribe_to_trading_pair("ETH-USD")
        self.assertTrue(result)
        mock_ws.send.assert_called_once()
        payload = mock_ws.send.call_args[0][0].payload
        self.assertEqual("subscribe", payload["type"])

    async def test_unsubscribe_from_trading_pair_no_ws(self):
        self.data_source._ws_assistant = None
        result = await self.data_source.unsubscribe_from_trading_pair("ETH-USD")
        self.assertFalse(result)

    async def test_unsubscribe_from_trading_pair_success(self):
        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws
        result = await self.data_source.unsubscribe_from_trading_pair("ETH-USD")
        self.assertTrue(result)
        mock_ws.send.assert_called_once()
        payload = mock_ws.send.call_args[0][0].payload
        self.assertEqual("unsubscribe", payload["type"])

    async def test_subscribe_to_trading_pair_handles_error(self):
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = Exception("Connection lost")
        self.data_source._ws_assistant = mock_ws
        result = await self.data_source.subscribe_to_trading_pair("ETH-USD")
        self.assertFalse(result)
