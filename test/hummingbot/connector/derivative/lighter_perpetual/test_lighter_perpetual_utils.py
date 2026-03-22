import unittest
from decimal import Decimal

from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_utils import (
    MarketDecimals,
    _market_decimals,
    _market_id_to_pair,
    _pair_to_market_id,
    from_raw_price,
    from_raw_size,
    get_all_trading_pairs,
    get_market_decimals,
    initialize_market_map,
    is_market_initialized,
    market_id_to_pair,
    pair_to_market_id,
    to_raw_price,
    to_raw_size,
)


class TestLighterPerpetualUtils(unittest.TestCase):

    def setUp(self):
        _pair_to_market_id.clear()
        _market_id_to_pair.clear()
        _market_decimals.clear()

    def tearDown(self):
        _pair_to_market_id.clear()
        _market_id_to_pair.clear()
        _market_decimals.clear()

    def _sample_order_books_response(self):
        return {
            "code": 200,
            "order_books": [
                {
                    "symbol": "ETH",
                    "market_id": 0,
                    "supported_price_decimals": 2,
                    "supported_size_decimals": 4,
                    "supported_quote_decimals": 6,
                },
                {
                    "symbol": "BTC",
                    "market_id": 1,
                    "supported_price_decimals": 1,
                    "supported_size_decimals": 5,
                    "supported_quote_decimals": 6,
                },
            ],
        }

    # --- initialize_market_map ---

    def test_initialize_market_map_populates_all_maps(self):
        initialize_market_map(self._sample_order_books_response())
        self.assertEqual(0, _pair_to_market_id["ETH-USD"])
        self.assertEqual(1, _pair_to_market_id["BTC-USD"])
        self.assertEqual("ETH-USD", _market_id_to_pair[0])
        self.assertEqual("BTC-USD", _market_id_to_pair[1])
        self.assertEqual(2, _market_decimals[0].price_decimals)
        self.assertEqual(4, _market_decimals[0].size_decimals)
        self.assertEqual(6, _market_decimals[0].quote_decimals)
        self.assertEqual(1, _market_decimals[1].price_decimals)
        self.assertEqual(5, _market_decimals[1].size_decimals)

    def test_initialize_market_map_clears_previous_data(self):
        _pair_to_market_id["OLD-PAIR"] = 99
        initialize_market_map(self._sample_order_books_response())
        self.assertNotIn("OLD-PAIR", _pair_to_market_id)
        self.assertEqual(2, len(_pair_to_market_id))

    def test_initialize_market_map_empty_response(self):
        initialize_market_map({"order_books": []})
        self.assertEqual(0, len(_pair_to_market_id))

    def test_initialize_market_map_missing_key(self):
        initialize_market_map({})
        self.assertEqual(0, len(_pair_to_market_id))

    def test_initialize_market_map_default_quote_decimals(self):
        response = {
            "order_books": [{
                "symbol": "SOL",
                "market_id": 5,
                "supported_price_decimals": 3,
                "supported_size_decimals": 2,
                # No supported_quote_decimals — should default to 6
            }]
        }
        initialize_market_map(response)
        self.assertEqual(6, _market_decimals[5].quote_decimals)

    # --- pair_to_market_id / market_id_to_pair ---

    def test_pair_to_market_id(self):
        initialize_market_map(self._sample_order_books_response())
        self.assertEqual(0, pair_to_market_id("ETH-USD"))
        self.assertEqual(1, pair_to_market_id("BTC-USD"))

    def test_pair_to_market_id_raises_for_unknown(self):
        initialize_market_map(self._sample_order_books_response())
        with self.assertRaises(KeyError):
            pair_to_market_id("UNKNOWN-USD")

    def test_market_id_to_pair(self):
        initialize_market_map(self._sample_order_books_response())
        self.assertEqual("ETH-USD", market_id_to_pair(0))
        self.assertEqual("BTC-USD", market_id_to_pair(1))

    def test_market_id_to_pair_raises_for_unknown(self):
        initialize_market_map(self._sample_order_books_response())
        with self.assertRaises(KeyError):
            market_id_to_pair(999)

    # --- get_market_decimals ---

    def test_get_market_decimals(self):
        initialize_market_map(self._sample_order_books_response())
        dec = get_market_decimals(0)
        self.assertIsInstance(dec, MarketDecimals)
        self.assertEqual(2, dec.price_decimals)
        self.assertEqual(4, dec.size_decimals)
        self.assertEqual(6, dec.quote_decimals)

    def test_get_market_decimals_raises_for_unknown(self):
        with self.assertRaises(KeyError):
            get_market_decimals(999)

    # --- to_raw_price / from_raw_price ---

    def test_to_raw_price(self):
        initialize_market_map(self._sample_order_books_response())
        # ETH market: price_decimals=2, so 1234.56 → 123456
        self.assertEqual(123456, to_raw_price(0, Decimal("1234.56")))

    def test_from_raw_price(self):
        initialize_market_map(self._sample_order_books_response())
        self.assertEqual(Decimal("1234.56"), from_raw_price(0, 123456))

    def test_price_roundtrip(self):
        initialize_market_map(self._sample_order_books_response())
        original = Decimal("2105.12")
        raw = to_raw_price(0, original)
        result = from_raw_price(0, raw)
        self.assertEqual(original, result)

    # --- to_raw_size / from_raw_size ---

    def test_to_raw_size(self):
        initialize_market_map(self._sample_order_books_response())
        # ETH market: size_decimals=4, so 1.2345 → 12345
        self.assertEqual(12345, to_raw_size(0, Decimal("1.2345")))

    def test_from_raw_size(self):
        initialize_market_map(self._sample_order_books_response())
        self.assertEqual(Decimal("1.2345"), from_raw_size(0, 12345))

    def test_size_roundtrip(self):
        initialize_market_map(self._sample_order_books_response())
        original = Decimal("0.5000")
        raw = to_raw_size(0, original)
        result = from_raw_size(0, raw)
        self.assertEqual(original, result)

    # --- is_market_initialized ---

    def test_is_market_initialized_false_initially(self):
        self.assertFalse(is_market_initialized())

    def test_is_market_initialized_true_after_init(self):
        initialize_market_map(self._sample_order_books_response())
        self.assertTrue(is_market_initialized())

    # --- get_all_trading_pairs ---

    def test_get_all_trading_pairs_empty(self):
        self.assertEqual([], get_all_trading_pairs())

    def test_get_all_trading_pairs(self):
        initialize_market_map(self._sample_order_books_response())
        pairs = get_all_trading_pairs()
        self.assertIn("ETH-USD", pairs)
        self.assertIn("BTC-USD", pairs)
        self.assertEqual(2, len(pairs))

    # --- MarketDecimals dataclass ---

    def test_market_decimals_defaults(self):
        md = MarketDecimals(price_decimals=2, size_decimals=4)
        self.assertEqual(6, md.quote_decimals)

    def test_market_decimals_custom_quote(self):
        md = MarketDecimals(price_decimals=2, size_decimals=4, quote_decimals=8)
        self.assertEqual(8, md.quote_decimals)
