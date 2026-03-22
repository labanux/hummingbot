import unittest
from unittest.mock import MagicMock, AsyncMock, patch

import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils as web_utils
from hummingbot.core.web_assistant.connections.data_types import RESTRequest


class TestLighterPerpetualWebUtils(unittest.TestCase):

    # --- rest_url ---

    def test_rest_url_mainnet(self):
        url = web_utils.rest_url("/api/v1/orderBooks", domain=CONSTANTS.DOMAIN)
        self.assertEqual(f"{CONSTANTS.PERPETUAL_BASE_URL}/api/v1/orderBooks", url)

    def test_rest_url_testnet(self):
        url = web_utils.rest_url("/api/v1/orderBooks", domain=CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(f"{CONSTANTS.TESTNET_BASE_URL}/api/v1/orderBooks", url)

    def test_rest_url_default_domain(self):
        url = web_utils.rest_url("/api/v1/account")
        self.assertEqual(f"{CONSTANTS.PERPETUAL_BASE_URL}/api/v1/account", url)

    # --- public_rest_url / private_rest_url ---

    def test_public_rest_url_delegates_to_rest_url(self):
        url = web_utils.public_rest_url("/api/v1/orderBooks", domain=CONSTANTS.TESTNET_DOMAIN)
        expected = web_utils.rest_url("/api/v1/orderBooks", domain=CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(expected, url)

    def test_private_rest_url_delegates_to_rest_url(self):
        url = web_utils.private_rest_url("/api/v1/account", domain=CONSTANTS.DOMAIN)
        expected = web_utils.rest_url("/api/v1/account", domain=CONSTANTS.DOMAIN)
        self.assertEqual(expected, url)

    # --- wss_url ---

    def test_wss_url_mainnet(self):
        url = web_utils.wss_url(domain=CONSTANTS.DOMAIN)
        self.assertEqual(CONSTANTS.PERPETUAL_WS_URL, url)

    def test_wss_url_testnet(self):
        url = web_utils.wss_url(domain=CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(CONSTANTS.TESTNET_WS_URL, url)

    def test_wss_url_default_domain(self):
        url = web_utils.wss_url()
        self.assertEqual(CONSTANTS.PERPETUAL_WS_URL, url)

    # --- build_api_factory ---

    def test_build_api_factory_returns_factory(self):
        factory = web_utils.build_api_factory()
        self.assertIsNotNone(factory)

    def test_build_api_factory_with_auth(self):
        mock_auth = MagicMock()
        factory = web_utils.build_api_factory(auth=mock_auth)
        self.assertIsNotNone(factory)

    def test_build_api_factory_with_throttler(self):
        throttler = web_utils.create_throttler()
        factory = web_utils.build_api_factory(throttler=throttler)
        self.assertIsNotNone(factory)

    # --- build_api_factory_without_time_synchronizer_pre_processor ---

    def test_build_api_factory_without_time_sync(self):
        throttler = web_utils.create_throttler()
        factory = web_utils.build_api_factory_without_time_synchronizer_pre_processor(throttler)
        self.assertIsNotNone(factory)

    # --- create_throttler ---

    def test_create_throttler(self):
        throttler = web_utils.create_throttler()
        self.assertIsNotNone(throttler)

    # --- get_current_server_time ---

    def test_get_current_server_time(self):
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            web_utils.get_current_server_time(None, None)
        )
        self.assertIsInstance(result, float)
        self.assertGreater(result, 0)

    # --- is_exchange_information_valid ---

    def test_is_exchange_information_valid_always_true(self):
        self.assertTrue(web_utils.is_exchange_information_valid({}))
        self.assertTrue(web_utils.is_exchange_information_valid({"any": "data"}))


class TestLighterPerpetualRESTPreProcessor(unittest.IsolatedAsyncioTestCase):

    async def test_pre_process_adds_content_type_header(self):
        preprocessor = web_utils.LighterPerpetualRESTPreProcessor()
        request = RESTRequest(method="GET", url="https://example.com")
        result = await preprocessor.pre_process(request)
        self.assertEqual("application/json", result.headers["Content-Type"])

    async def test_pre_process_preserves_existing_headers(self):
        preprocessor = web_utils.LighterPerpetualRESTPreProcessor()
        request = RESTRequest(method="GET", url="https://example.com", headers={"X-Custom": "val"})
        result = await preprocessor.pre_process(request)
        self.assertEqual("application/json", result.headers["Content-Type"])
        self.assertEqual("val", result.headers["X-Custom"])

    async def test_pre_process_creates_headers_if_none(self):
        preprocessor = web_utils.LighterPerpetualRESTPreProcessor()
        request = RESTRequest(method="GET", url="https://example.com")
        request.headers = None
        result = await preprocessor.pre_process(request)
        self.assertIsNotNone(result.headers)
        self.assertEqual("application/json", result.headers["Content-Type"])
