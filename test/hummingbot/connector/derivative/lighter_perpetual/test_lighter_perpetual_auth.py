import unittest
from unittest.mock import MagicMock, patch

from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_auth import LighterPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSJSONRequest


class TestLighterPerpetualAuth(unittest.IsolatedAsyncioTestCase):

    @patch("hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_auth.lighter")
    def setUp(self, mock_lighter):
        self.mock_signer = MagicMock()
        mock_lighter.SignerClient.return_value = self.mock_signer

        self.api_key_index = 5
        self.api_private_key = "0x" + "ab" * 20  # noqa: mock
        self.account_index = 100
        self.base_url = "https://testnet.zklighter.elliot.ai"

        self.auth = LighterPerpetualAuth(
            api_key_index=self.api_key_index,
            api_private_key=self.api_private_key,
            account_index=self.account_index,
            base_url=self.base_url,
        )

        mock_lighter.SignerClient.assert_called_once_with(
            url=self.base_url,
            account_index=self.account_index,
            api_private_keys={self.api_key_index: self.api_private_key},
        )

    async def test_rest_authenticate_returns_request_unchanged(self):
        request = RESTRequest(method="GET", url="https://example.com/api")
        result = await self.auth.rest_authenticate(request)
        self.assertIs(result, request)

    async def test_ws_authenticate_returns_request_unchanged(self):
        request = WSJSONRequest(payload={"type": "subscribe"})
        result = await self.auth.ws_authenticate(request)
        self.assertIs(result, request)

    def test_get_auth_token_success(self):
        self.mock_signer.create_auth_token_with_expiry.return_value = ("mock_token_abc", None)
        token = self.auth.get_auth_token()
        self.assertEqual("mock_token_abc", token)
        self.mock_signer.create_auth_token_with_expiry.assert_called_once_with(
            deadline=600,
            api_key_index=self.api_key_index,
        )

    def test_get_auth_token_raises_on_error(self):
        self.mock_signer.create_auth_token_with_expiry.return_value = (None, "signing failed")
        with self.assertRaises(ValueError) as ctx:
            self.auth.get_auth_token()
        self.assertIn("signing failed", str(ctx.exception))

    def test_signer_property(self):
        self.assertIs(self.auth.signer, self.mock_signer)

    def test_account_index_property(self):
        self.assertEqual(self.account_index, self.auth.account_index)

    def test_api_key_index_property(self):
        self.assertEqual(self.api_key_index, self.auth.api_key_index)
