import lighter

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class LighterPerpetualAuth(AuthBase):
    """Wraps lighter.SignerClient for Hummingbot's AuthBase interface.

    The SignerClient handles:
    - Signing via native ctypes shared library (microseconds, non-blocking)
    - Nonce management via OptimisticNonceManager (auto-fetches from API on init)
    - Async sign+send via create_order(), cancel_order(), etc.
    """

    def __init__(self, api_key_index: int, api_private_key: str,
                 account_index: int, base_url: str):
        super().__init__()
        self._signer = lighter.SignerClient(
            url=base_url,
            account_index=account_index,
            api_private_keys={api_key_index: api_private_key},
        )
        self._api_key_index = api_key_index
        self._account_index = account_index

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        # Lighter REST auth is tx-level (signed payload via SignerClient), not header-level.
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # Auth tokens are added at subscribe-message level (account_all_orders, user_stats).
        return request

    def get_auth_token(self) -> str:
        """Generate short-lived auth token for authenticated REST/WS endpoints."""
        auth, error = self._signer.create_auth_token_with_expiry(
            deadline=10 * 60,  # 10 minutes
            api_key_index=self._api_key_index,
        )
        if error:
            raise ValueError(f"Auth token generation failed: {error}")
        return auth

    @property
    def signer(self) -> lighter.SignerClient:
        """Expose SignerClient for direct use by the derivative connector.
        The connector calls signer.create_order(), signer.cancel_order(), etc."""
        return self._signer

    @property
    def account_index(self) -> int:
        return self._account_index

    @property
    def api_key_index(self) -> int:
        return self._api_key_index
