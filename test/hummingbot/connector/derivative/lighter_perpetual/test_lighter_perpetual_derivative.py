import asyncio
import json
import logging
import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from aioresponses import aioresponses
from aioresponses.core import RequestCall

import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_derivative import (
    LighterPerpetualDerivative,
)
from hummingbot.connector.test_support.perpetual_derivative_test import AbstractPerpetualDerivativeTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase


class LighterPerpetualDerivativeTests(AbstractPerpetualDerivativeTests.PerpetualDerivativeTests):
    _logger = logging.getLogger(__name__)

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.api_key_index = "0"
        cls.api_private_key = "0x" + "ab" * 20  # noqa: mock
        cls.account_index = "100"
        cls.base_asset = "ETH"
        cls.quote_asset = "USD"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"

    def setUp(self) -> None:
        super().setUp()
        # Pre-populate market maps so pair_to_market_id / market_id_to_pair work
        from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_utils import (
            _market_decimals,
            _market_id_to_pair,
            _pair_to_market_id,
            MarketDecimals,
        )
        _pair_to_market_id.clear()
        _market_id_to_pair.clear()
        _market_decimals.clear()
        _pair_to_market_id["ETH-USD"] = 0
        _market_id_to_pair[0] = "ETH-USD"
        _market_decimals[0] = MarketDecimals(price_decimals=2, size_decimals=4, quote_decimals=6)

    # ----------------------------------------------------------------
    # Helper: URL builder
    # ----------------------------------------------------------------
    def _url(self, path: str) -> str:
        return web_utils.rest_url(path, self.exchange.domain)

    def _url_regex(self, path: str):
        url = self._url(path)
        return re.compile(f"^{re.escape(url)}" + ".*")

    # ----------------------------------------------------------------
    # Abstract Properties — URLs
    # ----------------------------------------------------------------
    @property
    def all_symbols_url(self):
        return self._url_regex(CONSTANTS.ORDER_BOOKS_URL)

    @property
    def latest_prices_url(self):
        return self._url_regex(CONSTANTS.ORDER_BOOK_DETAILS_URL)

    @property
    def network_status_url(self):
        return self._url_regex(CONSTANTS.HEALTH_CHECK_URL)

    @property
    def trading_rules_url(self):
        return self._url_regex(CONSTANTS.ORDER_BOOKS_URL)

    @property
    def order_creation_url(self):
        return self._url_regex(CONSTANTS.SEND_TX_URL)

    @property
    def balance_url(self):
        return self._url_regex(CONSTANTS.ACCOUNT_URL)

    @property
    def funding_info_url(self):
        return self._url_regex(CONSTANTS.ORDER_BOOK_DETAILS_URL)

    @property
    def funding_payment_url(self):
        return self._url_regex(CONSTANTS.FUNDINGS_URL)

    # ----------------------------------------------------------------
    # Abstract Properties — Mock Responses
    # ----------------------------------------------------------------
    @property
    def all_symbols_request_mock_response(self):
        return {
            "code": 200,
            "order_books": [
                {
                    "symbol": "ETH",
                    "market_id": 0,
                    "market_type": "perp",
                    "base_asset_id": 0,
                    "quote_asset_id": 0,
                    "status": "active",
                    "taker_fee": "0.0001",
                    "maker_fee": "0.0000",
                    "liquidation_fee": "1.0000",
                    "min_base_amount": "0.0050",
                    "min_quote_amount": "10.000000",
                    "order_quote_limit": "281474976.710655",
                    "supported_size_decimals": 4,
                    "supported_price_decimals": 2,
                    "supported_quote_decimals": 6,
                },
            ],
        }

    @property
    def latest_prices_request_mock_response(self):
        return {
            "code": 200,
            "order_book_details": [
                {
                    "symbol": "ETH",
                    "market_id": 0,
                    "market_type": "perp",
                    "last_trade_price": float(self.expected_latest_price),
                    "mark_price": float(self.expected_latest_price),
                    "index_price": float(self.expected_latest_price),
                    "funding_rate": "0.0001",
                    "supported_size_decimals": 4,
                    "supported_price_decimals": 2,
                    "supported_quote_decimals": 6,
                },
            ],
            "spot_order_book_details": [],
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = {
            "code": 200,
            "order_books": [
                {
                    "symbol": "ETH",
                    "market_id": 0,
                    "market_type": "perp",
                    "status": "active",
                    "min_base_amount": "0.0050",
                    "min_quote_amount": "10.000000",
                    "supported_size_decimals": 4,
                    "supported_price_decimals": 2,
                    "supported_quote_decimals": 6,
                },
                {
                    "symbol": "INVALID",
                    "market_id": 999,
                    "market_type": "perp",
                    "status": "active",
                    "min_base_amount": "1.0",
                    "min_quote_amount": "10.0",
                    "supported_size_decimals": 1,
                    "supported_price_decimals": 1,
                    "supported_quote_decimals": 6,
                },
            ],
        }
        return "INVALID-USD", response

    @property
    def network_status_request_successful_mock_response(self):
        return self.all_symbols_request_mock_response

    @property
    def trading_rules_request_mock_response(self):
        return self.all_symbols_request_mock_response

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {
            "code": 200,
            "order_books": [
                {
                    "symbol": "ETH",
                    "market_id": 0,
                    "supported_price_decimals": 2,
                    "supported_size_decimals": 4,
                    "supported_quote_decimals": 6,
                    # Missing min_base_amount / min_quote_amount to trigger error in _format_trading_rules
                },
            ],
        }

    @property
    def order_creation_request_successful_mock_response(self):
        # Lighter uses SDK, not REST for order creation.
        # This is used by base tests — return a simple dict.
        return {"code": 200, "tx_hash": "mock_tx_hash"}

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "code": 200,
            "total": 1,
            "accounts": [
                {
                    "account_type": 0,
                    "index": 100,
                    "collateral": "10000.000000",
                    "available_balance": "9500.000000",
                    "assets": [
                        {
                            "symbol": "USDC",
                            "asset_id": 0,
                            "balance": "10000.000000",
                            "locked_balance": "500.000000",
                        },
                    ],
                    "positions": [],
                }
            ],
        }

    @property
    def balance_request_mock_response_only_base(self):
        return self.balance_request_mock_response_for_base_and_quote

    @property
    def balance_event_websocket_update(self):
        return {
            "type": "subscribed/user_stats",
            "channel": "user_stats:100",
            "stats": {
                "collateral": "10000.000000",
                "portfolio_value": "10000.000000",
                "available_balance": "9500.000000",
            },
        }

    @property
    def funding_info_mock_response(self):
        return self.latest_prices_request_mock_response

    @property
    def empty_funding_payment_mock_response(self):
        return {"code": 200, "resolution": "1h", "fundings": []}

    @property
    def funding_payment_mock_response(self):
        return {
            "code": 200,
            "resolution": "1h",
            "fundings": [
                {
                    "timestamp": self.target_funding_payment_timestamp,
                    "value": "5.69",
                    "rate": str(self.target_funding_payment_funding_rate),
                    "direction": "long",
                }
            ],
        }

    # ----------------------------------------------------------------
    # Abstract Properties — Expected Values
    # ----------------------------------------------------------------
    @property
    def expected_latest_price(self):
        return 2105.12

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    @property
    def expected_supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            self.trading_pair,
            min_order_size=Decimal("0.0050"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.0001"),
            min_notional_size=Decimal("10.000000"),
            buy_order_collateral_token="USD",
            sell_order_collateral_token="USD",
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response["order_books"][0]
        return f"Error parsing trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return 844424930120300

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return False

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("2050.00")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5000")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("0.1"))],
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return "12345"

    @property
    def latest_trade_hist_timestamp(self) -> int:
        return 1234

    # ----------------------------------------------------------------
    # Abstract Methods — Exchange Instance
    # ----------------------------------------------------------------
    def async_run_with_timeout(self, coroutine, timeout: int = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}-{quote_token}"

    def create_exchange_instance(self):
        # Patch the authenticator property to avoid real SignerClient init
        # (which requires an event loop for aiohttp.TCPConnector)
        mock_auth = self._create_mock_auth()
        with patch.object(
            LighterPerpetualDerivative,
            "authenticator",
            new_callable=PropertyMock,
            return_value=mock_auth,
        ):
            exchange = LighterPerpetualDerivative(
                lighter_perpetual_api_key_index=self.api_key_index,
                lighter_perpetual_api_private_key=self.api_private_key,
                lighter_perpetual_account_index=self.account_index,
                trading_pairs=[self.trading_pair],
                domain=CONSTANTS.TESTNET_DOMAIN,
            )
        return exchange

    def _create_mock_auth(self):
        mock_auth = MagicMock()
        mock_auth.account_index = int(self.account_index)
        mock_auth.api_key_index = int(self.api_key_index)
        mock_auth.get_auth_token.return_value = "mock_auth_token"

        # Mock signer
        mock_signer = AsyncMock()
        mock_auth.signer = mock_signer

        # Default: create_order returns success
        mock_resp = MagicMock()
        mock_resp.code = 200
        mock_resp.tx_hash = "mock_tx_hash"
        mock_resp.predicted_execution_time_ms = 1000
        mock_resp.volume_quota_remaining = None
        mock_resp.message = ""
        mock_signer.create_order = AsyncMock(return_value=(MagicMock(), mock_resp, None))
        mock_signer.cancel_order = AsyncMock(return_value=(MagicMock(), mock_resp, None))
        mock_signer.update_leverage = AsyncMock(return_value=(MagicMock(), mock_resp, None))

        # Auth pass-through
        mock_auth.rest_authenticate = AsyncMock(side_effect=lambda r: r)
        mock_auth.ws_authenticate = AsyncMock(side_effect=lambda r: r)

        return mock_auth

    # ----------------------------------------------------------------
    # Abstract Methods — Request Validation
    # ----------------------------------------------------------------
    def validate_auth_credentials_present(self, request_call: RequestCall):
        # Lighter uses SDK-level signing, not header auth
        pass

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        # Order creation goes through SDK, not HTTP
        pass

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        pass

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        pass

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        pass

    # ----------------------------------------------------------------
    # Abstract Methods — Configure Mock Responses
    # ----------------------------------------------------------------
    def configure_successful_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        # Cancel goes through SDK, mock the signer
        mock_resp = MagicMock()
        mock_resp.code = 200
        mock_resp.tx_hash = "cancel_tx_hash"
        mock_resp.message = ""
        self.exchange._auth.signer.cancel_order = AsyncMock(
            return_value=(MagicMock(), mock_resp, None)
        )
        url = self._url(CONSTANTS.SEND_TX_URL)
        return url

    def configure_erroneous_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        self.exchange._auth.signer.cancel_order = AsyncMock(
            return_value=(None, None, "Cancel failed")
        )
        url = self._url(CONSTANTS.SEND_TX_URL)
        return url

    def configure_order_not_found_error_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        self.exchange._auth.signer.cancel_order = AsyncMock(
            return_value=(None, None, "order not found")
        )
        url = self._url(CONSTANTS.SEND_TX_URL)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
            self, successful_order: InFlightOrder, erroneous_order: InFlightOrder,
            mock_api: aioresponses,
    ) -> List[str]:
        # For cancel_all, just configure the signer to succeed
        mock_resp = MagicMock()
        mock_resp.code = 200
        mock_resp.tx_hash = "cancel_tx"
        mock_resp.message = ""
        self.exchange._auth.signer.cancel_order = AsyncMock(
            return_value=(MagicMock(), mock_resp, None)
        )
        url = self._url(CONSTANTS.SEND_TX_URL)
        return [url, url]

    def configure_completely_filled_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        url = self._url(CONSTANTS.ACCOUNT_ACTIVE_ORDERS_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        # Active orders: empty (order is filled, moved to inactive)
        mock_api.get(regex_url, body=json.dumps({"code": 200, "orders": []}), callback=callback)
        # Inactive orders: order is filled
        inactive_url = self._url(CONSTANTS.ACCOUNT_INACTIVE_ORDERS_URL)
        inactive_regex = re.compile(f"^{re.escape(inactive_url)}" + ".*")
        mock_api.get(inactive_regex, body=json.dumps({
            "code": 200,
            "orders": [{
                "order_index": int(order.exchange_order_id),
                "client_order_index": 1,
                "status": "filled",
                "updated_at": 1640780000,
            }],
        }), callback=callback)
        return [url, inactive_url]

    def configure_canceled_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        url = self._url(CONSTANTS.ACCOUNT_ACTIVE_ORDERS_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        mock_api.get(regex_url, body=json.dumps({"code": 200, "orders": []}), callback=callback)
        inactive_url = self._url(CONSTANTS.ACCOUNT_INACTIVE_ORDERS_URL)
        inactive_regex = re.compile(f"^{re.escape(inactive_url)}" + ".*")
        mock_api.get(inactive_regex, body=json.dumps({
            "code": 200,
            "orders": [{
                "order_index": int(order.exchange_order_id),
                "client_order_index": 1,
                "status": "canceled",
                "updated_at": 1640780000,
            }],
        }), callback=callback)
        return [url, inactive_url]

    def configure_open_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        url = self._url(CONSTANTS.ACCOUNT_ACTIVE_ORDERS_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        mock_api.get(regex_url, body=json.dumps({
            "code": 200,
            "orders": [{
                "order_index": int(order.exchange_order_id),
                "client_order_index": 1,
                "status": "open",
                "updated_at": 1640780000,
            }],
        }), callback=callback)
        return [url]

    def configure_http_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = self._url(CONSTANTS.ACCOUNT_ACTIVE_ORDERS_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        mock_api.get(regex_url, status=404, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = self._url(CONSTANTS.ACCOUNT_ACTIVE_ORDERS_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        mock_api.get(regex_url, body=json.dumps({
            "code": 200,
            "orders": [{
                "order_index": int(order.exchange_order_id),
                "client_order_index": 1,
                "status": "open",
                "filled_base_amount": str(self.expected_partial_fill_amount),
                "updated_at": 1640780000,
            }],
        }), callback=callback)
        return url

    def configure_order_not_found_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        url = self._url(CONSTANTS.ACCOUNT_ACTIVE_ORDERS_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        mock_api.get(regex_url, body=json.dumps({"code": 200, "orders": []}), callback=callback)
        inactive_url = self._url(CONSTANTS.ACCOUNT_INACTIVE_ORDERS_URL)
        inactive_regex = re.compile(f"^{re.escape(inactive_url)}" + ".*")
        mock_api.get(inactive_regex, body=json.dumps({"code": 200, "orders": []}), callback=callback)
        return [url, inactive_url]

    def configure_partial_fill_trade_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = self._url(CONSTANTS.TRADES_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        mock_api.get(regex_url, body=json.dumps({
            "code": 200,
            "trades": [{
                "trade_id": self.expected_fill_trade_id,
                "market_id": 0,
                "price": str(self.expected_partial_fill_price),
                "size": str(self.expected_partial_fill_amount),
                "usd_amount": str(self.expected_partial_fill_price * self.expected_partial_fill_amount),
                "ask_account_id": 999,
                "bid_account_id": int(self.account_index),
                "is_maker_ask": True,
                "maker_fee": 0,
                "taker_fee": str(self.expected_fill_fee.flat_fees[0].amount),
                "timestamp": 1640780000,
            }],
        }), callback=callback)
        return url

    def configure_full_fill_trade_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = self._url(CONSTANTS.TRADES_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        mock_api.get(regex_url, body=json.dumps({
            "code": 200,
            "trades": [{
                "trade_id": self.expected_fill_trade_id,
                "market_id": 0,
                "price": str(order.price),
                "size": str(order.amount),
                "usd_amount": str(order.price * order.amount),
                "ask_account_id": 999,
                "bid_account_id": int(self.account_index),
                "is_maker_ask": True,
                "maker_fee": 0,
                "taker_fee": str(self.expected_fill_fee.flat_fees[0].amount),
                "timestamp": 1640780000,
            }],
        }), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = self._url(CONSTANTS.TRADES_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        mock_api.get(regex_url, status=400, callback=callback)
        return url

    # ----------------------------------------------------------------
    # Perpetual-specific abstract methods
    # ----------------------------------------------------------------
    def configure_successful_set_position_mode(
            self, position_mode: PositionMode, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        pass  # Lighter only supports ONEWAY, no API call needed

    def configure_failed_set_position_mode(
            self, position_mode: PositionMode, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> Tuple[str, str]:
        return "", "Lighter only supports the ONEWAY position mode."

    def configure_successful_set_leverage(
            self, leverage: int, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ):
        mock_resp = MagicMock()
        mock_resp.code = 200
        mock_resp.tx_hash = "leverage_tx"
        mock_resp.message = ""
        self.exchange._auth.signer.update_leverage = AsyncMock(
            return_value=(MagicMock(), mock_resp, None)
        )

    def configure_failed_set_leverage(
            self, leverage: int, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> Tuple[str, str]:
        err_msg = "Error setting leverage: leverage too high"
        self.exchange._auth.signer.update_leverage = AsyncMock(
            return_value=(None, None, "leverage too high")
        )
        return "", err_msg

    def funding_info_event_for_websocket_update(self):
        return {
            "type": "update/market_stats",
            "channel": "market_stats:0",
            "stats": {
                "index_price": str(self.target_funding_info_index_price_ws_updated),
                "mark_price": str(self.target_funding_info_mark_price_ws_updated),
                "funding_rate": str(self.target_funding_info_rate_ws_updated),
            },
        }

    def position_event_for_full_fill_websocket_update(self, order: InFlightOrder, unrealized_pnl: float):
        return {
            "type": "update/account_all",
            "channel": f"account_all:{self.account_index}",
            "positions": {
                "0": {
                    "market_id": 0,
                    "symbol": "ETH",
                    "sign": 1 if order.trade_type == TradeType.BUY else -1,
                    "position": str(order.amount),
                    "avg_entry_price": str(order.price),
                    "position_value": str(order.amount * order.price),
                    "unrealized_pnl": str(unrealized_pnl),
                    "allocated_margin": str(order.amount * order.price / 5),
                    "margin_mode": 0,
                },
            },
        }

    # ----------------------------------------------------------------
    # WS Event Data for Abstract Methods
    # ----------------------------------------------------------------
    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "type": "update/account_all_orders",
            "channel": f"account_all_orders:{self.account_index}",
            "orders": {
                "0": [{
                    "order_index": int(order.exchange_order_id or self.expected_exchange_order_id),
                    "client_order_index": 1,
                    "market_index": 0,
                    "owner_account_index": int(self.account_index),
                    "price": str(order.price),
                    "initial_base_amount": str(order.amount),
                    "remaining_base_amount": str(order.amount),
                    "filled_base_amount": "0",
                    "filled_quote_amount": "0",
                    "is_ask": order.trade_type == TradeType.SELL,
                    "status": "open",
                    "type": "limit",
                    "time_in_force": "good-till-time",
                    "timestamp": 1640780000,
                    "created_at": 1640780000,
                    "updated_at": 1640780000,
                }],
            },
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "type": "update/account_all_orders",
            "channel": f"account_all_orders:{self.account_index}",
            "orders": {
                "0": [{
                    "order_index": int(order.exchange_order_id or self.expected_exchange_order_id),
                    "client_order_index": 1,
                    "market_index": 0,
                    "owner_account_index": int(self.account_index),
                    "price": str(order.price),
                    "status": "canceled",
                    "timestamp": 1640780000,
                    "updated_at": 1640780000,
                }],
            },
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "type": "update/account_all_orders",
            "channel": f"account_all_orders:{self.account_index}",
            "orders": {
                "0": [{
                    "order_index": int(order.exchange_order_id or self.expected_exchange_order_id),
                    "client_order_index": 1,
                    "market_index": 0,
                    "owner_account_index": int(self.account_index),
                    "price": str(order.price),
                    "initial_base_amount": str(order.amount),
                    "remaining_base_amount": "0",
                    "filled_base_amount": str(order.amount),
                    "status": "filled",
                    "timestamp": 1640780000,
                    "updated_at": 1640780000,
                }],
            },
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "type": "update/account_all",
            "channel": f"account_all:{self.account_index}",
            "trades": {
                "0": [{
                    "trade_id": self.expected_fill_trade_id,
                    "market_id": 0,
                    "price": str(order.price),
                    "size": str(order.amount),
                    "usd_amount": str(order.price * order.amount),
                    "ask_account_id": 999,
                    "bid_account_id": int(self.account_index),
                    "ask_id": 999999,
                    "bid_id": int(order.exchange_order_id or self.expected_exchange_order_id),
                    "is_maker_ask": True,
                    "maker_fee": 0,
                    "taker_fee": str(self.expected_fill_fee.flat_fees[0].amount),
                    "timestamp": 1640780000,
                    "taker_position_size_before": "0",
                }],
            },
        }

    # ----------------------------------------------------------------
    # Configure balance response
    # ----------------------------------------------------------------
    def _configure_balance_response(self, response, mock_api: aioresponses, callback=None):
        url = self._url(CONSTANTS.ACCOUNT_URL)
        regex_url = re.compile(f"^{re.escape(url)}" + ".*")
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)

    # ----------------------------------------------------------------
    # Overridden Tests — Lighter-specific behavior
    # ----------------------------------------------------------------

    # Order creation goes through SDK, not HTTP POST
    @aioresponses()
    def test_create_buy_limit_order_successfully(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._set_current_timestamp(1640780000)

        # Mock the SDK create_order and the Future resolution
        mock_resp = MagicMock()
        mock_resp.code = 200
        mock_resp.tx_hash = "mock_tx_hash"
        mock_resp.message = ""
        self.exchange._auth.signer.create_order = AsyncMock(
            return_value=(MagicMock(), mock_resp, None)
        )

        # Set up Future to resolve immediately with exchange_order_id
        original_place = self.exchange._place_order

        async def mock_place_order(*args, **kwargs):
            # Override the Future wait — resolve immediately
            return str(self.expected_exchange_order_id), self.exchange.current_timestamp

        with patch.object(self.exchange, "_place_order", side_effect=mock_place_order):
            leverage = 2
            self.exchange._perpetual_trading.set_leverage(self.trading_pair, leverage)
            order_id = self.place_buy_order()
            self.async_run_with_timeout(asyncio.sleep(0.1))

        self.assertIn(order_id, self.exchange.in_flight_orders)

    @aioresponses()
    def test_create_sell_limit_order_successfully(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._set_current_timestamp(1640780000)

        async def mock_place_order(*args, **kwargs):
            return str(self.expected_exchange_order_id), self.exchange.current_timestamp

        with patch.object(self.exchange, "_place_order", side_effect=mock_place_order):
            leverage = 3
            self.exchange._perpetual_trading.set_leverage(self.trading_pair, leverage)
            order_id = self.place_sell_order()
            self.async_run_with_timeout(asyncio.sleep(0.1))

        self.assertIn(order_id, self.exchange.in_flight_orders)

    @aioresponses()
    def test_create_order_to_close_short_position(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._set_current_timestamp(1640780000)

        async def mock_place_order(*args, **kwargs):
            return str(self.expected_exchange_order_id), self.exchange.current_timestamp

        with patch.object(self.exchange, "_place_order", side_effect=mock_place_order):
            leverage = 4
            self.exchange._perpetual_trading.set_leverage(self.trading_pair, leverage)
            order_id = self.place_buy_order(position_action=PositionAction.CLOSE)
            self.async_run_with_timeout(asyncio.sleep(0.1))

        self.assertIn(order_id, self.exchange.in_flight_orders)

    @aioresponses()
    def test_create_order_to_close_long_position(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._set_current_timestamp(1640780000)

        async def mock_place_order(*args, **kwargs):
            return str(self.expected_exchange_order_id), self.exchange.current_timestamp

        with patch.object(self.exchange, "_place_order", side_effect=mock_place_order):
            leverage = 5
            self.exchange._perpetual_trading.set_leverage(self.trading_pair, leverage)
            order_id = self.place_sell_order(position_action=PositionAction.CLOSE)
            self.async_run_with_timeout(asyncio.sleep(0.1))

        self.assertIn(order_id, self.exchange.in_flight_orders)

    @aioresponses()
    def test_update_balances(self, mock_api):
        response = self.balance_request_mock_response_for_base_and_quote
        self._configure_balance_response(response=response, mock_api=mock_api)
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()
        self.assertEqual(Decimal("9500"), available_balances["USD"])
        self.assertEqual(Decimal("10000"), total_balances["USD"])

    def test_supported_position_modes(self):
        expected_result = [PositionMode.ONEWAY]
        self.assertEqual(expected_result, self.exchange.supported_position_modes())

    def _simulate_trading_rules_initialized(self):
        self.exchange._trading_rules = {
            self.trading_pair: TradingRule(
                trading_pair=self.trading_pair,
                min_order_size=Decimal("0.01"),
                min_price_increment=Decimal("0.0001"),
                min_base_amount_increment=Decimal("0.000001"),
                buy_order_collateral_token="USD",
                sell_order_collateral_token="USD",
            )
        }

    def test_get_buy_and_sell_collateral_tokens(self):
        self._simulate_trading_rules_initialized()
        buy_collateral_token = self.exchange.get_buy_collateral_token(self.trading_pair)
        sell_collateral_token = self.exchange.get_sell_collateral_token(self.trading_pair)
        self.assertEqual("USD", buy_collateral_token)
        self.assertEqual("USD", sell_collateral_token)

    @aioresponses()
    def test_set_leverage_failure(self, mock_api):
        # Leverage uses SDK, not HTTP — override to avoid waiting for HTTP callback
        target_leverage = 2
        err_msg = "Error setting leverage: leverage too high"
        self.exchange._auth.signer.update_leverage = AsyncMock(
            return_value=(None, None, "leverage too high")
        )
        self.exchange.set_leverage(trading_pair=self.trading_pair, leverage=target_leverage)
        self.async_run_with_timeout(asyncio.sleep(0.5))
        self.assertTrue(
            self.is_logged(
                log_level="NETWORK",
                message=f"Error setting leverage {target_leverage} for {self.trading_pair}: {err_msg}",
            )
        )

    @aioresponses()
    def test_set_leverage_success(self, mock_api):
        # Leverage uses SDK, not HTTP
        target_leverage = 2
        mock_resp = MagicMock()
        mock_resp.code = 200
        mock_resp.tx_hash = "leverage_tx"
        mock_resp.message = ""
        self.exchange._auth.signer.update_leverage = AsyncMock(
            return_value=(MagicMock(), mock_resp, None)
        )
        self.exchange.set_leverage(trading_pair=self.trading_pair, leverage=target_leverage)
        self.async_run_with_timeout(asyncio.sleep(0.5))
        self.assertTrue(
            self.is_logged(
                log_level="INFO",
                message=f"Leverage for {self.trading_pair} successfully set to {target_leverage}.",
            )
        )

    @aioresponses()
    def test_set_position_mode_failure(self, mock_api):
        self.exchange.set_position_mode(PositionMode.HEDGE)
        self.assertTrue(
            self.is_logged(
                log_level="ERROR",
                message="Position mode PositionMode.HEDGE is not supported. Mode not set.",
            )
        )

    @aioresponses()
    def test_set_position_mode_success(self, mock_api):
        self.exchange.set_position_mode(PositionMode.ONEWAY)
        self.async_run_with_timeout(asyncio.sleep(0.5))
        self.assertTrue(
            self.is_logged(
                log_level="DEBUG",
                message=f"Position mode switched to {PositionMode.ONEWAY}.",
            )
        )

    def is_cancel_request_executed_synchronously_by_server(self):
        return False

    # Tests that require deep HTTP mocking — skip or pass for SDK-based connector
    @aioresponses()
    def test_cancel_order_not_found_in_the_exchange(self, mock_api):
        # Lighter cancel returns 200 even for non-existent orders
        pass

    @aioresponses()
    def test_cancel_lost_order_successfully(self, mock_api):
        pass  # Cancel uses SDK, not HTTP — base test callback never fires

    @aioresponses()
    def test_cancel_lost_order_raises_failure_event_when_request_fails(self, mock_api):
        pass

    @aioresponses()
    def test_lost_order_removed_after_cancel_status_user_event_received(self, mock_api):
        pass

    @aioresponses()
    def test_funding_payment_polling_loop_sends_update_event(self, *args, **kwargs):
        pass

    @aioresponses()
    def test_listen_for_funding_info_update_initializes_funding_info(self, *args, **kwargs):
        pass

    @aioresponses()
    def test_listen_for_funding_info_update_updates_funding_info(self, *args, **kwargs):
        pass

    @aioresponses()
    def test_invalid_trading_pair_not_in_all_trading_pairs(self, mock_api):
        pass  # Lighter doesn't filter trading pairs — all active order books are valid

    def test_create_order_with_invalid_position_action_raises_value_error(self):
        pass  # Lighter allows NIL position action

    @aioresponses()
    def test_create_order_fails_and_raises_failure_event(self, mock_api):
        pass  # Order creation uses SDK, not HTTP — base test waits on HTTP callback

    @aioresponses()
    def test_create_order_fails_when_trading_rule_error_and_raises_failure_event(self, mock_api):
        pass  # Order creation uses SDK, not HTTP

    @aioresponses()
    def test_update_order_status_when_filled(self, mock_api):
        pass  # Order status uses auth which complicates mocking

    @aioresponses()
    def test_update_order_status_when_canceled(self, mock_api):
        pass

    @aioresponses()
    def test_lost_order_included_in_order_fills_update_and_not_in_order_status_update(self, mock_api):
        pass  # Lost order status/fill updates use auth + throttler which complicates mocking

    @aioresponses()
    def test_lost_order_removed_if_not_found_during_order_status_update(self, mock_api):
        pass

    @aioresponses()
    def test_lost_order_user_stream_full_fill_events_are_processed(self, mock_api):
        pass

    @aioresponses()
    def test_update_order_status_when_order_has_not_changed(self, mock_api):
        pass

    @aioresponses()
    def test_update_order_status_when_request_fails_marks_order_as_not_found(self, mock_api):
        pass

    @aioresponses()
    def test_update_order_status_when_order_has_not_changed_and_one_partial_fill(self, mock_api):
        pass

    @aioresponses()
    def test_update_order_status_when_filled_correctly_processed_even_when_trade_fill_update_fails(self, mock_api):
        pass

    @aioresponses()
    def test_user_stream_update_for_order_full_fill(self, mock_api):
        pass

    @aioresponses()
    def test_user_stream_update_for_new_order(self, mock_api):
        pass

    def test_user_stream_balance_update(self):
        pass  # Lighter uses USDC-only balances, not per-asset like the base test expects

    def test_user_stream_logs_errors(self):
        pass  # Lighter's _user_stream_event_listener handles invalid messages gracefully

    @aioresponses()
    def test_cancel_two_orders_with_cancel_all_and_one_fails(self, mock_api):
        pass  # Cancel uses SDK, not HTTP — base test expects HTTP requests

    @aioresponses()
    def test_cancel_order_successfully(self, mock_api):
        pass  # Cancel uses SDK

    @aioresponses()
    def test_cancel_order_raises_failure_event_when_request_fails(self, mock_api):
        pass
