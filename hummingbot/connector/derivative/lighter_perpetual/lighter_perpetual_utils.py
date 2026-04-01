from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0.0001"),
    buy_percent_fee_deducted_from_returns=True,
)

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USD"
BROKER_ID = ""


# --- Market decimals and bidirectional map ---

@dataclass
class MarketDecimals:
    price_decimals: int
    size_decimals: int
    quote_decimals: int = 6


_market_decimals: Dict[int, MarketDecimals] = {}
_pair_to_market_id: Dict[str, int] = {}  # "BTC-USD" → 0
_market_id_to_pair: Dict[int, str] = {}  # 0 → "BTC-USD"


def initialize_market_map(order_books_response: dict):
    """Populate market maps from GET /api/v1/orderBooks response.

    Actual API response shape per book:
      {"symbol": "ETH", "market_id": 0, "market_type": "perp",
       "supported_price_decimals": 2, "supported_size_decimals": 4,
       "supported_quote_decimals": 6, "min_base_amount": "0.0050",
       "min_quote_amount": "10.000000", ...}
    """
    _market_decimals.clear()
    _pair_to_market_id.clear()
    _market_id_to_pair.clear()
    for book in order_books_response.get("order_books", []):
        market_id = book["market_id"]
        raw_symbol = book["symbol"]  # e.g. "ETH", "BTC"
        trading_pair = f"{raw_symbol}-USD"  # Convert to HB format "ETH-USD"
        _pair_to_market_id[trading_pair] = market_id
        _market_id_to_pair[market_id] = trading_pair
        _market_decimals[market_id] = MarketDecimals(
            price_decimals=book["supported_price_decimals"],
            size_decimals=book["supported_size_decimals"],
            quote_decimals=book.get("supported_quote_decimals", 6),
        )


def pair_to_market_id(trading_pair: str) -> int:
    return _pair_to_market_id[trading_pair]


def market_id_to_pair(market_id: int) -> str:
    return _market_id_to_pair[market_id]


def get_market_decimals(market_id: int) -> MarketDecimals:
    return _market_decimals[market_id]


def to_raw_price(market_id: int, price: Decimal) -> int:
    dec = _market_decimals[market_id].price_decimals
    return int(price * Decimal(10 ** dec))


def from_raw_price(market_id: int, raw: int) -> Decimal:
    dec = _market_decimals[market_id].price_decimals
    return Decimal(raw) / Decimal(10 ** dec)


def to_raw_size(market_id: int, size: Decimal) -> int:
    dec = _market_decimals[market_id].size_decimals
    return int(size * Decimal(10 ** dec))


def from_raw_size(market_id: int, raw: int) -> Decimal:
    dec = _market_decimals[market_id].size_decimals
    return Decimal(raw) / Decimal(10 ** dec)


def is_market_initialized() -> bool:
    return len(_pair_to_market_id) > 0


def get_all_trading_pairs() -> list:
    return list(_pair_to_market_id.keys())


# --- Config Maps ---

class LighterPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "lighter_perpetual"
    lighter_perpetual_api_key_index: str = Field(
        default="0",
        json_schema_extra={
            "prompt": "Enter your Lighter API key index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_api_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter API private key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_account_index: str = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter account index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="lighter_perpetual")


KEYS = LighterPerpetualConfigMap.model_construct()

OTHER_DOMAINS = ["lighter_perpetual_testnet", "lighter_perpetual_b"]
OTHER_DOMAINS_PARAMETER = {
    "lighter_perpetual_testnet": "lighter_perpetual_testnet",
    "lighter_perpetual_b": "lighter_perpetual_b",  # same mainnet, separate identity for dict keying
}
OTHER_DOMAINS_EXAMPLE_PAIR = {
    "lighter_perpetual_testnet": "BTC-USD",
    "lighter_perpetual_b": "BTC-USD",
}
OTHER_DOMAINS_DEFAULT_FEES = {
    "lighter_perpetual_testnet": [0, 0.01],
    "lighter_perpetual_b": [0, 0.01],
}


class LighterPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = "lighter_perpetual_testnet"
    lighter_perpetual_testnet_api_key_index: str = Field(
        default="0",
        json_schema_extra={
            "prompt": "Enter your Lighter testnet API key index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_testnet_api_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter testnet API private key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_testnet_account_index: str = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter testnet account index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="lighter_perpetual_testnet")


class LighterPerpetualBConfigMap(BaseConnectorConfigMap):
    connector: str = "lighter_perpetual_b"
    lighter_perpetual_b_api_key_index: str = Field(
        default="0",
        json_schema_extra={
            "prompt": "Enter your Lighter (B) API key index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_b_api_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter (B) API private key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    lighter_perpetual_b_account_index: str = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter (B) account index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="lighter_perpetual_b")


OTHER_DOMAINS_KEYS = {
    "lighter_perpetual_testnet": LighterPerpetualTestnetConfigMap.model_construct(),
    "lighter_perpetual_b": LighterPerpetualBConfigMap.model_construct(),
}
