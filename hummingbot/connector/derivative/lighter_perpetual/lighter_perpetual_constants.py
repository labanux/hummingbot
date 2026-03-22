from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "lighter_perpetual"
BROKER_ID = ""
MAX_ORDER_ID_LEN = None  # Lighter uses integer client_order_index (uint48)

DOMAIN = EXCHANGE_NAME
TESTNET_DOMAIN = "lighter_perpetual_testnet"

# REST URLs
PERPETUAL_BASE_URL = "https://mainnet.zklighter.elliot.ai"
TESTNET_BASE_URL = "https://testnet.zklighter.elliot.ai"

# WebSocket URLs
PERPETUAL_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
TESTNET_WS_URL = "wss://testnet.zklighter.elliot.ai/stream"

# REST API paths (all under /api/v1/)
ORDER_BOOKS_URL = "/api/v1/orderBooks"
ORDER_BOOK_URL = "/api/v1/orderBook"
ORDER_BOOK_DETAILS_URL = "/api/v1/orderBookDetails"
SEND_TX_URL = "/api/v1/sendTx"
ACCOUNT_URL = "/api/v1/account"
ACCOUNT_ACTIVE_ORDERS_URL = "/api/v1/accountActiveOrders"
ACCOUNT_INACTIVE_ORDERS_URL = "/api/v1/accountInactiveOrders"
TRADES_URL = "/api/v1/trades"
FUNDINGS_URL = "/api/v1/fundings"
NEXT_NONCE_URL = "/api/v1/nextNonce"
HEALTH_CHECK_URL = "/api/v1/orderBooks"  # lightweight endpoint for connectivity check

CURRENCY = "USDC"

FUNDING_RATE_UPDATE_INTERVAL_SECOND = 60

HEARTBEAT_TIME_INTERVAL = 30.0
WS_KEEPALIVE_INTERVAL = 90  # Send frame every 90s, well under 2-min cutoff

# WS Channel names (subscribe with /, receive with :)
WS_ORDER_BOOK_CHANNEL = "order_book"
WS_ACCOUNT_ALL_CHANNEL = "account_all"
WS_ACCOUNT_ALL_ORDERS_CHANNEL = "account_all_orders"
WS_USER_STATS_CHANNEL = "user_stats"
WS_MARKET_STATS_CHANNEL = "market_stats"

# Order type mapping: Hummingbot OrderType → Lighter integer
LIGHTER_ORDER_TYPE = {
    OrderType.LIMIT: 0,        # ORDER_TYPE_LIMIT
    OrderType.MARKET: 1,       # ORDER_TYPE_MARKET
    OrderType.LIMIT_MAKER: 0,  # ORDER_TYPE_LIMIT (post-only via TIF)
}

# Time-in-force mapping: Hummingbot OrderType → Lighter integer
LIGHTER_TIME_IN_FORCE = {
    OrderType.LIMIT: 1,        # ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
    OrderType.MARKET: 0,       # ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
    OrderType.LIMIT_MAKER: 2,  # ORDER_TIME_IN_FORCE_POST_ONLY
}

# Margin mode constants
MARGIN_MODE_CROSS = 0
MARGIN_MODE_ISOLATED = 1

# Order status sets
LIGHTER_OPEN_STATES = {"in-progress", "pending", "open"}
LIGHTER_FILLED_STATES = {"filled"}
LIGHTER_CANCELED_STATES = {
    "canceled", "canceled-post-only", "canceled-reduce-only",
    "canceled-position-not-allowed", "canceled-margin-not-allowed",
    "canceled-too-much-slippage", "canceled-not-enough-liquidity",
    "canceled-self-trade", "canceled-expired", "canceled-oco",
    "canceled-child", "canceled-liquidation", "canceled-invalid-balance",
}

# Order state mapping
ORDER_STATE = {}
for _s in LIGHTER_OPEN_STATES:
    ORDER_STATE[_s] = OrderState.OPEN if _s == "open" else OrderState.PENDING_CREATE
for _s in LIGHTER_FILLED_STATES:
    ORDER_STATE[_s] = OrderState.FILLED
for _s in LIGHTER_CANCELED_STATES:
    ORDER_STATE[_s] = OrderState.CANCELED


def lighter_status_to_hb_state(status: str) -> OrderState:
    state = ORDER_STATE.get(status)
    if state is None:
        raise ValueError(f"Unknown Lighter order status: {status!r}")
    return state


# Error detection — messages from Lighter API that indicate an order is gone
ORDER_NOT_FOUND_MESSAGES = {
    "order not found", "order already canceled", "order does not exist",
    "account not found",  # code 21100 — returned when cancelling unknown order
    "not found",          # code 29404 — generic not-found
}
UNKNOWN_ORDER_MESSAGE = "order not found"
# Error codes from Lighter API
WS_AUTH_REQUIRED_CODE = 20001
API_KEY_NOT_FOUND_CODE = 21109

# Market order slippage (for price protection on IOC orders)
MARKET_ORDER_SLIPPAGE = 0.05

# Rate limits (conservative — actual limits to be confirmed from API)
MAX_REQUEST = 600
ALL_ENDPOINTS_LIMIT = "All"

RATE_LIMITS = [
    RateLimit(ALL_ENDPOINTS_LIMIT, limit=MAX_REQUEST, time_interval=60),
    RateLimit(limit_id=ORDER_BOOKS_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ORDER_BOOK_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ORDER_BOOK_DETAILS_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=SEND_TX_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ACCOUNT_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ACCOUNT_ACTIVE_ORDERS_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=TRADES_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=FUNDINGS_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=HEALTH_CHECK_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
]
