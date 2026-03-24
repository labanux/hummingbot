from bidict import bidict

from hummingbot.core.api_throttler.data_types import RateLimit

REST_URL = "https://mainnet.zklighter.elliot.ai"
CANDLES_ENDPOINT = "/api/v1/candles"
ORDER_BOOKS_ENDPOINT = "/api/v1/orderBooks"
HEALTH_CHECK_ENDPOINT = "/api/v1/orderBooks"

# Lighter has no WebSocket candle channel; we poll REST instead.
WSS_URL = "wss://mainnet.zklighter.elliot.ai/stream"

INTERVALS = bidict({
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
})

MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 500

RATE_LIMITS = [
    RateLimit(CANDLES_ENDPOINT, limit=600, time_interval=60),
]

# How often to poll REST for fresh candle data (seconds)
POLL_INTERVAL = 10
