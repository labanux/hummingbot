import asyncio
import logging
import time
from typing import Dict, List, Optional

import numpy as np

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.lighter_perpetual_candles import constants as CONSTANTS
from hummingbot.logger import HummingbotLogger


class LighterPerpetualCandles(CandlesBase):
    """
    Candle feed for Lighter perpetual exchange.

    Lighter does not expose a WebSocket candle channel, so this implementation
    polls the REST ``/api/v1/candles`` endpoint at a fixed interval to keep the
    candle buffer up to date.
    """

    _logger: Optional[HummingbotLogger] = None
    _market_id_map: Dict[str, int] = {}

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        super().__init__(trading_pair, interval, max_records)
        self._market_id: Optional[int] = None

    # ------------------------------------------------------------------ #
    #  Properties required by CandlesBase                                 #
    # ------------------------------------------------------------------ #

    @property
    def name(self):
        return f"lighter_perpetual_{self._trading_pair}"

    @property
    def rest_url(self):
        return CONSTANTS.REST_URL

    @property
    def wss_url(self):
        return CONSTANTS.WSS_URL

    @property
    def health_check_url(self):
        return f"{CONSTANTS.REST_URL}{CONSTANTS.HEALTH_CHECK_ENDPOINT}"

    @property
    def candles_url(self):
        return f"{CONSTANTS.REST_URL}{CONSTANTS.CANDLES_ENDPOINT}"

    @property
    def candles_endpoint(self):
        return CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_max_result_per_rest_request(self):
        return CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS

    def get_exchange_trading_pair(self, trading_pair):
        return trading_pair

    # ------------------------------------------------------------------ #
    #  Exchange data initialisation (market-id lookup)                    #
    # ------------------------------------------------------------------ #

    async def initialize_exchange_data(self):
        if self._trading_pair in self._market_id_map:
            self._market_id = self._market_id_map[self._trading_pair]
            return
        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=f"{CONSTANTS.REST_URL}{CONSTANTS.ORDER_BOOKS_ENDPOINT}",
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.CANDLES_ENDPOINT,
        )
        for book in data.get("order_books", []):
            pair = f"{book['symbol']}-USD"
            self._market_id_map[pair] = book["market_id"]
        self._market_id = self._market_id_map.get(self._trading_pair)
        if self._market_id is None:
            raise ValueError(f"Trading pair {self._trading_pair} not found on Lighter")

    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(
            url=self.health_check_url,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.CANDLES_ENDPOINT,
        )
        return NetworkStatus.CONNECTED

    # ------------------------------------------------------------------ #
    #  REST candle fetching                                               #
    # ------------------------------------------------------------------ #

    def _get_rest_candles_params(self,
                                 start_time: Optional[int] = None,
                                 end_time: Optional[int] = None,
                                 limit: Optional[int] = None) -> dict:
        return {
            "market_id": str(self._market_id),
            "resolution": CONSTANTS.INTERVALS[self.interval],
            "start_timestamp": str(start_time),
            "end_timestamp": str(end_time),
            "count_back": str(limit or self.candles_max_result_per_rest_request),
        }

    def _parse_rest_candles(self, data: dict, end_time: Optional[int] = None) -> List[List[float]]:
        candles = []
        for row in data.get("c", []):
            candles.append([
                self.ensure_timestamp_in_seconds(row["t"]),
                float(row["o"]),
                float(row["h"]),
                float(row["l"]),
                float(row["c"]),
                float(row["v"]),                # base volume
                float(row.get("V", 0)),          # quote volume
                0.,                              # n_trades (not provided)
                0.,                              # taker_buy_base_volume
                0.,                              # taker_buy_quote_volume
            ])
        return sorted(candles, key=lambda x: x[0])

    # ------------------------------------------------------------------ #
    #  REST polling (replaces WebSocket-based subscription)               #
    # ------------------------------------------------------------------ #

    async def listen_for_subscriptions(self):
        """
        Lighter has no WebSocket candle channel.
        Poll the REST API at a fixed interval to keep the buffer current.
        """
        while True:
            try:
                end_time = int(time.time())
                start_time = end_time - self.interval_in_seconds * self.max_records
                candles: np.ndarray = await self.fetch_candles(
                    start_time=start_time, end_time=end_time)

                if len(candles) > 0:
                    if len(self._candles) == 0:
                        for row in candles[-self.max_records:]:
                            self._candles.append(row)
                    else:
                        latest_ts = int(self._candles[-1][0])
                        for row in candles:
                            ts = int(row[0])
                            if ts > latest_ts:
                                self._candles.append(row)
                                latest_ts = ts
                            elif ts == latest_ts:
                                self._candles[-1] = row

                await asyncio.sleep(CONSTANTS.POLL_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Error polling Lighter candles. Retrying in 5 s ...")
                await self._sleep(5.0)

    # ------------------------------------------------------------------ #
    #  WebSocket stubs (not used, but required by CandlesBase)            #
    # ------------------------------------------------------------------ #

    def ws_subscription_payload(self):
        return {}

    def _parse_websocket_message(self, data):
        return None

    async def _on_order_stream_interruption(self, websocket_assistant=None):
        # Do NOT clear candles on interruption — we use REST polling.
        pass
