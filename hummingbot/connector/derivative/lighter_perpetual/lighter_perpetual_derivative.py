import asyncio
import time
from decimal import Decimal
from typing import Any, AsyncIterable, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.lighter_perpetual import (
    lighter_perpetual_constants as CONSTANTS,
    lighter_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_order_book_data_source import (
    LighterPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_auth import LighterPerpetualAuth
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_api_user_stream_data_source import (
    LighterPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_utils import (
    initialize_market_map,
    is_market_initialized,
    market_id_to_pair,
    pair_to_market_id,
    to_raw_price,
    to_raw_size,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class LighterPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 12.0

    def __init__(
            self,
            balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
            rate_limits_share_pct: Decimal = Decimal("100"),
            lighter_perpetual_api_key_index: str = "0",
            lighter_perpetual_api_private_key: str = "",
            lighter_perpetual_account_index: str = "0",
            trading_pairs: Optional[List[str]] = None,
            trading_required: bool = True,
            domain: str = CONSTANTS.DOMAIN,
    ):
        self.lighter_api_key_index = int(lighter_perpetual_api_key_index) if lighter_perpetual_api_key_index else 0
        self.lighter_api_private_key = lighter_perpetual_api_private_key
        self.lighter_account_index = int(lighter_perpetual_account_index) if lighter_perpetual_account_index else 0
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._position_mode = None
        self._last_trade_history_timestamp = None
        # Future-based correlation dict for order ID resolution
        self._order_id_futures: Dict[int, asyncio.Future] = {}
        self._next_client_order_idx = 1
        # Lock to serialize SDK calls (create/cancel) to prevent nonce race conditions
        self._sdk_lock = asyncio.Lock()
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # === Abstract Properties ===

    @property
    def name(self) -> str:
        return self._domain

    @property
    def authenticator(self) -> Optional[LighterPerpetualAuth]:
        if self._trading_required:
            base_url = CONSTANTS.PERPETUAL_BASE_URL if self._domain == CONSTANTS.DOMAIN else CONSTANTS.TESTNET_BASE_URL
            return LighterPerpetualAuth(
                api_key_index=self.lighter_api_key_index,
                api_private_key=self.lighter_api_private_key,
                account_index=self.lighter_account_index,
                base_url=base_url,
            )
        return None

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.ORDER_BOOKS_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.ORDER_BOOKS_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.HEALTH_CHECK_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False  # Cancel is async — WS confirms

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    # === Abstract Methods ===

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False  # Lighter uses nonces, not timestamps

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return any(msg in str(status_update_exception).lower() for msg in CONSTANTS.ORDER_NOT_FOUND_MESSAGES)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return any(msg in str(cancelation_exception).lower() for msg in CONSTANTS.ORDER_NOT_FOUND_MESSAGES)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth)

    def _create_order_book_data_source(self) -> PerpetualAPIOrderBookDataSource:
        return LighterPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return LighterPerpetualUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    # === Network check ===

    async def _make_network_check_request(self):
        await self._api_get(path_url=self.check_network_request_path)

    # === Trading Rules ===

    async def _make_trading_rules_request(self) -> Any:
        return await self._api_get(path_url=self.trading_rules_request_path)

    async def _make_trading_pairs_request(self) -> Any:
        return await self._api_get(path_url=self.trading_pairs_request_path)

    async def _update_trading_rules(self):
        exchange_info = await self._api_get(path_url=self.trading_rules_request_path)
        # Initialize market maps from orderBooks response
        initialize_market_map(exchange_info)
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule

    async def _initialize_trading_pair_symbol_map(self):
        try:
            exchange_info = await self._api_get(path_url=self.trading_pairs_request_path)
            initialize_market_map(exchange_info)
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for book in exchange_info.get("order_books", []):
            raw_symbol = book["symbol"]  # e.g. "ETH", "BTC"
            trading_pair = f"{raw_symbol}-USD"  # Convert to HB format
            if trading_pair in mapping.inverse:
                continue
            mapping[trading_pair] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        return_val: List[TradingRule] = []
        for book in exchange_info_dict.get("order_books", []):
            try:
                raw_symbol = book["symbol"]  # e.g. "ETH"
                trading_pair = f"{raw_symbol}-USD"
                price_dec = book["supported_price_decimals"]
                size_dec = book["supported_size_decimals"]
                # min_base_amount and min_quote_amount are already human-readable
                # decimals (e.g. "0.0050", "10.000000") — no division needed
                return_val.append(
                    TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=Decimal(book["min_base_amount"]),
                        min_price_increment=Decimal(10) ** Decimal(-price_dec),
                        min_base_amount_increment=Decimal(10) ** Decimal(-size_dec),
                        min_notional_size=Decimal(book["min_quote_amount"]),
                        supports_limit_orders=True,
                        buy_order_collateral_token=CONSTANTS.CURRENCY,
                        sell_order_collateral_token=CONSTANTS.CURRENCY,
                    )
                )
            except Exception:
                self.logger().error(f"Error parsing trading pair rule {book}. Skipping.", exc_info=True)
        return return_val

    # === Fee ===

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 position_action: PositionAction,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = is_maker or False
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _update_trading_fees(self):
        pass

    # === Client Order Index ===

    def _next_client_order_index(self) -> int:
        idx = self._next_client_order_idx
        self._next_client_order_idx += 1
        return idx

    # === Order Placement ===

    async def _place_order(
            self,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            trade_type: TradeType,
            order_type: OrderType,
            price: Decimal,
            position_action: PositionAction = PositionAction.NIL,
            **kwargs,
    ) -> Tuple[str, float]:
        market_id = pair_to_market_id(trading_pair)
        client_order_index = self._next_client_order_index()

        # Set up Future for WS correlation of exchange order_index
        future = asyncio.get_event_loop().create_future()
        self._order_id_futures[client_order_index] = future

        # For MARKET orders, price may be NaN — derive from order book with slippage
        if order_type == OrderType.MARKET or price != price:  # NaN != NaN is True
            ob_price = self.get_price(trading_pair, is_buy=(trade_type == TradeType.BUY))
            slippage = Decimal(str(CONSTANTS.MARKET_ORDER_SLIPPAGE))
            if trade_type == TradeType.BUY:
                price = ob_price * (Decimal("1") + slippage)
            else:
                price = ob_price * (Decimal("1") - slippage)

        price_raw = to_raw_price(market_id, price)
        size_raw = to_raw_size(market_id, amount)

        reduce_only = position_action == PositionAction.CLOSE

        # Pre-flight check: don't send reduce_only orders when there's no position.
        # Lighter silently rejects these (status "canceled-reduce-only") causing
        # an UNRESOLVED order loop. Fail fast instead.
        if reduce_only:
            pos = self._perpetual_trading.get_position(trading_pair)
            if pos is None or pos.amount == Decimal("0"):
                self._order_id_futures.pop(client_order_index, None)
                raise IOError(
                    f"Cannot place reduce_only order {order_id}: "
                    f"no open position for {trading_pair}"
                )

        # IOC/MARKET orders require order_expiry=0; LIMIT orders use default (-1 → 28 days)
        is_ioc = order_type == OrderType.MARKET
        order_expiry = self._auth.signer.DEFAULT_IOC_EXPIRY if is_ioc else self._auth.signer.DEFAULT_28_DAY_ORDER_EXPIRY

        # Serialize SDK calls to prevent nonce race conditions
        async with self._sdk_lock:
            tx, resp, err = await self._auth.signer.create_order(
                market_index=market_id,
                client_order_index=client_order_index,
                base_amount=size_raw,
                price=price_raw,
                is_ask=(trade_type == TradeType.SELL),
                order_type=CONSTANTS.LIGHTER_ORDER_TYPE[order_type],
                time_in_force=CONSTANTS.LIGHTER_TIME_IN_FORCE[order_type],
                reduce_only=reduce_only,
                order_expiry=order_expiry,
            )
        if err:
            self._order_id_futures.pop(client_order_index, None)
            raise IOError(f"Error submitting order {order_id}: {err}")
        # SDK decorator may swallow API errors — check resp.code
        if resp is not None and hasattr(resp, 'code') and resp.code != 200:
            self._order_id_futures.pop(client_order_index, None)
            msg = getattr(resp, 'message', 'unknown')
            raise IOError(f"API rejected order {order_id}: code={resp.code} message={msg}")

        # Wait for WS to fire the order_index (with timeout).
        try:
            exchange_order_id = await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError:
            self._order_id_futures.pop(client_order_index, None)
            # Fallback: poll REST with retries
            # MARKET/IOC orders fill immediately — use more retries with shorter delays
            max_retries = 4 if is_ioc else 2
            exchange_order_id = None
            for attempt in range(max_retries):
                if attempt > 0:
                    await self._sleep(1.5)
                exchange_order_id = await self._poll_order_by_client_index(
                    client_order_index, trading_pair, check_inactive_first=is_ioc,
                )
                if exchange_order_id is not None:
                    break
            if exchange_order_id is None:
                if reduce_only:
                    # Reduce-only orders that can't be found were likely rejected
                    # by Lighter (status "canceled-reduce-only"). Fail immediately
                    # instead of creating a zombie UNRESOLVED order that loops for
                    # ~32s before eventually failing anyway.
                    raise IOError(
                        f"Reduce-only order {order_id} not found on exchange — "
                        f"position may already be closed"
                    )
                # Order was accepted by API (no err, resp.code==200) but we can't
                # resolve the exchange order_index yet.  Return a synthetic ID so the
                # order is tracked as OPEN and _request_order_status can resolve it
                # later via client_order_index lookup.
                self.logger().warning(
                    f"Could not resolve exchange order_index for {order_id} "
                    f"(client_order_index={client_order_index}). Using deferred resolution."
                )
                return f"UNRESOLVED:{client_order_index}", self.current_timestamp

        return str(exchange_order_id), self.current_timestamp

    async def _poll_order_by_client_index(self, client_order_index: int,
                                           trading_pair: str,
                                           check_inactive_first: bool = False) -> Optional[int]:
        """Fallback REST poll when WS correlation times out.

        :param check_inactive_first: If True, check inactive (filled) orders before
            active orders. Useful for MARKET/IOC orders that fill immediately.
        """
        try:
            market_id = pair_to_market_id(trading_pair)
            auth_token = self._auth.get_auth_token()
            auth_headers = {"Authorization": auth_token}

            endpoints = [
                (CONSTANTS.ACCOUNT_ACTIVE_ORDERS_URL, {
                    "account_index": self.lighter_account_index,
                    "market_id": market_id,
                }),
                (CONSTANTS.ACCOUNT_INACTIVE_ORDERS_URL, {
                    "account_index": self.lighter_account_index,
                    "market_id": market_id,
                    "limit": 100,
                }),
            ]
            if check_inactive_first:
                endpoints.reverse()

            active_count = 0
            inactive_count = 0
            for url, params in endpoints:
                resp = await self._api_get(
                    path_url=url,
                    params=params,
                    headers=auth_headers,
                )
                orders = resp.get("orders", [])
                if url == CONSTANTS.ACCOUNT_ACTIVE_ORDERS_URL:
                    active_count = len(orders)
                else:
                    inactive_count = len(orders)
                for order in orders:
                    if int(order.get("client_order_index", -1)) == client_order_index:
                        return order["order_index"]

            self.logger().debug(
                f"Order with client_order_index={client_order_index} not found in "
                f"{active_count} active / {inactive_count} inactive orders"
            )
        except Exception:
            self.logger().warning(
                f"Error polling order by client_order_index {client_order_index}",
                exc_info=True,
            )
        return None

    # === Order Cancellation ===

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        if tracked_order.exchange_order_id is None:
            self.logger().debug(
                f"Cannot cancel order {order_id}: exchange order ID not yet available."
            )
            await self._order_tracker.process_order_not_found(order_id)
            return False

        exchange_oid = tracked_order.exchange_order_id
        # Resolve deferred exchange_order_id if needed
        if str(exchange_oid).startswith("UNRESOLVED:"):
            coi = int(str(exchange_oid).split(":")[1])
            resolved = await self._poll_order_by_client_index(
                coi, tracked_order.trading_pair, check_inactive_first=True,
            )
            if resolved is None:
                self.logger().debug(
                    f"Cannot cancel order {order_id}: exchange order ID still unresolved."
                )
                await self._order_tracker.process_order_not_found(order_id)
                return False
            exchange_oid = resolved

        market_id = pair_to_market_id(tracked_order.trading_pair)
        order_index = int(exchange_oid)

        async with self._sdk_lock:
            tx, resp, err = await self._auth.signer.cancel_order(
                market_index=market_id,
                order_index=order_index,
            )
        if err:
            if any(msg in err.lower() for msg in CONSTANTS.ORDER_NOT_FOUND_MESSAGES):
                self.logger().debug(
                    f"Order {order_id} does not exist on Lighter. No cancellation needed."
                )
                await self._order_tracker.process_order_not_found(order_id)
            raise IOError(f"Error cancelling order {order_id}: {err}")
        return True

    # === Order Status ===

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        try:
            if tracked_order.exchange_order_id:
                exchange_order_id = tracked_order.exchange_order_id
            else:
                exchange_order_id = await tracked_order.get_exchange_order_id()
        except asyncio.TimeoutError:
            exchange_order_id = None

        # Determine if we need to search by client_order_index (deferred resolution)
        search_by_coi = False
        client_order_index = None
        if exchange_order_id and str(exchange_order_id).startswith("UNRESOLVED:"):
            search_by_coi = True
            client_order_index = int(str(exchange_order_id).split(":")[1])

        market_id = pair_to_market_id(tracked_order.trading_pair)
        auth_token = self._auth.get_auth_token()
        auth_headers = {"Authorization": auth_token}

        # For unresolved orders, check inactive first (MARKET/IOC orders fill immediately)
        endpoints = [
            (CONSTANTS.ACCOUNT_ACTIVE_ORDERS_URL, {
                "account_index": self.lighter_account_index,
                "market_id": market_id,
            }),
            (CONSTANTS.ACCOUNT_INACTIVE_ORDERS_URL, {
                "account_index": self.lighter_account_index,
                "market_id": market_id,
                "limit": 100,
            }),
        ]
        if search_by_coi:
            endpoints.reverse()

        for url, params in endpoints:
            resp = await self._api_get(
                path_url=url,
                params=params,
                headers=auth_headers,
            )
            for order in resp.get("orders", []):
                matched = False
                if search_by_coi:
                    matched = int(order.get("client_order_index", -1)) == client_order_index
                else:
                    matched = str(order.get("order_index")) == str(exchange_order_id)
                if matched:
                    new_state = CONSTANTS.lighter_status_to_hb_state(order["status"])
                    return OrderUpdate(
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=order.get("updated_at", time.time() * 1e3) * 1e-3,
                        new_state=new_state,
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=str(order["order_index"]),
                    )

        # Order not found in active or inactive lists.
        # For UNRESOLVED orders: if no open position exists, the reduce-only order
        # was silently rejected by Lighter. Return CANCELED so the executor stops
        # retrying instead of looping for ~32s before marking as FAILED.
        if search_by_coi:
            pos = self._perpetual_trading.get_position(tracked_order.trading_pair)
            if pos is None or pos.amount == Decimal("0"):
                self.logger().info(
                    f"UNRESOLVED order {tracked_order.client_order_id} not found and no open "
                    f"position for {tracked_order.trading_pair} — marking as CANCELED."
                )
                return OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=time.time(),
                    new_state=OrderState.CANCELED,
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                )

        raise IOError(f"Order {tracked_order.client_order_id} not found in active or inactive orders")

    async def _update_order_status(self):
        await self._update_orders_fills(orders=list(self._order_tracker.all_fillable_orders.values()))
        await self._update_orders()

    async def _update_lost_orders_status(self):
        await self._update_orders_fills(orders=list(self._order_tracker.lost_orders.values()))
        await self._update_lost_orders()

    # === Trade History ===

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []
        if order.exchange_order_id is None:
            return trade_updates
        # Skip if exchange_order_id is still unresolved — can't query trades by COI
        if str(order.exchange_order_id).startswith("UNRESOLVED:"):
            return trade_updates

        try:
            auth_token = self._auth.get_auth_token()
            auth_headers = {"Authorization": auth_token}
            resp = await self._api_get(
                path_url=CONSTANTS.TRADES_URL,
                params={
                    "account_index": self.lighter_account_index,
                    "order_index": int(order.exchange_order_id),
                    "sort_by": "timestamp",
                    "limit": 100,
                },
                headers=auth_headers,
            )
            for trade in resp.get("trades", []):
                trade_update = self._parse_trade_to_trade_update(trade, order)
                if trade_update:
                    trade_updates.append(trade_update)
        except Exception:
            self.logger().warning(
                f"Failed to fetch trade updates for order {order.client_order_id}",
                exc_info=True,
            )
        return trade_updates

    def _parse_trade_to_trade_update(self, trade: Dict[str, Any],
                                      order: InFlightOrder) -> Optional[TradeUpdate]:
        """Parse a Lighter trade dict into a TradeUpdate."""
        account_id = self.lighter_account_index

        if trade.get("ask_account_id") == account_id:
            side = TradeType.SELL
            fee_raw = trade.get("taker_fee", 0) if not trade.get("is_maker_ask", False) else trade.get("maker_fee", 0)
        elif trade.get("bid_account_id") == account_id:
            side = TradeType.BUY
            fee_raw = trade.get("maker_fee", 0) if not trade.get("is_maker_ask", False) else trade.get("taker_fee", 0)
        else:
            return None

        market_id = trade.get("market_id", 0)
        try:
            from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_utils import get_market_decimals
            decimals = get_market_decimals(market_id)
            price_decimals = decimals.price_decimals
        except (KeyError, ImportError):
            price_decimals = 2

        fee_amount = Decimal(str(fee_raw)) / Decimal(10 ** price_decimals) if fee_raw else Decimal("0")

        # Determine position action from position size before trade
        position_size_before = trade.get("taker_position_size_before", "0")
        if position_size_before == "0":
            position_action = PositionAction.OPEN
        else:
            position_action = PositionAction.CLOSE

        fee_asset = order.quote_asset
        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent_token=fee_asset,
            flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)],
        )

        return TradeUpdate(
            trade_id=str(trade.get("trade_id", "")),
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=Decimal(str(trade.get("size", "0"))),
            fill_quote_amount=Decimal(str(trade.get("usd_amount", "0"))),
            fill_price=Decimal(str(trade.get("price", "0"))),
            fill_timestamp=trade.get("timestamp", time.time()) * 1e-3 if trade.get("timestamp", 0) > 1e10 else trade.get("timestamp", time.time()),
        )

    # === User Stream Event Listener ===

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Lighter. Check API key and network connection.",
                )
                await self._sleep(1.0)

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                if not isinstance(event_message, dict):
                    continue

                channel = event_message.get("channel", "")
                msg_type = event_message.get("type", "")

                if CONSTANTS.WS_ACCOUNT_ALL_ORDERS_CHANNEL in channel:
                    # Order updates — orders is dict: {"market_id": [order1, order2, ...]}
                    orders = event_message.get("orders", {})
                    if isinstance(orders, dict):
                        for market_key, order_list in orders.items():
                            if isinstance(order_list, list):
                                for order_msg in order_list:
                                    if isinstance(order_msg, dict):
                                        self._process_order_message(order_msg)
                            elif isinstance(order_list, dict):
                                self._process_order_message(order_list)
                    elif isinstance(orders, list):
                        for order_msg in orders:
                            if isinstance(order_msg, dict):
                                self._process_order_message(order_msg)

                elif CONSTANTS.WS_ACCOUNT_ALL_CHANNEL in channel:
                    # Account data: positions, assets, trades
                    # trades is dict: {"market_id": [trade1, trade2, ...]}
                    if "trades" in event_message:
                        trades = event_message.get("trades", {})
                        if isinstance(trades, dict):
                            for market_key, trade_list in trades.items():
                                if isinstance(trade_list, list):
                                    for trade_msg in trade_list:
                                        if isinstance(trade_msg, dict):
                                            await self._process_ws_trade_message(trade_msg)
                        elif isinstance(trades, list):
                            for trade_msg in trades:
                                await self._process_ws_trade_message(trade_msg)
                    if "positions" in event_message:
                        self._process_ws_positions(event_message.get("positions", {}))
                    if "assets" in event_message:
                        self._process_ws_assets(event_message.get("assets", {}))

                elif CONSTANTS.WS_USER_STATS_CHANNEL in channel:
                    # Balance updates
                    stats = event_message.get("stats", {})
                    if stats:
                        self._process_ws_user_stats(stats)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """Process an order update from WS."""
        client_order_index = order_msg.get("client_order_index")
        order_index = order_msg.get("order_index")
        status = order_msg.get("status", "")

        # Resolve Future if pending (for _place_order correlation)
        if client_order_index is not None and order_index is not None:
            self._resolve_order_id_future(client_order_index, order_index)

        # Find tracked order by exchange_order_id or client_order_id
        exchange_order_id = str(order_index) if order_index else ""
        client_order_id = str(order_msg.get("client_order_id", ""))

        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if tracked_order is None and exchange_order_id:
            tracked_order = self._order_tracker.all_updatable_orders_by_exchange_order_id.get(exchange_order_id)
        if tracked_order is None:
            # Try by client_order_index as string
            for o in self._order_tracker.all_updatable_orders.values():
                if hasattr(o, '_lighter_client_order_index') and o._lighter_client_order_index == client_order_index:
                    tracked_order = o
                    break

        if tracked_order is None:
            self.logger().debug(f"Ignoring order message with order_index={order_index}: not in in_flight_orders.")
            return

        if exchange_order_id:
            tracked_order.update_exchange_order_id(exchange_order_id)

        try:
            new_state = CONSTANTS.lighter_status_to_hb_state(status)
        except ValueError:
            self.logger().warning(f"Unknown Lighter order status: {status!r}. Defaulting to OPEN.")
            new_state = CONSTANTS.ORDER_STATE.get(status, CONSTANTS.ORDER_STATE.get("open"))
            if new_state is None:
                return

        update_timestamp = order_msg.get("updated_at", time.time() * 1e3)
        if update_timestamp > 1e12:
            update_timestamp = update_timestamp * 1e-3

        order_update = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=update_timestamp,
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id or tracked_order.exchange_order_id,
        )
        self._order_tracker.process_order_update(order_update=order_update)

    async def _process_ws_trade_message(self, trade: Dict[str, Any]):
        """Process a trade fill from WS account_all channel."""
        account_id = self.lighter_account_index

        # Determine our order side
        if trade.get("ask_account_id") == account_id:
            exchange_order_id = str(trade.get("ask_id", ""))
            client_order_index = trade.get("ask_client_id")
            fee_raw = trade.get("taker_fee", 0) if not trade.get("is_maker_ask", False) else trade.get("maker_fee", 0)
        elif trade.get("bid_account_id") == account_id:
            exchange_order_id = str(trade.get("bid_id", ""))
            client_order_index = trade.get("bid_client_id")
            fee_raw = trade.get("maker_fee", 0) if not trade.get("is_maker_ask", False) else trade.get("taker_fee", 0)
        else:
            return

        # Resolve Future if pending (market order race condition)
        if client_order_index is not None:
            order_index = trade.get("ask_id") if trade.get("ask_account_id") == account_id else trade.get("bid_id")
            if order_index is not None:
                self._resolve_order_id_future(client_order_index, order_index)

        tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        if tracked_order is None:
            self.logger().debug(f"Ignoring trade message for exchange_order_id={exchange_order_id}: not tracked.")
            return

        market_id = trade.get("market_id", 0)
        try:
            from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_utils import get_market_decimals
            decimals = get_market_decimals(market_id)
            price_decimals = decimals.price_decimals
        except (KeyError, ImportError):
            price_decimals = 2

        fee_amount = Decimal(str(fee_raw)) / Decimal(10 ** price_decimals) if fee_raw else Decimal("0")
        fee_asset = tracked_order.quote_asset

        position_size_before = trade.get("taker_position_size_before", "0")
        position_action = PositionAction.OPEN if position_size_before == "0" else PositionAction.CLOSE

        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent_token=fee_asset,
            flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)],
        )

        fill_timestamp = trade.get("timestamp", time.time())
        if fill_timestamp > 1e12:
            fill_timestamp = fill_timestamp * 1e-3

        trade_update = TradeUpdate(
            trade_id=str(trade.get("trade_id", "")),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fill_timestamp=fill_timestamp,
            fill_price=Decimal(str(trade.get("price", "0"))),
            fill_base_amount=Decimal(str(trade.get("size", "0"))),
            fill_quote_amount=Decimal(str(trade.get("usd_amount", "0"))),
            fee=fee,
        )
        self._order_tracker.process_trade_update(trade_update)

    def _resolve_order_id_future(self, client_order_index: int, order_index: int):
        """Resolve the Future for a pending _place_order call."""
        future = self._order_id_futures.pop(client_order_index, None)
        if future and not future.done():
            future.set_result(order_index)

    def _process_ws_positions(self, positions: Dict[str, Any]):
        """Update positions from WS account_all channel."""
        for pos_key, pos in positions.items():
            if not isinstance(pos, dict):
                continue
            market_id = pos.get("market_id")
            try:
                hb_trading_pair = market_id_to_pair(market_id)
            except KeyError:
                continue

            sign = pos.get("sign", 1)
            amount = Decimal(str(pos.get("position", "0"))) * sign
            position_side = PositionSide.LONG if sign == 1 else PositionSide.SHORT
            entry_price = Decimal(str(pos.get("avg_entry_price", "0")))
            unrealized_pnl = Decimal(str(pos.get("unrealized_pnl", "0")))

            # Derive leverage from position_value / allocated_margin
            position_value = Decimal(str(pos.get("position_value", "0")))
            allocated_margin = Decimal(str(pos.get("allocated_margin", "1")))
            leverage = position_value / allocated_margin if allocated_margin else Decimal("1")

            p_key = self._perpetual_trading.position_key(hb_trading_pair, position_side)
            if amount != 0:
                _position = Position(
                    trading_pair=hb_trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage,
                )
                self._perpetual_trading.set_position(p_key, _position)
            else:
                self._perpetual_trading.remove_position(p_key)

    def _process_ws_assets(self, assets: Dict[str, Any]):
        """Update balances from WS account_all assets."""
        if not assets:
            return
        for asset_id, asset in assets.items():
            if not isinstance(asset, dict):
                continue
            if asset.get("symbol") == "USDC":
                total = Decimal(str(asset.get("balance", "0")))
                locked = Decimal(str(asset.get("locked_balance", "0")))
                self._account_balances[CONSTANTS.CURRENCY] = total
                self._account_available_balances[CONSTANTS.CURRENCY] = total - locked

    def _process_ws_user_stats(self, stats: Dict[str, Any]):
        """Update balances from WS user_stats channel."""
        collateral = stats.get("collateral")
        available = stats.get("available_balance")
        if collateral is not None:
            self._account_balances[CONSTANTS.CURRENCY] = Decimal(str(collateral))
        if available is not None:
            self._account_available_balances[CONSTANTS.CURRENCY] = Decimal(str(available))

    # === Balances ===

    def _extract_account(self, resp: Dict[str, Any]) -> Dict[str, Any]:
        """Extract the account object from REST /api/v1/account response.
        Response shape: {"accounts": [{...account data...}]}
        """
        accounts = resp.get("accounts", [])
        if accounts and isinstance(accounts, list):
            return accounts[0]
        return resp  # Fallback for unexpected shape

    async def _update_balances(self):
        resp = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_URL,
            params={"by": "index", "value": self.lighter_account_index},
        )
        account = self._extract_account(resp)
        # Prefer top-level collateral/available_balance (always present, asset_id-independent)
        collateral = account.get("collateral")
        available = account.get("available_balance")
        if collateral is not None and available is not None:
            self._account_balances[CONSTANTS.CURRENCY] = Decimal(str(collateral))
            self._account_available_balances[CONSTANTS.CURRENCY] = Decimal(str(available))
            return
        # Fallback: scan assets list for USDC by symbol
        assets = account.get("assets", [])
        if isinstance(assets, list):
            for asset in assets:
                if not isinstance(asset, dict):
                    continue
                if asset.get("symbol") == "USDC":
                    total = Decimal(str(asset.get("balance", "0")))
                    locked = Decimal(str(asset.get("locked_balance", "0")))
                    self._account_balances[CONSTANTS.CURRENCY] = total
                    self._account_available_balances[CONSTANTS.CURRENCY] = total - locked
                    return
        elif isinstance(assets, dict):
            # WS shape: {"asset_id_str": {"symbol": "USDC", ...}}
            for asset_id, asset in assets.items():
                if not isinstance(asset, dict):
                    continue
                if asset.get("symbol") == "USDC":
                    total = Decimal(str(asset.get("balance", "0")))
                    locked = Decimal(str(asset.get("locked_balance", "0")))
                    self._account_balances[CONSTANTS.CURRENCY] = total
                    self._account_available_balances[CONSTANTS.CURRENCY] = total - locked
                    return

    # === Positions ===

    async def _update_positions(self):
        resp = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_URL,
            params={"by": "index", "value": self.lighter_account_index},
        )
        account = self._extract_account(resp)
        # REST returns positions as a list; WS returns as dict
        raw_positions = account.get("positions", [])
        processed_pairs = set()

        positions_iter = (
            enumerate(raw_positions) if isinstance(raw_positions, list)
            else raw_positions.items()
        )

        for pos_key, pos in positions_iter:
            if not isinstance(pos, dict):
                continue
            market_id = pos.get("market_id")
            try:
                hb_trading_pair = market_id_to_pair(market_id)
            except KeyError:
                continue

            if hb_trading_pair in processed_pairs:
                continue
            processed_pairs.add(hb_trading_pair)

            sign = pos.get("sign", 1)
            amount = Decimal(str(pos.get("position", "0"))) * sign
            position_side = PositionSide.LONG if sign == 1 else PositionSide.SHORT
            entry_price = Decimal(str(pos.get("avg_entry_price", "0")))
            unrealized_pnl = Decimal(str(pos.get("unrealized_pnl", "0")))

            position_value = Decimal(str(pos.get("position_value", "0")))
            allocated_margin = Decimal(str(pos.get("allocated_margin", "1")))
            leverage = position_value / allocated_margin if allocated_margin else Decimal("1")

            p_key = self._perpetual_trading.position_key(hb_trading_pair, position_side)
            if amount != 0:
                _position = Position(
                    trading_pair=hb_trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage,
                )
                self._perpetual_trading.set_position(p_key, _position)
            else:
                self._perpetual_trading.remove_position(p_key)

        if not raw_positions:
            keys = list(self._perpetual_trading.account_positions.keys())
            for key in keys:
                self._perpetual_trading.remove_position(key)

    # === Position Mode ===

    async def _get_position_mode(self) -> Optional[PositionMode]:
        return PositionMode.ONEWAY

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        if mode != PositionMode.ONEWAY:
            return False, "Lighter only supports the ONEWAY position mode."
        return True, ""

    # === Leverage ===

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        market_id = pair_to_market_id(trading_pair)
        try:
            async with self._sdk_lock:
                tx, resp, err = await self._auth.signer.update_leverage(
                    market_index=market_id,
                    margin_mode=CONSTANTS.MARGIN_MODE_CROSS,
                    leverage=leverage,
                )
            if err:
                return False, f"Error setting leverage: {err}"
            return True, ""
        except Exception as e:
            return False, f"Error setting leverage for {trading_pair}: {e}"

    # === Funding ===

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        market_id = pair_to_market_id(trading_pair)
        try:
            now = int(time.time())
            resp = await self._api_get(
                path_url=CONSTANTS.FUNDINGS_URL,
                params={
                    "market_id": market_id,
                    "resolution": "1h",
                    "start_timestamp": now - 7200,
                    "end_timestamp": now,
                    "count_back": 1,
                },
            )
            fundings = resp.get("fundings", [])
            if fundings:
                latest = fundings[-1]
                rate = Decimal(str(latest.get("rate", "0")))
                timestamp = latest.get("timestamp", 0)
                # Payment amount not directly available from market-level funding endpoint
                payment = Decimal("-1")
                return timestamp, rate, payment
        except Exception:
            self.logger().warning(
                f"Error fetching funding info for {trading_pair}", exc_info=True
            )
        return 0, Decimal("-1"), Decimal("-1")

    # === Last Traded Price ===

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        market_id = pair_to_market_id(trading_pair)
        resp = await self._api_get(
            path_url=CONSTANTS.ORDER_BOOK_DETAILS_URL,
            params={"market_id": market_id, "filter": "perp"},
        )
        for detail in resp.get("order_book_details", []):
            if detail.get("market_id") == market_id:
                return float(detail.get("last_trade_price", 0))
        raise RuntimeError(f"Price not found for trading_pair={trading_pair}")

    # === Status Polling ===

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )

    # === Price helper for market orders ===

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        res: List[Dict[str, str]] = []
        resp = await self._api_get(
            path_url=CONSTANTS.ORDER_BOOK_DETAILS_URL,
            params={"filter": "perp"},
        )
        for detail in resp.get("order_book_details", []):
            res.append({
                "symbol": detail.get("symbol", ""),
                "price": str(detail.get("last_trade_price", 0)),
            })
        return res
