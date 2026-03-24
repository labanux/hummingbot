"""
VRCS – VWAP / ROC / Candle-trend Strategy

A directional trading controller that uses short-term trend indicators
(validated on TAO, CRV, ASTER — see local-md/trend-indicator.md) to decide
between aggressive trend-following entries and passive market-making entries.

Signal logic
------------
LONG  (signal = 1):  Stoch %K(14) < 10  AND  RSI(5) < 35
                     AND  ATR(7) expanding  AND  MACD(8,26) > 0
SHORT (signal = -1): Price > VWAP(30) by > 1 %  AND  ROC(15) > 1 %
NEUTRAL (signal = 0): no clear trend

Entry pricing
-------------
- Trend detected  → aggressive entry *ahead* of mid price
    LONG:   mid + buy_spread
    SHORT:  mid − sell_spread
- Neutral         → passive entry *behind* mid price (classic PMM)
    BUY:    mid − buy_spread
    SELL:   mid + sell_spread

Position management
-------------------
- Only 1 position at a time (entry → close before next entry).
- Exits are handled by the triple-barrier (TP / SL / time-limit).
- In neutral mode both buy & sell orders are placed; when one fills the
  other is cancelled automatically.
"""
from decimal import Decimal
from typing import List, Optional

import pandas as pd
from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TripleBarrierConfig,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import (
    CreateExecutorAction,
    ExecutorAction,
    StopExecutorAction,
)


# ------------------------------------------------------------------ #
#  Config                                                             #
# ------------------------------------------------------------------ #

class VRCSConfig(DirectionalTradingControllerConfigBase):
    controller_name: str = "vrcs"

    # -- spread parameters (single values) --
    buy_spread: Decimal = Field(
        default=Decimal("0.0005"),
        json_schema_extra={
            "prompt": "Enter the buy spread (e.g., 0.0005 for 0.05%): ",
            "prompt_on_new": True, "is_updatable": True})
    sell_spread: Decimal = Field(
        default=Decimal("0.0005"),
        json_schema_extra={
            "prompt": "Enter the sell spread (e.g., 0.0005 for 0.05%): ",
            "prompt_on_new": True, "is_updatable": True})

    # -- candle feed --
    candles_connector: str = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the candles connector (leave empty to use trading connector): ",
            "prompt_on_new": True})
    candles_trading_pair: str = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the candles trading pair (leave empty to use trading pair): ",
            "prompt_on_new": True})
    interval: str = Field(
        default="1m",
        json_schema_extra={
            "prompt": "Enter the candle interval (e.g., 1m): ",
            "prompt_on_new": True})

    # -- order lifecycle --
    executor_refresh_time: int = Field(
        default=15,
        json_schema_extra={
            "prompt": "Enter executor refresh time in seconds (e.g., 15): ",
            "prompt_on_new": True, "is_updatable": True})

    # -- override defaults for single-position mode --
    max_executors_per_side: int = Field(default=1)

    @field_validator("candles_connector", mode="before")
    @classmethod
    def set_candles_connector(cls, v, info: ValidationInfo):
        if v is None or v == "":
            return info.data.get("connector_name")
        return v

    @field_validator("candles_trading_pair", mode="before")
    @classmethod
    def set_candles_trading_pair(cls, v, info: ValidationInfo):
        if v is None or v == "":
            return info.data.get("trading_pair")
        return v

    @property
    def triple_barrier_config(self) -> TripleBarrierConfig:
        return TripleBarrierConfig(
            stop_loss=self.stop_loss,
            take_profit=self.take_profit,
            time_limit=self.time_limit,
            trailing_stop=self.trailing_stop,
            open_order_type=OrderType.LIMIT,
            take_profit_order_type=self.take_profit_order_type,
            stop_loss_order_type=OrderType.MARKET,
            time_limit_order_type=OrderType.MARKET,
        )


# ------------------------------------------------------------------ #
#  Controller                                                         #
# ------------------------------------------------------------------ #

class VRCSController(DirectionalTradingControllerBase):
    # 300 candles warmup as per trend-indicator spec
    CANDLE_BUFFER = 300

    def __init__(self, config: VRCSConfig, *args, **kwargs):
        self.config = config
        self.max_records = self.CANDLE_BUFFER + 50  # small margin
        super().__init__(config, *args, **kwargs)

    # -- candle feed registration ----------------------------------- #

    def get_candles_config(self) -> List[CandlesConfig]:
        return [CandlesConfig(
            connector=self.config.candles_connector,
            trading_pair=self.config.candles_trading_pair,
            interval=self.config.interval,
            max_records=self.max_records,
        )]

    # -- indicator computation -------------------------------------- #

    async def update_processed_data(self):
        df = self.market_data_provider.get_candles_df(
            connector_name=self.config.candles_connector,
            trading_pair=self.config.candles_trading_pair,
            interval=self.config.interval,
            max_records=self.max_records,
        )
        if df.empty or len(df) < 30:
            self.processed_data = {"signal": 0, "features": pd.DataFrame()}
            return

        self._compute_indicators(df)

        # --- LONG conditions ---
        long_cond = (
            (df["stoch_k_14"] < 10) &
            (df["rsi_5"] < 35) &
            (df["atr_7"] > df["atr_7"].shift(1)) &
            (df["macd_line"] > 0)
        )

        # --- SHORT conditions ---
        short_cond = (
            (df["vwap_dist_30"] > 1.0) &
            (df["roc_15"] > 1.0)
        )

        df["signal"] = 0
        df.loc[long_cond, "signal"] = 1
        df.loc[short_cond, "signal"] = -1

        self.processed_data["signal"] = int(df["signal"].iloc[-1])
        self.processed_data["features"] = df

    @staticmethod
    def _compute_indicators(df: pd.DataFrame):
        # --- Stochastic %K(14) — raw, no smoothing ---
        lowest_low = df["low"].rolling(14).min()
        highest_high = df["high"].rolling(14).max()
        denom = highest_high - lowest_low
        df["stoch_k_14"] = ((df["close"] - lowest_low) / denom.replace(0, float("nan"))) * 100

        # --- RSI(5) — Wilder's smoothing ---
        delta = df["close"].diff()
        gains = delta.where(delta > 0, 0.0)
        losses = (-delta).where(delta < 0, 0.0)
        avg_gain = gains.ewm(com=4, min_periods=5).mean()
        avg_loss = losses.ewm(com=4, min_periods=5).mean()
        rs = avg_gain / avg_loss.replace(0, float("nan"))
        df["rsi_5"] = 100 - (100 / (1 + rs))

        # --- ATR(7) — EMA span=7 ---
        tr1 = df["high"] - df["low"]
        tr2 = (df["high"] - df["close"].shift(1)).abs()
        tr3 = (df["low"] - df["close"].shift(1)).abs()
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df["atr_7"] = true_range.ewm(span=7).mean()

        # --- MACD(8,26,9) line ---
        ema_fast = df["close"].ewm(span=8, adjust=False).mean()
        ema_slow = df["close"].ewm(span=26, adjust=False).mean()
        df["macd_line"] = ema_fast - ema_slow

        # --- VWAP(30) distance % ---
        vp = df["close"] * df["volume"]
        cum_vp = vp.rolling(30).sum()
        cum_v = df["volume"].rolling(30).sum()
        vwap_30 = cum_vp / cum_v.replace(0, float("nan"))
        df["vwap_dist_30"] = ((df["close"] - vwap_30) / vwap_30.replace(0, float("nan"))) * 100

        # --- ROC(15) % ---
        close_15 = df["close"].shift(15)
        df["roc_15"] = ((df["close"] - close_15) / close_15.replace(0, float("nan"))) * 100

    # -- entry / exit logic ----------------------------------------- #

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        actions.extend(self.stop_actions_proposal())
        actions.extend(self.create_actions_proposal())
        return actions

    def create_actions_proposal(self) -> List[ExecutorAction]:
        create_actions: List[ExecutorAction] = []
        signal = self.processed_data.get("signal", 0)

        # --- check for any active position (filled order) ---
        active_positions = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.is_active and x.is_trading,
        )
        if len(active_positions) > 0:
            return create_actions  # position open — wait for exit

        mid = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        amount = self.config.total_amount_quote / mid

        # --- pending (unfilled) orders ---
        pending = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.is_active and not x.is_trading,
        )

        if signal == 1:
            # LONG — aggressive entry ahead of mid
            if not self._has_pending_side(pending, TradeType.BUY):
                entry_price = mid * (1 + self.config.buy_spread)
                create_actions.append(self._make_create_action(
                    TradeType.BUY, entry_price, amount, "trend_long"))
        elif signal == -1:
            # SHORT — aggressive entry ahead of mid
            if not self._has_pending_side(pending, TradeType.SELL):
                entry_price = mid * (1 - self.config.sell_spread)
                create_actions.append(self._make_create_action(
                    TradeType.SELL, entry_price, amount, "trend_short"))
        else:
            # NEUTRAL — passive PMM: buy behind, sell behind
            # Split amount in half so both sides fit within budget
            if len(pending) == 0 and self._cooldown_ok():
                half_amount = amount / Decimal("2")
                buy_price = mid * (1 - self.config.buy_spread)
                sell_price = mid * (1 + self.config.sell_spread)
                create_actions.append(self._make_create_action(
                    TradeType.BUY, buy_price, half_amount, "neutral_buy"))
                create_actions.append(self._make_create_action(
                    TradeType.SELL, sell_price, half_amount, "neutral_sell"))

        return create_actions

    def stop_actions_proposal(self) -> List[ExecutorAction]:
        stop_actions: List[ExecutorAction] = []

        active_positions = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.is_active and x.is_trading,
        )
        pending = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.is_active and not x.is_trading,
        )

        # 1) One side filled → cancel all unfilled orders
        if len(active_positions) > 0 and len(pending) > 0:
            for p in pending:
                stop_actions.append(StopExecutorAction(
                    controller_id=self.config.id, executor_id=p.id))

        # 2) Refresh stale unfilled orders so prices stay current
        signal = self.processed_data.get("signal", 0)
        for p in pending:
            age = self.market_data_provider.time() - p.timestamp
            if age > self.config.executor_refresh_time:
                stop_actions.append(StopExecutorAction(
                    controller_id=self.config.id, executor_id=p.id))

        # 3) Signal changed → cancel pending orders from the wrong mode
        if signal != 0 and len(pending) > 0:
            for p in pending:
                level = p.custom_info.get("level_id", "")
                if level.startswith("neutral_"):
                    stop_actions.append(StopExecutorAction(
                        controller_id=self.config.id, executor_id=p.id))

        # 4) Kill executors stuck in a close-retry loop (e.g. position was
        #    closed manually on the exchange so reduce_only orders keep failing)
        shutting_down = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.is_active and x.status == RunnableStatus.SHUTTING_DOWN,
        )
        for ex in shutting_down:
            retries = ex.custom_info.get("current_retries", 0)
            max_retries = ex.custom_info.get("max_retries", 5)
            if retries >= max_retries:
                self.logger().warning(
                    f"Force-stopping executor {ex.id}: stuck in close-retry loop "
                    f"({retries}/{max_retries} retries). Position may have been "
                    f"closed externally."
                )
                stop_actions.append(StopExecutorAction(
                    controller_id=self.config.id, executor_id=ex.id))

        return stop_actions

    # -- helpers ---------------------------------------------------- #

    def _make_create_action(
        self, side: TradeType, price: Decimal, amount: Decimal, level_id: str,
    ) -> CreateExecutorAction:
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=PositionExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=side,
                entry_price=price,
                amount=amount,
                triple_barrier_config=self.config.triple_barrier_config,
                leverage=self.config.leverage,
                level_id=level_id,
            ),
        )

    def _has_pending_side(self, pending, side: TradeType) -> bool:
        return any(p.side == side for p in pending)

    def _cooldown_ok(self) -> bool:
        last_closed = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: not x.is_active,
        )
        if not last_closed:
            return True
        max_close_ts = max(
            (e.close_timestamp for e in last_closed if e.close_timestamp), default=0)
        return self.market_data_provider.time() - max_close_ts > self.config.cooldown_time

    # -- status display --------------------------------------------- #

    def to_format_status(self) -> List[str]:
        lines: List[str] = []
        signal = self.processed_data.get("signal", 0)
        signal_label = {1: "LONG", -1: "SHORT"}.get(signal, "NEUTRAL")
        lines.append(f"  Signal: {signal_label} ({signal})")

        df = self.processed_data.get("features", pd.DataFrame())
        if not df.empty:
            cols = ["timestamp", "close", "stoch_k_14", "rsi_5", "atr_7",
                    "macd_line", "vwap_dist_30", "roc_15", "signal"]
            show = [c for c in cols if c in df.columns]
            lines.append(format_df_for_printout(df[show].tail(5), table_format="psql"))
        return lines
