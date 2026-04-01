"""
VRCS Hedge – Delta-neutral hedge companion for VRCS

Runs alongside the VRCS controller in the same hummingbot process.
When VRCS opens a position (limit fill detected), this controller
immediately opens the opposite position via market order on a second
connector (subaccount B) with the same dollar amount.

Each side manages its own exit via triple barrier (TP / SL / time-limit).
Net P&L per cycle = TP − SL (deterministic when fees are zero).

Requirements:
  - A second connector instance (e.g. lighter_perpetual_b) configured
    with subaccount B credentials.
  - The master_controller_id must match the VRCS controller's config id.
  - The strategy script must inject master executor state into
    self.master_executors_info before determine_executor_actions runs.
"""
from decimal import Decimal
from typing import List

from pydantic import Field

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
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
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


# ------------------------------------------------------------------ #
#  Config                                                             #
# ------------------------------------------------------------------ #

class VRCSHedgeConfig(ControllerConfigBase):
    controller_name: str = "vrcs_hedge"
    controller_type: str = "directional_trading"

    connector_name: str = Field(
        json_schema_extra={
            "prompt": "Enter the hedge connector name (e.g. lighter_perpetual_b): ",
            "prompt_on_new": True})
    trading_pair: str = Field(
        json_schema_extra={
            "prompt": "Enter the trading pair (e.g. LIT-USD): ",
            "prompt_on_new": True})
    total_amount_quote: Decimal = Field(
        default=Decimal("120"),
        json_schema_extra={
            "prompt": "Enter the total amount in quote for the hedge: ",
            "prompt_on_new": True, "is_updatable": True})
    leverage: int = Field(default=2)
    position_mode: str = Field(default="ONEWAY")

    master_controller_id: str = Field(
        json_schema_extra={
            "prompt": "Enter the master (VRCS) controller config id: ",
            "prompt_on_new": True})

    stop_loss: Decimal = Field(default=Decimal("0.0007"))
    take_profit: Decimal = Field(default=Decimal("0.0008"))
    time_limit: int = Field(default=300)
    take_profit_order_type: OrderType = Field(default=OrderType.MARKET)

    @property
    def triple_barrier_config(self) -> TripleBarrierConfig:
        return TripleBarrierConfig(
            stop_loss=self.stop_loss,
            take_profit=self.take_profit,
            time_limit=self.time_limit,
            open_order_type=OrderType.MARKET,
            take_profit_order_type=self.take_profit_order_type,
            stop_loss_order_type=OrderType.MARKET,
            time_limit_order_type=OrderType.MARKET,
        )

    def update_markets(self, markets):
        return markets.add_or_update(self.connector_name, self.trading_pair)


# ------------------------------------------------------------------ #
#  Controller                                                         #
# ------------------------------------------------------------------ #

class VRCSHedgeController(ControllerBase):

    def __init__(self, config: VRCSHedgeConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)
        # Populated by the strategy script each tick with the master's executors
        self.master_executors_info: List[ExecutorInfo] = []

    def _get_master_state(self):
        """Read master state directly from master_executors_info (fresh, not via processed_data).

        A master executor counts as "has position" if it's actively trading OR
        if it's in SHUTTING_DOWN state (closing via TP/SL but position still open).
        """
        master_with_position = [
            e for e in self.master_executors_info
            if e.is_active and (e.is_trading or e.status == RunnableStatus.SHUTTING_DOWN)
        ]
        if master_with_position:
            return True, master_with_position[0].side
        return False, None

    async def update_processed_data(self):
        # Store for status display only
        has_pos, side = self._get_master_state()
        self.processed_data["master_has_position"] = has_pos
        self.processed_data["master_side"] = side

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        actions.extend(self.stop_actions_proposal())
        actions.extend(self.create_actions_proposal())
        return actions

    def create_actions_proposal(self) -> List[ExecutorAction]:
        master_has_position, master_side = self._get_master_state()

        if not master_has_position or master_side is None:
            return []

        # Already have an active or closing hedge — nothing to do
        my_active = [e for e in self.executors_info
                     if e.is_active and (e.is_trading or e.status == RunnableStatus.SHUTTING_DOWN)]
        my_pending = [e for e in self.executors_info
                     if e.is_active and not e.is_trading and e.status != RunnableStatus.SHUTTING_DOWN]
        if my_active or my_pending:
            return []

        opposite_side = TradeType.SELL if master_side == TradeType.BUY else TradeType.BUY
        mid = self.market_data_provider.get_price_by_type(
            self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)
        amount = self.config.total_amount_quote / mid

        return [CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=PositionExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=opposite_side,
                amount=amount,
                triple_barrier_config=self.config.triple_barrier_config,
                leverage=self.config.leverage,
                level_id="hedge",
            ),
        )]

    def stop_actions_proposal(self) -> List[ExecutorAction]:
        stop_actions = []
        master_has_position, master_side = self._get_master_state()

        my_active = [e for e in self.executors_info if e.is_active and e.is_trading]
        my_pending = [e for e in self.executors_info if e.is_active and not e.is_trading]

        # 1) Master side changed → hedge is on wrong side, force-close
        #    so a correct hedge can open on the next tick.
        #    When master has no position, let Bot B exit via its own triple barrier
        #    to preserve the TP-SL edge.
        if master_has_position and master_side is not None:
            expected_hedge_side = TradeType.SELL if master_side == TradeType.BUY else TradeType.BUY
            for e in my_active:
                if e.side != expected_hedge_side:
                    stop_actions.append(StopExecutorAction(
                        controller_id=self.config.id, executor_id=e.id))

        # 2) Cancel stale pending hedge orders that never filled
        for p in my_pending:
            age = self.market_data_provider.time() - p.timestamp
            if age > 10:
                stop_actions.append(StopExecutorAction(
                    controller_id=self.config.id, executor_id=p.id))

        return stop_actions

    def to_format_status(self) -> List[str]:
        master_has_pos, master_side = self._get_master_state()
        my_active = [e for e in self.executors_info if e.is_active and e.is_trading]
        lines = [
            f"  Master position: {'YES (' + str(master_side) + ')' if master_has_pos else 'NONE'}",
            f"  Hedge active: {'YES' if my_active else 'NO'}",
        ]
        return lines
