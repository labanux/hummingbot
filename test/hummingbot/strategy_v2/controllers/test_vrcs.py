import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

import pandas as pd

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

from controllers.directional_trading.vrcs import VRCSConfig, VRCSController


class TestVRCSController(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        self.config = VRCSConfig(
            id="test_vrcs",
            controller_name="vrcs",
            connector_name="lighter_perpetual",
            trading_pair="LIT-USD",
            total_amount_quote=Decimal("50"),
            buy_spread=Decimal("0.0005"),
            sell_spread=Decimal("0.0005"),
            candles_connector="lighter_perpetual",
            candles_trading_pair="LIT-USD",
            interval="1m",
            executor_refresh_time=10,
            cooldown_time=3,
            max_executors_per_side=1,
            leverage=1,
            stop_loss=Decimal("0.001"),
            take_profit=Decimal("0.0008"),
            time_limit=300,
            take_profit_order_type=OrderType.MARKET,
        )
        self.mock_market_data_provider = MagicMock(spec=MarketDataProvider)
        self.mock_market_data_provider.time.return_value = 1000000.0
        self.mock_market_data_provider.get_price_by_type.return_value = Decimal("1.0")
        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)
        self.controller = VRCSController(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )
        self.controller.processed_data = {"signal": 0, "features": pd.DataFrame()}

    def _make_executor(self, executor_id="ex1", is_active=True, is_trading=False,
                       status=RunnableStatus.RUNNING, side=TradeType.BUY,
                       timestamp=999980.0, close_timestamp=None,
                       custom_info=None):
        mock = MagicMock(spec=ExecutorInfo)
        mock.id = executor_id
        mock.is_active = is_active
        mock.is_trading = is_trading
        mock.status = status
        mock.side = side
        mock.timestamp = timestamp
        mock.close_timestamp = close_timestamp
        mock.custom_info = custom_info or {"level_id": "", "current_retries": 0, "max_retries": 5}
        mock.controller_id = self.config.id
        return mock

    # ------------------------------------------------------------------ #
    #  Tests for stuck executor detection (stop rule #4)                  #
    # ------------------------------------------------------------------ #

    def test_stop_actions_kills_stuck_shutting_down_executor(self):
        """Executor stuck in close-retry loop (retries >= max) should be force-stopped."""
        stuck = self._make_executor(
            executor_id="stuck_ex",
            is_active=True,
            is_trading=True,
            status=RunnableStatus.SHUTTING_DOWN,
            custom_info={"level_id": "trend_long", "current_retries": 5, "max_retries": 5},
        )
        self.controller.executors_info = [stuck]
        actions = self.controller.stop_actions_proposal()
        stop_ids = [a.executor_id for a in actions if isinstance(a, StopExecutorAction)]
        self.assertIn("stuck_ex", stop_ids)

    def test_stop_actions_does_not_kill_healthy_shutting_down_executor(self):
        """Executor shutting down with retries below max should NOT be force-stopped."""
        healthy = self._make_executor(
            executor_id="healthy_ex",
            is_active=True,
            is_trading=True,
            status=RunnableStatus.SHUTTING_DOWN,
            custom_info={"level_id": "trend_long", "current_retries": 2, "max_retries": 5},
        )
        self.controller.executors_info = [healthy]
        actions = self.controller.stop_actions_proposal()
        stop_ids = [a.executor_id for a in actions if isinstance(a, StopExecutorAction)]
        self.assertNotIn("healthy_ex", stop_ids)

    def test_stop_actions_does_not_kill_running_executor(self):
        """A normally running executor should NOT be force-stopped, even with high retries."""
        running = self._make_executor(
            executor_id="running_ex",
            is_active=True,
            is_trading=True,
            status=RunnableStatus.RUNNING,
            custom_info={"level_id": "trend_long", "current_retries": 10, "max_retries": 5},
        )
        self.controller.executors_info = [running]
        actions = self.controller.stop_actions_proposal()
        stop_ids = [a.executor_id for a in actions if isinstance(a, StopExecutorAction)]
        self.assertNotIn("running_ex", stop_ids)

    def test_stop_actions_stuck_executor_with_missing_retry_info(self):
        """Executor with no retry info in custom_info uses defaults and should NOT be killed."""
        no_info = self._make_executor(
            executor_id="no_info_ex",
            is_active=True,
            is_trading=True,
            status=RunnableStatus.SHUTTING_DOWN,
            custom_info={"level_id": "trend_long"},
        )
        self.controller.executors_info = [no_info]
        actions = self.controller.stop_actions_proposal()
        stop_ids = [a.executor_id for a in actions if isinstance(a, StopExecutorAction)]
        # current_retries defaults to 0, max_retries defaults to 5 → 0 < 5 → not killed
        self.assertNotIn("no_info_ex", stop_ids)

    # ------------------------------------------------------------------ #
    #  Tests for existing stop rules                                      #
    # ------------------------------------------------------------------ #

    def test_stop_actions_cancels_pending_when_position_active(self):
        """Rule 1: When a position is filled, cancel all unfilled (pending) orders."""
        active = self._make_executor("active_ex", is_active=True, is_trading=True)
        pending = self._make_executor("pending_ex", is_active=True, is_trading=False, timestamp=999999.0)
        self.controller.executors_info = [active, pending]
        actions = self.controller.stop_actions_proposal()
        stop_ids = [a.executor_id for a in actions if isinstance(a, StopExecutorAction)]
        self.assertIn("pending_ex", stop_ids)

    def test_stop_actions_refreshes_stale_pending(self):
        """Rule 2: Cancel pending orders older than executor_refresh_time."""
        stale = self._make_executor(
            "stale_ex", is_active=True, is_trading=False,
            timestamp=999980.0,  # age = 1000000 - 999980 = 20 > 10
        )
        self.controller.executors_info = [stale]
        actions = self.controller.stop_actions_proposal()
        stop_ids = [a.executor_id for a in actions if isinstance(a, StopExecutorAction)]
        self.assertIn("stale_ex", stop_ids)

    def test_stop_actions_cancels_neutral_on_signal_change(self):
        """Rule 3: When signal changes to trend, cancel pending neutral orders."""
        self.controller.processed_data["signal"] = 1
        neutral = self._make_executor(
            "neutral_ex", is_active=True, is_trading=False,
            timestamp=999999.0,  # fresh enough to not trigger refresh
            custom_info={"level_id": "neutral_buy", "current_retries": 0, "max_retries": 5},
        )
        self.controller.executors_info = [neutral]
        actions = self.controller.stop_actions_proposal()
        stop_ids = [a.executor_id for a in actions if isinstance(a, StopExecutorAction)]
        self.assertIn("neutral_ex", stop_ids)

    # ------------------------------------------------------------------ #
    #  Tests for create_actions_proposal                                  #
    # ------------------------------------------------------------------ #

    def test_create_actions_long_signal(self):
        """Signal=1 should create a BUY order."""
        self.controller.processed_data["signal"] = 1
        self.controller.executors_info = []
        actions = self.controller.create_actions_proposal()
        self.assertEqual(len(actions), 1)
        self.assertIsInstance(actions[0], CreateExecutorAction)
        self.assertEqual(actions[0].executor_config.side, TradeType.BUY)

    def test_create_actions_short_signal(self):
        """Signal=-1 should create a SELL order."""
        self.controller.processed_data["signal"] = -1
        self.controller.executors_info = []
        actions = self.controller.create_actions_proposal()
        self.assertEqual(len(actions), 1)
        self.assertIsInstance(actions[0], CreateExecutorAction)
        self.assertEqual(actions[0].executor_config.side, TradeType.SELL)

    def test_create_actions_neutral_signal(self):
        """Signal=0 should create both BUY and SELL orders."""
        self.controller.processed_data["signal"] = 0
        self.controller.executors_info = []
        actions = self.controller.create_actions_proposal()
        self.assertEqual(len(actions), 2)
        sides = {a.executor_config.side for a in actions}
        self.assertEqual(sides, {TradeType.BUY, TradeType.SELL})

    def test_create_actions_blocked_by_active_position(self):
        """No new orders when there's an active position."""
        active = self._make_executor("active", is_active=True, is_trading=True)
        self.controller.executors_info = [active]
        self.controller.processed_data["signal"] = 1
        actions = self.controller.create_actions_proposal()
        self.assertEqual(len(actions), 0)
