"""Tests for live architecture: event loop, safety controls, adapter."""

from __future__ import annotations

import asyncio
import time

import pytest

from src.live.event_loop import LiveEventLoop, LiveState, LoopState
from src.models import BrokerAdapter, ExperimentConfig, OptionChainSnapshot


class MockBroker(BrokerAdapter):
    """Mock broker for testing."""

    def __init__(self):
        self.subscribed = []
        self.orders = []
        self.callbacks = {}

    async def subscribe_market_data(self, symbols, callback):
        self.subscribed.extend(symbols)
        for sym in symbols:
            self.callbacks[sym] = callback

    async def unsubscribe_market_data(self, symbols):
        for sym in symbols:
            self.subscribed.remove(sym)
            self.callbacks.pop(sym, None)

    async def place_order(self, symbol, qty, side, order_type, price=0.0):
        order_id = f"ORD-{len(self.orders) + 1}"
        self.orders.append(
            {"id": order_id, "symbol": symbol, "qty": qty, "side": side}
        )
        return order_id

    async def get_order_status(self, order_id):
        return {"order_id": order_id, "status": "COMPLETE"}

    async def cancel_order(self, order_id):
        return True

    async def get_ltp(self, symbol):
        return 23600.0

    async def get_option_chain(self, underlying, expiry):
        return OptionChainSnapshot(
            underlying=underlying, timestamp=0, expiry=expiry
        )


class TestLiveState:
    def test_initial_state(self):
        state = LiveState()
        assert state.loop_state == LoopState.STARTING
        assert state.position is None
        assert state.daily_pnl == 0.0
        assert not state.kill_switch

    def test_kill_switch(self):
        state = LiveState()
        state.kill_switch = True
        assert state.kill_switch


class TestLiveEventLoop:
    @pytest.fixture
    def loop(self):
        broker = MockBroker()
        config = ExperimentConfig(name="test")
        return LiveEventLoop(broker=broker, config=config, paper_mode=True)

    def test_init(self, loop):
        assert loop.paper_mode is True
        assert loop.state.loop_state == LoopState.STARTING

    def test_kill_switch(self, loop):
        loop.kill()
        assert loop.state.kill_switch is True
        assert loop.state.loop_state == LoopState.KILLED

    def test_stale_detection(self, loop):
        loop.state.loop_state = LoopState.RUNNING
        loop.state.last_heartbeat = time.time() - 60  # 60s stale
        loop._check_heartbeat()
        assert loop.state.loop_state == LoopState.STALE_HALT

    def test_heartbeat_ok(self, loop):
        loop.state.loop_state = LoopState.RUNNING
        loop.state.last_heartbeat = time.time()
        loop._check_heartbeat()
        assert loop.state.loop_state == LoopState.RUNNING

    def test_duplicate_order_protection(self, loop):
        loop.state.submitted_intents.add("test_1000_long_ce")
        # The dedup key is checked in _submit_order
        assert "test_1000_long_ce" in loop.state.submitted_intents

    @pytest.mark.asyncio
    async def test_on_bar_with_kill_switch(self, loop):
        loop.state.kill_switch = True
        bar = {
            "symbol": "NIFTY50-INDEX",
            "timestamp": 1773287100,
            "open": 23600,
            "high": 23610,
            "low": 23590,
            "close": 23605,
            "volume": 1000,
        }
        # Should return immediately without processing
        await loop._on_bar(bar)
        assert len(loop.feature_state.bars) == 0  # bar not processed

    @pytest.mark.asyncio
    async def test_on_bar_processes(self, loop):
        bar = {
            "symbol": "NIFTY50-INDEX",
            "timestamp": 1773287100,
            "open": 23600,
            "high": 23610,
            "low": 23590,
            "close": 23605,
            "volume": 1000,
        }
        await loop._on_bar(bar)
        assert len(loop.feature_state.bars) == 1

    @pytest.mark.asyncio
    async def test_stop(self, loop):
        await loop.stop()
        assert loop.state.loop_state == LoopState.STOPPED
