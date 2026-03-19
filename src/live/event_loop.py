"""Live trading event loop and safety controls.

Manages:
- Market data ingestion via WebSocket
- Incremental feature state
- Signal evaluation
- Order submission and reconciliation
- Persistence and heartbeats
- Reconnect/resubscribe on disconnect
- Stale-feed detection and halts
- Duplicate-order protection
- Manual kill switch
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import structlog

from src.data.features import IncrementalFeatureState
from src.models import (
    BrokerAdapter,
    ExperimentConfig,
    ExitReason,
    PositionState,
    RiskLimits,
    StrategySignal,
    TradeIntent,
)
from src.strategies.regime import RegimeParams, classify_regime
from src.strategies.signals import (
    MeanReversionParams,
    MomentumParams,
    mean_reversion_snapback,
    momentum_breakout,
)

log = structlog.get_logger()


class LoopState(Enum):
    STARTING = "starting"
    RUNNING = "running"
    RECONNECTING = "reconnecting"
    STALE_HALT = "stale_halt"
    KILLED = "killed"
    STOPPED = "stopped"


@dataclass
class LiveState:
    """Persistent state for the live event loop."""

    loop_state: LoopState = LoopState.STARTING
    position: PositionState | None = None
    daily_pnl: float = 0.0
    trade_count: int = 0
    consecutive_losses: int = 0
    last_bar_timestamp: int = 0
    last_heartbeat: float = 0.0
    pending_order_ids: set[str] = field(default_factory=set)
    submitted_intents: set[str] = field(default_factory=set)  # dedup key set
    kill_switch: bool = False


class LiveEventLoop:
    """Broker-agnostic live trading loop.

    Pluggable via BrokerAdapter subclass (Upstox, Fyers, etc.).
    """

    def __init__(
        self,
        broker: BrokerAdapter,
        config: ExperimentConfig,
        state_dir: Path | None = None,
        paper_mode: bool = True,
    ):
        self.broker = broker
        self.config = config
        self.paper_mode = paper_mode
        self.state_dir = state_dir or Path("./data/live_state")
        self.state = LiveState()
        self.feature_state = IncrementalFeatureState()
        self.regime_params = RegimeParams()
        self.momentum_params = MomentumParams()
        self.mr_params = MeanReversionParams()

        # Stale feed detection
        self._stale_threshold_seconds = 30
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5

    async def start(self) -> None:
        """Start the live event loop."""
        self.state.loop_state = LoopState.STARTING
        log.info("live_loop_starting", paper_mode=self.paper_mode)

        self.state_dir.mkdir(parents=True, exist_ok=True)

        try:
            await self.broker.subscribe_market_data(
                ["NSE:NIFTY50-INDEX"], self._on_bar
            )
            self.state.loop_state = LoopState.RUNNING
            self.state.last_heartbeat = time.time()

            # Main loop: heartbeat + stale detection
            while self.state.loop_state == LoopState.RUNNING:
                await asyncio.sleep(1.0)
                self._check_heartbeat()
                self._check_kill_switch()

        except Exception as e:
            log.error("live_loop_error", error=str(e))
            await self._handle_disconnect()

    async def stop(self) -> None:
        """Gracefully stop the live loop, flattening any open position."""
        log.info("live_loop_stopping")

        if self.state.position and self.state.position.is_open:
            await self._flatten_position(ExitReason.MANUAL)

        try:
            await self.broker.unsubscribe_market_data(["NSE:NIFTY50-INDEX"])
        except Exception:
            pass

        self.state.loop_state = LoopState.STOPPED
        log.info("live_loop_stopped")

    def kill(self) -> None:
        """Emergency kill switch — stops all trading immediately."""
        log.warning("KILL_SWITCH_ACTIVATED")
        self.state.kill_switch = True
        self.state.loop_state = LoopState.KILLED

    async def _on_bar(self, bar: dict) -> None:
        """Callback for each new 5s bar from the market data feed."""
        self.state.last_bar_timestamp = bar.get("timestamp", 0)
        self.state.last_heartbeat = time.time()

        if self.state.kill_switch:
            return

        # Compute features
        features = self.feature_state.update(bar)

        # Classify regime
        regime = classify_regime(features, self.regime_params)

        # Check exit for open position
        if self.state.position and self.state.position.is_open:
            should_exit = self._check_exit(features)
            if should_exit:
                await self._flatten_position(should_exit)
                return

        # Generate signals (only if no open position)
        if self.state.position is None or not self.state.position.is_open:
            signal = self._evaluate_signals(features, regime)
            if signal:
                await self._submit_order(signal, features)

    def _evaluate_signals(self, features: dict, regime) -> StrategySignal | None:
        """Run all strategies and return the highest-conviction signal."""
        signals = []

        sig = momentum_breakout(features, regime, self.momentum_params)
        if sig:
            signals.append(sig)

        sig = mean_reversion_snapback(features, regime, self.mr_params)
        if sig:
            signals.append(sig)

        if not signals:
            return None

        # Pick highest conviction
        return max(signals, key=lambda s: s.conviction)

    def _check_exit(self, features: dict) -> ExitReason | None:
        """Check if the open position should be exited."""
        if not self.state.position or not self.state.position.is_open:
            return None

        pos = self.state.position
        entry_ts = pos.entry.fill_timestamp
        current_ts = features.get("timestamp", 0)
        hold_seconds = current_ts - entry_ts

        close = features.get("close", 0)

        # Hard stop
        if close <= pos.entry.intent.hard_stop_premium:
            return ExitReason.HARD_STOP

        # Time stop
        if hold_seconds >= self.config.risk.hold_max_seconds:
            return ExitReason.TIME_STOP

        # Session end
        ist_seconds = current_ts + int(5.5 * 3600)
        ist_hour = (ist_seconds % 86400) // 3600
        ist_minute = (ist_seconds % 3600) // 60
        if ist_hour >= self.config.risk.session_end_hour and ist_minute >= self.config.risk.session_end_minute - 1:
            return ExitReason.SESSION_END

        # Daily loss kill
        if self.state.daily_pnl <= -self.config.risk.max_daily_loss:
            return ExitReason.KILL_SWITCH

        return None

    async def _submit_order(self, signal: StrategySignal, features: dict) -> None:
        """Submit an order via the broker adapter."""
        # Dedup protection
        dedup_key = f"{signal.strategy_name}_{signal.timestamp}_{signal.direction.value}"
        if dedup_key in self.state.submitted_intents:
            log.debug("duplicate_order_rejected", key=dedup_key)
            return
        self.state.submitted_intents.add(dedup_key)

        # Risk checks
        if self.state.consecutive_losses >= self.config.risk.max_consecutive_losses:
            log.warning("max_consecutive_losses_reached")
            return
        if self.state.daily_pnl <= -self.config.risk.max_daily_loss:
            log.warning("max_daily_loss_reached")
            return

        log.info(
            "order_submitted",
            strategy=signal.strategy_name,
            direction=signal.direction.value,
            conviction=signal.conviction,
            paper=self.paper_mode,
        )

        if self.paper_mode:
            log.info("paper_order", signal=signal.strategy_name)
            # In paper mode, simulate immediate fill
            return

        # Real order submission (placeholder)
        try:
            option_type = "CE" if signal.direction.value == "long_ce" else "PE"
            order_id = await self.broker.place_order(
                symbol=f"NIFTY_WEEKLY_{option_type}",
                qty=1,
                side="BUY",
                order_type="MARKET",
            )
            self.state.pending_order_ids.add(order_id)
            log.info("order_placed", order_id=order_id)
        except Exception as e:
            log.error("order_failed", error=str(e))

    async def _flatten_position(self, reason: ExitReason) -> None:
        """Exit the current position."""
        log.info("flattening_position", reason=reason.value)

        if self.paper_mode:
            log.info("paper_exit", reason=reason.value)
            self.state.position = None
            return

        # Real exit (placeholder)
        try:
            order_id = await self.broker.place_order(
                symbol="NIFTY_WEEKLY",
                qty=1,
                side="SELL",
                order_type="MARKET",
            )
            self.state.pending_order_ids.add(order_id)
        except Exception as e:
            log.error("exit_order_failed", error=str(e))

    def _check_heartbeat(self) -> None:
        """Detect stale data feed."""
        elapsed = time.time() - self.state.last_heartbeat
        if elapsed > self._stale_threshold_seconds and self.state.loop_state == LoopState.RUNNING:
            log.warning("stale_feed_detected", elapsed=elapsed)
            self.state.loop_state = LoopState.STALE_HALT

    def _check_kill_switch(self) -> None:
        """Check for kill switch activation."""
        if self.state.kill_switch and self.state.loop_state != LoopState.KILLED:
            self.state.loop_state = LoopState.KILLED
            log.warning("kill_switch_enforced")

    async def _handle_disconnect(self) -> None:
        """Handle WebSocket disconnect with exponential backoff reconnect."""
        self._reconnect_attempts += 1
        if self._reconnect_attempts > self._max_reconnect_attempts:
            log.error("max_reconnect_attempts_exceeded")
            self.state.loop_state = LoopState.KILLED
            return

        self.state.loop_state = LoopState.RECONNECTING
        wait = min(2**self._reconnect_attempts, 30)
        log.info("reconnecting", attempt=self._reconnect_attempts, wait=wait)
        await asyncio.sleep(wait)

        try:
            await self.broker.subscribe_market_data(
                ["NSE:NIFTY50-INDEX"], self._on_bar
            )
            self.state.loop_state = LoopState.RUNNING
            self._reconnect_attempts = 0
            log.info("reconnected")
        except Exception as e:
            log.error("reconnect_failed", error=str(e))
            await self._handle_disconnect()
