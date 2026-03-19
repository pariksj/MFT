"""Event-driven backtesting simulator.

Simulates trading with:
- Bar-close decisions, next-bar execution
- Conservative fees, slippage, and spread assumptions
- Thin-contract rejection
- Hard stop, time stop, session-end flattening
- Cooldowns, max daily loss, max consecutive-loss kill
- One position at a time, no overnight carry
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import structlog

from src.models import (
    ExitReason,
    ExperimentConfig,
    FillEvent,
    PositionState,
    RiskLimits,
    SignalDirection,
    StrategySignal,
    TradeIntent,
)

log = structlog.get_logger()


@dataclass
class DailyState:
    """Tracks daily risk state."""

    date: str
    realized_pnl: float = 0.0
    trade_count: int = 0
    consecutive_losses: int = 0
    last_exit_timestamp: int = 0
    killed: bool = False
    kill_reason: str = ""


@dataclass
class SimulatorResult:
    """Output of a simulation run."""

    trades: list[PositionState] = field(default_factory=list)
    daily_states: list[DailyState] = field(default_factory=list)
    signals_generated: int = 0
    signals_rejected: int = 0
    rejection_reasons: dict = field(default_factory=dict)


def simulate_fill(
    intent: TradeIntent,
    execution_bar: dict,
    config: ExperimentConfig,
) -> FillEvent:
    """Simulate a fill on the next bar after signal.

    Uses pessimistic assumptions:
    - Entry at bar open + half spread + slippage
    - Fees per lot
    """
    open_price = execution_bar.get("open", intent.entry_premium)

    # Pessimistic fill: pay half the spread + slippage above open
    slippage = open_price * config.slippage_pct
    fill_price = open_price + intent.estimated_spread / 2 + slippage

    return FillEvent(
        intent=intent,
        fill_timestamp=execution_bar["timestamp"],
        fill_price=fill_price,
        fees=config.fee_per_lot,
        slippage=slippage,
    )


def simulate_exit(
    position: PositionState,
    exit_bar: dict,
    reason: ExitReason,
    config: ExperimentConfig,
) -> PositionState:
    """Simulate an exit fill."""
    close_price = exit_bar.get("close", position.entry.fill_price)

    # Pessimistic exit: receive close - half spread - slippage
    slippage = close_price * config.slippage_pct
    fill_price = max(0.05, close_price - position.entry.intent.estimated_spread / 2 - slippage)

    exit_fill = FillEvent(
        intent=position.entry.intent,
        fill_timestamp=exit_bar["timestamp"],
        fill_price=fill_price,
        fees=config.fee_per_lot,
        slippage=slippage,
    )

    return PositionState(
        entry=position.entry,
        current_premium=fill_price,
        unrealized_pnl=0.0,
        bars_held=position.bars_held,
        exit=exit_fill,
        exit_reason=reason,
    )


def check_exit_conditions(
    position: PositionState,
    current_bar: dict,
    config: ExperimentConfig,
) -> ExitReason | None:
    """Check if an open position should be exited.

    Returns the exit reason, or None if position should remain open.
    """
    entry_ts = position.entry.fill_timestamp
    current_ts = current_bar["timestamp"]
    hold_seconds = current_ts - entry_ts
    current_close = current_bar.get("close", 0)

    # Hard stop: premium dropped below stop level
    hard_stop = position.entry.intent.hard_stop_premium
    if current_close <= hard_stop:
        return ExitReason.HARD_STOP

    # Time stop: held too long
    if hold_seconds >= config.risk.hold_max_seconds:
        return ExitReason.TIME_STOP

    # Session end flattening
    hour = (current_ts % 86400) // 3600
    minute = (current_ts % 3600) // 60
    # Adjust for IST (UTC+5:30)
    ist_seconds = current_ts + 5 * 3600 + 30 * 60
    ist_hour = (ist_seconds % 86400) // 3600
    ist_minute = (ist_seconds % 3600) // 60
    if (
        ist_hour > config.risk.session_end_hour
        or (ist_hour == config.risk.session_end_hour and ist_minute >= config.risk.session_end_minute - 1)
    ):
        return ExitReason.SESSION_END

    return None


def can_enter(
    daily: DailyState,
    current_ts: int,
    config: ExperimentConfig,
) -> tuple[bool, str]:
    """Check if a new entry is allowed given daily risk state.

    Returns (allowed, reason_if_rejected).
    """
    if daily.killed:
        return False, f"daily_killed:{daily.kill_reason}"

    # Max daily loss
    if daily.realized_pnl <= -config.risk.max_daily_loss:
        daily.killed = True
        daily.kill_reason = "max_daily_loss"
        return False, "max_daily_loss"

    # Max consecutive losses
    if daily.consecutive_losses >= config.risk.max_consecutive_losses:
        daily.killed = True
        daily.kill_reason = "max_consecutive_losses"
        return False, "max_consecutive_losses"

    # Cooldown after loss
    if daily.last_exit_timestamp > 0:
        cooldown = current_ts - daily.last_exit_timestamp
        if cooldown < config.risk.cooldown_after_loss_seconds and daily.consecutive_losses > 0:
            return False, f"cooldown:{cooldown}s"

    # No entry in last 15 minutes
    ist_seconds = current_ts + 5 * 3600 + 30 * 60
    ist_hour = (ist_seconds % 86400) // 3600
    ist_minute = (ist_seconds % 3600) // 60
    if (
        ist_hour > config.risk.no_entry_after_hour
        or (ist_hour == config.risk.no_entry_after_hour and ist_minute >= config.risk.no_entry_after_minute)
    ):
        return False, "session_end_cutoff"

    return True, ""


def run_simulation(
    signals_by_timestamp: dict[int, list[TradeIntent]],
    option_bars: pd.DataFrame,
    config: ExperimentConfig,
) -> SimulatorResult:
    """Run the event-driven simulation.

    Args:
        signals_by_timestamp: Map of timestamp → list of TradeIntents (from signal + contract selection).
        option_bars: DataFrame of option bars with columns [timestamp, strike, option_type, open, high, low, close, volume].
            Used for execution fills and mark-to-market.
        config: Experiment configuration.

    Returns:
        SimulatorResult with all trades and daily states.
    """
    result = SimulatorResult()

    if option_bars.empty:
        log.warning("simulation_no_option_bars")
        return result

    # Group option bars by date
    timestamps = sorted(option_bars["timestamp"].unique())
    if not timestamps:
        return result

    # Simple date extraction: group by calendar day (IST)
    option_bars = option_bars.copy()
    option_bars["date"] = pd.to_datetime(option_bars["timestamp"] + 5.5 * 3600, unit="s").dt.date.astype(str)

    dates = sorted(option_bars["date"].unique())

    for date_str in dates:
        daily = DailyState(date=date_str)
        day_bars = option_bars[option_bars["date"] == date_str].sort_values("timestamp")
        day_timestamps = sorted(day_bars["timestamp"].unique())

        position: PositionState | None = None

        for i, ts in enumerate(day_timestamps):
            current_bar = day_bars[day_bars["timestamp"] == ts].iloc[0].to_dict()

            # Check exit for open position
            if position is not None and position.is_open:
                exit_reason = check_exit_conditions(position, current_bar, config)
                if exit_reason is not None:
                    position = simulate_exit(position, current_bar, exit_reason, config)
                    result.trades.append(position)

                    # Update daily state
                    pnl = position.realized_pnl
                    daily.realized_pnl += pnl
                    daily.trade_count += 1
                    daily.last_exit_timestamp = ts
                    if pnl < 0:
                        daily.consecutive_losses += 1
                    else:
                        daily.consecutive_losses = 0

                    position = None

            # Try to enter on signal (bar-close decision, next-bar execution)
            if position is None and ts in signals_by_timestamp:
                intents = signals_by_timestamp[ts]
                result.signals_generated += len(intents)

                for intent in intents:
                    allowed, reject_reason = can_enter(daily, ts, config)
                    if not allowed:
                        result.signals_rejected += 1
                        result.rejection_reasons[reject_reason] = (
                            result.rejection_reasons.get(reject_reason, 0) + 1
                        )
                        continue

                    # Execute on next bar
                    if i + 1 < len(day_timestamps):
                        next_ts = day_timestamps[i + 1]
                        next_bar = day_bars[day_bars["timestamp"] == next_ts].iloc[0].to_dict()
                        entry_fill = simulate_fill(intent, next_bar, config)
                        position = PositionState(
                            entry=entry_fill,
                            current_premium=entry_fill.fill_price,
                        )
                        break  # only one position at a time
                    else:
                        result.signals_rejected += 1
                        result.rejection_reasons["no_next_bar"] = (
                            result.rejection_reasons.get("no_next_bar", 0) + 1
                        )

        # Session-end: flatten any remaining position
        if position is not None and position.is_open and day_timestamps:
            last_bar = day_bars[day_bars["timestamp"] == day_timestamps[-1]].iloc[0].to_dict()
            position = simulate_exit(position, last_bar, ExitReason.SESSION_END, config)
            result.trades.append(position)
            daily.realized_pnl += position.realized_pnl
            daily.trade_count += 1

        result.daily_states.append(daily)

    return result
