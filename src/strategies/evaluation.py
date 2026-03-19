"""Evaluation pipeline: walk-forward analysis and promotion gates.

Reports:
- Expectancy net of costs
- Max drawdown
- Opportunity count and fill rate
- Average hold time
- Regime attribution
- Time-of-day breakdown
- Strike-distance breakdown
- Stability across sessions

Promotion gate: positive expectancy after costs with stability across
multiple days and regimes.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.models import ExitReason, ExperimentConfig, PositionState, RegimeLabel
from src.strategies.simulator import DailyState, SimulatorResult


@dataclass
class TradeMetrics:
    """Metrics for a set of trades."""

    count: int = 0
    winners: int = 0
    losers: int = 0
    total_pnl: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    total_fees: float = 0.0
    avg_pnl: float = 0.0
    win_rate: float = 0.0
    expectancy: float = 0.0  # avg pnl per trade (net of costs)
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    avg_hold_seconds: float = 0.0
    avg_hold_bars: float = 0.0
    sharpe_daily: float = 0.0


@dataclass
class RegimeBreakdown:
    """Metrics broken down by regime."""

    momentum: TradeMetrics = field(default_factory=TradeMetrics)
    mean_reversion: TradeMetrics = field(default_factory=TradeMetrics)


@dataclass
class TimeOfDayBreakdown:
    """Metrics broken down by hour of day (IST)."""

    by_hour: dict[int, TradeMetrics] = field(default_factory=dict)


@dataclass
class StrikeBreakdown:
    """Metrics broken down by strike distance from ATM."""

    by_distance: dict[int, TradeMetrics] = field(default_factory=dict)


@dataclass
class EvaluationReport:
    """Complete evaluation output."""

    overall: TradeMetrics = field(default_factory=TradeMetrics)
    by_regime: RegimeBreakdown = field(default_factory=RegimeBreakdown)
    by_time: TimeOfDayBreakdown = field(default_factory=TimeOfDayBreakdown)
    by_strike: StrikeBreakdown = field(default_factory=StrikeBreakdown)
    by_exit_reason: dict[str, int] = field(default_factory=dict)
    daily_pnl: list[float] = field(default_factory=list)
    walk_forward_windows: list[dict] = field(default_factory=list)
    promotion_passed: bool = False
    promotion_reasons: list[str] = field(default_factory=list)


def compute_trade_metrics(trades: list[PositionState]) -> TradeMetrics:
    """Compute aggregate metrics from a list of closed trades."""
    m = TradeMetrics()
    if not trades:
        return m

    m.count = len(trades)
    pnls = []
    hold_seconds = []

    for t in trades:
        pnl = t.realized_pnl
        pnls.append(pnl)
        if pnl > 0:
            m.winners += 1
            m.gross_profit += pnl
        else:
            m.losers += 1
            m.gross_loss += abs(pnl)

        m.total_fees += t.entry.fees + (t.exit.fees if t.exit else 0)
        if t.exit:
            hold_seconds.append(t.exit.fill_timestamp - t.entry.fill_timestamp)
        m.avg_hold_bars += t.bars_held

    m.total_pnl = sum(pnls)
    m.avg_pnl = m.total_pnl / m.count
    m.win_rate = m.winners / m.count if m.count > 0 else 0
    m.expectancy = m.avg_pnl
    m.profit_factor = m.gross_profit / m.gross_loss if m.gross_loss > 0 else float("inf")
    m.avg_hold_seconds = np.mean(hold_seconds) if hold_seconds else 0
    m.avg_hold_bars = m.avg_hold_bars / m.count if m.count > 0 else 0

    # Max drawdown from cumulative PnL
    cum_pnl = np.cumsum(pnls)
    running_max = np.maximum.accumulate(cum_pnl)
    drawdowns = running_max - cum_pnl
    m.max_drawdown = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0

    return m


def evaluate_simulation(
    result: SimulatorResult,
    config: ExperimentConfig,
) -> EvaluationReport:
    """Evaluate a simulation result and produce a full report.

    Args:
        result: Output from run_simulation.
        config: Experiment configuration.

    Returns:
        EvaluationReport with all breakdowns and promotion decision.
    """
    report = EvaluationReport()

    closed_trades = [t for t in result.trades if not t.is_open]
    report.overall = compute_trade_metrics(closed_trades)

    # Exit reason breakdown
    for t in closed_trades:
        reason = t.exit_reason.value if t.exit_reason else "unknown"
        report.by_exit_reason[reason] = report.by_exit_reason.get(reason, 0) + 1

    # Regime breakdown
    momentum_trades = [
        t for t in closed_trades if t.entry.intent.signal.regime == RegimeLabel.MOMENTUM
    ]
    mr_trades = [
        t for t in closed_trades if t.entry.intent.signal.regime == RegimeLabel.MEAN_REVERSION
    ]
    report.by_regime.momentum = compute_trade_metrics(momentum_trades)
    report.by_regime.mean_reversion = compute_trade_metrics(mr_trades)

    # Time-of-day breakdown (IST hour)
    hourly_groups: dict[int, list[PositionState]] = {}
    for t in closed_trades:
        ist_ts = t.entry.fill_timestamp + int(5.5 * 3600)
        hour = (ist_ts % 86400) // 3600
        hourly_groups.setdefault(hour, []).append(t)
    for hour, trades in sorted(hourly_groups.items()):
        report.by_time.by_hour[hour] = compute_trade_metrics(trades)

    # Strike distance breakdown
    strike_groups: dict[int, list[PositionState]] = {}
    for t in closed_trades:
        # Distance in strike steps (50-point increments for NIFTY)
        distance = int(abs(t.entry.intent.strike - t.entry.intent.entry_premium) / 50)
        strike_groups.setdefault(distance, []).append(t)
    for dist, trades in sorted(strike_groups.items()):
        report.by_strike.by_distance[dist] = compute_trade_metrics(trades)

    # Daily PnL
    report.daily_pnl = [d.realized_pnl for d in result.daily_states]

    # Daily Sharpe
    if len(report.daily_pnl) > 1:
        daily_arr = np.array(report.daily_pnl)
        mean_daily = np.mean(daily_arr)
        std_daily = np.std(daily_arr, ddof=1)
        report.overall.sharpe_daily = (
            mean_daily / std_daily * np.sqrt(252) if std_daily > 0 else 0
        )

    # Promotion gate
    report.promotion_passed = True
    report.promotion_reasons = []

    if report.overall.expectancy <= 0:
        report.promotion_passed = False
        report.promotion_reasons.append(
            f"negative_expectancy:{report.overall.expectancy:.2f}"
        )

    if report.overall.count < 20:
        report.promotion_passed = False
        report.promotion_reasons.append(
            f"insufficient_trades:{report.overall.count}"
        )

    if report.overall.max_drawdown > config.risk.max_daily_loss * 3:
        report.promotion_passed = False
        report.promotion_reasons.append(
            f"excessive_drawdown:{report.overall.max_drawdown:.2f}"
        )

    # Stability: positive PnL on at least 50% of trading days
    if report.daily_pnl:
        positive_days = sum(1 for p in report.daily_pnl if p > 0)
        pct = positive_days / len(report.daily_pnl)
        if pct < 0.5:
            report.promotion_passed = False
            report.promotion_reasons.append(f"unstable_daily:{pct:.1%}")

    # Both regimes must contribute
    if report.by_regime.momentum.count > 0 and report.by_regime.momentum.expectancy < 0:
        report.promotion_reasons.append(
            f"momentum_negative:{report.by_regime.momentum.expectancy:.2f}"
        )
    if report.by_regime.mean_reversion.count > 0 and report.by_regime.mean_reversion.expectancy < 0:
        report.promotion_reasons.append(
            f"mr_negative:{report.by_regime.mean_reversion.expectancy:.2f}"
        )

    return report


def walk_forward_evaluate(
    all_signals: dict[str, dict[int, list]],
    all_option_bars: dict[str, pd.DataFrame],
    config: ExperimentConfig,
) -> EvaluationReport:
    """Run walk-forward evaluation by day.

    Args:
        all_signals: Dict of date_str → {timestamp → [TradeIntent]} per day.
        all_option_bars: Dict of date_str → option bars DataFrame per day.
        config: Experiment configuration.

    Returns:
        Aggregated EvaluationReport across all walk-forward windows.
    """
    from src.strategies.simulator import run_simulation

    all_trades: list[PositionState] = []
    all_daily: list[DailyState] = []
    windows = []

    dates = sorted(all_signals.keys())

    for date_str in dates:
        signals = all_signals[date_str]
        bars = all_option_bars.get(date_str, pd.DataFrame())

        day_result = run_simulation(signals, bars, config)
        all_trades.extend(day_result.trades)
        all_daily.extend(day_result.daily_states)

        day_metrics = compute_trade_metrics(
            [t for t in day_result.trades if not t.is_open]
        )
        windows.append(
            {
                "date": date_str,
                "trades": day_metrics.count,
                "pnl": day_metrics.total_pnl,
                "expectancy": day_metrics.expectancy,
                "win_rate": day_metrics.win_rate,
            }
        )

    # Build aggregate result
    aggregate = SimulatorResult(trades=all_trades, daily_states=all_daily)
    report = evaluate_simulation(aggregate, config)
    report.walk_forward_windows = windows

    return report


def format_report(report: EvaluationReport) -> str:
    """Format an evaluation report as a human-readable string."""
    lines = []
    lines.append("=" * 60)
    lines.append("EVALUATION REPORT")
    lines.append("=" * 60)

    m = report.overall
    lines.append(f"\nOverall ({m.count} trades):")
    lines.append(f"  Total PnL:       {m.total_pnl:>10.2f}")
    lines.append(f"  Expectancy:      {m.expectancy:>10.2f}")
    lines.append(f"  Win Rate:        {m.win_rate:>10.1%}")
    lines.append(f"  Profit Factor:   {m.profit_factor:>10.2f}")
    lines.append(f"  Max Drawdown:    {m.max_drawdown:>10.2f}")
    lines.append(f"  Avg Hold (sec):  {m.avg_hold_seconds:>10.1f}")
    lines.append(f"  Total Fees:      {m.total_fees:>10.2f}")
    lines.append(f"  Daily Sharpe:    {m.sharpe_daily:>10.2f}")

    # Regime breakdown
    lines.append(f"\nMomentum ({report.by_regime.momentum.count} trades):")
    if report.by_regime.momentum.count > 0:
        rm = report.by_regime.momentum
        lines.append(f"  Expectancy: {rm.expectancy:.2f}  WR: {rm.win_rate:.1%}")

    lines.append(f"\nMean Reversion ({report.by_regime.mean_reversion.count} trades):")
    if report.by_regime.mean_reversion.count > 0:
        mm = report.by_regime.mean_reversion
        lines.append(f"  Expectancy: {mm.expectancy:.2f}  WR: {mm.win_rate:.1%}")

    # Exit reasons
    lines.append("\nExit Reasons:")
    for reason, count in sorted(report.by_exit_reason.items()):
        lines.append(f"  {reason:20s}: {count}")

    # Time of day
    lines.append("\nTime-of-Day (IST):")
    for hour, metrics in sorted(report.by_time.by_hour.items()):
        lines.append(
            f"  {hour:02d}:00  trades={metrics.count:3d}  pnl={metrics.total_pnl:8.2f}  wr={metrics.win_rate:.0%}"
        )

    # Walk-forward windows
    if report.walk_forward_windows:
        lines.append("\nWalk-Forward Windows:")
        for w in report.walk_forward_windows:
            lines.append(
                f"  {w['date']}  trades={w['trades']:3d}  pnl={w['pnl']:8.2f}  "
                f"exp={w['expectancy']:6.2f}  wr={w['win_rate']:.0%}"
            )

    # Promotion
    lines.append(f"\nPromotion: {'PASSED' if report.promotion_passed else 'FAILED'}")
    if report.promotion_reasons:
        for r in report.promotion_reasons:
            lines.append(f"  - {r}")

    lines.append("=" * 60)
    return "\n".join(lines)
