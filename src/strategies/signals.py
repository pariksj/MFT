"""Strategy library: momentum and mean-reversion signal families.

Two long-premium signal families:
1. Momentum: breakout continuation and pullback-continuation
2. Mean-reversion: snapback reversal and failed-breakout reversal

All strategies respect regime gating — they only fire when the regime
classifier confirms their environment.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from src.models import RegimeLabel, SignalDirection, StrategySignal


@dataclass
class MomentumParams:
    """Tunable parameters for momentum strategies."""

    # Breakout: close must exceed OR high/low by this multiple of recent vol
    breakout_vol_multiple: float = 1.5
    # Minimum absolute return over 12 bars for breakout confirmation
    min_ret_12: float = 0.001
    # Pullback: retracement from recent high/low as fraction
    pullback_depth_min: float = 0.3
    pullback_depth_max: float = 0.7
    # Trend alignment: slope must agree with direction
    min_slope_alignment: float = 0.0002
    # Conviction scaling
    base_conviction: float = 0.6


@dataclass
class MeanReversionParams:
    """Tunable parameters for mean-reversion strategies."""

    # VWAP deviation threshold for snapback entry
    vwap_deviation_threshold: float = 0.003
    # Failed breakout: must have exceeded OR then returned inside
    failed_breakout_bars_max: int = 12  # max bars since breakout
    # Range contraction: range_expansion below this
    range_contraction_threshold: float = 0.8
    # Conviction scaling
    base_conviction: float = 0.5


def momentum_breakout(
    features: dict,
    regime: RegimeLabel,
    params: MomentumParams | None = None,
) -> StrategySignal | None:
    """Momentum breakout: price exceeds opening range on strong trend + vol.

    Fires when:
    - Regime is MOMENTUM
    - Price breaks above OR high (bullish) or below OR low (bearish)
    - 12-bar return exceeds minimum
    - Trend slope agrees with direction
    - Realized vol is elevated but not extreme
    """
    if regime != RegimeLabel.MOMENTUM:
        return None
    if params is None:
        params = MomentumParams()

    close = features.get("close", 0)
    or_high = features.get("or_high", 0)
    or_low = features.get("or_low", 0)
    ret_12 = features.get("ret_12", 0)
    slope = features.get("trend_slope_60", 0)
    vol = features.get("realized_vol_60", 0)
    timestamp = features.get("timestamp", 0)

    if or_high == 0 or or_low == 0:
        return None

    # Bullish breakout
    if (
        close > or_high
        and ret_12 > params.min_ret_12
        and slope > params.min_slope_alignment
    ):
        vol_bonus = min(vol / 0.01, 1.0) * 0.2  # up to 0.2 extra conviction from vol
        conviction = min(params.base_conviction + vol_bonus, 1.0)
        return StrategySignal(
            strategy_name="momentum_breakout",
            timestamp=timestamp,
            direction=SignalDirection.LONG_CE,
            conviction=conviction,
            regime=regime,
            features={"ret_12": ret_12, "slope": slope, "vol": vol},
        )

    # Bearish breakout
    if (
        close < or_low
        and ret_12 < -params.min_ret_12
        and slope < -params.min_slope_alignment
    ):
        vol_bonus = min(vol / 0.01, 1.0) * 0.2
        conviction = min(params.base_conviction + vol_bonus, 1.0)
        return StrategySignal(
            strategy_name="momentum_breakout",
            timestamp=timestamp,
            direction=SignalDirection.LONG_PE,
            conviction=conviction,
            regime=regime,
            features={"ret_12": ret_12, "slope": slope, "vol": vol},
        )

    return None


def momentum_pullback(
    features: dict,
    regime: RegimeLabel,
    recent_high: float,
    recent_low: float,
    params: MomentumParams | None = None,
) -> StrategySignal | None:
    """Momentum pullback continuation: enter on retracement in a trending market.

    Fires when:
    - Regime is MOMENTUM
    - Price has pulled back 30-70% from recent swing high/low
    - Underlying trend slope still agrees
    - Volume not collapsing
    """
    if regime != RegimeLabel.MOMENTUM:
        return None
    if params is None:
        params = MomentumParams()

    close = features.get("close", 0)
    slope = features.get("trend_slope_60", 0)
    timestamp = features.get("timestamp", 0)

    # Bullish pullback in uptrend
    if slope > params.min_slope_alignment and recent_high > recent_low:
        swing_range = recent_high - recent_low
        if swing_range <= 0:
            return None
        pullback = (recent_high - close) / swing_range
        if params.pullback_depth_min <= pullback <= params.pullback_depth_max:
            conviction = params.base_conviction * (1 - abs(pullback - 0.5))
            return StrategySignal(
                strategy_name="momentum_pullback",
                timestamp=timestamp,
                direction=SignalDirection.LONG_CE,
                conviction=max(0.3, min(conviction, 1.0)),
                regime=regime,
                features={"pullback": pullback, "slope": slope},
            )

    # Bearish pullback in downtrend
    if slope < -params.min_slope_alignment and recent_high > recent_low:
        swing_range = recent_high - recent_low
        if swing_range <= 0:
            return None
        pullback = (close - recent_low) / swing_range
        if params.pullback_depth_min <= pullback <= params.pullback_depth_max:
            conviction = params.base_conviction * (1 - abs(pullback - 0.5))
            return StrategySignal(
                strategy_name="momentum_pullback",
                timestamp=timestamp,
                direction=SignalDirection.LONG_PE,
                conviction=max(0.3, min(conviction, 1.0)),
                regime=regime,
                features={"pullback": pullback, "slope": slope},
            )

    return None


def mean_reversion_snapback(
    features: dict,
    regime: RegimeLabel,
    params: MeanReversionParams | None = None,
) -> StrategySignal | None:
    """Mean-reversion snapback: enter when price deviates far from VWAP in calm market.

    Fires when:
    - Regime is MEAN_REVERSION
    - VWAP deviation exceeds threshold
    - Range is not expanding (no trending breakout)
    - Direction: buy CE if below VWAP (expect snap up), buy PE if above (expect snap down)
    """
    if regime != RegimeLabel.MEAN_REVERSION:
        return None
    if params is None:
        params = MeanReversionParams()

    vwap_dev = features.get("vwap_deviation", 0)
    range_exp = features.get("range_expansion", 1.0)
    timestamp = features.get("timestamp", 0)

    if range_exp > params.range_contraction_threshold * 1.5:
        return None  # range too expanded for MR

    # Below VWAP → expect snap up → buy CE
    if vwap_dev < -params.vwap_deviation_threshold:
        conviction = min(abs(vwap_dev) / (params.vwap_deviation_threshold * 3), 1.0)
        conviction = max(params.base_conviction, conviction)
        return StrategySignal(
            strategy_name="mean_reversion_snap",
            timestamp=timestamp,
            direction=SignalDirection.LONG_CE,
            conviction=conviction,
            regime=regime,
            features={"vwap_dev": vwap_dev, "range_exp": range_exp},
        )

    # Above VWAP → expect snap down → buy PE
    if vwap_dev > params.vwap_deviation_threshold:
        conviction = min(abs(vwap_dev) / (params.vwap_deviation_threshold * 3), 1.0)
        conviction = max(params.base_conviction, conviction)
        return StrategySignal(
            strategy_name="mean_reversion_snap",
            timestamp=timestamp,
            direction=SignalDirection.LONG_PE,
            conviction=conviction,
            regime=regime,
            features={"vwap_dev": vwap_dev, "range_exp": range_exp},
        )

    return None


def mean_reversion_failed_breakout(
    features: dict,
    regime: RegimeLabel,
    bars_since_breakout: int,
    params: MeanReversionParams | None = None,
) -> StrategySignal | None:
    """Failed breakout reversal: price broke OR but quickly returned inside.

    Fires when:
    - Regime is MEAN_REVERSION
    - OR breakout flag was recently true but price is back inside range
    - The breakout failed within N bars
    """
    if regime != RegimeLabel.MEAN_REVERSION:
        return None
    if params is None:
        params = MeanReversionParams()

    close = features.get("close", 0)
    or_high = features.get("or_high", 0)
    or_low = features.get("or_low", 0)
    timestamp = features.get("timestamp", 0)

    if or_high == 0 or or_low == 0:
        return None
    if bars_since_breakout <= 0 or bars_since_breakout > params.failed_breakout_bars_max:
        return None

    or_mid = (or_high + or_low) / 2

    # Failed upside breakout → price back below OR high → expect mean reversion down
    if close < or_high and close > or_mid:
        recency_bonus = 1.0 - (bars_since_breakout / params.failed_breakout_bars_max)
        conviction = params.base_conviction * (0.5 + 0.5 * recency_bonus)
        return StrategySignal(
            strategy_name="mean_reversion_failed_breakout",
            timestamp=timestamp,
            direction=SignalDirection.LONG_PE,
            conviction=max(0.3, min(conviction, 1.0)),
            regime=regime,
            features={"bars_since_breakout": bars_since_breakout},
        )

    # Failed downside breakout → price back above OR low → expect mean reversion up
    if close > or_low and close < or_mid:
        recency_bonus = 1.0 - (bars_since_breakout / params.failed_breakout_bars_max)
        conviction = params.base_conviction * (0.5 + 0.5 * recency_bonus)
        return StrategySignal(
            strategy_name="mean_reversion_failed_breakout",
            timestamp=timestamp,
            direction=SignalDirection.LONG_CE,
            conviction=max(0.3, min(conviction, 1.0)),
            regime=regime,
            features={"bars_since_breakout": bars_since_breakout},
        )

    return None
