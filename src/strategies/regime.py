"""Regime classifier: deterministic labeling of each 5s bar.

Labels each timestamp as momentum-favorable, mean-reversion-favorable,
or no-trade using trend strength, realized volatility, breadth confirmation,
and option-liquidity filters.

Designed to be identical in batch and streaming modes.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.models import RegimeLabel


@dataclass
class RegimeParams:
    """Tunable thresholds for regime classification."""

    # Trend strength: absolute slope threshold for momentum
    # Calibrated: p50=~0.01, p90=~0.5 for 5s NIFTY bars
    trend_slope_momentum_threshold: float = 0.3
    # Trend weakness: absolute slope below this = mean-reversion candidate
    trend_slope_mr_threshold: float = 0.15

    # Volatility: realized vol must be in this range for momentum
    # Calibrated: mean=~0.0007, p90=~0.001 for 5s NIFTY bars
    vol_min_momentum: float = 0.0004
    vol_max_momentum: float = 0.003
    # Volatility: realized vol range for mean-reversion
    vol_min_mr: float = 0.0003
    vol_max_mr: float = 0.002

    # Breadth: advancing pct threshold for momentum confirmation
    # Calibrated: breadth is often 0 or 1 for 5s bars (binary at this freq)
    breadth_momentum_threshold: float = 0.60  # >60% advancing for bullish momentum
    breadth_mr_range: tuple[float, float] = (0.20, 0.80)  # wider neutral band for MR

    # Range expansion: high expansion favors momentum
    range_expansion_momentum: float = 1.2
    range_expansion_mr_max: float = 2.0  # allow more range for MR

    # Session time filters (minutes from open)
    no_trade_first_minutes: int = 2  # skip first 2 minutes (opening chaos)
    no_trade_last_minutes: int = 10  # skip last 10 minutes (closing pressure)

    # Option liquidity (when available)
    min_atm_volume: int = 50
    max_atm_spread_pct: float = 0.03


def classify_regime(features: dict, params: RegimeParams | None = None) -> RegimeLabel:
    """Classify a single bar's regime from its feature dict.

    Works identically in batch (applied row-wise) and streaming modes.

    Args:
        features: Dict with feature keys matching FeatureRow fields.
        params: Optional override of default thresholds.

    Returns:
        RegimeLabel for this bar.
    """
    if params is None:
        params = RegimeParams()

    # Session time filter
    minutes = features.get("or_minutes_elapsed", 0)
    if minutes < params.no_trade_first_minutes:
        return RegimeLabel.NO_TRADE

    # Approximate: 375 minutes total session, reject last N
    if minutes > (375 - params.no_trade_last_minutes):
        return RegimeLabel.NO_TRADE

    # Option liquidity filter (if data available)
    atm_vol = features.get("atm_volume", 0)
    spread_pct = features.get("atm_spread_pct", 0.0)
    if atm_vol > 0 and atm_vol < params.min_atm_volume:
        return RegimeLabel.NO_TRADE
    if spread_pct > 0 and spread_pct > params.max_atm_spread_pct:
        return RegimeLabel.NO_TRADE

    # Core regime features
    slope = abs(features.get("trend_slope_60", 0.0))
    vol = features.get("realized_vol_60", 0.0)
    breadth = features.get("breadth_advancing_pct", 0.5)
    range_exp = features.get("range_expansion", 1.0)

    # Momentum regime
    is_momentum = (
        slope >= params.trend_slope_momentum_threshold
        and params.vol_min_momentum <= vol <= params.vol_max_momentum
        and range_exp >= params.range_expansion_momentum
        and (breadth >= params.breadth_momentum_threshold or breadth <= (1 - params.breadth_momentum_threshold))
    )
    if is_momentum:
        return RegimeLabel.MOMENTUM

    # Mean-reversion regime
    is_mr = (
        slope <= params.trend_slope_mr_threshold
        and params.vol_min_mr <= vol <= params.vol_max_mr
        and range_exp <= params.range_expansion_mr_max
        and params.breadth_mr_range[0] <= breadth <= params.breadth_mr_range[1]
    )
    if is_mr:
        return RegimeLabel.MEAN_REVERSION

    return RegimeLabel.NO_TRADE


def classify_regime_batch(features_df: pd.DataFrame, params: RegimeParams | None = None) -> pd.Series:
    """Classify regimes for an entire feature DataFrame.

    Args:
        features_df: DataFrame with feature columns.
        params: Optional override of default thresholds.

    Returns:
        Series of RegimeLabel values aligned to the DataFrame index.
    """
    if params is None:
        params = RegimeParams()

    labels = []
    for _, row in features_df.iterrows():
        labels.append(classify_regime(row.to_dict(), params))
    return pd.Series(labels, index=features_df.index, name="regime")
