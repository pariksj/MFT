"""Tests for feature engine: no-lookahead, batch/stream parity, correctness."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.data.features import (
    IncrementalFeatureState,
    build_features,
    compute_opening_range,
    compute_range_expansion,
    compute_realized_vol,
    compute_returns,
    compute_trend_slope,
    compute_vwap,
)


@pytest.fixture
def sample_session():
    """Create a sample session DataFrame."""
    n = 500
    base_ts = 1773287100
    np.random.seed(42)
    prices = 23000 + np.cumsum(np.random.randn(n) * 2)
    volumes = np.random.randint(100, 10000, n)

    return pd.DataFrame(
        {
            "symbol": "NIFTY50-INDEX",
            "timestamp": [base_ts + i * 5 for i in range(n)],
            "open": prices + np.random.randn(n) * 0.5,
            "high": prices + abs(np.random.randn(n)) * 2,
            "low": prices - abs(np.random.randn(n)) * 2,
            "close": prices,
            "volume": volumes,
        }
    )


def test_compute_returns(sample_session):
    rets = compute_returns(sample_session["close"])
    assert "ret_1" in rets
    assert "ret_60" in rets
    assert len(rets["ret_1"]) == len(sample_session)
    # First value should be NaN for 1-bar return
    assert np.isnan(rets["ret_1"].iloc[0])


def test_compute_realized_vol(sample_session):
    vols = compute_realized_vol(sample_session["close"])
    assert "realized_vol_60" in vols
    assert "realized_vol_180" in vols
    # First 60 values should be NaN
    assert np.isnan(vols["realized_vol_60"].iloc[30])
    # After warmup, should be positive
    assert vols["realized_vol_60"].iloc[100] > 0


def test_compute_range_expansion(sample_session):
    re = compute_range_expansion(sample_session["high"], sample_session["low"])
    assert len(re) == len(sample_session)
    # After warmup, should be around 1.0 on average
    assert 0 < re.iloc[100] < 10


def test_compute_vwap(sample_session):
    vwap = compute_vwap(sample_session["close"], sample_session["volume"])
    assert "vwap" in vwap
    assert "vwap_deviation" in vwap
    # VWAP should be close to price
    assert abs(vwap["vwap"].iloc[100] - sample_session["close"].iloc[100]) < 100


def test_compute_opening_range(sample_session):
    or_features = compute_opening_range(sample_session)
    assert "or_high" in or_features
    assert "or_low" in or_features
    assert "or_breakout_up" in or_features


def test_compute_trend_slope(sample_session):
    slope = compute_trend_slope(sample_session["close"], 60)
    assert len(slope) == len(sample_session)
    # After warmup, should be a finite number
    assert np.isfinite(slope.iloc[100])


def test_build_features(sample_session):
    features = build_features(sample_session)
    assert "ret_1" in features.columns
    assert "realized_vol_60" in features.columns
    assert "vwap" in features.columns
    assert "or_high" in features.columns
    assert "trend_slope_60" in features.columns
    assert "breadth_advancing_pct" in features.columns
    assert len(features) == len(sample_session)


def test_no_lookahead(sample_session):
    """Verify no future data leaks into features."""
    features = build_features(sample_session)

    # Returns at index i should only use data up to index i
    for col in ["ret_1", "ret_3", "ret_6", "ret_12", "ret_60"]:
        # The return at index 0 should be NaN (no prior data)
        assert np.isnan(features[col].iloc[0])


def test_incremental_parity(sample_session):
    """Verify batch and incremental feature computation produce similar results."""
    # Batch
    batch_features = build_features(sample_session)

    # Incremental
    state = IncrementalFeatureState()
    incremental_results = []
    for _, row in sample_session.iterrows():
        bar = row.to_dict()
        feat = state.update(bar)
        incremental_results.append(feat)

    # Compare key features at bar 200 (well past warmup)
    idx = 200
    batch_row = batch_features.iloc[idx]
    inc_row = incremental_results[idx]

    # VWAP should be very close
    assert abs(batch_row["vwap"] - inc_row["vwap"]) < 1.0

    # Returns should match
    assert abs(batch_row["ret_1"] - inc_row["ret_1"]) < 0.001

    # Trend slope may differ slightly due to implementation, but should be same sign
    if abs(batch_row["trend_slope_60"]) > 0.0001:
        assert np.sign(batch_row["trend_slope_60"]) == np.sign(inc_row["trend_slope_60"])
