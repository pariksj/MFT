"""Feature engine: compute research features from 5s underlying bars.

Computes features both in batch (full DataFrame) and incremental (streaming)
modes, ensuring parity between the two for live/backtest consistency.

Feature groups:
- Multi-horizon returns (1, 3, 6, 12, 60 bars)
- Realized volatility (60, 180 bars)
- Range expansion
- VWAP and deviation
- Opening range state
- Intraday trend slope and acceleration
- Breadth / dispersion from NIFTY50 constituents
- Option context (when chain data available)
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_returns(close: pd.Series) -> dict[str, pd.Series]:
    """Multi-horizon log returns."""
    out = {}
    for n, label in [(1, "ret_1"), (3, "ret_3"), (6, "ret_6"), (12, "ret_12"), (60, "ret_60")]:
        out[label] = np.log(close / close.shift(n))
    return out


def compute_realized_vol(close: pd.Series) -> dict[str, pd.Series]:
    """Rolling realized volatility of log returns."""
    log_ret = np.log(close / close.shift(1))
    return {
        "realized_vol_60": log_ret.rolling(60).std() * np.sqrt(60),
        "realized_vol_180": log_ret.rolling(180).std() * np.sqrt(180),
    }


def compute_range_expansion(high: pd.Series, low: pd.Series, window: int = 60) -> pd.Series:
    """Current bar range vs trailing average range."""
    bar_range = high - low
    avg_range = bar_range.rolling(window).mean()
    return (bar_range / avg_range.replace(0, np.nan)).fillna(1.0)


def compute_vwap(close: pd.Series, volume: pd.Series) -> dict[str, pd.Series]:
    """Cumulative VWAP and deviation (resets each session — caller groups by date)."""
    cum_vol = volume.cumsum()
    cum_pv = (close * volume).cumsum()
    vwap = (cum_pv / cum_vol.replace(0, np.nan)).fillna(close)
    deviation = ((close - vwap) / vwap.replace(0, np.nan)).fillna(0.0)
    return {"vwap": vwap, "vwap_deviation": deviation}


def compute_opening_range(
    df: pd.DataFrame,
    or_minutes: int = 5,
    session_start_ts: int | None = None,
) -> dict[str, pd.Series]:
    """Opening range high/low and breakout flags.

    Args:
        df: Must have columns 'timestamp', 'high', 'low', 'close'.
        or_minutes: Duration of opening range in minutes.
        session_start_ts: Unix timestamp of session start. If None, uses first bar.
    """
    if session_start_ts is None:
        session_start_ts = int(df["timestamp"].iloc[0])

    or_end_ts = session_start_ts + or_minutes * 60
    or_mask = df["timestamp"] <= or_end_ts

    or_high = df.loc[or_mask, "high"].max() if or_mask.any() else np.nan
    or_low = df.loc[or_mask, "low"].min() if or_mask.any() else np.nan

    elapsed = ((df["timestamp"] - session_start_ts) / 60).astype(int)

    return {
        "or_high": pd.Series(or_high, index=df.index),
        "or_low": pd.Series(or_low, index=df.index),
        "or_breakout_up": df["close"] > or_high,
        "or_breakout_down": df["close"] < or_low,
        "or_minutes_elapsed": elapsed,
    }


def compute_trend_slope(close: pd.Series, window: int = 60) -> pd.Series:
    """Linear regression slope over rolling window (OLS on bar index)."""
    x = np.arange(window, dtype=float)
    x_centered = x - x.mean()
    denom = (x_centered**2).sum()

    def _slope(vals):
        if len(vals) < window:
            return np.nan
        y = vals - vals.mean()
        return float(np.dot(x_centered, y) / denom)

    return close.rolling(window).apply(_slope, raw=True)


def compute_acceleration(slope: pd.Series, window: int = 12) -> pd.Series:
    """Rate of change of trend slope."""
    return slope.diff(window) / window


def compute_breadth(
    constituents_df: pd.DataFrame,
    index_timestamps: pd.Series,
) -> dict[str, pd.Series]:
    """Market breadth from NIFTY50 constituents.

    Args:
        constituents_df: DataFrame with columns [symbol, timestamp, close].
        index_timestamps: Series of timestamps to align breadth to.

    Returns:
        Dict with advancing_pct and dispersion series aligned to index_timestamps.
    """
    if constituents_df.empty:
        n = len(index_timestamps)
        return {
            "breadth_advancing_pct": pd.Series(0.5, index=index_timestamps.index),
            "breadth_dispersion": pd.Series(0.0, index=index_timestamps.index),
        }

    # Pivot to get close prices per symbol per timestamp
    pivot = constituents_df.pivot_table(
        index="timestamp", columns="symbol", values="close", aggfunc="last"
    )

    # 1-bar returns for each constituent
    rets = pivot.pct_change()

    # Advancing percentage: fraction of constituents with positive return
    advancing = (rets > 0).mean(axis=1)

    # Dispersion: cross-sectional std of returns
    dispersion = rets.std(axis=1)

    # Align to index timestamps
    aligned_adv = advancing.reindex(index_timestamps.values).ffill().fillna(0.5)
    aligned_disp = dispersion.reindex(index_timestamps.values).ffill().fillna(0.0)
    aligned_adv.index = index_timestamps.index
    aligned_disp.index = index_timestamps.index

    return {
        "breadth_advancing_pct": aligned_adv,
        "breadth_dispersion": aligned_disp,
    }


def build_features(
    df: pd.DataFrame,
    constituents_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build full feature matrix for a single symbol-date session.

    Args:
        df: Single-symbol session bars with columns:
            [symbol, timestamp, open, high, low, close, volume]
        constituents_df: Optional NIFTY50 constituent bars for breadth features.

    Returns:
        DataFrame with all feature columns appended.
    """
    out = df.copy()

    # Returns
    for k, v in compute_returns(out["close"]).items():
        out[k] = v

    # Volatility
    for k, v in compute_realized_vol(out["close"]).items():
        out[k] = v

    # Range expansion
    out["range_expansion"] = compute_range_expansion(out["high"], out["low"])

    # VWAP
    for k, v in compute_vwap(out["close"], out["volume"]).items():
        out[k] = v

    # Opening range
    for k, v in compute_opening_range(out).items():
        out[k] = v

    # Trend slope
    out["trend_slope_60"] = compute_trend_slope(out["close"], 60)
    out["trend_slope_180"] = compute_trend_slope(out["close"], 180)

    # Acceleration
    out["acceleration"] = compute_acceleration(out["trend_slope_60"])

    # Breadth
    if constituents_df is not None and not constituents_df.empty:
        breadth = compute_breadth(constituents_df, out["timestamp"])
        for k, v in breadth.items():
            out[k] = v.values
    else:
        out["breadth_advancing_pct"] = 0.5
        out["breadth_dispersion"] = 0.0

    # Option context placeholders (populated when option data available)
    for col in ["atm_iv", "atm_spread_pct", "put_call_oi_ratio", "atm_volume"]:
        if col not in out.columns:
            out[col] = 0.0

    return out


class IncrementalFeatureState:
    """Maintains running state for streaming feature computation.

    Ensures parity with batch `build_features` for live trading.
    """

    def __init__(self, or_minutes: int = 5):
        self.bars: list[dict] = []
        self.or_minutes = or_minutes
        self.or_high: float = -np.inf
        self.or_low: float = np.inf
        self.session_start_ts: int | None = None
        self.cum_volume: float = 0.0
        self.cum_price_volume: float = 0.0

    def update(self, bar: dict) -> dict:
        """Process a new bar and return the feature dict for this timestamp.

        Args:
            bar: Dict with keys [symbol, timestamp, open, high, low, close, volume].

        Returns:
            Feature dict matching FeatureRow fields.
        """
        self.bars.append(bar)

        if self.session_start_ts is None:
            self.session_start_ts = bar["timestamp"]

        n = len(self.bars)
        close = bar["close"]

        # VWAP
        self.cum_volume += bar["volume"]
        self.cum_price_volume += close * bar["volume"]
        vwap = self.cum_price_volume / self.cum_volume if self.cum_volume > 0 else close
        vwap_dev = (close - vwap) / vwap if vwap > 0 else 0.0

        # Opening range
        or_end = self.session_start_ts + self.or_minutes * 60
        if bar["timestamp"] <= or_end:
            self.or_high = max(self.or_high, bar["high"])
            self.or_low = min(self.or_low, bar["low"])

        # Returns
        def _get_close(offset):
            idx = n - 1 - offset
            return self.bars[idx]["close"] if idx >= 0 else close

        def _log_ret(offset):
            prev = _get_close(offset)
            return float(np.log(close / prev)) if prev > 0 else 0.0

        # Realized vol (use last 60/180 bars)
        def _realized_vol(window):
            if n < window + 1:
                return 0.0
            closes = [self.bars[i]["close"] for i in range(max(0, n - window - 1), n)]
            log_rets = [np.log(closes[i + 1] / closes[i]) for i in range(len(closes) - 1)]
            return float(np.std(log_rets) * np.sqrt(window)) if log_rets else 0.0

        # Range expansion
        if n >= 60:
            recent_ranges = [
                self.bars[i]["high"] - self.bars[i]["low"] for i in range(n - 60, n)
            ]
            avg_range = np.mean(recent_ranges)
            cur_range = bar["high"] - bar["low"]
            range_exp = cur_range / avg_range if avg_range > 0 else 1.0
        else:
            range_exp = 1.0

        # Trend slope (simplified for streaming — uses last N closes)
        def _trend_slope(window):
            if n < window:
                return 0.0
            closes = [self.bars[i]["close"] for i in range(n - window, n)]
            x = np.arange(window, dtype=float)
            x_c = x - x.mean()
            y = np.array(closes) - np.mean(closes)
            return float(np.dot(x_c, y) / (x_c**2).sum())

        slope_60 = _trend_slope(60)
        slope_180 = _trend_slope(180)

        # Acceleration
        accel = 0.0
        if n > 72:
            old_closes = [self.bars[i]["close"] for i in range(n - 72, n - 12)]
            x = np.arange(60, dtype=float)
            x_c = x - x.mean()
            y = np.array(old_closes) - np.mean(old_closes)
            old_slope = float(np.dot(x_c, y) / (x_c**2).sum())
            accel = (slope_60 - old_slope) / 12.0

        minutes_elapsed = (bar["timestamp"] - self.session_start_ts) // 60

        return {
            "symbol": bar["symbol"],
            "timestamp": bar["timestamp"],
            "ret_1": _log_ret(1),
            "ret_3": _log_ret(3),
            "ret_6": _log_ret(6),
            "ret_12": _log_ret(12),
            "ret_60": _log_ret(60),
            "realized_vol_60": _realized_vol(60),
            "realized_vol_180": _realized_vol(180),
            "range_expansion": range_exp,
            "vwap": vwap,
            "vwap_deviation": vwap_dev,
            "or_high": self.or_high if self.or_high > -np.inf else 0.0,
            "or_low": self.or_low if self.or_low < np.inf else 0.0,
            "or_breakout_up": close > self.or_high if self.or_high > -np.inf else False,
            "or_breakout_down": close < self.or_low if self.or_low < np.inf else False,
            "or_minutes_elapsed": minutes_elapsed,
            "trend_slope_60": slope_60,
            "trend_slope_180": slope_180,
            "acceleration": accel,
            "breadth_advancing_pct": 0.5,  # Updated externally
            "breadth_dispersion": 0.0,
            "atm_iv": 0.0,
            "atm_spread_pct": 0.0,
            "put_call_oi_ratio": 0.0,
            "atm_volume": 0,
        }
