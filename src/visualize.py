"""Visualization module for the NIFTY scalping platform.

Generates charts for:
1. Price action with regime overlay and signal markers
2. Regime distribution (pie + time-series)
3. Signal analysis: by strategy, direction, time-of-day
4. Feature distributions: volatility, trend slope, breadth
5. Session comparison dashboard
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # non-interactive backend
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.patches import Patch

from src.models import RegimeLabel

# Style
sns.set_theme(style="darkgrid", palette="muted")
REGIME_COLORS = {
    "momentum": "#e74c3c",
    "mean_reversion": "#3498db",
    "no_trade": "#95a5a6",
}
SIGNAL_COLORS = {
    "long_ce": "#2ecc71",
    "long_pe": "#e74c3c",
}
STRATEGY_COLORS = {
    "momentum_breakout": "#e74c3c",
    "momentum_pullback": "#f39c12",
    "mean_reversion_snap": "#3498db",
    "mean_reversion_failed_breakout": "#9b59b6",
}


def _ts_to_ist(ts: int | float) -> pd.Timestamp:
    """Convert unix timestamp to IST datetime."""
    return pd.Timestamp(ts, unit="s", tz="UTC").tz_convert("Asia/Kolkata")


def _add_ist_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add IST datetime column from timestamp."""
    df = df.copy()
    df["ist_time"] = pd.to_datetime(df["timestamp"] + 5.5 * 3600, unit="s")
    return df


def plot_price_with_regimes(
    features_df: pd.DataFrame,
    signals: list[dict] | None = None,
    date: str | None = None,
    output_path: Path | None = None,
) -> plt.Figure:
    """Plot price action with regime coloring and signal markers.

    Args:
        features_df: Feature DataFrame with columns [timestamp, close, regime].
        signals: Optional list of signal dicts with [timestamp, close, direction, strategy].
        date: Optional date string to filter.
        output_path: Optional path to save the figure.
    """
    df = _add_ist_column(features_df)
    if date:
        df = df[df["date"] == date] if "date" in df.columns else df

    fig, axes = plt.subplots(3, 1, figsize=(16, 12), height_ratios=[3, 1, 1], sharex=True)
    fig.suptitle(
        f"NIFTY50 5s Price Action with Regime Overlay{f' — {date}' if date else ''}",
        fontsize=14,
        fontweight="bold",
    )

    # Panel 1: Price + regime background + signals
    ax1 = axes[0]
    ax1.plot(df["ist_time"], df["close"], color="black", linewidth=0.5, alpha=0.8, label="Close")

    # Color background by regime
    if "regime" in df.columns:
        for regime_val, color in REGIME_COLORS.items():
            mask = df["regime"].apply(
                lambda x: x.value if hasattr(x, "value") else str(x)
            ) == regime_val
            if mask.any():
                ax1.fill_between(
                    df["ist_time"], df["close"].min(), df["close"].max(),
                    where=mask, alpha=0.15, color=color, label=regime_val.replace("_", " ").title(),
                )

    # Plot signals
    if signals:
        for sig in signals:
            if date and sig.get("date") != date:
                continue
            sig_time = pd.Timestamp(sig["timestamp"] + 5.5 * 3600, unit="s")
            color = SIGNAL_COLORS.get(sig["direction"], "gray")
            marker = "^" if sig["direction"] == "long_ce" else "v"
            ax1.scatter(sig_time, sig["close"], c=color, marker=marker, s=30, zorder=5, alpha=0.7)

    ax1.set_ylabel("NIFTY50 Close")
    ax1.legend(loc="upper left", fontsize=8)

    # Panel 2: Realized volatility
    ax2 = axes[1]
    ax2.plot(df["ist_time"], df["realized_vol_60"], color="#e74c3c", linewidth=0.7, label="RVol 60-bar")
    if "realized_vol_180" in df.columns:
        ax2.plot(df["ist_time"], df["realized_vol_180"], color="#3498db", linewidth=0.7, label="RVol 180-bar")
    ax2.set_ylabel("Realized Vol")
    ax2.legend(loc="upper right", fontsize=8)

    # Panel 3: Trend slope
    ax3 = axes[2]
    ax3.plot(df["ist_time"], df["trend_slope_60"], color="#2ecc71", linewidth=0.7, label="Slope 60-bar")
    ax3.axhline(y=0, color="gray", linestyle="--", linewidth=0.5)
    ax3.set_ylabel("Trend Slope")
    ax3.set_xlabel("Time (IST)")
    ax3.legend(loc="upper right", fontsize=8)

    plt.tight_layout()
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
    return fig


def plot_regime_distribution(
    features_df: pd.DataFrame,
    output_path: Path | None = None,
) -> plt.Figure:
    """Pie chart + time breakdown of regime distribution."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Regime Distribution", fontsize=14, fontweight="bold")

    # Extract regime values
    regime_vals = features_df["regime"].apply(
        lambda x: x.value if hasattr(x, "value") else str(x)
    )

    # Pie chart
    counts = regime_vals.value_counts()
    colors = [REGIME_COLORS.get(r, "#666") for r in counts.index]
    axes[0].pie(
        counts.values, labels=[r.replace("_", " ").title() for r in counts.index],
        colors=colors, autopct="%1.1f%%", startangle=90, textprops={"fontsize": 10},
    )
    axes[0].set_title("Overall Distribution")

    # By date
    if "date" in features_df.columns:
        df = features_df.copy()
        df["regime_str"] = regime_vals
        date_regime = df.groupby(["date", "regime_str"]).size().unstack(fill_value=0)
        bar_colors = [REGIME_COLORS.get(c, "#666") for c in date_regime.columns]
        date_regime.plot(kind="bar", stacked=True, ax=axes[1], color=bar_colors)
        axes[1].set_title("By Session Date")
        axes[1].set_xlabel("Date")
        axes[1].set_ylabel("Bar Count")
        axes[1].legend(title="Regime", fontsize=8)
        axes[1].tick_params(axis="x", rotation=45)
    else:
        axes[1].text(0.5, 0.5, "No date column available", ha="center", va="center")

    plt.tight_layout()
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
    return fig


def plot_signal_analysis(
    report: dict,
    output_path: Path | None = None,
) -> plt.Figure:
    """Multi-panel signal analysis dashboard."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(
        f"Signal Analysis — {report['total_signals']} signals from {report['total_bars']} bars",
        fontsize=14, fontweight="bold",
    )

    # 1. By strategy (bar chart)
    ax = axes[0, 0]
    strategies = report.get("signals_by_strategy", {})
    if strategies:
        names = list(strategies.keys())
        vals = list(strategies.values())
        colors = [STRATEGY_COLORS.get(n, "#666") for n in names]
        bars = ax.barh(
            [n.replace("_", " ").title() for n in names], vals, color=colors
        )
        ax.bar_label(bars, padding=3)
    ax.set_title("Signals by Strategy")
    ax.set_xlabel("Count")

    # 2. By direction (pie)
    ax = axes[0, 1]
    directions = report.get("signals_by_direction", {})
    if directions:
        labels = [d.replace("_", " ").upper() for d in directions.keys()]
        colors = [SIGNAL_COLORS.get(d, "#666") for d in directions.keys()]
        ax.pie(
            list(directions.values()), labels=labels, colors=colors,
            autopct="%1.1f%%", startangle=90, textprops={"fontsize": 10},
        )
    ax.set_title("Signal Direction Split")

    # 3. By time bucket (bar chart)
    ax = axes[1, 0]
    time_buckets = report.get("signals_by_time_bucket", {})
    if time_buckets:
        times = sorted(time_buckets.keys())
        vals = [time_buckets[t] for t in times]
        ax.bar(times, vals, color="#3498db", alpha=0.8)
        ax.set_xlabel("Time (IST)")
        ax.tick_params(axis="x", rotation=45)
    ax.set_title("Signals by Time of Day")
    ax.set_ylabel("Count")

    # 4. By date (bar chart)
    ax = axes[1, 1]
    by_date = report.get("signals_by_date", {})
    if by_date:
        dates = sorted(by_date.keys())
        vals = [by_date[d] for d in dates]
        ax.bar(dates, vals, color="#2ecc71", alpha=0.8)
        ax.set_xlabel("Date")
    ax.set_title("Signals by Session")
    ax.set_ylabel("Count")

    plt.tight_layout()
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
    return fig


def plot_feature_distributions(
    features_df: pd.DataFrame,
    output_path: Path | None = None,
) -> plt.Figure:
    """Distribution plots for key features."""
    fig, axes = plt.subplots(2, 3, figsize=(16, 10))
    fig.suptitle("Feature Distributions", fontsize=14, fontweight="bold")

    feature_configs = [
        ("trend_slope_60", "Trend Slope (60-bar)", "#2ecc71"),
        ("realized_vol_60", "Realized Vol (60-bar)", "#e74c3c"),
        ("vwap_deviation", "VWAP Deviation", "#3498db"),
        ("range_expansion", "Range Expansion", "#f39c12"),
        ("breadth_advancing_pct", "Breadth Advancing %", "#9b59b6"),
        ("acceleration", "Acceleration", "#1abc9c"),
    ]

    for i, (col, title, color) in enumerate(feature_configs):
        ax = axes[i // 3, i % 3]
        if col in features_df.columns:
            data = features_df[col].dropna()
            if len(data) > 0:
                ax.hist(data, bins=80, color=color, alpha=0.7, edgecolor="white", linewidth=0.3)
                ax.axvline(data.mean(), color="black", linestyle="--", linewidth=1, label=f"mean={data.mean():.4f}")
                ax.axvline(data.median(), color="gray", linestyle=":", linewidth=1, label=f"median={data.median():.4f}")
                ax.legend(fontsize=7)
        ax.set_title(title)
        ax.set_ylabel("Count")

    plt.tight_layout()
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
    return fig


def plot_session_comparison(
    features_df: pd.DataFrame,
    output_path: Path | None = None,
) -> plt.Figure:
    """Compare sessions side by side: price, vol, regime."""
    if "date" not in features_df.columns:
        fig, ax = plt.subplots(1, 1, figsize=(8, 4))
        ax.text(0.5, 0.5, "No date column for comparison", ha="center", va="center")
        return fig

    dates = sorted(features_df["date"].unique())
    n_dates = len(dates)
    fig, axes = plt.subplots(n_dates, 2, figsize=(16, 4 * n_dates), squeeze=False)
    fig.suptitle("Session Comparison", fontsize=14, fontweight="bold")

    for i, date_str in enumerate(dates):
        session = features_df[features_df["date"] == date_str].copy()
        session = _add_ist_column(session)

        # Price
        ax_price = axes[i, 0]
        ax_price.plot(session["ist_time"], session["close"], color="black", linewidth=0.6)
        ax_price.set_title(f"{date_str} — Price")
        ax_price.set_ylabel("Close")

        # Regime coloring
        if "regime" in session.columns:
            regime_vals = session["regime"].apply(
                lambda x: x.value if hasattr(x, "value") else str(x)
            )
            for regime_val, color in REGIME_COLORS.items():
                mask = regime_vals == regime_val
                if mask.any():
                    ax_price.fill_between(
                        session["ist_time"], session["close"].min(), session["close"].max(),
                        where=mask, alpha=0.15, color=color,
                    )

        # Vol + breadth
        ax_feat = axes[i, 1]
        ax_feat.plot(session["ist_time"], session["realized_vol_60"], color="#e74c3c", linewidth=0.7, label="RVol60")
        ax_feat_twin = ax_feat.twinx()
        ax_feat_twin.plot(
            session["ist_time"], session["breadth_advancing_pct"],
            color="#9b59b6", linewidth=0.7, alpha=0.6, label="Breadth",
        )
        ax_feat.set_title(f"{date_str} — Vol & Breadth")
        ax_feat.set_ylabel("Realized Vol", color="#e74c3c")
        ax_feat_twin.set_ylabel("Breadth %", color="#9b59b6")
        ax_feat.legend(loc="upper left", fontsize=7)
        ax_feat_twin.legend(loc="upper right", fontsize=7)

    plt.tight_layout()
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
    return fig


def plot_signal_overlay_per_session(
    features_df: pd.DataFrame,
    report: dict,
    output_dir: Path | None = None,
) -> list[plt.Figure]:
    """Per-session price chart with signal markers."""
    if "date" not in features_df.columns:
        return []

    dates = sorted(features_df["date"].unique())
    figs = []

    for date_str in dates:
        session = features_df[features_df["date"] == date_str].copy()
        session = _add_ist_column(session)

        # Get signals for this date
        date_signals = report.get("signals_per_bar", {}).get(date_str, [])

        fig, ax = plt.subplots(1, 1, figsize=(16, 6))
        ax.plot(session["ist_time"], session["close"], color="black", linewidth=0.5, alpha=0.8)

        # Regime background
        if "regime" in session.columns:
            regime_vals = session["regime"].apply(
                lambda x: x.value if hasattr(x, "value") else str(x)
            )
            for regime_val, color in REGIME_COLORS.items():
                mask = regime_vals == regime_val
                if mask.any():
                    ax.fill_between(
                        session["ist_time"], session["close"].min(), session["close"].max(),
                        where=mask, alpha=0.12, color=color,
                    )

        # Signal markers
        for sig in date_signals:
            sig_time = pd.Timestamp(sig["timestamp"] + 5.5 * 3600, unit="s")
            color = SIGNAL_COLORS.get(sig["direction"], "gray")
            marker = "^" if sig["direction"] == "long_ce" else "v"
            strat_color = STRATEGY_COLORS.get(sig["strategy"], color)
            ax.scatter(sig_time, sig["close"], c=strat_color, marker=marker, s=40, zorder=5, alpha=0.8, edgecolors="black", linewidths=0.3)

        # Legend
        legend_elements = [
            Patch(facecolor=REGIME_COLORS["momentum"], alpha=0.3, label="Momentum"),
            Patch(facecolor=REGIME_COLORS["mean_reversion"], alpha=0.3, label="Mean Reversion"),
            Patch(facecolor=REGIME_COLORS["no_trade"], alpha=0.3, label="No Trade"),
        ]
        for strat, col in STRATEGY_COLORS.items():
            legend_elements.append(
                plt.scatter([], [], c=col, marker="o", s=40, label=strat.replace("_", " ").title())
            )
        ax.legend(handles=legend_elements, loc="upper left", fontsize=8)

        ax.set_title(f"NIFTY50 — {date_str} — {len(date_signals)} signals", fontsize=12, fontweight="bold")
        ax.set_ylabel("Close")
        ax.set_xlabel("Time (IST)")

        plt.tight_layout()
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_dir / f"signals_{date_str}.png", dpi=150, bbox_inches="tight")
        figs.append(fig)

    return figs


def generate_full_report(
    features_parquet: Path,
    report_json: Path,
    output_dir: Path,
) -> None:
    """Generate all visualization charts from feature data and backtest report.

    Args:
        features_parquet: Path to the features parquet file.
        report_json: Path to the backtest report JSON.
        output_dir: Directory to save all charts.
    """
    import structlog
    log = structlog.get_logger()

    output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    df = pd.read_parquet(features_parquet)
    with open(report_json) as f:
        report = json.load(f)

    log.info("generating_charts", bars=len(df), signals=report["total_signals"])

    # Classify regimes (needed for coloring)
    from src.strategies.regime import classify_regime_batch
    df["regime"] = classify_regime_batch(df)

    # 1. Feature distributions
    log.info("chart", name="feature_distributions")
    plot_feature_distributions(df, output_dir / "feature_distributions.png")
    plt.close("all")

    # 2. Regime distribution
    log.info("chart", name="regime_distribution")
    plot_regime_distribution(df, output_dir / "regime_distribution.png")
    plt.close("all")

    # 3. Signal analysis
    log.info("chart", name="signal_analysis")
    plot_signal_analysis(report, output_dir / "signal_analysis.png")
    plt.close("all")

    # 4. Session comparison
    log.info("chart", name="session_comparison")
    plot_session_comparison(df, output_dir / "session_comparison.png")
    plt.close("all")

    # 5. Per-session signal overlays
    log.info("chart", name="signal_overlays")
    plot_signal_overlay_per_session(df, report, output_dir / "sessions")
    plt.close("all")

    # 6. Combined price+regime for each date
    dates = sorted(df["date"].unique()) if "date" in df.columns else []
    for date_str in dates:
        log.info("chart", name=f"price_regime_{date_str}")
        date_signals = report.get("signals_per_bar", {}).get(date_str, [])
        plot_price_with_regimes(
            df[df["date"] == date_str] if "date" in df.columns else df,
            signals=date_signals,
            date=date_str,
            output_path=output_dir / f"price_regime_{date_str}.png",
        )
        plt.close("all")

    log.info("report_complete", output_dir=str(output_dir), charts_generated=5 + len(dates))
