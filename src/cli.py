"""CLI entrypoints: collect-history, build-dataset, backtest, paper-trade."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click
import structlog

structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger()


@click.group()
def cli():
    """NIFTY 5s Options Scalping Platform."""
    pass


@cli.command()
@click.option("--raw-dir", default="./nifty50", help="Directory with raw JSON files")
@click.option("--db-path", default="./data/nifty_scalper.duckdb", help="DuckDB database path")
def collect_history(raw_dir: str, db_path: str):
    """Ingest raw JSON data into DuckDB."""
    from src.data.ingest import ingest_directory

    raw = Path(raw_dir)
    db = Path(db_path)

    if not raw.exists():
        log.error("raw_directory_not_found", path=str(raw))
        sys.exit(1)

    result = ingest_directory(raw, db)
    log.info(
        "ingest_complete",
        files=result["files"],
        bars=result["bars"],
        errors=len(result["errors"]),
    )

    if result.get("validations"):
        valid_count = sum(1 for v in result["validations"] if v["valid"])
        total = len(result["validations"])
        log.info("validation_summary", valid=valid_count, total=total)

        for v in result["validations"]:
            if not v["valid"]:
                log.warning(
                    "invalid_session",
                    date=v["date"],
                    symbol=v["symbol"],
                    issues=v["issues"],
                )


@cli.command()
@click.option("--db-path", default="./data/nifty_scalper.duckdb", help="DuckDB database path")
@click.option("--symbol", default="NIFTY50-INDEX", help="Symbol to build features for")
@click.option("--output", default="./data/processed/features.parquet", help="Output parquet path")
def build_dataset(db_path: str, symbol: str, output: str):
    """Build feature dataset from ingested bars."""
    from src.data.features import build_features
    from src.data.ingest import query_bars

    db = Path(db_path)
    if not db.exists():
        log.error("database_not_found", path=str(db))
        log.info("run_collect_history_first")
        sys.exit(1)

    # Load bars
    df = query_bars(db, symbol=symbol)
    if df.empty:
        log.error("no_bars_found", symbol=symbol)
        sys.exit(1)

    log.info("loaded_bars", symbol=symbol, count=len(df))

    # Load constituent bars for breadth
    constituents_df = query_bars(db)
    non_index = constituents_df[constituents_df["symbol"] != symbol]

    # Build features per date
    all_features = []
    for date_str, group in df.groupby("date"):
        log.info("building_features", date=date_str, bars=len(group))
        date_constituents = non_index[non_index["date"] == date_str]
        features = build_features(group, date_constituents)
        all_features.append(features)

    if not all_features:
        log.error("no_features_built")
        sys.exit(1)

    import pandas as pd

    combined = pd.concat(all_features, ignore_index=True)

    # Save to parquet
    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(out_path, index=False)
    log.info("features_saved", path=str(out_path), rows=len(combined))


@cli.command()
@click.option("--features", default="./data/processed/features.parquet", help="Feature parquet")
@click.option("--config-file", default=None, help="Experiment config JSON")
@click.option("--output", default="./data/processed/backtest_report.json", help="Report output")
def backtest(features: str, config_file: str | None, output: str):
    """Run backtest simulation with regime-gated strategies.

    Note: This requires option bar data for execution simulation.
    With spot-only data, this runs in signal-generation mode to count
    and analyze signals without PnL claims.
    """
    import pandas as pd

    from src.models import ExperimentConfig, RegimeLabel
    from src.strategies.evaluation import (
        EvaluationReport,
        compute_trade_metrics,
        format_report,
    )
    from src.strategies.regime import RegimeParams, classify_regime_batch
    from src.strategies.signals import (
        MeanReversionParams,
        MomentumParams,
        mean_reversion_snapback,
        momentum_breakout,
    )

    feat_path = Path(features)
    if not feat_path.exists():
        log.error("features_not_found", path=str(feat_path))
        log.info("run_build_dataset_first")
        sys.exit(1)

    # Load config
    if config_file:
        with open(config_file) as f:
            cfg_data = json.load(f)
        config = ExperimentConfig(**cfg_data)
    else:
        config = ExperimentConfig(name="default_backtest")

    # Load features
    df = pd.read_parquet(feat_path)
    log.info("loaded_features", rows=len(df))

    # Classify regimes
    regimes = classify_regime_batch(df)
    df["regime"] = regimes

    # Generate signals (spot-only mode — no option PnL)
    signals = []
    momentum_params = MomentumParams()
    mr_params = MeanReversionParams()

    for _, row in df.iterrows():
        features_dict = row.to_dict()
        regime = row["regime"]

        sig = momentum_breakout(features_dict, regime, momentum_params)
        if sig:
            signals.append(sig)
            continue

        sig = mean_reversion_snapback(features_dict, regime, mr_params)
        if sig:
            signals.append(sig)

    # Report signal statistics
    regime_counts = df["regime"].value_counts()
    log.info("regime_distribution", counts=regime_counts.to_dict())
    log.info("signals_generated", count=len(signals))

    signal_by_strategy = {}
    signal_by_direction = {}
    for s in signals:
        signal_by_strategy[s.strategy_name] = signal_by_strategy.get(s.strategy_name, 0) + 1
        signal_by_direction[s.direction.value] = signal_by_direction.get(s.direction.value, 0) + 1

    report_data = {
        "mode": "signal_generation_only",
        "note": "Option bar data required for PnL simulation. This report shows signal statistics only.",
        "total_bars": len(df),
        "regime_distribution": {k.value if hasattr(k, "value") else str(k): int(v) for k, v in regime_counts.items()},
        "total_signals": len(signals),
        "signals_by_strategy": signal_by_strategy,
        "signals_by_direction": signal_by_direction,
        "config": config.name,
    }

    # Save report
    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(report_data, f, indent=2, default=str)

    log.info("backtest_report_saved", path=str(out_path))
    log.info("signal_report", **report_data)

    # Print summary
    click.echo("\n" + "=" * 60)
    click.echo("BACKTEST SIGNAL REPORT (spot-only mode)")
    click.echo("=" * 60)
    click.echo(f"Total bars:     {len(df)}")
    click.echo(f"Total signals:  {len(signals)}")
    click.echo(f"By strategy:    {signal_by_strategy}")
    click.echo(f"By direction:   {signal_by_direction}")
    click.echo(f"Regime dist:    {report_data['regime_distribution']}")
    click.echo("=" * 60)
    click.echo(
        "\nNote: Add option OHLCV data to enable full PnL simulation."
    )


@cli.command()
@click.option("--features", default="./data/processed/features.parquet", help="Feature parquet")
@click.option("--report", default="./data/processed/backtest_report.json", help="Backtest report JSON")
@click.option("--output-dir", default="./data/processed/charts", help="Output directory for charts")
def visualize(features: str, report: str, output_dir: str):
    """Generate visualization charts from features and backtest report."""
    from src.visualize import generate_full_report

    feat_path = Path(features)
    report_path = Path(report)

    if not feat_path.exists():
        log.error("features_not_found", path=str(feat_path))
        sys.exit(1)
    if not report_path.exists():
        log.error("report_not_found", path=str(report_path))
        sys.exit(1)

    generate_full_report(feat_path, report_path, Path(output_dir))
    click.echo(f"Charts saved to {output_dir}/")


@cli.command()
@click.option("--paper/--live", default=True, help="Paper trading mode (default: paper)")
@click.option("--config-file", default=None, help="Experiment config JSON")
def paper_trade(paper: bool, config_file: str | None):
    """Start paper trading (or live with --live flag).

    Connects to broker WebSocket and runs the event loop.
    """
    import asyncio

    from src.live.event_loop import LiveEventLoop
    from src.live.upstox_adapter import UpstoxAdapter
    from src.models import ExperimentConfig

    if config_file:
        with open(config_file) as f:
            cfg_data = json.load(f)
        config = ExperimentConfig(**cfg_data)
    else:
        config = ExperimentConfig(name="paper_trade")

    broker = UpstoxAdapter()
    loop = LiveEventLoop(broker=broker, config=config, paper_mode=paper)

    if not paper:
        click.echo("WARNING: Live trading mode. Use at your own risk.")
        click.confirm("Continue?", abort=True)

    click.echo(f"Starting {'paper' if paper else 'LIVE'} trading...")
    click.echo("Press Ctrl+C to stop.")

    try:
        asyncio.run(loop.start())
    except KeyboardInterrupt:
        click.echo("\nStopping...")
        asyncio.run(loop.stop())


# Expose individual commands as module-level callables for setuptools entry points
def collect_history_entry():
    cli(["collect-history"] + sys.argv[1:], standalone_mode=True)


def build_dataset_entry():
    cli(["build-dataset"] + sys.argv[1:], standalone_mode=True)


def backtest_entry():
    cli(["backtest"] + sys.argv[1:], standalone_mode=True)


def paper_trade_entry():
    cli(["paper-trade"] + sys.argv[1:], standalone_mode=True)


if __name__ == "__main__":
    cli()
