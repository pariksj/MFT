"""Ingest raw JSON data from the nifty50/ directory into DuckDB.

Handles:
- Reading 5s OHLCV JSON files produced by the TypeScript collector
- Deduplication, sorting, gap detection
- Session completeness validation
- Storage as partitioned tables in DuckDB
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import structlog

from src.models import UnderlyingBar

log = structlog.get_logger()

# NSE equity-derivatives session: 09:15 – 15:30 IST
SESSION_START_SECONDS = 9 * 3600 + 15 * 60  # 33300
SESSION_END_SECONDS = 15 * 3600 + 30 * 60  # 55800
EXPECTED_BARS_PER_SESSION = (SESSION_END_SECONDS - SESSION_START_SECONDS) // 5  # 4500


def load_json_bars(filepath: Path) -> list[dict]:
    """Load a single JSON file of 5s bars, returning raw dicts."""
    with open(filepath) as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Expected list, got {type(data)} in {filepath}")
    return data


def json_to_underlying_bars(filepath: Path) -> list[UnderlyingBar]:
    """Parse a JSON file into typed UnderlyingBar objects."""
    raw = load_json_bars(filepath)
    symbol = filepath.stem.split("_")[1]  # NSE_{SYMBOL}_{DATE}_5s
    # Handle compound symbols like TATAMOTORS-EQ
    parts = filepath.stem.split("_")
    # Format: NSE_{SYMBOL}_{DATE}_5s — symbol may contain hyphens
    # Date is YYYY-MM-DD which has hyphens, so parse from the known positions
    symbol = "_".join(parts[1:-2])  # everything between NSE_ and _DATE_5s

    bars = []
    for row in raw:
        bars.append(
            UnderlyingBar(
                symbol=symbol,
                timestamp=int(row["timestamp"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]),
            )
        )
    return bars


def bars_to_dataframe(bars: list[UnderlyingBar]) -> pd.DataFrame:
    """Convert a list of UnderlyingBar to a pandas DataFrame."""
    if not bars:
        return pd.DataFrame(
            columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"]
        )
    records = [
        {
            "symbol": b.symbol,
            "timestamp": b.timestamp,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
        }
        for b in bars
    ]
    df = pd.DataFrame(records)
    df = df.sort_values("timestamp").drop_duplicates(subset=["symbol", "timestamp"])
    return df.reset_index(drop=True)


def validate_session(df: pd.DataFrame, date_str: str) -> dict:
    """Validate session completeness for a single symbol-date."""
    if df.empty:
        return {"date": date_str, "symbol": "", "bar_count": 0, "valid": False, "issues": ["empty"]}

    symbol = df["symbol"].iloc[0]
    issues = []

    # Check bar count
    n_bars = len(df)
    if n_bars < EXPECTED_BARS_PER_SESSION * 0.95:
        issues.append(f"low_bar_count:{n_bars}/{EXPECTED_BARS_PER_SESSION}")

    # Check for duplicates (should be none after dedup)
    dups = df.duplicated(subset=["timestamp"]).sum()
    if dups > 0:
        issues.append(f"duplicates:{dups}")

    # Check ordering
    if not df["timestamp"].is_monotonic_increasing:
        issues.append("not_sorted")

    # Check for gaps > 5 seconds
    diffs = np.diff(df["timestamp"].values)
    gap_count = int(np.sum(diffs > 5))
    if gap_count > 10:  # allow a few gaps for low-volume periods
        issues.append(f"gaps:{gap_count}")

    return {
        "date": date_str,
        "symbol": symbol,
        "bar_count": n_bars,
        "valid": len(issues) == 0,
        "issues": issues,
    }


def ingest_directory(
    raw_dir: Path,
    db_path: Path,
    table_name: str = "underlying_bars",
) -> dict:
    """Ingest all JSON files from the nifty50/ directory tree into DuckDB.

    Returns summary stats of ingestion.
    """
    json_files = sorted(raw_dir.rglob("*.json"))
    if not json_files:
        log.warning("no_json_files_found", directory=str(raw_dir))
        return {"files": 0, "bars": 0, "errors": []}

    all_frames = []
    errors = []
    validations = []

    for fp in json_files:
        try:
            bars = json_to_underlying_bars(fp)
            if not bars:
                errors.append(f"empty:{fp.name}")
                continue
            df = bars_to_dataframe(bars)
            # Extract date from parent directory name
            date_str = fp.parent.name
            df["date"] = date_str
            validation = validate_session(df, date_str)
            validations.append(validation)
            all_frames.append(df)
            log.info(
                "ingested",
                file=fp.name,
                bars=len(df),
                valid=validation["valid"],
            )
        except Exception as e:
            errors.append(f"error:{fp.name}:{e}")
            log.error("ingest_error", file=fp.name, error=str(e))

    if not all_frames:
        return {"files": len(json_files), "bars": 0, "errors": errors}

    combined = pd.concat(all_frames, ignore_index=True)

    # Write to DuckDB
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM combined
        ORDER BY date, symbol, timestamp
        """
    )

    row_count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    con.close()

    log.info(
        "ingest_complete",
        files=len(json_files),
        total_bars=row_count,
        errors=len(errors),
    )

    return {
        "files": len(json_files),
        "bars": row_count,
        "errors": errors,
        "validations": validations,
    }


def query_bars(
    db_path: Path,
    symbol: str | None = None,
    date: str | None = None,
    table_name: str = "underlying_bars",
) -> pd.DataFrame:
    """Query bars from DuckDB with optional filters."""
    con = duckdb.connect(str(db_path), read_only=True)
    where_clauses = []
    if symbol:
        where_clauses.append(f"symbol = '{symbol}'")
    if date:
        where_clauses.append(f"date = '{date}'")
    where = " AND ".join(where_clauses)
    query = f"SELECT * FROM {table_name}"
    if where:
        query += f" WHERE {where}"
    query += " ORDER BY timestamp"
    df = con.execute(query).fetchdf()
    con.close()
    return df
