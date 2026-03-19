"""Tests for data layer: ingestion, validation, and query."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.data.ingest import (
    EXPECTED_BARS_PER_SESSION,
    bars_to_dataframe,
    ingest_directory,
    json_to_underlying_bars,
    load_json_bars,
    query_bars,
    validate_session,
)
from src.models import UnderlyingBar


@pytest.fixture
def sample_bars_json(tmp_path):
    """Create a sample JSON file with 5s bars."""
    bars = []
    base_ts = 1773287100  # 2026-03-12 09:15:00 IST approx
    for i in range(100):
        bars.append(
            {
                "timestamp": base_ts + i * 5,
                "datetime": f"test_{i}",
                "open": 23000 + i * 0.1,
                "high": 23001 + i * 0.1,
                "low": 22999 + i * 0.1,
                "close": 23000.5 + i * 0.1,
                "volume": 1000 + i,
            }
        )

    date_dir = tmp_path / "2026-03-12"
    date_dir.mkdir()
    filepath = date_dir / "NSE_RELIANCE_2026-03-12_5s.json"
    with open(filepath, "w") as f:
        json.dump(bars, f)
    return filepath


@pytest.fixture
def sample_db(tmp_path, sample_bars_json):
    """Create a DuckDB with sample data."""
    db_path = tmp_path / "test.duckdb"
    raw_dir = sample_bars_json.parent.parent
    ingest_directory(raw_dir, db_path)
    return db_path


def test_load_json_bars(sample_bars_json):
    bars = load_json_bars(sample_bars_json)
    assert len(bars) == 100
    assert bars[0]["timestamp"] == 1773287100


def test_json_to_underlying_bars(sample_bars_json):
    bars = json_to_underlying_bars(sample_bars_json)
    assert len(bars) == 100
    assert isinstance(bars[0], UnderlyingBar)
    assert bars[0].symbol == "RELIANCE"


def test_bars_to_dataframe(sample_bars_json):
    bars = json_to_underlying_bars(sample_bars_json)
    df = bars_to_dataframe(bars)
    assert len(df) == 100
    assert df["timestamp"].is_monotonic_increasing


def test_dedup_in_dataframe(sample_bars_json):
    bars = json_to_underlying_bars(sample_bars_json)
    bars_dup = bars + bars[:5]  # add duplicates
    df = bars_to_dataframe(bars_dup)
    assert len(df) == 100  # duplicates removed


def test_validate_session_low_count(sample_bars_json):
    bars = json_to_underlying_bars(sample_bars_json)
    df = bars_to_dataframe(bars)
    result = validate_session(df, "2026-03-12")
    assert not result["valid"]
    assert any("low_bar_count" in issue for issue in result["issues"])


def test_validate_session_empty():
    df = pd.DataFrame(
        columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"]
    )
    result = validate_session(df, "2026-03-12")
    assert not result["valid"]
    assert "empty" in result["issues"]


def test_ingest_directory(tmp_path, sample_bars_json):
    db_path = tmp_path / "test.duckdb"
    raw_dir = sample_bars_json.parent.parent
    result = ingest_directory(raw_dir, db_path)
    assert result["files"] == 1
    assert result["bars"] == 100
    assert len(result["errors"]) == 0


def test_query_bars(sample_db):
    df = query_bars(sample_db, symbol="RELIANCE")
    assert len(df) == 100

    df_all = query_bars(sample_db)
    assert len(df_all) == 100

    df_empty = query_bars(sample_db, symbol="NONEXISTENT")
    assert len(df_empty) == 0


def test_query_bars_by_date(sample_db):
    df = query_bars(sample_db, date="2026-03-12")
    assert len(df) == 100

    df_empty = query_bars(sample_db, date="2099-01-01")
    assert len(df_empty) == 0


def test_ingest_empty_file(tmp_path):
    """Test handling of empty JSON files."""
    date_dir = tmp_path / "2026-03-12"
    date_dir.mkdir()
    filepath = date_dir / "NSE_TATAMOTORS-EQ_2026-03-12_5s.json"
    with open(filepath, "w") as f:
        json.dump([], f)

    db_path = tmp_path / "test.duckdb"
    result = ingest_directory(tmp_path, db_path)
    assert len(result["errors"]) > 0 or result["bars"] == 0


def test_out_of_order_bars(tmp_path):
    """Test that out-of-order bars get sorted."""
    bars = [
        {"timestamp": 100, "datetime": "t3", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10},
        {"timestamp": 90, "datetime": "t1", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10},
        {"timestamp": 95, "datetime": "t2", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10},
    ]
    date_dir = tmp_path / "2026-03-12"
    date_dir.mkdir()
    filepath = date_dir / "NSE_TEST_2026-03-12_5s.json"
    with open(filepath, "w") as f:
        json.dump(bars, f)

    ubars = json_to_underlying_bars(filepath)
    df = bars_to_dataframe(ubars)
    assert df["timestamp"].is_monotonic_increasing
