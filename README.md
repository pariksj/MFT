# MFT

Research-first market data and execution sandbox for NIFTY 5s options scalping. This repo now contains two distinct layers:

- Python strategy, feature, backtest, visualization, and live/paper-trading code under `src/`
- checked-in parquet market data snapshots under `data/`

Shared operating context for the wider Eltsuh workspace should stay in `eltsuh-ops`. This repo should stay focused on market data artifacts, ingestion utilities, strategy code, and tests.

## Current Data Snapshot

Recent commits replaced the compressed DuckDB artifact with split parquet files:

- `data/spot_candles.parquet`: 162,000 5-second candles, 3 symbols, 12 sessions, `2026-03-04` to `2026-03-19`
- `data/option_candles.parquet`: 2,671,248 5-second candles, 284 contracts, 2 underlyings, `2026-03-04` to `2026-03-19`
- Spot coverage now includes `NSE:NIFTY50-INDEX`, `NSE:NIFTYBANK-INDEX`, and `NSE:INDIAVIX-INDEX`

See [SCHEMA.md](/Users/rohan/repos/eltsuh/MFT/SCHEMA.md) for the current parquet schema and example queries.

## Setup

Install the Python package and development tools:

```bash
python3 -m pip install -e '.[dev]'
```

If you need Bun-based collection utilities, install Bun separately. The Bun files in the repo are support scripts, not the primary app entrypoint.

## Common Commands

Use the packaged Python entrypoints:

```bash
collect-history --raw-dir <raw-json-dir> --db-path ./data/nifty_scalper.duckdb
build-dataset --db-path ./data/nifty_scalper.duckdb --symbol NIFTY50-INDEX
backtest --features ./data/processed/features.parquet
paper-trade --paper
```

`collect-history` expects a local raw JSON tree; the checked-in snapshot in this repo is parquet-only.

The visualization command exists on the Click CLI but is not exposed as a setuptools script in `pyproject.toml`, so run it as:

```bash
python3 -m src.cli visualize --features ./data/processed/features.parquet --report ./data/processed/backtest_report.json
```

## Query The Checked-In Parquet

DuckDB can query the committed parquet files directly:

```bash
duckdb -c "SELECT count(*) FROM 'data/spot_candles.parquet';"
duckdb -c "SELECT underlying, count(*) FROM 'data/option_candles.parquet' GROUP BY 1 ORDER BY 1;"
```

Example join:

```sql
SELECT s.ts, s.close AS spot, c.close AS call_price, p.close AS put_price
FROM 'data/spot_candles.parquet' s
JOIN 'data/option_candles.parquet' c
  ON c.epoch = s.epoch
 AND c.session_date = s.session_date
 AND c.underlying = 'NSE:NIFTY50-INDEX'
 AND c.strike = 24000
 AND c.option_type = 'CE'
JOIN 'data/option_candles.parquet' p
  ON p.epoch = s.epoch
 AND p.session_date = s.session_date
 AND p.underlying = 'NSE:NIFTY50-INDEX'
 AND p.strike = 24000
 AND p.option_type = 'PE'
WHERE s.symbol = 'NSE:NIFTY50-INDEX'
  AND s.session_date = '2026-03-18'
ORDER BY s.epoch;
```

## Repo Layout

- `src/`: Python package for ingestion, feature engineering, regime logic, simulation, visualization, and live trading adapters
- `tests/`: pytest suite covering data, feature, live, and strategy modules
- `data/`: checked-in parquet snapshots for spot and option candles
- `SCHEMA.md`: current parquet schema and coverage notes
- root `*.py` and `*.ts`: collection/build utilities, including FYERS fetchers and legacy Bun scripts

## Notes

- `fetch_relevant_index_options.py`, `fetch_indiavix.py`, and Bun fetch scripts depend on `FYERS_ACCESS_TOKEN` and `FYERS_TOKEN_ID`
- `decompress.sh` is a legacy helper for older revisions that stored `market.duckdb.zst`; the current HEAD stores parquet files in `data/`
