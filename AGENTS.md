# AGENTS.md

## Scope

This repo is the bounded MFT workspace for NIFTY and BANKNIFTY 5-second market data, strategy research, and paper/live execution code. Keep shared workspace planning, product memory, and cross-repo coordination in `eltsuh-ops`, not duplicated here.

## Working Defaults

- Prefer Python for net-new platform work under `src/` and `tests/`
- Treat the root TypeScript and Bun files as support tooling for collection and conversion flows; extend them only when you are intentionally working in that path
- Keep this repo narrow: market data artifacts, ingestion utilities, feature generation, strategy logic, evaluation, and execution adapters

## Data Artifacts

- The current checked-in market snapshot lives in `data/spot_candles.parquet` and `data/option_candles.parquet`
- Do not rewrite, regenerate, or replace large binary data artifacts unless the user explicitly asks for it
- Before documenting schema, coverage, dates, or row counts, verify claims against the committed parquet files with DuckDB or another local query tool
- `decompress.sh` is for older revisions that used `market.duckdb.zst`; do not treat it as the primary path for current HEAD

## Documentation

- Update `README.md` when repo purpose, entrypoints, setup, or artifact layout changes
- Update `SCHEMA.md` when parquet structure, row counts, symbol coverage, or query examples change
- Keep docs explicit about whether a script is current, legacy, or credential-gated

## External APIs And Secrets

- Scripts that call FYERS or broker APIs require explicit user intent and should rely on environment variables such as `FYERS_ACCESS_TOKEN` and `FYERS_TOKEN_ID`
- Never hardcode credentials or commit derived secrets, tokens, or session dumps

## Verification

- For Python code changes, run targeted `pytest` coverage first and broaden only as needed
- For documentation-only changes, verify commands, paths, and counts against the repository contents before finalizing
