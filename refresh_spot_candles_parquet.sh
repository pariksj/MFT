#!/bin/zsh

set -euo pipefail

BASE="/Users/pariksj/Desktop/SigiQ/test-data"
TARGET="$BASE/data/spot_candles.parquet"
TMP="$BASE/data/spot_candles.parquet.next"
BACKUP="$BASE/data/spot_candles.parquet.bak"

if [[ -f "$TARGET" ]]; then
  cp "$TARGET" "$BACKUP"
fi

duckdb -c "
COPY (
  WITH existing AS (
    SELECT *
    FROM '$TARGET'
  ),
  new_rows AS (
    SELECT
      symbol,
      CAST(sessionDate AS DATE) AS session_date,
      epochSeconds::BIGINT AS epoch,
      CAST(timestamp AS TIMESTAMP) AS ts,
      open::DOUBLE AS open,
      high::DOUBLE AS high,
      low::DOUBLE AS low,
      close::DOUBLE AS close,
      volume::BIGINT AS volume
    FROM read_json_auto(
      [
        '$BASE/nifty50-5s/*/*.json',
        '$BASE/banknifty-5s/*/*.json',
        '$BASE/indiavix-5s/*/*.json'
      ],
      maximum_object_size=10485760
    )
    WHERE kind = 'underlying' AND intervalSeconds = 5
  ),
  merged AS (
    SELECT * FROM existing
    UNION ALL
    SELECT * FROM new_rows
  )
  SELECT
    symbol,
    session_date,
    epoch,
    ts,
    open,
    high,
    low,
    close,
    volume
  FROM merged
  QUALIFY row_number() OVER (PARTITION BY symbol, epoch ORDER BY session_date DESC) = 1
  ORDER BY session_date, symbol, epoch
) TO '$TMP' (FORMAT PARQUET, COMPRESSION ZSTD);
"

mv "$TMP" "$TARGET"
echo "Updated $TARGET"
