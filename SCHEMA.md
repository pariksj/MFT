# Market Data Schema

The checked-in market dataset at HEAD lives in two parquet files under `data/`:

- `data/spot_candles.parquet`
- `data/option_candles.parquet`

Resolution: **5-second candles**  
Date range: **2026-03-04 to 2026-03-19**  
Sessions: **12 trading days**  
Total rows: **2,833,248**

This replaces the older layout that used `market.duckdb.zst` and a merged parquet export.

## `data/spot_candles.parquet`

162,000 rows | 3 symbols | 12 dates

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | VARCHAR | Spot symbol, for example `NSE:NIFTY50-INDEX` |
| `session_date` | DATE | Trading session date |
| `epoch` | BIGINT | Unix epoch seconds in UTC |
| `ts` | TIMESTAMP | UTC timestamp derived from `epoch` |
| `open` | DOUBLE | Open price |
| `high` | DOUBLE | High price |
| `low` | DOUBLE | Low price |
| `close` | DOUBLE | Close price |
| `volume` | BIGINT | Volume |

Symbols included:

- `NSE:NIFTY50-INDEX`
- `NSE:NIFTYBANK-INDEX`
- `NSE:INDIAVIX-INDEX`

Per-symbol coverage:

- 54,000 rows per symbol
- 4,500 candles per session

## `data/option_candles.parquet`

2,671,248 rows | 284 option contracts | 12 dates | 4 expiries overall

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | VARCHAR | Option contract symbol, for example `NSE:NIFTY2631024250CE` |
| `underlying` | VARCHAR | `NSE:NIFTY50-INDEX` or `NSE:NIFTYBANK-INDEX` |
| `session_date` | DATE | Trading session date |
| `expiry` | DATE | Option expiry date |
| `strike` | DOUBLE | Strike price |
| `option_type` | VARCHAR | `CE` or `PE` |
| `epoch` | BIGINT | Unix epoch seconds in UTC |
| `ts` | TIMESTAMP | UTC timestamp derived from `epoch` |
| `open` | DOUBLE | Open price |
| `high` | DOUBLE | High price |
| `low` | DOUBLE | Low price |
| `close` | DOUBLE | Close price |
| `volume` | BIGINT | Traded volume |
| `open_interest` | BIGINT | Open interest |

Coverage summary:

- NIFTY options: 160 contracts, 3 expiries, 43 strikes, 1,196,978 rows
- BANKNIFTY options: 124 contracts, 1 expiry, 62 strikes, 1,474,270 rows

By underlying and side:

| Underlying | Option Type | Rows | Distinct Strikes | Distinct Expiries |
|-----------|-------------|------|------------------|-------------------|
| `NSE:NIFTY50-INDEX` | `CE` | 598,478 | 43 | 3 |
| `NSE:NIFTY50-INDEX` | `PE` | 598,500 | 43 | 3 |
| `NSE:NIFTYBANK-INDEX` | `CE` | 736,528 | 62 | 1 |
| `NSE:NIFTYBANK-INDEX` | `PE` | 737,742 | 62 | 1 |

## Time Semantics

All timestamps are stored in UTC. Indian market hours of `09:15` to `15:30` IST correspond to `03:45` to `10:00` UTC.

To inspect values in IST with DuckDB:

```sql
SELECT ts, ts + INTERVAL 5 HOUR + INTERVAL 30 MINUTE AS ts_ist
FROM 'data/spot_candles.parquet'
LIMIT 5;
```

## Example Queries

```sql
-- NIFTY 50 spot candles for one date
SELECT *
FROM 'data/spot_candles.parquet'
WHERE symbol = 'NSE:NIFTY50-INDEX'
  AND session_date = '2026-03-18'
ORDER BY epoch;

-- NIFTY 24000 CE option candles for one date
SELECT *
FROM 'data/option_candles.parquet'
WHERE underlying = 'NSE:NIFTY50-INDEX'
  AND strike = 24000
  AND option_type = 'CE'
  AND session_date = '2026-03-18'
ORDER BY epoch;

-- Distinct expiries available for NIFTY
SELECT DISTINCT expiry
FROM 'data/option_candles.parquet'
WHERE underlying = 'NSE:NIFTY50-INDEX'
ORDER BY expiry;

-- Join spot with ATM call and put prices on each tick
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
