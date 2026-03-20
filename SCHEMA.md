# Market Data Schema

Database files in this directory:

- **`market.duckdb`** — DuckDB database (238 MB)
- **`market.parquet`** — Single Parquet file, ZSTD compressed (44 MB)

Resolution: **5-second candles**
Date range: **2026-03-04 to 2026-03-19** (12 trading days)
Total rows: **4,327,233**

---

## DuckDB Tables

### `spot_candles` — Index & Stock 5s OHLCV

1,655,985 rows | 52 symbols | 12 dates

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `symbol` | VARCHAR | NO | Fyers symbol, e.g. `NSE:NIFTY50-INDEX`, `NSE:RELIANCE-EQ` |
| `session_date` | DATE | NO | Trading day (`2026-03-04` to `2026-03-19`) |
| `epoch` | BIGINT | NO | Unix epoch seconds (UTC) |
| `ts` | TIMESTAMP | NO | UTC timestamp derived from epoch |
| `open` | DOUBLE | NO | Open price |
| `high` | DOUBLE | NO | High price |
| `low` | DOUBLE | NO | Low price |
| `close` | DOUBLE | NO | Close price |
| `volume` | BIGINT | NO | Volume |

**Indexes:**
- `idx_spot_sym_date` on (`symbol`, `session_date`, `epoch`)

**Symbols included:**
- `NSE:NIFTY50-INDEX` — NIFTY 50 index (all 12 dates)
- `NSE:NIFTYBANK-INDEX` — Bank NIFTY index (all 12 dates)
- 50 Nifty stocks (e.g. `NSE:RELIANCE-EQ`, `NSE:HDFCBANK-EQ`, etc.) — 7 dates (Mar 11-19)

**Note on timestamps:** All timestamps are UTC. Indian market hours (9:15 AM - 3:30 PM IST) correspond to 3:45 AM - 10:00 AM UTC. To convert to IST in queries: `ts + INTERVAL 5 HOUR 30 MINUTE`.

---

### `option_candles` — NIFTY & BANKNIFTY Option Chain 5s OHLCV

2,671,248 rows | 284 option contracts | 12 dates

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `symbol` | VARCHAR | NO | Option contract symbol, e.g. `NSE:NIFTY2631024250CE` |
| `underlying` | VARCHAR | NO | Underlying index: `NSE:NIFTY50-INDEX` or `NSE:NIFTYBANK-INDEX` |
| `session_date` | DATE | NO | Trading day |
| `expiry` | DATE | NO | Option expiry date |
| `strike` | DOUBLE | NO | Strike price |
| `option_type` | VARCHAR(2) | NO | `CE` (Call) or `PE` (Put) |
| `epoch` | BIGINT | NO | Unix epoch seconds (UTC) |
| `ts` | TIMESTAMP | NO | UTC timestamp derived from epoch |
| `open` | DOUBLE | NO | Option open price |
| `high` | DOUBLE | NO | Option high price |
| `low` | DOUBLE | NO | Option low price |
| `close` | DOUBLE | NO | Option close price |
| `volume` | BIGINT | NO | Option volume |
| `open_interest` | BIGINT | YES | Open interest (NULL for some rows) |

**Indexes:**
- `idx_opt_sym_date` on (`symbol`, `session_date`, `epoch`)
- `idx_opt_underlying` on (`underlying`, `session_date`, `expiry`, `strike`, `option_type`)

**Coverage:**
- NIFTY options: 160 contracts, 3 expiries, 43 strikes, 1,196,978 rows
- BANKNIFTY options: 124 contracts, 1 expiry, 62 strikes, 1,474,270 rows

---

## Parquet File

`market.parquet` contains both tables merged with an additional discriminator column:

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | VARCHAR | `spot` or `option` |
| `symbol` | VARCHAR | Same as above |
| `underlying` | VARCHAR | NULL for spot rows |
| `session_date` | DATE | Trading day |
| `expiry` | DATE | NULL for spot rows |
| `strike` | DOUBLE | NULL for spot rows |
| `option_type` | VARCHAR | NULL for spot rows |
| `epoch` | BIGINT | Unix epoch seconds |
| `ts` | TIMESTAMP | UTC timestamp |
| `open` | DOUBLE | Open price |
| `high` | DOUBLE | High price |
| `low` | DOUBLE | Low price |
| `close` | DOUBLE | Close price |
| `volume` | BIGINT | Volume |
| `open_interest` | BIGINT | NULL for spot rows |

---

## Example Queries

```sql
-- NIFTY 50 spot candles for a specific date
SELECT * FROM spot_candles
WHERE symbol = 'NSE:NIFTY50-INDEX' AND session_date = '2026-03-18'
ORDER BY epoch;

-- NIFTY 24000 CE option candles on a given day
SELECT * FROM option_candles
WHERE underlying = 'NSE:NIFTY50-INDEX'
  AND strike = 24000 AND option_type = 'CE'
  AND session_date = '2026-03-18'
ORDER BY epoch;

-- Join spot + ATM option for NIFTY at each 5s tick
SELECT s.ts, s.close AS spot,
       c.close AS call_price, p.close AS put_price
FROM spot_candles s
JOIN option_candles c ON c.epoch = s.epoch
  AND c.underlying = 'NSE:NIFTY50-INDEX'
  AND c.strike = 24000 AND c.option_type = 'CE'
  AND c.session_date = s.session_date
JOIN option_candles p ON p.epoch = s.epoch
  AND p.underlying = 'NSE:NIFTY50-INDEX'
  AND p.strike = 24000 AND p.option_type = 'PE'
  AND p.session_date = s.session_date
WHERE s.symbol = 'NSE:NIFTY50-INDEX'
  AND s.session_date = '2026-03-18'
ORDER BY s.epoch;

-- All available strikes for NIFTY options on a date
SELECT DISTINCT strike, option_type, expiry
FROM option_candles
WHERE underlying = 'NSE:NIFTY50-INDEX' AND session_date = '2026-03-18'
ORDER BY strike, option_type;

-- Query parquet directly (no DuckDB file needed)
SELECT * FROM 'market.parquet'
WHERE table_name = 'spot' AND symbol = 'NSE:NIFTY50-INDEX'
LIMIT 10;
```

---

## File Locations

```
test-data/
  market.duckdb          # DuckDB database
  market.parquet         # Single Parquet export
  build_db.ts            # Script to rebuild DuckDB from JSON
  nifty50/               # NIFTY 50 index + stock JSON files by date
  banknifty/             # Bank NIFTY index JSON files by date
  nifty50-options/       # NIFTY option chain JSON files by date
  banknifty-options/     # Bank NIFTY option chain JSON files by date
```
