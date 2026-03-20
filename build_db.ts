/**
 * build_db.ts — Load all 5s OHLCV data into a structured DuckDB database.
 *
 * Tables:
 *   spot_candles     — NIFTY50-INDEX, NIFTYBANK-INDEX, INDIAVIX-INDEX, and Nifty 50 stock 5s candles
 *   option_candles   — NIFTY + BANKNIFTY option chain 5s candles
 *
 * Handles two JSON formats:
 *   "rich"   (has kind, symbol, epochSeconds, etc.)
 *   "simple" (has timestamp, datetime, open, high, low, close, volume)
 *
 * Usage: bun run build_db.ts
 */

import { Database } from "duckdb-async";
import { readdirSync, existsSync } from "fs";
import { join, basename } from "path";

const BASE = "/Users/pariksj/Desktop/SigiQ/test-data";
const DB_PATH = join(BASE, "market.duckdb");

// --- helpers ---

function listJsonFiles(dir: string): string[] {
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.endsWith("_5s.json"))
    .map((f) => join(dir, f));
}

function dateSubdirs(root: string): string[] {
  if (!existsSync(root)) return [];
  return readdirSync(root)
    .filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d))
    .sort()
    .map((d) => join(root, d));
}

// Parse symbol + metadata from filename for simple-format files
// e.g. NSE_ADANIENT-EQ_2026-03-12_5s.json
function parseFilename(fname: string) {
  const base = basename(fname, "_5s.json"); // NSE_ADANIENT-EQ_2026-03-12
  const parts = base.split("_");
  const date = parts.pop()!; // 2026-03-12
  const exchange = parts.shift()!; // NSE
  const rest = parts.join("_"); // ADANIENT-EQ or NIFTY50-INDEX
  return { symbol: `${exchange}:${rest}`, sessionDate: date };
}

// --- main ---

async function main() {
  // Remove old DB if exists
  if (existsSync(DB_PATH)) {
    const { unlinkSync } = await import("fs");
    unlinkSync(DB_PATH);
    console.log("Removed old database");
  }

  const db = await Database.create(DB_PATH);

  // Create tables
  await db.run(`
    CREATE TABLE spot_candles (
      symbol        VARCHAR NOT NULL,
      session_date  DATE NOT NULL,
      epoch         BIGINT NOT NULL,
      ts            TIMESTAMP NOT NULL,
      open          DOUBLE NOT NULL,
      high          DOUBLE NOT NULL,
      low           DOUBLE NOT NULL,
      close         DOUBLE NOT NULL,
      volume        BIGINT NOT NULL
    );
  `);

  await db.run(`
    CREATE TABLE option_candles (
      symbol        VARCHAR NOT NULL,
      underlying    VARCHAR NOT NULL,
      session_date  DATE NOT NULL,
      expiry        DATE NOT NULL,
      strike        DOUBLE NOT NULL,
      option_type   VARCHAR(2) NOT NULL,
      epoch         BIGINT NOT NULL,
      ts            TIMESTAMP NOT NULL,
      open          DOUBLE NOT NULL,
      high          DOUBLE NOT NULL,
      low           DOUBLE NOT NULL,
      close         DOUBLE NOT NULL,
      volume        BIGINT NOT NULL,
      open_interest BIGINT
    );
  `);

  let spotRows = 0;
  let optionRows = 0;

  // --- Load spot data ---
  const spotDirs = [
    join(BASE, "nifty50"),
    join(BASE, "nifty50-5s"),
    join(BASE, "banknifty-5s"),
    join(BASE, "indiavix-5s"),
  ];

  for (const root of spotDirs) {
    for (const dateDir of dateSubdirs(root)) {
      for (const file of listJsonFiles(dateDir)) {
        const data = JSON.parse(await Bun.file(file).text()) as any[];
        if (data.length === 0) continue;

        const isRich = "kind" in data[0];
        const { symbol, sessionDate } = isRich
          ? { symbol: data[0].symbol, sessionDate: data[0].sessionDate }
          : parseFilename(file);

        const stmt = await db.prepare(`
          INSERT INTO spot_candles VALUES (?, ?, ?, epoch_ms(?::BIGINT * 1000), ?, ?, ?, ?, ?)
        `);

        for (const row of data) {
          const epoch = isRich ? row.epochSeconds : row.timestamp;
          await stmt.run(
            symbol,
            sessionDate,
            epoch,
            epoch,
            row.open,
            row.high,
            row.low,
            row.close,
            row.volume
          );
          spotRows++;
        }
        await stmt.finalize();

        process.stdout.write(`\rSpot: ${spotRows.toLocaleString()} rows loaded`);
      }
    }
  }
  console.log(`\nSpot loading complete: ${spotRows.toLocaleString()} rows`);

  // --- Load options data ---
  const optionDirs = [
    { root: join(BASE, "nifty50-options"), underlying: "NSE:NIFTY50-INDEX" },
    { root: join(BASE, "banknifty-options"), underlying: "NSE:NIFTYBANK-INDEX" },
  ];

  for (const { root, underlying: defaultUnderlying } of optionDirs) {
    for (const dateDir of dateSubdirs(root)) {
      for (const file of listJsonFiles(dateDir)) {
        const data = JSON.parse(await Bun.file(file).text()) as any[];
        if (data.length === 0) continue;

        const isRich = "kind" in data[0];

        // For simple format, parse from filename
        let symbol: string, sessionDate: string, underlying: string, expiry: string, strike: number, optionType: string;

        if (isRich) {
          symbol = data[0].symbol;
          sessionDate = data[0].sessionDate;
          underlying = data[0].underlying;
          expiry = data[0].expiry;
          strike = data[0].strike;
          optionType = data[0].optionType;
        } else {
          const parsed = parseFilename(file);
          symbol = parsed.symbol;
          sessionDate = parsed.sessionDate;
          underlying = defaultUnderlying;
          // Would need to parse from symbol name — but all option files should be rich format
          expiry = "";
          strike = 0;
          optionType = "";
        }

        const stmt = await db.prepare(`
          INSERT INTO option_candles VALUES (?, ?, ?, ?, ?, ?, ?, epoch_ms(?::BIGINT * 1000), ?, ?, ?, ?, ?, ?)
        `);

        for (const row of data) {
          const epoch = isRich ? row.epochSeconds : row.timestamp;
          const oi = isRich ? (row.openInterest ?? null) : null;
          await stmt.run(
            symbol,
            underlying,
            sessionDate,
            expiry,
            strike,
            optionType,
            epoch,
            epoch,
            row.open,
            row.high,
            row.low,
            row.close,
            row.volume,
            oi
          );
          optionRows++;
        }
        await stmt.finalize();

        process.stdout.write(`\rOptions: ${optionRows.toLocaleString()} rows loaded`);
      }
    }
  }
  console.log(`\nOptions loading complete: ${optionRows.toLocaleString()} rows`);

  // --- Create indexes ---
  console.log("Creating indexes...");
  await db.run(`CREATE INDEX idx_spot_sym_date ON spot_candles (symbol, session_date, epoch);`);
  await db.run(`CREATE INDEX idx_opt_sym_date ON option_candles (symbol, session_date, epoch);`);
  await db.run(`CREATE INDEX idx_opt_underlying ON option_candles (underlying, session_date, expiry, strike, option_type);`);

  // --- Summary ---
  const spotCount = (await db.all(`SELECT count(*) as c FROM spot_candles`))[0].c;
  const optCount = (await db.all(`SELECT count(*) as c FROM option_candles`))[0].c;
  const spotSymbols = (await db.all(`SELECT count(DISTINCT symbol) as c FROM spot_candles`))[0].c;
  const optSymbols = (await db.all(`SELECT count(DISTINCT symbol) as c FROM option_candles`))[0].c;
  const spotDates = (await db.all(`SELECT count(DISTINCT session_date) as c FROM spot_candles`))[0].c;
  const optDates = (await db.all(`SELECT count(DISTINCT session_date) as c FROM option_candles`))[0].c;

  console.log(`\n=== DATABASE SUMMARY ===`);
  console.log(`Spot:    ${Number(spotCount).toLocaleString()} rows | ${spotSymbols} symbols | ${spotDates} dates`);
  console.log(`Options: ${Number(optCount).toLocaleString()} rows | ${optSymbols} symbols | ${optDates} dates`);
  console.log(`Total:   ${(Number(spotCount) + Number(optCount)).toLocaleString()} rows`);
  console.log(`DB file: ${DB_PATH}`);

  await db.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
