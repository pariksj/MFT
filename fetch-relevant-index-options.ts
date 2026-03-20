import { existsSync, readdirSync } from "node:fs";
import { fetchDayHistory, type HistoryRow } from "./fyers-history";

const ROOT = "/Users/pariksj/Desktop/SigiQ/test-data";
const NIFTY_DIR = `${ROOT}/nifty50`;
const NIFTY_OPTIONS_DIR = `${ROOT}/nifty50-options`;
const BANKNIFTY_DIR = `${ROOT}/banknifty`;
const BANKNIFTY_OPTIONS_DIR = `${ROOT}/banknifty-options`;

const NIFTY_SPOT = "NSE:NIFTY50-INDEX";
const BANKNIFTY_SPOT = "NSE:NIFTYBANK-INDEX";

const NIFTY_STEP = 50;
const BANKNIFTY_STEP = 100;
const BUFFER_STEPS = 1;

function parseDate(dateStr: string): Date {
  return new Date(`${dateStr}T00:00:00+05:30`);
}

function formatDate(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addDays(date: Date, days: number): Date {
  const next = new Date(date.getTime());
  next.setDate(next.getDate() + days);
  return next;
}

function nextWeekdayInclusive(date: Date, targetDay: number): Date {
  const currentDay = date.getDay();
  const delta = (targetDay - currentDay + 7) % 7;
  return addDays(date, delta);
}

function lastWeekdayOfMonth(year: number, monthIndex: number, targetDay: number): Date {
  const cursor = new Date(Date.UTC(year, monthIndex + 1, 0, 0, 0, 0));
  while (cursor.getUTCDay() !== targetDay) {
    cursor.setUTCDate(cursor.getUTCDate() - 1);
  }
  return new Date(
    `${cursor.getUTCFullYear()}-${String(cursor.getUTCMonth() + 1).padStart(2, "0")}-${String(cursor.getUTCDate()).padStart(2, "0")}T00:00:00+05:30`,
  );
}

function isLastTuesdayOfMonth(date: Date): boolean {
  const lastTuesday = lastWeekdayOfMonth(date.getFullYear(), date.getMonth(), 2);
  return formatDate(date) === formatDate(lastTuesday);
}

function getNiftyFrontExpiry(sessionDate: string): string {
  const date = parseDate(sessionDate);
  const expiry = nextWeekdayInclusive(date, 2);
  return formatDate(expiry);
}

function getBankNiftyFrontExpiry(sessionDate: string): string {
  const date = parseDate(sessionDate);
  let expiry = lastWeekdayOfMonth(date.getFullYear(), date.getMonth(), 4);
  if (expiry.getTime() < date.getTime()) {
    const nextMonth = new Date(date.getTime());
    nextMonth.setMonth(nextMonth.getMonth() + 1);
    expiry = lastWeekdayOfMonth(nextMonth.getFullYear(), nextMonth.getMonth(), 4);
  }
  return formatDate(expiry);
}

function weeklyExpiryCode(expiry: string): string {
  const date = parseDate(expiry);
  const year = String(date.getFullYear() % 100).padStart(2, "0");
  const month = String(date.getMonth() + 1);
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}${month}${day}`;
}

function monthlyExpiryCode(expiry: string): string {
  const date = parseDate(expiry);
  const year = String(date.getFullYear() % 100).padStart(2, "0");
  const month = date.toLocaleString("en-US", {
    month: "short",
    timeZone: "Asia/Kolkata",
  }).toUpperCase();
  return `${year}${month}`;
}

function buildNiftyOptionSymbol(expiry: string, strike: number, optionType: "CE" | "PE"): string {
  const expiryCode = isLastTuesdayOfMonth(parseDate(expiry))
    ? monthlyExpiryCode(expiry)
    : weeklyExpiryCode(expiry);
  return `NSE:NIFTY${expiryCode}${strike}${optionType}`;
}

function buildBankNiftyOptionSymbol(expiry: string, strike: number, optionType: "CE" | "PE"): string {
  return `NSE:BANKNIFTY${monthlyExpiryCode(expiry)}${strike}${optionType}`;
}

function safeFilePath(baseDir: string, sessionDate: string, symbol: string): string {
  return `${baseDir}/${sessionDate}/${symbol.replaceAll(":", "_")}_${sessionDate}_5s.json`;
}

async function loadRows(filePath: string): Promise<HistoryRow[]> {
  return (await Bun.file(filePath).json()) as HistoryRow[];
}

function strikeRange(low: number, high: number, step: number): number[] {
  const start = Math.floor(low / step) * step - BUFFER_STEPS * step;
  const end = Math.ceil(high / step) * step + BUFFER_STEPS * step;
  const strikes: number[] = [];
  for (let strike = start; strike <= end; strike += step) {
    strikes.push(strike);
  }
  return strikes;
}

function summarizeRange(rows: HistoryRow[]): { low: number; high: number } {
  const low = Math.min(...rows.map((row) => row.low));
  const high = Math.max(...rows.map((row) => row.high));
  return { low, high };
}

async function ensureBankNiftySpot(sessionDate: string): Promise<HistoryRow[]> {
  const existingPath = safeFilePath(BANKNIFTY_DIR, sessionDate, BANKNIFTY_SPOT);
  if (existsSync(existingPath)) {
    return loadRows(existingPath);
  }

  const result = await fetchDayHistory({
    kind: "underlying",
    symbol: BANKNIFTY_SPOT,
    sessionDate,
    outDir: `${BANKNIFTY_DIR}/${sessionDate}`,
  });
  return result.rows;
}

async function ensureNiftySpot(sessionDate: string): Promise<HistoryRow[]> {
  const existingPath = safeFilePath(NIFTY_DIR, sessionDate, NIFTY_SPOT);
  if (!existsSync(existingPath)) {
    throw new Error(`Missing NIFTY spot file for ${sessionDate}: ${existingPath}`);
  }
  return loadRows(existingPath);
}

async function fetchOptionSet(config: {
  sessionDate: string;
  expiry: string;
  strikes: number[];
  outRoot: string;
  underlyingSpot: string;
  buildSymbol: (expiry: string, strike: number, optionType: "CE" | "PE") => string;
}): Promise<{ fetched: number; skipped: number; failed: number }> {
  let fetched = 0;
  let skipped = 0;
  let failed = 0;

  for (const strike of config.strikes) {
    for (const optionType of ["CE", "PE"] as const) {
      const symbol = config.buildSymbol(config.expiry, strike, optionType);
      try {
        const result = await fetchDayHistory({
          kind: "option",
          symbol,
          sessionDate: config.sessionDate,
          outDir: `${config.outRoot}/${config.sessionDate}`,
          underlying: config.underlyingSpot,
          expiry: config.expiry,
          strike,
          optionType,
        });

        if (result.skipped) {
          skipped += 1;
        } else {
          fetched += 1;
        }
      } catch (error) {
        failed += 1;
        console.error(`FAILED ${config.sessionDate} ${symbol}: ${(error as Error).message}`);
      }
    }
  }

  return { fetched, skipped, failed };
}

function getSessionDates(): string[] {
  return readdirSync(NIFTY_DIR, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && /^\d{4}-\d{2}-\d{2}$/.test(entry.name))
    .map((entry) => entry.name)
    .sort();
}

const sessionDates = getSessionDates();

let totalFetched = 0;
let totalSkipped = 0;
let totalFailed = 0;

for (const sessionDate of sessionDates) {
  console.log(`\n=== ${sessionDate} ===`);

  const niftySpotRows = await ensureNiftySpot(sessionDate);
  const niftyRange = summarizeRange(niftySpotRows);
  const niftyExpiry = getNiftyFrontExpiry(sessionDate);
  const niftyStrikes = strikeRange(niftyRange.low, niftyRange.high, NIFTY_STEP);
  console.log(
    `NIFTY expiry ${niftyExpiry} | range ${niftyRange.low.toFixed(2)}-${niftyRange.high.toFixed(2)} | strikes ${niftyStrikes[0]}-${niftyStrikes[niftyStrikes.length - 1]} (${niftyStrikes.length})`,
  );

  const niftySummary = await fetchOptionSet({
    sessionDate,
    expiry: niftyExpiry,
    strikes: niftyStrikes,
    outRoot: NIFTY_OPTIONS_DIR,
    underlyingSpot: NIFTY_SPOT,
    buildSymbol: buildNiftyOptionSymbol,
  });

  totalFetched += niftySummary.fetched;
  totalSkipped += niftySummary.skipped;
  totalFailed += niftySummary.failed;

  const bankSpotRows = await ensureBankNiftySpot(sessionDate);
  const bankRange = summarizeRange(bankSpotRows);
  const bankExpiry = getBankNiftyFrontExpiry(sessionDate);
  const bankStrikes = strikeRange(bankRange.low, bankRange.high, BANKNIFTY_STEP);
  console.log(
    `BANKNIFTY expiry ${bankExpiry} | range ${bankRange.low.toFixed(2)}-${bankRange.high.toFixed(2)} | strikes ${bankStrikes[0]}-${bankStrikes[bankStrikes.length - 1]} (${bankStrikes.length})`,
  );

  const bankSummary = await fetchOptionSet({
    sessionDate,
    expiry: bankExpiry,
    strikes: bankStrikes,
    outRoot: BANKNIFTY_OPTIONS_DIR,
    underlyingSpot: BANKNIFTY_SPOT,
    buildSymbol: buildBankNiftyOptionSymbol,
  });

  totalFetched += bankSummary.fetched;
  totalSkipped += bankSummary.skipped;
  totalFailed += bankSummary.failed;
}

console.log(`\nDONE fetched=${totalFetched} skipped=${totalSkipped} failed=${totalFailed}`);
