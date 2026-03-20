import { existsSync, mkdirSync } from "node:fs";

const AUTH = process.env.FYERS_ACCESS_TOKEN ?? "";
const TOKEN_ID = process.env.FYERS_TOKEN_ID ?? "";

if (!AUTH || !TOKEN_ID) {
  console.error("Set FYERS_ACCESS_TOKEN and FYERS_TOKEN_ID env vars");
  process.exit(1);
}

const BASE_URL = "https://api-t1.fyers.in/indus/history";
const HEADERS = {
  accept: "*/*",
  authorization: AUTH,
  origin: "https://trade.fyers.in",
  referer: "https://trade.fyers.in/",
  "user-agent": "Mozilla/5.0",
};

const SESSION_START = "09:15:00";
const SESSION_END = "15:30:00";
const INTERVAL_SECONDS = 5;
const WINDOW_SECONDS = 25 * 60;
const COUNTBACK = 329;

type FyersCandle = [number, number, number, number, number, number, number?];

export type HistoryKind = "underlying" | "option";

export type FetchHistorySpec = {
  kind: HistoryKind;
  symbol: string;
  sessionDate: string;
  outDir: string;
  exchange?: string;
  underlying?: string;
  expiry?: string;
  strike?: number;
  optionType?: "CE" | "PE";
};

export type HistoryRow = {
  kind: HistoryKind;
  source: "fyers";
  exchange: string;
  symbol: string;
  intervalSeconds: number;
  sessionDate: string;
  timestamp: string;
  epochSeconds: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  underlying?: string;
  expiry?: string;
  strike?: number;
  optionType?: "CE" | "PE";
  openInterest?: number;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function istToEpoch(date: string, time: string): number {
  return Math.floor(new Date(`${date}T${time}+05:30`).getTime() / 1000);
}

function safeFilename(symbol: string): string {
  return symbol.replaceAll(":", "_");
}

function outputPath(symbol: string, sessionDate: string, outDir: string): string {
  return `${outDir}/${safeFilename(symbol)}_${sessionDate}_5s.json`;
}

function toIsoTimestamp(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toISOString();
}

function buildParams(symbol: string, from: number, to: number): URLSearchParams {
  return new URLSearchParams({
    symbol,
    resolution: "5S",
    from: String(from),
    to: String(to),
    token_id: TOKEN_ID,
    dataReq: String(to),
    contFlag: "1",
    countback: String(COUNTBACK),
    currencyCode: "INR",
  });
}

function mapRow(spec: FetchHistorySpec, candle: FyersCandle): HistoryRow {
  const row: HistoryRow = {
    kind: spec.kind,
    source: "fyers",
    exchange: spec.exchange ?? "NSE",
    symbol: spec.symbol,
    intervalSeconds: INTERVAL_SECONDS,
    sessionDate: spec.sessionDate,
    timestamp: toIsoTimestamp(candle[0]),
    epochSeconds: candle[0],
    open: candle[1],
    high: candle[2],
    low: candle[3],
    close: candle[4],
    volume: candle[5] ?? 0,
  };

  if (spec.kind === "option") {
    row.underlying = spec.underlying;
    row.expiry = spec.expiry;
    row.strike = spec.strike;
    row.optionType = spec.optionType;
    row.openInterest = candle[6] ?? 0;
  }

  return row;
}

export async function fetchDayHistory(
  spec: FetchHistorySpec,
): Promise<{ rows: HistoryRow[]; outFile: string; skipped: boolean }> {
  const outFile = outputPath(spec.symbol, spec.sessionDate, spec.outDir);
  if (existsSync(outFile)) {
    const existing = await Bun.file(outFile).json();
    return { rows: existing as HistoryRow[], outFile, skipped: true };
  }

  if (!existsSync(spec.outDir)) {
    mkdirSync(spec.outDir, { recursive: true });
  }

  const dayStart = istToEpoch(spec.sessionDate, SESSION_START);
  const dayEnd = istToEpoch(spec.sessionDate, SESSION_END);
  const byTimestamp = new Map<number, FyersCandle>();

  let from = dayStart;
  while (from < dayEnd) {
    const to = Math.min(from + WINDOW_SECONDS, dayEnd);
    const params = buildParams(spec.symbol, from, to);
    let attempt = 0;

    while (true) {
      const response = await fetch(`${BASE_URL}?${params}`, { headers: HEADERS });

      if (response.status === 429) {
        const backoffMs = 1000 + Math.min(attempt, 5) * 500;
        await sleep(backoffMs);
        attempt += 1;
        continue;
      }

      if (!response.ok) {
        const body = await response.text();
        throw new Error(`FYERS ${response.status} for ${spec.symbol} ${spec.sessionDate}: ${body}`);
      }

      const payload = (await response.json()) as {
        candles?: FyersCandle[];
        s?: string;
        message?: string;
      };

      for (const candle of payload.candles ?? []) {
        byTimestamp.set(candle[0], candle);
      }

      break;
    }

    from = to;
    if (from < dayEnd) {
      await sleep(120);
    }
  }

  const rows = [...byTimestamp.values()]
    .sort((left, right) => left[0] - right[0])
    .map((candle) => mapRow(spec, candle));

  await Bun.write(outFile, JSON.stringify(rows, null, 2));
  return { rows, outFile, skipped: false };
}

if (import.meta.main) {
  const kind = Bun.argv[2] as HistoryKind | undefined;
  const symbol = Bun.argv[3];
  const sessionDate = Bun.argv[4];
  const outDir = Bun.argv[5];
  const underlying = Bun.argv[6];
  const expiry = Bun.argv[7];
  const strikeArg = Bun.argv[8];
  const optionType = Bun.argv[9] as "CE" | "PE" | undefined;

  if (!kind || !symbol || !sessionDate || !outDir) {
    console.error(
      "Usage: bun run fyers-history.ts <underlying|option> <symbol> <YYYY-MM-DD> <outDir> [underlying] [expiry] [strike] [CE|PE]",
    );
    process.exit(1);
  }

  const result = await fetchDayHistory({
    kind,
    symbol,
    sessionDate,
    outDir,
    underlying,
    expiry,
    strike: strikeArg ? Number(strikeArg) : undefined,
    optionType,
  });

  console.log(`${result.skipped ? "SKIP" : "SAVED"} ${result.rows.length} rows -> ${result.outFile}`);
}
