// Load credentials from environment variables — never hardcode secrets
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

const symbol = Bun.argv[2];
const dateStr = Bun.argv[3];
const outDir = Bun.argv[4] || ".";
if (!symbol || !dateStr) {
  console.error("Usage: bun run data.ts <symbol> <YYYY-MM-DD> [outDir]");
  process.exit(1);
}

// Convert IST time string to epoch seconds for the given date
function istToEpoch(date: string, time: string): number {
  return Math.floor(new Date(`${date}T${time}+05:30`).getTime() / 1000);
}

const dayStart = istToEpoch(dateStr, "09:15:00");
const dayEnd = istToEpoch(dateStr, "15:30:00");
const windowSecs = 25 * 60; // 25 minutes

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const jitter = () => 800 + Math.floor(Math.random() * 1200); // 800–2000ms random delay

function formatTime(epoch: number): string {
  return new Date(epoch * 1000).toLocaleTimeString("en-IN", {
    timeZone: "Asia/Kolkata",
    hour12: true,
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  });
}

type Candle = [number, number, number, number, number, number]; // ts, open, high, low, close, vol
const allCandles: Map<number, Candle> = new Map();

let from = dayStart;

while (from < dayEnd) {
  const to = Math.min(from + windowSecs, dayEnd);
  const countback = 300;

  console.log(`Fetching ${formatTime(from)} → ${formatTime(to)}`);

  const params = new URLSearchParams({
    symbol,
    resolution: "5S",
    from: String(from),
    to: String(to),
    token_id: TOKEN_ID,
    dataReq: String(to),
    contFlag: "1",
    countback: String(countback),
    currencyCode: "INR",
  });

  let success = false;
  while (!success) {
    const res = await fetch(`${BASE_URL}?${params}`, { headers: HEADERS });

    if (res.status === 429) {
      const backoff = 2000 + Math.floor(Math.random() * 2000);
      console.log(`Rate limited (429), sleeping ${backoff}ms...`);
      await sleep(backoff);
      continue;
    }

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`HTTP ${res.status}: ${body}`);
    }

    const json = (await res.json()) as { candles?: Candle[] };
    const candles = json.candles ?? [];
    for (const c of candles) {
      allCandles.set(c[0], c);
    }
    console.log(`  Got ${candles.length} candles`);
    success = true;
  }

  from = to;
  if (from < dayEnd) await sleep(jitter());
}

const sorted = [...allCandles.values()].sort((a, b) => a[0] - b[0]);

const labeled = sorted.map((c) => ({
  timestamp: c[0],
  datetime: new Date(c[0] * 1000).toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }),
  open: c[1],
  high: c[2],
  low: c[3],
  close: c[4],
  volume: c[5],
}));

const outName = `${outDir}/${symbol.replace(":", "_")}_${dateStr}_5s.json`;
await Bun.write(outName, JSON.stringify(labeled, null, 2));
console.log(`Saved ${labeled.length} candles to ${outName}`);
