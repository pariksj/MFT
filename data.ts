const AUTH = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2NvdW50X3N0YXR1cyI6IkFDVElWRSIsImFwcFR5cGUiOiIiLCJhdF9oYXNoIjoiZ0FBQUFBQnB1OF9BX3lObkZkSnFQUndJVTNZU3ZESTVvWDVUVXFCd2dsLURsTkZad28zaEhEY1VCd0VyeUxEWV8xRVVuZHlfUTZLSkhGT1ZxbG5wZ3NtT0VONy1yWktmbHdRT2VvZzFaSGtieWxhS0VDWC05OHc9IiwiYXVkIjpbIng6MCIsIng6MSIsIng6MiIsImQ6MSJdLCJkZXZpY2VfaGFzaCI6ImdBQUFBQUJwdThfQV9ZWHdydWpSd3RaellIQmg3UEpEalkxbTM0ZzBNaXVwRk1zbktJaDAzMGZLOXljQURfOHBENDV5SDRoaVNoSUNHbE1oVmdUSzIzQXlGXzBCcTdhT2d6RS1Ia1QtQW1Ydnl2NF9tMWNtTlVTMWlaSFhfTldzZGlHamdRbWdSNFFrIiwiZGlzcGxheV9uYW1lIjoiTUlISVIgTUFSS0FORFlBIiwiZXhwIjoxNzczOTY4NDAwLCJmZWF0dXJlX3NldCI6Ik5vSWdSZ2hnemdsZ3hpQXVrQT09IiwiZnlfaWQiOiJYTTAzMjk2IiwiaHNtX2tleSI6IjZiYzZmYmRiNDQyZjZkY2JiMGIwNmMzYjFjNDdmYzIwNzM0NWNmODk4YTRjMzkxN2FmNTVkYjY1IiwiaWF0IjoxNzczOTE2MDk2LCJpc0RkcGlFbmFibGVkIjoiTiIsImlzTXRmRW5hYmxlZCI6Ik4iLCJpc3MiOiJodHRwczovL2xvZ2luLmZ5ZXJzLmluIiwibmJmIjoxNzczOTE2MDk2LCJvbXMiOiJLMSIsInBvYV9mbGFnIjoiTiIsInN1YiI6ImFjY2Vzc190b2tlbiJ9.PI-JCBeGvNTPeM8xHF2joS0wdyteyo9uSOy3UlHRXS8";
const TOKEN_ID = "gAAAAABpu8_A_yNnFdJqPRwIU3YSvDI5oX5TUqBwgl-DlNFZwo3hHDcUBwEryLDY_1EUndy_Q6KJHFOVqlnpgsmOEN7-rZKflwQOeog1ZHkbylaKECX-98w%3D";

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
