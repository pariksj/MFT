import { mkdirSync, existsSync } from "fs";

// Nifty 50 constituents (NSE symbols) + the index itself
const SYMBOLS = [
  "NSE:NIFTY50-INDEX",
  "NSE:ADANIENT-EQ",
  "NSE:ADANIPORTS-EQ",
  "NSE:APOLLOHOSP-EQ",
  "NSE:ASIANPAINT-EQ",
  "NSE:AXISBANK-EQ",
  "NSE:BAJAJ-AUTO-EQ",
  "NSE:BAJFINANCE-EQ",
  "NSE:BAJAJFINSV-EQ",
  "NSE:BEL-EQ",
  "NSE:BPCL-EQ",
  "NSE:BHARTIARTL-EQ",
  "NSE:BRITANNIA-EQ",
  "NSE:CIPLA-EQ",
  "NSE:COALINDIA-EQ",
  "NSE:DRREDDY-EQ",
  "NSE:EICHERMOT-EQ",
  "NSE:GRASIM-EQ",
  "NSE:HCLTECH-EQ",
  "NSE:HDFCBANK-EQ",
  "NSE:HDFCLIFE-EQ",
  "NSE:HEROMOTOCO-EQ",
  "NSE:HINDALCO-EQ",
  "NSE:HINDUNILVR-EQ",
  "NSE:ICICIBANK-EQ",
  "NSE:ITC-EQ",
  "NSE:INDUSINDBK-EQ",
  "NSE:INFY-EQ",
  "NSE:JSWSTEEL-EQ",
  "NSE:KOTAKBANK-EQ",
  "NSE:LT-EQ",
  "NSE:M&M-EQ",
  "NSE:MARUTI-EQ",
  "NSE:NTPC-EQ",
  "NSE:NESTLEIND-EQ",
  "NSE:ONGC-EQ",
  "NSE:POWERGRID-EQ",
  "NSE:RELIANCE-EQ",
  "NSE:SBILIFE-EQ",
  "NSE:SHRIRAMFIN-EQ",
  "NSE:SBIN-EQ",
  "NSE:SUNPHARMA-EQ",
  "NSE:TCS-EQ",
  "NSE:TATACONSUM-EQ",
  "NSE:TATAMOTORS-EQ",
  "NSE:TATASTEEL-EQ",
  "NSE:TECHM-EQ",
  "NSE:TITAN-EQ",
  "NSE:TRENT-EQ",
  "NSE:ULTRACEMCO-EQ",
  "NSE:WIPRO-EQ",
];

// Trading days for last week (Mon Mar 12 - Wed Mar 19, 2026, excluding weekends)
// If any is a market holiday, the API will just return empty candles — no harm
const DATES = [
  "2026-03-12",
  "2026-03-13",
  "2026-03-16",
  "2026-03-17",
  "2026-03-18",
  "2026-03-19",
];

const BASE_DIR = "/Users/pariksj/Desktop/SigiQ/test-data/nifty50";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const jitter = () => 3000 + Math.floor(Math.random() * 4000); // 3-7s between symbols

const totalJobs = SYMBOLS.length * DATES.length;
let completed = 0;
let skipped = 0;
let failed = 0;

for (const date of DATES) {
  // Create date folder: nifty50/2026-03-18/
  const dateDir = `${BASE_DIR}/${date}`;
  if (!existsSync(dateDir)) mkdirSync(dateDir, { recursive: true });

  for (const symbol of SYMBOLS) {
    const safeName = symbol.replace(":", "_");
    const outFile = `${dateDir}/${safeName}_${date}_5s.json`;

    // Skip if already downloaded
    if (existsSync(outFile)) {
      console.log(`SKIP ${symbol} ${date} (already exists)`);
      skipped++;
      completed++;
      continue;
    }

    console.log(`\n[${ completed + 1}/${totalJobs}] ${symbol} ${date}`);

    try {
      const proc = Bun.spawn(
        ["bun", "run", "data.ts", symbol, date, dateDir],
        {
          cwd: "/Users/pariksj/Desktop/SigiQ/test-data",
          stdout: "inherit",
          stderr: "inherit",
        }
      );
      const exitCode = await proc.exited;

      if (exitCode !== 0) {
        console.log(`FAILED ${symbol} ${date} (exit code ${exitCode})`);
        failed++;
      }
    } catch (e: any) {
      console.log(`ERROR ${symbol} ${date}: ${e.message}`);
      failed++;
    }

    completed++;

    // Human-like pause between symbols (3-7 seconds)
    const pause = jitter();
    console.log(`Pausing ${(pause / 1000).toFixed(1)}s before next...`);
    await sleep(pause);
  }

  // Longer pause between dates (8-15 seconds)
  const datePause = 8000 + Math.floor(Math.random() * 7000);
  console.log(`\nDate ${date} done. Pausing ${(datePause / 1000).toFixed(1)}s before next date...\n`);
  await sleep(datePause);
}

console.log(`\nDONE: ${completed} total, ${skipped} skipped, ${failed} failed`);
