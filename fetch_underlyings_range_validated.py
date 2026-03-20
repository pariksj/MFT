from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path("/Users/pariksj/Desktop/SigiQ/test-data")
BASE_URL = "https://api-t1.fyers.in/indus/history"
INTERVAL_SECONDS = 5
EXPECTED_BARS = 4500


@dataclass(frozen=True)
class Target:
    symbol: str
    out_root: Path
    label: str


TARGETS = (
    Target(symbol="NSE:NIFTY50-INDEX", out_root=ROOT / "nifty50-5s", label="NIFTY"),
    Target(symbol="NSE:NIFTYBANK-INDEX", out_root=ROOT / "banknifty-5s", label="BANKNIFTY"),
    Target(symbol="NSE:INDIAVIX-INDEX", out_root=ROOT / "indiavix-5s", label="INDIAVIX"),
)

AUTH = os.environ.get("FYERS_ACCESS_TOKEN", "")
TOKEN_ID = os.environ.get("FYERS_TOKEN_ID", "")
if not AUTH or not TOKEN_ID:
    print("Set FYERS_ACCESS_TOKEN and FYERS_TOKEN_ID env vars", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    "accept": "*/*",
    "authorization": AUTH,
    "origin": "https://trade.fyers.in",
    "referer": "https://trade.fyers.in/",
    "user-agent": "Mozilla/5.0",
}

SESSION = requests.Session()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_weekdays(start: date, end: date):
    current = start
    while current <= end:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


def epoch_seconds(session_date: str, time_str: str) -> int:
    return int(datetime.fromisoformat(f"{session_date}T{time_str}+05:30").timestamp())


def to_rows(symbol: str, session_date: str, candles: list[list[Any]]) -> list[dict[str, Any]]:
    return [
        {
            "kind": "underlying",
            "source": "fyers",
            "exchange": "NSE",
            "symbol": symbol,
            "intervalSeconds": INTERVAL_SECONDS,
            "sessionDate": session_date,
            "timestamp": datetime.fromtimestamp(int(candle[0]), tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "epochSeconds": int(candle[0]),
            "open": float(candle[1]),
            "high": float(candle[2]),
            "low": float(candle[3]),
            "close": float(candle[4]),
            "volume": int(candle[5]),
        }
        for candle in candles
    ]


def safe_name(symbol: str) -> str:
    return symbol.replace(":", "_")


def output_path(out_dir: Path, symbol: str, session_date: str) -> Path:
    return out_dir / f"{safe_name(symbol)}_{session_date}_5s.json"


def fetch_full_day(symbol: str, session_date: str) -> tuple[str, list[dict[str, Any]]]:
    start = epoch_seconds(session_date, "09:15:00")
    end = epoch_seconds(session_date, "15:30:00")
    response = SESSION.get(
        BASE_URL,
        params={
            "symbol": symbol,
            "resolution": "5S",
            "from": str(start),
            "to": str(end),
            "token_id": TOKEN_ID,
            "dataReq": str(end),
            "contFlag": "1",
            "countback": "329",
            "currencyCode": "INR",
        },
        headers=HEADERS,
        timeout=30,
    )
    payload = response.json()
    candles = payload.get("candles", [])
    if not candles:
        return payload.get("s", "no_data"), []

    diffs = sorted({candles[i + 1][0] - candles[i][0] for i in range(len(candles) - 1)})
    if diffs == [5] and len(candles) >= EXPECTED_BARS:
        return "5s", to_rows(symbol, session_date, candles)
    if diffs == [60]:
        return "1m_fallback", []
    return f"unexpected:{diffs}:{len(candles)}", []


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "Usage: python3 fetch_underlyings_range_validated.py <YYYY-MM-DD> <YYYY-MM-DD>",
            file=sys.stderr,
        )
        return 1

    start = parse_date(sys.argv[1])
    end = parse_date(sys.argv[2])
    dates = list(iter_weekdays(start, end))
    total_jobs = len(dates) * len(TARGETS)
    fetched = 0
    skipped = 0
    no_data = 0
    fallback = 0
    failed = 0
    completed = 0

    print(
        f"Validated 5s range {start.isoformat()} -> {end.isoformat()} | weekdays={len(dates)} | jobs={total_jobs}"
    )

    for session_day in dates:
        session_date = session_day.isoformat()
        print(f"\n=== {session_date} ===")
        for target in TARGETS:
            completed += 1
            out_dir = target.out_root / session_date
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = output_path(out_dir, target.symbol, session_date)
            print(f"[{completed}/{total_jobs}] {target.label} {session_date}")

            if out_file.exists():
                skipped += 1
                print("  SKIP existing")
                continue

            try:
                status, rows = fetch_full_day(target.symbol, session_date)
                if status == "5s":
                    with out_file.open("w") as handle:
                        json.dump(rows, handle, indent=2)
                    fetched += 1
                    print(f"  SAVED rows={len(rows)}")
                elif status == "no_data":
                    no_data += 1
                    print("  NO_DATA")
                elif status == "1m_fallback":
                    fallback += 1
                    print("  SKIP non-5s fallback")
                else:
                    failed += 1
                    print(f"  FAILED {status}", file=sys.stderr)
            except Exception as exc:
                failed += 1
                print(f"  FAILED {exc}", file=sys.stderr)

    print(
        f"\nDONE fetched={fetched} skipped={skipped} no_data={no_data} fallback={fallback} failed={failed}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
