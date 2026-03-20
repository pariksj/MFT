from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from typing import Any

import requests

BASE_URL = "https://api-t1.fyers.in/indus/history"
SESSION_START = "09:15:00"
SESSION_END = "15:30:00"
WINDOW_SECONDS = int(os.environ.get("FYERS_WINDOW_SECONDS", str(25 * 60)))

AUTH = os.environ.get("FYERS_ACCESS_TOKEN", "")
TOKEN_ID = os.environ.get("FYERS_TOKEN_ID", "")
if not AUTH or not TOKEN_ID:
    print("Set FYERS_ACCESS_TOKEN and FYERS_TOKEN_ID env vars", file=sys.stderr)
    raise SystemExit(1)

HEADERS = {
    "accept": "*/*",
    "authorization": AUTH,
    "origin": "https://trade.fyers.in",
    "referer": "https://trade.fyers.in/",
    "user-agent": "Mozilla/5.0",
}

SESSION = requests.Session()


def epoch_seconds(session_date: str, time_str: str) -> int:
    return int(datetime.fromisoformat(f"{session_date}T{time_str}+05:30").timestamp())


def classify(candles: list[list[Any]]) -> dict[str, Any]:
    if not candles:
        return {"status": "no_data", "rows": 0, "diffs": []}
    diffs = sorted({int(candles[i + 1][0]) - int(candles[i][0]) for i in range(len(candles) - 1)})
    status = "other"
    if diffs == [5]:
        status = "5s"
    elif diffs == [60]:
        status = "1m"
    return {
        "status": status,
        "rows": len(candles),
        "diffs": diffs,
        "first_epoch": int(candles[0][0]),
        "last_epoch": int(candles[-1][0]),
    }


def request_history(symbol: str, start: int, end: int, data_req: int) -> list[list[Any]]:
    params = {
        "symbol": symbol,
        "resolution": "5S",
        "from": str(start),
        "to": str(end),
        "token_id": TOKEN_ID,
        "dataReq": str(data_req),
        "contFlag": "1",
        "countback": "329",
        "currencyCode": "INR",
    }
    for attempt in range(6):
        response = SESSION.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        if response.status_code == 429:
            time.sleep(1 + attempt * 0.5)
            continue
        response.raise_for_status()
        payload = response.json()
        return payload.get("candles", [])
    raise RuntimeError(f"Too many rate limits for {symbol} {start} {end}")


def fetch_full_day(symbol: str, session_date: str, data_req_mode: str) -> dict[str, Any]:
    start = epoch_seconds(session_date, SESSION_START)
    end = epoch_seconds(session_date, SESSION_END)
    data_req = int(time.time()) if data_req_mode == "now" else end
    candles = request_history(symbol, start, end, data_req)
    result = classify(candles)
    result["mode"] = f"full_day_{data_req_mode}"
    return result


def fetch_chunked_day(symbol: str, session_date: str, data_req_mode: str) -> dict[str, Any]:
    day_start = epoch_seconds(session_date, SESSION_START)
    day_end = epoch_seconds(session_date, SESSION_END)
    deduped: dict[int, list[Any]] = {}
    current_from = day_start

    while current_from < day_end:
        current_to = min(current_from + WINDOW_SECONDS, day_end)
        data_req = int(time.time()) if data_req_mode == "now" else current_to
        for candle in request_history(symbol, current_from, current_to, data_req):
            deduped[int(candle[0])] = candle
        current_from = current_to
        if current_from < day_end:
            time.sleep(0.1)

    candles = [deduped[key] for key in sorted(deduped)]
    result = classify(candles)
    result["mode"] = f"chunked_{data_req_mode}_{WINDOW_SECONDS}s"
    return result


def main() -> int:
    if len(sys.argv) < 3:
        print(
            "Usage: python3 probe_fyers_5s_modes.py <symbol> <YYYY-MM-DD> [<YYYY-MM-DD> ...]",
            file=sys.stderr,
        )
        return 1

    symbol = sys.argv[1]
    dates = sys.argv[2:]
    out = []
    for session_date in dates:
        print(f"Probing {symbol} {session_date}...", file=sys.stderr)
        full_day_end = fetch_full_day(symbol, session_date, "end")
        full_day_now = fetch_full_day(symbol, session_date, "now")
        chunked_end = fetch_chunked_day(symbol, session_date, "end")
        chunked_now = fetch_chunked_day(symbol, session_date, "now")
        out.append(
            {
                "symbol": symbol,
                "session_date": session_date,
                "full_day_end": full_day_end,
                "full_day_now": full_day_now,
                "chunked_end": chunked_end,
                "chunked_now": chunked_now,
            }
        )

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
