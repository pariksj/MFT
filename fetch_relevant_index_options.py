from __future__ import annotations

import json
import math
import os
import sys
import time
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path("/Users/pariksj/Desktop/SigiQ/test-data")
NIFTY_DIR = ROOT / "nifty50"
NIFTY_OPTIONS_DIR = ROOT / "nifty50-options"
BANKNIFTY_DIR = ROOT / "banknifty"
BANKNIFTY_OPTIONS_DIR = ROOT / "banknifty-options"

NIFTY_SPOT = "NSE:NIFTY50-INDEX"
BANKNIFTY_SPOT = "NSE:NIFTYBANK-INDEX"

NIFTY_STEP = 50
BANKNIFTY_STEP = 100
BUFFER_STEPS = 1

SESSION_START = "09:15:00"
SESSION_END = "15:30:00"
INTERVAL_SECONDS = 5
WINDOW_SECONDS = 25 * 60
COUNTBACK = 329
BASE_URL = "https://api-t1.fyers.in/indus/history"


@dataclass(frozen=True)
class FetchSpec:
    kind: str
    symbol: str
    session_date: str
    out_dir: Path
    exchange: str = "NSE"
    underlying: str | None = None
    expiry: str | None = None
    strike: int | None = None
    option_type: str | None = None


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


def parse_session_date(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def format_session_date(value: date) -> str:
    return value.isoformat()


def last_weekday_of_month(year: int, month: int, target_weekday: int) -> date:
    last_day = monthrange(year, month)[1]
    current = date(year, month, last_day)
    while current.weekday() != target_weekday:
        current -= timedelta(days=1)
    return current


def next_weekday_inclusive(current: date, target_weekday: int) -> date:
    delta = (target_weekday - current.weekday()) % 7
    return current + timedelta(days=delta)


def nifty_front_expiry(session_date: str) -> str:
    current = parse_session_date(session_date)
    return format_session_date(next_weekday_inclusive(current, 1))


def banknifty_front_expiry(session_date: str) -> str:
    current = parse_session_date(session_date)
    expiry = last_weekday_of_month(current.year, current.month, 3)
    if expiry < current:
        if current.month == 12:
            expiry = last_weekday_of_month(current.year + 1, 1, 3)
        else:
            expiry = last_weekday_of_month(current.year, current.month + 1, 3)
    return format_session_date(expiry)


def is_last_tuesday_of_month(expiry: str) -> bool:
    expiry_date = parse_session_date(expiry)
    return expiry_date == last_weekday_of_month(expiry_date.year, expiry_date.month, 1)


def weekly_expiry_code(expiry: str) -> str:
    expiry_date = parse_session_date(expiry)
    return f"{expiry_date.year % 100:02d}{expiry_date.month}{expiry_date.day:02d}"


def monthly_expiry_code(expiry: str) -> str:
    expiry_date = parse_session_date(expiry)
    return f"{expiry_date.year % 100:02d}{expiry_date.strftime('%b').upper()}"


def build_nifty_option_symbol(expiry: str, strike: int, option_type: str) -> str:
    code = monthly_expiry_code(expiry) if is_last_tuesday_of_month(expiry) else weekly_expiry_code(expiry)
    return f"NSE:NIFTY{code}{strike}{option_type}"


def build_banknifty_option_symbol(expiry: str, strike: int, option_type: str) -> str:
    return f"NSE:BANKNIFTY{monthly_expiry_code(expiry)}{strike}{option_type}"


def ist_to_epoch(session_date: str, time_str: str) -> int:
    dt = datetime.fromisoformat(f"{session_date}T{time_str}+05:30")
    return int(dt.timestamp())


def safe_name(symbol: str) -> str:
    return symbol.replace(":", "_")


def output_path(out_dir: Path, symbol: str, session_date: str) -> Path:
    return out_dir / f"{safe_name(symbol)}_{session_date}_5s.json"


def read_rows(path: Path) -> list[dict[str, Any]]:
    with path.open() as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"Expected list in {path}")
    return data


def summarize_range(rows: list[dict[str, Any]]) -> tuple[float, float]:
    lows = [float(row["low"]) for row in rows]
    highs = [float(row["high"]) for row in rows]
    return min(lows), max(highs)


def rounded_strikes(low: float, high: float, step: int) -> list[int]:
    start = math.floor(low / step) * step - BUFFER_STEPS * step
    end = math.ceil(high / step) * step + BUFFER_STEPS * step
    return list(range(start, end + step, step))


def map_row(spec: FetchSpec, candle: list[Any]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "kind": spec.kind,
        "source": "fyers",
        "exchange": spec.exchange,
        "symbol": spec.symbol,
        "intervalSeconds": INTERVAL_SECONDS,
        "sessionDate": spec.session_date,
        "timestamp": datetime.fromtimestamp(int(candle[0]), tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "epochSeconds": int(candle[0]),
        "open": float(candle[1]),
        "high": float(candle[2]),
        "low": float(candle[3]),
        "close": float(candle[4]),
        "volume": int(candle[5]),
    }
    if spec.kind == "option":
        row["underlying"] = spec.underlying
        row["expiry"] = spec.expiry
        row["strike"] = spec.strike
        row["optionType"] = spec.option_type
        row["openInterest"] = int(candle[6]) if len(candle) > 6 and candle[6] is not None else 0
    return row


def fetch_day_history(spec: FetchSpec) -> tuple[list[dict[str, Any]], Path, bool]:
    spec.out_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_path(spec.out_dir, spec.symbol, spec.session_date)
    if out_file.exists():
        return read_rows(out_file), out_file, True

    day_start = ist_to_epoch(spec.session_date, SESSION_START)
    day_end = ist_to_epoch(spec.session_date, SESSION_END)
    deduped: dict[int, list[Any]] = {}
    current_from = day_start

    while current_from < day_end:
        current_to = min(current_from + WINDOW_SECONDS, day_end)
        params = {
            "symbol": spec.symbol,
            "resolution": "5S",
            "from": str(current_from),
            "to": str(current_to),
            "token_id": TOKEN_ID,
            "dataReq": str(current_to),
            "contFlag": "1",
            "countback": str(COUNTBACK),
            "currencyCode": "INR",
        }
        attempt = 0

        while True:
            response = SESSION.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
            if response.status_code == 429:
                time.sleep(1 + min(attempt, 5) * 0.5)
                attempt += 1
                continue
            if response.status_code != 200:
                raise RuntimeError(
                    f"FYERS {response.status_code} for {spec.symbol} {spec.session_date}: {response.text}"
                )
            payload = response.json()
            for candle in payload.get("candles", []):
                deduped[int(candle[0])] = candle
            break

        current_from = current_to
        if current_from < day_end:
            time.sleep(0.12)

    rows = [map_row(spec, deduped[key]) for key in sorted(deduped)]
    with out_file.open("w") as handle:
        json.dump(rows, handle, indent=2)
    return rows, out_file, False


def ensure_spot_rows(base_dir: Path, symbol: str, session_date: str) -> list[dict[str, Any]]:
    path = output_path(base_dir / session_date, symbol, session_date)
    if not path.exists():
        rows, _, _ = fetch_day_history(
            FetchSpec(
                kind="underlying",
                symbol=symbol,
                session_date=session_date,
                out_dir=base_dir / session_date,
            )
        )
        return rows
    return read_rows(path)


def nifty_spot_rows(session_date: str) -> list[dict[str, Any]]:
    return ensure_spot_rows(NIFTY_DIR, NIFTY_SPOT, session_date)


def banknifty_spot_rows(session_date: str) -> list[dict[str, Any]]:
    return ensure_spot_rows(BANKNIFTY_DIR, BANKNIFTY_SPOT, session_date)


def fetch_option_chain_range(
    *,
    session_date: str,
    expiry: str,
    strikes: list[int],
    out_root: Path,
    underlying: str,
    builder,
) -> tuple[int, int, int]:
    fetched = 0
    skipped = 0
    failed = 0

    for strike in strikes:
        for option_type in ("CE", "PE"):
            symbol = builder(expiry, strike, option_type)
            try:
                _, _, was_skipped = fetch_day_history(
                    FetchSpec(
                        kind="option",
                        symbol=symbol,
                        session_date=session_date,
                        out_dir=out_root / session_date,
                        underlying=underlying,
                        expiry=expiry,
                        strike=strike,
                        option_type=option_type,
                    )
                )
                if was_skipped:
                    skipped += 1
                else:
                    fetched += 1
            except Exception as exc:
                failed += 1
                print(f"FAILED {session_date} {symbol}: {exc}", file=sys.stderr)
    return fetched, skipped, failed


def session_dates(cli_dates: list[str]) -> list[str]:
    if cli_dates:
        return sorted(dict.fromkeys(cli_dates))
    return sorted(
        child.name
        for child in NIFTY_DIR.iterdir()
        if child.is_dir() and len(child.name) == 10 and child.name.count("-") == 2
    )


def main() -> int:
    requested_dates = sys.argv[1:]
    total_fetched = 0
    total_skipped = 0
    total_failed = 0

    for session_date in session_dates(requested_dates):
        print(f"\n=== {session_date} ===")

        nifty_rows = nifty_spot_rows(session_date)
        nifty_low, nifty_high = summarize_range(nifty_rows)
        nifty_expiry = nifty_front_expiry(session_date)
        nifty_strikes = rounded_strikes(nifty_low, nifty_high, NIFTY_STEP)
        print(
            f"NIFTY expiry {nifty_expiry} | range {nifty_low:.2f}-{nifty_high:.2f} | "
            f"strikes {nifty_strikes[0]}-{nifty_strikes[-1]} ({len(nifty_strikes)})"
        )
        fetched, skipped, failed = fetch_option_chain_range(
            session_date=session_date,
            expiry=nifty_expiry,
            strikes=nifty_strikes,
            out_root=NIFTY_OPTIONS_DIR,
            underlying=NIFTY_SPOT,
            builder=build_nifty_option_symbol,
        )
        total_fetched += fetched
        total_skipped += skipped
        total_failed += failed

        bank_rows = banknifty_spot_rows(session_date)
        bank_low, bank_high = summarize_range(bank_rows)
        bank_expiry = banknifty_front_expiry(session_date)
        bank_strikes = rounded_strikes(bank_low, bank_high, BANKNIFTY_STEP)
        print(
            f"BANKNIFTY expiry {bank_expiry} | range {bank_low:.2f}-{bank_high:.2f} | "
            f"strikes {bank_strikes[0]}-{bank_strikes[-1]} ({len(bank_strikes)})"
        )
        fetched, skipped, failed = fetch_option_chain_range(
            session_date=session_date,
            expiry=bank_expiry,
            strikes=bank_strikes,
            out_root=BANKNIFTY_OPTIONS_DIR,
            underlying=BANKNIFTY_SPOT,
            builder=build_banknifty_option_symbol,
        )
        total_fetched += fetched
        total_skipped += skipped
        total_failed += failed

    print(f"\nDONE fetched={total_fetched} skipped={total_skipped} failed={total_failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
