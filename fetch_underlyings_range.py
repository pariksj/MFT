from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from fetch_relevant_index_options import FetchSpec, fetch_day_history

ROOT = Path("/Users/pariksj/Desktop/SigiQ/test-data")


@dataclass(frozen=True)
class Target:
    symbol: str
    out_root: Path
    label: str


TARGETS = (
    Target(symbol="NSE:NIFTY50-INDEX", out_root=ROOT / "nifty50", label="NIFTY"),
    Target(symbol="NSE:NIFTYBANK-INDEX", out_root=ROOT / "banknifty", label="BANKNIFTY"),
    Target(symbol="NSE:INDIAVIX-INDEX", out_root=ROOT / "indiavix", label="INDIAVIX"),
)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_weekdays(start: date, end: date):
    current = start
    while current <= end:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python3 fetch_underlyings_range.py <YYYY-MM-DD> <YYYY-MM-DD>", file=sys.stderr)
        return 1

    start = parse_date(sys.argv[1])
    end = parse_date(sys.argv[2])
    if start > end:
        print("Start date must be <= end date", file=sys.stderr)
        return 1

    dates = list(iter_weekdays(start, end))
    total_jobs = len(dates) * len(TARGETS)
    fetched = 0
    skipped = 0
    empty = 0
    failed = 0
    completed = 0

    print(
        f"Range {start.isoformat()} -> {end.isoformat()} | weekdays={len(dates)} | jobs={total_jobs}"
    )

    for session_day in dates:
        session_date = session_day.isoformat()
        print(f"\n=== {session_date} ===")
        for target in TARGETS:
            completed += 1
            print(f"[{completed}/{total_jobs}] {target.label} {session_date}")
            try:
                rows, _, was_skipped = fetch_day_history(
                    FetchSpec(
                        kind="underlying",
                        symbol=target.symbol,
                        session_date=session_date,
                        out_dir=target.out_root / session_date,
                    )
                )
                if was_skipped:
                    skipped += 1
                    print(f"  SKIP rows={len(rows)}")
                else:
                    fetched += 1
                    if not rows:
                        empty += 1
                        print("  NO_DATA")
                    else:
                        print(f"  SAVED rows={len(rows)}")
            except Exception as exc:
                failed += 1
                print(f"  FAILED {exc}", file=sys.stderr)

    print(
        f"\nDONE fetched={fetched} skipped={skipped} empty={empty} failed={failed} jobs={total_jobs}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
