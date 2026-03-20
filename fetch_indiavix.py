from __future__ import annotations

import sys
from pathlib import Path

from fetch_relevant_index_options import FetchSpec, fetch_day_history

ROOT = Path("/Users/pariksj/Desktop/SigiQ/test-data")
SOURCE_DIR = ROOT / "nifty50"
OUT_DIR = ROOT / "indiavix"
VIX_SYMBOL = "NSE:INDIAVIX-INDEX"


def session_dates(cli_dates: list[str]) -> list[str]:
    if cli_dates:
        return sorted(dict.fromkeys(cli_dates))
    return sorted(
        child.name
        for child in SOURCE_DIR.iterdir()
        if child.is_dir() and len(child.name) == 10 and child.name.count("-") == 2
    )


def main() -> int:
    fetched = 0
    skipped = 0
    failed = 0

    for session_date in session_dates(sys.argv[1:]):
        print(f"\n=== {session_date} ===")
        try:
            _, _, was_skipped = fetch_day_history(
                FetchSpec(
                    kind="underlying",
                    symbol=VIX_SYMBOL,
                    session_date=session_date,
                    out_dir=OUT_DIR / session_date,
                )
            )
            if was_skipped:
                skipped += 1
            else:
                fetched += 1
        except Exception as exc:
            failed += 1
            print(f"FAILED {session_date}: {exc}", file=sys.stderr)

    print(f"\nDONE fetched={fetched} skipped={skipped} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
