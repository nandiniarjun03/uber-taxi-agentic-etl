"""Generate synthetic Uber NCR booking CSVs in the exact source format.

    python scripts/generate_synthetic_data.py --rows 150000 --out lakehouse/raw/uber_rides/ncr_ride_bookings.csv
    python scripts/generate_synthetic_data.py --rows 400 --out data/sample/ncr_ride_bookings_sample.csv

Use the real Kaggle export (ncr_ride_bookings.csv) instead when you have it - just copy it
into the landing zone.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.synthetic_data import generate_rows, write_csv  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=150_000)
    p.add_argument("--start", default="2024-01-01")
    p.add_argument("--end", default="2024-12-31")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="lakehouse/raw/uber_rides/ncr_ride_bookings.csv")
    a = p.parse_args()
    rows = generate_rows(a.rows, date.fromisoformat(a.start), date.fromisoformat(a.end), a.seed)
    path = write_csv(rows, a.out)
    print(f"Wrote {len(rows):,} rows to {path}")


if __name__ == "__main__":
    main()
