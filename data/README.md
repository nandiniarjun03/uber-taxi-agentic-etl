# data/

| Path | Content |
|---|---|
| `sample/ncr_ride_bookings_sample.csv` | 500 synthetic bookings in the exact source format (quoted IDs, literal `null`, dd-MM-yyyy dates) - used by notebooks and as a fallback landing file |
| `corrupted/*.csv` | the same sample with one deliberate fault each (see `src/fault_injection.py`) - quality checks must fail on these |

The full dataset (`ncr_ride_bookings.csv`, 150,000 rows, ~25 MB, Kaggle "Uber Ride Analytics" export) is **not committed**.
Download it and place it in `lakehouse/raw/uber_rides/` (local) or upload it to `raw/uber_rides/` in ADLS.
Regenerate the files here with `python scripts/generate_synthetic_data.py --rows 500 --out data/sample/ncr_ride_bookings_sample.csv`.
