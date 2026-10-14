"""Deliberate data corruption for testing the quality checks and the agents.

Each fault mutates the source CSV rows (or the landing zone) the way a real upstream
problem would, and declares where the agentic system is expected to stop and which
rule(s) should explain it. ``scripts/evaluate_agents.py`` uses these to compute the
detection, false-alarm and recovery rates.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from pathlib import Path

from src.synthetic_data import NULL, read_csv, write_csv


@dataclass
class FaultSpec:
    name: str
    description: str
    expected_status: str  # FAIL | PASS_WITH_WARNINGS
    expected_stop_node: str  # etl | quality | analytics
    expected_rules: list[str] = field(default_factory=list)  # any of these explains the fault
    runner_failure: bool = False  # simulated Databricks job failure (not a data change)


FAULTS: dict[str, FaultSpec] = {
    f.name: f
    for f in [
        FaultSpec("missing_source_file", "Landing zone is empty (upstream export did not arrive)",
                  "FAIL", "etl", ["MissingSourceError"]),
        FaultSpec("schema_change", "Source renamed 'Booking Value' to 'Fare Amount' and dropped 'Payment Method'",
                  "FAIL", "etl", ["SourceSchemaError"]),
        FaultSpec("job_failure", "Databricks job task crashed (simulated executor loss)",
                  "FAIL", "etl", ["job_failure"], runner_failure=True),
        FaultSpec("duplicate_records", "8% of bookings delivered twice",
                  "FAIL", "quality", ["bronze_unique_booking_id", "silver_quarantine_rate"]),
        FaultSpec("null_spike", "12% of rows lost Customer ID / Pickup Location",
                  "FAIL", "quality", ["bronze_key_completeness"]),
        FaultSpec("out_of_range_values", "Negative booking values and driver ratings above 5",
                  "FAIL", "quality", ["bronze_value_ranges"]),
        FaultSpec("status_flag_mismatch", "Completed bookings carrying a customer-cancellation flag and reason",
                  "FAIL", "quality", ["bronze_status_flag_consistency"]),
        FaultSpec("row_count_drop", "Only 45% of the usual volume arrived",
                  "FAIL", "quality", ["bronze_volume_vs_previous_load"]),
        FaultSpec("missing_dates", "Seven consecutive days missing from the export",
                  "FAIL", "quality", ["bronze_timeliness"]),
        FaultSpec("payment_method_gap", "0.6% of Completed bookings missing Payment Method (soft rule)",
                  "PASS_WITH_WARNINGS", "analytics", ["completed_rides_missing_payment_method"]),
    ]
}


def _completed(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r["Booking Status"] == "Completed"]


def corrupt_rows(fault: str, header: list[str], rows: list[dict], seed: int = 7) -> tuple[list[str], list[dict]]:
    """Return (header, rows) with the fault applied. Rows are copied, the input is untouched."""
    rng = random.Random(seed)
    rows = [dict(r) for r in rows]
    header = list(header)
    n = len(rows)

    if fault == "schema_change":
        header = ["Fare Amount" if c == "Booking Value" else c for c in header if c != "Payment Method"]
        for r in rows:
            r["Fare Amount"] = r.pop("Booking Value")
            r.pop("Payment Method", None)
    elif fault == "duplicate_records":
        rows += [dict(r) for r in rng.sample(rows, int(n * 0.08))]
    elif fault == "null_spike":
        for r in rng.sample(rows, int(n * 0.12)):
            r[rng.choice(["Customer ID", "Pickup Location"])] = NULL
    elif fault == "out_of_range_values":
        completed = _completed(rows)
        for i, r in enumerate(rng.sample(completed, int(len(completed) * 0.05))):
            if i % 2:
                r["Booking Value"] = str(-abs(float(r["Booking Value"])))
            else:
                r["Driver Ratings"] = "6.5"
    elif fault == "status_flag_mismatch":
        completed = _completed(rows)
        for r in rng.sample(completed, int(len(completed) * 0.04)):
            r["Cancelled Rides by Customer"] = "1"
            r["Reason for cancelling by Customer"] = "Change of plans"
    elif fault == "row_count_drop":
        rows = rng.sample(rows, int(n * 0.45))
    elif fault == "missing_dates":
        dates = sorted({r["Date"] for r in rows}, key=lambda d: d[6:] + d[3:5] + d[:2])
        start = len(dates) // 3
        drop = set(dates[start:start + 7])
        rows = [r for r in rows if r["Date"] not in drop]
    elif fault == "payment_method_gap":
        completed = _completed(rows)
        for r in rng.sample(completed, max(1, int(len(completed) * 0.006))):
            r["Payment Method"] = NULL
    elif fault in ("job_failure", "missing_source_file", "none"):
        pass
    else:
        raise ValueError(f"Unknown fault '{fault}'. Known: {sorted(FAULTS)}")
    return header, rows


def apply_fault_to_landing_zone(raw_dir: Path | str, fault: str, seed: int = 7) -> None:
    """Apply a fault in place to every CSV in a local landing-zone directory."""
    raw_dir = Path(raw_dir)
    files = sorted(raw_dir.glob("*.csv"))
    if fault == "missing_source_file":
        for f in files:
            f.unlink()
        return
    for f in files:
        header, rows = read_csv(f)
        header, rows = corrupt_rows(fault, header, rows, seed)
        write_csv(rows, f, columns=header)
