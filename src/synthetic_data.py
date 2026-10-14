"""Synthetic Uber NCR ride-booking generator that mirrors the source CSV exactly.

Used for unit tests, CI and fault-injection experiments when the real 150k-row export
(``ncr_ride_bookings.csv``) is not available. Characteristics reproduced:

* 21 columns in source order, Date as dd-MM-yyyy and Time as HH:mm:ss
* Booking ID / Customer ID wrapped in literal quotes ("CNR5884300")
* missing values written as the literal text ``null``
* outcome columns populated only for the matching Booking Status
"""

from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from src.config import SOURCE_COLUMNS

STATUS_WEIGHTS = {
    "Completed": 0.62,
    "Cancelled by Driver": 0.18,
    "No Driver Found": 0.07,
    "Cancelled by Customer": 0.07,
    "Incomplete": 0.06,
}
VEHICLES = {  # vehicle -> (weight, fare per km)
    "Auto": (0.25, 14.0),
    "Go Mini": (0.20, 16.0),
    "Go Sedan": (0.18, 19.0),
    "Bike": (0.15, 9.0),
    "Premier Sedan": (0.12, 24.0),
    "eBike": (0.07, 8.0),
    "Uber XL": (0.03, 30.0),
}
PAYMENTS = {"UPI": 0.45, "Cash": 0.25, "Uber Wallet": 0.12, "Credit Card": 0.10, "Debit Card": 0.08}
LOCATIONS = [
    "Connaught Place", "Saket", "Hauz Khas", "Lajpat Nagar", "Karol Bagh", "Rajouri Garden", "Dwarka Sector 21",
    "Janakpuri", "Vasant Kunj", "Nehru Place", "Greater Kailash", "Chandni Chowk", "Kashmere Gate",
    "AIIMS", "Mayur Vihar", "Laxmi Nagar", "Noida Sector 18", "Noida Film City", "Indirapuram",
    "Vaishali", "Ghaziabad", "Cyber Hub", "MG Road", "Sohna Road", "Golf Course Road", "Udyog Vihar",
    "Huda City Centre", "IGI Airport", "New Delhi Railway Station", "Anand Vihar ISBT", "Pitampura",
    "Rohini", "Shahdara", "Okhla", "Jasola", "Tilak Nagar", "Punjabi Bagh", "Faridabad Sector 15",
    "Barakhamba Road", "Khan Market",
]
CUSTOMER_REASONS = ["Wrong Address", "Change of plans", "Driver is not moving towards pickup location",
                    "Driver asked to cancel", "AC is not working"]
DRIVER_REASONS = ["Personal & Car related issues", "Customer related issue", "The customer was coughing/sick",
                  "More than permitted people in there"]
INCOMPLETE_REASONS = ["Vehicle Breakdown", "Customer Demand", "Other Issue"]
NULL = "null"


def _choice(rng: random.Random, weights: dict) -> str:
    keys = list(weights)
    vals = [w[0] if isinstance(w, tuple) else w for w in weights.values()]
    return rng.choices(keys, weights=vals, k=1)[0]


def generate_rows(
    n_rows: int, start: date = date(2024, 1, 1), end: date = date(2024, 12, 31), seed: int = 42
) -> list[dict]:
    """Return ``n_rows`` clean bookings as dicts keyed by the source column names (all strings)."""
    rng = random.Random(seed)
    span = (end - start).days + 1
    booking_ids = rng.sample(range(1_000_000, 9_999_999), n_rows)
    customers = [f"CID{n}" for n in rng.sample(range(1_000_000, 9_999_999), max(1, int(n_rows * 0.9)))]
    rows = []
    for i in range(n_rows):
        # spread rows evenly over the date range so no calendar day is empty
        day = start + timedelta(days=(i * span // n_rows) if n_rows >= span else rng.randrange(span))
        ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randrange(86400))
        status = _choice(rng, STATUS_WEIGHTS)
        vehicle = _choice(rng, VEHICLES)
        pickup, drop = rng.sample(LOCATIONS, 2)
        r = {c: NULL for c in SOURCE_COLUMNS}
        r.update({
            "Date": ts.strftime("%d-%m-%Y"),
            "Time": ts.strftime("%H:%M:%S"),
            "Booking ID": f'"CNR{booking_ids[i]}"',
            "Booking Status": status,
            "Customer ID": f'"{rng.choice(customers)}"',
            "Vehicle Type": vehicle,
            "Pickup Location": pickup,
            "Drop Location": drop,
        })
        if status != "No Driver Found":
            r["Avg VTAT"] = f"{rng.uniform(2, 20):.1f}"
        if status in ("Completed", "Incomplete"):
            distance = rng.uniform(1.5, 48)
            fare = VEHICLES[vehicle][1] * distance * rng.uniform(0.85, 1.25) + 40
            r["Avg CTAT"] = f"{rng.uniform(10, 45):.1f}"
            r["Ride Distance"] = f"{distance:.2f}"
            r["Booking Value"] = f"{fare:.0f}"
            r["Payment Method"] = _choice(rng, PAYMENTS)
        if status == "Completed":
            r["Driver Ratings"] = f"{rng.uniform(3.0, 5.0):.1f}"
            r["Customer Rating"] = f"{rng.uniform(3.0, 5.0):.1f}"
        elif status == "Cancelled by Customer":
            r["Cancelled Rides by Customer"] = "1"
            r["Reason for cancelling by Customer"] = rng.choice(CUSTOMER_REASONS)
        elif status == "Cancelled by Driver":
            r["Cancelled Rides by Driver"] = "1"
            r["Driver Cancellation Reason"] = rng.choice(DRIVER_REASONS)
        elif status == "Incomplete":
            r["Incomplete Rides"] = "1"
            r["Incomplete Rides Reason"] = rng.choice(INCOMPLETE_REASONS)
        rows.append(r)
    rows.sort(key=lambda r: (r["Date"][6:] + r["Date"][3:5] + r["Date"][:2], r["Time"]))
    return rows


def write_csv(rows: list[dict], path: Path | str, columns: Optional[list[str]] = None) -> Path:
    """Write rows in the source format (quotes inside IDs are escaped as "" by the csv module)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = columns or SOURCE_COLUMNS
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def read_csv(path: Path | str) -> tuple[list[str], list[dict]]:
    with Path(path).open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), list(reader)
