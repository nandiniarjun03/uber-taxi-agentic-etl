"""Unit tests for Bronze ingestion and Silver transformation logic (no table writes)."""

from __future__ import annotations

import datetime as dt

import pytest

from src.etl_pipeline import (
    SILVER_COLUMNS,
    SourceSchemaError,
    standardise,
    transform_silver,
    validate_source_headers,
    window_predicate,
)
from src.synthetic_data import NULL, generate_rows
from tests.conftest import requires_spark

pytestmark = requires_spark


def _row(**overrides) -> dict:
    """One valid Completed booking in source format, with overrides by source column name."""
    base = generate_rows(50, seed=3)
    row = dict(next(r for r in base if r["Booking Status"] == "Completed"))
    row.update(overrides)
    return row


def test_bronze_strips_quotes_and_reads_null_text(bronze_factory):
    rows = [_row(**{"Booking ID": '"CNR1234567"', "Customer ID": '"CID7654321"', "Avg CTAT": NULL})]
    b = bronze_factory(rows).collect()[0]
    assert b["booking_id"] == "CNR1234567"
    assert b["customer_id"] == "CID7654321"
    assert b["avg_ctat"] is None
    assert b["batch_id"] == "test-batch"
    assert b["source_file"].endswith(".csv")
    assert b["booking_date"] is not None


def test_bronze_has_all_metadata_columns(bronze_factory, clean_rows):
    df = bronze_factory(clean_rows[:20])
    for c in ("ingestion_timestamp", "source_file", "batch_id", "booking_date", "booking_month"):
        assert c in df.columns
    assert len([c for c in df.columns if c not in ("ingestion_timestamp", "source_file", "batch_id",
                                                     "booking_date", "booking_month")]) == 21


def test_header_validation_detects_schema_change(spark, csv_factory, clean_rows):
    from src.config import SOURCE_COLUMNS
    from src.fault_injection import corrupt_rows

    header, rows = corrupt_rows("schema_change", SOURCE_COLUMNS, clean_rows[:10])
    bad = csv_factory(rows, header)
    good = csv_factory(clean_rows[:10])
    validate_source_headers(spark, [good])  # no error
    with pytest.raises(SourceSchemaError, match="Booking Value"):
        validate_source_headers(spark, [good, bad])


def test_timestamp_parsing_accepts_spec_and_iso_formats(bronze_factory):
    rows = [_row(Date="23-03-2024", Time="12:29:38"), _row(Date="2024-03-24", Time="07:05:00",
                                                          **{"Booking ID": '"CNR0000002"'})]
    std = standardise(bronze_factory(rows)).orderBy("booking_timestamp").collect()
    assert std[0]["booking_timestamp"] == dt.datetime(2024, 3, 23, 12, 29, 38)
    assert std[0]["booking_hour"] == 12
    assert std[0]["day_of_week"] == "Saturday"
    assert std[1]["booking_date"] == dt.date(2024, 3, 24)


def test_standardise_canonicalises_domains(bronze_factory):
    rows = [_row(**{"Vehicle Type": "e-bike", "Payment Method": "upi", "Booking Status": "COMPLETED"})]
    r = standardise(bronze_factory(rows)).collect()[0]
    assert (r["vehicle_type"], r["payment_method"], r["booking_status"]) == ("eBike", "UPI", "Completed")
    assert r["is_completed"] is True and r["is_cancelled"] is False


def test_clean_data_produces_no_quarantine(bronze_factory, clean_rows):
    silver, quarantine = transform_silver(bronze_factory(clean_rows))
    assert quarantine.count() == 0
    assert silver.count() == len(clean_rows)
    assert silver.columns == SILVER_COLUMNS


def test_derived_cancellation_columns(bronze_factory, clean_rows):
    silver, _ = transform_silver(bronze_factory(clean_rows))
    cust = silver.filter("booking_status = 'Cancelled by Customer'").first()
    drv = silver.filter("booking_status = 'Cancelled by Driver'").first()
    assert cust["is_cancelled"] and cust["cancelled_by"] == "customer"
    assert cust["cancellation_reason"] == cust["customer_cancellation_reason"]
    assert drv["cancelled_by"] == "driver" and drv["cancellation_reason"] == drv["driver_cancellation_reason"]
    nd = silver.filter("booking_status = 'No Driver Found'").first()
    assert nd["is_cancelled"] is False and nd["cancellation_reason"] is None


@pytest.mark.parametrize(
    "overrides,reason",
    [
        ({"Booking Value": "-120"}, "negative_booking_value"),
        ({"Ride Distance": "-3.2"}, "negative_ride_distance"),
        ({"Driver Ratings": "5.6"}, "driver_rating_out_of_range"),
        ({"Customer Rating": "0.5"}, "customer_rating_out_of_range"),
        ({"Customer ID": NULL}, "missing_required_key"),
        ({"Date": "31-02-2024"}, "missing_required_key"),
        ({"Booking Value": NULL}, "completed_missing_value_or_distance"),
        ({"Booking Value": "abc"}, "invalid_numeric_value"),
        ({"Vehicle Type": "Helicopter"}, "invalid_vehicle_type"),
        ({"Booking Status": "Teleported"}, "invalid_booking_status"),
        ({"Payment Method": "Barter"}, "invalid_payment_method"),
        ({"Cancelled Rides by Customer": "1", "Reason for cancelling by Customer": "Wrong Address"},
         "customer_cancel_flag_mismatch"),
        ({"Incomplete Rides Reason": "Vehicle Breakdown"}, "reason_flag_mismatch"),
    ],
)
def test_invalid_rows_go_to_quarantine_with_reason(bronze_factory, overrides, reason):
    good = _row(**{"Booking ID": '"CNR1000001"'})
    bad = _row(**{"Booking ID": '"CNR1000002"', **overrides})
    silver, quarantine = transform_silver(bronze_factory([good, bad]))
    q = quarantine.collect()
    assert silver.count() == 1 and len(q) == 1
    assert reason in q[0]["reject_reasons"]
    assert q[0]["booking_id"] == "CNR1000002"


def test_quarantine_keeps_raw_values(bronze_factory):
    bad = _row(**{"Booking ID": '"CNR1000003"', "Booking Value": "abc"})
    _, quarantine = transform_silver(bronze_factory([bad]))
    assert quarantine.first()["booking_value"] == "abc"


def test_duplicates_are_removed_keeping_one(bronze_factory, clean_rows):
    rows = clean_rows[:50] + clean_rows[:5]
    silver, quarantine = transform_silver(bronze_factory(rows))
    assert silver.count() == 50
    assert silver.select("booking_id").distinct().count() == 50
    assert quarantine.count() == 5
    assert all("duplicate_booking_id" in r["reject_reasons"] for r in quarantine.collect())


def test_window_predicate():
    assert window_predicate(None, None) is None
    assert window_predicate("2024-03-01", "2024-03-31") == "booking_date >= '2024-03-01' AND booking_date <= '2024-03-31'"
    with pytest.raises(ValueError):
        window_predicate("2024-13-01", None)
