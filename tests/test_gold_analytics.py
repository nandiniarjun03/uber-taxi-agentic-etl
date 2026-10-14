"""Gold aggregations reconcile with Silver."""

from __future__ import annotations

from pyspark.sql import functions as F

from src import gold_analytics as g
from src.etl_pipeline import transform_silver
from tests.conftest import requires_spark

pytestmark = requires_spark


def _silver(bronze_factory, rows):
    silver, _ = transform_silver(bronze_factory(rows))
    return silver.cache()


def test_daily_bookings_reconcile_with_silver(bronze_factory, clean_rows):
    silver = _silver(bronze_factory, clean_rows)
    daily = g.daily_bookings(silver)
    totals = daily.agg(F.sum("total_bookings"), F.sum("completed_bookings"), F.sum("revenue")).first()
    assert totals[0] == silver.count()
    assert totals[1] == silver.filter("booking_status = 'Completed'").count()
    expected_rev = silver.filter("booking_status = 'Completed'").agg(F.sum("booking_value")).first()[0]
    assert abs(totals[2] - expected_rev) < 1.0
    assert daily.filter((F.col("completion_rate") < 0) | (F.col("completion_rate") > 1)).count() == 0


def test_revenue_by_vehicle_payment_matches_completed_value(bronze_factory, clean_rows):
    silver = _silver(bronze_factory, clean_rows)
    rev = g.revenue_by_vehicle_payment(silver).agg(F.sum("revenue")).first()[0]
    expected = silver.filter("is_completed").agg(F.sum("booking_value")).first()[0]
    assert abs(rev - expected) < 0.5


def test_cancellations_split_by_party(bronze_factory, clean_rows):
    silver = _silver(bronze_factory, clean_rows)
    c = g.cancellations_by_reason(silver)
    parties = {r["cancelled_by"] for r in c.select("cancelled_by").distinct().collect()}
    assert parties == {"customer", "driver"}
    shares = c.groupBy("cancelled_by").agg(F.sum("share_within_party").alias("s")).collect()
    assert all(abs(r["s"] - 1.0) < 0.01 for r in shares)


def test_vehicle_performance_and_top_locations(bronze_factory, clean_rows):
    silver = _silver(bronze_factory, clean_rows)
    perf = g.vehicle_performance(silver)
    assert perf.count() == silver.select("vehicle_type").distinct().count()
    assert perf.filter(~F.col("avg_driver_rating").between(1, 5)).count() == 0
    top = g.top_locations(silver, top_n=5)
    assert top.filter("location_type = 'pickup'").count() == 5
    assert g.peak_hour_demand(silver).agg(F.sum("bookings")).first()[0] == silver.count()
    inc = g.incomplete_rides_by_reason(silver)
    assert inc.agg(F.sum("incomplete_rides")).first()[0] == silver.filter("booking_status = 'Incomplete'").count()
