"""Silver -> Gold: business-ready aggregate tables.

Each builder is a pure function ``silver_df -> DataFrame`` (unit-testable); ``build_gold_tables``
reads Silver, writes every table to gold/<table> and optionally runs OPTIMIZE / ZORDER.
Gold tables are fully rebuilt on each run, which keeps them idempotent and always
consistent with Silver.
"""

from __future__ import annotations

import logging
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config import (
    STATUS_CANCELLED_CUSTOMER,
    STATUS_CANCELLED_DRIVER,
    STATUS_COMPLETED,
    STATUS_INCOMPLETE,
    STATUS_NO_DRIVER,
    Config,
)
from src.spark_utils import optimize_table, read_table, write_table

log = logging.getLogger(__name__)


def _count_if(cond) -> F.Column:
    return F.sum(F.when(cond, 1).otherwise(0))


def _completed_value() -> F.Column:
    return F.when(F.col("booking_status") == STATUS_COMPLETED, F.col("booking_value"))


def daily_bookings(silver: DataFrame) -> DataFrame:
    """Bookings, outcome mix, completion rate and revenue per booking date."""
    status = F.col("booking_status")
    return (
        silver.groupBy("booking_date")
        .agg(
            F.count(F.lit(1)).alias("total_bookings"),
            _count_if(status == STATUS_COMPLETED).alias("completed_bookings"),
            _count_if(status == STATUS_CANCELLED_CUSTOMER).alias("cancelled_by_customer"),
            _count_if(status == STATUS_CANCELLED_DRIVER).alias("cancelled_by_driver"),
            _count_if(status == STATUS_NO_DRIVER).alias("no_driver_found"),
            _count_if(status == STATUS_INCOMPLETE).alias("incomplete_rides"),
            F.round(F.coalesce(F.sum(_completed_value()), F.lit(0.0)), 2).alias("revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("completion_rate", F.round(F.col("completed_bookings") / F.col("total_bookings"), 4))
        .withColumn("cancellation_rate", F.round(
            (F.col("cancelled_by_customer") + F.col("cancelled_by_driver")) / F.col("total_bookings"), 4))
        .orderBy("booking_date")
    )


def cancellations_by_reason(silver: DataFrame) -> DataFrame:
    """Customer vs driver cancellations by unified reason, with share within each party."""
    cancelled = silver.filter(F.col("is_cancelled"))
    agg = cancelled.groupBy("cancelled_by", "cancellation_reason").agg(F.count(F.lit(1)).alias("cancellations"))
    w = Window.partitionBy("cancelled_by")
    return agg.withColumn(
        "share_within_party", F.round(F.col("cancellations") / F.sum("cancellations").over(w), 4)
    ).orderBy("cancelled_by", F.desc("cancellations"))


def revenue_by_vehicle_payment(silver: DataFrame) -> DataFrame:
    """Revenue, ride count and unit economics for completed rides per vehicle type and payment method."""
    completed = silver.filter(F.col("booking_status") == STATUS_COMPLETED)
    return (
        completed.groupBy("vehicle_type", F.coalesce(F.col("payment_method"), F.lit("Unknown")).alias("payment_method"))
        .agg(
            F.count(F.lit(1)).alias("completed_rides"),
            F.round(F.sum("booking_value"), 2).alias("revenue"),
            F.round(F.avg("booking_value"), 2).alias("avg_booking_value"),
            F.round(F.avg("ride_distance"), 2).alias("avg_ride_distance_km"),
            F.round(F.sum("booking_value") / F.sum("ride_distance"), 2).alias("revenue_per_km"),
        )
        .orderBy(F.desc("revenue"))
    )


def peak_hour_demand(silver: DataFrame) -> DataFrame:
    """Demand heatmap: bookings and completion rate by day of week and hour."""
    return (
        silver.groupBy("day_of_week", "booking_hour")
        .agg(
            F.count(F.lit(1)).alias("bookings"),
            _count_if(F.col("booking_status") == STATUS_COMPLETED).alias("completed"),
            _count_if(F.col("booking_status") == STATUS_NO_DRIVER).alias("no_driver_found"),
        )
        .withColumn("completion_rate", F.round(F.col("completed") / F.col("bookings"), 4))
        .withColumn("demand_rank", F.dense_rank().over(Window.orderBy(F.desc("bookings"))))
        .orderBy("demand_rank")
    )


def top_locations(silver: DataFrame, top_n: int = 20) -> DataFrame:
    """Top pickup and drop locations by bookings, with completion rate and revenue."""

    def by(col: str, kind: str) -> DataFrame:
        return silver.groupBy(F.col(col).alias("location")).agg(
            F.count(F.lit(1)).alias("bookings"),
            F.round(F.avg(F.col("is_completed").cast("int")), 4).alias("completion_rate"),
            F.round(F.coalesce(F.sum(_completed_value()), F.lit(0.0)), 2).alias("revenue"),
        ).withColumn("location_type", F.lit(kind))

    both = by("pickup_location", "pickup").unionByName(by("drop_location", "drop"))
    w = Window.partitionBy("location_type").orderBy(F.desc("bookings"), "location")
    return both.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= top_n).orderBy(
        "location_type", "rank")


def vehicle_performance(silver: DataFrame) -> DataFrame:
    """Per vehicle type: volume, completion, average VTAT/CTAT, ratings and distance."""
    return (
        silver.groupBy("vehicle_type")
        .agg(
            F.count(F.lit(1)).alias("bookings"),
            F.round(F.avg(F.col("is_completed").cast("int")), 4).alias("completion_rate"),
            F.round(F.avg("avg_vtat"), 2).alias("avg_vtat"),
            F.round(F.avg("avg_ctat"), 2).alias("avg_ctat"),
            F.round(F.avg("driver_rating"), 3).alias("avg_driver_rating"),
            F.round(F.avg("customer_rating"), 3).alias("avg_customer_rating"),
            F.round(F.avg(F.when(F.col("is_completed"), F.col("ride_distance"))), 2).alias("avg_ride_distance_km"),
            F.round(F.coalesce(F.sum(_completed_value()), F.lit(0.0)), 2).alias("revenue"),
        )
        .orderBy(F.desc("bookings"))
    )


def incomplete_rides_by_reason(silver: DataFrame) -> DataFrame:
    incomplete = silver.filter(F.col("booking_status") == STATUS_INCOMPLETE)
    total = F.sum("incomplete_rides").over(Window.partitionBy())
    return (
        incomplete.groupBy("incomplete_reason", "vehicle_type")
        .agg(F.count(F.lit(1)).alias("incomplete_rides"))
        .withColumn("share", F.round(F.col("incomplete_rides") / total, 4))
        .orderBy(F.desc("incomplete_rides"))
    )


GOLD_BUILDERS: dict[str, tuple[Callable[[DataFrame], DataFrame], list[str]]] = {
    # table name -> (builder, ZORDER columns)
    "daily_bookings": (daily_bookings, ["booking_date"]),
    "cancellations_by_reason": (cancellations_by_reason, ["cancelled_by"]),
    "revenue_by_vehicle_payment": (revenue_by_vehicle_payment, ["vehicle_type"]),
    "peak_hour_demand": (peak_hour_demand, ["day_of_week"]),
    "top_locations": (top_locations, ["location_type"]),
    "vehicle_performance": (vehicle_performance, ["vehicle_type"]),
    "incomplete_rides_by_reason": (incomplete_rides_by_reason, ["incomplete_reason"]),
}


def build_gold_tables(spark: SparkSession, cfg: Config) -> dict:
    """Build every Gold table from Silver. Returns {table: row_count} plus optimisation info."""
    silver = read_table(spark, cfg.silver_path, cfg).cache()
    counts: dict[str, int] = {}
    optimized = []
    for name, (builder, zorder) in GOLD_BUILDERS.items():
        df = builder(silver)
        path = cfg.gold_path(name)
        write_table(df, path, cfg)
        counts[name] = read_table(spark, path, cfg).count()
        if optimize_table(spark, path, cfg, zorder_by=zorder):
            optimized.append(name)
    silver.unpersist()
    log.info("Gold tables built: %s", counts)
    return {"stage": "gold", "tables": counts, "optimized": optimized}


def gold_kpis(spark: SparkSession, cfg: Config) -> dict:
    """Headline KPIs used in run reports and as history for plausibility checks."""
    daily = read_table(spark, cfg.gold_path("daily_bookings"), cfg)
    row = daily.agg(
        F.sum("total_bookings").alias("bookings"),
        F.sum("completed_bookings").alias("completed"),
        F.round(F.sum("revenue"), 2).alias("revenue"),
        F.min("booking_date").alias("from_date"),
        F.max("booking_date").alias("to_date"),
    ).collect()[0]
    bookings = int(row["bookings"] or 0)
    return {
        "bookings": bookings,
        "completed": int(row["completed"] or 0),
        "completion_rate": round((row["completed"] or 0) / bookings, 4) if bookings else None,
        "revenue": float(row["revenue"] or 0.0),
        "from_date": str(row["from_date"]),
        "to_date": str(row["to_date"]),
    }
