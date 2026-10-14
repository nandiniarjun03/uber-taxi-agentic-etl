"""Reusable data-quality and business-rule validation.

Two kinds of rules live here:

1. **Row rules** (``row_rule_conditions``): boolean Spark expressions that flag a single
   booking as invalid. The Silver job uses them to route rows to quarantine, and the
   Bronze checks use the *same* expressions to measure violation rates on raw data.
2. **Dataset checks** (``run_bronze_checks``, ``run_silver_checks``, ``run_gold_checks``):
   aggregate checks that each return a structured :class:`CheckResult`
   (rule, layer, status, metric value, threshold, sample failing records).

Status semantics: PASS (metric <= warn), WARN (warn < metric <= fail), FAIL (metric > fail).
``hard=True`` marks rules whose FAIL must stop the pipeline; the agents can never
override them.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from functools import reduce
from typing import Any, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import (
    ALLOWED_PAYMENT_METHODS,
    ALLOWED_STATUSES,
    ALLOWED_VEHICLE_TYPES,
    GOLD_TABLES,
    REQUIRED_COLUMNS,
    STATUS_CANCELLED_CUSTOMER,
    STATUS_CANCELLED_DRIVER,
    STATUS_COMPLETED,
    STATUS_INCOMPLETE,
    Config,
    QualityThresholds,
)

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
SAMPLE_COLUMNS = ["booking_id", "booking_status", "booking_date", "vehicle_type", "booking_value", "ride_distance",
                  "driver_rating", "customer_rating", "payment_method"]


@dataclass
class CheckResult:
    rule: str
    layer: str
    category: str
    status: str
    metric_value: float
    threshold: dict
    hard: bool
    description: str
    failing_count: int = 0
    total_count: int = 0
    sample_failures: list[dict] = field(default_factory=list)
    details: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status != FAIL

    def to_dict(self) -> dict:
        return json.loads(json.dumps(asdict(self), default=str))


class DataQualityError(RuntimeError):
    def __init__(self, results: list[CheckResult]):
        self.results = results
        failed = [r.rule for r in results if r.status == FAIL]
        super().__init__(f"Data quality checks failed: {failed}")


def grade(metric: float, warn: float, fail: float) -> str:
    if metric > fail:
        return FAIL
    if metric > warn:
        return WARN
    return PASS


def _rate(n: int, total: int) -> float:
    return round(n / total, 6) if total else 0.0


def any_of(conditions: list[Column]) -> Column:
    """OR of boolean conditions, null-safe (null counts as False)."""
    return reduce(lambda a, b: a | b, [F.coalesce(c, F.lit(False)) for c in conditions])


def _flag(col: str) -> Column:
    """Cancellation / incomplete flags are 1 or null; treat anything else non-null as set."""
    return F.coalesce(F.col(col).cast("int"), F.lit(0)) == 1


# --------------------------------------------------------------------------------------
# Row rules (shared by Silver quarantine and Bronze profiling)
# --------------------------------------------------------------------------------------
def row_rule_conditions() -> dict[str, Column]:
    """name -> condition that is TRUE when the row violates the rule.

    Expects a DataFrame already passed through ``etl_pipeline.standardise`` (typed columns).
    """
    status = F.col("booking_status")
    in_range = lambda c: F.col(c).isNull() | F.col(c).between(1.0, 5.0)  # noqa: E731
    has = lambda c: F.col(c).isNotNull()  # noqa: E731
    return {
        "missing_required_key": any_of([F.col(c).isNull() for c in REQUIRED_COLUMNS]),
        "invalid_numeric_value": F.coalesce(F.col("_numeric_parse_error"), F.lit(False)),
        "invalid_booking_status": status.isNotNull() & ~status.isin(*ALLOWED_STATUSES),
        "invalid_vehicle_type": has("vehicle_type") & ~F.col("vehicle_type").isin(*ALLOWED_VEHICLE_TYPES),
        "invalid_payment_method": has("payment_method") & ~F.col("payment_method").isin(*ALLOWED_PAYMENT_METHODS),
        "negative_booking_value": F.coalesce(F.col("booking_value") < 0, F.lit(False)),
        "negative_ride_distance": F.coalesce(F.col("ride_distance") < 0, F.lit(False)),
        "negative_turnaround_time": F.coalesce((F.col("avg_vtat") < 0) | (F.col("avg_ctat") < 0), F.lit(False)),
        "driver_rating_out_of_range": ~in_range("driver_rating"),
        "customer_rating_out_of_range": ~in_range("customer_rating"),
        "completed_missing_value_or_distance": (status == STATUS_COMPLETED)
        & (F.col("booking_value").isNull() | F.col("ride_distance").isNull()),
        "customer_cancel_flag_mismatch": status.isNotNull()
        & (_flag("cancelled_by_customer_flag") != (status == STATUS_CANCELLED_CUSTOMER)),
        "driver_cancel_flag_mismatch": status.isNotNull()
        & (_flag("cancelled_by_driver_flag") != (status == STATUS_CANCELLED_DRIVER)),
        "incomplete_flag_mismatch": status.isNotNull() & (_flag("incomplete_flag") != (status == STATUS_INCOMPLETE)),
        "reason_flag_mismatch": (has("customer_cancellation_reason") != _flag("cancelled_by_customer_flag"))
        | (has("driver_cancellation_reason") != _flag("cancelled_by_driver_flag"))
        | (has("incomplete_reason") != _flag("incomplete_flag")),
    }


# Grouping of row rules into dataset-level checks: name -> (category, rules, threshold attr, hard)
BRONZE_RULE_GROUPS: dict[str, tuple[str, list[str], str, bool, str]] = {
    "bronze_key_completeness": (
        "completeness", ["missing_required_key"], "key_null_rate", True,
        "Share of bookings missing Booking ID, Date/Time, Status, Customer ID, Vehicle Type, Pickup or Drop",
    ),
    "bronze_value_ranges": (
        "validity",
        ["invalid_numeric_value", "negative_booking_value", "negative_ride_distance", "negative_turnaround_time",
         "driver_rating_out_of_range", "customer_rating_out_of_range"],
        "value_range_rate", True,
        "Share of bookings with negative value/distance/VTAT/CTAT, ratings outside 1-5 or non-numeric text",
    ),
    "bronze_allowed_domains": (
        "validity", ["invalid_booking_status", "invalid_vehicle_type", "invalid_payment_method"], "domain_rate", True,
        "Share of bookings with a status, vehicle type or payment method outside the allowed lists",
    ),
    "bronze_status_flag_consistency": (
        "consistency", ["customer_cancel_flag_mismatch", "driver_cancel_flag_mismatch", "incomplete_flag_mismatch"],
        "flag_consistency_rate", True,
        "Share of bookings whose cancellation/incomplete flags contradict Booking Status",
    ),
    "bronze_reason_flag_pairing": (
        "consistency", ["reason_flag_mismatch"], "reason_pairing_rate", True,
        "Share of bookings where a reason is present without its flag, or a flag without its reason",
    ),
    "bronze_completed_outcome_completeness": (
        "outcome_completeness", ["completed_missing_value_or_distance"], "completed_outcome_rate", True,
        "Share of Completed bookings missing Booking Value or Ride Distance",
    ),
}


def _profile(std_df: DataFrame) -> tuple[int, dict[str, int]]:
    """Single pass: total rows + violation count per row rule."""
    conds = row_rule_conditions()
    aggs = [F.count(F.lit(1)).alias("_total")] + [
        F.sum(F.coalesce(c, F.lit(False)).cast("int")).alias(n) for n, c in conds.items()
    ]
    row = std_df.agg(*aggs).collect()[0].asDict()
    total = int(row.pop("_total") or 0)
    return total, {k: int(v or 0) for k, v in row.items()}


def _samples(df: DataFrame, condition: Column, n: int = 5) -> list[dict]:
    cols = [c for c in SAMPLE_COLUMNS if c in df.columns]
    rows = df.filter(condition).select(*cols).limit(n).collect()
    return [{k: (str(v) if v is not None else None) for k, v in r.asDict().items()} for r in rows]


# --------------------------------------------------------------------------------------
# Individual dataset checks (pure functions over DataFrames / numbers -> CheckResult)
# --------------------------------------------------------------------------------------
def check_rule_groups(std_df: DataFrame, layer: str, t: QualityThresholds, with_samples: bool = True,
                      groups: Optional[dict] = None, strict: bool = False) -> list[CheckResult]:
    """Evaluate grouped row rules. ``strict`` (Silver) means any violation is a failure."""
    groups = groups or BRONZE_RULE_GROUPS
    total, counts = _profile(std_df)
    conds = row_rule_conditions()
    results = []
    for name, (category, rules, thr_attr, hard, desc) in groups.items():
        cond = any_of([conds[r] for r in rules])
        failing = sum(counts[r] for r in rules)  # upper bound; exact count below if non-zero
        if failing:
            failing = std_df.filter(cond).count()
        warn, fail = (0.0, 0.0) if strict else getattr(t, thr_attr)
        metric = _rate(failing, total)
        status = grade(metric, warn, fail)
        results.append(
            CheckResult(
                rule=name if not strict else name.replace("bronze_", f"{layer}_"),
                layer=layer,
                category=category,
                status=status,
                metric_value=metric,
                threshold={"warn": warn, "fail": fail},
                hard=hard,
                description=desc,
                failing_count=failing,
                total_count=total,
                sample_failures=_samples(std_df, cond) if (with_samples and failing) else [],
                details={r: counts[r] for r in rules if counts[r]},
            )
        )
    return results


def check_not_empty(total: int, layer: str) -> CheckResult:
    return CheckResult(
        rule=f"{layer}_not_empty", layer=layer, category="volume", status=PASS if total > 0 else FAIL,
        metric_value=float(total), threshold={"min_rows": 1}, hard=True,
        description=f"{layer.title()} table contains at least one row", total_count=total,
    )


def check_uniqueness(df: DataFrame, layer: str, t: QualityThresholds, strict: bool = False) -> CheckResult:
    keyed = df.filter(F.col("booking_id").isNotNull())
    total = keyed.count()
    distinct = keyed.select("booking_id").distinct().count()
    dups = total - distinct
    warn, fail = (0.0, 0.0) if strict else t.duplicate_rate
    metric = _rate(dups, total)
    samples = []
    if dups:
        dup_ids = keyed.groupBy("booking_id").count().filter("count > 1").limit(5)
        samples = [{"booking_id": r["booking_id"], "copies": int(r["count"])} for r in dup_ids.collect()]
    return CheckResult(
        rule=f"{layer}_unique_booking_id", layer=layer, category="uniqueness", status=grade(metric, warn, fail),
        metric_value=metric, threshold={"warn": warn, "fail": fail}, hard=True,
        description="Share of Booking IDs that appear more than once", failing_count=dups, total_count=total,
        sample_failures=samples,
    )


def check_completed_payment(std_df: DataFrame, layer: str, t: QualityThresholds) -> CheckResult:
    completed = std_df.filter(F.col("booking_status") == STATUS_COMPLETED)
    total = completed.count()
    cond = F.col("payment_method").isNull()
    missing = completed.filter(cond).count()
    warn, fail = t.completed_payment_missing_rate
    metric = _rate(missing, total)
    return CheckResult(
        rule="completed_rides_missing_payment_method", layer=layer, category="outcome_completeness",
        status=grade(metric, warn, fail), metric_value=metric, threshold={"warn": warn, "fail": fail}, hard=False,
        description="Share of Completed bookings with a null Payment Method", failing_count=missing,
        total_count=total, sample_failures=_samples(completed, cond) if missing else [],
    )


def check_timeliness(std_df: DataFrame, layer: str, t: QualityThresholds,
                     expected_from: Optional[str] = None, expected_to: Optional[str] = None) -> CheckResult:
    """No missing booking dates inside the observed (or expected) date range."""
    stats = std_df.filter(F.col("booking_date").isNotNull()).agg(
        F.min("booking_date").alias("min_d"), F.max("booking_date").alias("max_d"),
        F.countDistinct("booking_date").alias("days"),
    ).collect()[0]
    if stats["min_d"] is None:
        return CheckResult(rule=f"{layer}_timeliness", layer=layer, category="timeliness", status=FAIL,
                           metric_value=1.0, threshold={"warn": 0, "fail": 0}, hard=True,
                           description="No parseable booking dates found")
    from datetime import date, timedelta

    start = date.fromisoformat(expected_from) if expected_from else stats["min_d"]
    end = date.fromisoformat(expected_to) if expected_to else stats["max_d"]
    span = (end - start).days + 1
    present = {r[0] for r in std_df.select("booking_date").distinct().collect() if r[0] is not None}
    missing = [start + timedelta(days=i) for i in range(span) if start + timedelta(days=i) not in present]
    warn, fail = t.missing_days_rate
    metric = _rate(len(missing), span)
    return CheckResult(
        rule=f"{layer}_timeliness", layer=layer, category="timeliness", status=grade(metric, warn, fail),
        metric_value=metric, threshold={"warn": warn, "fail": fail}, hard=True,
        description="Share of calendar days with no bookings inside the expected date range",
        failing_count=len(missing), total_count=span,
        sample_failures=[{"missing_date": d.isoformat()} for d in missing[:10]],
        details={"min_date": str(start), "max_date": str(end)},
    )


def check_volume(current: int, previous: Optional[int], layer: str, t: QualityThresholds) -> CheckResult:
    """Row count within tolerance of the previous load (first load passes with no baseline)."""
    warn, fail = t.volume_change
    if not previous:
        return CheckResult(rule=f"{layer}_volume_vs_previous_load", layer=layer, category="volume", status=PASS,
                           metric_value=0.0, threshold={"warn": warn, "fail": fail}, hard=True,
                           description="Relative row-count change versus the previous load (no baseline yet)",
                           total_count=current, details={"previous": previous, "current": current})
    change = abs(current - previous) / previous
    return CheckResult(
        rule=f"{layer}_volume_vs_previous_load", layer=layer, category="volume", status=grade(change, warn, fail),
        metric_value=round(change, 6), threshold={"warn": warn, "fail": fail}, hard=True,
        description="Relative row-count change versus the previous load",
        failing_count=abs(current - previous), total_count=current,
        details={"previous": previous, "current": current, "direction": "drop" if current < previous else "increase"},
    )


def check_schema(df: DataFrame, expected: list[str], layer: str) -> CheckResult:
    missing = [c for c in expected if c not in df.columns]
    return CheckResult(
        rule=f"{layer}_schema", layer=layer, category="validity", status=FAIL if missing else PASS,
        metric_value=float(len(missing)), threshold={"missing_columns": 0}, hard=True,
        description="All expected columns are present", failing_count=len(missing), total_count=len(expected),
        details={"missing_columns": missing},
    )


def check_reconciliation(bronze: int, silver: int, quarantine: int, layer: str = "silver") -> CheckResult:
    diff = bronze - (silver + quarantine)
    return CheckResult(
        rule="bronze_to_silver_reconciliation", layer=layer, category="reconciliation",
        status=PASS if diff == 0 else FAIL, metric_value=float(diff), threshold={"difference": 0}, hard=True,
        description="Bronze rows = Silver rows + Quarantine rows", failing_count=abs(diff), total_count=bronze,
        details={"bronze": bronze, "silver": silver, "quarantine": quarantine},
    )


def check_quarantine_rate(quarantine: int, bronze: int, t: QualityThresholds, breakdown: dict) -> CheckResult:
    warn, fail = t.quarantine_rate
    metric = _rate(quarantine, bronze)
    return CheckResult(
        rule="silver_quarantine_rate", layer="silver", category="validity", status=grade(metric, warn, fail),
        metric_value=metric, threshold={"warn": warn, "fail": fail}, hard=True,
        description="Share of Bronze rows rejected to quarantine", failing_count=quarantine, total_count=bronze,
        details={"reject_reason_counts": breakdown},
    )


# --------------------------------------------------------------------------------------
# Layer runners (used by the Databricks quality task and by the quality agent)
# --------------------------------------------------------------------------------------
def _audit_counts(spark: SparkSession, cfg: Config, stage: str) -> tuple[Optional[int], Optional[int]]:
    from src.etl_pipeline import read_audit

    rows = read_audit(spark, cfg, stage=stage, limit=2)
    current = rows[0]["rows_out"] if rows else None
    previous = rows[1]["rows_out"] if len(rows) > 1 else None
    return current, previous


def run_bronze_checks(spark: SparkSession, cfg: Config, bronze_df: Optional[DataFrame] = None,
                      previous_count: Optional[int] = None, use_audit: bool = True) -> list[CheckResult]:
    from src.etl_pipeline import BRONZE_DATA_COLUMNS, standardise
    from src.spark_utils import read_table

    t = cfg.thresholds
    bronze_df = bronze_df if bronze_df is not None else read_table(spark, cfg.bronze_path, cfg)
    results = [check_schema(bronze_df, BRONZE_DATA_COLUMNS, "bronze")]
    if results[0].status == FAIL:
        return results
    std = standardise(bronze_df).cache()
    total = std.count()
    results.append(check_not_empty(total, "bronze"))
    if total == 0:
        return results
    results += check_rule_groups(std, "bronze", t)
    results.append(check_uniqueness(std, "bronze", t))
    results.append(check_timeliness(std, "bronze", t))
    if use_audit and previous_count is None:
        _, previous_count = _audit_counts(spark, cfg, "bronze")
    results.append(check_volume(total, previous_count, "bronze", t))
    std.unpersist()
    return results


SILVER_GROUPS = {
    "silver_completeness": ("completeness", ["missing_required_key"], "key_null_rate", True,
                            "Silver rows missing any required key column (must be 0)"),
    "silver_validity": ("validity", ["invalid_numeric_value", "invalid_booking_status", "invalid_vehicle_type",
                                     "invalid_payment_method", "negative_booking_value", "negative_ride_distance",
                                     "negative_turnaround_time", "driver_rating_out_of_range",
                                     "customer_rating_out_of_range"], "value_range_rate", True,
                        "Silver rows violating value ranges or allowed domains (must be 0)"),
    "silver_consistency": ("consistency", ["customer_cancel_flag_mismatch", "driver_cancel_flag_mismatch",
                                           "incomplete_flag_mismatch", "reason_flag_mismatch"],
                           "flag_consistency_rate", True,
                           "Silver rows whose flags/reasons contradict Booking Status (must be 0)"),
    "silver_outcome_completeness": ("outcome_completeness", ["completed_missing_value_or_distance"],
                                    "completed_outcome_rate", True,
                                    "Completed Silver rows missing Booking Value or Ride Distance (must be 0)"),
}


def run_silver_checks(spark: SparkSession, cfg: Config) -> list[CheckResult]:
    from src.spark_utils import read_table

    t = cfg.thresholds
    silver = read_table(spark, cfg.silver_path, cfg).withColumn("_numeric_parse_error", F.lit(False)).cache()
    quarantine = read_table(spark, cfg.quarantine_path, cfg)
    bronze_count = read_table(spark, cfg.bronze_path, cfg).count()
    silver_count = silver.count()
    q_count = quarantine.count()
    breakdown = {
        r["reason"]: int(r["count"])
        for r in quarantine.select(F.explode("reject_reasons").alias("reason")).groupBy("reason").count().collect()
    }
    results = [check_not_empty(silver_count, "silver")]
    results += check_rule_groups(silver, "silver", t, groups=SILVER_GROUPS, strict=True)
    results.append(check_uniqueness(silver, "silver", t, strict=True))
    results.append(check_completed_payment(silver, "silver", t))
    results.append(check_reconciliation(bronze_count, silver_count, q_count))
    results.append(check_quarantine_rate(q_count, bronze_count, t, breakdown))
    silver.unpersist()
    return results


def run_gold_checks(spark: SparkSession, cfg: Config, history: Optional[list[dict]] = None) -> list[CheckResult]:
    """Gold validation: totals reconcile with Silver, no missing dates, plausible rates."""
    from src.spark_utils import read_table, table_exists

    t = cfg.thresholds
    results: list[CheckResult] = []
    missing_tables = [n for n in GOLD_TABLES if not table_exists(spark, cfg.gold_path(n), cfg)]
    results.append(CheckResult(
        rule="gold_tables_present", layer="gold", category="completeness", status=FAIL if missing_tables else PASS,
        metric_value=float(len(missing_tables)), threshold={"missing_tables": 0}, hard=True,
        description="Every Gold table was built", details={"missing": missing_tables},
    ))
    if missing_tables:
        return results

    silver = read_table(spark, cfg.silver_path, cfg)
    daily = read_table(spark, cfg.gold_path("daily_bookings"), cfg)
    s = silver.agg(F.count(F.lit(1)).alias("n"),
                   F.sum(F.when(F.col("is_completed"), F.col("booking_value"))).alias("rev")).collect()[0]
    d = daily.agg(F.sum("total_bookings").alias("n"), F.sum("revenue").alias("rev"),
                  (F.sum("completed_bookings") / F.sum("total_bookings")).alias("cr")).collect()[0]
    diff = int((s["n"] or 0) - (d["n"] or 0))
    results.append(CheckResult(
        rule="gold_daily_totals_reconcile", layer="gold", category="reconciliation",
        status=PASS if diff == 0 else FAIL, metric_value=float(diff), threshold={"difference": 0}, hard=True,
        description="Sum of daily bookings equals Silver row count",
        details={"silver_rows": s["n"], "gold_rows": d["n"]},
    ))
    rev_by_vehicle = read_table(spark, cfg.gold_path("revenue_by_vehicle_payment"), cfg).agg(
        F.sum("revenue")).collect()[0][0] or 0.0
    rev_diff = abs((s["rev"] or 0.0) - rev_by_vehicle)
    results.append(CheckResult(
        rule="gold_revenue_reconcile", layer="gold", category="reconciliation",
        status=PASS if rev_diff <= t.reconciliation_tolerance else FAIL, metric_value=round(rev_diff, 4),
        threshold={"tolerance": t.reconciliation_tolerance}, hard=True,
        description="Revenue by vehicle/payment equals Silver completed booking value",
        details={"silver_revenue": s["rev"], "gold_revenue": rev_by_vehicle},
    ))
    results.append(check_timeliness(daily, "gold", t))
    results[-1].rule = "gold_no_missing_dates"

    cr = float(d["cr"] or 0.0)
    lo, hi = t.completion_rate_bounds
    hist = [h["completion_rate"] for h in (history or []) if h.get("completion_rate") is not None]
    baseline = sum(hist) / len(hist) if hist else None
    drift = abs(cr - baseline) if baseline is not None else 0.0
    plausible = lo <= cr <= hi
    status = FAIL if not plausible else (WARN if drift > t.completion_rate_drift else PASS)
    results.append(CheckResult(
        rule="gold_completion_rate_plausible", layer="gold", category="validity", status=status,
        metric_value=round(cr, 4), threshold={"min": lo, "max": hi, "max_drift_vs_history": t.completion_rate_drift},
        hard=False, description="Overall completion rate is within bounds and close to history",
        details={"history_baseline": baseline, "drift": round(drift, 4)},
    ))
    perf = read_table(spark, cfg.gold_path("vehicle_performance"), cfg)
    bad = perf.filter(~F.col("avg_driver_rating").between(1, 5) | ~F.col("avg_customer_rating").between(1, 5)
                      | (F.col("avg_vtat") < 0) | (F.col("avg_ctat") < 0)).count()
    results.append(CheckResult(
        rule="gold_averages_plausible", layer="gold", category="validity", status=FAIL if bad else PASS,
        metric_value=float(bad), threshold={"implausible_rows": 0}, hard=True,
        description="Average ratings within 1-5 and turnaround times non-negative per vehicle type",
        failing_count=bad, total_count=perf.count(),
    ))
    return results


def run_checks(spark: SparkSession, cfg: Config, layer: str, **kwargs: Any) -> list[CheckResult]:
    runners = {"bronze": run_bronze_checks, "silver": run_silver_checks, "gold": run_gold_checks}
    if layer not in runners:
        raise ValueError(f"Unknown layer '{layer}'")
    return runners[layer](spark, cfg, **kwargs)


def summarize(results: list[CheckResult]) -> dict:
    return {
        "checks_total": len(results),
        "checks_passed": sum(r.status == PASS for r in results),
        "checks_warned": sum(r.status == WARN for r in results),
        "checks_failed": sum(r.status == FAIL for r in results),
        "hard_failures": [r.rule for r in results if r.status == FAIL and r.hard],
        "soft_failures": [r.rule for r in results if r.status == FAIL and not r.hard],
        "warnings": [r.rule for r in results if r.status == WARN],
    }


def assert_checks_pass(results: list[CheckResult]) -> None:
    """Raise if any check FAILs - used by the Databricks quality task so the job run fails."""
    if any(r.status == FAIL for r in results):
        raise DataQualityError(results)
