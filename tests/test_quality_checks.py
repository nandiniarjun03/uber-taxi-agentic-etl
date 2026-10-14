"""Quality rules: pass on clean data, fail correctly on deliberately corrupted data."""

from __future__ import annotations

import pytest

from src import quality_checks as qc
from src.config import SOURCE_COLUMNS, QualityThresholds
from src.fault_injection import corrupt_rows
from tests.conftest import requires_spark

T = QualityThresholds()


# ---- pure functions (no Spark) --------------------------------------------------------
def test_grade_boundaries():
    assert qc.grade(0.0, 0.01, 0.05) == "PASS"
    assert qc.grade(0.01, 0.01, 0.05) == "PASS"
    assert qc.grade(0.02, 0.01, 0.05) == "WARN"
    assert qc.grade(0.06, 0.01, 0.05) == "FAIL"


def test_volume_check():
    assert qc.check_volume(1000, None, "bronze", T).status == "PASS"  # first load, no baseline
    assert qc.check_volume(950, 1000, "bronze", T).status == "PASS"
    assert qc.check_volume(850, 1000, "bronze", T).status == "WARN"
    drop = qc.check_volume(450, 1000, "bronze", T)
    assert drop.status == "FAIL" and drop.details["direction"] == "drop"


def test_reconciliation_check():
    assert qc.check_reconciliation(100, 95, 5).status == "PASS"
    r = qc.check_reconciliation(100, 90, 5)
    assert r.status == "FAIL" and r.metric_value == 5


def test_check_result_is_structured():
    d = qc.check_volume(450, 1000, "bronze", T).to_dict()
    for key in ("rule", "layer", "status", "metric_value", "threshold", "sample_failures", "hard"):
        assert key in d


def test_summarize_and_assert():
    results = [qc.check_volume(1000, 1000, "bronze", T), qc.check_volume(10, 1000, "bronze", T)]
    s = qc.summarize(results)
    assert s["checks_failed"] == 1 and s["hard_failures"] == ["bronze_volume_vs_previous_load"]
    with pytest.raises(qc.DataQualityError):
        qc.assert_checks_pass(results)


# ---- Spark-based checks ---------------------------------------------------------------
@requires_spark
def test_clean_bronze_passes_all_checks(spark, cfg, bronze_factory, clean_rows):
    results = qc.run_bronze_checks(spark, cfg, bronze_factory(clean_rows), use_audit=False)
    assert {r.status for r in results} == {"PASS"}, [(r.rule, r.status, r.metric_value) for r in results]
    rules = {r.rule for r in results}
    assert {"bronze_key_completeness", "bronze_unique_booking_id", "bronze_value_ranges",
            "bronze_allowed_domains", "bronze_status_flag_consistency", "bronze_reason_flag_pairing",
            "bronze_completed_outcome_completeness", "bronze_timeliness",
            "bronze_volume_vs_previous_load"} <= rules


@requires_spark
@pytest.mark.parametrize(
    "fault,rule",
    [
        ("duplicate_records", "bronze_unique_booking_id"),
        ("null_spike", "bronze_key_completeness"),
        ("out_of_range_values", "bronze_value_ranges"),
        ("status_flag_mismatch", "bronze_status_flag_consistency"),
        ("missing_dates", "bronze_timeliness"),
        ("row_count_drop", "bronze_volume_vs_previous_load"),
    ],
)
def test_corrupted_bronze_fails_expected_rule(spark, cfg, bronze_factory, clean_rows, fault, rule):
    # 600 rows over a year leaves gaps; use a dense 60-day range so timeliness is meaningful
    from datetime import date

    from src.synthetic_data import generate_rows

    rows = generate_rows(1200, start=date(2024, 1, 1), end=date(2024, 2, 29), seed=5)
    _, bad = corrupt_rows(fault, SOURCE_COLUMNS, rows)
    results = {r.rule: r for r in qc.run_bronze_checks(spark, cfg, bronze_factory(bad),
                                                       previous_count=len(rows), use_audit=False)}
    assert results[rule].status == "FAIL", results[rule]
    assert results[rule].hard
    if results[rule].failing_count and rule not in ("bronze_volume_vs_previous_load",):
        assert results[rule].sample_failures, "failing records should be sampled"


@requires_spark
def test_payment_gap_is_only_a_warning(spark, bronze_factory, clean_rows):
    from src.etl_pipeline import standardise

    _, bad = corrupt_rows("payment_method_gap", SOURCE_COLUMNS, clean_rows)
    r = qc.check_completed_payment(standardise(bronze_factory(bad)), "silver", T)
    assert r.status == "WARN" and not r.hard
    assert r.rule == "completed_rides_missing_payment_method"


@requires_spark
def test_silver_rules_are_strict(spark, bronze_factory, clean_rows):
    from src.etl_pipeline import standardise

    _, bad = corrupt_rows("out_of_range_values", SOURCE_COLUMNS, clean_rows)
    std = standardise(bronze_factory(bad))
    res = {r.rule: r for r in qc.check_rule_groups(std, "silver", T, groups=qc.SILVER_GROUPS, strict=True)}
    assert res["silver_validity"].status == "FAIL"
    assert res["silver_validity"].threshold == {"warn": 0.0, "fail": 0.0}
