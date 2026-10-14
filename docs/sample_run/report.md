# Run report `2026-10-14T20-04-38-8af0`

**Status:** ⚠️ PASS_WITH_WARNINGS  
**Request:** Run the full ETL pipeline for the latest Uber bookings and validate every layer  
**Path:** supervisor → etl → quality → analytics  
**Duration:** 80.84 s · **LLM:** offline-rules (0 tokens, 2 calls)  
**Escalated to human:** False

## Summary

Run 2026-10-14T20-04-38-8af0 finished with status PASS_WITH_WARNINGS after nodes supervisor, etl, quality, analytics. Row counts: {'bronze': 150000, 'silver': 148767, 'quarantine': 1233}. Warnings to investigate: bronze_unique_booking_id. No human action required.

## Row counts

| Layer | Rows |
|---|---|
| bronze | 150000 |
| silver | 148767 |
| quarantine | 1233 |

## Gold KPIs

```json
{
  "bookings": 148767,
  "completed": 92244,
  "completion_rate": 0.6201,
  "revenue": 46859058.0,
  "from_date": "2024-01-01",
  "to_date": "2024-12-30"
}
```

## Findings

| Rule | Layer | Severity | Explanation | Recommended action |
|---|---|---|---|---|
| `bronze_unique_booking_id` | bronze | **warning** | Share of Booking IDs that appear more than once: observed 0.82% vs threshold warn > 0.50%, fail > 2.00% (1233 of 150000 records). | Check whether the same file was delivered twice or overlapping extracts were landed; de-duplicate at source and re-ingest. |

## Quality checks

| Rule | Layer | Status | Metric | Threshold |
|---|---|---|---|---|
| `bronze_schema` | bronze | PASS | 0.0 | {'missing_columns': 0} |
| `bronze_not_empty` | bronze | PASS | 150000.0 | {'min_rows': 1} |
| `bronze_key_completeness` | bronze | PASS | 0.0 | {'warn': 0.005, 'fail': 0.02} |
| `bronze_value_ranges` | bronze | PASS | 0.0 | {'warn': 0.001, 'fail': 0.01} |
| `bronze_allowed_domains` | bronze | PASS | 0.0 | {'warn': 0.001, 'fail': 0.01} |
| `bronze_status_flag_consistency` | bronze | PASS | 0.0 | {'warn': 0.001, 'fail': 0.01} |
| `bronze_reason_flag_pairing` | bronze | PASS | 0.0 | {'warn': 0.001, 'fail': 0.01} |
| `bronze_completed_outcome_completeness` | bronze | PASS | 0.0 | {'warn': 0.001, 'fail': 0.01} |
| `bronze_unique_booking_id` | bronze | WARN | 0.00822 | {'warn': 0.005, 'fail': 0.02} |
| `bronze_timeliness` | bronze | PASS | 0.0 | {'warn': 0.0, 'fail': 0.01} |
| `bronze_volume_vs_previous_load` | bronze | PASS | 0.0 | {'warn': 0.1, 'fail': 0.2} |
| `silver_not_empty` | silver | PASS | 148767.0 | {'min_rows': 1} |
| `silver_completeness` | silver | PASS | 0.0 | {'warn': 0.0, 'fail': 0.0} |
| `silver_validity` | silver | PASS | 0.0 | {'warn': 0.0, 'fail': 0.0} |
| `silver_consistency` | silver | PASS | 0.0 | {'warn': 0.0, 'fail': 0.0} |
| `silver_outcome_completeness` | silver | PASS | 0.0 | {'warn': 0.0, 'fail': 0.0} |
| `silver_unique_booking_id` | silver | PASS | 0.0 | {'warn': 0.0, 'fail': 0.0} |
| `completed_rides_missing_payment_method` | silver | PASS | 0.0 | {'warn': 0.001, 'fail': 0.05} |
| `bronze_to_silver_reconciliation` | silver | PASS | 0.0 | {'difference': 0} |
| `silver_quarantine_rate` | silver | PASS | 0.00822 | {'warn': 0.02, 'fail': 0.05} |
| `gold_tables_present` | gold | PASS | 0.0 | {'missing_tables': 0} |
| `gold_daily_totals_reconcile` | gold | PASS | 0.0 | {'difference': 0} |
| `gold_revenue_reconcile` | gold | PASS | 0.0 | {'tolerance': 0.01} |
| `gold_no_missing_dates` | gold | PASS | 0.0 | {'warn': 0.0, 'fail': 0.01} |
| `gold_completion_rate_plausible` | gold | PASS | 0.6201 | {'min': 0.3, 'max': 0.95, 'max_drift_vs_history': 0.15} |
| `gold_averages_plausible` | gold | PASS | 0.0 | {'implausible_rows': 0} |

## Tool calls (audit log)

| Time | Agent | Tool | Status | ms |
|---|---|---|---|---|
| 2026-10-14T20:04:38+00:00 | etl | `run_etl_job` | OK | 1 |
| 2026-10-14T20:04:38+00:00 | etl | `get_run_status` | OK | 0 |
| 2026-10-14T20:04:40+00:00 | etl | `get_run_status` | OK | 0 |
| 2026-10-14T20:04:42+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:04:44+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:04:46+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:04:48+00:00 | etl | `get_run_status` | OK | 2 |
| 2026-10-14T20:04:50+00:00 | etl | `get_run_status` | OK | 0 |
| 2026-10-14T20:04:52+00:00 | etl | `get_run_status` | OK | 0 |
| 2026-10-14T20:04:54+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:04:56+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:04:58+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:00+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:02+00:00 | etl | `get_run_status` | OK | 4 |
| 2026-10-14T20:05:04+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:06+00:00 | etl | `get_run_status` | OK | 2 |
| 2026-10-14T20:05:08+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:10+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:12+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:14+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:16+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:18+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:20+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:22+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:24+00:00 | etl | `get_run_status` | OK | 0 |
| 2026-10-14T20:05:26+00:00 | etl | `get_run_status` | OK | 1 |
| 2026-10-14T20:05:28+00:00 | etl | `get_run_status` | OK | 0 |
| 2026-10-14T20:05:28+00:00 | quality | `run_quality_checks` | OK | 11361 |
| 2026-10-14T20:05:40+00:00 | quality | `run_quality_checks` | OK | 5601 |
| 2026-10-14T20:05:45+00:00 | quality | `compare_row_counts` | OK | 1327 |
| 2026-10-14T20:05:46+00:00 | analytics | `build_gold_tables` | OK | 10480 |
| 2026-10-14T20:05:57+00:00 | analytics | `run_quality_checks` | OK | 1796 |
| 2026-10-14T20:05:59+00:00 | analytics | `gold_kpis` | OK | 199 |
| 2026-10-14T20:05:59+00:00 | analytics | `write_report` | OK | 13 |
