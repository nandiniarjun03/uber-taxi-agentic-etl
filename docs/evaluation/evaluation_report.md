# Agentic ETL - fault-injection evaluation

Generated 2026-10-14T20:11:53+00:00 · 3,000 synthetic rows per scenario · LLM client: `offline-rules`

## Metrics

| Metric | Value |
|---|---|
| detection rate | 1.0 |
| false alarm rate | 0.0 |
| recovery rate | 1.0 |
| status accuracy | 1.0 |
| median agent seconds | 11.94 |
| manual validation minutes baseline | 45.0 |
| time saved per run minutes | 44.8 |
| total tokens | 0 |
| avg tokens per run | 0.0 |
| llm client | offline-rules |
| rows per scenario | 3000 |

## Scenarios

| Scenario | Expected | Actual | Stop node (expected) | Rules flagged | Explained | Recovered | Agent s |
|---|---|---|---|---|---|---|---|
| healthy_1 | PASS | PASS | analytics (analytics) | - | yes | - | 24.06 |
| healthy_2 | PASS | PASS | analytics (analytics) | - | yes | - | 22.21 |
| healthy_3 | PASS | PASS | analytics (analytics) | - | yes | - | 19.58 |
| missing_source_file | FAIL | FAIL | etl (etl) | MissingSourceError | yes | yes | 0.24 |
| schema_change | FAIL | FAIL | etl (etl) | SourceSchemaError | yes | yes | 0.24 |
| job_failure | FAIL | FAIL | etl (etl) | job_failure | yes | yes | 0.24 |
| duplicate_records | FAIL | FAIL | quality (quality) | bronze_unique_booking_id, silver_quarantine_rate | yes | yes | 11.57 |
| null_spike | FAIL | FAIL | quality (quality) | bronze_key_completeness, silver_quarantine_rate | yes | yes | 12.28 |
| out_of_range_values | FAIL | FAIL | quality (quality) | bronze_value_ranges, silver_quarantine_rate | yes | yes | 11.94 |
| status_flag_mismatch | FAIL | FAIL | quality (quality) | bronze_status_flag_consistency, silver_quarantine_rate | yes | yes | 11.9 |
| row_count_drop | FAIL | FAIL | quality (quality) | bronze_volume_vs_previous_load, bronze_timeliness | yes | yes | 11.1 |
| missing_dates | FAIL | FAIL | quality (quality) | bronze_timeliness | yes | yes | 12.95 |
| payment_method_gap | PASS_WITH_WARNINGS | PASS_WITH_WARNINGS | analytics (analytics) | completed_rides_missing_payment_method | yes | yes | 18.34 |
