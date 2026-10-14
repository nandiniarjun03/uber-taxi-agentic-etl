"""End-to-end: CSV -> Bronze -> Silver -> Gold Delta tables, idempotency, incremental loads and
the agent workflow on real Spark. Needs table writes (runs on Linux CI)."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

from src import etl_pipeline, gold_analytics
from src import quality_checks as qc
from src.agents import AgentSystem, OfflineLLM
from src.fault_injection import apply_fault_to_landing_zone
from src.runners import LocalJobRunner, ReportStore, SparkPipelineOps
from src.synthetic_data import generate_rows, write_csv
from tests.conftest import requires_writes

pytestmark = requires_writes

ROWS = generate_rows(900, start=date(2024, 1, 1), end=date(2024, 3, 31), seed=21)


def _land(cfg, rows=ROWS):
    return write_csv(rows, Path(cfg.raw_path) / "ncr_ride_bookings.csv")


def test_full_pipeline_end_to_end(spark, cfg):
    _land(cfg)
    m = etl_pipeline.run_bronze_to_silver(spark, cfg)
    assert m["bronze"]["rows_out"] == len(ROWS)
    assert m["silver"]["rows_out"] + m["silver"]["rows_quarantined"] == len(ROWS)
    gold_analytics.build_gold_tables(spark, cfg)
    results = qc.run_bronze_checks(spark, cfg) + qc.run_silver_checks(spark, cfg) + qc.run_gold_checks(spark, cfg)
    assert qc.summarize(results)["checks_failed"] == 0, [(r.rule, r.status) for r in results if r.status != "PASS"]


def test_rerun_is_idempotent(spark, cfg):
    _land(cfg)
    etl_pipeline.run_bronze_to_silver(spark, cfg)
    etl_pipeline.run_bronze_to_silver(spark, cfg)
    from src.spark_utils import read_table

    assert read_table(spark, cfg.bronze_path, cfg).count() == len(ROWS)
    assert read_table(spark, cfg.silver_path, cfg).count() == len(ROWS)


def test_incremental_window_replaces_only_that_window(spark, cfg):
    from src.spark_utils import read_table

    _land(cfg)
    etl_pipeline.run_bronze_to_silver(spark, cfg)
    etl_pipeline.run_bronze_to_silver(spark, cfg, "2024-02-01", "2024-02-29")
    silver = read_table(spark, cfg.silver_path, cfg)
    assert silver.count() == len(ROWS)
    feb_batches = silver.filter("booking_date between '2024-02-01' and '2024-02-29'").select("batch_id").distinct()
    jan_batches = silver.filter("booking_date < '2024-02-01'").select("batch_id").distinct()
    assert feb_batches.count() == 1 and jan_batches.count() == 1
    assert feb_batches.first()[0] != jan_batches.first()[0]


def _agents(cfg, spark, fail=False):
    cfg = replace(cfg, agent=replace(cfg.agent, job_poll_seconds=0.1, job_max_retries=0))
    return AgentSystem(cfg, llm=OfflineLLM(), ops=SparkPipelineOps(cfg, spark), store=ReportStore(cfg),
                       runner=LocalJobRunner(cfg, spark=spark, simulate_failure=fail))


def test_agents_full_run_on_clean_data(spark, cfg):
    _land(cfg)
    state = _agents(cfg, spark).run("Run the full ETL pipeline")
    assert state["nodes_run"] == ["supervisor", "etl", "quality", "analytics"]
    assert state["status"] == "PASS", state["findings"]
    assert state["kpis"]["bookings"] == len(ROWS)


def test_agents_stop_on_duplicate_fault(spark, cfg):
    raw = _land(cfg)
    etl_pipeline.run_bronze_to_silver(spark, cfg)
    apply_fault_to_landing_zone(raw.parent, "duplicate_records")
    state = _agents(cfg, spark).run("Run the full ETL pipeline")
    assert state["status"] == "FAIL" and state["nodes_run"][-1] == "quality"
    assert "bronze_unique_booking_id" in [f["rule"] for f in state["findings"] if f["severity"] == "critical"]


def test_agents_stop_on_missing_source(spark, cfg):
    Path(cfg.raw_path).mkdir(parents=True, exist_ok=True)
    state = _agents(cfg, spark).run("Run the full ETL pipeline")
    assert state["nodes_run"] == ["supervisor", "etl"]
    assert state["findings"][0]["rule"] == "MissingSourceError"
