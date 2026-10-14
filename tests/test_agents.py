"""Agent workflow tests with fake job runner / pipeline ops (no Spark, no Azure)."""

from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path

import pytest

from src.agents import (
    AGENT_TOOL_ALLOWLIST,
    AgentSystem,
    LLMClient,
    OfflineLLM,
    ToolNotAllowedError,
    _merge_findings,
    mask_sensitive,
)
from src.config import Config
from src.runners import JobRun, PathNotAllowedError, ReportStore


def _check(rule, status="PASS", hard=True, layer="bronze", metric=0.0):
    return {"rule": rule, "layer": layer, "category": "validity", "status": status, "metric_value": metric,
            "threshold": {"warn": 0.001, "fail": 0.01}, "hard": hard, "description": f"desc {rule}",
            "failing_count": 3 if status != "PASS" else 0, "total_count": 100,
            "sample_failures": [{"booking_id": "CNR1234567", "customer_id": "CID9999999"}] if status != "PASS" else [],
            "details": {}}


class FakeRunner:
    def __init__(self, outcomes):
        """outcomes: list of (result_state, error_type) consumed per submit."""
        self.outcomes = list(outcomes)
        self.submits = 0
        self._runs = {}

    def submit(self, job, params):
        self.submits += 1
        result, err = self.outcomes.pop(0) if self.outcomes else ("SUCCESS", None)
        rid = f"r{self.submits}"
        output = {"bronze": {"rows_out": 1000}, "silver": {"rows_out": 990, "rows_quarantined": 10}} \
            if result == "SUCCESS" else {"trace_tail": "Traceback ..."}
        self._runs[rid] = JobRun(run_id=rid, job=job, state="TERMINATED", result_state=result, error_type=err,
                                 message=f"{err}: boom" if err else "ok", output=output, ended_at=time.time())
        return rid

    def get_status(self, run_id):
        return self._runs[run_id]

    def cancel(self, run_id):
        pass


class FakeOps:
    def __init__(self, bronze=None, silver=None, gold=None):
        self.results = {
            "bronze": bronze or [_check("bronze_key_completeness"), _check("bronze_unique_booking_id")],
            "silver": silver or [_check("silver_validity", layer="silver"),
                                 _check("completed_rides_missing_payment_method", layer="silver", hard=False)],
            "gold": gold or [_check("gold_daily_totals_reconcile", layer="gold")],
        }
        self.gold_built = False

    def run_quality_checks(self, layer, history=None):
        return self.results[layer]

    def table_stats(self, layer, table=None):
        return {"layer": layer, "rows": 1000, "exists": True}

    def row_counts(self):
        return {"bronze": 1000, "silver": 990, "quarantine": 10}

    def build_gold(self):
        self.gold_built = True
        return {"tables": {"daily_bookings": 366}}

    def gold_kpis(self):
        return {"bookings": 990, "completion_rate": 0.62, "revenue": 123456.0}


@pytest.fixture
def cfg(tmp_path) -> Config:
    c = Config().with_local_root(tmp_path / "lake")
    return replace(c, agent=replace(c.agent, job_poll_seconds=0.01, job_max_retries=1))


def make_system(cfg, runner=None, ops=None, llm=None, approve=False):
    return AgentSystem(cfg, llm=llm or OfflineLLM(), runner=runner or FakeRunner([]), ops=ops or FakeOps(),
                       store=ReportStore(cfg), approval_callback=lambda a, d: approve)


# ---- routing ---------------------------------------------------------------------------
@pytest.mark.parametrize("request_text,route", [
    ("Run the full pipeline", "run_etl"),
    ("Ingest today's files", "run_etl"),
    ("Validate the bronze and silver data", "quality"),
    ("Rebuild the gold analytics tables", "analytics"),
    ("Why did the last run fail?", "explain"),
    ("Delete the silver table", "explain"),
])
def test_offline_routing(request_text, route):
    assert OfflineLLM._route({"request": request_text})["route"] == route


# ---- workflow paths ------------------------------------------------------------------
def test_full_run_passes_through_all_nodes(cfg):
    ops = FakeOps()
    state = make_system(cfg, ops=ops).run("Run the full ETL pipeline")
    assert state["nodes_run"] == ["supervisor", "etl", "quality", "analytics"]
    assert state["status"] == "PASS"
    assert ops.gold_built
    report = json.loads(Path(state["report_paths"]["json"]).read_text())
    assert report["status"] == "PASS" and report["row_counts"]["bronze"] == 1000
    assert report["escalated_to_human"] is False
    assert Path(state["report_paths"]["markdown"]).read_text(encoding="utf-8").startswith("# Run report")


def test_soft_warning_gives_pass_with_warnings(cfg):
    ops = FakeOps(silver=[_check("completed_rides_missing_payment_method", "WARN", hard=False, layer="silver",
                                 metric=0.004)])
    state = make_system(cfg, ops=ops).run("Run the pipeline")
    assert state["status"] == "PASS_WITH_WARNINGS"
    assert state["nodes_run"][-1] == "analytics"
    f = state["findings"][0]
    assert f["rule"] == "completed_rides_missing_payment_method" and f["severity"] == "warning"
    assert "0.40%" in f["explanation"]


def test_hard_quality_failure_stops_before_analytics(cfg):
    ops = FakeOps(bronze=[_check("bronze_key_completeness", "FAIL", metric=0.12)])
    state = make_system(cfg, ops=ops).run("Run the pipeline")
    assert state["nodes_run"] == ["supervisor", "etl", "quality"]
    assert state["status"] == "FAIL"
    assert not ops.gold_built
    crit = [f for f in state["findings"] if f["severity"] == "critical"]
    assert crit[0]["rule"] == "bronze_key_completeness" and crit[0]["recommended_action"]
    assert Path(state["report_paths"]["markdown"]).exists()


def test_non_retryable_job_failure_ends_at_etl(cfg):
    runner = FakeRunner([("FAILED", "SourceSchemaError")])
    state = make_system(cfg, runner=runner).run("Run the pipeline")
    assert state["nodes_run"] == ["supervisor", "etl"]
    assert state["status"] == "FAIL" and runner.submits == 1
    assert state["findings"][0]["rule"] == "SourceSchemaError"


def test_transient_job_failure_is_retried(cfg):
    runner = FakeRunner([("FAILED", "SimulatedJobFailure"), ("SUCCESS", None)])
    state = make_system(cfg, runner=runner).run("Run the pipeline")
    assert runner.submits == 2 and state["etl_attempts"] == 2
    assert state["nodes_run"][-1] == "analytics"


def test_retry_limit_is_respected(cfg):
    runner = FakeRunner([("FAILED", "SimulatedJobFailure")] * 5)
    state = make_system(cfg, runner=runner).run("Run the pipeline")
    assert runner.submits == 2  # 1 attempt + job_max_retries=1
    assert state["findings"][0]["rule"] == "job_failure"


def test_validation_only_route_skips_etl(cfg):
    runner = FakeRunner([])
    state = make_system(cfg, runner=runner).run("Validate the existing data quality")
    assert state["nodes_run"] == ["supervisor", "quality", "analytics"]
    assert runner.submits == 0


def test_explain_route_answers_from_history(cfg):
    system = make_system(cfg, ops=FakeOps(bronze=[_check("bronze_key_completeness", "FAIL", metric=0.12)]))
    system.run("Run the pipeline")
    state = system.run("Why did the last run fail?")
    assert state["nodes_run"] == ["supervisor", "explain"]
    assert state["status"] == "ANSWERED"
    assert "bronze_key_completeness" in state["answer"]


# ---- guardrails ------------------------------------------------------------------------
def test_destructive_request_is_escalated(cfg):
    runner = FakeRunner([])
    state = make_system(cfg, runner=runner).run("Drop the silver table and delete quarantine")
    assert state["status"] == "ESCALATED" and state["escalated_to_human"]
    assert runner.submits == 0


def test_large_backfill_requires_approval(cfg):
    runner = FakeRunner([])
    state = make_system(cfg, runner=runner).run("Backfill", from_date="2024-01-01", to_date="2024-06-30")
    assert state["status"] == "ESCALATED" and runner.submits == 0
    approved = make_system(cfg, runner=FakeRunner([]), approve=True).run(
        "Backfill", from_date="2024-01-01", to_date="2024-06-30")
    assert approved["nodes_run"][-1] == "analytics"


def test_tool_allowlist_blocks_unapproved_tools(cfg):
    system = make_system(cfg)
    with pytest.raises(ToolNotAllowedError):
        system.toolkit.call("explain", "run_etl_job", job="bronze_silver")
    assert system.toolkit.calls[-1]["status"] == "DENIED"
    assert "run_etl_job" not in AGENT_TOOL_ALLOWLIST["explain"]


def test_every_tool_call_is_logged(cfg):
    state = make_system(cfg).run("Run the pipeline")
    saved = json.loads((Path(state["report_paths"]["json"]).parent / "state.json").read_text())
    tools = [c["tool"] for c in saved["tool_calls"]]
    assert {"run_etl_job", "get_run_status", "run_quality_checks", "build_gold_tables"} <= set(tools)
    assert all("duration_ms" in c for c in saved["tool_calls"])


def test_report_store_rejects_paths_outside_reports(cfg):
    store = ReportStore(cfg)
    for bad in ("../silver/x.json", "/etc/passwd", "C:/x.txt"):
        with pytest.raises(PathNotAllowedError):
            store.write_text(bad, "x")


def test_masking_hides_ids():
    masked = mask_sensitive({"note": "booking CNR1234567 by CID7654321", "customer_id": "CID1",
                             "rows": [{"booking_id": "CNR5884300"}]})
    assert "CNR1234567" not in masked["note"] and "CID7654321" not in masked["note"]
    assert masked["customer_id"] is None
    assert masked["rows"][0]["booking_id"] == "CNR***300"


# ---- LLM combination -------------------------------------------------------------------
class LenientLLM(LLMClient):
    """Simulates a model that tries to downgrade everything to 'info'."""

    name = "lenient"

    def complete_json(self, task, system, payload):
        self.usage["calls"] += 1
        self.usage["total_tokens"] += 100
        if task == "classify":
            return {"findings": [{"rule": f["rule"], "severity": "info", "explanation": "looks fine",
                                  "recommended_action": "none"} for f in payload["findings"]]}
        return OfflineLLM().complete_json(task, system, payload)


def test_model_cannot_override_hard_rule(cfg):
    ops = FakeOps(bronze=[_check("bronze_value_ranges", "FAIL", metric=0.03)])
    state = make_system(cfg, ops=ops, llm=LenientLLM()).run("Run the pipeline")
    assert state["status"] == "FAIL" and state["nodes_run"][-1] == "quality"
    assert state["findings"][0]["severity"] == "critical"


def test_merge_soft_fail_never_below_warning():
    merged = _merge_findings([_check("x", "FAIL", hard=False)], [{"rule": "x", "severity": "info"}])
    assert merged[0]["severity"] == "warning"


def test_llm_failure_falls_back_to_rules(cfg):
    class BrokenLLM(LLMClient):
        name = "broken"

        def complete_json(self, task, system, payload):
            raise RuntimeError("endpoint down")

    llm = BrokenLLM()
    state = make_system(cfg, llm=llm).run("Run the pipeline")
    assert state["status"] == "PASS"
    assert llm.usage["fallbacks"] > 0


def test_graph_structure():
    system = AgentSystem(Config(), llm=OfflineLLM(), runner=object(), ops=object())
    mermaid = system.app.get_graph().draw_mermaid()
    for node in ("supervisor", "etl", "quality", "analytics", "explain"):
        assert node in mermaid
    for label in ("run_etl", "success", "failure", "pass", "fail"):
        assert label in mermaid
