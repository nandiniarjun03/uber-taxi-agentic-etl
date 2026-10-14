"""Fault-injection evaluation of the agentic system (Section 7 of the brief).

For every scenario a fresh local lakehouse is created, a clean baseline load is run (so the
volume check has a previous load), the fault is injected into the landing zone and the full
agent workflow is executed. Healthy scenarios measure the false-alarm rate.

    python scripts/evaluate_agents.py --rows 3000 --healthy-runs 3

Writes docs/evaluation/evaluation_results.json and docs/evaluation/evaluation_report.md.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import statistics
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import etl_pipeline, gold_analytics  # noqa: E402
from src.agents import AgentSystem, make_llm  # noqa: E402
from src.config import get_config  # noqa: E402
from src.fault_injection import FAULTS, apply_fault_to_landing_zone  # noqa: E402
from src.runners import LocalJobRunner, ReportStore, SparkPipelineOps  # noqa: E402
from src.spark_utils import get_spark  # noqa: E402
from src.synthetic_data import generate_rows, write_csv  # noqa: E402


def run_scenario(spark, base_cfg, workdir: Path, name: str, fault: str | None, rows: int, seed: int) -> dict:
    root = workdir / name
    shutil.rmtree(root, ignore_errors=True)
    cfg = base_cfg.with_local_root(root)
    cfg = replace(cfg, agent=replace(cfg.agent, job_poll_seconds=0.2, job_max_retries=1))
    raw_file = Path(cfg.raw_path) / "ncr_ride_bookings.csv"
    write_csv(generate_rows(rows, seed=seed), raw_file)

    # Baseline load: gives the volume check a previous load and the gold check a KPI history.
    etl_pipeline.run_bronze_to_silver(spark, cfg)
    gold_analytics.build_gold_tables(spark, cfg)

    spec = FAULTS.get(fault) if fault else None
    if fault:
        apply_fault_to_landing_zone(raw_file.parent, fault, seed=seed)

    system = AgentSystem(
        cfg, llm=make_llm(cfg), ops=SparkPipelineOps(cfg, spark), store=ReportStore(cfg),
        runner=LocalJobRunner(cfg, spark=spark, simulate_failure=bool(spec and spec.runner_failure)),
    )
    t0 = time.time()
    state = system.run("Run the full ETL pipeline and validate every layer")
    elapsed = time.time() - t0

    findings = state.get("findings", [])
    critical = [f["rule"] for f in findings if f["severity"] == "critical"]
    warnings = [f["rule"] for f in findings if f["severity"] == "warning"]
    expected_status = spec.expected_status if spec else "PASS"
    expected_stop = spec.expected_stop_node if spec else "analytics"
    stop_node = state["nodes_run"][-1]
    explained = (not spec) or any(
        f["rule"] in spec.expected_rules and f.get("explanation") and f.get("recommended_action") for f in findings)
    return {
        "scenario": name,
        "fault": fault or "none",
        "description": spec.description if spec else "Clean data (healthy run)",
        "expected_status": expected_status,
        "actual_status": state.get("status"),
        "expected_stop_node": expected_stop,
        "stop_node": stop_node,
        "nodes_run": state["nodes_run"],
        "detected": (state.get("status") != "PASS") if spec else None,
        "false_alarm": (state.get("status") != "PASS") if not spec else None,
        "status_correct": state.get("status") == expected_status,
        "stopped_at_right_node": stop_node == expected_stop,
        "explained_correctly": bool(explained),
        "recovered": stop_node == expected_stop and bool(explained) and state.get("status") == expected_status,
        "critical_rules": critical,
        "warning_rules": warnings,
        "expected_rules": spec.expected_rules if spec else [],
        "etl_attempts": state.get("etl_attempts"),
        "agent_seconds": round(elapsed, 2),
        "llm_client": system.llm.name,
        "tokens": system.llm.usage.get("total_tokens", 0),
        "llm_calls": system.llm.usage.get("calls", 0),
        "report": state.get("report_paths", {}).get("markdown"),
        "summary": state.get("summary"),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=3000)
    p.add_argument("--healthy-runs", type=int, default=3)
    p.add_argument("--faults", nargs="*", default=list(FAULTS))
    p.add_argument("--workdir", default=str(ROOT / "lakehouse_eval"))
    p.add_argument("--out", default=str(ROOT / "docs" / "evaluation"))
    a = p.parse_args()
    logging.basicConfig(level=logging.WARNING)

    cfg = get_config()
    spark = get_spark(cfg=cfg)
    workdir = Path(a.workdir)
    results = []
    for i in range(a.healthy_runs):
        print(f"[healthy {i + 1}/{a.healthy_runs}]", flush=True)
        results.append(run_scenario(spark, cfg, workdir, f"healthy_{i + 1}", None, a.rows, seed=100 + i))
    for fault in a.faults:
        print(f"[fault] {fault}", flush=True)
        results.append(run_scenario(spark, cfg, workdir, fault, fault, a.rows, seed=7))

    faulty = [r for r in results if r["fault"] != "none"]
    healthy = [r for r in results if r["fault"] == "none"]
    manual_min = cfg.agent.manual_validation_minutes
    agent_s = [r["agent_seconds"] for r in results]
    metrics = {
        "detection_rate": round(sum(r["detected"] for r in faulty) / len(faulty), 3) if faulty else None,
        "false_alarm_rate": round(sum(r["false_alarm"] for r in healthy) / len(healthy), 3) if healthy else None,
        "recovery_rate": round(sum(r["recovered"] for r in faulty) / len(faulty), 3) if faulty else None,
        "status_accuracy": round(sum(r["status_correct"] for r in results) / len(results), 3),
        "median_agent_seconds": round(statistics.median(agent_s), 2),
        "manual_validation_minutes_baseline": manual_min,
        "time_saved_per_run_minutes": round(manual_min - statistics.median(agent_s) / 60, 1),
        "total_tokens": sum(r["tokens"] for r in results),
        "avg_tokens_per_run": round(sum(r["tokens"] for r in results) / len(results), 1),
        "llm_client": results[0]["llm_client"] if results else None,
        "rows_per_scenario": a.rows,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "evaluation_results.json").write_text(json.dumps({"metrics": metrics, "scenarios": results},
                                                             indent=2, default=str), encoding="utf-8")
    md = ["# Agentic ETL - fault-injection evaluation", "",
          f"Generated {metrics['generated_at']} · {a.rows:,} synthetic rows per scenario · "
          f"LLM client: `{metrics['llm_client']}`", "", "## Metrics", "", "| Metric | Value |", "|---|---|"]
    md += [f"| {k.replace('_', ' ')} | {v} |" for k, v in metrics.items() if k != "generated_at"]
    md += ["", "## Scenarios", "",
           "| Scenario | Expected | Actual | Stop node (expected) | Rules flagged | Explained | Recovered | Agent s |",
           "|---|---|---|---|---|---|---|---|"]
    for r in results:
        md.append(f"| {r['scenario']} | {r['expected_status']} | {r['actual_status']} | {r['stop_node']} "
                  f"({r['expected_stop_node']}) | {', '.join(r['critical_rules'] + r['warning_rules']) or '-'} | "
                  f"{'yes' if r['explained_correctly'] else 'no'} | "
                  f"{'-' if r['fault'] == 'none' else ('yes' if r['recovered'] else 'no')} | {r['agent_seconds']} |")
    (out / "evaluation_report.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
