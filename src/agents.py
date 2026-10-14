"""Multi-agent system that runs, validates and reports on the ETL pipeline.

Graph (LangGraph ``StateGraph``)::

    __start__ --> supervisor
    supervisor --run_etl--> etl | --quality--> quality | --analytics--> analytics | --explain--> explain
    etl       --success--> quality   | --failure--> __end__
    quality   --pass-----> analytics | --fail-----> __end__
    analytics --> __end__
    explain   --> __end__

Components in this module:

* ``FoundryLLM``   - the single client for the GPT deployment on Microsoft Foundry
                     (Entra ID or API key, JSON-mode outputs, token budget, retries).
* ``OfflineLLM``   - deterministic stand-in with the same interface, used when no Foundry
                     endpoint is configured (CI, offline demos) or the token budget is spent.
* ``PipelineToolkit`` - typed tools (run_etl_job, get_run_status, run_quality_checks,
                     get_table_stats, compare_row_counts, build_gold_tables, write_report,
                     read_run_history) behind guardrails: per-agent allowlists, audit log
                     of every call, masked arguments, path restrictions.
* ``AgentSystem``  - the supervisor and the four worker nodes, decision logic that
                     combines deterministic rules with the model's judgement, and the run
                     report writer.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any, Callable, Literal, Optional, TypedDict

from langchain_core.tools import StructuredTool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field

from src.config import Config, get_config
from src.runners import JobRunner, ReportStore, SparkPipelineOps, make_runner

log = logging.getLogger("agents")

ROUTES = ("run_etl", "quality", "analytics", "explain")
SEVERITIES = ("critical", "warning", "info")
DESTRUCTIVE_WORDS = ("delete", "drop", "truncate", "purge", "wipe", "vacuum", "remove all")

# --------------------------------------------------------------------------------------
# Prompts (structured JSON outputs so the orchestrator can act on them reliably)
# --------------------------------------------------------------------------------------
SYSTEM_SUPERVISOR = """You are the supervisor of an agentic ETL platform for Uber ride-booking data \
(Bronze/Silver/Gold medallion layers on Azure Databricks with ADLS Gen2).
Classify the user's request into exactly one route:
- "run_etl": run the pipeline (ingest/load/refresh/process data), optionally for a date window
- "quality": validate existing Bronze/Silver data without re-running ingestion
- "analytics": (re)build or validate the Gold analytics tables only
- "explain": a question about past runs, data, failures or results (read-only)
Also extract an optional booking-date window and flag destructive intent (delete/drop/truncate data).
Respond with JSON only:
{"route": "run_etl|quality|analytics|explain", "from_date": "YYYY-MM-DD"|null, "to_date": "YYYY-MM-DD"|null,
 "destructive": true|false, "reason": "<one sentence>"}"""

SYSTEM_CLASSIFY = """You are a senior data-quality engineer reviewing automated check results for an \
Uber ride-booking ETL pipeline. You receive structured check results (never raw data).
For each finding:
1. severity: "critical" (data must not be published), "warning" (publish but investigate) or "info".
   A finding marked hard_rule_failed=true is ALWAYS critical - you may never downgrade it.
   You may escalate a warning to critical if the evidence justifies it.
2. explanation: 1-2 sentences for a data engineer, quoting the metric and threshold, naming the
   most likely cause (upstream export, schema drift, duplicate delivery, late data, bad joins ...).
3. recommended_action: one concrete next step.
Use only numbers present in the input. Respond with JSON only:
{"findings": [{"rule": str, "severity": "critical|warning|info", "explanation": str, "recommended_action": str}]}"""

SYSTEM_JOB_FAILURE = """You are an on-call data engineer. A Databricks ETL job for Uber ride-booking data \
failed. Using only the error type, message and trace tail provided, explain the most likely root cause in \
plain language (2 sentences max) and give one recommended action. Say whether retrying could help.
Respond with JSON only: {"explanation": str, "recommended_action": str, "retry_may_help": true|false}"""

SYSTEM_SUMMARY = """You write the executive summary of an automated ETL validation run for Uber \
ride-booking data. Given the structured run result, write 3-5 sentences: overall verdict, row counts, \
key findings and what (if anything) a human must do. Respond with JSON only: {"summary": str}"""

SYSTEM_ANSWER = """You answer questions about an Uber ride-booking ETL platform using ONLY the run \
history, latest report and table statistics provided. If the answer is not in the context, say so. \
Be concise and specific (numbers, rule names, dates). Respond with JSON only: {"answer": str}"""

# Deterministic remediation hints (used by the offline client and as fallback text)
REMEDIATION = {
    "MissingSourceError": "Confirm the upstream export ran and landed CSV files in raw/uber_rides/, then re-trigger the job.",
    "SourceSchemaError": "Compare the new header with the 21-column contract; update the mapping in config.py only after the producer confirms the change.",
    "SimulatedJobFailure": "Check the failed task's driver/executor logs; increase cluster memory or repartition, then retry.",
    "job_failure": "Check the failed task's driver/executor logs; increase cluster memory or repartition, then retry.",
    "bronze_key_completeness": "Find the batch/source file with the null spike and ask the producer to re-export it; do not publish until resolved.",
    "bronze_unique_booking_id": "Check whether the same file was delivered twice or overlapping extracts were landed; de-duplicate at source and re-ingest.",
    "bronze_value_ranges": "Inspect quarantined rows for negative fares or ratings above 5 and raise a fix with the source system.",
    "bronze_allowed_domains": "Review new status / vehicle / payment values; extend the allowed lists in config.py only if they are legitimate.",
    "bronze_status_flag_consistency": "Inspect bookings whose cancellation/incomplete flags contradict status; likely an upstream join or mapping bug.",
    "bronze_reason_flag_pairing": "Check the export logic that populates reason columns together with their flags.",
    "bronze_completed_outcome_completeness": "Completed bookings must carry fare and distance; check the fare/trip join upstream.",
    "bronze_timeliness": "Identify the missing dates and backfill them with an incremental load once the source delivers them.",
    "bronze_volume_vs_previous_load": "Compare file sizes with the previous delivery; a partial export is likely - re-request the full extract.",
    "completed_rides_missing_payment_method": "Check the source export for the affected dates and re-ingest if the gap is upstream.",
    "silver_quarantine_rate": "Review quarantine/ by primary_reject_reason to find the dominant failure and fix it at source.",
    "bronze_to_silver_reconciliation": "Rows were lost between layers; re-run Silver and inspect the job logs.",
    "gold_daily_totals_reconcile": "Rebuild Gold from the current Silver table; check for concurrent writes.",
    "gold_revenue_reconcile": "Rebuild Gold; verify booking_value typing in Silver.",
    "gold_no_missing_dates": "Backfill the missing dates through an incremental load.",
    "gold_completion_rate_plausible": "Compare the completion rate with recent runs; confirm whether the shift is a real business change.",
}


# --------------------------------------------------------------------------------------
# Sensitive data masking (applied to everything sent to the model and to the audit log)
# --------------------------------------------------------------------------------------
_ID_PATTERNS = [(re.compile(r"\b(CNR)(\d{3,})(\d{3})\b"), r"\1***\3"), (re.compile(r"\b(CID)\d+\b"), r"\1***")]


def mask_sensitive(obj: Any) -> Any:
    """Mask Booking IDs (CNR***123) and Customer IDs (CID***) recursively."""
    if isinstance(obj, str):
        for pattern, repl in _ID_PATTERNS:
            obj = pattern.sub(repl, obj)
        return obj
    if isinstance(obj, dict):
        return {k: (None if k == "customer_id" else mask_sensitive(v)) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [mask_sensitive(v) for v in obj]
    return obj


# --------------------------------------------------------------------------------------
# LLM clients
# --------------------------------------------------------------------------------------
class TokenBudgetExceeded(RuntimeError):
    pass


class LLMClient:
    """Common interface: ``complete_json(task, system, payload) -> dict``."""

    name = "base"

    def __init__(self, max_tokens_per_run: int = 12000):
        self.max_tokens_per_run = max_tokens_per_run
        self.reset_usage()

    def reset_usage(self) -> None:
        self.usage = {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0,
                      "latency_s": 0.0, "fallbacks": 0}

    def complete_json(self, task: str, system: str, payload: dict) -> dict:
        raise NotImplementedError


def _fmt_metric(f: dict) -> tuple[str, str]:
    """Human-readable metric and threshold (rates as percentages)."""
    m, thr = f.get("metric_value"), f.get("threshold") or {}
    if isinstance(m, (int, float)) and isinstance(thr, dict) and "warn" in thr:
        return f"{m:.2%}", f"warn > {thr['warn']:.2%}, fail > {thr['fail']:.2%}"
    return str(m), json.dumps(thr)


class OfflineLLM(LLMClient):
    """Deterministic reasoning that mirrors the prompts. Keeps the system fully functional
    (and testable) without a Foundry deployment."""

    name = "offline-rules"

    def complete_json(self, task: str, system: str, payload: dict) -> dict:
        self.usage["calls"] += 1
        return getattr(self, f"_{task}")(payload)

    @staticmethod
    def _route(p: dict) -> dict:
        text = p["request"].lower()
        dates = re.findall(r"\d{4}-\d{2}-\d{2}", text)
        destructive = any(w in text for w in DESTRUCTIVE_WORDS)
        if destructive:
            route = "explain"
        elif re.search(r"\b(why|what|which|how|when|explain|show|tell|summari[sz]e|did)\b", text) or text.endswith("?"):
            route = "explain"
        elif re.search(r"\b(gold|analytics|aggregate|kpi)", text) and not re.search(r"\b(run|ingest|load)\b", text):
            route = "analytics"
        elif re.search(r"\b(validat|quality|check|verify)", text) and not re.search(r"\b(run|ingest|load|refresh)\b", text):
            route = "quality"
        else:
            route = "run_etl"
        return {"route": route, "from_date": dates[0] if dates else None,
                "to_date": dates[1] if len(dates) > 1 else (dates[0] if dates else None),
                "destructive": destructive, "reason": f"keyword routing -> {route}"}

    @staticmethod
    def _classify(p: dict) -> dict:
        out = []
        for f in p["findings"]:
            sev = "critical" if f["status"] == "FAIL" else "warning"
            pct, thr = _fmt_metric(f)
            detail = f" Details: {json.dumps(f.get('details'))[:200]}." if f.get("details") else ""
            out.append({
                "rule": f["rule"], "severity": sev,
                "explanation": f"{f['description']}: observed {pct} vs threshold {thr} "
                               f"({f.get('failing_count', 0)} of {f.get('total_count', 0)} records).{detail}",
                "recommended_action": REMEDIATION.get(f["rule"], "Inspect the sample failing records and the source batch."),
            })
        return {"findings": out}

    @staticmethod
    def _job_failure(p: dict) -> dict:
        et = p.get("error_type") or "job_failure"
        return {"explanation": f"The job failed with {et}: {p.get('message', '')[:300]}",
                "recommended_action": REMEDIATION.get(et, REMEDIATION["job_failure"]),
                "retry_may_help": et not in ("MissingSourceError", "SourceSchemaError")}

    @staticmethod
    def _summary(p: dict) -> dict:
        crit = [f["rule"] for f in p.get("findings", []) if f["severity"] == "critical"]
        warn = [f["rule"] for f in p.get("findings", []) if f["severity"] == "warning"]
        s = (f"Run {p['run_id']} finished with status {p['status']} after nodes {', '.join(p['nodes_run'])}. "
             f"Row counts: {p.get('row_counts')}. ")
        if crit:
            s += f"Critical findings: {', '.join(crit)}; the run was stopped and data was not published. "
        if warn:
            s += f"Warnings to investigate: {', '.join(warn)}. "
        if not crit and not warn:
            s += "All checks passed. "
        s += "Human action required." if p.get("escalated_to_human") or crit else "No human action required."
        return {"summary": s}

    @staticmethod
    def _answer(p: dict) -> dict:
        hist = p.get("run_history") or []
        if not hist:
            return {"answer": "There is no run history yet - trigger a pipeline run first."}
        last = hist[-1]
        failed = [h for h in hist if h.get("status") == "FAIL"]
        ans = (f"The latest run {last['run_id']} ({last.get('route')}) ended with status {last['status']}; "
               f"row counts {last.get('row_counts')}.")
        if last.get("failed_rules"):
            ans += f" It failed on: {', '.join(last['failed_rules'])}."
        ans += f" {len(failed)} of the last {len(hist)} runs failed."
        return {"answer": ans}


class FoundryLLM(LLMClient):
    """Single client for the GPT deployment on Microsoft Foundry (Azure OpenAI endpoint).

    Auth: Microsoft Entra ID via ``DefaultAzureCredential`` (preferred - managed identity,
    service principal or az login) or an API key from the secret scope.
    """

    name = "foundry"

    def __init__(self, cfg: Config):
        super().__init__(cfg.agent.max_tokens_per_run)
        from openai import AzureOpenAI

        a = cfg.agent
        self.deployment = a.foundry_deployment
        self.max_tokens = a.max_tokens_per_call
        self.temperature = a.llm_temperature
        self.retries = a.llm_max_retries
        if a.foundry_auth == "key":
            from src.config import get_secret

            self.client = AzureOpenAI(azure_endpoint=a.foundry_endpoint, api_version=a.foundry_api_version,
                                      api_key=get_secret("azure-openai-api-key"))
        else:
            from azure.identity import DefaultAzureCredential, get_bearer_token_provider

            provider = get_bearer_token_provider(DefaultAzureCredential(),
                                                 "https://cognitiveservices.azure.com/.default")
            self.client = AzureOpenAI(azure_endpoint=a.foundry_endpoint, api_version=a.foundry_api_version,
                                      azure_ad_token_provider=provider)

    def complete_json(self, task: str, system: str, payload: dict) -> dict:
        if self.usage["total_tokens"] >= self.max_tokens_per_run:
            raise TokenBudgetExceeded(f"Token budget of {self.max_tokens_per_run} reached")
        user = json.dumps(mask_sensitive(payload), default=str)[:12000]
        last_exc: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            t0 = time.time()
            try:
                resp = self.client.chat.completions.create(
                    model=self.deployment,
                    messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
                    response_format={"type": "json_object"},
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                )
                self.usage["calls"] += 1
                self.usage["latency_s"] += time.time() - t0
                if resp.usage:
                    self.usage["prompt_tokens"] += resp.usage.prompt_tokens
                    self.usage["completion_tokens"] += resp.usage.completion_tokens
                    self.usage["total_tokens"] += resp.usage.total_tokens
                return json.loads(resp.choices[0].message.content or "{}")
            except Exception as exc:  # network, throttling, invalid JSON
                last_exc = exc
                time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"Foundry call failed after {self.retries + 1} attempts: {last_exc}")


def make_llm(cfg: Config) -> LLMClient:
    mode = cfg.agent.llm_mode
    if mode == "foundry" or (mode == "auto" and cfg.agent.foundry_endpoint):
        return FoundryLLM(cfg)
    return OfflineLLM(cfg.agent.max_tokens_per_run)


# --------------------------------------------------------------------------------------
# Tools (typed inputs) + guardrails
# --------------------------------------------------------------------------------------
class RunEtlJobInput(BaseModel):
    job: Literal["bronze_silver", "gold"] = Field("bronze_silver", description="Which Databricks job to trigger")
    from_date: Optional[str] = Field(None, description="Start of booking-date window (YYYY-MM-DD), None = full load")
    to_date: Optional[str] = Field(None, description="End of booking-date window (YYYY-MM-DD)")


class RunIdInput(BaseModel):
    run_id: str = Field(..., description="Run id returned by run_etl_job")


class LayerInput(BaseModel):
    layer: Literal["bronze", "silver", "gold"] = Field(..., description="Medallion layer to validate")


class TableStatsInput(BaseModel):
    layer: Literal["bronze", "silver", "quarantine", "gold"]
    table: Optional[str] = Field(None, description="Gold table name (gold layer only)")


class NoInput(BaseModel):
    pass


class WriteReportInput(BaseModel):
    path: str = Field(..., description="Path relative to the reports/ container, e.g. runs/<id>/report.md")
    content: str = Field(..., description="Report body (markdown or JSON)")


class HistoryInput(BaseModel):
    limit: int = Field(10, ge=1, le=50)


class ToolNotAllowedError(PermissionError):
    pass


# Agents may only call the tools listed for them (least privilege).
AGENT_TOOL_ALLOWLIST: dict[str, set[str]] = {
    "supervisor": {"read_run_history"},
    "etl": {"run_etl_job", "get_run_status", "cancel_run", "compare_row_counts", "write_report"},
    "quality": {"run_quality_checks", "get_table_stats", "compare_row_counts", "read_run_history", "write_report"},
    "analytics": {"build_gold_tables", "run_quality_checks", "get_table_stats", "compare_row_counts",
                  "gold_kpis", "read_run_history", "write_report"},
    "explain": {"read_run_history", "read_report", "get_table_stats", "write_report"},  # read-only on data
}


class PipelineToolkit:
    """Wraps pipeline operations as typed LangChain tools and enforces the guardrails."""

    def __init__(self, cfg: Config, runner: JobRunner, ops: Any, store: ReportStore):
        self.cfg, self.runner, self.ops, self.store = cfg, runner, ops, store
        self.calls: list[dict] = []
        self.history_supplier: Callable[[], list[dict]] = lambda: []
        self.tools: dict[str, StructuredTool] = {t.name: t for t in self._build()}

    def _build(self) -> list[StructuredTool]:
        ops, runner, store = self.ops, self.runner, self.store

        def run_etl_job(job: str = "bronze_silver", from_date: Optional[str] = None,
                        to_date: Optional[str] = None) -> dict:
            return {"run_id": runner.submit(job, {"from_date": from_date, "to_date": to_date})}

        def get_run_status(run_id: str) -> dict:
            return runner.get_status(run_id).to_dict()

        def cancel_run(run_id: str) -> dict:
            runner.cancel(run_id)
            return {"cancelled": run_id}

        def run_quality_checks(layer: str) -> list[dict]:
            return ops.run_quality_checks(layer, history=self.history_supplier() if layer == "gold" else None)

        def get_table_stats(layer: str, table: Optional[str] = None) -> dict:
            return ops.table_stats(layer, table)

        def compare_row_counts() -> dict:
            c = ops.row_counts()
            b, s, q = c.get("bronze") or 0, c.get("silver") or 0, c.get("quarantine") or 0
            return {**c, "reconciled": b == s + q, "difference": b - s - q,
                    "quarantine_rate": round(q / b, 6) if b else None}

        def build_gold_tables() -> dict:
            return ops.build_gold()

        def gold_kpis() -> dict:
            return ops.gold_kpis()

        def write_report(path: str, content: str) -> dict:
            return {"path": store.write_text(path, content)}

        def read_report(path: str) -> dict:
            return {"path": path, "content": (store.read_text(path) or "")[:8000]}

        def read_run_history(limit: int = 10) -> list[dict]:
            return store.read_jsonl("run_history.jsonl", limit)

        class ReadReportInput(BaseModel):
            path: str

        spec = [
            (run_etl_job, RunEtlJobInput, "Trigger the Bronze->Silver (or Gold) Databricks job; returns run_id."),
            (get_run_status, RunIdInput, "Get state, result, error and output metrics of a job run."),
            (cancel_run, RunIdInput, "Cancel a running job (used on timeout)."),
            (run_quality_checks, LayerInput, "Run the data-quality rules for one layer; returns structured results."),
            (get_table_stats, TableStatsInput, "Row count, column count and booking-date range of a table."),
            (compare_row_counts, NoInput, "Bronze vs Silver + Quarantine row counts and reconciliation."),
            (build_gold_tables, NoInput, "Rebuild all Gold aggregate tables from Silver."),
            (gold_kpis, NoInput, "Headline KPIs (bookings, completion rate, revenue) from Gold."),
            (write_report, WriteReportInput, "Write a run report under reports/ (only allowed location)."),
            (read_report, ReadReportInput, "Read a previously written report from reports/."),
            (read_run_history, HistoryInput, "Summaries of the most recent agent runs."),
        ]
        return [StructuredTool.from_function(func=f, name=f.__name__, description=d, args_schema=s)
                for f, s, d in spec]

    def call(self, agent: str, tool: str, **kwargs: Any) -> Any:
        """Invoke a tool on behalf of an agent: allowlist check, timing and audit logging."""
        entry = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "agent": agent, "tool": tool,
                 "args": mask_sensitive(kwargs if tool != "write_report" else {"path": kwargs.get("path")})}
        if tool not in AGENT_TOOL_ALLOWLIST.get(agent, set()):
            entry.update(status="DENIED", error="tool not in agent allowlist")
            self.calls.append(entry)
            raise ToolNotAllowedError(f"Agent '{agent}' may not call '{tool}'")
        t0 = time.time()
        try:
            result = self.tools[tool].invoke(kwargs)
            entry.update(status="OK")
            return result
        except Exception as exc:
            entry.update(status="ERROR", error=f"{type(exc).__name__}: {str(exc)[:300]}")
            raise
        finally:
            entry["duration_ms"] = int((time.time() - t0) * 1000)
            self.calls.append(entry)
            log.info("tool_call %s", json.dumps(entry, default=str))


# --------------------------------------------------------------------------------------
# Shared state
# --------------------------------------------------------------------------------------
class PipelineState(TypedDict, total=False):
    run_id: str
    request: str
    route: str
    route_reason: str
    from_date: Optional[str]
    to_date: Optional[str]
    nodes_run: list[str]
    etl_status: str  # SUCCESS | FAILED
    etl_run: dict
    etl_attempts: int
    quality_decision: str  # pass | fail
    check_results: list[dict]
    findings: list[dict]
    row_counts: dict
    gold: dict
    kpis: dict
    status: str  # PASS | PASS_WITH_WARNINGS | FAIL | ESCALATED | ANSWERED
    answer: str
    summary: str
    escalated_to_human: bool
    escalation_reason: str
    errors: list[str]
    report_paths: dict
    started_at: float


def _merge_findings(checks: list[dict], llm_findings: list[dict]) -> list[dict]:
    """Combine deterministic results with the model's judgement.

    Hard-rule failures are locked to ``critical`` - the model can explain them but never
    downgrade them. For soft rules the model may pick any severity, but a FAIL stays at
    least ``warning``.
    """
    by_rule = {f.get("rule"): f for f in llm_findings if isinstance(f, dict)}
    fallback = {f["rule"]: f for f in OfflineLLM._classify({"findings": checks})["findings"]}
    merged = []
    for c in checks:
        m = by_rule.get(c["rule"]) or fallback[c["rule"]]
        sev = m.get("severity") if m.get("severity") in SEVERITIES else fallback[c["rule"]]["severity"]
        if c["status"] == "FAIL" and c["hard"]:
            sev = "critical"
        elif c["status"] == "FAIL" and sev == "info":
            sev = "warning"
        merged.append({
            "rule": c["rule"], "layer": c["layer"], "severity": sev, "check_status": c["status"],
            "hard_rule": c["hard"], "metric_value": c["metric_value"], "threshold": c["threshold"],
            "explanation": m.get("explanation") or fallback[c["rule"]]["explanation"],
            "recommended_action": m.get("recommended_action") or fallback[c["rule"]]["recommended_action"],
            "sample_failures": mask_sensitive(c.get("sample_failures", [])[:3]),
        })
    return merged


def _window_days(from_date: Optional[str], to_date: Optional[str]) -> int:
    if not from_date:
        return 0
    end = date.fromisoformat(to_date) if to_date else date.today()
    return (end - date.fromisoformat(from_date)).days + 1


# --------------------------------------------------------------------------------------
# The agent system
# --------------------------------------------------------------------------------------
class AgentSystem:
    def __init__(self, cfg: Optional[Config] = None, llm: Optional[LLMClient] = None,
                 runner: Optional[JobRunner] = None, ops: Any = None, store: Optional[ReportStore] = None,
                 approval_callback: Optional[Callable[[str, dict], bool]] = None):
        self.cfg = cfg or get_config()
        self.llm = llm or make_llm(self.cfg)
        self.fallback_llm = OfflineLLM()
        self.store = store or ReportStore(self.cfg)
        self.ops = ops or SparkPipelineOps(self.cfg)
        self.runner = runner or make_runner(self.cfg, spark=getattr(self.ops, "_spark", None))
        self.toolkit = PipelineToolkit(self.cfg, self.runner, self.ops, self.store)
        self.toolkit.history_supplier = self._kpi_history
        self.approval_callback = approval_callback or (lambda action, details: self.cfg.agent.auto_approve)
        self.checkpointer = MemorySaver()
        self.app = self.build_graph()

    # ---- helpers --------------------------------------------------------------------
    def _llm(self, task: str, system: str, payload: dict) -> dict:
        """Call the model; on any failure (or exhausted budget) fall back to deterministic logic."""
        try:
            return self.llm.complete_json(task, system, payload)
        except Exception as exc:
            log.warning("LLM %s failed (%s) - using deterministic fallback", task, exc)
            self.llm.usage["fallbacks"] = self.llm.usage.get("fallbacks", 0) + 1
            return self.fallback_llm.complete_json(task, system, payload)

    def _tool(self, agent: str, tool: str, **kwargs: Any) -> Any:
        return self.toolkit.call(agent, tool, **kwargs)

    def _kpi_history(self) -> list[dict]:
        hist = self.store.read_jsonl("run_history.jsonl", 20)
        return [{"completion_rate": h.get("kpis", {}).get("completion_rate")} for h in hist
                if h.get("status") in ("PASS", "PASS_WITH_WARNINGS")]

    @staticmethod
    def _visit(state: PipelineState, node: str) -> list[str]:
        return list(state.get("nodes_run", [])) + [node]

    # ---- nodes ----------------------------------------------------------------------
    def supervisor(self, state: PipelineState) -> dict:
        decision = self._llm("route", SYSTEM_SUPERVISOR, {"request": state["request"]})
        offline = OfflineLLM._route({"request": state["request"]})
        route = decision.get("route") if decision.get("route") in ROUTES else offline["route"]
        destructive = bool(decision.get("destructive")) or offline["destructive"]  # deterministic guard wins
        update: dict = {
            "nodes_run": self._visit(state, "supervisor"),
            "route": route,
            "route_reason": decision.get("reason", ""),
            "from_date": state.get("from_date") or decision.get("from_date"),
            "to_date": state.get("to_date") or decision.get("to_date"),
        }
        if destructive and not self.approval_callback("destructive_request", {"request": state["request"]}):
            update.update(route="explain", escalated_to_human=True,
                          escalation_reason="Destructive request (delete/drop/truncate) requires human approval; "
                                            "no pipeline data was modified.")
        return update

    def etl(self, state: PipelineState) -> dict:
        update: dict = {"nodes_run": self._visit(state, "etl")}
        f, t = state.get("from_date"), state.get("to_date")
        if f and _window_days(f, t) > self.cfg.agent.backfill_limit_days:
            details = {"from_date": f, "to_date": t, "days": _window_days(f, t),
                       "limit": self.cfg.agent.backfill_limit_days}
            if not self.approval_callback("backfill", details):
                update.update(etl_status="FAILED", status="ESCALATED", escalated_to_human=True,
                              escalation_reason=f"Backfill of {details['days']} days exceeds the "
                                                f"{details['limit']}-day limit; human approval required.",
                              findings=[])
                return {**update, **self._finalize({**state, **update})}

        attempts, run = 0, {}
        max_attempts = 1 + self.cfg.agent.job_max_retries
        while attempts < max_attempts:
            attempts += 1
            run_id = self._tool("etl", "run_etl_job", job="bronze_silver", from_date=f, to_date=t)["run_id"]
            deadline = time.time() + self.cfg.agent.job_timeout_seconds
            while True:
                run = self._tool("etl", "get_run_status", run_id=run_id)
                if run["state"] in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                    break
                if time.time() > deadline:
                    self._tool("etl", "cancel_run", run_id=run_id)
                    run.update(result_state="TIMEDOUT", error_type="Timeout",
                               message=f"Job exceeded {self.cfg.agent.job_timeout_seconds}s")
                    break
                time.sleep(self.cfg.agent.job_poll_seconds)
            if run.get("result_state") == "SUCCESS" or not run.get("retryable", True):
                break
            log.warning("ETL attempt %s failed (%s) - retrying", attempts, run.get("error_type"))

        update.update(etl_run=run, etl_attempts=attempts)
        if run.get("result_state") == "SUCCESS":
            out = run.get("output") or {}
            s = out.get("silver", {})
            update.update(etl_status="SUCCESS", row_counts={
                "bronze": out.get("bronze", {}).get("rows_out"), "silver": s.get("rows_out"),
                "quarantine": s.get("rows_quarantined")})
            return update

        diag = self._llm("job_failure", SYSTEM_JOB_FAILURE, {
            "error_type": run.get("error_type"), "message": run.get("message"),
            "trace_tail": (run.get("output") or {}).get("trace_tail", "")[-800:], "attempts": attempts})
        error_rule = run.get("error_type") or "job_failure"
        update.update(etl_status="FAILED", status="FAIL", findings=[{
            "rule": "job_failure" if error_rule == "SimulatedJobFailure" else error_rule, "layer": "etl",
            "severity": "critical", "check_status": "FAIL", "hard_rule": True,
            "explanation": diag.get("explanation") or run.get("message"),
            "recommended_action": diag.get("recommended_action") or REMEDIATION["job_failure"],
            "retry_may_help": diag.get("retry_may_help"), "attempts": attempts,
        }])
        return {**update, **self._finalize({**state, **update})}

    def quality(self, state: PipelineState) -> dict:
        update: dict = {"nodes_run": self._visit(state, "quality")}
        checks: list[dict] = []
        for layer in ("bronze", "silver"):
            try:
                checks += self._tool("quality", "run_quality_checks", layer=layer)
            except Exception as exc:  # e.g. table missing on a validation-only run
                checks.append({"rule": f"{layer}_checks_executable", "layer": layer, "category": "completeness",
                               "status": "FAIL", "metric_value": 1.0, "threshold": {}, "hard": True,
                               "description": f"Quality checks could not run: {type(exc).__name__}: {exc}"[:300],
                               "failing_count": 0, "total_count": 0, "sample_failures": [], "details": {}})
        counts = self._tool("quality", "compare_row_counts")
        non_pass = [c for c in checks if c["status"] != "PASS"]
        llm_out = self._llm("classify", SYSTEM_CLASSIFY, {
            "findings": [{**c, "hard_rule_failed": c["status"] == "FAIL" and c["hard"],
                          "sample_failures": c.get("sample_failures", [])[:3]} for c in non_pass]}) if non_pass else {}
        findings = _merge_findings(non_pass, llm_out.get("findings", []))
        critical = [f for f in findings if f["severity"] == "critical"]
        update.update(check_results=checks, findings=findings,
                      row_counts={k: counts.get(k) for k in ("bronze", "silver", "quarantine")},
                      quality_decision="fail" if critical else "pass")
        if critical:
            update["status"] = "FAIL"
            return {**update, **self._finalize({**state, **update})}
        return update

    def analytics(self, state: PipelineState) -> dict:
        update: dict = {"nodes_run": self._visit(state, "analytics")}
        try:
            gold = self._tool("analytics", "build_gold_tables")
            checks = self._tool("analytics", "run_quality_checks", layer="gold")
            kpis = self._tool("analytics", "gold_kpis")
        except Exception as exc:
            gold, kpis = {"error": str(exc)[:300]}, {}
            checks = [{"rule": "gold_build", "layer": "gold", "category": "completeness", "status": "FAIL",
                       "metric_value": 1.0, "threshold": {}, "hard": True,
                       "description": f"Gold build failed: {type(exc).__name__}: {exc}"[:300],
                       "failing_count": 0, "total_count": 0, "sample_failures": [], "details": {}}]
        non_pass = [c for c in checks if c["status"] != "PASS"]
        llm_out = self._llm("classify", SYSTEM_CLASSIFY, {"findings": [
            {**c, "hard_rule_failed": c["status"] == "FAIL" and c["hard"]} for c in non_pass]}) if non_pass else {}
        findings = list(state.get("findings", [])) + _merge_findings(non_pass, llm_out.get("findings", []))
        if not state.get("row_counts"):
            counts = self._tool("analytics", "compare_row_counts")
            update["row_counts"] = {k: counts.get(k) for k in ("bronze", "silver", "quarantine")}
        sev = {f["severity"] for f in findings}
        status = "FAIL" if "critical" in sev else ("PASS_WITH_WARNINGS" if "warning" in sev else "PASS")
        update.update(gold=gold, kpis=kpis, findings=findings, status=status,
                      check_results=list(state.get("check_results", [])) + checks)
        return {**update, **self._finalize({**state, **update})}

    def explain(self, state: PipelineState) -> dict:
        update: dict = {"nodes_run": self._visit(state, "explain")}
        if state.get("escalated_to_human"):
            update.update(status="ESCALATED", answer=state.get("escalation_reason", ""))
            return {**update, **self._finalize({**state, **update})}
        history = self._tool("explain", "read_run_history", limit=10)
        latest = None
        if history:
            latest = self._tool("explain", "read_report", path=f"runs/{history[-1]['run_id']}/report.json")
        stats = {}
        for layer in ("bronze", "silver", "quarantine"):
            try:
                stats[layer] = self._tool("explain", "get_table_stats", layer=layer)
            except Exception as exc:
                stats[layer] = {"error": type(exc).__name__}
        ans = self._llm("answer", SYSTEM_ANSWER, {"question": state["request"], "run_history": history,
                                                  "latest_report": (latest or {}).get("content", "")[:4000],
                                                  "table_stats": stats})
        update.update(status="ANSWERED", answer=ans.get("answer", ""))
        return {**update, **self._finalize({**state, **update})}

    # ---- reporting ------------------------------------------------------------------
    def _finalize(self, state: PipelineState) -> dict:
        """Build the run result, ask the model for a summary and write JSON + markdown reports."""
        duration = round(time.time() - state.get("started_at", time.time()), 2)
        findings = state.get("findings", [])
        checks = state.get("check_results", [])
        result = {
            "run_id": state["run_id"],
            "request": state.get("request"),
            "route": state.get("route"),
            "nodes_run": state.get("nodes_run", []),
            "status": state.get("status", "UNKNOWN"),
            "row_counts": state.get("row_counts", {}),
            "checks_passed": sum(c["status"] == "PASS" for c in checks),
            "checks_warned": sum(c["status"] == "WARN" for c in checks),
            "checks_failed": sum(c["status"] == "FAIL" for c in checks),
            "findings": [{k: f.get(k) for k in ("rule", "layer", "severity", "explanation", "recommended_action")}
                         for f in findings if f["severity"] != "info"],
            "escalated_to_human": bool(state.get("escalated_to_human")) or state.get("status") == "FAIL",
            "kpis": state.get("kpis", {}),
            "etl_attempts": state.get("etl_attempts"),
            "duration_s": duration,
            "llm": {"client": self.llm.name, **self.llm.usage},
        }
        # Monitoring: flag duration drift against the history baseline.
        hist = self.store.read_jsonl("run_history.jsonl", 20)
        durations = sorted(h["duration_s"] for h in hist if h.get("route") == result["route"] and h.get("duration_s"))
        if len(durations) >= 3 and duration > 2 * durations[len(durations) // 2]:
            result["findings"].append({"rule": "run_duration_drift", "layer": "monitoring", "severity": "info",
                                       "explanation": f"Run took {duration}s vs median {durations[len(durations)//2]}s.",
                                       "recommended_action": "Check cluster size and input volume."})
        if state.get("answer"):
            result["answer"] = state["answer"]
            summary = state["answer"]
        else:
            summary = self._llm("summary", SYSTEM_SUMMARY, result).get("summary", "")
        result["summary"] = summary

        agent = state["nodes_run"][-1] if state.get("nodes_run") else "explain"
        base = f"runs/{state['run_id']}"
        json_path = self._tool(agent, "write_report", path=f"{base}/report.json",
                               content=json.dumps(result, indent=2, default=str))["path"]
        md_path = self._tool(agent, "write_report", path=f"{base}/report.md",
                             content=render_markdown(result, findings, checks, self.toolkit.calls))["path"]
        state_dump = {k: v for k, v in state.items() if k not in ("check_results",)}
        self._tool(agent, "write_report", path=f"{base}/state.json",
                   content=json.dumps({**state_dump, "tool_calls": self.toolkit.calls}, indent=2, default=str))
        self.store.append_line("run_history.jsonl", json.dumps({
            "run_id": result["run_id"], "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "route": result["route"], "status": result["status"], "nodes_run": result["nodes_run"],
            "row_counts": result["row_counts"], "kpis": result["kpis"], "duration_s": duration,
            "failed_rules": [f["rule"] for f in result["findings"] if f["severity"] == "critical"],
            "tokens": self.llm.usage.get("total_tokens", 0)}, default=str))
        return {"summary": summary, "report_paths": {"json": json_path, "markdown": md_path}}

    # ---- graph ----------------------------------------------------------------------
    def build_graph(self):
        g = StateGraph(PipelineState)
        g.add_node("supervisor", self.supervisor)
        g.add_node("etl", self.etl)
        g.add_node("quality", self.quality)
        g.add_node("analytics", self.analytics)
        g.add_node("explain", self.explain)
        g.add_edge(START, "supervisor")
        g.add_conditional_edges("supervisor", lambda s: s["route"],
                                {"run_etl": "etl", "quality": "quality", "analytics": "analytics",
                                 "explain": "explain"})
        g.add_conditional_edges("etl", lambda s: "success" if s.get("etl_status") == "SUCCESS" else "failure",
                                {"success": "quality", "failure": END})
        g.add_conditional_edges("quality", lambda s: s.get("quality_decision", "fail"),
                                {"pass": "analytics", "fail": END})
        g.add_edge("analytics", END)
        g.add_edge("explain", END)
        return g.compile(checkpointer=self.checkpointer)

    def run(self, request: str, from_date: Optional[str] = None, to_date: Optional[str] = None,
            run_id: Optional[str] = None) -> PipelineState:
        """Execute one request end to end and return the final state (also persisted to reports/)."""
        run_id = run_id or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S") + f"-{uuid.uuid4().hex[:4]}"
        self.llm.reset_usage()
        self.toolkit.calls = []
        initial: PipelineState = {"run_id": run_id, "request": request, "from_date": from_date, "to_date": to_date,
                                  "nodes_run": [], "findings": [], "started_at": time.time(),
                                  "escalated_to_human": False}
        cfg = {"configurable": {"thread_id": run_id}, "recursion_limit": 12}
        return self.app.invoke(initial, cfg)


def render_markdown(result: dict, findings: list[dict], checks: list[dict], calls: list[dict]) -> str:
    icon = {"PASS": "✅", "PASS_WITH_WARNINGS": "⚠️", "FAIL": "❌", "ESCALATED": "🙋", "ANSWERED": "💬"}
    lines = [
        f"# Run report `{result['run_id']}`",
        "",
        f"**Status:** {icon.get(result['status'], '')} {result['status']}  ",
        f"**Request:** {result.get('request')}  ",
        f"**Path:** {' → '.join(result['nodes_run'])}  ",
        f"**Duration:** {result['duration_s']} s · **LLM:** {result['llm']['client']} "
        f"({result['llm'].get('total_tokens', 0)} tokens, {result['llm'].get('calls', 0)} calls)  ",
        f"**Escalated to human:** {result['escalated_to_human']}",
        "",
        "## Summary",
        "",
        result.get("summary", ""),
        "",
    ]
    if result.get("row_counts"):
        lines += ["## Row counts", "", "| Layer | Rows |", "|---|---|"]
        lines += [f"| {k} | {v} |" for k, v in result["row_counts"].items()]
        lines.append("")
    if result.get("kpis"):
        lines += ["## Gold KPIs", "", "```json", json.dumps(result["kpis"], indent=2), "```", ""]
    if findings:
        lines += ["## Findings", "", "| Rule | Layer | Severity | Explanation | Recommended action |",
                  "|---|---|---|---|---|"]
        for f in findings:
            lines.append(f"| `{f['rule']}` | {f.get('layer')} | **{f['severity']}** | "
                         f"{str(f.get('explanation','')).replace('|', '/')} | "
                         f"{str(f.get('recommended_action','')).replace('|', '/')} |")
        lines.append("")
    if checks:
        lines += ["## Quality checks", "", "| Rule | Layer | Status | Metric | Threshold |", "|---|---|---|---|---|"]
        lines += [f"| `{c['rule']}` | {c['layer']} | {c['status']} | {c['metric_value']} | {c['threshold']} |"
                  for c in checks]
        lines.append("")
    if calls:
        lines += ["## Tool calls (audit log)", "", "| Time | Agent | Tool | Status | ms |", "|---|---|---|---|---|"]
        lines += [f"| {c['ts']} | {c['agent']} | `{c['tool']}` | {c.get('status')} | {c.get('duration_ms', '')} |"
                  for c in calls]
    return "\n".join(lines) + "\n"


def export_graph(app, out_dir: str = "docs") -> dict:
    """Export the compiled graph as Mermaid text and (if reachable) PNG via mermaid.ink."""
    from pathlib import Path

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    graph = app.get_graph()
    (out / "agent_graph.mmd").write_text(graph.draw_mermaid(), encoding="utf-8")
    paths = {"mermaid": str(out / "agent_graph.mmd")}
    try:
        (out / "agent_graph.png").write_bytes(graph.draw_mermaid_png())
        paths["png"] = str(out / "agent_graph.png")
    except Exception as exc:  # offline environments
        log.warning("PNG export skipped: %s", exc)
    return paths
