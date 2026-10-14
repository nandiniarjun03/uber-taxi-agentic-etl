"""Execution backends used by the agents' tools.

* ``DatabricksJobRunner`` triggers and polls a Databricks Job through the Databricks SDK.
* ``LocalJobRunner`` runs the same Bronze -> Silver code in-process (local Spark) in a
  background thread, so the agents poll it exactly like a remote job. Used for development,
  CI and the fault-injection evaluation.
* ``SparkPipelineOps`` exposes read-mostly pipeline operations (quality checks, table
  stats, Gold build) to the agents.
* ``ReportStore`` reads/writes run reports under the ``reports`` container only.
"""

from __future__ import annotations

import json
import logging
import time
import traceback
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Optional, Protocol

from src.config import Config

log = logging.getLogger(__name__)

TERMINAL_STATES = {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}
# Errors that will not go away by retrying the same job
NON_RETRYABLE_ERRORS = {"MissingSourceError", "SourceSchemaError", "DataQualityError"}


@dataclass
class JobRun:
    run_id: str
    job: str
    state: str = "PENDING"  # PENDING | RUNNING | TERMINATED | INTERNAL_ERROR
    result_state: Optional[str] = None  # SUCCESS | FAILED | TIMEDOUT | CANCELED
    error_type: Optional[str] = None
    message: str = ""
    started_at: float = field(default_factory=time.time)
    ended_at: Optional[float] = None
    output: dict = field(default_factory=dict)
    task_states: dict = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def duration_s(self) -> Optional[float]:
        return round((self.ended_at or time.time()) - self.started_at, 2)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["duration_s"] = self.duration_s
        d["retryable"] = self.error_type not in NON_RETRYABLE_ERRORS
        return d


class JobRunner(Protocol):
    def submit(self, job: str, params: dict) -> str: ...

    def get_status(self, run_id: str) -> JobRun: ...

    def cancel(self, run_id: str) -> None: ...


class SimulatedJobFailure(RuntimeError):
    """Stands in for a crashed Databricks task (e.g. executor lost) during fault injection."""


class LocalJobRunner:
    """Runs pipeline stages with local Spark on a worker thread."""

    def __init__(self, cfg: Config, spark=None, simulate_failure: bool = False):
        self.cfg = cfg
        self._spark = spark
        self.simulate_failure = simulate_failure
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-job")
        self._runs: dict[str, tuple[JobRun, Future]] = {}

    @property
    def spark(self):
        if self._spark is None:
            from src.spark_utils import get_spark

            self._spark = get_spark(cfg=self.cfg)
        return self._spark

    def _execute(self, job: str, params: dict) -> dict:
        from src import etl_pipeline, gold_analytics

        if self.simulate_failure:
            raise SimulatedJobFailure("Task 'silver' failed: ExecutorLostFailure (executor 1 exited, OOM)")
        if job == "bronze_silver":
            return etl_pipeline.run_bronze_to_silver(
                self.spark, self.cfg, params.get("from_date"), params.get("to_date")
            )
        if job == "gold":
            return gold_analytics.build_gold_tables(self.spark, self.cfg)
        raise ValueError(f"Unknown job '{job}'")

    def submit(self, job: str, params: dict) -> str:
        run_id = f"local-{uuid.uuid4().hex[:8]}"
        run = JobRun(run_id=run_id, job=job, state="RUNNING")
        future = self._pool.submit(self._execute, job, params)
        self._runs[run_id] = (run, future)
        return run_id

    def get_status(self, run_id: str) -> JobRun:
        run, future = self._runs[run_id]
        if future.done() and not run.is_terminal:
            run.state = "TERMINATED"
            run.ended_at = time.time()
            exc = future.exception()
            if exc is None:
                run.result_state = "SUCCESS"
                run.output = future.result()
                run.message = "Job completed"
            else:
                run.result_state = "FAILED"
                run.error_type = type(exc).__name__
                run.message = str(exc)[:2000]
                run.output = {"trace_tail": "".join(traceback.format_exception(exc))[-1500:]}
        return run

    def cancel(self, run_id: str) -> None:
        run, future = self._runs[run_id]
        future.cancel()
        run.state, run.result_state, run.ended_at = "TERMINATED", "CANCELED", time.time()


class DatabricksJobRunner:  # pragma: no cover - requires a Databricks workspace
    """Trigger and monitor Databricks Jobs with the Databricks SDK.

    Authentication follows the SDK's unified auth: DATABRICKS_HOST plus either
    DATABRICKS_TOKEN, an Azure service principal (ARM_CLIENT_ID/SECRET/TENANT_ID) or the
    Azure CLI login.
    """

    def __init__(self, cfg: Config):
        from databricks.sdk import WorkspaceClient

        self.cfg = cfg
        self.client = WorkspaceClient(host=cfg.databricks.host) if cfg.databricks.host else WorkspaceClient()
        self.job_ids = {
            "bronze_silver": cfg.databricks.bronze_silver_job_id,
            "full_pipeline": cfg.databricks.full_pipeline_job_id,
        }
        self._jobs: dict[str, str] = {}

    def submit(self, job: str, params: dict) -> str:
        job_id = self.job_ids.get(job)
        if not job_id:
            raise ValueError(f"No Databricks job id configured for '{job}'")
        job_params = {k: str(v) for k, v in params.items() if v is not None}
        waiter = self.client.jobs.run_now(job_id=job_id, job_parameters=job_params or None)
        run_id = str(waiter.run_id)
        self._jobs[run_id] = job
        return run_id

    def get_status(self, run_id: str) -> JobRun:
        r = self.client.jobs.get_run(int(run_id))
        life = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "PENDING"
        result = r.state.result_state.value if r.state and r.state.result_state else None
        tasks = {t.task_key: (t.state.result_state.value if t.state and t.state.result_state else
                              t.state.life_cycle_state.value if t.state else None) for t in (r.tasks or [])}
        run = JobRun(run_id=run_id, job=self._jobs.get(run_id, "?"), state=life, result_state=result,
                     message=(r.state.state_message or "") if r.state else "", task_states=tasks,
                     started_at=(r.start_time or 0) / 1000, ended_at=(r.end_time or 0) / 1000 or None)
        if result and result != "SUCCESS":
            for t in r.tasks or []:
                if t.state and t.state.result_state and t.state.result_state.value != "SUCCESS":
                    out = self.client.jobs.get_run_output(t.run_id)
                    run.error_type = (out.error or "TaskFailed").split(":")[0].strip()[:80]
                    run.message = f"Task '{t.task_key}': {out.error or ''}"[:2000]
                    run.output = {"failed_task": t.task_key, "trace_tail": (out.error_trace or "")[-1500:]}
                    break
        elif result == "SUCCESS":
            for t in r.tasks or []:
                out = self.client.jobs.get_run_output(t.run_id)
                # run_task.py prints its metrics as the last JSON line of stdout
                for line in reversed((out.logs or "").splitlines()):
                    if line.startswith("{"):
                        try:
                            run.output[t.task_key] = json.loads(line)
                        except json.JSONDecodeError:
                            pass
                        break
        return run

    def cancel(self, run_id: str) -> None:
        self.client.jobs.cancel_run(int(run_id))


def make_runner(cfg: Config, spark=None, simulate_failure: bool = False) -> JobRunner:
    if cfg.execution_backend == "databricks":
        return DatabricksJobRunner(cfg)
    return LocalJobRunner(cfg, spark=spark, simulate_failure=simulate_failure)


class SparkPipelineOps:
    """Pipeline operations the agents may call. Everything here is read-only on Bronze/Silver."""

    def __init__(self, cfg: Config, spark=None):
        self.cfg = cfg
        self._spark = spark

    @property
    def spark(self):
        if self._spark is None:
            from src.spark_utils import get_spark

            self._spark = get_spark(cfg=self.cfg)
        return self._spark

    def run_quality_checks(self, layer: str, history: Optional[list[dict]] = None) -> list[dict]:
        from src import quality_checks as qc

        kwargs: dict[str, Any] = {"history": history} if layer == "gold" else {}
        return [r.to_dict() for r in qc.run_checks(self.spark, self.cfg, layer, **kwargs)]

    def table_stats(self, layer: str, table: Optional[str] = None) -> dict:
        from pyspark.sql import functions as F

        from src.spark_utils import read_table, table_exists

        path = {
            "bronze": self.cfg.bronze_path,
            "silver": self.cfg.silver_path,
            "quarantine": self.cfg.quarantine_path,
        }.get(layer) or self.cfg.gold_path(table or "daily_bookings")
        if not table_exists(self.spark, path, self.cfg):
            return {"layer": layer, "table": table, "exists": False}
        df = read_table(self.spark, path, self.cfg)
        stats: dict[str, Any] = {"layer": layer, "table": table or layer, "exists": True, "rows": df.count(),
                                 "columns": len(df.columns)}
        if "booking_date" in df.columns:
            mm = df.agg(F.min("booking_date"), F.max("booking_date")).collect()[0]
            stats.update(min_booking_date=str(mm[0]), max_booking_date=str(mm[1]))
        return stats

    def row_counts(self) -> dict:
        return {layer: self.table_stats(layer).get("rows") for layer in ("bronze", "silver", "quarantine")}

    def build_gold(self) -> dict:
        from src.gold_analytics import build_gold_tables

        return build_gold_tables(self.spark, self.cfg)

    def gold_kpis(self) -> dict:
        from src.gold_analytics import gold_kpis

        return gold_kpis(self.spark, self.cfg)


class PathNotAllowedError(PermissionError):
    pass


class ReportStore:
    """Writes run reports to the ``reports`` container (local folder or ADLS Gen2).

    Paths are relative to the reports root; absolute paths and ``..`` are rejected so the
    agents can never write outside reports/.
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._fs = None
        if cfg.storage_mode == "adls":  # pragma: no cover - needs Azure
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            svc = DataLakeServiceClient(f"https://{cfg.storage_account}.dfs.core.windows.net",
                                        credential=DefaultAzureCredential())
            self._fs = svc.get_file_system_client("reports")
        else:
            self.root = Path(cfg.container_root("reports"))

    @staticmethod
    def safe_path(rel: str) -> str:
        p = PurePosixPath(rel.replace("\\", "/"))
        if p.is_absolute() or ".." in p.parts or ":" in rel or not p.parts:
            raise PathNotAllowedError(f"Path '{rel}' is outside the reports container")
        return p.as_posix()

    def write_text(self, rel: str, text: str) -> str:
        rel = self.safe_path(rel)
        if self._fs is not None:  # pragma: no cover
            self._fs.get_file_client(rel).upload_data(text.encode("utf-8"), overwrite=True)
            return f"{self.cfg.reports_root}/{rel}"
        target = self.root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        return target.as_posix()

    def read_text(self, rel: str) -> Optional[str]:
        rel = self.safe_path(rel)
        if self._fs is not None:  # pragma: no cover
            try:
                return self._fs.get_file_client(rel).download_file().readall().decode("utf-8")
            except Exception:
                return None
        target = self.root / rel
        return target.read_text(encoding="utf-8") if target.exists() else None

    def append_line(self, rel: str, line: str) -> None:
        existing = self.read_text(rel) or ""
        self.write_text(rel, existing + line.rstrip("\n") + "\n")

    def read_jsonl(self, rel: str, limit: int = 20) -> list[dict]:
        text = self.read_text(rel) or ""
        rows = [json.loads(line) for line in text.splitlines() if line.strip()]
        return rows[-limit:]
