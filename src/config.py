"""Central configuration for the uber-taxi-agentic-etl project.

Every path, threshold and model setting lives here so that pipeline and agent
code never hard-code values. Values come from (in priority order):

1. Environment variables (a local ``.env`` file is loaded if python-dotenv is installed)
2. A Databricks secret scope, for secrets only (see ``get_secret``)
3. The defaults below

Two storage modes are supported:

* ``STORAGE_MODE=adls``  -> abfss://<container>@<account>.dfs.core.windows.net/...
* ``STORAGE_MODE=local`` -> ./lakehouse/<container>/...   (dev, tests, CI)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:  # optional: load .env for local development
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# --------------------------------------------------------------------------------------
# Source schema (21 columns, in file order) and domain values
# --------------------------------------------------------------------------------------
SOURCE_COLUMNS: list[str] = [
    "Date",
    "Time",
    "Booking ID",
    "Booking Status",
    "Customer ID",
    "Vehicle Type",
    "Pickup Location",
    "Drop Location",
    "Avg VTAT",
    "Avg CTAT",
    "Cancelled Rides by Customer",
    "Reason for cancelling by Customer",
    "Cancelled Rides by Driver",
    "Driver Cancellation Reason",
    "Incomplete Rides",
    "Incomplete Rides Reason",
    "Booking Value",
    "Ride Distance",
    "Driver Ratings",
    "Customer Rating",
    "Payment Method",
]

# snake_case names used from Bronze onwards
COLUMN_RENAMES: dict[str, str] = {
    "Date": "date_raw",
    "Time": "time_raw",
    "Booking ID": "booking_id",
    "Booking Status": "booking_status",
    "Customer ID": "customer_id",
    "Vehicle Type": "vehicle_type",
    "Pickup Location": "pickup_location",
    "Drop Location": "drop_location",
    "Avg VTAT": "avg_vtat",
    "Avg CTAT": "avg_ctat",
    "Cancelled Rides by Customer": "cancelled_by_customer_flag",
    "Reason for cancelling by Customer": "customer_cancellation_reason",
    "Cancelled Rides by Driver": "cancelled_by_driver_flag",
    "Driver Cancellation Reason": "driver_cancellation_reason",
    "Incomplete Rides": "incomplete_flag",
    "Incomplete Rides Reason": "incomplete_reason",
    "Booking Value": "booking_value",
    "Ride Distance": "ride_distance",
    "Driver Ratings": "driver_rating",
    "Customer Rating": "customer_rating",
    "Payment Method": "payment_method",
}

STATUS_COMPLETED = "Completed"
STATUS_CANCELLED_CUSTOMER = "Cancelled by Customer"
STATUS_CANCELLED_DRIVER = "Cancelled by Driver"
STATUS_NO_DRIVER = "No Driver Found"
STATUS_INCOMPLETE = "Incomplete"

ALLOWED_STATUSES: list[str] = [
    STATUS_COMPLETED,
    STATUS_CANCELLED_CUSTOMER,
    STATUS_CANCELLED_DRIVER,
    STATUS_NO_DRIVER,
    STATUS_INCOMPLETE,
]
ALLOWED_VEHICLE_TYPES: list[str] = [
    "Auto",
    "Bike",
    "eBike",
    "Go Mini",
    "Go Sedan",
    "Premier Sedan",
    "Uber XL",
]
ALLOWED_PAYMENT_METHODS: list[str] = ["UPI", "Cash", "Uber Wallet", "Credit Card", "Debit Card"]

# Accepted source formats for the Date + Time columns (spec: dd-MM-yyyy; the public
# Kaggle export uses yyyy-MM-dd, so both are accepted).
TIMESTAMP_FORMATS: list[str] = ["dd-MM-yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss"]

# Columns that must never be null in Silver
REQUIRED_COLUMNS: list[str] = [
    "booking_id",
    "customer_id",
    "booking_timestamp",
    "booking_status",
    "vehicle_type",
    "pickup_location",
    "drop_location",
]

GOLD_TABLES: list[str] = [
    "daily_bookings",
    "cancellations_by_reason",
    "revenue_by_vehicle_payment",
    "peak_hour_demand",
    "top_locations",
    "vehicle_performance",
    "incomplete_rides_by_reason",
]


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    return str(_env(name, str(default))).strip().lower() in {"1", "true", "yes", "y"}


def get_secret(key: str, scope: Optional[str] = None) -> Optional[str]:
    """Return a secret from the Databricks secret scope, falling back to env vars.

    Secrets are never stored in code or in the repo. On Databricks they live in a
    secret scope (optionally backed by Azure Key Vault); locally they come from the
    environment / .env file.
    """
    scope = scope or _env("DATABRICKS_SECRET_SCOPE", "uber-etl")
    try:  # pragma: no cover - only available on a Databricks cluster
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark).secrets.get(scope=scope, key=key)
    except Exception:
        pass
    return _env(key.upper().replace("-", "_"))


# --------------------------------------------------------------------------------------
# Quality thresholds: (warn_threshold, fail_threshold) on violation *rates* (0..1)
# --------------------------------------------------------------------------------------
@dataclass
class QualityThresholds:
    # Bronze (raw) profile checks
    key_null_rate: tuple[float, float] = (0.005, 0.02)
    duplicate_rate: tuple[float, float] = (0.005, 0.02)
    value_range_rate: tuple[float, float] = (0.001, 0.01)
    domain_rate: tuple[float, float] = (0.001, 0.01)
    flag_consistency_rate: tuple[float, float] = (0.001, 0.01)
    reason_pairing_rate: tuple[float, float] = (0.001, 0.01)
    completed_outcome_rate: tuple[float, float] = (0.001, 0.01)
    completed_payment_missing_rate: tuple[float, float] = (0.001, 0.05)
    # Volume: relative change vs the previous load
    volume_change: tuple[float, float] = (0.10, 0.20)
    # Timeliness: share of days missing inside the observed date range
    missing_days_rate: tuple[float, float] = (0.0, 0.01)
    # Silver: share of Bronze rows sent to quarantine
    quarantine_rate: tuple[float, float] = (0.02, 0.05)
    # Gold plausibility
    completion_rate_bounds: tuple[float, float] = (0.30, 0.95)
    completion_rate_drift: float = 0.15  # absolute change vs history
    reconciliation_tolerance: float = 0.01  # currency units for revenue reconciliation

    @classmethod
    def from_env(cls) -> "QualityThresholds":
        t = cls()
        t.quarantine_rate = (
            _env_float("DQ_QUARANTINE_WARN", t.quarantine_rate[0]),
            _env_float("DQ_QUARANTINE_FAIL", t.quarantine_rate[1]),
        )
        t.volume_change = (
            _env_float("DQ_VOLUME_WARN", t.volume_change[0]),
            _env_float("DQ_VOLUME_FAIL", t.volume_change[1]),
        )
        t.completed_payment_missing_rate = (
            _env_float("DQ_PAYMENT_MISSING_WARN", t.completed_payment_missing_rate[0]),
            _env_float("DQ_PAYMENT_MISSING_FAIL", t.completed_payment_missing_rate[1]),
        )
        return t


@dataclass
class AgentSettings:
    llm_mode: str = "auto"  # auto | foundry | offline
    foundry_endpoint: Optional[str] = None
    foundry_deployment: str = "gpt-4o"
    foundry_api_version: str = "2024-10-21"
    foundry_auth: str = "entra"  # entra | key
    max_tokens_per_call: int = 800
    max_tokens_per_run: int = 12000
    llm_temperature: float = 0.0
    llm_max_retries: int = 2
    job_max_retries: int = 1
    job_poll_seconds: float = 2.0
    job_timeout_seconds: int = 3600
    backfill_limit_days: int = 31
    auto_approve: bool = False
    manual_validation_minutes: float = 45.0  # baseline for the "time saved" metric


@dataclass
class DatabricksSettings:
    host: Optional[str] = None
    bronze_silver_job_id: Optional[int] = None
    full_pipeline_job_id: Optional[int] = None


@dataclass
class Config:
    storage_mode: str = "local"
    storage_account: Optional[str] = None
    local_root: Path = field(default_factory=lambda: PROJECT_ROOT / "lakehouse")
    dataset: str = "uber_rides"
    table_format: str = "delta"  # delta | parquet | memory
    execution_backend: str = "local"  # local | databricks
    optimize_gold: bool = False  # OPTIMIZE / ZORDER (Databricks or Delta >= 2)
    source_glob: str = "*.csv"
    thresholds: QualityThresholds = field(default_factory=QualityThresholds)
    agent: AgentSettings = field(default_factory=AgentSettings)
    databricks: DatabricksSettings = field(default_factory=DatabricksSettings)

    # ---- containers / paths ---------------------------------------------------------
    containers: tuple[str, ...] = ("raw", "bronze", "silver", "gold", "quarantine", "reports")

    def container_root(self, container: str) -> str:
        if container not in self.containers:
            raise ValueError(f"Unknown container '{container}'")
        if self.storage_mode == "adls":
            if not self.storage_account:
                raise ValueError("STORAGE_ACCOUNT_NAME must be set when STORAGE_MODE=adls")
            return f"abfss://{container}@{self.storage_account}.dfs.core.windows.net"
        return (self.local_root / container).resolve().as_posix()

    @property
    def raw_path(self) -> str:
        return f"{self.container_root('raw')}/{self.dataset}"

    @property
    def bronze_path(self) -> str:
        return f"{self.container_root('bronze')}/{self.dataset}"

    @property
    def silver_path(self) -> str:
        return f"{self.container_root('silver')}/{self.dataset}"

    @property
    def quarantine_path(self) -> str:
        return f"{self.container_root('quarantine')}/{self.dataset}"

    @property
    def reports_root(self) -> str:
        return self.container_root("reports")

    @property
    def audit_path(self) -> str:
        """Delta table with one row per pipeline stage execution (row counts, batch id)."""
        return f"{self.container_root('reports')}/_pipeline_audit"

    def gold_path(self, table: str) -> str:
        return f"{self.container_root('gold')}/{table}"

    # ---- construction ---------------------------------------------------------------
    @classmethod
    def from_env(cls) -> "Config":
        cfg = cls(
            storage_mode=_env("STORAGE_MODE", "local"),
            storage_account=_env("STORAGE_ACCOUNT_NAME"),
            local_root=Path(_env("LOCAL_LAKEHOUSE_ROOT", str(PROJECT_ROOT / "lakehouse"))),
            dataset=_env("DATASET_NAME", "uber_rides"),
            table_format=_env("TABLE_FORMAT", "delta"),
            execution_backend=_env("EXECUTION_BACKEND", "local"),
            optimize_gold=_env_bool("OPTIMIZE_GOLD", False),
            source_glob=_env("SOURCE_GLOB", "*.csv"),
            thresholds=QualityThresholds.from_env(),
            agent=AgentSettings(
                llm_mode=_env("LLM_MODE", "auto"),
                foundry_endpoint=_env("AZURE_OPENAI_ENDPOINT"),
                foundry_deployment=_env("AZURE_OPENAI_DEPLOYMENT", "gpt-4o"),
                foundry_api_version=_env("AZURE_OPENAI_API_VERSION", "2024-10-21"),
                foundry_auth=_env("FOUNDRY_AUTH", "entra"),
                max_tokens_per_call=_env_int("LLM_MAX_TOKENS_PER_CALL", 800),
                max_tokens_per_run=_env_int("LLM_MAX_TOKENS_PER_RUN", 12000),
                llm_max_retries=_env_int("LLM_MAX_RETRIES", 2),
                job_max_retries=_env_int("JOB_MAX_RETRIES", 1),
                job_poll_seconds=_env_float("JOB_POLL_SECONDS", 2.0),
                job_timeout_seconds=_env_int("JOB_TIMEOUT_SECONDS", 3600),
                backfill_limit_days=_env_int("BACKFILL_LIMIT_DAYS", 31),
                auto_approve=_env_bool("AGENT_AUTO_APPROVE", False),
                manual_validation_minutes=_env_float("MANUAL_VALIDATION_MINUTES", 45.0),
            ),
            databricks=DatabricksSettings(
                host=_env("DATABRICKS_HOST"),
                bronze_silver_job_id=int(_env("DATABRICKS_BRONZE_SILVER_JOB_ID", "0")) or None,
                full_pipeline_job_id=int(_env("DATABRICKS_FULL_JOB_ID", "0")) or None,
            ),
        )
        return cfg

    def with_local_root(self, root: Path | str) -> "Config":
        """Copy of this config pointing at another local lakehouse (tests, fault injection)."""
        from dataclasses import replace

        return replace(self, storage_mode="local", local_root=Path(root))


def get_config() -> Config:
    return Config.from_env()
