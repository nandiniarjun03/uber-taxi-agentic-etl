"""Bronze and Silver layers of the Uber ride-booking medallion pipeline (PySpark).

raw/uber_rides/*.csv  --ingest_bronze-->  bronze/uber_rides (Delta, all strings + metadata)
bronze/uber_rides     --build_silver--->  silver/uber_rides (typed, standardised, deduplicated)
                                     +-->  quarantine/uber_rides (rejected rows + reasons)

Both stages are idempotent: a full load overwrites the table, an incremental load
(``from_date`` / ``to_date`` on booking date) replaces exactly that window with Delta
``replaceWhere``. Every stage appends a row to the audit table used by the volume and
reconciliation checks.
"""

from __future__ import annotations

import csv
import io
import logging
import uuid
from datetime import date, datetime, timezone
from functools import reduce
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src import quality_checks as qc
from src.config import (
    ALLOWED_PAYMENT_METHODS,
    ALLOWED_STATUSES,
    ALLOWED_VEHICLE_TYPES,
    COLUMN_RENAMES,
    SOURCE_COLUMNS,
    STATUS_CANCELLED_CUSTOMER,
    STATUS_CANCELLED_DRIVER,
    STATUS_COMPLETED,
    TIMESTAMP_FORMATS,
    Config,
)
from src.spark_utils import read_table, table_exists, write_table

log = logging.getLogger(__name__)

BRONZE_METADATA_COLUMNS = ["ingestion_timestamp", "source_file", "batch_id", "booking_date", "booking_month"]
BRONZE_DATA_COLUMNS = [COLUMN_RENAMES[c] for c in SOURCE_COLUMNS]
NUMERIC_COLUMNS = {
    "avg_vtat": "double",
    "avg_ctat": "double",
    "booking_value": "double",
    "ride_distance": "double",
    "driver_rating": "double",
    "customer_rating": "double",
    "cancelled_by_customer_flag": "int",
    "cancelled_by_driver_flag": "int",
    "incomplete_flag": "int",
}

SILVER_COLUMNS = [
    "booking_id",
    "customer_id",
    "booking_timestamp",
    "booking_date",
    "booking_month",
    "booking_hour",
    "day_of_week",
    "booking_status",
    "vehicle_type",
    "pickup_location",
    "drop_location",
    "avg_vtat",
    "avg_ctat",
    "cancelled_by_customer_flag",
    "customer_cancellation_reason",
    "cancelled_by_driver_flag",
    "driver_cancellation_reason",
    "incomplete_flag",
    "incomplete_reason",
    "booking_value",
    "ride_distance",
    "driver_rating",
    "customer_rating",
    "payment_method",
    "is_completed",
    "is_cancelled",
    "cancelled_by",
    "cancellation_reason",
    "source_file",
    "batch_id",
    "ingestion_timestamp",
    "silver_processed_at",
]

AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("batch_id", T.StringType()),
        T.StructField("stage", T.StringType()),
        T.StructField("run_ts", T.TimestampType()),
        T.StructField("rows_in", T.LongType()),
        T.StructField("rows_out", T.LongType()),
        T.StructField("rows_quarantined", T.LongType()),
        T.StructField("duplicates_removed", T.LongType()),
        T.StructField("window_from", T.StringType()),
        T.StructField("window_to", T.StringType()),
        T.StructField("source_files", T.StringType()),
    ]
)


class MissingSourceError(RuntimeError):
    """Raised when no source CSV files are present in the landing zone."""


class SourceSchemaError(RuntimeError):
    """Raised when a source file's header does not match the expected 21-column schema."""


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def raw_schema() -> T.StructType:
    """Explicit 21-column schema for the source CSV. Everything is read as string in Bronze
    so that malformed values are preserved for auditing instead of silently becoming null."""
    return T.StructType([T.StructField(c, T.StringType(), True) for c in SOURCE_COLUMNS])


def window_predicate(from_date: Optional[str], to_date: Optional[str]) -> Optional[str]:
    """SQL predicate on booking_date for an incremental window (None = full load)."""
    clauses = []
    if from_date:
        date.fromisoformat(from_date)  # validate
        clauses.append(f"booking_date >= '{from_date}'")
    if to_date:
        date.fromisoformat(to_date)
        clauses.append(f"booking_date <= '{to_date}'")
    return " AND ".join(clauses) if clauses else None


def parse_booking_timestamp(date_col: str = "date_raw", time_col: str = "time_raw") -> Column:
    """Combine the separate Date (dd-MM-yyyy) and Time (HH:mm:ss) columns into one timestamp.

    ``try_to_timestamp`` returns null instead of failing under ANSI mode, so unparseable
    values end up in quarantine rather than crashing the job.
    """
    combined = f"concat_ws(' ', trim({date_col}), trim({time_col}))"
    parsed = F.coalesce(*[F.expr(f"try_to_timestamp({combined}, '{fmt}')") for fmt in TIMESTAMP_FORMATS])
    # Booking times are local (Delhi) wall-clock times: keep them timezone-free.
    return parsed.cast("timestamp_ntz")


def _strip_quotes(col: Column) -> Column:
    return F.trim(F.regexp_replace(col, r'^[\s"\']+|[\s"\']+$', ""))


def _canonical(col_name: str, allowed: list[str], aliases: dict[str, str]) -> Column:
    """Map free-text domain values onto canonical spellings (case/punctuation-insensitive).
    Unknown values are kept as-is so that the domain rules can quarantine them."""
    key = F.trim(F.regexp_replace(F.regexp_replace(F.lower(F.col(col_name)), r"[^a-z ]", ""), r"\s+", " "))
    mapping: dict[str, str] = {v.lower(): v for v in allowed}
    mapping.update(aliases)
    mapping_expr = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    return F.when(F.col(col_name).isNull(), None).otherwise(F.coalesce(mapping_expr[key], F.trim(F.col(col_name))))


STATUS_ALIASES = {
    "complete": STATUS_COMPLETED,
    "canceled by customer": STATUS_CANCELLED_CUSTOMER,
    "customer cancelled": STATUS_CANCELLED_CUSTOMER,
    "canceled by driver": STATUS_CANCELLED_DRIVER,
    "driver cancelled": STATUS_CANCELLED_DRIVER,
    "no driver": "No Driver Found",
}
VEHICLE_ALIASES = {"e bike": "eBike", "ebike": "eBike", "gomini": "Go Mini", "uberxl": "Uber XL", "xl": "Uber XL"}
PAYMENT_ALIASES = {"wallet": "Uber Wallet", "creditcard": "Credit Card", "debitcard": "Debit Card"}


# --------------------------------------------------------------------------------------
# Source discovery and validation
# --------------------------------------------------------------------------------------
def list_source_files(spark: SparkSession, cfg: Config) -> list[str]:
    """List CSV files in the landing zone using the Hadoop FileSystem API (works for local
    paths and abfss://), falling back to dbutils on Spark Connect clusters."""
    if cfg.storage_mode == "local":
        from pathlib import Path

        return sorted(p.resolve().as_posix() for p in Path(cfg.raw_path).glob(cfg.source_glob)
                      if p.is_file() and p.stat().st_size > 0)
    pattern = f"{cfg.raw_path}/{cfg.source_glob}"
    try:
        jvm = spark._jvm  # type: ignore[attr-defined]
        hpath = jvm.org.apache.hadoop.fs.Path(pattern)
        fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())  # type: ignore[attr-defined]
        statuses = fs.globStatus(hpath) or []
        files = [s.getPath().toString() for s in statuses if s.isFile() and s.getLen() > 0]
    except Exception:  # pragma: no cover - Spark Connect / serverless
        from pyspark.dbutils import DBUtils  # type: ignore

        files = [f.path for f in DBUtils(spark).fs.ls(cfg.raw_path) if f.path.endswith(".csv") and f.size > 0]
    return sorted(files)


def read_header(spark: SparkSession, path: str) -> list[str]:
    first_line = spark.read.text(path).limit(1).collect()
    if not first_line:
        return []
    text = first_line[0][0].lstrip("﻿")
    return [c.strip().strip('"') for c in next(csv.reader(io.StringIO(text)))]


def validate_source_headers(spark: SparkSession, files: list[str]) -> None:
    """Fail fast on schema drift (renamed, missing or extra columns) before anything is written."""
    problems = []
    for path in files:
        header = read_header(spark, path)
        if header != SOURCE_COLUMNS:
            missing = [c for c in SOURCE_COLUMNS if c not in header]
            extra = [c for c in header if c not in SOURCE_COLUMNS]
            problems.append(f"{path}: missing={missing} unexpected={extra} order_ok={not missing and not extra}")
    if problems:
        raise SourceSchemaError("Source schema mismatch:\n" + "\n".join(problems))


# --------------------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------------------
def read_raw(spark: SparkSession, files: list[str]) -> DataFrame:
    """Read the source CSVs with the explicit schema. The literal text ``null`` is a real null."""
    return (
        spark.read.option("header", "true")
        .option("nullValue", "null")
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .schema(raw_schema())
        .csv(files)
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


def to_bronze(raw_df: DataFrame, batch_id: str, ingestion_ts: Optional[datetime] = None) -> DataFrame:
    """Rename to snake_case, strip quotes from IDs, normalise blanks/'null' to null and add
    ingestion metadata. Values stay as strings - typing happens in Silver."""
    ingestion_ts = ingestion_ts or datetime.now(timezone.utc).replace(tzinfo=None)
    df = raw_df
    for src, dst in COLUMN_RENAMES.items():
        df = df.withColumnRenamed(src, dst)
    for c in BRONZE_DATA_COLUMNS:
        cleaned = _strip_quotes(F.col(c)) if c in ("booking_id", "customer_id") else F.trim(F.col(c))
        df = df.withColumn(c, F.when(cleaned.isin("", "null", "NULL", "None"), None).otherwise(cleaned))
    ts = parse_booking_timestamp()
    return df.select(
        *BRONZE_DATA_COLUMNS,
        F.lit(ingestion_ts).cast("timestamp").alias("ingestion_timestamp"),
        F.col("source_file"),
        F.lit(batch_id).alias("batch_id"),
        F.to_date(ts).alias("booking_date"),
        F.date_format(ts, "yyyy-MM").alias("booking_month"),
    )


def ingest_bronze(
    spark: SparkSession,
    cfg: Config,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    batch_id: Optional[str] = None,
) -> dict:
    """Land raw CSV files into the Bronze Delta table. Returns stage metrics."""
    batch_id = batch_id or new_batch_id()
    files = list_source_files(spark, cfg)
    if not files:
        raise MissingSourceError(f"No source files matching '{cfg.source_glob}' in {cfg.raw_path}")
    validate_source_headers(spark, files)

    bronze = to_bronze(read_raw(spark, files), batch_id)
    predicate = window_predicate(from_date, to_date)
    if predicate:
        bronze = bronze.filter(predicate)
    write_table(bronze, cfg.bronze_path, cfg, partition_by=["booking_month"], replace_where=predicate)

    written = read_table(spark, cfg.bronze_path, cfg).filter(F.col("batch_id") == batch_id).count()
    metrics = {
        "stage": "bronze",
        "batch_id": batch_id,
        "rows_in": written,
        "rows_out": written,
        "window_from": from_date,
        "window_to": to_date,
        "source_files": [f.rsplit("/", 1)[-1] for f in files],
    }
    write_audit(spark, cfg, metrics)
    log.info("Bronze ingestion complete: %s", metrics)
    return metrics


# --------------------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------------------
def standardise(bronze_df: DataFrame) -> DataFrame:
    """Type, standardise and enrich Bronze rows (no rows are dropped here).

    Adds ``_numeric_parse_error`` so that non-numeric text in numeric columns is rejected
    instead of silently turning into null.
    """
    # Detect unparseable numbers on the raw strings *before* casting them.
    parse_errors = [F.col(c).isNotNull() & F.expr(f"try_cast({c} AS {t})").isNull() for c, t in NUMERIC_COLUMNS.items()]
    df = bronze_df.withColumn("_numeric_parse_error", reduce(lambda a, b: a | b, parse_errors))
    for c, typ in NUMERIC_COLUMNS.items():
        df = df.withColumn(c, F.expr(f"try_cast({c} AS {typ})"))

    ts = parse_booking_timestamp()
    df = (
        df.withColumn("booking_timestamp", ts)
        .withColumn("booking_date", F.to_date("booking_timestamp"))
        .withColumn("booking_month", F.date_format("booking_timestamp", "yyyy-MM"))
        .withColumn("booking_hour", F.hour("booking_timestamp"))
        .withColumn("day_of_week", F.date_format("booking_timestamp", "EEEE"))
        .withColumn("booking_status", _canonical("booking_status", ALLOWED_STATUSES, STATUS_ALIASES))
        .withColumn("vehicle_type", _canonical("vehicle_type", ALLOWED_VEHICLE_TYPES, VEHICLE_ALIASES))
        .withColumn("payment_method", _canonical("payment_method", ALLOWED_PAYMENT_METHODS, PAYMENT_ALIASES))
    )
    status = F.col("booking_status")
    df = (
        df.withColumn("is_completed", status == F.lit(STATUS_COMPLETED))
        .withColumn("is_cancelled", status.isin(STATUS_CANCELLED_CUSTOMER, STATUS_CANCELLED_DRIVER))
        .withColumn(
            "cancelled_by",
            F.when(status == STATUS_CANCELLED_CUSTOMER, "customer").when(status == STATUS_CANCELLED_DRIVER, "driver"),
        )
        .withColumn(
            "cancellation_reason",
            F.when(status == STATUS_CANCELLED_CUSTOMER, F.col("customer_cancellation_reason")).when(
                status == STATUS_CANCELLED_DRIVER, F.col("driver_cancellation_reason")
            ),
        )
    )
    return df


def flag_rejections(std_df: DataFrame) -> DataFrame:
    """Attach ``reject_reasons`` (array) using the shared row rules from quality_checks,
    then mark all but one row per Booking ID as ``duplicate_booking_id``.

    The surviving row per Booking ID is the valid one if any, else the latest ingested.
    """
    reasons = F.filter(
        F.array(*[F.when(cond, F.lit(name)) for name, cond in qc.row_rule_conditions().items()]),
        lambda x: x.isNotNull(),
    )
    df = std_df.withColumn("reject_reasons", reasons)
    w = Window.partitionBy("booking_id").orderBy(
        F.size("reject_reasons").asc(), F.col("ingestion_timestamp").desc(), F.col("booking_timestamp").desc()
    )
    df = df.withColumn("_rn", F.row_number().over(w))
    is_dup = F.col("booking_id").isNotNull() & (F.col("_rn") > 1)
    return df.withColumn(
        "reject_reasons",
        F.when(is_dup, F.array_union(F.col("reject_reasons"), F.array(F.lit("duplicate_booking_id")))).otherwise(
            F.col("reject_reasons")
        ),
    ).drop("_rn")


def transform_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze -> (silver_df, quarantine_df). Pure function, used by the job and the unit tests."""
    # Keep the untouched Bronze values so quarantine stores exactly what was received.
    with_raw = bronze_df.withColumn("_raw", F.struct(*BRONZE_DATA_COLUMNS))
    flagged = flag_rejections(standardise(with_raw)).withColumn("silver_processed_at", F.current_timestamp())
    silver = flagged.filter(F.size("reject_reasons") == 0).select(*SILVER_COLUMNS)
    quarantine = flagged.filter(F.size("reject_reasons") > 0).select(
        "_raw.*",
        "source_file",
        "batch_id",
        "ingestion_timestamp",
        "booking_date",
        F.col("reject_reasons"),
        F.col("reject_reasons")[0].alias("primary_reject_reason"),
        F.current_timestamp().alias("quarantined_at"),
    )
    return silver, quarantine


def build_silver(
    spark: SparkSession,
    cfg: Config,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    batch_id: Optional[str] = None,
) -> dict:
    """Clean Bronze into Silver and Quarantine. Returns stage metrics (for reconciliation)."""
    batch_id = batch_id or new_batch_id()
    bronze = read_table(spark, cfg.bronze_path, cfg)
    predicate = window_predicate(from_date, to_date)
    if predicate:
        bronze = bronze.filter(predicate)
    bronze = bronze.select(*BRONZE_DATA_COLUMNS, "ingestion_timestamp", "source_file", "batch_id").cache()
    rows_in = bronze.count()

    silver, quarantine = transform_silver(bronze)
    write_table(silver, cfg.silver_path, cfg, partition_by=["booking_month"], replace_where=predicate)
    write_table(quarantine, cfg.quarantine_path, cfg, replace_where=predicate)

    silver_rows = read_table(spark, cfg.silver_path, cfg)
    quarantine_rows = read_table(spark, cfg.quarantine_path, cfg)
    if predicate:
        silver_rows = silver_rows.filter(predicate)
        quarantine_rows = quarantine_rows.filter(predicate)
    rows_out = silver_rows.count()
    q_count = quarantine_rows.count()
    dups = quarantine_rows.filter(F.array_contains("reject_reasons", "duplicate_booking_id")).count()
    bronze.unpersist()

    metrics = {
        "stage": "silver",
        "batch_id": batch_id,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_quarantined": q_count,
        "duplicates_removed": dups,
        "window_from": from_date,
        "window_to": to_date,
    }
    write_audit(spark, cfg, metrics)
    log.info("Silver build complete: %s", metrics)
    return metrics


# --------------------------------------------------------------------------------------
# Audit table (row counts per stage, used for volume and reconciliation checks)
# --------------------------------------------------------------------------------------
def write_audit(spark: SparkSession, cfg: Config, metrics: dict) -> None:
    row = (
        metrics.get("batch_id"),
        metrics.get("stage"),
        datetime.now(timezone.utc).replace(tzinfo=None),
        int(metrics.get("rows_in") or 0),
        int(metrics.get("rows_out") or 0),
        int(metrics.get("rows_quarantined") or 0),
        int(metrics.get("duplicates_removed") or 0),
        metrics.get("window_from"),
        metrics.get("window_to"),
        ",".join(metrics.get("source_files") or []),
    )
    # Built from literals (no Python worker round-trip) so it runs on any cluster type.
    df = spark.range(1).select(
        *[F.lit(v).cast(f.dataType).alias(f.name) for v, f in zip(row, AUDIT_SCHEMA.fields)]
    )
    write_table(df, cfg.audit_path, cfg, mode="append")


def read_audit(spark: SparkSession, cfg: Config, stage: Optional[str] = None, limit: int = 20) -> list[dict]:
    """Most recent audit rows first."""
    if not table_exists(spark, cfg.audit_path, cfg):
        return []
    df = read_table(spark, cfg.audit_path, cfg)
    if stage:
        df = df.filter(F.col("stage") == stage)
    return [r.asDict() for r in df.orderBy(F.col("run_ts").desc()).limit(limit).collect()]


def run_bronze_to_silver(
    spark: SparkSession, cfg: Config, from_date: Optional[str] = None, to_date: Optional[str] = None
) -> dict:
    batch_id = new_batch_id()
    return {
        "batch_id": batch_id,
        "bronze": ingest_bronze(spark, cfg, from_date, to_date, batch_id),
        "silver": build_silver(spark, cfg, from_date, to_date, batch_id),
    }
