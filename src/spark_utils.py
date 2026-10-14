"""Spark session and table I/O helpers shared by the pipeline, quality checks and agents.

Tables are written as Delta by default (``TABLE_FORMAT=delta``). Parquet is a fallback for
environments without the Delta jars (full loads, dynamic partition overwrite), and
``memory`` keeps tables as checkpointed DataFrames for local demos and tests.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from src.config import Config

log = logging.getLogger(__name__)


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "uber-taxi-agentic-etl", cfg: Optional[Config] = None) -> SparkSession:
    """Return the active Spark session (Databricks) or build a local one with Delta enabled."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if is_databricks():  # pragma: no cover
        return SparkSession.builder.getOrCreate()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    table_format = (cfg.table_format if cfg else os.environ.get("TABLE_FORMAT", "delta")).lower()
    if table_format == "delta":
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        builder = configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def configure_adls_oauth(spark: SparkSession, cfg: Config) -> None:  # pragma: no cover - needs Azure
    """Configure ABFS OAuth access with a service principal whose secrets live in a secret scope.

    Not needed when the cluster already has the equivalent ``spark_conf`` (see databricks.yml)
    or when Unity Catalog external locations are used.
    """
    from src.config import get_secret

    account = cfg.storage_account
    suffix = f"{account}.dfs.core.windows.net"
    tenant_id = get_secret("sp-tenant-id")
    spark.conf.set(f"fs.azure.account.auth.type.{suffix}", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{suffix}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{suffix}", get_secret("sp-client-id"))
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{suffix}", get_secret("sp-client-secret"))
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{suffix}",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )


def table_exists(spark: SparkSession, path: str, cfg: Config) -> bool:
    try:
        read_table(spark, path, cfg).limit(0).collect()
        return True
    except Exception:
        return False


# In-memory table store for TABLE_FORMAT=memory: same idempotent semantics as Delta
# (overwrite / replaceWhere / append) without touching disk. Used for demos and tests on
# machines where Spark cannot write files (e.g. Windows without winutils.exe).
_MEMORY_TABLES: dict[str, DataFrame] = {}


def read_table(spark: SparkSession, path: str, cfg: Config) -> DataFrame:
    if cfg.table_format == "memory":
        if path not in _MEMORY_TABLES:
            raise FileNotFoundError(f"Table not found: {path}")
        return _MEMORY_TABLES[path]
    return spark.read.format(cfg.table_format).load(path)


def _write_memory(df: DataFrame, path: str, replace_where: Optional[str], mode: str) -> None:
    existing = _MEMORY_TABLES.get(path)
    if existing is not None and mode == "append":
        df = existing.unionByName(df, allowMissingColumns=True)
    elif existing is not None and replace_where is not None:
        keep = existing.filter(f"NOT coalesce(({replace_where}), false)")
        df = keep.unionByName(df, allowMissingColumns=True)
    _MEMORY_TABLES[path] = df.localCheckpoint(eager=True)


def write_table(
    df: DataFrame,
    path: str,
    cfg: Config,
    partition_by: Optional[list[str]] = None,
    replace_where: Optional[str] = None,
    mode: str = "overwrite",
) -> None:
    """Write a table idempotently.

    * full overwrite when ``replace_where`` is None
    * Delta ``replaceWhere`` (or Parquet dynamic partition overwrite) for incremental windows,
      so re-running the same booking-date window replaces exactly the same partitions.
    """
    if cfg.table_format == "memory":
        return _write_memory(df, path, replace_where, mode)
    writer = df.write.format(cfg.table_format).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if mode == "overwrite":
        if cfg.table_format == "delta":
            if replace_where is None:
                writer = writer.option("overwriteSchema", "true")
            else:
                writer = writer.option("replaceWhere", replace_where)
        else:
            writer = writer.option("partitionOverwriteMode", "static" if replace_where is None else "dynamic")
    writer.save(path)


def optimize_table(spark: SparkSession, path: str, cfg: Config, zorder_by: Optional[list[str]] = None) -> bool:
    """Run OPTIMIZE (+ ZORDER) on a Delta table when enabled. Returns True if executed."""
    if cfg.table_format != "delta" or not cfg.optimize_gold:
        return False
    try:
        sql = f"OPTIMIZE delta.`{path}`"
        if zorder_by:
            sql += f" ZORDER BY ({', '.join(zorder_by)})"
        spark.sql(sql)
        return True
    except Exception as exc:  # pragma: no cover - depends on runtime
        log.warning("OPTIMIZE skipped for %s: %s", path, exc)
        return False
