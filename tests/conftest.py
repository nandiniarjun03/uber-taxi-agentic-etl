from __future__ import annotations

import os
import platform
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config import Config  # noqa: E402
from src.synthetic_data import generate_rows, write_csv  # noqa: E402

HAS_JAVA = bool(os.environ.get("JAVA_HOME") or shutil.which("java"))
TABLE_FORMAT = os.environ.get("TABLE_FORMAT", "delta")
# Writing files on Windows needs Hadoop's winutils.exe; TABLE_FORMAT=memory avoids disk writes.
CAN_WRITE = TABLE_FORMAT == "memory" or platform.system() != "Windows" or bool(
    os.environ.get("HADOOP_HOME") and Path(os.environ["HADOOP_HOME"], "bin", "winutils.exe").exists())

requires_spark = pytest.mark.skipif(not HAS_JAVA, reason="Java not available for local Spark")
requires_writes = pytest.mark.skipif(not (HAS_JAVA and CAN_WRITE),
                                     reason="Table writes need Linux/macOS, winutils on Windows, or TABLE_FORMAT=memory")


@pytest.fixture(scope="session")
def spark():
    if not HAS_JAVA:
        pytest.skip("Java not available")
    from src.spark_utils import get_spark

    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    session = get_spark("uber-etl-tests")
    yield session
    session.stop()


@pytest.fixture
def cfg(tmp_path) -> Config:
    from dataclasses import replace

    return replace(Config().with_local_root(tmp_path / "lakehouse"), table_format=TABLE_FORMAT)


@pytest.fixture(scope="session")
def clean_rows() -> list[dict]:
    return generate_rows(600, seed=11)


@pytest.fixture
def csv_factory(tmp_path):
    """Write rows (optionally with a custom header) to a CSV and return its path."""
    counter = {"n": 0}

    def make(rows, header=None) -> str:
        counter["n"] += 1
        return str(write_csv(rows, tmp_path / f"src_{counter['n']}.csv", columns=header))

    return make


@pytest.fixture
def bronze_factory(spark, csv_factory):
    """rows -> Bronze DataFrame (read_raw + to_bronze), without writing anything."""
    from src.etl_pipeline import read_raw, to_bronze

    def make(rows, header=None):
        return to_bronze(read_raw(spark, [csv_factory(rows, header)]), batch_id="test-batch").cache()

    return make
