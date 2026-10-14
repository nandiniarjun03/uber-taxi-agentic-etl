"""Run the full Phase-1 pipeline locally (same code as the Databricks Job tasks).

    python scripts/run_local_pipeline.py                 # full load
    python scripts/run_local_pipeline.py --from-date 2024-03-01 --to-date 2024-03-31

Order: bronze -> silver -> quality(bronze, silver) -> gold -> quality(gold). Exits non-zero
if any check FAILs.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import etl_pipeline, gold_analytics, quality_checks  # noqa: E402
from src.config import get_config  # noqa: E402
from src.spark_utils import get_spark  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--from-date")
    p.add_argument("--to-date")
    a = p.parse_args()
    cfg = get_config()
    spark = get_spark(cfg=cfg)

    print(json.dumps(etl_pipeline.run_bronze_to_silver(spark, cfg, a.from_date, a.to_date), indent=2, default=str))
    results = quality_checks.run_bronze_checks(spark, cfg) + quality_checks.run_silver_checks(spark, cfg)
    print(json.dumps(gold_analytics.build_gold_tables(spark, cfg), indent=2))
    results += quality_checks.run_gold_checks(spark, cfg)

    print(f"\n{'STATUS':6} {'LAYER':7} {'RULE':45} METRIC")
    for r in results:
        print(f"{r.status:6} {r.layer:7} {r.rule:45} {r.metric_value}")
    summary = quality_checks.summarize(results)
    print("\n" + json.dumps(summary, indent=2))
    print("\nGold KPIs:", json.dumps(gold_analytics.gold_kpis(spark, cfg), indent=2))
    return 1 if summary["checks_failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
