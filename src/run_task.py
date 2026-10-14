"""Entry point for Databricks Job tasks (and local runs).

    python src/run_task.py --task bronze  [--from-date 2024-03-01 --to-date 2024-03-31] [--batch-id ID]
    python src/run_task.py --task silver  [...]
    python src/run_task.py --task gold
    python src/run_task.py --task quality --layer silver [--fail-on-warn]

Each task prints a JSON metrics line as its last stdout line; on Databricks the agents read
it back through the Jobs API (``get_run_output().logs``). A failing quality task raises, which fails the
job run and triggers the job's retry/alert policy.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Make ``src`` importable when executed as a file by spark_python_task.
_ROOT = Path(__file__).resolve().parent.parent if "__file__" in globals() else Path(sys.argv[0]).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src import etl_pipeline, gold_analytics, quality_checks  # noqa: E402
from src.config import get_config  # noqa: E402
from src.spark_utils import get_spark  # noqa: E402


def _blank_to_none(v):
    return v if v not in (None, "", "None", "null") else None


def main(argv: list[str] | None = None) -> dict:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--task", required=True, choices=["bronze", "silver", "gold", "quality", "bronze_silver"])
    p.add_argument("--layer", choices=["bronze", "silver", "gold"], default="silver")
    p.add_argument("--from-date", dest="from_date")
    p.add_argument("--to-date", dest="to_date")
    p.add_argument("--batch-id", dest="batch_id")
    p.add_argument("--fail-on-warn", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = get_config()
    spark = get_spark(cfg=cfg)
    from_date, to_date, batch_id = map(_blank_to_none, (args.from_date, args.to_date, args.batch_id))

    if args.task == "bronze":
        result = etl_pipeline.ingest_bronze(spark, cfg, from_date, to_date, batch_id)
    elif args.task == "silver":
        result = etl_pipeline.build_silver(spark, cfg, from_date, to_date, batch_id)
    elif args.task == "bronze_silver":
        result = etl_pipeline.run_bronze_to_silver(spark, cfg, from_date, to_date)
    elif args.task == "gold":
        result = gold_analytics.build_gold_tables(spark, cfg)
    else:
        checks = quality_checks.run_checks(spark, cfg, args.layer)
        result = {"stage": f"quality_{args.layer}", **quality_checks.summarize(checks),
                  "results": [c.to_dict() for c in checks]}
        for c in checks:
            print(f"[{c.status:4}] {c.layer:6} {c.rule:45} metric={c.metric_value} threshold={c.threshold}")
        if args.fail_on_warn and any(c.status == "WARN" for c in checks):
            raise quality_checks.DataQualityError(checks)
        quality_checks.assert_checks_pass(checks)

    # The last stdout line is the task's metrics; the Jobs API exposes it via get_run_output().logs
    print(json.dumps(result, default=str))
    return result


if __name__ == "__main__":
    main()
