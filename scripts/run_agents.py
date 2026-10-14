"""Run the multi-agent system from the command line.

    python scripts/run_agents.py "Run the full pipeline for today's load"
    python scripts/run_agents.py "Validate the existing bronze and silver data"
    python scripts/run_agents.py "Why did the last run fail?"
    python scripts/run_agents.py "Backfill 2024-01-01 to 2024-06-30" --approve

Reads configuration from the environment / .env (see .env.example). Without
AZURE_OPENAI_ENDPOINT the deterministic offline reasoning client is used.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.agents import AgentSystem  # noqa: E402
from src.config import get_config  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("request", nargs="?", default="Run the full ETL pipeline and validate every layer")
    p.add_argument("--from-date")
    p.add_argument("--to-date")
    p.add_argument("--approve", action="store_true", help="pre-approve destructive actions / large backfills")
    p.add_argument("--verbose", action="store_true")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO if a.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    cfg = get_config()
    system = AgentSystem(cfg, approval_callback=lambda action, details: a.approve)
    state = system.run(a.request, from_date=a.from_date, to_date=a.to_date)

    report = json.loads(Path(state["report_paths"]["json"]).read_text()) if cfg.storage_mode == "local" else state
    print(json.dumps({k: report.get(k) for k in ("run_id", "route", "nodes_run", "status", "row_counts",
                                                  "checks_passed", "checks_warned", "checks_failed", "findings",
                                                  "escalated_to_human")}, indent=2, default=str))
    print("\nSummary:", state.get("summary"))
    print("Report:", state["report_paths"]["markdown"])


if __name__ == "__main__":
    main()
