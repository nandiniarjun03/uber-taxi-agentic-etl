"""Generate the Databricks notebooks in notebooks/ (kept as code so they diff cleanly).

    python scripts/build_notebooks.py
"""

from __future__ import annotations

from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parent.parent
md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell

BOOTSTRAP = '''import os, sys
from pathlib import Path

# Repo root on sys.path (works in a Databricks Git folder and locally)
ROOT = Path(os.getcwd()).resolve().parent if Path(os.getcwd()).name == "notebooks" else Path(os.getcwd()).resolve()
sys.path.insert(0, str(ROOT))
ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
print("repo root:", ROOT, "| on Databricks:", ON_DATABRICKS)'''

WIDGETS = '''if ON_DATABRICKS:
    dbutils.widgets.text("storage_account", "", "ADLS Gen2 account")
    dbutils.widgets.text("secret_scope", "uber-etl", "Secret scope")
    os.environ["STORAGE_MODE"] = "adls"
    os.environ["STORAGE_ACCOUNT_NAME"] = dbutils.widgets.get("storage_account")
    os.environ["DATABRICKS_SECRET_SCOPE"] = dbutils.widgets.get("secret_scope")
    os.environ.setdefault("OPTIMIZE_GOLD", "true")

from src.config import get_config
cfg = get_config()
print("storage mode:", cfg.storage_mode, "| format:", cfg.table_format)
for layer in ("raw", "bronze", "silver", "quarantine", "reports"):
    print(f"{layer:11}", cfg.container_root(layer))'''

setup = nbf.v4.new_notebook()
setup.cells = [
    md("# 00 · Environment setup — uber-taxi-agentic-etl\n\n"
       "One-time setup: Spark session, ADLS Gen2 access through a service principal stored in a "
       "secret scope, container/folder layout, schema creation, connectivity checks and a test call to the "
       "GPT deployment on Microsoft Foundry.\n\n"
       "**Prerequisites** (see `infra/provision_azure.sh`): ADLS Gen2 account with containers `raw, bronze, silver, "
       "gold, quarantine, reports`; Key Vault-backed secret scope `uber-etl` holding `sp-client-id`, "
       "`sp-client-secret`, `sp-tenant-id`; a GPT deployment on Foundry."),
    code("%pip install -q -r ../requirements.txt\n# dbutils.library.restartPython()  # uncomment on Databricks after the first install"),
    code(BOOTSTRAP),
    md("## 1. Configuration (no hard-coded values — everything comes from `src/config.py`)"),
    code(WIDGETS),
    md("## 2. Spark session and ADLS access\n\nSecrets are read from the secret scope with `dbutils.secrets.get` — "
       "never written in code. (Skip if the cluster already has the OAuth `spark_conf` from `databricks.yml`, or "
       "if you use a Unity Catalog external location.)"),
    code('''from src.spark_utils import get_spark, configure_adls_oauth
spark = get_spark(cfg=cfg)
if cfg.storage_mode == "adls":
    configure_adls_oauth(spark, cfg)
print("Spark", spark.version)'''),
    md("## 3. Create the layer folders and verify connectivity"),
    code('''if ON_DATABRICKS:
    for layer in ("raw", "bronze", "silver", "quarantine"):
        dbutils.fs.mkdirs(f"{cfg.container_root(layer)}/{cfg.dataset}")
    for c in cfg.containers:
        files = dbutils.fs.ls(cfg.container_root(c))
        print(f"OK  {c:11} {len(files)} entries")
else:
    for layer in ("raw", "bronze", "silver", "quarantine", "gold", "reports"):
        Path(cfg.container_root(layer)).mkdir(parents=True, exist_ok=True)
    print("local lakehouse ready at", cfg.local_root)'''),
    md("## 4. Land the source file in `raw/uber_rides/`\n\nUpload `ncr_ride_bookings.csv` (150k rows) to the "
       "landing zone. If it is missing, the synthetic sample from `data/sample/` is copied so the pipeline can run."),
    code('''from src.etl_pipeline import list_source_files
files = list_source_files(spark, cfg)
if not files:
    sample = ROOT / "data" / "sample" / "ncr_ride_bookings_sample.csv"
    if ON_DATABRICKS:
        dbutils.fs.cp(f"file:{sample}", f"{cfg.raw_path}/ncr_ride_bookings_sample.csv")
    else:
        import shutil; Path(cfg.raw_path).mkdir(parents=True, exist_ok=True); shutil.copy(sample, cfg.raw_path)
    files = list_source_files(spark, cfg)
print(files)'''),
    md("## 5. Databases / schemas\n\nRegister the Delta locations as tables so analysts can query them with SQL "
       "(Hive metastore shown; with Unity Catalog use `CREATE SCHEMA <catalog>.uber_etl` and external locations)."),
    code('''spark.sql("CREATE SCHEMA IF NOT EXISTS uber_etl")
from src.config import GOLD_TABLES
from src.spark_utils import table_exists
locations = {"bronze_rides": cfg.bronze_path, "silver_rides": cfg.silver_path, "quarantine_rides": cfg.quarantine_path,
             **{f"gold_{t}": cfg.gold_path(t) for t in GOLD_TABLES}}
if cfg.table_format == "delta":
    for name, path in locations.items():
        if table_exists(spark, path, cfg):
            spark.sql(f"CREATE TABLE IF NOT EXISTS uber_etl.{name} USING DELTA LOCATION '{path}'")
            print("registered uber_etl." + name)'''),
    md("## 6. Test the Foundry GPT endpoint\n\nAuthenticates with Microsoft Entra ID (`DefaultAzureCredential`) "
       "unless `FOUNDRY_AUTH=key`. Uses JSON-mode output like the agents do."),
    code('''from src.agents import make_llm
llm = make_llm(cfg)
print("client:", llm.name)
reply = llm.complete_json("route", "Reply with JSON only: {\\"route\\": \\"explain\\", \\"reason\\": \\"<why>\\"}",
                          {"request": "Why did yesterday's run fail?"})
print(reply, llm.usage)'''),
    md("Setup complete → continue with `01_pipeline_and_agents.ipynb`."),
]

demo = nbf.v4.new_notebook()
demo.cells = [
    md("# 01 · Pipeline walkthrough and agent run\n\n"
       "Runs Phase 1 (Bronze → Silver → Gold with quality checks) and Phase 2 (the LangGraph multi-agent system), "
       "showing table previews, check results, fault injection and the generated run report — the screenshots "
       "required by the project report come from this notebook."),
    code(BOOTSTRAP),
    code(WIDGETS),
    code("from src.spark_utils import get_spark, read_table\nspark = get_spark(cfg=cfg)"),
    md("## Phase 1 — Bronze → Silver"),
    code('''from src import etl_pipeline, gold_analytics, quality_checks as qc
metrics = etl_pipeline.run_bronze_to_silver(spark, cfg)   # pass from_date/to_date for an incremental load
metrics'''),
    code('display(read_table(spark, cfg.bronze_path, cfg).limit(10)) if ON_DATABRICKS else read_table(spark, cfg.bronze_path, cfg).show(5, truncate=False)'),
    code('display(read_table(spark, cfg.silver_path, cfg).limit(10)) if ON_DATABRICKS else read_table(spark, cfg.silver_path, cfg).show(5)'),
    code('''q = read_table(spark, cfg.quarantine_path, cfg)
q.groupBy("primary_reject_reason").count().orderBy("count", ascending=False).show(truncate=False)'''),
    md("## Data quality checks"),
    code('''import pandas as pd
checks = qc.run_bronze_checks(spark, cfg) + qc.run_silver_checks(spark, cfg)
pd.DataFrame([{k: c.to_dict()[k] for k in ("layer", "rule", "category", "status", "metric_value", "threshold", "hard")}
              for c in checks])'''),
    md("## Gold analytics"),
    code('''gold = gold_analytics.build_gold_tables(spark, cfg)
for t in gold["tables"]:
    print(f"--- gold/{t}")
    read_table(spark, cfg.gold_path(t), cfg).show(5)
gold_checks = qc.run_gold_checks(spark, cfg)
pd.DataFrame([{k: c.to_dict()[k] for k in ("rule", "status", "metric_value")} for c in gold_checks])'''),
    md("## Phase 2 — Multi-agent run\n\nThe supervisor routes the request; `etl` triggers the Databricks job "
       "(or the local runner), `quality` validates Bronze/Silver, `analytics` builds and validates Gold, and the "
       "run report is written to `reports/`."),
    code('''from src.agents import AgentSystem, export_graph
system = AgentSystem(cfg)
state = system.run("Run the full ETL pipeline and validate every layer")
print(state["status"], state["nodes_run"])
print(state["summary"])'''),
    code('''from IPython.display import Image, Markdown, display as ipy_display
paths = export_graph(system.app, str(ROOT / "docs"))
if "png" in paths: ipy_display(Image(paths["png"]))
ipy_display(Markdown(system.store.read_text(f"runs/{state['run_id']}/report.md")))'''),
    md("## Ask the explain agent"),
    code('''answer = system.run("Why did the last run end with warnings, and what should I do?")
print(answer["answer"])'''),
    md("## Fault injection (local mode)\n\nCorrupt the landing zone and watch the agents stop at the right node. "
       "`scripts/evaluate_agents.py` runs every scenario and computes detection / false-alarm / recovery rates."),
    code('''from src.fault_injection import FAULTS
pd.DataFrame([{"fault": f.name, "description": f.description, "expected": f.expected_status,
               "stops_at": f.expected_stop_node, "rules": ", ".join(f.expected_rules)} for f in FAULTS.values()])'''),
]

for nb in (setup, demo):
    nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
out = ROOT / "notebooks"
out.mkdir(exist_ok=True)
nbf.write(setup, out / "setup.ipynb")
nbf.write(demo, out / "01_pipeline_and_agents.ipynb")
print("notebooks written to", out)
