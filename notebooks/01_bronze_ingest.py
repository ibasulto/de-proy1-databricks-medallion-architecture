# Databricks notebook source

# MAGIC %md
# MAGIC # 01 · Bronze ingestion
# MAGIC Raw, immutable delta copies from the Volume landing zone with lineage
# MAGIC (`_ingestion_ts`, `_load_id`, `_source_file`).

# COMMAND ----------

import sys
from pathlib import Path

repo_path = (
    dbutils.widgets.get("repo_path")
    or "/Workspace/Repos/anonymous/de-proy1-databricks-medallion-architecture"
)
src = str(Path(repo_path) / "src")
if src not in sys.path:
    sys.path.insert(0, src)

# COMMAND ----------

from jobs import pipeline
from utils.config import load

cfg = load()

use_autoloader = (dbutils.widgets.get("use_autoloader") or "false").lower() == "true"
loaded = pipeline.run_bronze(spark, cfg, use_autoloader=use_autoloader)

print(f"Bronze loaded ({'autoloader' if use_autoloader else 'batch'}):")
for line in loaded:
    print(" -", line)
