# Databricks notebook source

# MAGIC %md
# MAGIC # 00 · Environment setup
# MAGIC Creates the catalog, the medallion schemas and the Volume (idempotent).
# MAGIC Run once per environment (dev/staging/prod).

# COMMAND ----------

import sys
from pathlib import Path

# Resolve the repo root (bundle injects the absolute path as whitespace-safe text).
repo_path = (
    dbutils.widgets.get("repo_path")
    or "/Workspace/Repos/anonymous/de-proy1-databricks-medallion-architecture"
)
src = str(Path(repo_path) / "src")
if src not in sys.path:
    sys.path.insert(0, src)

# COMMAND ----------

from utils.config import load
from utils.environment import ensure_environment

cfg = load()
print(
    "Target:",
    cfg.catalog,
    "|",
    cfg.bronze_schema,
    "|",
    cfg.silver_schema,
    "|",
    cfg.gold_schema,
    "|",
    cfg.volume,
)

created = ensure_environment(spark, cfg)
print("Environment ready:")
for item in created:
    print(" -", item)
