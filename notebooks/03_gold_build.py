# Databricks notebook source

# MAGIC %md
# MAGIC # 03 · Gold star schema
# MAGIC Builds dimensions + facts and the retail analytics around the 4-4-5
# MAGIC calendar (MTD/YTD/MC/SSS), then reconciles totals across layers.

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
as_of = dbutils.widgets.get("as_of") or None

tables = pipeline.run_gold(spark, cfg, as_of=as_of)
print("Gold tables built:")
for t in tables:
    print(" -", t)

# COMMAND ----------

reconciliations = pipeline.run_cuadraturas(spark, cfg)
for check in reconciliations:
    status = "PASS" if check.get("passed") else "FAIL"
    print(
        f" [{status}] {check.get('name')} | expected={check.get('expected'):,.0f} actual={check.get('actual'):,.0f} diff%={check.get('diff_pct')}"
    )
