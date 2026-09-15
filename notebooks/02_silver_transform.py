# Databricks notebook source

# MAGIC %md
# MAGIC # 02 · Silver transformation + DQ
# MAGIC Cleansing, typing (`decimal(18,4)`, `bigint`), SCD1 MERGE and
# MAGIC expectations per entity. Evidence goes to the Volume as JSON + Markdown.

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
report = pipeline.run_silver(spark, cfg)

passed = sum(1 for r in report if r.get("passed"))
total = len(report)
print(f"Silver DQ: {passed}/{total} checks passed")
for r in report:
    status = "PASS" if r.get("passed") else "FAIL"
    print(f" [{status}] {r.get('expectation_type')}:{r.get('column')} -> {r.get('observed')}")
