# Databricks notebook source

# MAGIC %md
# MAGIC # 04 · Quality gate (decision report)
# MAGIC Runs expectations + cuadraturas over the whole pipeline and writes a
# MAGIC single "overall" verdict into the Volume. CI uses the same logic.

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
outcome = pipeline.run_quality(spark, cfg)
print(f"Verdict: {'PASS' if outcome['passed'] else 'FAIL'}")
print(f"- silver expectations: {outcome['silver_checks']}")
print(f"- gold tables: {len(outcome['gold_tables'])}")
for check in outcome["reconciliations"]:
    print(f"  [{'PASS' if check.get('passed') else 'FAIL'}] {check.get('name')}")

if not outcome["passed"]:
    raise RuntimeError("Quality gate failed: review dq_reports/overall in the Volume")
