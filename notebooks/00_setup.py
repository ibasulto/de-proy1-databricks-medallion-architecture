# Databricks notebook source

# MAGIC %md
# MAGIC # 00 · Environment setup
# MAGIC Creates the catalog, the medallion schemas and the Volume (idempotent).
# MAGIC Run once per environment (dev/staging/prod).

# COMMAND ----------

import sys
from pathlib import Path

SPARK_CONF_KEYS = {
    "catalog": "spark.dab.catalog",
    "bronze_schema": "spark.dab.bronze_schema",
    "silver_schema": "spark.dab.silver_schema",
    "gold_schema": "spark.dab.gold_schema",
    "volume": "spark.dab.volume",
    "data_location": "spark.dab.data_location",
}

DEFAULTS = {
    "catalog": "retail_lakehouse",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "gold",
    "volume": "retail_volumes",
    "data_location": "volume",
}

for key, conf in SPARK_CONF_KEYS.items():
    value = spark.conf.get(conf, None)
    if value:
        DEFAULTS[key] = value

# Resolve the repo root (bundle injects the absolute path as whitespace-safe text).
repo_path = (
    dbutils.widgets.get("repo_path")
    or "/Workspace/Repos/anonymous/de-proy1-databricks-medallion-architecture"
)
src = str(Path(repo_path) / "src")
if src not in sys.path:
    sys.path.insert(0, src)

# COMMAND ----------

from utils.config import Cfg
from utils.environment import ensure_environment

cfg = Cfg(**DEFAULTS)
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
