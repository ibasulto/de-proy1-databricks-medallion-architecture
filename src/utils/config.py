"""Runtime configuration for notebooks and jobs.

Works in Databricks (dbutils.widgets + env vars) and locally (env vars).
Kept free of Spark imports so it can be used by tests and the generator.

Expected widgets (all optional; env vars / defaults fill the gaps):
  catalog, bronze_schema, silver_schema, gold_schema, volume, raw_path,
  data_location (volume | workspace), run_date (YYYY-MM-DD as-of date), env.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

_DEFAULTS = {
    "catalog": "retail_lakehouse",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "gold",
    "volume": "retail_volumes",
    "raw_path": "raw_landing",
    "data_location": "volume",  # where raw files land: volume | workspace
    "env": "dev",
    "run_date": None,
    "repo_path": None,
    "dq_threshold": "0.05",
    "uc": "false",
}

_ENV_MAP = {
    "catalog": "DAB_CATALOG",
    "bronze_schema": "DAB_BRONZE_SCHEMA",
    "silver_schema": "DAB_SILVER_SCHEMA",
    "gold_schema": "DAB_GOLD_SCHEMA",
    "volume": "DAB_VOLUME",
    "raw_path": "DAB_RAW_PATH",
    "data_location": "DATA_LOCATION",
    "env": "DAB_ENV",
    "run_date": "DAB_RUN_DATE",
    "repo_path": "DAB_REPO_PATH",
    "uc": "DAB_UC",
}

_SPARK_CONF_KEYS = {
    "catalog": "spark.dab.catalog",
    "bronze_schema": "spark.dab.bronze_schema",
    "silver_schema": "spark.dab.silver_schema",
    "gold_schema": "spark.dab.gold_schema",
    "volume": "spark.dab.volume",
    "data_location": "spark.dab.data_location",
    "uc": "spark.dab.uc",
}


@dataclass
class Cfg:
    catalog: str = _DEFAULTS["catalog"]
    bronze_schema: str = _DEFAULTS["bronze_schema"]
    silver_schema: str = _DEFAULTS["silver_schema"]
    gold_schema: str = _DEFAULTS["gold_schema"]
    volume: str = _DEFAULTS["volume"]
    raw_path: str = _DEFAULTS["raw_path"]
    data_location: str = _DEFAULTS["data_location"]
    env: str = _DEFAULTS["env"]
    run_date: str | None = _DEFAULTS["run_date"]
    repo_path: str | None = _DEFAULTS["repo_path"]
    dq_threshold: float = float(_DEFAULTS["dq_threshold"])
    uc: bool = False
    extras: dict = field(default_factory=dict)

    def name(self, schema: str, table: str) -> str:
        """Fully qualified table name.

        - Unity Catalog: ``catalog.schema.table`` (3 part)
        - Community Edition / hive_metastore: ``schema.table`` (2 part)
        """
        if self.uc:
            return f"{self.catalog}.{schema}.{table}"
        return f"{schema}.{table}"

    def bronze(self, table: str) -> str:
        return self.name(self.bronze_schema, table)

    def silver(self, table: str) -> str:
        return self.name(self.silver_schema, table)

    def gold(self, table: str) -> str:
        return self.name(self.gold_schema, table)

    def dq_report_root(self) -> str:
        """Where DQ evidence lives (Volume on UC, DBFS/FileStore otherwise)."""
        if self.uc:
            return f"/Volumes/{self.catalog}/{self.volume}/dq_reports"
        return "/FileStore/medallion/dq_reports"


def _read_widgets() -> dict:
    """Read Databricks notebook widgets if the session provides `dbutils`."""
    dbutils = globals().get("dbutils")
    if dbutils is None:
        try:
            from IPython import get_ipython

            shell = get_ipython()
            if shell is not None:
                dbutils = shell.user_ns.get("dbutils")
        except Exception:
            return {}
    if dbutils is None:
        return {}

    out: dict[str, str | None] = {}
    for key in _DEFAULTS:
        try:
            out[key] = dbutils.widgets.get(key)
        except Exception:
            continue
    return out


def _read_spark_conf(mapping: dict | None = None) -> dict:
    """Read ``spark.dab.*`` conf from the session, mirroring notebook 00_setup."""
    mapping = mapping or _SPARK_CONF_KEYS
    spark = globals().get("spark")
    if spark is None:
        try:
            from IPython import get_ipython

            shell = get_ipython()
            if shell is not None:
                spark = shell.user_ns.get("spark")
        except Exception:
            return {}
    if spark is None:
        return {}

    out: dict[str, str | None] = {}
    for key, conf in mapping.items():
        try:
            value = spark.conf.get(conf, None)
        except Exception:
            continue
        if value:
            out[key] = value
    return out


def load(catalog: str | None = None, **overrides) -> Cfg:
    """Build a Cfg. Priority: kwargs > Databricks widgets > env vars > defaults."""
    values: dict[str, str | None] = {**dict(_DEFAULTS)}  # defaults

    for key, var in _ENV_MAP.items():  # env vars
        if os.environ.get(var):
            values[key] = os.environ[var]

    for key, val in _read_spark_conf().items():  # Spark session conf (DAB job)
        if val:
            values[key] = val

    for key, val in _read_widgets().items():  # Databricks widgets
        if val:
            values[key] = val

    if catalog:  # explicit call args win
        values["catalog"] = catalog
    for key, val in overrides.items():
        if val is not None:
            values[key] = str(val)

    return Cfg(
        catalog=values["catalog"],
        bronze_schema=values["bronze_schema"],
        silver_schema=values["silver_schema"],
        gold_schema=values["gold_schema"],
        volume=values["volume"],
        raw_path=values["raw_path"],
        data_location=values["data_location"],
        env=values["env"],
        run_date=values["run_date"],
        repo_path=values["repo_path"],
        dq_threshold=float(values["dq_threshold"]),
        uc=_to_bool(values.get("uc")),
    )


def _to_bool(value: str | bool | None) -> bool:
    """Parse a boolean from str/bool/None (lenient, notebook/DAB friendly)."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "on"}
