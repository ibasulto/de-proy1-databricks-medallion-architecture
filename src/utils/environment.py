"""One-time environment setup: catalog, schemas and Volume (UC).

Run from a notebook (Cell 1 style) or via ``databricks bundle run`` so the
medallion has a target before the first load. Idempotent.
"""

from __future__ import annotations


def ensure_environment(spark, cfg) -> list[str]:
    created = []
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
    for schema in (cfg.bronze_schema, cfg.silver_schema, cfg.gold_schema):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")
        created.append(f"{cfg.catalog}.{schema}")
    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.volume}
        COMMENT 'Raw landing zone + DQ evidence for the retail medallion'
        """
    )
    created.append(f"{cfg.catalog}.{cfg.volume}")
    return created
