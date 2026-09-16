"""One-time environment setup: catalog, schemas and Volume (UC).

Run from a notebook (Cell 1 style) or via ``databricks bundle run`` so the
medallion has a target before the first load. Idempotent.

On Community Edition (no Unity Catalog) it skips catalog/volume and only
creates the bare medallion schemas.
"""

from __future__ import annotations


def ensure_environment(spark, cfg) -> list[str]:
    created = []
    schemas = (cfg.bronze_schema, cfg.silver_schema, cfg.gold_schema)
    if cfg.uc:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
        for schema in schemas:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")
            created.append(f"{cfg.catalog}.{schema}")
        # Volume landing zone: catalog.<volume-schema> is the volume schema;
        # raw_landing + dq_reports are the actual Volumes inside it.
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.volume}")
        for vol in (cfg.raw_path, "dq_reports"):
            spark.sql(
                f"""
                CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.volume}.{vol}
                COMMENT 'Retail medallion {vol} volume'
                """
            )
            created.append(f"{cfg.catalog}.{cfg.volume}.{vol}")
    else:
        for schema in schemas:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            created.append(schema)
    return created
