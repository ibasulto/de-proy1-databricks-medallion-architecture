"""PySpark helpers for the medallion pipeline.

Imported only from Databricks notebooks / jobs. Kept separate from
`config`/`calendar_445` so those stay testable with plain Python.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame

MONEY_TYPE = "decimal(18,4)"
KEY_TYPE = "bigint"


def reader_options(fmt: str) -> dict:
    if fmt == "csv":
        return {
            "header": "true",
            "inferSchema": "true",
            "multiLine": "true",
            "ignoreLeadingWhiteSpace": "true",
            "ignoreTrailingWhiteSpace": "true",
        }
    if fmt == "json":
        return {"multiLine": "true"}
    return {}


def read_raw(spark, source: str, fmt: str, extra_options: dict | None = None) -> DataFrame:
    """Read a raw file (from a Volume or workspace path) keeping source provenance."""
    opts = reader_options(fmt)
    if extra_options:
        opts.update(extra_options)
    df = spark.read.options(**opts).format(fmt).load(source)
    return df


def with_tracking(df, load_id: str, source: str) -> DataFrame:
    """Add row-level lineage columns, the same discipline used in Bronze."""
    from pyspark.sql import functions as F

    now = _dt.datetime.utcnow().isoformat(timespec="seconds")
    return (
        df.withColumn("_ingestion_ts", F.lit(now))
        .withColumn("_load_id", F.lit(load_id))
        .withColumn("_source_file", F.coalesce(F.col("_source_file"), F.lit(source)))
    )


def cast_to(df, cols: Iterable[str], sql_type: str) -> DataFrame:
    """Cast a set of columns to a spark SQL type (e.g. decimal(18,4), bigint)."""
    from pyspark.sql import functions as F

    out = df
    for c in cols:
        out = out.withColumn(c, F.col(c).cast(sql_type))
    return out


def cast_money(df, cols: Sequence[str]) -> DataFrame:
    return cast_to(df, cols, MONEY_TYPE)


def cast_keys(df, cols: Sequence[str]) -> DataFrame:
    return cast_to(df, cols, KEY_TYPE)


def dedupe_keep_last(df, keys: Sequence[str], order_cols: Sequence[str]) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def maintain(spark, table: str, zorder_cols: Sequence[str] | None = None) -> None:
    """OPTIMIZE (ZORDER) + VACUUM retention. Best practice after loads."""
    opt = f"OPTIMIZE {table}"
    if zorder_cols:
        opt += " ZORDER BY (" + ", ".join(zorder_cols) + ")"
    spark.sql(opt)
    spark.sql(f"VACUUM {table} RETAIN 168 HOURS")


def table_exists(spark, fq_table: str) -> bool:
    return bool(spark.catalog.tableExists(fq_table))


def create_schema(spark, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def register_volume(spark, catalog: str, volume: str, comment: str) -> None:
    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS {catalog}.{volume}
        COMMENT '{comment}'
        """
    )
