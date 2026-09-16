"""Bronze layer: raw, immutable delta copies of each source with lineage.

Adds ``_ingestion_ts``, ``_load_id`` and ``_source_file`` to every row while
preserving the original payload (no business logic here).
"""

from __future__ import annotations

import datetime as dt

from utils import naming


def _raw_root(cfg, spark) -> str:
    """Return the directory that mirrors resources/data/raw.

    - ``volume`` (UC/Premium): /Volumes/<catalog>/<volume>/<raw_path>
    - ``workspace`` (Community Edition): DBFS/FileStore landing zone, or an
      explicit absolute path if ``raw_path`` starts with dbfs:/ file:/ or /.
    """
    if cfg.data_location == "volume":
        return f"/Volumes/{cfg.catalog}/{cfg.volume}/{cfg.raw_path}"
    if cfg.raw_path.startswith(("dbfs:", "file:", "/")):
        return cfg.raw_path
    return f"dbfs:/FileStore/medallion/{cfg.raw_path}"


def raw_source_path(cfg, spark, source_dir: str, extra_files: str | None = "*") -> str:
    root = _raw_root(cfg, spark)
    if extra_files is None:
        return f"{root}/{source_dir}"
    return f"{root}/{source_dir}/{extra_files}"


def _source_file_col(cfg, df):
    """Return the source file path column for lineage.

    Unity Catalog serverless does not allow ``input_file_name()``; it exposes
    ``_metadata.file_path`` instead. Classic compute keeps ``input_file_name()``.
    """
    from pyspark.sql import functions as F

    if cfg.uc:
        return F.col("_metadata.file_path")
    return F.input_file_name()


def save_tracking(cfg, df, load_id: str):
    """Add lineage columns; original columns are never dropped."""
    from pyspark.sql import functions as F

    now = dt.datetime.utcnow().isoformat(timespec="seconds")
    return (
        df.withColumn("_ingestion_ts", F.lit(now))
        .withColumn("_load_id", F.lit(load_id))
        .withColumn(
            "_source_file",
            F.coalesce(F.col("_source_file"), _source_file_col(cfg, df)),
        )
    )


def load_bronze(
    spark,
    cfg,
    table: str,
    source_dir: str,
    fmt: str = "csv",
    mode: str = "overwrite",
    extra_files: str | None = "*",
) -> None:
    """Read every raw file under ``source_dir`` into ``<catalog>.bronze.brz_<table>``.

    ``mode='overwrite'`` makes the job idempotent (snapshot semantic).
    """

    fq = cfg.bronze(bronze_table_name(table))
    path = raw_source_path(cfg, spark, source_dir, extra_files=extra_files)

    reader = spark.read.format(fmt).option("header", "true").option("multiLine", "true")
    if fmt == "csv":
        reader = reader.option("inferSchema", "true")
    df = reader.load(path)
    df = df.withColumn("_source_file", _source_file_col(cfg, df))

    df = save_tracking(cfg, df, load_id=f"bronze:{table}:{dt.datetime.utcnow():%Y%m%d%H%M%S}")
    if mode == "overwrite":
        df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(fq)
    else:
        df.write.mode("append").format("delta").saveAsTable(fq)


def auto_loader_bronze(spark, cfg, table: str, source_dir: str, checkpoint_dir: str) -> None:
    """Incremental Auto Loader streaming target (option for premium-capable envs).

    Falls back gracefully: the batch path in :func:`load_bronze` is the default
    used by the bundle jobs so Community Edition works out of the box.
    """
    from pyspark.sql import functions as F

    fq = cfg.bronze(bronze_table_name(table))
    path = raw_source_path(cfg, spark, source_dir)
    cp = f"{checkpoint_dir}/{table}"
    src_col = _source_file_col(cfg, spark.read.format("csv").load(path).limit(1))
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(path)
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_load_id", F.lit(f"autoloader:{table}"))
        .withColumn("_source_file", src_col)
        .writeStream.option("checkpointLocation", cp)
        .trigger(availableNow=True)
        .toTable(fq)
        .awaitTermination()
    )


def bronze_table_name(table: str) -> str:
    return naming.bronze_table(table)
