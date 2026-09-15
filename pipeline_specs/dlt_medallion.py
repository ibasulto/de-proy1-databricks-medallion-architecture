"""DLT (Delta Live Tables) alternative for premium Databricks workspaces.

Community Edition has no DLT, so the shipped pipeline uses classic notebooks +
batch ingestion (``notebooks/0*.py``) that work everywhere. This module maps the
same medallion onto DLT pipelines (``CREATE PIPELINE`` / DAB ``pipeline``) so you
can show streaming + expectation-based DQ if you land a Premium/AWS/GCP tenant.

Run with: ``databricks pipelines create --json ...`` or as a DAB ``pipeline``.
"""

from __future__ import annotations

import dlt  # type: ignore
from pyspark.sql import functions as F  # type: ignore


@dlt.table(name="brz_sales_daily_store_sku", comment="Raw inlet (DLT mirror of Bronze)")
def bronze_sales():
    return (
        dlt.read_stream("raw_sales")
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_load_id", F.lit("dlt"))
    )


@dlt.table(
    name="sil_daily_sales",
    comment="Cleansed sales; expectations as quality gates.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("net_sales_non_negative", "net_sales >= 0")
@dlt.expect_or_drop(
    "valid_keys", "date_key IS NOT NULL AND store_key IS NOT NULL AND product_key IS NOT NULL"
)
def silver_sales():
    raw = dlt.read("brz_sales_daily_store_sku").filter("date_key IS NOT NULL")
    return raw.selectExpr(
        "cast(date_key as bigint) as date_key",
        "cast(store_key as bigint) as store_key",
        "cast(product_key as bigint) as product_key",
        "cast(unit_qty as int) as unit_qty",
        "cast(gross_sales as decimal(18,4)) as gross_sales",
        "cast(net_sales as decimal(18,4)) as net_sales",
        "cast(tax_amount as decimal(18,4)) as tax_amount",
        "cast(cost_amount as decimal(18,4)) as cost_amount",
    ).withColumn("margin_amount", F.round(F.col("net_sales") - F.col("cost_amount"), 4))


@dlt.table(
    name="fact_sales",
    comment="Gold fact_sales (day-store-sku) from the DLT silver layer.",
    table_properties={"quality": "gold"},
)
def fact_sales():
    return dlt.read("sil_daily_sales")


@dlt.table(
    name="fact_sales_store_daily",
    comment="Store-day retail KPIs with MTD/YTD/SSS on 4-4-5 calendar.",
    table_properties={"quality": "gold"},
)
def fact_store_daily():
    from pyspark.sql.window import Window  # type: ignore

    cal = dlt.read("sil_calendar").select(
        "date_key", "commerce_period", "commerce_year", "date_key_sss"
    )
    sales = (
        dlt.read("fact_sales")
        .groupBy("date_key", "store_key")
        .agg(F.sum("net_sales").alias("net_sales"), F.sum("unit_qty").alias("unit_qty"))
    )
    joined = sales.join(F.broadcast(cal), "date_key", "left")
    windows = [
        F.sum("net_sales")
        .over(
            Window.partitionBy("store_key", "commerce_period")
            .orderBy("date_key")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        .alias("mtd_sales"),
        F.sum("net_sales")
        .over(
            Window.partitionBy("store_key", "commerce_year")
            .orderBy("date_key")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        .alias("ytd_sales"),
    ]
    return joined.select("*", *windows)


# To enable: replace the notebook tasks in databricks.yml with a `pipeline`
# resource (requires a Premium+ workspace) and load raw sources with:
#   source = manage / auto loader streaming into dlt.read_stream(...)
