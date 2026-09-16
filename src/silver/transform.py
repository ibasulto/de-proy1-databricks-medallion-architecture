"""Silver layer: cleansed, conformed, typed and validated entities.

Reads the Bronze (``brz_*``) tables and emits ``sil_*`` delta tables via
SCD1-style MERGE. Applies the data-quality expectations from
``src.utils.quality`` and writes an evidence report into the Volume.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame

from utils.quality import DataFrameExpectations, write_report
from utils.spark_utils import cast_money

MONEY_COLUMNS = [
    "gross_amount",
    "discount_amount",
    "net_amount",
    "tax_amount",
    "cost_amount",
    "gross_sales",
    "net_sales",
    "discount_sales",
    "tax_sales",
    "cost_sales",
    "on_hand_cost",
    "unit_cost",
    "retail_price",
    "budget_amount",
]


def clean_store(df):
    from pyspark.sql import functions as F

    return (
        df.selectExpr("*", "'Active' as _src_status")
        .withColumn("store_key", F.col("store_key").cast("bigint"))
        .withColumn("open_m2", F.col("open_m2").cast("int"))
        .withColumn("open_date", F.to_date(F.col("open_date")))
        .withColumn(
            "status",
            F.when(F.col("status").isin("Active", "Planned"), F.col("status")).otherwise("Unknown"),
        )
        .dropDuplicates(["store_key"])
        .drop("_src_status")
    )


def clean_product(df):
    from pyspark.sql import functions as F

    return (
        df.withColumn("product_key", F.col("product_key").cast("bigint"))
        .withColumn("ean", F.lpad(F.col("ean").cast("string"), 13, "0"))
        .withColumn("retail_price", F.col("retail_price").cast("decimal(18,4)"))
        .withColumn("unit_cost", F.col("unit_cost").cast("decimal(18,4)"))
        .withColumn("uom", F.upper(F.trim(F.col("uom"))))
        .withColumn("barcode_weight_kg", F.col("barcode_weight_kg").cast("decimal(10,3)"))
        .dropDuplicates(["product_key"])
    )


def clean_calendar(df):
    from pyspark.sql import functions as F

    return (
        df.withColumn("date_key", F.col("date_key").cast("bigint"))
        .withColumn("date_value", F.to_date(F.col("date_value")))
        .withColumn("commerce_year", F.col("commerce_year").cast("int"))
        .withColumn("commerce_quarter", F.col("commerce_quarter").cast("int"))
        .withColumn("commerce_month", F.col("commerce_month").cast("int"))
        .withColumn("commerce_week", F.col("commerce_week").cast("int"))
        .withColumn("quarter_week", F.col("quarter_week").cast("int"))
        .withColumn("iso_week", F.col("iso_week").cast("int"))
        .withColumn("date_key_sss", F.col("date_key_sss").cast("bigint"))
        .withColumn("date_key_mc", F.col("date_key_mc").cast("bigint"))
        .dropDuplicates(["date_key"])
    )


def clean_daily_sales(df):
    from pyspark.sql import functions as F

    out = (
        df.withColumn("date_key", F.col("date_key").cast("bigint"))
        .withColumn("store_key", F.col("store_key").cast("bigint"))
        .withColumn("product_key", F.col("product_key").cast("bigint"))
        .withColumn("unit_qty", F.col("unit_qty").cast("int"))
        .withColumn("ticket_count", F.col("ticket_count").cast("int"))
        .withColumn("line_count", F.col("line_count").cast("int"))
        .withColumn("promo_line_count", F.col("promo_line_count").cast("int"))
        .dropDuplicates(["date_key", "store_key", "product_key"])
    )
    return cast_money(
        out, ["gross_sales", "net_sales", "discount_amount", "tax_amount", "cost_amount"]
    )


def clean_pos_line(df):
    from pyspark.sql import functions as F

    out = (
        df.withColumn("txn_date", F.to_date(F.col("txn_date")))
        .withColumn("line_id", F.col("line_id").cast("int"))
        .withColumn("store_key", F.col("store_key").cast("bigint"))
        .withColumn("product_key", F.col("product_key").cast("bigint"))
        .withColumn("unit_qty", F.col("unit_qty").cast("int"))
        .withColumn("is_promo", F.col("is_promo") == "true")
        .withColumn("payment_type", F.upper(F.trim(F.col("payment_type"))))
        .withColumn("channel", F.upper(F.trim(F.col("channel"))))
        .dropDuplicates(["txn_id", "line_id"])
    )
    return cast_money(
        out, ["gross_amount", "discount_amount", "net_amount", "tax_amount", "cost_amount"]
    )


def clean_store_daily_kpi(df):
    from pyspark.sql import functions as F

    out = (
        df.withColumn("date_key", F.col("date_key").cast("bigint"))
        .withColumn("store_key", F.col("store_key").cast("bigint"))
        .withColumn("ticket_count", F.col("ticket_count").cast("int"))
        .withColumn("transaction_count", F.col("transaction_count").cast("int"))
        .withColumn("traffic_estimate", F.col("traffic_estimate").cast("int"))
        .withColumn("avg_basket", F.col("avg_basket").cast("decimal(18,4)"))
        .withColumn("sales_m2", F.col("sales_m2").cast("decimal(18,4)"))
        .dropDuplicates(["date_key", "store_key"])
    )
    return cast_money(out, ["gross_sales", "net_sales"])


def clean_daily_inventory(df):
    from pyspark.sql import functions as F

    out = (
        df.withColumn("date_key", F.col("date_key").cast("bigint"))
        .withColumn("store_key", F.col("store_key").cast("bigint"))
        .withColumn("product_key", F.col("product_key").cast("bigint"))
        .withColumn("on_hand_qty", F.col("on_hand_qty").cast("int"))
        .withColumn("received_qty", F.col("received_qty").cast("int"))
        .withColumn("sold_qty", F.col("sold_qty").cast("int"))
        .withColumn("adjusted_qty", F.col("adjusted_qty").cast("int"))
        .withColumn("in_transit_qty", F.col("in_transit_qty").cast("int"))
        .withColumn(
            "stock_state",
            F.when(
                F.col("stock_state").isin("IN_STOCK", "LOW_STOCK", "OUT_OF_STOCK"),
                F.col("stock_state"),
            ).otherwise("UNKNOWN"),
        )
        .dropDuplicates(["date_key", "store_key", "product_key"])
    )
    return cast_money(out, ["on_hand_cost", "unit_cost", "retail_price"])


def clean_budget(df):
    from pyspark.sql import functions as F

    out = (
        df.withColumn("calendar_month_start", F.to_date(F.col("calendar_month_start")))
        .withColumn("store_key", F.col("store_key").cast("bigint"))
        .withColumn("budget_qty_units", F.col("budget_qty_units").cast("int"))
        .withColumn("dept", F.initcap(F.trim(F.col("dept"))))
        .dropDuplicates(["commerce_period", "store_key", "dept"])
    )
    return cast_money(out, ["budget_amount"])


def _with_key(df, key_cols):
    from pyspark.sql import functions as F

    cols = key_cols + [
        c
        for c in df.columns
        if c not in set(key_cols) and c not in {"_ingestion_ts", "_load_id", "_source_file", "_rn"}
    ]
    return df.select(*cols, F.current_timestamp().alias("sil_updated_ts"))


def merge_into(spark, df: DataFrame, fq: str, key_cols: list[str]) -> None:
    """SCD1 upsert: update changed rows, insert new ones."""

    src = "updates"
    df = _with_key(df, key_cols)
    df.createOrReplaceTempView(src)
    fq = _normalize(fq)
    keys_sql = " AND ".join(f"t.{k} <=> {src}.{k}" for k in key_cols)
    spark.sql(
        f"""
        MERGE INTO {fq} t
        USING {src}
        ON {keys_sql}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _normalize(name: str) -> str:
    return ".".join(f"`{p}`" for p in name.split("."))


VIOLATION_FIELDS = {"_ingestion_ts", "_load_id", "_source_file", "_rn"}


def _apply_dq(
    spark, df, fq, table_label, checks: Callable[[DataFrameExpectations], None]
) -> list[dict]:
    work = df.drop(*[c for c in VIOLATION_FIELDS if c in df.columns])
    dq = DataFrameExpectations(
        work, table=fq, dq_threshold=spark.conf.get("spark.dq.threshold", "0.05")
    )
    checks(dq)
    try:
        return dq.run_all()
    except ValueError:
        rows = [r.as_row() for r in dq.results]
        rows.append(
            {
                "expectation_type": "overall",
                "column": "",
                "passed": False,
                "observed": f"{table_label}: data quality failed",
                "detail": '{"threshold": 0.05}',
            }
        )
        return rows


def define_silver(spark, cfg) -> list[dict]:
    """Pipeline definition: (bronze table, silver table, cleaner, keys, dq checks)."""

    def dq_store(e: DataFrameExpectations) -> None:
        e.expect_column_values_to_not_be_null("store_key").expect_row_count_to_be_between(
            1
        ).expect_column_distinct_values_to_contain_set(
            "chain_format", {"hyper", "super", "express"}
        )

    def dq_product(e: DataFrameExpectations) -> None:
        e.expect_column_values_to_not_be_null("product_key").expect_column_values_to_not_be_null(
            "sku"
        ).expect_column_distinct_values_to_contain_set(
            "category", {c["category"] for c in _CATEGORIES}
        ).expect_row_count_to_be_between(1)

    def dq_calendar(e: DataFrameExpectations) -> None:
        e.expect_column_values_to_not_be_null("date_key").expect_column_values_to_be_between(
            "commerce_month", 1, 12
        ).expect_column_values_to_be_between(
            "commerce_quarter", 1, 4
        ).expect_column_values_to_be_between("commerce_week", 1, 53)

    def dq_sales(e: DataFrameExpectations) -> None:
        e.expect_column_values_to_not_be_null("date_key").expect_column_values_to_not_be_null(
            "store_key"
        ).expect_column_values_to_not_be_null("product_key").expect_column_values_to_be_between(
            "net_sales", 0.0, None
        ).expect_row_count_to_be_between(1)

    def dq_pos(e: DataFrameExpectations) -> None:
        e.expect_column_values_to_not_be_null("txn_id").expect_column_values_to_not_be_null(
            "txn_date"
        ).expect_column_values_to_be_between(
            "net_amount", 0.0, None
        ).expect_row_count_to_be_between(1)

    def dq_kpi(e: DataFrameExpectations) -> None:
        e.expect_column_values_to_not_be_null("date_key").expect_column_values_to_not_be_null(
            "store_key"
        ).expect_column_values_to_be_between("net_sales", 0.0, None).expect_row_count_to_be_between(
            1
        )

    def dq_inventory(e: DataFrameExpectations) -> None:
        e.expect_column_values_to_not_be_null("store_key").expect_column_values_to_not_be_null(
            "product_key"
        ).expect_column_values_to_be_between(
            "on_hand_qty", 0, None
        ).expect_column_distinct_values_to_contain_set(
            "stock_state", {"IN_STOCK", "LOW_STOCK", "OUT_OF_STOCK", "UNKNOWN"}
        ).expect_row_count_to_be_between(1)

    def dq_budget(e: DataFrameExpectations) -> None:
        e.expect_column_values_to_not_be_null(
            "commerce_period"
        ).expect_column_values_to_not_be_null("store_key").expect_column_values_to_be_between(
            "budget_amount", 0.0, None
        ).expect_row_count_to_be_between(1)

    specs = [
        {
            "bronze": "brz_stores_master",
            "silver": "sil_store",
            "clean": clean_store,
            "keys": ["store_key"],
            "dq": dq_store,
            "source": "stores",
        },
        {
            "bronze": "brz_products_master",
            "silver": "sil_product",
            "clean": clean_product,
            "keys": ["product_key"],
            "dq": dq_product,
            "source": "products",
        },
        {
            "bronze": "brz_calendar_445",
            "silver": "sil_calendar_445",
            "clean": clean_calendar,
            "keys": ["date_key"],
            "dq": dq_calendar,
            "source": "calendar",
        },
        {
            "bronze": "brz_sales_daily_store_sku",
            "silver": "sil_daily_sales",
            "clean": clean_daily_sales,
            "keys": ["date_key", "store_key", "product_key"],
            "dq": dq_sales,
            "source": "sales",
        },
        {
            "bronze": "brz_pos_line_items",
            "silver": "sil_pos_line",
            "clean": clean_pos_line,
            "keys": ["txn_id", "line_id"],
            "dq": dq_pos,
            "source": "pos",
        },
        {
            "bronze": "brz_store_daily_kpi",
            "silver": "sil_store_daily_kpi",
            "clean": clean_store_daily_kpi,
            "keys": ["date_key", "store_key"],
            "dq": dq_kpi,
            "source": "kpi",
        },
        {
            "bronze": "brz_daily_inventory",
            "silver": "sil_daily_inventory",
            "clean": clean_daily_inventory,
            "keys": ["date_key", "store_key", "product_key"],
            "dq": dq_inventory,
            "source": "inventory",
        },
        {
            "bronze": "brz_budget",
            "silver": "sil_budget",
            "clean": clean_budget,
            "keys": ["commerce_period", "store_key", "dept"],
            "dq": dq_budget,
            "source": "budget",
        },
    ]
    return specs


_CATEGORIES = [
    "Fresh",
    "Dairy",
    "Bakery",
    "Beverages",
    "Snacks",
    "Pantry",
    "Household",
    "PersonalCare",
    "Electronics",
    "Clothing",
]


def run_silver(spark, cfg) -> list[dict]:
    """Execute all silver transformations + DQ; returns the aggregate report."""
    all_reports = []
    for spec in define_silver(spark, cfg):
        brz = spec["bronze"]
        if not spark.catalog.tableExists(cfg.name(cfg.bronze_schema, brz)):
            continue
        df = spark.table(cfg.name(cfg.bronze_schema, brz))
        cleaned = spec["clean"](df)
        fq = cfg.name(cfg.silver_schema, spec["silver"])
        merge_into(spark, cleaned, fq, spec["keys"])
        report = _apply_dq(spark, cleaned, fq, spec["silver"], spec["dq"]).collect()
        all_reports.extend(report)
    report_path = f"{cfg.dq_report_root()}/silver"
    write_report(spark, all_reports, report_path, "Silver data quality")
    return all_reports
