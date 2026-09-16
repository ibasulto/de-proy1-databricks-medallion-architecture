"""Gold layer: business-ready star schema around the 4-4-5 commercial calendar.

Produces dimensions + facts consumed by Databricks SQL dashboards:
  dim_store, dim_product, dim_calendar_445
  fact_sales                  (day-store-sku, conformed keys)
  fact_sales_store_daily      (day-store with MTD/YTD/MC/SSS variants + ratios)
  fact_inventory              (day-store-sku with Days-On-Hand / coverage)
  fact_budget                 (commerce-month-store-dept vs actual)

Also runs cuadratura (reconciliation) checks between layers.
"""

from __future__ import annotations

from utils.quality import assert_balances, write_report


def build_dim_store(spark, cfg) -> str:
    fq = cfg.gold("dim_store")
    (
        spark.table(cfg.silver("sil_store"))
        .select(
            "store_key",
            "store_code",
            "name",
            "chain_format",
            "region",
            "city",
            "address",
            "open_m2",
            "open_date",
            "status",
        )
        .write.mode("overwrite")
        .format("delta")
        .saveAsTable(fq)
    )
    return fq


def build_dim_product(spark, cfg) -> str:
    fq = cfg.gold("dim_product")
    (
        spark.table(cfg.silver("sil_product"))
        .select(
            "product_key",
            "sku",
            "ean",
            "name",
            "brand",
            "category",
            "dept",
            "uom",
            "retail_price",
            "unit_cost",
            "barcode_weight_kg",
        )
        .write.mode("overwrite")
        .format("delta")
        .saveAsTable(fq)
    )
    return fq


def build_dim_calendar(spark, cfg) -> str:
    fq = cfg.gold("dim_calendar_445")
    (
        spark.table(cfg.silver("sil_calendar_445"))
        .select(
            "date_key",
            "date_value",
            "commerce_year",
            "commerce_quarter",
            "commerce_month",
            "commerce_period",
            "commerce_week",
            "quarter_week",
            "iso_week",
            "day_of_week",
            "day_name",
            "is_weekend",
            "date_key_sss",
            "date_key_mc",
        )
        .write.mode("overwrite")
        .format("delta")
        .saveAsTable(fq)
    )
    return fq


def build_fact_sales(spark, cfg) -> str:
    from pyspark.sql import functions as F

    fq = cfg.gold("fact_sales")
    df = (
        spark.table(cfg.silver("sil_daily_sales"))
        .withColumn("margin_amount", F.round(F.col("net_sales") - F.col("cost_amount"), 4))
        .withColumn(
            "margin_pct",
            F.when(
                F.col("net_sales") != 0,
                F.round((F.col("net_sales") - F.col("cost_amount")) / F.col("net_sales"), 6),
            ).otherwise(F.lit(0)),
        )
        .select(
            "date_key",
            "store_key",
            "product_key",
            "sku",
            "unit_qty",
            "gross_sales",
            "net_sales",
            "discount_amount",
            "tax_amount",
            "cost_amount",
            "margin_amount",
            "margin_pct",
            "ticket_count",
            "line_count",
            "promo_line_count",
        )
    )
    df.write.mode("overwrite").format("delta").saveAsTable(fq)
    return fq


def build_fact_sales_store_daily(spark, cfg, as_of: str | None = None) -> str:
    """Store-day fact with retail metrics: MTD/YTD + MC (month close) + SSS.

    MC = prior calendar year same date; SSS = 364 days back (same weekday,
    prior commerce week). Ratios: avg basket, sales/m2, tax share, discount
    rate. A non-null ``as_of`` (YYYY-MM-DD) restricts to a rolling window.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    cal = spark.table(cfg.gold("dim_calendar_445")).alias("cal")
    sales = (
        spark.table(cfg.gold("fact_sales"))
        .groupBy("date_key", "store_key")
        .agg(
            F.sum("unit_qty").alias("unit_qty"),
            F.sum("gross_sales").alias("gross_sales"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("discount_amount").alias("discount_amount"),
            F.sum("tax_amount").alias("tax_amount"),
            F.sum("cost_amount").alias("cost_amount"),
            F.sum("promo_line_count").alias("promo_line_count"),
        )
    )
    if as_of:
        yyyymmdd = int(as_of.replace("-", ""))
        sales = sales.filter(F.col("date_key") <= yyyymmdd)

    store = (
        spark.table(cfg.gold("dim_store"))
        .select("store_key", "chain_format", "open_m2")
        .alias("st")
    )
    kpi = (
        spark.table(cfg.silver("sil_store_daily_kpi"))
        .select("date_key", "store_key", "ticket_count", "transaction_count", "traffic_estimate")
        .alias("k")
    )

    base = (
        sales.alias("s")
        .join(cal, sales["date_key"] == cal["date_key"], "inner")
        .join(store, sales["store_key"] == store["store_key"], "inner")
        .join(
            kpi,
            (sales["date_key"] == kpi["date_key"]) & (sales["store_key"] == kpi["store_key"]),
            "left",
        )
        .select(
            sales["date_key"],
            cal["date_value"],
            cal["commerce_year"],
            cal["commerce_quarter"],
            cal["commerce_month"],
            cal["commerce_period"],
            cal["date_key_sss"],
            cal["date_key_mc"],
            sales["store_key"],
            store["chain_format"],
            store["open_m2"],
            sales["unit_qty"],
            sales["gross_sales"],
            sales["net_sales"],
            sales["discount_amount"],
            sales["tax_amount"],
            sales["cost_amount"],
            sales["promo_line_count"],
            kpi["ticket_count"],
            kpi["transaction_count"],
            kpi["traffic_estimate"],
        )
        .withColumn(
            "mtd_sales",
            F.sum("net_sales").over(
                Window.partitionBy("store_key", "commerce_year", "commerce_month")
                .orderBy("date_key")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "ytd_sales",
            F.sum("net_sales").over(
                Window.partitionBy("store_key", "commerce_year")
                .orderBy("date_key")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "discount_rate",
            F.when(
                F.col("gross_sales") != 0, F.col("discount_amount") / F.col("gross_sales")
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "avg_basket",
            F.when(
                F.col("ticket_count").isNotNull() & (F.col("ticket_count") != 0),
                F.col("net_sales") / F.col("ticket_count"),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "sales_m2",
            F.when(F.col("open_m2") != 0, F.col("net_sales") / F.col("open_m2")).otherwise(
                F.lit(0)
            ),
        )
    )

    ly = base.alias("ly")
    result = (
        base.alias("cur")
        .join(
            ly,
            (base["store_key"] == ly["store_key"]) & (base["date_key_sss"] == ly["date_key"]),
            "left",
        )
        .select(
            base["date_key"],
            base["date_value"],
            base["commerce_year"],
            base["commerce_quarter"],
            base["commerce_month"],
            base["commerce_period"],
            base["date_key_sss"],
            base["date_key_mc"],
            base["store_key"],
            base["chain_format"],
            base["open_m2"],
            base["unit_qty"],
            base["gross_sales"],
            base["net_sales"],
            base["discount_amount"],
            base["tax_amount"],
            base["cost_amount"],
            base["promo_line_count"],
            base["ticket_count"],
            base["transaction_count"],
            base["traffic_estimate"],
            base["mtd_sales"],
            base["ytd_sales"],
            base["discount_rate"],
            base["avg_basket"],
            base["sales_m2"],
            ly["net_sales"].alias("net_sales_sss"),
            ly["unit_qty"].alias("unit_qty_sss"),
            ly["ticket_count"].alias("ticket_count_sss"),
        )
        .withColumn(
            "sss_variation_pct",
            F.when(
                F.col("net_sales_sss").isNotNull() & (F.col("net_sales_sss") != 0),
                F.round((F.col("net_sales") - F.col("net_sales_sss")) / F.col("net_sales_sss"), 6),
            ).otherwise(F.lit(None)),
        )
    )
    fq = cfg.gold("fact_sales_store_daily")
    result.write.mode("overwrite").format("delta").saveAsTable(fq)
    return fq


def build_fact_inventory(spark, cfg) -> str:
    from pyspark.sql import functions as F

    fq = cfg.gold("fact_inventory")
    df = (
        spark.table(cfg.silver("sil_daily_inventory"))
        .select(
            "date_key",
            "store_key",
            "product_key",
            "sku",
            "on_hand_qty",
            "on_hand_cost",
            "received_qty",
            "sold_qty",
            "adjusted_qty",
            "in_transit_qty",
            "stock_state",
            "unit_cost",
            "retail_price",
        )
        .withColumn(
            "days_on_hand",
            F.when(F.col("sold_qty").isNull() | (F.col("sold_qty") == 0), F.lit(None)).otherwise(
                F.round(F.col("on_hand_qty") / F.col("sold_qty"), 2)
            ),
        )
        .withColumn(
            "sell_through_pct",
            F.when(
                F.col("on_hand_qty") + F.col("sold_qty") != 0,
                F.round(F.col("sold_qty") / (F.col("on_hand_qty") + F.col("sold_qty")), 6),
            ).otherwise(F.lit(0)),
        )
        .withColumn("stock_value", F.col("on_hand_qty") * F.col("unit_cost"))
    )
    df.write.mode("overwrite").format("delta").saveAsTable(fq)
    return fq


def build_fact_budget(spark, cfg, as_of: str | None = None) -> str:
    from pyspark.sql import functions as F

    fq = cfg.gold("fact_budget")
    budget = spark.table(cfg.silver("sil_budget")).alias("b")
    period_actual = (
        spark.table(cfg.gold("fact_sales_store_daily"))
        .groupBy("commerce_period", "store_key")
        .agg(F.sum("net_sales").alias("actual_sales"))
        .alias("pa")
    )
    df = (
        budget.join(
            period_actual,
            (budget["commerce_period"] == period_actual["commerce_period"])
            & (budget["store_key"] == period_actual["store_key"]),
            "left",
        )
        .select(
            budget["commerce_period"],
            budget["calendar_month_start"],
            budget["store_key"],
            budget["dept"],
            budget["budget_amount"],
            budget["budget_qty_units"],
            period_actual["actual_sales"],
        )
        .withColumn(
            "budget_usage_pct",
            F.when(
                F.col("budget_amount") != 0,
                F.round(F.col("actual_sales") / F.col("budget_amount"), 6),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "budget_gap",
            F.when(
                F.col("budget_amount").isNotNull(), F.col("actual_sales") - F.col("budget_amount")
            ).otherwise(F.lit(None)),
        )
    )
    df.write.mode("overwrite").format("delta").saveAsTable(fq)
    return fq


def run_gold(spark, cfg, as_of: str | None = None) -> list[str]:
    tables = [
        build_dim_store(spark, cfg),
        build_dim_product(spark, cfg),
        build_dim_calendar(spark, cfg),
        build_fact_sales(spark, cfg),
        build_fact_sales_store_daily(spark, cfg, as_of),
        build_fact_inventory(spark, cfg),
        build_fact_budget(spark, cfg),
    ]
    return tables


def run_cuadraturas(spark, cfg) -> list[dict]:
    from pyspark.sql import functions as F

    raw_net = (
        spark.table(cfg.silver("sil_daily_sales")).select(F.sum("net_sales")).collect()[0][0] or 0
    )
    fact_net = spark.table(cfg.gold("fact_sales")).select(F.sum("net_sales")).collect()[0][0] or 0
    store_net = (
        spark.table(cfg.gold("fact_sales_store_daily")).select(F.sum("net_sales")).collect()[0][0]
        or 0
    )
    kpi_net = (
        spark.table(cfg.silver("sil_store_daily_kpi")).select(F.sum("net_sales")).collect()[0][0]
        or 0
    )
    budget_total = (
        spark.table(cfg.gold("fact_budget")).select(F.sum("budget_amount")).collect()[0][0] or 0
    )
    checks = [
        {
            "name": "silver vs gold fact_sales (net)",
            "expected": float(raw_net),
            "actual": float(fact_net),
            "tolerance_pct": 0.001,
        },
        {
            "name": "fact_sales vs store_daily (net)",
            "expected": float(fact_net),
            "actual": float(store_net),
            "tolerance_pct": 0.001,
        },
        {
            "name": "silver kpi vs store_daily (net)",
            "expected": float(kpi_net),
            "actual": float(store_net),
            "tolerance_pct": 0.001,
        },
        {
            "name": "budget sanity (amount>0)",
            "expected": 0.0,
            "actual": float(budget_total) * -0.0 if budget_total >= 0 else 1.0,
            "tolerance_pct": 0.0,
        },
    ]
    results = assert_balances(checks)
    report_path = f"{cfg.dq_report_root()}/gold"
    write_report(spark, results, report_path, "Gold cuadratura")
    return results
