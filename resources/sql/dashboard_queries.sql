-- Databricks SQL dashboard queries (gold layer)
-- Catalog/schema are templated for the targeted env: use
--   {catalog}.gold.{table}  (bundle spark_conf injects catalog)

-- 01 · KPI totales de la casa (nivel company)
SELECT
    fact.date_key,
    sum(fact.net_sales)                AS net_sales,
    sum(fact.gross_sales)              AS gross_sales,
    sum(fact.discount_amount)          AS discount_amount,
    sum(fact.unit_qty)                 AS unit_qty,
    sum(fact.tax_amount)               AS tax_amount,
    round(sum(fact.cost_amount), 2)    AS cost_amount,
    round(sum(fact.net_sales - fact.cost_amount), 2) AS margin_amount,
    count(DISTINCT fact.store_key)     AS stores_active
FROM ${catalog}.gold.fact_sales_store_daily AS fact
GROUP BY fact.date_key
ORDER BY fact.date_key DESC
LIMIT 60;

-- 02 · Ventas comparadas por store (SSS vs MC vs MTD/YTD)
SELECT
    fact.store_key,
    dt.name                                   AS store_name,
    fact.date_key,
    fact.net_sales,
    fact.net_sales_sss,
    fact.sss_variation_pct,
    fact.mtd_sales,
    fact.ytd_sales,
    fact.avg_basket,
    fact.sales_m2
FROM ${catalog}.gold.fact_sales_store_daily AS fact
JOIN ${catalog}.gold.dim_store AS dt
    ON fact.store_key = dt.store_key
WHERE fact.commerce_period = '${commerce_period}'
ORDER BY fact.net_sales DESC;

-- 03 · Canal x categoria (fact_sales, grain day-store-sku)
SELECT
    fact.date_key,
    dp.category,
    dp.dept,
    sum(fact.unit_qty)      AS qty,
    sum(fact.net_sales)     AS net_sales,
    sum(fact.margin_amount) AS margin
FROM ${catalog}.gold.fact_sales AS fact
JOIN ${catalog}.gold.dim_product AS dp
    ON fact.product_key = dp.product_key
GROUP BY fact.date_key, dp.category, dp.dept
ORDER BY net_sales DESC;

-- 04 · Estado de inventario (coverage / stock-out)
SELECT
    fact.date_key,
    fact.store_key,
    fact.stock_state,
    count(*)                        AS products,
    sum(fact.on_hand_qty)           AS units_on_hand,
    sum(fact.stock_value)           AS stock_value,
    round(avg(fact.days_on_hand), 1) AS avg_days_on_hand
FROM ${catalog}.gold.fact_inventory AS fact
WHERE fact.date_key = (SELECT max(date_key) FROM ${catalog}.gold.fact_inventory)
GROUP BY fact.date_key, fact.store_key, fact.stock_state
ORDER BY fact.store_key, fact.stock_state;

-- 05 · Budget vs real (plan de mercadería por depto)
SELECT
    bud.commerce_period,
    bud.store_key,
    bud.dept,
    round(sum(bud.budget_amount), 0) AS budget,
    round(sum(bud.actual_sales), 0)  AS actual,
    round(sum(bud.budget_gap), 0)    AS gap,
    round(sum(bud.budget_usage_pct) * 100, 1) AS usage_pct
FROM ${catalog}.gold.fact_budget AS bud
GROUP BY bud.commerce_period, bud.store_key, bud.dept
ORDER BY bud.commerce_period, gap;

-- 06 · SSS por periodo (var. % vs la misma semana anio anterior)
SELECT
    cal.commerce_period,
    cal.commerce_week,
    round(sum(fact.net_sales), 2)         AS net_sales,
    round(sum(fact.net_sales_sss), 2)     AS net_sales_sss,
    round((sum(fact.net_sales) - sum(fact.net_sales_sss)) / sum(fact.net_sales_sss) * 100, 2) AS sss_var_pct
FROM ${catalog}.gold.fact_sales_store_daily AS fact
JOIN ${catalog}.gold.dim_calendar_445 AS cal
    ON fact.date_key = cal.date_key
GROUP BY cal.commerce_period, cal.commerce_week
ORDER BY cal.commerce_period, cal.commerce_week;

-- 07 · Trafico, tickets y canasta (comercio / store)
SELECT
    fact.store_key,
    fact.commerce_period,
    sum(fact.ticket_count)      AS tickets,
    sum(fact.transaction_count) AS transactions,
    round(sum(fact.net_sales) / sum(fact.ticket_count), 2) AS avg_basket,
    round(sum(fact.traffic_estimate), 0) AS traffic
FROM ${catalog}.gold.fact_sales_store_daily AS fact
GROUP BY fact.store_key, fact.commerce_period
ORDER BY fact.commerce_period, avg_basket DESC;