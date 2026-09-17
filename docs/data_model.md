# Modelo de datos – retail medallion

Catálogo Unity Catalog `workspace`, esquemas `bronze`, `silver`, `gold` y volume
schema `retail_volumes`. Llaves primarias: `bigint`; dinero: `decimal(18,4)`.

> El detalle de linaje, operación y cuadraturas está en
> [guia_implementacion.md](guia_implementacion.md).

## Calendario comercial 4-4-5 (`dim_calendar_445`)

Cada año comercial arranca el lunes anterior/igual al 1 de enero y se divide en
trimestres de 13 semanas en meses de 4-4-5 semanas (28/28/35 días).

| Columna | Tipo | Ejemplo | Nota |
|---|---|---|---|
| `date_key` | bigint | `20250601` | YYYYMMDD |
| `date_value` | date | `2025-06-01` | |
| `commerce_year` / `quarter` / `month` | int | 2025 / 2 / 6 | |
| `commerce_period` | string | `2025-06` | mes comercial |
| `commerce_week` | int | 24 | 1..53 del año comercial |
| `quarter_week` | int | 11 | 1..13 dentro del trimestre |
| `date_key_sss` | bigint | `20240603` | **SSS**: misma fecha 364 días atrás (mismo día de semana) |
| `date_key_mc` | bigint | `20240601` | **MC (month close)**: mismo día del año anterior |

## Tablas Silver

| Tabla | Grain | Llaves | Notas |
|---|---|---|---|
| `sil_store` | 1 fila/tienda | `store_key` | chain_format ∈ {hyper, super, express} |
| `sil_product` | 1 fila/SKU | `product_key` | category ∈ 10 categorías, EAN 13 |
| `sil_calendar_445` | 1 fila/día | `date_key` | calendario 4-4-5 |
| `sil_daily_sales` | tienda×producto×día | `date_key, store_key, product_key` | agregado de POS |
| `sil_pos_line` | 1 fila/línea de ticket | `txn_id, line_id` | transacciones a nivel línea |
| `sil_store_daily_kpi` | tienda×día | `date_key, store_key` | tickets, tráfico, canasta, m2 |
| `sil_daily_inventory` | tienda×producto×día | `date_key, store_key, product_key` | on_hand, sold, stock_state |
| `sil_budget` | tienda×depto×mes comercial | `commerce_period, store_key, dept` | plan de mercadería |

## Tablas Gold (star schema)

```text
dim_store ───────┬──────────────┬──────────────┬──────────────┐
dim_product ─────┼── fact_sales ┼── fact_inventory             │
dim_calendar_445 ┴── fact_sales_store_daily ──── fact_budget ──┤
```

- `dim_store`, `dim_product`, `dim_calendar_445`: dimensiones limpias.
- `fact_sales` (grain day-store-sku): `unit_qty`, `gross_sales`, `net_sales`,
  `discount_amount`, `tax_amount`, `cost_amount`, `margin_amount`,
  `margin_pct`, más `ticket_count`, `line_count`, `promo_line_count`.
- `fact_sales_store_daily` (grain day-store): `net_sales`, `mtd_sales`,
  `ytd_sales`, `net_sales_sss`, `sss_variation_pct`, `discount_rate`,
  `avg_basket`, `sales_m2`, tickets/tráfico.
- `fact_inventory` (grain day-store-sku): `on_hand_qty`, `days_on_hand`,
  `stock_value`, `sell_through_pct`, `stock_state`.
- `fact_budget` (grain month-store-dept): `budget_amount`, `actual_sales`,
  `budget_usage_pct`, `budget_gap`.

## Convenciones de calidad

- **Money**: `decimal(18,4)` en Silver (los raw vienen como string con 2
  decimales, estilo un extracto de BD).
- **Expectations por entidad** (suite en `src/silver/transform.py::define_silver`):
  no nulos en llaves, rangos, sets de valores permitidos, `net_sales >= 0`.
- **Cuadraturas** (Gold): `sum(net_sales)` de Silver == `fact_sales` ==
  `fact_sales_store_daily` (tolerancia 0.001 %).