# Arquitectura – Medallion retail en Databricks

> Para el detalle as-built (entorno desplegado, operación y BI) ver
> [guia_implementacion.md](guia_implementacion.md).

Modelo de datos en 3 capas (bronze / silver / gold) con **Unity Catalog**, tablas
**Delta**, calidad tipo "expectations" y CI/CD con **Databricks Asset Bundles** +
GitHub Actions. El lenguaje de negocio es **retail chileno/generalista**: POS,
inventario diario, presupuesto por depto y un calendario comercial **4-4-5**.

Entorno de referencia: catálogo Unity Catalog **`workspace`**, schemas
`bronze`/`silver`/`gold`, volume schema `retail_volumes` y compute
**serverless-only**.

```text
 Datos (git, no produccions)                     Databricks Workspace
 ──────────────────────────────                  ──────────────────────────
 resources/data/raw (Volumes)
   │  upload_ui / verbatim / dbutils.fs          catalog workspace
   ▼                                            ┌──────────────────────────┐
 /Volumes/workspace/retail_volumes/              │ raw_landing/ (Volume)    │
   stores master, products master, calendar       │ pos_line_items/ ...     │
   pos_line_items_YYYYMMDD.csv.gz                 └──────────┬───────────────┘
   sales/daily_store_sku/*, kpi/store_daily/*                 ▼
   inventory/daily/*, budget/store_month/*       ┌──────────────────────────┐
                                                  │ bronze  brz_*  (Delta)   │
 notebooks 01..04 (DAB notebook_task)             │  + _ingestion_ts/_load_id│
   │                                               │  + _source_file          │
   ▼                                               └──────────┬───────────────┘
                                                              ▼
                                               ┌──────────────────────────┐
                                               │ silver sil_* (SCD1 MERGE) │
                                               │  clean + types + DQ       │
                                               │  dq_reports/ (Volume)     │
                                               └──────────┬───────────────┘
                                                          ▼
                                               ┌──────────────────────────┐
                                               │ gold  dim_* / fact_*     │
                                               │  star schema + cuadratura│
                                               └──────────┬───────────────┘
                                                          ▼
                                        Databricks SQL dashboards (retail KPIs)
```

## Capas

| Capa | Objetivo | Reglas | Salidas |
|---|---|---|---|
| **Bronze** | Copia inmutable de la fuente | Sin lógica de negocio; trazabilidad por fila; snapshot (masters/budget) o append (daily/transacciones) | `brz_*` |
| **Silver** | Entidad confiable y tipada | `decimal(18,4)` para dinero, `bigint` para llaves, SCD1 MERGE, expectations que **fallan o filtran** | `sil_store`, `sil_product`, `sil_calendar_445`, `sil_daily_sales`, `sil_pos_line`, `sil_store_daily_kpi`, `sil_daily_inventory`, `sil_budget` |
| **Gold** | Analítica lista para dashboard | Star schema conformado + indicadores (MTD/YTD/MC/SSS) + cuadratura de totales | `dim_store`, `dim_product`, `dim_calendar_445`, `fact_sales`, `fact_sales_store_daily`, `fact_inventory`, `fact_budget` |

## Decisiones

- **Serverless-only + Unity Catalog**: el bundle no define job clusters; las
  tareas corren en compute serverless y los datos viven en Volumes UC
  (`data_location=volume`, `uc=true`). El spec DLT
  (`pipeline_specs/dlt_medallion.py`) queda como *upgrade path*.
- **Autoloader** soportado como opción (`use_autoloader=true`) pero el default
  es batch (portable y sin checkpoints obligatorios).
- **Calidad** = expectations propias estilo Great Expectations: cada entity
  corre su suite y la evidencia (JSON + Markdown) se persiste en el Volume.
- **Cuadratura**: los totales de Silver deben replicarse en Gold y el KPI store
  diario; el quality gate (notebook 04 / CI) falla si difieren > tolerancia.
- **Naming** (`src/utils/naming.py`): `brz_*`, `sil_*`, `dim_*`, `fact_*`;
  jobs `jb_*`; volumen `retail_volumes`.

## Componentes en el repo

```
databricks.yml                 # DAB: targets dev/prod, job jb_medallion
notebooks/                     # 00 setup · 01 bronze · 02 silver · 03 gold · 04 quality
src/                           # código Python compartido (tests/lint local)
  utils/{config,naming,calendar_445,spark_utils,quality,environment}.py
  data_generation/generator.py # datos sintéticos (std lib, determinista)
  bronze/ silver/ gold/        # transformaciones medallion
  jobs/pipeline.py             # orquestación de los notebooks
scripts/generate_data.py       # CLI del generador (--mini para smoke tests)
tests/                         # pytest + ruff corren en CI
resources/sql/dashboard_queries.sql
pipeline_specs/dlt_medallion.py
```

## Flujo de datos

1. **Generación**: `python scripts/generate_data.py` produce los CSV.gz en
   `resources/data/raw` (local) → se suben al Volume (ver runbook).
2. **Bronze**: lee todos los archivos de cada `source_dir` y escribe
   `brz_<source>` con lineage.
3. **Silver**: limpia, tipa y hace SCD1 (`MERGE`), aplica expectations y guarda
   el reporte en `dq_reports/silver`.
4. **Gold**: construye dims/facts, calcula retail KPIs (SSS a 364 días, MC al
   año previo) y valida cuadraturas.
5. **Dashboard**: Databricks SQL sobre Gold (`resources/sql/dashboard_queries.sql`).