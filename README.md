# de-proy1-databricks-medallion-architecture

Proyecto **portafolio de Data Engineering**: pipeline completo de retail con
**arquitectura metálica (bronze/silver/gold)** sobre **Databricks + Delta**,
PySpark, integración de calidad de datos tipo *Great Expectations*, datos
sintéticos deterministas y **CI/CD** con Databricks Asset Bundles (DAB) y GitHub
Actions.

> Datos 100 % sintéticos (seed fija) para que el proyecto se pueda clonar y
> reproducir sin enmascarar información de una empresa real.

## Stack

| Capa | Tecnología |
|---|---|
| Orquestación | Databricks Workflows + DAB (`databricks.yml`) |
| Datos | Delta tables en Unity Catalog, esquemas `bronze`/`silver`/`gold` |
| Procesamiento | PySpark, SCD1 MERGE, window functions, calendario retail 4-4-5 |
| Calidad | Expectations propias + cuadraturas → reportes JSON/Markdown en el workspace |
| CI/CD | GitHub Actions (`ci`, `deploy_dev`, `deploy_prod`) |
| BI | Databricks SQL (queries en `resources/sql`) |

## Esquema

```text
Bronze (brz_*)  →  Silver (sil_*)  →  Gold (dim_* / fact_*)
  ingestión         limpieza+DQ       star schema + KPI
```

- **Bronze**: copia inmutable de la fuente con lineage (`_ingestion_ts`,
  `_load_id`, `_source_file`).
- **Silver**: tipado (`decimal(18,4)`, `bigint`), SCD1, suites de expectations.
- **Gold**: `fact_sales`, `fact_sales_store_daily` (MTD/YTD/MC/**SSS**),
  `fact_inventory`, `fact_budget`, y dimensiones de calendario 4-4-5.

## Quickstart local

```bash
python -m venv .venv && .venv\Scripts\activate   # or: uv venv
pip install -e ".[dev]"

python scripts/generate_data.py --mini           # dataset pequeño (smoke)
ruff check .
pytest
```

Genera el set completo (≈40 tiendas, 400 SKU, 2 años):

```bash
python scripts/generate_data.py --seed 42 --stores 40 --products 400 --tickets 60
```

## Despliegue en Databricks

Ver **[docs/runbook.md](docs/runbook.md)** paso a paso. En resumen:

```bash
databricks configure --token                 # workspace URL + PAT
databricks bundle validate --target dev
databricks bundle deploy  --target dev
databricks bundle run     --target dev jb_medallion
```

CI/CD listo: en cada push a `main` corre lint + tests (CI) y deploy a dev; un
tag `v*` despliega a prod. Secrets en GitHub: `DATABRICKS_HOST`,
`DATABRICKS_TOKEN`.

## Documentación

- [Arquitectura](docs/architecture.md)
- [Modelo de datos](docs/data_model.md)
- [Runbook](docs/runbook.md)
- [Guía de portafolio](docs/portfolio_guide.md)

## Notas

- El calendario **4-4-5** y los indicadores SSS/MC replican el vocabulario del
  proyecto retail anterior del autor (Cognos → Fabric).
- La pipeline usa batch por defecto (compatible con Community Edition);
  `pipeline_specs/dlt_medallion.py` ofrece el "upgrade path" a DLT en Premium.