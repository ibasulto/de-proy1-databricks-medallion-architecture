# Runbook – poner el pipeline en Databricks

> Guía completa as-built: [guia_implementacion.md](guia_implementacion.md).
> Este runbook es el resumen operativo.

## 0 · Requisitos

- Workspace Databricks con **Unity Catalog** y compute **serverless**.
- **Personal Access Token (PAT)**.
- **Databricks CLI** (CI usa `databricks/setup-cli`).
- Python ≥ 3.11 y el repo clonado.

## 1 · Autenticación (variables de entorno)

No se dejan credenciales en disco; el CLI lee el entorno:

```powershell
$env:DATABRICKS_HOST  = "https://dbc-9b53ae7b-9ca1.cloud.databricks.com"
$env:DATABRICKS_TOKEN = "<DATABRICKS_TOKEN>"
databricks auth env
```

## 2 · Datos sintéticos y aterrizaje

```powershell
python -m venv .venv ; .\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
python scripts/generate_data.py --seed 42 --stores 40 --products 400 --tickets 60

# Subir al Volume gobernado (UC)
databricks fs cp --recursive resources/data/raw dbfs:/Volumes/workspace/retail_volumes/raw_landing
```

Estructura esperada en `raw_landing/`: `_manifest.json`, `stores/`, `products/`,
`calendar/`, `pos_line_items/`, `sales/`, `kpi/`, `inventory/`, `budget/`.

## 3 · DAB (Databricks Asset Bundles)

```powershell
databricks bundle validate --target dev
databricks bundle deploy   --target dev
databricks bundle run      --target dev jb_medallion
databricks bundle destroy  --target dev
```

Variables sobreescribibles por target (`databricks.yml`): `catalog`, `*_schema`,
`volume`, `data_location`, `uc`, `repo_path`, `as_of`.

El job encadena: `setup_environment → bronze_ingestion → silver_transform →
gold_build → quality_gate`.

## 4 · CI/CD (GitHub Actions)

| Workflow | Trigger | Hace |
|---|---|---|
| `ci.yml` | push/PR a `main` | lint (ruff) + pytest + smoke del generador |
| `deploy_dev.yml` | push a `main` | `bundle deploy/run --target dev` |
| `deploy_prod.yml` | tag `v*` o manual | validate + deploy/run `--target prod` |

Secrets requeridos: `DATABRICKS_HOST` (`https://<workspace>.cloud.databricks.com`)
y `DATABRICKS_TOKEN` (PAT).

**Antes de relanzar el job completo** (bronze diario es `append`), dropear las
tablas bronze diarias para no duplicar:

```powershell
foreach ($t in @("brz_sales_daily_store_sku","brz_pos_line_items","brz_store_daily_kpi","brz_daily_inventory")) {
  databricks tables delete "workspace.bronze.$t"
}
```

## 5 · Databricks SQL dashboard

1. **SQL Editor** → warehouse `Serverless Starter Warehouse`.
2. Pega `resources/sql/dashboard_queries.sql` reemplazando `${catalog}` →
   `workspace` y `${commerce_period}` → p. ej. `2026-09-01`.
3. Crea visuales y arma el dashboard; comparte con *Can view*.

## 6 · Power BI Desktop (opcional)

Server Hostname `dbc-9b53ae7b-9ca1.cloud.databricks.com`,
HTTP Path `/sql/1.0/warehouses/991197c826186c6f`, auth **Personal Access Token**.
Detalle y troubleshooting en [guia_implementacion.md](guia_implementacion.md) §7.

## 7 · Evidencia de calidad

Cada corrida persiste en el Volume:
`dq_reports/{silver,gold,overall}/dq_results.json` (machine-readable) y
`dq_report.md` (reporte). Las cuadraturas de Gold deben reconciliar `net_sales`
entre Silver y Gold; si difieren, `quality_gate` falla y el CI queda en rojo.

```powershell
databricks fs ls  dbfs:/Volumes/workspace/retail_volumes/dq_reports/overall/dq_report.md/
databricks fs cat dbfs:/Volumes/workspace/retail_volumes/dq_reports/overall/dq_report.md/part-00000-....txt
```
