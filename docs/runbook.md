# Runbook – poner el pipeline en Databricks (Community Edition)

## 0 · Requisitos

- Cuenta Databricks (Community Edition o trial) `https://community.cloud.databricks.com`.
- Databricks CLI instalado y autenticado:
  ```bash
  databricks configure --token   # workspace URL + PAT
  ```
- Python 3.11+ (local) con el repo clonado.

## 1 · Generar los datos sintéticos

```bash
# Repo root
python scripts/generate_data.py --seed 42 --stores 40 --products 400 --tickets 60
# CSVs.gz en resources/data/raw/ ...
```

**Community Edition**: crea el catálogo/schemas y súbelos al workspace
(Warehouse → SQL Editor puede crear objetos; para Volumes se requiere UC, que CE
no soporta → usa `data_location=workspace`).

1. En el notebook `00_setup` deja `data_location=workspace` y ejecútalo para
   crear `bronze`/`silver`/`gold`.
2. Sube `resources/data/raw` mediante **Repos** (Clone repo) y copia los CSVgz a
   un path de workspace: `dbutils.fs.cp("file:/.../resources/data/raw",
   "file:/Workspace/Shared/raw_landing")` o arrastrando archivos en la UI.
3. En `01_bronze_ingest` usa el widget `repo_path` con la ruta de tu clone en
   Repos.
4. Ejecuta los notebooks 01 → 02 → 03 → 04 en orden (o crea el job `jb_medallion`).

> En plan **Premium/AWS/Azure/GCP**: hay Volumes y DLT → cambia
> `data_location=volume`, sube los archivos con `dbutils.fs.cp` a
> `/Volumes/<catalog>/retail_volumes/raw_landing`, y opcionalmente activa
> Auto Loader (`use_autoloader=true`) o la pipeline DLT.

## 2 · DAB (Databricks Asset Bundles)

```bash
databricks bundle validate --target dev        # compila y valida
databricks bundle deploy --target dev          # crea el job medallion_dev_retail
databricks bundle run --target dev jb_medallion   # ejecuta el pipeline completo
databricks bundle destroy --target dev         # borra
```

Variables que puedes sobreescribir por target (`databricks.yml`):
`catalog`, `*_schema`, `volume`, `data_location`, `repo_path`, `as_of`.

## 3 · CI/CD (GitHub Actions)

| Workflow | Trigger | Hace |
|---|---|---|
| `ci.yml` | push/PR a main | lint (ruff) + pytest + smoke del generador |
| `deploy_dev.yml` | push a main | `bundle deploy/run --target dev` |
| `deploy_prod.yml` | tag `v*` o manual | validation + deploy/run `--target prod` |

Secrets requeridos en el repo: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`.

## 4 · Databricks SQL dashboard

1. Warehouse → SQL Editor → pega `resources/sql/dashboard_queries.sql`
   ajustando `${catalog}` → `retail_lakehouse` (o tu catálogo).
2. Crea los visuales (KPI cards, time series, barras por store).
3. Comparte con "viewer" para la demo.

## 5 · Evidencia de calidad

Cada corrida persiste en el workspace/volume:
`dq_reports/silver|gold|overall/dq_results.json` (machine-readable) y
`dq_report.md` (lindo para el portafolio).
Cuadraturas: si `sum(net_sales)` difiere entre capas, el job `04_quality_checks`
falla y GitHub Actions lo marca en rojo.