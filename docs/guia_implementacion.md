# Guía de implementación — Pipeline Medallion retail en Databricks

> Documento **as-built**: describe el entorno realmente desplegado y verificado,
> con los pasos para reproducirlo de cero y operarlo. No contiene secretos.

## Control del documento

| Campo | Valor |
|---|---|
| Título | Guía de implementación — Medallion retail (Databricks) |
| Versión | 1.1.0 |
| Estado | Aprobado / verificado en producción (dev y prod en verde) |
| Autor | Ignacio Basulto — Data Engineer |
| Audiencia | Data Engineers, Analytics Engineers, soporte/operaciones |
| Repositorio | `https://github.com/ibasulto/de-proy1-databricks-medallion-architecture` |
| Release | tag `v1.0.0` (pipeline) · `v1.1.0` (documentación) |
| Alcance | Bronze/Silver/Gold + calidad + CI/CD + BI (Databricks SQL y Power BI) |

**Convenciones**

- `<...>` = valor a reemplazar. El **PAT nunca** se escribe en documentos ni en el repo.
- Los comandos asumen PowerShell en Windows (ajusta rutas en Linux/macOS).
- Idioma del contenido: español; se mantienen términos técnicos en inglés.

---

## 1 · Resumen ejecutivo

Pipeline de **arquitectura medallion** (bronze → silver → gold) sobre **Databricks
+ Delta + Unity Catalog**, con PySpark, calidad de datos estilo *Great
Expectations*, cuadraturas entre capas y **CI/CD** completo con **Databricks Asset
Bundles (DAB)** y **GitHub Actions**. Los datos son **100 % sintéticos y
deterministas** (seed fija), por lo que el proyecto se clona y reproduce sin
información de una empresa real.

**Estado actual (as-built):** pipeline desplegado y ejecutado de punta a punta en
`dev` y `prod` vía CI/CD. Todas las expectativas de calidad y las cuadraturas
pasan (`dq_reports/overall` = PASS). Tablas pobladas: 8 bronze, 8 silver, 7 gold.

**Qué demuestra:** modelado dimensional, calendario retail **4-4-5**, indicadores
**SSS/MTD/YTD/MC**, SCD1, calidad y reconciliación de totales, orquestación con
DAB, y automatización CI/CD.

---

## 2 · Arquitectura as-built

```text
 resources/data/raw (local, sintético)        Databricks Workspace (Unity Catalog)
 ────────────────────────────────────         ────────────────────────────────────────
 scripts/generate_data.py  ──►  .csv.gz  ──►  /Volumes/workspace/retail_volumes/raw_landing
                                                        │
                                                        ▼
                                             ┌───────────────────────────────┐
                                             │ BRONZE  workspace.bronze      │  Delta, append/overwrite
                                             │  brz_* + _ingestion_ts/_load_id│  + _source_file (lineage)
                                             └───────────────┬───────────────┘
                                                             ▼
                                             ┌───────────────────────────────┐
                                             │ SILVER  workspace.silver      │  tipado + SCD1 MERGE
                                             │  sil_*  + expectations        │  dq_reports/silver
                                             └───────────────┬───────────────┘
                                                             ▼
                                             ┌───────────────────────────────┐
                                             │ GOLD  workspace.gold          │  star schema + KPI
                                             │  dim_* / fact_* + cuadraturas │  dq_reports/gold
                                             └───────────────┬───────────────┘
                                                             ▼
                                      Databricks SQL dashboard  ·  Power BI Desktop
```

**Decisiones clave**

| Decisión | Motivo |
|---|---|
| Compute **serverless-only** (sin job clusters) | Disponibilidad inmediata y compatible con el workspace; evita administrar clústeres. |
| **Unity Catalog** con catálogo `workspace` | Gobierno, 3-part naming (`catalog.schema.table`) y Volumes para aterrizar archivos. |
| Datos en **Volumes** (`data_location=volume`) | El root de DBFS está deshabilitado en el workspace; los Volumes son el almacenamiento gobernado. |
| **Batch** por defecto (`use_autoloader=false`) | Portabilidad; Auto Loader queda como *upgrade path* documentado. |
| Calidad propio estilo GE + **cuadraturas** | Evidencia machine-readable + Markdown y corte del job si los totales no cuadran. |

---

## 3 · Inventario del entorno (as-built)

**Workspace y catálogo**

| Elemento | Valor |
|---|---|
| Workspace URL | `https://dbc-9b53ae7b-9ca1.cloud.databricks.com` |
| Unity Catalog catalog | `workspace` |
| Schemas | `bronze`, `silver`, `gold` |
| Volume schema / volumes | `retail_volumes` → `raw_landing`, `dq_reports` |
| Ruta del repo en Repos | `/Workspace/Repos/ignacio.basulto@gmail.com/de-proy1-databricks-medallion-architecture` |
| SQL warehouse | `Serverless Starter Warehouse` (id `991197c826186c6f`) |
| HTTP Path (BI) | `/sql/1.0/warehouses/991197c826186c6f` |

**Objetos desplegados**

| Objeto | Nombre | Notas |
|---|---|---|
| Job dev | `medallion_dev_retail` | Prefijo `[dev <usuario>]` por `mode: development` |
| Job prod | `medallion_prod_retail` | `mode: production` con `root_path` explícito |
| Bundle | `de-proy1-databricks-medallion` | Targets `dev` (default) y `prod` |
| Tag de release | `v1.0.0` / `v1.1.0` | Tag `v*` dispara `Deploy - prod` |

**Volúmenes de datos verificados**

| Capa | Tablas | Filas (referencia) |
|---|---|---|
| Bronze | `brz_stores_master`, `brz_products_master`, `brz_calendar_445`, `brz_budget`, `brz_sales_daily_store_sku`, `brz_pos_line_items`, `brz_store_daily_kpi`, `brz_daily_inventory` | — |
| Silver | `sil_store`, `sil_product`, `sil_calendar_445`, `sil_daily_sales`, `sil_pos_line`, `sil_store_daily_kpi`, `sil_daily_inventory`, `sil_budget` | — |
| Gold | `dim_store` (40), `dim_product` (400), `dim_calendar_445` (730), `fact_sales` (3.816.341), `fact_sales_store_daily` (29.200), `fact_inventory` (977.252), `fact_budget` (4.000) | — |

---

## 4 · Prerrequisitos

- Cuenta de **Databricks** con **Unity Catalog** y compute **serverless** (este proyecto corrió en un workspace con las características de *Databricks Free/Starter*: UC + serverless SQL).
- **Personal Access Token (PAT)** con permisos de workspace.
- **Databricks CLI** ≥ v0.2xx (CI usa `databricks/setup-cli`).
- **Python** ≥ 3.11 (local) y el repo clonado.
- **Git** y (para CI/CD) un repositorio en **GitHub** con permisos de *Settings → Secrets*.
- (Opcional BI) **Power BI Desktop** actualizado.

---

## 5 · Puesta en marcha de cero (paso a paso)

### 5.1 · Preparación local

```powershell
git clone https://github.com/ibasulto/de-proy1-databricks-medallion-architecture.git
cd de-proy1-databricks-medallion-architecture

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

Genera el dataset sintético (rápido para smoke, o completo para el volumen):

```powershell
# Smoke (local, segundos)
python scripts/generate_data.py --mini --out resources/data/raw

# Completo (≈40 tiendas, 400 SKU, 2 años)
python scripts/generate_data.py --seed 42 --stores 40 --products 400 --tickets 60
```

Valida localmente (esto es lo que corre el CI):

```powershell
ruff check .
pytest        # 24 tests (calendario 4-4-5, naming, config, generator, quality)
```

> La carpeta `resources/data/raw/` está **gitignored**: los datos no se versionan;
> se generan localmente y se suben al Volume.

### 5.2 · Workspace y CLI

El CLI se autentica por **variables de entorno** (recomendado para no dejar
credenciales en disco). Define en tu sesión:

```powershell
$env:DATABRICKS_HOST  = "https://dbc-9b53ae7b-9ca1.cloud.databricks.com"
$env:DATABRICKS_TOKEN = "<DATABRICKS_TOKEN>"   # PAT; NO se versiona
```

Verifica:

```powershell
databricks auth env          # o: databricks current-user me
```

Clona el repo dentro de Databricks (**Repos / Git folders**) y confirma la ruta
que usarás como `repo_path`:

```text
/Workspace/Repos/ignacio.basulto@gmail.com/de-proy1-databricks-medallion-architecture
```

> En `databricks.yml` la variable `repo_path` ya apunta a esa ruta. Si tu usuario
> difiere, sobreescríbela por target.

### 5.3 · Aterrizar los datos crudos en el Volume

Sube (o copia) el contenido de `resources/data/raw` al Volume `raw_landing`. La
estructura esperada es:

```text
/Volumes/workspace/retail_volumes/raw_landing/
  _manifest.json
  stores/master.csv.gz
  products/master.csv.gz
  calendar/calendar_445.csv.gz
  pos_line_items/pos_line_items_YYYYMMDD.csv.gz
  sales/daily_store_sku/*.csv.gz
  kpi/store_daily/*.csv.gz
  inventory/daily/*.csv.gz
  budget/store_month/store_month_budget.csv.gz
```

Alternativas para subir:

- **UI**: Catalog → `workspace` → `retail_volumes` → volumen `raw_landing` → *Upload*.
- **CLI**:
  ```powershell
  databricks fs cp --recursive "resources/data/raw" "dbfs:/Volumes/workspace/retail_volumes/raw_landing"
  ```

### 5.4 · Desplegar y ejecutar con DAB

```powershell
databricks bundle validate --target dev
databricks bundle deploy   --target dev
databricks bundle run      --target dev jb_medallion
```

El job `jb_medallion` encadena, con dependencias:

```text
setup_environment → bronze_ingestion → silver_transform → gold_build → quality_gate
```

- `setup_environment` (notebook 00) crea catálogo, schemas y Volumes (idempotente).
- `bronze_ingestion` (01) escribe `brz_*` con lineage.
- `silver_transform` (02) tipa, hace SCD1 y corre expectations → `dq_reports/silver`.
- `gold_build` (03) construye dims/facts + KPIs → `dq_reports/gold`.
- `quality_gate` (04) reconcilia totales (cuadraturas) y escribe `dq_reports/overall`.

### 5.5 · Verificación

Conteo de filas por capa (SQL warehouse):

```sql
SELECT 'fact_sales' AS tabla, count(*) AS filas FROM workspace.gold.fact_sales
UNION ALL SELECT 'fact_sales_store_daily', count(*) FROM workspace.gold.fact_sales_store_daily
UNION ALL SELECT 'fact_inventory',          count(*) FROM workspace.gold.fact_inventory
UNION ALL SELECT 'fact_budget',             count(*) FROM workspace.gold.fact_budget;
```

Reporte de calidad (todo debe estar en **PASS**):

```powershell
databricks fs ls  "dbfs:/Volumes/workspace/retail_volumes/dq_reports/overall/dq_report.md/"
databricks fs cat "dbfs:/Volumes/workspace/retail_volumes/dq_reports/overall/dq_report.md/part-00000-....txt"
```

> El writer de texto de Spark crea una **carpeta** `dq_report.md/` con un
> `part-*.txt`. En este workspace `fs download` y las APIs de Volúmenes dan
> 403/404, por lo que la lectura se hace con `fs ls` + `fs cat` (o
> `read_files`/notebook).

Criterios de aceptación:

- [ ] Los 5 tasks del job terminan en `SUCCESS`.
- [ ] `dq_reports/overall/dq_report.md` sin `FAIL`.
- [ ] Las 4 cuadraturas pasan (silver↔gold fact_sales, fact_sales↔store_daily, kpi↔store_daily, budget sanity).

### 5.6 · CI/CD en GitHub Actions

**Workflows** (`.github/workflows/`):

| Workflow | Trigger | Qué hace |
|---|---|---|
| `ci.yml` | push / PR a `main` | `ruff` + `pytest` + smoke del generador |
| `deploy_dev.yml` | push a `main` | `bundle validate/deploy/run --target dev` |
| `deploy_prod.yml` | tag `v*` o manual | `bundle validate/deploy/run --target prod` |

**Secretos** (Settings → Secrets and variables → Actions → *New repository secret*):

| Nombre | Valor | Formato EXACTO |
|---|---|---|
| `DATABRICKS_HOST` | URL del workspace | `https://dbc-9b53ae7b-9ca1.cloud.databricks.com` (con `https://`, sin espacios ni salto de línea) |
| `DATABRICKS_TOKEN` | PAT | `<DATABRICKS_TOKEN>` (solo la cadena `dapi...`, sin `=` ni etiquetas) |

> **Error frecuente:** un `DATABRICKS_HOST` sin `https://` o con espacios produce
> `parse "...": first path segment in URL cannot contain colon` en `bundle
> validate`. Corrige el secreto y relanza.

Las acciones usan `actions/checkout@v7` y `actions/setup-python@v7` (**Node 24**).

---

## 6 · Cómo ver el dashboard y ejecutar SQL

### 6.1 · SQL Editor

1. Entra al workspace: `https://dbc-9b53ae7b-9ca1.cloud.databricks.com`.
2. Menú lateral → **SQL Editor** (o **SQL Warehouses**).
3. Selecciona **Serverless Starter Warehouse** (id `991197c826186c6f`).
4. Pega las queries de `resources/sql/dashboard_queries.sql`, reemplazando:
   - `${catalog}` → `workspace`
   - `${commerce_period}` → un período existente, p. ej. `2026-09-01`
5. Ejecuta. Las 7 consultas cubren: KPIs de la casa, ventas por tienda (SSS/MC/
   MTD/YTD), canal×categoría, estado de inventario, budget vs real, SSS por
   período y tráfico/tickets/canasta.

### 6.2 · Crear un dashboard

1. En el resultado de una query: **+ Add visualization** (o **New dashboard**).
2. Crea tarjetas por query: KPIs, series de tiempo, barras por tienda.
3. **Add to dashboard**, nombra el dashboard y compártelo con rol *Can view*.

### 6.3 · Vía notebook (alternativa a SQL Editor)

```python
spark.sql("SELECT * FROM workspace.gold.fact_sales_store_daily ORDER BY date_key DESC LIMIT 100").display()
```

---

## 7 · Power BI Desktop conectado a Gold

**Datos de conexión**

| Campo | Valor |
|---|---|
| Server Hostname | `dbc-9b53ae7b-9ca1.cloud.databricks.com` |
| HTTP Path | `/sql/1.0/warehouses/991197c826186c6f` |
| Autenticación | Personal Access Token → `<DATABRICKS_TOKEN>` |
| Catálogo / schema | `workspace` / `gold` |
| Tablas | `fact_sales`, `fact_sales_store_daily`, `fact_inventory`, `fact_budget`, `dim_store`, `dim_product`, `dim_calendar_445` |

**Pasos**

1. Abre Power BI Desktop → **Get data**.
2. Busca el conector **Databricks** (en hosts `*.cloud.databricks.com` usa
   *Databricks*; en Azure se llama *Azure Databricks*). **Connect**.
3. Ingresa **Server Hostname** y **HTTP Path**.
4. Elige el **Data Connectivity Mode**:
   - **Import**: rápida para reportes; refresca bajo demanda.
   - **DirectQuery**: recomendado con SQL warehouses cuando necesitas datos en vivo.
5. Autenticación: **Personal Access Token** → pega el PAT.
6. En el **Navigator**, navega `workspace` → `gold` y selecciona las tablas.
7. (Opcional) **Transform Data** para modelar y luego crear medidas DAX.

**Troubleshooting BI**

| Síntoma | Causa | Acción |
|---|---|---|
| No aparece la tabla | Warehouse detenido / sin permiso | Enciende el warehouse y verifica permisos sobre `workspace.gold` |
| Error de driver ADBC/ODBC | Mezcla de drivers en el modelo | En *Advanced Editor* fija `Implementation="2.0"` (ADBC) **o** `default` (ODBC) para **todas** las conexiones del modelo |
| Conexión rechazada | Host/PAT incorrectos | Verifica host con `https://` y que el PAT esté vigente |
| Datos desactualizados (Import) | Falta de refresh | **Refresh** tras cada corrida del pipeline |

> También existe el archivo de apoyo `powerbi_conexion.pbids` (host + HTTP path
> precargados) en la carpeta `Documentacion` de Drive: al abrirlo, Power BI
> Desktop queda listo para pedir el PAT.

---

## 8 · Operación y re-ejecución

**Idempotencia por fuente** (`src/jobs/pipeline.py`):

| Fuente | Modo | Efecto al re-ejecutar |
|---|---|---|
| `stores/master`, `products/master`, `calendar_445`, `budget/store_month` | **overwrite** | Reemplaza (snapshot) |
| `pos_line_items`, `sales/daily_store_sku`, `kpi/store_daily`, `inventory/daily` | **append** | **Duplica** si se re-ejecuta sobre las mismas fechas |

**Regla operativa (CI/CD):** antes de relanzar el job completo (push a `main` o
tag de release), se **eliminan las tablas bronze diarias** para evitar duplicar:

```powershell
foreach ($t in @("brz_sales_daily_store_sku","brz_pos_line_items","brz_store_daily_kpi","brz_daily_inventory")) {
  databricks tables delete "workspace.bronze.$t"
}
```

Silver deduplica (`dropDuplicates`) en el MERGE y Gold hace `overwrite`, por lo que
aun con duplicados en bronze el gate de calidad **se mantiene en PASS**.

**Release a prod (tag):**

```powershell
# 1) dropear bronze diaria (ver regla)
# 2) recrear/crear el tag y pushear (dispara Deploy - prod)
git tag -a v1.1.0 -m "docs: implementation guide + Power BI"
git push origin v1.1.0
```

`prod` reusa el **mismo workspace y tablas** que `dev` (catálogo `workspace`), por
lo que el job prod reconstruye los mismos objetos.

---

## 9 · Troubleshooting (as-built)

Tabla de incidentes reales resueltos durante la puesta en marcha:

| Error | Causa | Solución |
|---|---|---|
| `parse "...": first path segment in URL cannot contain colon` | `DATABRICKS_HOST` mal formado en GitHub | Reemplazar el secreto por `https://<workspace>.cloud.databricks.com` sin espacios |
| `target with 'mode: production' must set 'workspace.root_path'` | `mode: production` exige `root_path` explícito | Agregar `workspace.root_path` en el target `prod` (formato `/Workspace/Users/.../.bundle/...`) |
| `input_file_name is not supported` (UC serverless) | Función no soportada en serverless | Usar `_metadata.file_path` cuando `uc=true` (`src/bronze/ingest.py`) |
| `_jsparkSession` / lectura de `input_file_name` | Acceso a APIs internas de Spark | Helpers serverless-safe en `src/utils/spark_utils.py` (`read_raw` sin `input_file_name`) |
| Tabla silver no existe en el primer MERGE | Destino inexistente | `merge_into` crea el target con esquema consistente (`_with_key` antes del MERGE) |
| `ModuleNotFoundError: utils.quality` | Import relativo desde notebook | Import absoluto en `src/jobs/pipeline.py` |
| `'list' object has no attribute 'collect'` | El reporte DQ ya es lista | Remover el `.collect()` |
| `KeyError`/dict indexado en categoría | Set construido desde dict | `set(_CATEGORIES)` |
| Cuadratura `stock_state` con valor `UNKNOWN` | Dominio generado no incluye `UNKNOWN` | Ajustar la expectativa a `{IN_STOCK, LOW_STOCK, OUT_OF_STOCK}` |
| `store_key` ambiguo en self-join SSS | Columnas homónimas | Alias `cur`/`ly` en el join de `fact_sales_store_daily` |
| Dashboard Q06 "cannot resolve `commerce_week`" | `commerce_week` no está en la fact | Unir `dim_calendar_445` por `date_key` para resolver `commerce_week` |
| No se puede descargar el reporte DQ | `fs download`/API no soportados en Volumes | Leer con `fs ls` + `fs cat` del `part-*.txt` |

---

## 10 · Seguridad y gobernanza

- **Nunca** versionar el PAT: solo en secretos de GitHub y en tu sesión (`DATABRICKS_*`).
- El PAT usado en ejemplos/mayores privilegios debe ser de **mínimo privilegio** y rotarse.
- Los `resources/data/raw/` están **gitignored**; los datos viven en el Volume.
- En producción real, preferir **service principals** (OAuth M2M) en lugar de PAT.
- Gobierno con Unity Catalog: `catalog.schema.table`, Volumes para archivos,
  permisos por schema.

---

## Apéndice A · Cheat sheet de comandos

```powershell
# Local
python -m venv .venv ; .\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
python scripts/generate_data.py --mini
ruff check . ; pytest

# Databricks CLI
databricks auth env
databricks bundle validate --target dev
databricks bundle deploy   --target dev
databricks bundle run      --target dev jb_medallion
databricks bundle destroy  --target dev

# Datos / evidencia
databricks fs cp --recursive resources/data/raw dbfs:/Volumes/workspace/retail_volumes/raw_landing
databricks fs ls  dbfs:/Volumes/workspace/retail_volumes/dq_reports/overall/
databricks fs cat dbfs:/Volumes/workspace/retail_volumes/dq_reports/overall/dq_report.md/part-00000-....txt

# Release
git tag -a v1.1.0 -m "release"
git push origin v1.1.0
```

## Apéndice B · Variables y widgets

| Variable (`databricks.yml`) | Default | Descripción |
|---|---|---|
| `env` | `dev` | Nombre del entorno (`dev`/`prod`) |
| `catalog` | `workspace` | Catálogo de Unity Catalog |
| `bronze_schema` / `silver_schema` / `gold_schema` | `bronze`/`silver`/`gold` | Schemas medallion |
| `volume` | `retail_volumes` | Volume schema |
| `data_location` | `volume` | Dónde aterrizan los crudos (`volume`/`workspace`) |
| `uc` | `true` | Unity Catalog activo |
| `repo_path` | `/Workspace/Repos/ignacio.basulto@gmail.com/de-proy1-databricks-medallion-architecture` | Ruta del repo en Repos |
| `as_of` | `""` | Fecha as-of opcional para Gold (`YYYY-MM-DD`) |

Variables de entorno (`src/utils/config.py`): `DAB_CATALOG`, `DAB_BRONZE_SCHEMA`,
`DAB_SILVER_SCHEMA`, `DAB_GOLD_SCHEMA`, `DAB_VOLUME`, `DAB_RAW_PATH`,
`DATA_LOCATION`, `DAB_ENV`, `DAB_RUN_DATE`, `DAB_REPO_PATH`, `DAB_UC`.

## Apéndice C · Modelo de datos (resumen)

- **Silver** (grain): `sil_store` (tienda), `sil_product` (SKU), `sil_calendar_445`
  (día), `sil_daily_sales` (tienda×SKU×día), `sil_pos_line` (línea), 
  `sil_store_daily_kpi` (tienda×día), `sil_daily_inventory` (tienda×SKU×día),
  `sil_budget` (tienda×depto×mes).
- **Gold**: `dim_store`, `dim_product`, `dim_calendar_445`; hechos `fact_sales`
  (día-tienda-SKU), `fact_sales_store_daily` (día-tienda, MTD/YTD/MC/SSS),
  `fact_inventory` (día-tienda-SKU), `fact_budget` (mes-tienda-depto).
- **Calendario 4-4-5**: `commerce_year/quarter/month/period/week`,
  `date_key_sss` (364 días atrás, mismo día de semana), `date_key_mc` (año anterior).
- **Cuadraturas**: `sum(net_sales)` Silver == `fact_sales` == `fact_sales_store_daily` (tolerancia 0.001 %).

## Apéndice D · Historial de cambios (hitos)

| Commit / Tag | Descripción |
|---|---|
| `4b803ec` | Pipeline medallion completo + DQ, DAB, CI/CD |
| `75953ca` | Job key estático `jb_medallion` |
| `bf9c396` | Workflow serverless-only (sin job cluster), landing en Volume UC (`catalog=workspace`) |
| `2425a3a` … `76475eb` | Fixes serverless: `_metadata.file_path`, helpers sin `input_file_name` |
| `7abf45f` … `b75adc5` | Fixes silver/DQ: create-on-first-run, `dq_threshold`, sets, `stock_state` |
| `d253e80` | Desambiguación `store_key` en self-join SSS |
| `54de586` | GitHub Actions a Node 24 (`checkout@v7`, `setup-python@v7`) |
| `fdea72a` | `root_path` explícito para `prod` |
| `1e675e0` | Fix dashboard Q06 (`commerce_week` vía `dim_calendar_445`) |
| `v1.0.0` | Release del pipeline (dev + prod en verde) |
| `v1.1.0` | Documentación as-built + conexión Power BI |
