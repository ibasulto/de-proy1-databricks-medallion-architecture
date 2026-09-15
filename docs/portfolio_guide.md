# Guía de portafolio – cómo presentar este proyecto

## Pitch de 30 segundos

> "Proyecto de ingeniería de datos con **arquitectura medallion en Databricks**:
> ingestión (Bronze) → limpieza y calidad (Silver) → analítica (Gold) sobre
> datos de retail **100 % sintéticos y deterministas**, con calendario comercial
> 4-4-5, indicadores SSS/MTD/YTD, calidad tipo Great Expectations y **CI/CD
> completo con Databricks Asset Bundles y GitHub Actions**."

## Lo que demuestro (mapa entrevista)

| Tema | Dónde está la evidencia |
|---|---|
| **Data modeling** | `docs/data_model.md` – calendar 4-4-5, grains, SCD1 |
| **Medallion + Delta** | `src/bronze`, `src/silver`, `src/gold`; tablas `brz_*`/`sil_*`/`dim_*`/`fact_*` |
| **PySpark** | merges SCD1, window functions (MTD/YTD), conformed keys |
| **Data quality** | `src/utils/quality.py` + suites por entidad + cuadraturas → reportes en el Volume |
| **Orquestación** | `databricks.yml` (DAB): job con dependencias, targets dev/prod |
| **CI/CD** | `.github/workflows/{ci,deploy_dev,deploy_prod}.yml` |
| **Dashboard** | `resources/sql/dashboard_queries.sql` sobre Gold |
| **Tests** | 23 unit tests (calendario, naming, generador, config, balances) |
| **Scalabilidad** | Auto Loader (`use_autoloader`) y spec DLT (`pipeline_specs/`) para Premium |

## Reproducible / honesto

- Los datos son **sintéticos** (generador `src/data_generation/generator.py`,
  seed fija) → cualquiera puede clonar, generar y correr.
- Nombres genéricos (Implementos del Hogar, Store 0001) → sin acusaciones de
  usar datos reales de un retailer.
- El proyecto referencia (internamente) el proyecto anterior Cognos→Fabric del
  usuario; el vocabulario (DATE_KEY, 4-4-5, SSS/MC) viene de ahí.

## Script de la demo

1. Muestra el repo: estructura + DAB.
2. Corre `python scripts/generate_data.py --mini` local (rápido) y muestra el
   manifest + un CSV.
3. Repo → Databricks: `databricks bundle deploy/run jb_medallion_dev`.
4. Abre el job: dependencias setup → bronze → silver → gold → quality.
5. En `dq_reports/overall/dq_report.md`: PASS en todas las checks.
6. Dashboard SQL: KPI totales, ventas por store, SSS por periodo, inventory y
   budget vs actual.

## Posibles preguntas y respuestas

**¿Por qué calendario 4-4-5?** Porque en retail los periodos deben tener semanas
completas; al comparar periodos se evita el efecto "días hábiles perdidos". SSS
= misma fecha 364 días antes (mismo día de semana) → compare apples-to-apples.

**¿SCD1 o SCD2?** SCD1 (sobreescribir) porque vendemos/dimensionamos masters
chicos y el interés está en el estado actual; comentamos que un histórico de
masters usaría SCD2.

**¿Por qué no DLT?** Community Edition no lo incluye; diseño el pipeline batch
portable y dejo la spec DLT lista para Premium (mismo modelo de datos).

**¿Cómo escala la ingestión?** Los jobs son idempotentes (snapshot vs append);
con `use_autoloader=true` Delta Auto Loader entrega incremental y disponible.