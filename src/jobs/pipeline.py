"""End-to-end pipeline orchestrator shared by DAB notebook tasks and CI.

Keeps the notebooks thin: each notebook resolves a ``Cfg`` from widgets/env and
calls one of these entry points.
"""

from __future__ import annotations

from utils import environment


def run_bronze(spark, cfg, use_autoloader: bool = False) -> list[str]:
    from bronze import ingest

    sources = [
        ("stores_master", "stores/master", "csv", True),  # full refresh (full files) -> snapshot
        ("products_master", "products/master", "csv", True),
        ("calendar_445", "calendar/calendar_445", "csv", True),
        ("sales_daily_store_sku", "sales/daily_store_sku", "csv", False),  # daily files -> append
        ("pos_line_items", "pos_line_items", "csv", False),
        ("store_daily_kpi", "kpi/store_daily", "csv", False),
        ("daily_inventory", "inventory/daily", "csv", False),
        ("budget", "budget/store_month", "csv", True),
    ]
    loaded = []
    for table, source_dir, fmt, snapshot in sources:
        mode = "overwrite" if snapshot else "append"
        if use_autoloader:
            cp = f"/Volumes/{cfg.catalog}/{cfg.volume}/checkpoints"
            ingest.auto_loader_bronze(spark, cfg, table, source_dir, cp)
        else:
            ingest.load_bronze(spark, cfg, table, source_dir, fmt=fmt, mode=mode)
        loaded.append(f"{cfg.bronze(table)} <- {source_dir}")
    return loaded


def run_silver(spark, cfg) -> list[dict]:
    from silver import transform

    return transform.run_silver(spark, cfg)


def run_gold(spark, cfg, as_of: str | None = None) -> list[str]:
    from gold import star

    tables = star.run_gold(spark, cfg, as_of)
    return tables


def run_cuadraturas(spark, cfg) -> list[dict]:
    from gold import star

    return star.run_cuadraturas(spark, cfg)


def run_quality(spark, cfg) -> dict:
    """Run all expectations + cuadraturas and return a single verdict."""
    from ..utils.quality import write_report

    silver_results = run_silver(spark, cfg)
    gold_tables = run_gold(spark, cfg)
    reconciliations = run_cuadraturas(spark, cfg)

    checks = list(silver_results) + reconciliations
    verdict = all(r.get("passed", False) for r in checks)
    path = f"/Volumes/{cfg.catalog}/{cfg.volume}/dq_reports/overall"
    write_report(spark, checks, path, "Overall data quality")
    return {
        "passed": verdict,
        "silver_checks": len(silver_results),
        "gold_tables": gold_tables,
        "reconciliations": reconciliations,
    }


def run_setup(spark, cfg) -> list[str]:
    return environment.ensure_environment(spark, cfg)
