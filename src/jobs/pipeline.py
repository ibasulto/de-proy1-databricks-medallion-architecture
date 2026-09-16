"""End-to-end pipeline orchestrator shared by DAB notebook tasks and CI.

Keeps the notebooks thin: each notebook resolves a ``Cfg`` from widgets/env and
calls one of these entry points.
"""

from __future__ import annotations

from utils import environment


def run_bronze(spark, cfg, use_autoloader: bool = False) -> list[str]:
    from bronze import ingest

    sources = [
        ("stores_master", "stores/master.csv.gz", "csv", True, False),  # full refresh (file) -> snapshot
        ("products_master", "products/master.csv.gz", "csv", True, False),
        ("calendar_445", "calendar/calendar_445.csv.gz", "csv", True, False),
        ("sales_daily_store_sku", "sales/daily_store_sku", "csv", False, True),  # daily files -> append
        ("pos_line_items", "pos_line_items", "csv", False, True),
        ("store_daily_kpi", "kpi/store_daily", "csv", False, True),
        ("daily_inventory", "inventory/daily", "csv", False, True),
        ("budget", "budget/store_month/store_month_budget.csv.gz", "csv", True, False),
    ]
    loaded = []
    for table, source_dir, fmt, snapshot, is_dir in sources:
        mode = "overwrite" if snapshot else "append"
        extra_files = "*" if is_dir else None
        if use_autoloader:
            cp = f"{cfg.dq_report_root()}/checkpoints"
            ingest.auto_loader_bronze(spark, cfg, table, source_dir, cp)
        else:
            ingest.load_bronze(spark, cfg, table, source_dir, fmt=fmt, mode=mode, extra_files=extra_files)
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
    path = f"{cfg.dq_report_root()}/overall"
    write_report(spark, checks, path, "Overall data quality")
    return {
        "passed": verdict,
        "silver_checks": len(silver_results),
        "gold_tables": gold_tables,
        "reconciliations": reconciliations,
    }


def run_setup(spark, cfg) -> list[str]:
    return environment.ensure_environment(spark, cfg)
