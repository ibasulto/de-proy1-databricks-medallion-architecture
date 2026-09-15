import csv
import gzip
from datetime import date
from pathlib import Path

from data_generation.generator import GenParams, generate


def _read_gz(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _sum(paths: list[Path], field: str) -> float:
    return round(sum(float(row[field]) for p in paths for row in _read_gz(p)), 2)


def _sample_params(tmp_path: Path) -> GenParams:
    return GenParams(
        seed=11,
        n_stores=3,
        n_products=20,
        start=date(2025, 6, 1),
        end=date(2025, 6, 3),
        inv_days=3,
        tickets_per_day_floor=8,
    )


def test_generates_all_expected_roots(tmp_path: Path):
    generate(_sample_params(tmp_path), tmp_path / "raw")
    assert (tmp_path / "raw" / "stores" / "master.csv.gz").exists()
    assert (tmp_path / "raw" / "products" / "master.csv.gz").exists()
    assert (tmp_path / "raw" / "calendar" / "calendar_445.csv.gz").exists()
    assert (tmp_path / "raw" / "pos_line_items").exists()
    assert (tmp_path / "raw" / "sales" / "daily_store_sku").exists()
    assert (tmp_path / "raw" / "kpi" / "store_daily").exists()
    assert (tmp_path / "raw" / "inventory" / "daily").exists()
    assert (tmp_path / "raw" / "budget" / "store_month").exists()
    assert (tmp_path / "raw" / "_manifest.json").exists()


def test_sales_net_matches_pos_and_kpi(tmp_path: Path):
    raw = tmp_path / "raw"
    generate(_sample_params(tmp_path), raw)
    pos = _sum(sorted((raw / "pos_line_items").glob("*.csv.gz")), "net_amount")
    kpi = _sum(sorted((raw / "kpi" / "store_daily").glob("*.csv.gz")), "net_sales")
    sales = _sum(sorted((raw / "sales" / "daily_store_sku").glob("*.csv.gz")), "net_sales")
    assert abs(pos - kpi) < 2.0
    assert abs(pos - sales) < 2.0


def test_deterministic_with_seed(tmp_path: Path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    p1 = _sample_params(tmp_path)
    p2 = _sample_params(tmp_path)
    generate(p1, a)
    generate(p2, b)
    fa = sorted((a / "sales" / "daily_store_sku").glob("*.csv.gz"))
    fb = sorted((b / "sales" / "daily_store_sku").glob("*.csv.gz"))
    assert [f.name for f in fa] == [f.name for f in fb]
    assert _sum(fa, "net_sales") == _sum(fb, "net_sales")


def test_calendar_has_445_columns(tmp_path: Path):
    raw = tmp_path / "raw"
    generate(_sample_params(tmp_path), raw)
    rows = _read_gz(raw / "calendar" / "calendar_445.csv.gz")
    assert rows and "date_key_sss" in rows[0]
    assert rows and "commerce_period" in rows[0]
