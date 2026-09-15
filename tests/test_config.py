import os

from utils.config import Cfg, load


def test_defaults_ce_two_part():
    cfg = load()
    assert cfg.catalog == "retail_lakehouse"
    assert cfg.silver("sil_store") == "silver.sil_store"


def test_uc_three_part():
    cfg = load(uc=True)
    assert cfg.silver("sil_store") == "retail_lakehouse.silver.sil_store"


def test_env_overrides():
    os.environ["DAB_CATALOG"] = "my_catalog"
    os.environ["DAB_GOLD_SCHEMA"] = "gold_v2"
    try:
        cfg = load()
        assert cfg.catalog == "my_catalog"
        assert cfg.gold("dim_date") == "gold_v2.dim_date"
    finally:
        os.environ.pop("DAB_CATALOG", None)
        os.environ.pop("DAB_GOLD_SCHEMA", None)


def test_kwargs_win():
    cfg = load(catalog="final", env="prd")
    assert cfg.catalog == "final"
    assert cfg.env == "prd"


def test_names_are_quoted_friendly():
    cfg = Cfg(volume="v", env="dev", uc=True)
    assert cfg.name("bronze", "brz_x").count(".") == 2
