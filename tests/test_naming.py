from utils import naming


def test_slugify_lowercase_and_collapse():
    assert naming.slugify("  Store Master ") == "store_master"


def test_bronze_table_prefix():
    assert naming.bronze_table("Sales/Daily Store SKU") == "brz_sales_daily_store_sku"


def test_silver_table_prefix():
    assert naming.silver_table("Daily Inventory") == "sil_daily_inventory"


def test_dim_fact_prefixes():
    assert naming.dim_table("Calendar 445") == "dim_calendar_445"
    assert naming.fact_table("Sales Store Daily") == "fact_sales_store_daily"


def test_suffix_keeps_base():
    assert naming.with_suffix("dim_calendar", "445") == "dim_calendar_445"
