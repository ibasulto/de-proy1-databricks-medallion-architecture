from datetime import date

from utils.calendar_445 import build_commerce_calendar, date_key_of


def test_structure_rows_for_every_date():
    rows = build_commerce_calendar(date(2025, 1, 1), date(2025, 12, 31))
    assert len(rows) == 365
    keys = {r["date_key"] for r in rows}
    assert len(keys) == 365


def test_quarter_weeks_are_13():
    rows = build_commerce_calendar(date(2025, 1, 1), date(2025, 12, 31))
    q1 = {r["quarter_week"] for r in rows if r["commerce_quarter"] == 1}
    assert q1 == set(range(1, 14))


def test_commerce_weeks_range():
    rows = build_commerce_calendar(date(2025, 1, 1), date(2025, 12, 31))
    weeks = {r["commerce_week"] for r in rows}
    assert max(weeks) <= 53
    assert min(weeks) == 1


def test_sss_is_364_days_ago():
    rows = build_commerce_calendar(date(2025, 1, 1), date(2025, 12, 31))
    jan2 = next(r for r in rows if r["date_value"] == "2025-01-02")
    assert jan2["date_key_sss"] == date_key_of(date(2024, 1, 4))  # 2024-01-04


def test_mc_is_year_ago():
    rows = build_commerce_calendar(date(2025, 1, 1), date(2025, 12, 31))
    row = next(r for r in rows if r["date_value"] == "2025-06-15")
    assert row["date_key_mc"] == date_key_of(date(2024, 6, 15))


def test_mondays_anchor_the_commerce_year():
    rows = build_commerce_calendar(date(2025, 1, 1), date(2025, 1, 5))
    assert all(r["commerce_week"] == 1 for r in rows)


def test_reversed_range_swaps():
    rows = build_commerce_calendar(date(2025, 12, 31), date(2025, 1, 1))
    assert len(rows) == 365
