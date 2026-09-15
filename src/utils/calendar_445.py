"""Retail commercial calendar (4-4-5).

A 4-4-5 calendar groups the year into quarters of 13 weeks, each quarter split
into months of 4-4-5 weeks (28/28/35 days). It is the standard for retail
period-over-period analysis because every period has whole weeks and, in a
52-week year, every date has an exact year-ago partner separated by 364 days
(same weekday = same-store-sales comparison, "SSS").

This module is pure Python (no Spark) so it can be unit-tested locally and
reused by the synthetic data generator, the Gold layer and the tests.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

# Weeks per month within a quarter: 4-4-5 (NRF/retail standard).
_MONTH_SIZES = [4, 4, 5] * 4  # 12 months, 52 weeks


@dataclass(frozen=True)
class CommerceDate:
    """Fully described day in the retail (4-4-5) commercial calendar."""

    date_key: int
    calendar_date: dt.date
    commerce_year: int
    commerce_quarter: int
    commerce_month: int
    commerce_period: str  # 'YYYY-MM'
    commerce_week: int  # 1..53 within commerce year
    quarter_week: int  # 1..13 within quarter
    iso_year: int
    iso_week: int
    day_of_week: int  # 1=Mon .. 7=Sun
    day_name: str
    is_weekend: bool

    def as_row(self) -> dict:
        return {
            "date_key": self.date_key,
            "date_value": self.calendar_date.isoformat(),
            "commerce_year": self.commerce_year,
            "commerce_quarter": self.commerce_quarter,
            "commerce_month": self.commerce_month,
            "commerce_period": self.commerce_period,
            "commerce_week": self.commerce_week,
            "quarter_week": self.quarter_week,
            "iso_year": self.iso_year,
            "iso_week": self.iso_week,
            "day_of_week": self.day_of_week,
            "day_name": self.day_name,
            "is_weekend": self.is_weekend,
            "date_key_sss": _int_of_date(self.calendar_date - dt.timedelta(days=364)),
            "date_key_mc": _int_of_date(_add_years(self.calendar_date, -1)),
        }


WEEKDAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _monday_on_or_before(date: dt.date) -> dt.date:
    return date - dt.timedelta(days=date.weekday())


def _int_of_date(date: dt.date) -> int:
    return date.year * 10000 + date.month * 100 + date.day


def _add_years(date: dt.date, years: int) -> dt.date:
    """Shift date by a number of years keeping month/day (28 Feb clamps to 28)."""
    try:
        return date.replace(year=date.year + years)
    except ValueError:  # Feb 29 -> Feb 28
        return date.replace(year=date.year + years, day=28)


def _year_weeks(cal_year: int) -> tuple[dt.date, list[tuple[dt.date, dt.date]]]:
    """Return (anchor, weeks) for a commerce year starting from `cal_year`.

    The commerce year anchors on the Monday on or before Jan 1st of the
    calendar year. Weeks are consecutive Monday-Sunday slices; the last slice
    may spill a few days into the next calendar year. Normally 52 weeks; a 53rd
    week occurs for some years.
    """
    anchor = _monday_on_or_before(dt.date(cal_year, 1, 1))
    last_monday_of_cal = _monday_on_or_before(dt.date(cal_year, 12, 31))
    weeks: list[tuple[dt.date, dt.date]] = []
    start = anchor
    while start <= last_monday_of_cal:
        weeks.append((start, start + dt.timedelta(days=6)))
        start += dt.timedelta(days=7)
    return anchor, weeks


def _month_boundaries(n_weeks: int) -> list[int]:
    """Cumulative week counts per month for a year with `n_weeks` (52 or 53)."""
    sizes = list(_MONTH_SIZES)
    if n_weeks == 53:
        sizes[-1] += 1  # append the 53rd week to the last month
    cumulative: list[int] = []
    acc = 0
    for size in sizes:
        acc += size
        cumulative.append(acc)
    return cumulative


def build_commerce_calendar(start: dt.date, end: dt.date) -> list[dict]:
    """Build the 4-4-5 calendar for every date in [start, end] (inclusive)."""
    if start > end:
        start, end = end, start

    # Pre-compute week->(year, quarter, month, week_index) for each commerce year.
    resolve: dict[dt.date, tuple[int, int, int, int]] = {}

    for cal_year in range(start.year - 1, end.year + 2):
        _, weeks = _year_weeks(cal_year)
        boundaries = _month_boundaries(len(weeks))
        month_idx = 0
        for i, (monday, _sunday) in enumerate(weeks, start=1):
            while month_idx < len(boundaries) and i > boundaries[month_idx]:
                month_idx += 1
            month_num = month_idx + 1
            quarter_num = (month_num - 1) // 3 + 1
            resolve[monday] = (cal_year, quarter_num, month_num, i)

    rows: list[dict] = []
    day = start
    one_day = dt.timedelta(days=1)
    while day <= end:
        anchor = _monday_on_or_before(day)
        entry = resolve.get(anchor)
        if entry is None:
            day += one_day
            continue
        cal_year, quarter_num, month_num, week_idx = entry
        weekday = day.weekday()  # 0=Mon
        cd = CommerceDate(
            date_key=_int_of_date(day),
            calendar_date=day,
            commerce_year=cal_year,
            commerce_quarter=quarter_num,
            commerce_month=month_num,
            commerce_period=f"{cal_year}-{month_num:02d}",
            commerce_week=week_idx,
            quarter_week=_quarter_week(cal_year, week_idx, month_num),
            iso_year=day.isocalendar()[0],
            iso_week=day.isocalendar()[1],
            day_of_week=weekday + 1,
            day_name=WEEKDAY_NAMES[weekday],
            is_weekend=weekday >= 5,
        )
        rows.append(cd.as_row())
        day += one_day

    return rows


def _quarter_week(cal_year: int, week_idx: int, month_num: int) -> int:
    """Week index within its quarter (1..13/14): sector week minus weeks before the quarter."""
    quarter_start = ((month_num - 1) // 3) * 3 + 1
    weeks_before_quarter = sum(_MONTH_SIZES[: quarter_start - 1])
    return week_idx - weeks_before_quarter


def date_key_of(date: dt.date) -> int:
    """Encode a date as int YYYYMMDD (matches the previous project's DATE_KEY)."""
    return _int_of_date(date)
