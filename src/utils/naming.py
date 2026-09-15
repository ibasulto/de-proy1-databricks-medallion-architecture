"""Naming conventions aligned with the medallion + the previous Cognos->Fabric
retail project (fact_/dim_/pl_/nb_ prefixes, _vN suffixes for repeated tables).

Layer prefixes used in this project:
  bronze tables : ``brz_<source>`` (raw copy)
  silver tables : ``sil_<entity>`` (cleansed/conformed)
  gold tables   : ``dim_<entity>`` / ``fact_<process>`` (star schema)
"""

from __future__ import annotations

import re

_WS_RE = re.compile(r"\s+")


def slugify(value: str, sep: str = "_") -> str:
    """Lowercase, strip non-alphanumeric and collapse whitespace."""
    value = _WS_RE.sub(sep, value.strip().lower())
    return re.sub(r"[^a-z0-9_]+", sep, value)


def bronze_table(source: str) -> str:
    return "brz_" + slugify(source)


def silver_table(entity: str) -> str:
    return "sil_" + slugify(entity)


def dim_table(entity: str) -> str:
    return "dim_" + slugify(entity)


def fact_table(process: str) -> str:
    return "fact_" + slugify(process)


def with_suffix(name: str, suffix: str) -> str:
    """Append a purpose suffix (e.g. dim_calendar_445) instead of v1/v2."""
    return f"{name}_{slugify(suffix)}"


def truncate(name: str, limit: int = 127) -> str:
    return name[:limit].rstrip("_")
