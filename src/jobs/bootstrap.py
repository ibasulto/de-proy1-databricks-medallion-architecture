"""Notebook bootstrap shared by the DAB notebook tasks.

Usage (inside a Databricks git notebook, first cell)::

    import sys
    from pathlib import Path

    ROOT = dbutils.widgets.get("repo_path") or "/Workspace/Repos/anonymous/de-proy1-databricks-medallion-architecture"
    SRC = str(Path(ROOT) / "src")
    if SRC not in sys.path:
        sys.path.insert(0, SRC)

Then ``from utils.config import load`` or ``from jobs.pipeline import ...`` work.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from utils.config import Cfg

from utils.config import load  # noqa: F401

__all__ = ["load_cfg"]


def load_cfg(overrides: dict | None = None) -> Cfg:
    """Resolve a runtime :class:`utils.config.Cfg` from widgets/env/kwargs."""
    return load(**({} if overrides is None else overrides))
