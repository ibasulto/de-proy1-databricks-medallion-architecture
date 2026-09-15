"""Generate the synthetic retail raw dataset.

Usage (from the repo root):
    python scripts/generate_data.py --seed 42 --stores 40 --products 400 --tickets 60
    python scripts/generate_data.py --mini   # tiny dataset for quick local tests
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.data_generation.generator import GenParams, generate

MINI = dict(
    seed=1,
    n_stores=3,
    n_products=12,
    start=date(2026, 1, 1),
    end=date(2026, 1, 31),
    inv_days=7,
    tickets_per_day_floor=12,
)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="resources/data/raw")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--stores", type=int, default=40)
    parser.add_argument("--products", type=int, default=400)
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2026-12-31")
    parser.add_argument("--inv-days", type=int, default=56)
    parser.add_argument("--tickets", type=int, default=60)
    parser.add_argument("--mini", action="store_true", help="tiny dataset for local smoke tests")
    args = parser.parse_args()

    if args.mini:
        params = GenParams(**MINI)
    else:
        params = GenParams(
            seed=args.seed,
            n_stores=args.stores,
            n_products=args.products,
            start=date.fromisoformat(args.start),
            end=date.fromisoformat(args.end),
            inv_days=args.inv_days,
            tickets_per_day_floor=args.tickets,
        )

    out = generate(params, Path(args.out))
    print(f"Raw data generated at: {out.resolve()}")


if __name__ == "__main__":
    main()
