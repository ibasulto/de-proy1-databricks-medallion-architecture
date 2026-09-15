"""Synthetic retail data generator.

Produces raw CSV files that mimic a retail source system (POS, inventory,
budgets, master dimensions and a 4-4-5 commercial calendar). Pure Python
standard library -> runs anywhere, deterministic via ``--seed``.

Files produced under ``<out>/<source>/`` and ready for Auto Loader:
  stores/master            stores.csv.gz            (dimension master)
  products/master          products.csv.gz          (dimension master)
  calendar/calendar_445    calendar_445.csv.gz      (dimension master)
  pos/                     pos_line_items_YYYYMMDD.csv.gz  (transactions)
  sales/daily_store_sku    daily_store_sku_YYYYMMDD.csv.gz (aggregated facts)
  kpi/store_daily          store_daily_kpi_YYYYMMDD.csv.gz
  inventory/daily          daily_inventory_YYYYMMDD.csv.gz (sliding window)
  budget/store_month       store_month_budget.csv.gz (full take)

Money columns are emitted as strings with 2 decimals (like a raw DB extract);
the Silver layer casts them to decimal(18,4).
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from utils.calendar_445 import build_commerce_calendar

TAX_RATE = 0.19  # VAT included in prices -> tax fraction 19/119

CATEGORIES = [
    {"category": "Fresh", "dept": "Perishables", "price_lo": 0.5, "price_hi": 15.0},
    {"category": "Dairy", "dept": "Perishables", "price_lo": 1.0, "price_hi": 8.0},
    {"category": "Bakery", "dept": "Perishables", "price_lo": 0.8, "price_hi": 12.0},
    {"category": "Beverages", "dept": "Grocery", "price_lo": 0.6, "price_hi": 9.0},
    {"category": "Snacks", "dept": "Grocery", "price_lo": 0.4, "price_hi": 6.0},
    {"category": "Pantry", "dept": "Grocery", "price_lo": 0.7, "price_hi": 25.0},
    {"category": "Household", "dept": "NonFood", "price_lo": 1.0, "price_hi": 35.0},
    {"category": "PersonalCare", "dept": "NonFood", "price_lo": 1.2, "price_hi": 20.0},
    {"category": "Electronics", "dept": "NonFood", "price_lo": 5.0, "price_hi": 150.0},
    {"category": "Clothing", "dept": "Textile", "price_lo": 3.0, "price_hi": 60.0},
]

FORMATS = [
    {
        "format": "hyper",
        "weight": 1.0,
        "m2": (5000, 12000),
        "products_mix": {
            0: 0.1,
            1: 0.1,
            2: 0.1,
            3: 0.12,
            4: 0.1,
            5: 0.16,
            6: 0.1,
            7: 0.1,
            8: 0.06,
            9: 0.06,
        },
    },
    {
        "format": "super",
        "weight": 3.0,
        "m2": (800, 4000),
        "products_mix": {
            0: 0.12,
            1: 0.13,
            2: 0.1,
            3: 0.14,
            4: 0.13,
            5: 0.12,
            6: 0.08,
            7: 0.09,
            8: 0.04,
            9: 0.05,
        },
    },
    {
        "format": "express",
        "weight": 5.0,
        "m2": (120, 700),
        "products_mix": {
            0: 0.08,
            1: 0.12,
            2: 0.06,
            3: 0.26,
            4: 0.24,
            5: 0.12,
            6: 0.05,
            7: 0.05,
            8: 0.02,
            9: 0.0,
        },
    },
]

REGIONS = [
    ("North", ["Iquique", "Antofagasta", "Calama"]),
    ("Central", ["Santiago", "Valparaiso", "Concepcion"]),
    ("South", ["Temuco", "Puerto Montt", "Valdivia"]),
]


def _money(value: float) -> str:
    return f"{value:.2f}"


@dataclass
class Product:
    product_key: int
    sku: str
    ean: str
    name: str
    brand: str
    category: str
    dept: str
    category_idx: int
    retail_price: float
    unit_cost: float
    uom: str
    barcode_weight_kg: float | None


@dataclass
class Store:
    store_key: int
    store_code: str
    name: str
    chain_format: str
    region: str
    city: str
    address: str
    open_m2: int
    open_date: date
    status: str


@dataclass
class GenParams:
    seed: int = 42
    n_stores: int = 40
    n_products: int = 400
    start: date = date(2025, 1, 1)
    end: date = date(2026, 12, 31)
    inv_days: int = 56
    tickets_per_day_floor: int = 60  # avg base tickets/day for a "super" store

    def __post_init__(self):
        if self.end < self.start:
            raise ValueError("end must be >= start")


@dataclass
class _Txn:
    txn_id: str
    store_key: int
    date_value: date
    lines: list = field(default_factory=list)


class Generator:
    def __init__(self, params: GenParams):
        self.p = params
        self.rng = random.Random(params.seed)
        self.today_counter = 0
        self.txn_counter = 0
        self.stores: list[Store] = []
        self.products: list[Product] = []
        self.cat_idx = {c["category"]: i for i, c in enumerate(CATEGORIES)}
        self._sales_by_sku: dict[tuple[date, int, int], dict] = defaultdict(dict)
        self._budget: dict[tuple[int, int, str], float] = {}
        self._calendar_rows = build_commerce_calendar(params.start, params.end)

    # ---------- masters ----------
    def make_stores(self) -> list[Store]:
        out = []
        for i in range(1, self.p.n_stores + 1):
            fmt_meta = self.rng.choices(FORMATS, weights=[f["weight"] for f in FORMATS])[0]
            region, cities = self.rng.choice(REGIONS)
            m2 = self.rng.randint(*fmt_meta["m2"])
            open_y = self.rng.randint(2000, 2023)
            out.append(
                Store(
                    store_key=i,
                    store_code=f"ST{i:04d}",
                    name=f"Store {i:04d} {region}",
                    chain_format=fmt_meta["format"],
                    region=region,
                    city=self.rng.choice(cities),
                    address=f"{self.rng.randint(1, 9999)} Main St",
                    open_m2=m2,
                    open_date=date(open_y, self.rng.randint(1, 12), self.rng.randint(1, 28)),
                    status="Active" if self.rng.random() > 0.03 else "Planned",
                )
            )
        self.stores = out
        return out

    def make_products(self) -> list[Product]:
        out = []
        brands = [f"Brand {chr(65 + i)}" for i in range(10)]
        categories_weights = [0.12, 0.12, 0.10, 0.14, 0.15, 0.12, 0.08, 0.09, 0.04, 0.04]
        for i in range(1, self.p.n_products + 1):
            idx = self.rng.choices(range(len(CATEGORIES)), weights=categories_weights)[0]
            meta = CATEGORIES[idx]
            price = round(self.rng.uniform(meta["price_lo"], meta["price_hi"]) * 4) / 4
            cost = round(price / (1 + self.rng.uniform(0.2, 0.6)), 2)
            uom = self.rng.choice(["EA", "KG", "LT", "PACK"])
            weight = round(self.rng.uniform(0.05, 5), 3) if uom == "KG" else None
            out.append(
                Product(
                    product_key=i,
                    sku=f"SKU{i:06d}",
                    ean=f"{self.rng.randint(1000000000000, 9999999999999)}",
                    name=f"{meta['category']} Product {i}",
                    brand=self.rng.choice(brands),
                    category=meta["category"],
                    dept=meta["dept"],
                    category_idx=idx,
                    retail_price=price,
                    unit_cost=cost,
                    uom=uom,
                    barcode_weight_kg=weight,
                )
            )
        self.products = out
        return out

    # ---------- volumes ----------
    def _day_season(self, d: date) -> float:
        """Base traffic multiplier by commerce period + weekday + generic float holidays."""
        month = d.month
        festive = 0.0
        if (d.month, d.day) in {(12, 25), (1, 1), (9, 18), (12, 31), (5, 1)}:
            festive = 0.10 if (d.month, d.day) == (12, 31) else -0.20
        season_idx = [1.02, 0.92, 0.97, 1.00, 1.05, 1.00, 0.95, 0.96, 1.02, 1.06, 1.12, 1.25][
            month - 1
        ]
        weekend = 1.35 if d.weekday() >= 5 else 1.0
        return season_idx + festive + weekend - 1.0

    def _tickets_for(self, store: Store, d: date) -> int:
        fmt = store.chain_format
        base = self.p.tickets_per_day_floor * {"hyper": 1.8, "super": 1.0, "express": 0.45}[fmt]
        season = self._day_season(d)
        normal = self.rng.gauss(base * season, base * 0.12)
        return max(1, int(round(normal)))

    def _draw_category(self, store_format: str) -> int:
        mix = next(f["products_mix"] for f in FORMATS if f["format"] == store_format)
        keys = list(mix.keys())
        weights = [mix[k] for k in keys]
        return self.rng.choices(keys, weights=weights)[0]

    def _line(self, store: Store, d: date) -> tuple[Product, int, float, float, float]:
        cat_idx = self._draw_category(store.chain_format)
        pool = [p for p in self.products if p.category_idx == cat_idx]
        if not pool:  # small catalogs (tests / --products small) may lack a category
            pool = self.products
        product = self.rng.choice(pool)
        qty = 1
        if product.uom == "KG":
            qty = 1  # weight stored in amount via barcode_weight
        else:
            qty = self.rng.choices([1, 1, 1, 2, 2, 3, 4, 6], weights=[40, 25, 15, 10, 5, 3, 1, 1])[
                0
            ]
        promo = self.rng.random() < 0.18
        disc = round(self.rng.choice([0.1, 0.15, 0.25, 0.3]), 2) if promo else 0.0
        if product.uom == "KG":
            amount = round(product.retail_price * product.barcode_weight_kg, 2)
        else:
            amount = round(product.retail_price * qty, 2)
        gross = amount
        discount = round(gross * disc, 2)
        net = round(gross - discount, 2)
        return product, qty, gross, discount, net

    # ---------- raw writers ----------
    def _write_chunk(self, path: Path, rows: list[tuple], fieldnames: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def generate_pos(self, out_root: Path) -> dict:
        pos_dir = out_root / "pos_line_items"
        sales_dir = out_root / "sales" / "daily_store_sku"
        kpi_dir = out_root / "kpi" / "store_daily"
        pos_dir.mkdir(parents=True, exist_ok=True)
        sales_dir.mkdir(parents=True, exist_ok=True)
        kpi_dir.mkdir(parents=True, exist_ok=True)

        pos_fields = [
            "txn_id",
            "line_id",
            "txn_date",
            "store_key",
            "product_key",
            "sku",
            "unit_qty",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "tax_amount",
            "cost_amount",
            "payment_type",
            "channel",
            "is_promo",
        ]
        sales_fields = [
            "date_key",
            "store_key",
            "product_key",
            "sku",
            "unit_qty",
            "gross_sales",
            "net_sales",
            "discount_amount",
            "tax_amount",
            "cost_amount",
            "ticket_count",
            "line_count",
            "promo_line_count",
        ]
        kpi_fields = [
            "date_key",
            "store_key",
            "ticket_count",
            "transaction_count",
            "avg_basket",
            "traffic_estimate",
            "sales_m2",
            "gross_sales",
            "net_sales",
        ]

        day = self.p.start
        while day <= self.p.end:
            dkey = day.isoformat().replace("-", "")
            pos_rows: list[dict] = []
            sales_agg: dict[tuple[int, int], dict] = {}
            store_agg: dict[int, dict] = {}
            self.txn_counter = 0
            for store in self.stores:
                n_tickets = self._tickets_for(store, day)
                store_row = {
                    "date_key": dkey,
                    "store_key": store.store_key,
                    "ticket_count": n_tickets,
                    "transaction_count": n_tickets,
                    "traffic_estimate": int(n_tickets * self.rng.gauss(1.4, 0.2)),
                    "gross_sales": 0.0,
                    "net_sales": 0.0,
                }
                for _ in range(n_tickets):
                    self.txn_counter += 1
                    txn_id = f"{dkey}{self.txn_counter:08d}"
                    n_lines = self.rng.choices(
                        [1, 2, 3, 4, 5, 6, 8, 10], weights=[20, 25, 18, 14, 10, 7, 4, 2]
                    )[0]
                    payment = self.rng.choices(
                        ["CASH", "CARD", "QR", "TRANSFER"], weights=[35, 45, 12, 8]
                    )[0]
                    channel = self.rng.choices(["POS", "SELF", "ONLINE"], weights=[75, 15, 10])[0]
                    for lid in range(n_lines):
                        product, qty, gross, discount, net = self._line(store, day)
                        tax = round(net * (TAX_RATE / (1 + TAX_RATE)), 2)
                        cost = round(product.unit_cost * qty, 2)
                        pos_rows.append(
                            {
                                "txn_id": txn_id,
                                "line_id": lid + 1,
                                "txn_date": day.isoformat(),
                                "store_key": store.store_key,
                                "product_key": product.product_key,
                                "sku": product.sku,
                                "unit_qty": qty,
                                "gross_amount": _money(gross),
                                "discount_amount": _money(discount),
                                "net_amount": _money(net),
                                "tax_amount": _money(tax),
                                "cost_amount": _money(cost),
                                "payment_type": payment,
                                "channel": channel,
                                "is_promo": "true" if discount > 0 else "false",
                            }
                        )
                        key = (store.store_key, product.product_key)
                        agg = sales_agg.setdefault(
                            key,
                            {
                                "qty": 0,
                                "gross": 0.0,
                                "net": 0.0,
                                "disc": 0.0,
                                "tax": 0.0,
                                "cost": 0.0,
                                "txn": 0,
                                "lines": 0,
                                "promo": 0,
                            },
                        )
                        agg["qty"] += qty
                        agg["gross"] += gross
                        agg["net"] += net
                        agg["disc"] += discount
                        agg["tax"] += tax
                        agg["cost"] += cost
                        agg["txn"] += 1
                        agg["lines"] += 1
                        agg["promo"] += 1 if discount > 0 else 0
                        store_row["gross_sales"] += gross
                        store_row["net_sales"] += net
                store_row["avg_basket"] = round(
                    store_row["net_sales"] / max(store_row["ticket_count"], 1), 2
                )
                store_row["sales_m2"] = round(
                    store_row["net_sales"] / max(store.open_m2, 1) * 1000, 2
                )
                store_row["gross_sales"] = _money(store_row["gross_sales"])
                store_row["net_sales"] = _money(store_row["net_sales"])
                store_agg[store.store_key] = store_row

            self._write_chunk(pos_dir / f"pos_line_items_{dkey}.csv.gz", pos_rows, pos_fields)
            sales_rows = []
            for (s_key, p_key), agg in sales_agg.items():
                product = next(p for p in self.products if p.product_key == p_key)
                sales_rows.append(
                    {
                        "date_key": dkey,
                        "store_key": s_key,
                        "product_key": p_key,
                        "sku": product.sku,
                        "unit_qty": agg["qty"],
                        "gross_sales": _money(agg["gross"]),
                        "net_sales": _money(agg["net"]),
                        "discount_amount": _money(agg["disc"]),
                        "tax_amount": _money(agg["tax"]),
                        "cost_amount": _money(agg["cost"]),
                        "ticket_count": agg["txn"],
                        "line_count": agg["lines"],
                        "promo_line_count": agg["promo"],
                    }
                )
                self._sales_by_sku[(day, s_key, p_key)] = {
                    "qty": agg["qty"],
                    "net": agg["net"],
                    "cost": agg["cost"],
                }
            self._write_chunk(
                sales_dir / f"daily_store_sku_{dkey}.csv.gz", sales_rows, sales_fields
            )
            self._write_chunk(
                kpi_dir / f"store_daily_kpi_{dkey}.csv.gz", list(store_agg.values()), kpi_fields
            )
            day += timedelta(days=1)
        return {"pos": pos_dir, "sales": sales_dir, "kpi": kpi_dir}

    def generate_masters(self, out_root: Path) -> dict:
        stores = [self._row_dict(s) for s in self.make_stores()]
        products = [self._row_dict(p) for p in self.make_products()]
        calendar_rows = self._calendar_rows
        masters = [
            (
                "stores/master",
                stores,
                [
                    "store_key",
                    "store_code",
                    "name",
                    "chain_format",
                    "region",
                    "city",
                    "address",
                    "open_m2",
                    "open_date",
                    "status",
                ],
            ),
            (
                "products/master",
                products,
                [
                    "product_key",
                    "sku",
                    "ean",
                    "name",
                    "brand",
                    "category",
                    "dept",
                    "retail_price",
                    "unit_cost",
                    "uom",
                    "barcode_weight_kg",
                ],
            ),
            ("calendar/calendar_445", calendar_rows, list(calendar_rows[0].keys())),
        ]
        out: dict[str, Path] = {}
        for rel, rows, fieldnames in masters:
            path = out_root / f"{rel}.csv.gz"
            self._write_chunk(path, rows, fieldnames)
            out[rel] = path
        return out

    def _row_dict(self, obj) -> dict:
        if isinstance(obj, Store):
            return {
                "store_key": obj.store_key,
                "store_code": obj.store_code,
                "name": obj.name,
                "chain_format": obj.chain_format,
                "region": obj.region,
                "city": obj.city,
                "address": obj.address,
                "open_m2": obj.open_m2,
                "open_date": obj.open_date.isoformat(),
                "status": obj.status,
            }
        if isinstance(obj, Product):
            return {
                "product_key": obj.product_key,
                "sku": obj.sku,
                "ean": obj.ean,
                "name": obj.name,
                "brand": obj.brand,
                "category": obj.category,
                "dept": obj.dept,
                "retail_price": _money(obj.retail_price),
                "unit_cost": _money(obj.unit_cost),
                "uom": obj.uom,
                "barcode_weight_kg": ""
                if obj.barcode_weight_kg is None
                else f"{obj.barcode_weight_kg:.3f}",
            }
        return obj

    def generate_inventory(self, out_root: Path) -> dict:
        inv_dir = out_root / "inventory" / "daily"
        inv_dir.mkdir(parents=True, exist_ok=True)
        fields = [
            "date_key",
            "store_key",
            "product_key",
            "sku",
            "on_hand_qty",
            "on_hand_cost",
            "received_qty",
            "sold_qty",
            "adjusted_qty",
            "in_transit_qty",
            "stock_state",
            "unit_cost",
            "retail_price",
        ]
        start = self.p.end - timedelta(days=self.p.inv_days - 1)
        on_hand: dict[tuple[int, int], int] = {}
        day = start
        while day <= self.p.end:
            dkey = day.isoformat().replace("-", "")
            rows = []
            for store in self.stores:
                for product in self.products:
                    key = (store.store_key, product.product_key)
                    sold = self._sales_by_sku.get(
                        (day, store.store_key, product.product_key), {}
                    ).get("qty", 0)
                    if key not in on_hand:
                        base = self.rng.randint(0, 30)
                        on_hand[key] = base
                    received = 0
                    if self.rng.random() < 0.02:
                        received = self.rng.randint(1, 15)
                    adjusted = 0
                    if self.rng.random() < 0.01:
                        adjusted = self.rng.randint(-3, 3)
                    on_hand[key] = max(0, on_hand[key] + received - sold + adjusted)
                    state = (
                        "IN_STOCK"
                        if on_hand[key] > 4
                        else ("LOW_STOCK" if on_hand[key] > 0 else "OUT_OF_STOCK")
                    )
                    rows.append(
                        {
                            "date_key": dkey,
                            "store_key": store.store_key,
                            "product_key": product.product_key,
                            "sku": product.sku,
                            "on_hand_qty": on_hand[key],
                            "on_hand_cost": _money(on_hand[key] * product.unit_cost),
                            "received_qty": received,
                            "sold_qty": sold,
                            "adjusted_qty": adjusted,
                            "in_transit_qty": self.rng.choice([0, 0, 0, 1, 2, 4]),
                            "stock_state": state,
                            "unit_cost": _money(product.unit_cost),
                            "retail_price": _money(product.retail_price),
                        }
                    )
            self._write_chunk(inv_dir / f"daily_inventory_{dkey}.csv.gz", rows, fields)
            day += timedelta(days=1)
        return {"inventory": inv_dir}

    def generate_budget(self, out_root: Path) -> dict:
        """Store x commerce-month x dept budget, target 0.92 of modeled sales."""
        bdir = out_root / "budget" / "store_month"
        bdir.mkdir(parents=True, exist_ok=True)
        fields = [
            "commerce_period",
            "calendar_month_start",
            "store_key",
            "dept",
            "budget_amount",
            "budget_qty_units",
        ]
        rows = []
        keys = sorted({(r["commerce_year"], r["commerce_month"]) for r in self._calendar_rows})
        for cy, cm in keys:
            period = f"{cy}-{cm:02d}"
            month_start = min(
                r["date_value"] for r in self._calendar_rows if r["commerce_period"] == period
            )
            for store in self.stores:
                for dept in {p.dept for p in self.products}:
                    year_target = store.open_m2 * self.rng.uniform(18000, 26000)
                    season = [
                        1.02,
                        0.92,
                        0.97,
                        1.00,
                        1.05,
                        1.00,
                        0.95,
                        0.96,
                        1.02,
                        1.06,
                        1.12,
                        1.15,
                    ][cm - 1]
                    month_target = year_target / 12 * season * self.rng.uniform(0.9, 1.1)
                    budget = round(month_target * (1.0 / 4), 2)
                    rows.append(
                        {
                            "commerce_period": period,
                            "calendar_month_start": month_start,
                            "store_key": store.store_key,
                            "dept": dept,
                            "budget_amount": _money(budget),
                            "budget_qty_units": self.rng.randint(5000, 60000),
                        }
                    )
        path = bdir / "store_month_budget.csv.gz"
        self._write_chunk(path, rows, fields)
        return {"budget": path}


def write_manifest(out_root: Path, generated: dict) -> Path:
    man = {
        "version": 1,
        "seed": "see params",
        "sources": {k: str(v.relative_to(out_root).as_posix()) for k, v in generated.items()},
    }
    out_root.joinpath("_manifest.json").write_text(json.dumps(man, indent=2), encoding="utf-8")
    return out_root / "_manifest.json"


def generate(params: GenParams, out_root: Path) -> Path:
    out_root = Path(out_root)
    g = Generator(params)
    masters = g.generate_masters(out_root)
    pos = g.generate_pos(out_root)
    inventory = g.generate_inventory(out_root)
    budget = g.generate_budget(out_root)
    all_out = {**masters, **pos, **inventory, **budget}
    write_manifest(out_root, all_out)
    return out_root


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="resources/data/raw", help="output root")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--stores", type=int, default=40)
    parser.add_argument("--products", type=int, default=400)
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2026-12-31")
    parser.add_argument("--inv-days", type=int, default=56)
    parser.add_argument("--tickets", type=int, default=60, help="base tickets/day/store")
    args = parser.parse_args()
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
    print(f"Raw data generated at: {out}")


if __name__ == "__main__":
    main()
