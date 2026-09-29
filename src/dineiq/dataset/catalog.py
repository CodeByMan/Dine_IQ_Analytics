"""Read-only checks for the dataset files and minimum counts named in the SRS."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TableSpec:
    filename: str
    primary_key: str
    minimum_rows: int


TABLES: dict[str, TableSpec] = {
    "customers": TableSpec("customers.csv", "Customer_ID", 50_000),
    "orders": TableSpec("orders.csv", "Order_ID", 100_000),
    "order_items": TableSpec("order_items.csv", "Order_Item_ID", 1_000_000),
    "menu_items": TableSpec("menu_items.csv", "Item_ID", 150),
    "menu_categories": TableSpec("menu_categories.csv", "Category_ID", 10),
    "restaurants": TableSpec("restaurants.csv", "Location_ID", 20),
    "pricing_history": TableSpec("pricing_history.csv", "Price_History_ID", 2),
    "promotions": TableSpec("promotions.csv", "Promotion_ID", 2),
    "ratings": TableSpec("ratings.csv", "Rating_ID", 100_000),
    "inventory": TableSpec("inventory.csv", "Inventory_ID", 1),
    "wastage": TableSpec("wastage.csv", "Wastage_ID", 50_000),
    "ordering_channels": TableSpec("ordering_channels.csv", "Channel_ID", 1),
}


@dataclass(frozen=True)
class TableCheck:
    table: str
    path: Path
    rows: int
    primary_key_present: bool
    meets_minimum: bool
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and self.primary_key_present and self.meets_minimum


def inspect_table(data_dir: Path, name: str, spec: TableSpec) -> TableCheck:
    path = data_dir / spec.filename
    if not path.is_file():
        return TableCheck(name, path, 0, False, False, "file missing")
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            has_key = spec.primary_key in (reader.fieldnames or [])
            rows = sum(1 for _ in reader)
        return TableCheck(name, path, rows, has_key, rows >= spec.minimum_rows,
                          None if has_key else f"missing column {spec.primary_key}")
    except (OSError, UnicodeError, csv.Error) as exc:
        return TableCheck(name, path, 0, False, False, str(exc))


def inspect_dataset(data_dir: Path) -> tuple[TableCheck, ...]:
    """Stream table row counts; source data is not loaded into memory or modified."""
    return tuple(inspect_table(data_dir, name, spec) for name, spec in TABLES.items())
