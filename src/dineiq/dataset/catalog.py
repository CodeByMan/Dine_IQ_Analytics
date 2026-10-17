from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from dineiq.dataset.schema import TABLE_SCHEMAS
from dineiq.dataset.ingestion_audit import resolve_source_paths


@dataclass(frozen=True)
class TableSpec:
    filename: str
    primary_key: str
    minimum_rows: int


TABLES: dict[str, TableSpec] = {
    "customers": TableSpec("Customers.csv", "Customer_ID", 50_000),
    "orders": TableSpec("Orders.csv", "Order_ID", 100_000),
    "order_items": TableSpec("Order_Items.csv", "Order_Item_ID", 1_000_000),
    "menu_items": TableSpec("Menu_Items.csv", "Item_ID", 150),
    "menu_categories": TableSpec("Menu_Categories.csv", "Category_ID", 10),
    "restaurants": TableSpec("Restaurants.csv", "Restaurant_ID", 20),
    "pricing_history": TableSpec("Pricing_History.csv", "Pricing_ID", 2),
    "promotions": TableSpec("Promotions.csv", "Promotion_ID", 2),
    "ratings": TableSpec("Ratings.csv", "Rating_ID", 100_000),
    # main dataset includes these tables; the SRS does not assign minimum counts.
    "inventory": TableSpec("Inventory.csv", "Inventory_ID", 0),
    "wastage": TableSpec("Wastage.csv", "Wastage_ID", 50_000),
    "menu_item_availability": TableSpec("Menu_Item_Availability.csv", "Availability_ID", 0),
}


@dataclass(frozen=True)
class TableCheck:
    table: str
    path: Path
    rows: int
    primary_key_present: bool
    meets_minimum: bool
    error: str | None = None
    missing_columns: tuple[str, ...] = ()
    unexpected_columns: tuple[str, ...] = ()
    duplicate_columns: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return (
            self.error is None
            and self.primary_key_present
            and self.meets_minimum
            and not self.missing_columns
            and not self.unexpected_columns
            and not self.duplicate_columns
        )


def inspect_table(data_dir: Path, name: str, spec: TableSpec) -> TableCheck:
    path = data_dir / spec.filename
    if not path.is_file():
        return TableCheck(name, path, 0, False, False, "file missing")
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            columns = tuple(reader.fieldnames or ())
            has_key = spec.primary_key in columns
            expected = TABLE_SCHEMAS[name].columns
            missing = tuple(column for column in expected if column not in columns)
            unexpected = tuple(column for column in columns if column not in expected)
            duplicates = tuple(sorted({column for column in columns if columns.count(column) > 1}))
            rows = sum(1 for _ in reader)
        error = None if has_key else f"missing primary key {spec.primary_key}"
        return TableCheck(name, path, rows, has_key, rows >= spec.minimum_rows,
                          error, missing, unexpected, duplicates)
    except (OSError, UnicodeError, csv.Error) as exc:
        return TableCheck(name, path, 0, False, False, str(exc))


def inspect_dataset(data_dir: Path) -> tuple[TableCheck, ...]:
    """Stream table row counts; source data is not loaded into memory or modified."""
    return tuple(inspect_table(data_dir, name, spec) for name, spec in TABLES.items())


def require_valid_dataset(data_dir: Path) -> tuple[TableCheck, ...]:
    """Fail closed before consumers combine source data with generated outputs."""
    checks = inspect_dataset(data_dir)
    failures = [
        f"{check.table}: {check.error or 'schema/minimum validation failed'}"
        for check in checks if not check.passed
    ]
    if failures:
        raise ValueError(
            "Configured source is not a validated main dataset candidate ("
            + "; ".join(failures)
            + "). Set DINEIQ_DATA_DIR to the project's authoritative raw_data directory."
        )
    return checks


def require_dataset_headers(data_dir: Path) -> None:
    """Quick, bounded startup guard; full row/minimum checks remain a CLI job."""
    failures: list[str] = []
    for name, spec in TABLES.items():
        paths = resolve_source_paths(data_dir, name)
        if not paths:
            failures.append(f"{name}: missing canonical or split CSV for {spec.filename}")
            continue
        expected = TABLE_SCHEMAS[name].columns
        for path in paths:
            try:
                with path.open("r", newline="", encoding="utf-8-sig") as stream:
                    columns = tuple(next(csv.reader(stream), ()))
            except (OSError, UnicodeError, csv.Error) as exc:
                failures.append(f"{name}/{path.name}: {type(exc).__name__}")
                continue
            missing = sorted(set(expected) - set(columns))
            unexpected = sorted(set(columns) - set(expected))
            duplicated = sorted({column for column in columns if columns.count(column) > 1})
            if missing or unexpected or duplicated:
                failures.append(
                    f"{name}/{path.name}: missing={missing}, extra={unexpected}, duplicate={duplicated}"
                )
    if failures:
        raise ValueError(
            "Configured source headers do not match main dataset: "
            + "; ".join(failures)
            + ". Set DINEIQ_DATA_DIR to the project's authoritative raw_data directory."
        )

# Public compatibility names retained for previously deployed callers. Active
# application code uses the dataset-neutral names above.
require_valid_dataset1b = require_valid_dataset
require_dataset1b_headers = require_dataset_headers
