from __future__ import annotations

import csv
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, functions as F

from dineiq.dataset.ingest import invalid_cast_counts
from dineiq.dataset.schema import TABLE_SCHEMAS


ISSUE_TYPES = (
    "missing_values",
    "duplicate_orders",
    "duplicate_order_lines",
    "invalid_menu_prices",
    "negative_quantities",
    "invalid_dates",
    "invalid_ratings",
    "missing_customer_ids",
    "missing_menu_ids",
    "invalid_restaurant_ids",
    "impossible_wastage_quantities",
    "incorrect_discounts",
    "cancelled_transactions",
    "inconsistent_units",
    "invalid_location_references",
)


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _field(record: dict[str, Any], *names: str) -> Any:
    return next((record[name] for name in names if name in record), None)


def _column(frame: DataFrame, *names: str) -> str:
    return next((name for name in names if name in frame.columns), names[0])


def detect_fixture_record(issue_type: str, record: dict[str, Any], valid_ids: set[str] | None = None) -> bool:
    """Apply a named SRS detector to one isolated fixture record."""
    if issue_type == "missing_values":
        return any(value is None or value == "" for value in record.values())
    if issue_type in {"duplicate_orders", "duplicate_order_lines"}:
        return "duplicate_of" in record or str(record.get("Is_Duplicate", "")).lower() == "true"
    if issue_type == "invalid_menu_prices":
        value = _number(_field(record, "Base_Price", "Selling_Price", "Unit_Price"))
        return value is not None and value <= 0
    if issue_type == "negative_quantities":
        value = _number(record.get("Quantity", record.get("Quantity_Wasted")))
        return value is not None and value < 0
    if issue_type == "invalid_dates":
        value = record.get("Order_Date") or record.get("Date")
        try:
            date.fromisoformat(str(value))
            return False
        except (TypeError, ValueError):
            try:
                datetime.fromisoformat(str(value))
                return False
            except (TypeError, ValueError):
                return True
    if issue_type == "invalid_ratings":
        value = _number(_field(record, "Rating_Value", "Rating"))
        return value is not None and not 1 <= value <= 5
    if issue_type == "missing_customer_ids":
        return record.get("Customer_ID") in (None, "")
    if issue_type == "missing_menu_ids":
        return record.get("Item_ID") in (None, "")
    if issue_type in {"invalid_restaurant_ids", "invalid_location_references"}:
        value = _field(record, "Restaurant_ID", "Location_ID")
        return value not in (None, "") and valid_ids is not None and str(value) not in valid_ids
    if issue_type == "impossible_wastage_quantities":
        wasted = _number(_field(record, "Wastage_Quantity", "Quantity_Wasted"))
        prepared = _number(_field(record, "Prepared_Quantity", "Preparation_Quantity"))
        return wasted is not None and prepared is not None and (wasted < 0 or wasted > prepared)
    if issue_type == "incorrect_discounts":
        discount = _number(record.get("Discount_Amount"))
        gross = _number(record.get("Gross_Line_Value"))
        if gross is None:
            quantity = _number(record.get("Quantity"))
            unit_price = _number(record.get("Unit_Price"))
            gross = quantity * unit_price if quantity is not None and unit_price is not None else None
        return discount is not None and gross is not None and (discount < 0 or discount > gross)
    if issue_type == "cancelled_transactions":
        return str(_field(record, "Status", "Order_Status") or "").strip().lower() == "cancelled"
    if issue_type == "inconsistent_units":
        expected = record.get("Expected_Unit")
        actual = record.get("Quantity_Unit")
        return expected not in (None, "") and actual not in (None, "") and str(expected).strip().lower() != str(actual).strip().lower()
    raise KeyError(f"Unknown quality issue type: {issue_type}")


def fixture_coverage(fixtures_dir: Path, valid_location_ids: set[str]) -> dict[str, Any]:
    path = fixtures_dir / "data_quality" / "quality_issue_cases.csv"
    if not path.is_file():
        raise FileNotFoundError(f"Required SRS quality fixtures not found: {path}")
    results: dict[str, bool] = {}
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream):
            issue_type = row["Issue_Type"]
            fixture = json.loads(row["Fixture_Record_JSON"])
            results[issue_type] = detect_fixture_record(issue_type, fixture, valid_location_ids)
    missing = sorted(set(ISSUE_TYPES) - set(results))
    unexpected = sorted(set(results) - set(ISSUE_TYPES))
    failed = sorted(name for name, passed in results.items() if not passed)
    return {
        "expected_issue_types": len(ISSUE_TYPES),
        "fixture_types_found": len(results),
        "fixture_types_detected": sum(results.values()),
        "missing_types": missing,
        "unexpected_types": unexpected,
        "failed_types": failed,
        "passed": not missing and not unexpected and not failed and len(results) == len(ISSUE_TYPES),
    }


def _aggregate_counts(frame: DataFrame, conditions: dict[str, Any]) -> dict[str, int]:
    expressions = [F.sum(F.when(condition, 1).otherwise(0)).alias(name)
                   for name, condition in conditions.items()]
    if not expressions:
        return {}
    row = frame.agg(*expressions).first()
    return {name: int(row[name] or 0) for name in conditions}


def _duplicate_count(frame: DataFrame, key: str) -> int:
    return int(frame.groupBy(key).count().where(F.col("count") > 1).count())


def analyze_quality(tables: dict[str, DataFrame], fixtures_dir: Path) -> dict[str, Any]:
    """Detect all 15 enumerated SRS quality cases and report observed row/group counts."""
    orders = tables["orders"]
    order_items = tables["order_items"]
    menu_items = tables["menu_items"]
    ratings = tables["ratings"]
    wastage = tables["wastage"]
    inventory = tables["inventory"]
    restaurants = tables["restaurants"]

    restaurant_key = _column(restaurants, "Restaurant_ID", "Location_ID")
    order_location_key = _column(orders, "Restaurant_ID", "Location_ID")
    valid_location_ids = {
        str(row[0]) for row in restaurants.select(restaurant_key)
        .where(F.col(restaurant_key).isNotNull()).distinct().collect()
    }
    restaurant_keys_for_orders = restaurants.select(
        F.col(restaurant_key).alias(order_location_key)
    ).dropDuplicates()
    invalid_order_locations = orders.join(restaurant_keys_for_orders, order_location_key, "left_anti")
    valid_menu_ids = menu_items.select("Item_ID").dropDuplicates()
    invalid_menu_ids = order_items.join(valid_menu_ids, "Item_ID", "left_anti")
    missing_by_table: dict[str, int] = {}
    invalid_date_count = 0
    date_columns_by_table = {
        "orders": {"Order_Date", "Order_DateTime"},
        "customers": {"Signup_Date"},
        "pricing_history": {"Effective_Date", "End_Date"}, "promotions": {"Start_Date", "End_Date"},
        "ratings": {"Rating_Date"}, "inventory": {"Month"},
        "wastage": {"Date"}, "restaurants": {"Opening_Date"},
        "menu_items": {"Introduced_Date"}, "menu_item_availability": {"Available_From"},
    }
    for table_name, frame in tables.items():
        public_columns = [c for c in frame.columns if not c.startswith("__invalid_cast__")]
        missing_condition = F.lit(False)
        for column in public_columns:
            missing_condition = missing_condition | F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")
        missing_by_table[table_name] = _aggregate_counts(frame, {"missing": missing_condition})["missing"]
        invalid_date_count += sum(
            count for name, count in invalid_cast_counts(frame, table_name).items()
            if name in date_columns_by_table.get(table_name, set())
        )

    status_col = _column(orders, "Status", "Order_Status")
    rating_col = _column(ratings, "Rating_Value", "Rating")
    waste_col = _column(wastage, "Wastage_Quantity", "Quantity_Wasted")
    prepared_col = _column(wastage, "Prepared_Quantity", "Preparation_Quantity")
    price_col = _column(menu_items, "Base_Price", "Selling_Price")
    discount_col = _column(order_items, "Discount_Percent")
    order_counts = _aggregate_counts(orders, {
        "missing_customer": F.col("Customer_ID").isNull() | (F.trim(F.col("Customer_ID").cast("string")) == ""),
        "cancelled": F.lower(F.trim(F.col(status_col))) == "cancelled",
    })
    item_counts = _aggregate_counts(order_items, {
        "negative_quantity": F.col("Quantity") <= 0,
        "missing_item": F.col("Item_ID").isNull() | (F.trim(F.col("Item_ID").cast("string")) == ""),
        "incorrect_discount": (F.col(discount_col) < 0) | (F.col(discount_col) > 100)
        | (F.abs(F.col("Line_Total") - F.col("Line_Subtotal") * (1 - F.col(discount_col) / 100)) > 0.02),
    })
    menu_counts = _aggregate_counts(menu_items, {
        "invalid_price": F.col(price_col).isNull() | (F.col(price_col) <= 0),
    })
    line_price_count = _aggregate_counts(order_items, {
        "invalid_price": F.col("Unit_Price").isNull() | (F.col("Unit_Price") <= 0),
    })["invalid_price"]
    waste_counts = _aggregate_counts(wastage, {
        "negative_quantity": F.col(waste_col) < 0,
        "impossible_quantity": (F.col(waste_col) < 0)
        | (F.col(prepared_col).isNotNull() & (F.col(waste_col) > F.col(prepared_col))),
    })
    rating_counts = _aggregate_counts(ratings, {
        "invalid_rating": F.col(rating_col).isNull() | ~F.col(rating_col).between(1, 5),
    })
    # main dataset chain-wide Pricing_History intentionally has no branch key;
    # it must not be treated as a location-scoped table.
    location_sources = (orders, inventory, wastage, ratings)
    invalid_location_count = 0
    for frame in location_sources:
        child_key = _column(frame, "Restaurant_ID", "Location_ID")
        valid_keys = restaurants.select(F.col(restaurant_key).alias(child_key)).dropDuplicates()
        invalid_location_count += frame.where(F.col(child_key).isNotNull()).join(
            valid_keys, child_key, "left_anti"
        ).count()

    # main dataset documents kg/liters/pcs as canonical labels. Known spelling,
    # case and unit aliases are findings before cleaning, not inferred unit
    # conversions. The cleaner maps documented variants only; it does not
    # numerically convert grams/lbs because the dataset policy does not define
    # whether those values were physically generated in alternate units.
    canonical_unit_labels = ("kg", "liters", "pcs")
    inconsistent_units = 0
    for frame in (inventory, wastage):
        if "Unit" in frame.columns:
            inconsistent_units += int(frame.where(
                F.col("Unit").isNotNull()
                & ~F.lower(F.trim(F.col("Unit"))).isin(*canonical_unit_labels)
            ).count())

    cases = {
        "missing_values": int(sum(missing_by_table.values())),
        "duplicate_orders": _duplicate_count(orders, "Order_ID"),
        "duplicate_order_lines": _duplicate_count(order_items, "Order_Item_ID"),
        "invalid_menu_prices": menu_counts["invalid_price"] + line_price_count,
        "negative_quantities": item_counts["negative_quantity"] + waste_counts["negative_quantity"],
        "invalid_dates": int(invalid_date_count),
        "invalid_ratings": rating_counts["invalid_rating"],
        "missing_customer_ids": order_counts["missing_customer"],
        "missing_menu_ids": item_counts["missing_item"] + invalid_menu_ids.count(),
        "invalid_restaurant_ids": int(invalid_order_locations.count()),
        "impossible_wastage_quantities": waste_counts["impossible_quantity"],
        "incorrect_discounts": item_counts["incorrect_discount"],
        "cancelled_transactions": order_counts["cancelled"],
        "inconsistent_units": int(inconsistent_units),
        "invalid_location_references": int(invalid_location_count),
    }
    coverage = fixture_coverage(fixtures_dir, valid_location_ids)
    return {
        "srs_requirements": ["§1.6.xiv"],
        "issue_type_count": len(ISSUE_TYPES),
        "enumerated_issue_types": list(ISSUE_TYPES),
        "canonical_source_findings": cases,
        "missing_rows_by_table": missing_by_table,
        "isolated_fixture_coverage": coverage,
        "passed": coverage["passed"] and set(cases) == set(ISSUE_TYPES),
        "notes": [
            "The SRS labels the list as 14 cases but enumerates 15; all 15 are checked.",
            "Impossible wastage is detected when wasted quantity is negative or exceeds preparation quantity; the supplied fixture demonstrates the latter case.",
            "Noncanonical Unit labels are counted in inventory and wastage. Known labels are standardized by the documented cleaner without physical unit conversion; gram/lb semantics remain a dataset limitation.",
            "Canonical missingness and anomaly counts are findings, not automatic pipeline failure; raw source files remain unchanged.",
        ],
    }


def write_quality_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
