from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
import math
from pathlib import Path
from typing import Any, Iterable

import pyarrow.dataset as ds

from dineiq.db.repository import ENTITY_FIELDS
from dineiq.db.schema import connect_database, initialize_database, register_ordering_channels


REFERENCE_TABLES = (
    "locations", "menu_categories", "menu_items", "pricing_history", "promotions",
)
FULL_TABLES = REFERENCE_TABLES + (
    "customers", "orders", "order_items", "ratings", "inventory", "wastage",
)
SOURCE_NAMES = {
    "locations": "restaurants",
    "menu_categories": "menu_categories",
    "menu_items": "menu_items",
    "pricing_history": "pricing_history",
    "promotions": "promotions",
    "customers": "customers",
    "orders": "orders",
    "order_items": "order_items",
    "ratings": "ratings",
    "inventory": "inventory",
    "wastage": "wastage",
}


def _value(value: Any) -> Any:
    """Convert Arrow/Python scalar values to SQLite-safe primitives."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _pick(row: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip() != "":
            return _value(value)
    return default


def _rows(path: Path, *, batch_size: int = 10_000) -> Iterable[list[dict[str, Any]]]:
    dataset = ds.dataset(str(path), format="parquet", partitioning="hive")
    for batch in dataset.to_batches(batch_size=batch_size):
        yield batch.to_pylist()


def _availability(cleaned_root: Path) -> dict[str, int]:
    path = cleaned_root / "menu_item_availability"
    if not path.is_dir():
        return {}
    available: dict[str, int] = {}
    for batch in _rows(path):
        for row in batch:
            item_id = _pick(row, "Item_ID")
            if item_id is not None:
                available[str(item_id)] = max(
                    available.get(str(item_id), 0),
                    int(bool(_pick(row, "Is_Available", default=False))),
                )
    return available


def _transform(entity: str, row: dict[str, Any], availability: dict[str, int]) -> dict[str, Any]:
    if entity == "locations":
        return {
            "Location_ID": _pick(row, "Location_ID", "Restaurant_ID"),
            "Restaurant_Name": _pick(row, "Restaurant_Name"),
            "City": _pick(row, "City"),
            "Region": _pick(row, "Region", "District", "Province"),
            "Address": _pick(row, "Address"),
            "Phone": None,
            "Opening_Date": _pick(row, "Opening_Date"),
            "Seating_Capacity": _pick(row, "Seating_Capacity"),
            "Average_Rating": None,
            "Is_Active": 1,
        }
    if entity == "menu_categories":
        return {
            "Category_ID": _pick(row, "Category_ID"),
            "Category_Name": _pick(row, "Category_Name"),
            "Description": _pick(row, "Description"),
        }
    if entity == "menu_items":
        item_id = _pick(row, "Item_ID")
        return {
            "Item_ID": item_id,
            "Item_Name": _pick(row, "Item_Name"),
            "Category_ID": _pick(row, "Category_ID"),
            "Description": _pick(row, "Description"),
            "Selling_Price": _pick(row, "Selling_Price", "Base_Price"),
            "Ingredient_Cost": _pick(row, "Ingredient_Cost", "Base_Cost"),
            "Is_Available": availability.get(str(item_id), 1),
            "Is_Active": int(bool(_pick(row, "Is_Active", default=True))),
        }
    if entity == "pricing_history":
        return {
            "Price_History_ID": _pick(row, "Price_History_ID", "Pricing_ID"),
            "Item_ID": _pick(row, "Item_ID"),
            # Main-dataset pricing is chain-wide; do not fabricate a branch.
            "Location_ID": None,
            "Effective_From": _pick(row, "Effective_From", "Effective_Date"),
            "Effective_To": _pick(row, "Effective_To", "End_Date"),
            "Old_Price": _pick(row, "Previous_Price"),
            "New_Price": _pick(row, "New_Price", "Price"),
            "Reason": _pick(row, "Reason"),
        }
    if entity == "promotions":
        return {
            "Promotion_ID": _pick(row, "Promotion_ID"),
            "Promotion_Name": _pick(row, "Promotion_Name"),
            "Promotion_Type": _pick(row, "Promotion_Type"),
            "Discount_Percentage": _pick(row, "Discount_Percentage", "Discount_Percent"),
            "Start_Date": _pick(row, "Start_Date"),
            "End_Date": _pick(row, "End_Date"),
            "Is_Active": 1,
            "Applicable_Item_IDs": _pick(row, "Applicable_Item_IDs"),
            "Description": _pick(row, "Description"),
        }
    if entity == "customers":
        # Deliberately exclude direct names/contact fields from operational storage.
        return {
            "Customer_ID": _pick(row, "Customer_ID"),
            "Customer_Segment": _pick(row, "Customer_Segment", "customer_segment"),
            "Signup_Date": _pick(row, "Signup_Date"),
            "Total_Orders": _pick(row, "Total_Orders", "total_orders"),
            "Total_Spend": _pick(row, "Total_Spend", "total_spend"),
            "Average_Order_Value": _pick(row, "Average_Order_Value", "average_order_value"),
            "Last_Order_Date": _pick(row, "Last_Order_Date", "last_order_date"),
            "Recency_Days": _pick(row, "Recency_Days", "recency_days"),
            "Frequency_Score": _pick(row, "Frequency_Score", "frequency_score"),
            "Monetary_Score": _pick(row, "Monetary_Score", "monetary_score"),
        }
    if entity == "orders":
        return {
            "Order_ID": _pick(row, "Order_ID"),
            "Customer_ID": _pick(row, "Customer_ID"),
            "Location_ID": _pick(row, "Location_ID", "Restaurant_ID"),
            "Promotion_ID": _pick(row, "Promotion_ID"),
            "Order_Date": _pick(row, "Order_Date"),
            "Order_Time": _pick(row, "Order_Time"),
            "Total_Amount": _pick(row, "Total_Amount"),
            "Order_Status": _pick(row, "Order_Status", "Status"),
            "Channel_ID": _pick(row, "Channel_ID", "Channel"),
            "Payment_Method": _pick(row, "Payment_Method"),
            "Cancellation_Reason": _pick(row, "Cancellation_Reason"),
        }
    if entity == "order_items":
        subtotal = _pick(row, "Line_Subtotal")
        total = _pick(row, "Line_Total")
        discount = _pick(row, "Discount_Amount")
        if discount is None and subtotal is not None and total is not None:
            discount = max(0.0, float(subtotal) - float(total))
        return {
            "Order_Item_ID": _pick(row, "Order_Item_ID"),
            "Order_ID": _pick(row, "Order_ID"),
            "Item_ID": _pick(row, "Item_ID"),
            "Quantity": _pick(row, "Quantity"),
            "Unit_Price": _pick(row, "Unit_Price"),
            "Discount_Amount": discount if discount is not None else 0.0,
            "Line_Total": total,
            "Gross_Profit": _pick(row, "Gross_Profit"),
        }
    if entity == "ratings":
        return {
            "Rating_ID": _pick(row, "Rating_ID"),
            "Customer_ID": _pick(row, "Customer_ID"),
            "Order_ID": _pick(row, "Order_ID"),
            "Item_ID": _pick(row, "Item_ID"),
            "Location_ID": _pick(row, "Location_ID", "Restaurant_ID"),
            "Rating": _pick(row, "Rating", "Rating_Value"),
            "Review_Date": _pick(row, "Review_Date", "Rating_Date"),
        }
    if entity == "inventory":
        closing = _pick(row, "Closing_Stock", default=0.0)
        return {
            "Inventory_ID": _pick(row, "Inventory_ID"),
            "Item_ID": _pick(row, "Item_ID"),
            "Location_ID": _pick(row, "Location_ID", "Restaurant_ID"),
            "Inventory_Date": _pick(row, "Inventory_Date", "Month"),
            "Opening_Stock": _pick(row, "Opening_Stock", default=0.0),
            "Purchases": _pick(row, "Purchases", "Received_Stock", default=0.0),
            "Units_Sold": _pick(row, "Units_Sold", "Consumed_Stock", default=0.0),
            "Wastage_Quantity": _pick(row, "Wastage_Quantity", default=0.0),
            "Closing_Stock": closing,
            "Reorder_Level": _pick(row, "Reorder_Level", default=0.0),
            "Adjustments": _pick(row, "Adjustments", default=0.0),
            "Stockout_Flag": int(float(closing) <= 0),
        }
    if entity == "wastage":
        quantity = _pick(row, "Quantity_Wasted", "Wastage_Quantity")
        cost = _pick(row, "Wastage_Cost", "Total_Wastage_Cost")
        unit_cost = _pick(row, "Unit_Cost")
        if unit_cost is None and quantity not in (None, 0) and cost is not None:
            unit_cost = float(cost) / float(quantity)
        elif unit_cost is None and quantity == 0 and cost == 0:
            unit_cost = 0.0
        return {
            "Wastage_ID": _pick(row, "Wastage_ID"),
            "Item_ID": _pick(row, "Item_ID"),
            "Location_ID": _pick(row, "Location_ID", "Restaurant_ID"),
            "Wastage_Date": _pick(row, "Wastage_Date", "Date"),
            "Quantity_Wasted": quantity,
            "Unit_Cost": unit_cost,
            "Wastage_Cost": cost,
            "Reason": _pick(row, "Reason", "Wastage_Reason"),
            "Preparation_Quantity": _pick(row, "Preparation_Quantity", "Prepared_Quantity"),
        }
    raise ValueError(f"Unsupported operational import entity: {entity}")


def _insert_batch(connection, entity: str, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    if entity == "orders":
        register_ordering_channels(connection, (record.get("Channel_ID") for record in records))
    fields = ENTITY_FIELDS[entity]
    identifier = fields[0]
    prepared = []
    for record in records:
        if record.get(identifier) in (None, ""):
            raise ValueError(f"Cleaned {entity} row has no {identifier}")
        prepared.append(tuple(_value(record.get(field)) for field in fields))
    columns = ",".join(f'"{field}"' for field in fields)
    placeholders = ",".join("?" for _ in fields)
    before = connection.total_changes
    connection.executemany(
        f'INSERT OR IGNORE INTO "{entity}" ({columns}) VALUES ({placeholders})',
        prepared,
    )
    return connection.total_changes - before


def _null_missing_rating_orders(connection, records: list[dict[str, Any]]) -> int:
    """Detach optional rating/order links whose cleaned parent order is absent.

    The operational schema intentionally permits ``ratings.Order_ID`` to be
    NULL.  Retaining the rating with a NULL link preserves every rating row
    without disabling foreign-key enforcement or inventing placeholder orders.
    The authoritative Parquet record remains unchanged.
    """
    order_ids = {
        str(record["Order_ID"])
        for record in records
        if record.get("Order_ID") not in (None, "")
    }
    if not order_ids:
        return 0

    existing: set[str] = set()
    ordered_ids = sorted(order_ids)
    # Stay below SQLite's commonly configured host-parameter limit.
    for start in range(0, len(ordered_ids), 500):
        chunk = ordered_ids[start:start + 500]
        placeholders = ",".join("?" for _ in chunk)
        rows = connection.execute(
            f'SELECT "Order_ID" FROM "orders" WHERE "Order_ID" IN ({placeholders})',
            chunk,
        )
        existing.update(str(row[0]) for row in rows)

    missing = order_ids - existing
    normalized = 0
    for record in records:
        value = record.get("Order_ID")
        if value not in (None, "") and str(value) in missing:
            record["Order_ID"] = None
            normalized += 1
    return normalized


def import_cleaned_database(database_path: str | Path, cleaned_root: str | Path, *,
                            references_only: bool = False) -> dict[str, Any]:
    """Load clean Parquet idempotently; existing operational edits win."""
    database_path = Path(database_path)
    cleaned_root = Path(cleaned_root)
    entities = REFERENCE_TABLES if references_only else FULL_TABLES
    missing = [SOURCE_NAMES[entity] for entity in entities
               if not (cleaned_root / SOURCE_NAMES[entity]).is_dir()]
    if missing:
        raise FileNotFoundError(
            "Cleaned Parquet is missing for: " + ", ".join(missing)
            + ". Run build-data-foundation successfully first."
        )

    initialize_database(database_path)
    availability = _availability(cleaned_root)
    inserted: dict[str, int] = {}
    scanned: dict[str, int] = {}
    totals: dict[str, int] = {}
    normalized_nullable_foreign_keys = {"ratings.Order_ID": 0}
    with connect_database(database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        for entity in entities:
            inserted[entity] = 0
            scanned[entity] = 0
            source_path = cleaned_root / SOURCE_NAMES[entity]
            for batch in _rows(source_path):
                records = [_transform(entity, row, availability) for row in batch]
                if entity == "ratings":
                    normalized_nullable_foreign_keys["ratings.Order_ID"] += (
                        _null_missing_rating_orders(connection, records)
                    )
                scanned[entity] += len(records)
                inserted[entity] += _insert_batch(connection, entity, records)
            totals[entity] = int(connection.execute(
                f'SELECT COUNT(*) FROM "{entity}"'
            ).fetchone()[0])
        connection.execute(
            "INSERT INTO audit_events(actor_user_id,actor_name,action,entity_type,entity_id,outcome,details) "
            "VALUES(NULL,'system','load_database','database',NULL,'success',?)",
            (json.dumps({
                "references_only": references_only,
                "scanned_rows": scanned,
                "inserted_rows": inserted,
                "database_totals": totals,
                "normalized_nullable_foreign_keys": normalized_nullable_foreign_keys,
            }, sort_keys=True),),
        )
    return {
        "references_only": references_only,
        "scanned_rows": scanned,
        "inserted_rows": inserted,
        "database_totals": totals,
        "normalized_nullable_foreign_keys": normalized_nullable_foreign_keys,
        "passed": all(totals[entity] > 0 for entity in entities),
    }
