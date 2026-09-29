"""Validated, parameterized CRUD operations for operational records."""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Mapping

from dineiq.db.permissions import Principal, allowed, scope_location_id
from dineiq.db.schema import connect_database

# UI field metadata and DB table/columns share this allowlist; arbitrary SQL names
# can never be supplied by a caller.
ENTITY_FIELDS: dict[str, tuple[str, ...]] = {
    "locations": ("Location_ID", "Restaurant_Name", "City", "Region", "Address", "Phone", "Opening_Date", "Seating_Capacity", "Average_Rating", "Is_Active"),
    "menu_categories": ("Category_ID", "Category_Name", "Description"),
    "menu_items": ("Item_ID", "Item_Name", "Category_ID", "Description", "Selling_Price", "Ingredient_Cost", "Is_Available", "Is_Active"),
    "pricing_history": ("Price_History_ID", "Item_ID", "Location_ID", "Effective_From", "Effective_To", "Old_Price", "New_Price", "Reason"),
    "customers": ("Customer_ID", "Customer_Segment", "Signup_Date", "Total_Orders", "Total_Spend", "Average_Order_Value", "Last_Order_Date", "Recency_Days", "Frequency_Score", "Monetary_Score"),
    "orders": ("Order_ID", "Customer_ID", "Location_ID", "Promotion_ID", "Order_Date", "Order_Time", "Total_Amount", "Order_Status", "Channel_ID", "Payment_Method", "Cancellation_Reason"),
    "order_items": ("Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Discount_Amount", "Line_Total", "Gross_Profit"),
    "promotions": ("Promotion_ID", "Promotion_Name", "Promotion_Type", "Discount_Percentage", "Start_Date", "End_Date", "Is_Active", "Applicable_Item_IDs", "Description"),
    "ratings": ("Rating_ID", "Customer_ID", "Order_ID", "Item_ID", "Location_ID", "Rating", "Review_Date"),
    "inventory": ("Inventory_ID", "Item_ID", "Location_ID", "Inventory_Date", "Opening_Stock", "Purchases", "Units_Sold", "Wastage_Quantity", "Closing_Stock", "Reorder_Level", "Adjustments", "Stockout_Flag"),
    "wastage": ("Wastage_ID", "Item_ID", "Location_ID", "Wastage_Date", "Quantity_Wasted", "Unit_Cost", "Wastage_Cost", "Reason", "Preparation_Quantity"),
}
TABLES = {entity: entity for entity in ENTITY_FIELDS}
_ID_FIELDS = {entity: fields[0] for entity, fields in ENTITY_FIELDS.items()}
_REQUIRED_FIELDS = {
    "locations": {"Location_ID", "Restaurant_Name"},
    "menu_categories": {"Category_ID", "Category_Name"},
    "menu_items": {"Item_ID", "Item_Name", "Selling_Price"},
    "pricing_history": {"Price_History_ID", "Item_ID", "Effective_From", "New_Price"},
    "customers": {"Customer_ID"},
    "orders": {"Order_ID", "Location_ID", "Order_Date", "Total_Amount", "Order_Status"},
    "order_items": {"Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Line_Total"},
    "promotions": {"Promotion_ID", "Promotion_Name", "Start_Date", "End_Date"},
    "ratings": {"Rating_ID", "Rating", "Review_Date"},
    "inventory": {"Inventory_ID", "Item_ID", "Location_ID", "Inventory_Date"},
    "wastage": {"Wastage_ID", "Item_ID", "Location_ID", "Wastage_Date", "Quantity_Wasted", "Unit_Cost", "Wastage_Cost"},
}


def _validate_values(entity: str, values: Mapping[str, Any]) -> dict[str, Any]:
    if entity not in ENTITY_FIELDS:
        raise ValueError("Unknown record type.")
    unknown = set(values) - set(ENTITY_FIELDS[entity])
    if unknown:
        raise ValueError(f"Unsupported fields: {sorted(unknown)}")
    result = dict(values)
    for key, value in result.items():
        if isinstance(value, str):
            value = value.strip()
            result[key] = value if value != "" else None
    if entity == "customers":
        forbidden = {"Customer_Name", "Synthetic_Contact", "Email", "Phone"}
        if forbidden.intersection(result):
            raise ValueError("Direct customer identifiers/contact fields are not stored.")
    if entity == "promotions" and isinstance(result.get("Applicable_Item_IDs"), (list, tuple, set)):
        result["Applicable_Item_IDs"] = json.dumps(sorted(map(str, result["Applicable_Item_IDs"])))
    return result


def _require_location_assignment(principal: Principal, entity: str) -> None:
    if principal.role == "manager" and entity not in {"locations", "menu_categories", "customers"} and not principal.assigned_location_id:
        raise PermissionError("Restaurant manager account has no assigned location.")


def list_records(database_path: str, entity: str, principal: Principal, *, limit: int = 500) -> list[dict[str, Any]]:
    if entity not in TABLES or not allowed(principal.role, entity, "read"):
        raise PermissionError("Role is not permitted to read this record type.")
    _require_location_assignment(principal, entity)
    columns = ",".join(f'"{name}"' for name in ENTITY_FIELDS[entity])
    sql = f'SELECT {columns} FROM "{TABLES[entity]}"'
    params: tuple[Any, ...] = ()
    scope = scope_location_id(principal, entity)
    if scope and "Location_ID" in ENTITY_FIELDS[entity]:
        sql += ' WHERE "Location_ID"=?'
        params = (scope,)
    elif scope and entity == "order_items":
        sql += ' WHERE "Order_ID" IN (SELECT Order_ID FROM orders WHERE Location_ID=?)'
        params = (scope,)
    sql += f' LIMIT {max(1, min(int(limit), 5000))}'
    with connect_database(database_path) as connection:
        return [dict(row) for row in connection.execute(sql, params).fetchall()]


def save_record(database_path: str, entity: str, values: Mapping[str, Any], principal: Principal,
                *, record_id: str | None = None) -> None:
    operation = "update" if record_id is not None else "create"
    if entity not in TABLES or not allowed(principal.role, entity, operation):
        raise PermissionError(f"Role is not permitted to {operation} this record type.")
    _require_location_assignment(principal, entity)
    data = _validate_values(entity, values)
    id_field = _ID_FIELDS[entity]
    if record_id is None:
        missing = sorted(_REQUIRED_FIELDS[entity] - set(data))
        if missing:
            raise ValueError(f"Missing required form fields: {missing}")
        fields = tuple(data)
        placeholders = ",".join("?" for _ in fields)
        sql = f'INSERT INTO "{TABLES[entity]}" ({",".join(chr(34)+f+chr(34) for f in fields)}) VALUES ({placeholders})'
        params = tuple(data[f] for f in fields)
    else:
        if id_field in data and str(data[id_field]) != str(record_id):
            raise ValueError("Record identifier cannot be changed.")
        data.pop(id_field, None)
        if not data:
            raise ValueError("No editable fields supplied.")
        set_sql = ",".join(f'"{field}"=?' for field in data)
        sql = f'UPDATE "{TABLES[entity]}" SET {set_sql} WHERE "{id_field}"=?'
        params = (*data.values(), record_id)
    with connect_database(database_path) as connection:
        scope = scope_location_id(principal, entity)
        if scope and "Location_ID" in ENTITY_FIELDS[entity]:
            if record_id is not None:
                current = connection.execute(
                    f'SELECT "Location_ID" FROM "{TABLES[entity]}" WHERE "{id_field}"=?', (record_id,)
                ).fetchone()
                if not current or current[0] != scope:
                    raise PermissionError("Record is outside the assigned location.")
                if data.get("Location_ID", scope) != scope:
                    raise PermissionError("Records cannot be moved outside the assigned location.")
            elif data.get("Location_ID") != scope:
                raise PermissionError("New records must use the assigned location.")
        elif scope and entity == "order_items":
            order_id = data.get("Order_ID")
            if record_id is not None:
                current = connection.execute(
                    "SELECT i.Order_ID,o.Location_ID FROM order_items i JOIN orders o ON o.Order_ID=i.Order_ID WHERE i.Order_Item_ID=?",
                    (record_id,),
                ).fetchone()
                if not current or current[1] != scope:
                    raise PermissionError("Order line is outside the assigned location.")
                order_id = order_id or current[0]
            parent = connection.execute("SELECT Location_ID FROM orders WHERE Order_ID=?", (order_id,),).fetchone()
            if not parent or parent[0] != scope:
                raise PermissionError("Order lines must belong to an order at the assigned location.")
        cursor = connection.execute(sql, params)
        if record_id is not None and cursor.rowcount == 0:
            raise LookupError("Record was not found.")
        connection.execute(
            "INSERT INTO audit_events(actor_user_id,actor_name,action,entity_type,entity_id,outcome,details) VALUES(?,?,?,?,?,'success',?)",
            (principal.user_id, principal.username, operation, entity, str(record_id or data.get(id_field, "")),
             json.dumps({"fields": sorted(data)})),
        )


def deactivate_record(database_path: str, entity: str, record_id: str, principal: Principal) -> None:
    if entity not in TABLES or not allowed(principal.role, entity, "deactivate"):
        raise PermissionError("Role is not permitted to deactivate this record type.")
    _require_location_assignment(principal, entity)
    if "Is_Active" not in ENTITY_FIELDS[entity]:
        raise ValueError("This record type has no active status; use update instead.")
    id_field = _ID_FIELDS[entity]
    with connect_database(database_path) as connection:
        scope = scope_location_id(principal, entity)
        sql = f'UPDATE "{TABLES[entity]}" SET Is_Active=0 WHERE "{id_field}"=?'
        params: tuple[Any, ...] = (record_id,)
        if scope and "Location_ID" in ENTITY_FIELDS[entity]:
            sql += " AND Location_ID=?"
            params = (record_id, scope)
        if connection.execute(sql, params).rowcount == 0:
            raise LookupError("Record was not found or is outside the assigned location.")
        connection.execute(
            "INSERT INTO audit_events(actor_user_id,actor_name,action,entity_type,entity_id,outcome,details) VALUES(?,?, 'deactivate', ?, ?, 'success', '{}')",
            (principal.user_id, principal.username, entity, str(record_id)),
        )
