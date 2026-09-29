"""Role-aware Streamlit CRUD screens for SRS operational records."""
from __future__ import annotations

import sqlite3
from typing import Any

import streamlit as st
import pandas as pd

from dineiq.db.auth import create_user
from dineiq.db.permissions import OPERATIONS, ROLES, Principal, allowed
from dineiq.db.repository import ENTITY_FIELDS, deactivate_record, list_records, save_record
from dineiq.db.schema import connect_database
from dineiq.ui.components import render_frame_visual_summary

FIELD_DEFAULTS = {
    "Is_Active": "1", "Is_Available": "1", "Opening_Stock": "0", "Purchases": "0",
    "Units_Sold": "0", "Wastage_Quantity": "0", "Closing_Stock": "0", "Reorder_Level": "0",
    "Adjustments": "0", "Stockout_Flag": "0", "Discount_Amount": "0",
}

LABELS = {
    "locations": "Restaurant locations", "menu_categories": "Menu categories", "menu_items": "Menu items",
    "pricing_history": "Historical menu prices", "customers": "Customer profiles (pseudonymous)",
    "orders": "Orders", "order_items": "Order lines", "promotions": "Promotions and campaigns",
    "ratings": "Ratings and reviews", "inventory": "Inventory", "wastage": "Wastage records",
}


def _input(label: str, default: Any = None, *, required: bool = False) -> Any:
    text = "" if default is None else str(default)
    suffix = " *" if required else ""
    return st.text_input(f"{label}{suffix}", value=text, help="Required" if required else "Leave blank for no value")


def _form_values(entity: str, fields: tuple[str, ...], existing: dict[str, Any] | None = None,
                 omit_identifier: bool = False) -> dict[str, Any]:
    existing = existing or {}
    values: dict[str, Any] = {}
    for field in fields:
        if omit_identifier and field == fields[0]:
            continue
        required = field == fields[0] or field in {
            "Restaurant_Name", "Category_Name", "Item_Name", "Selling_Price", "New_Price",
            "Effective_From", "Customer_ID", "Location_ID", "Order_Date", "Total_Amount", "Order_Status",
            "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Line_Total", "Promotion_Name",
            "Start_Date", "End_Date", "Rating", "Review_Date", "Inventory_Date", "Wastage_Date",
            "Quantity_Wasted", "Unit_Cost", "Wastage_Cost",
        }
        default = existing.get(field) if field in existing else FIELD_DEFAULTS.get(field)
        values[field] = _input(field.replace("_", " "), default, required=required)
    return values


def _admin_users(database_path: str, principal: Principal) -> None:
    if principal.role != "administrator":
        return
    st.subheader("User access administration")
    with connect_database(database_path) as connection:
        users = connection.execute("SELECT user_id, username, role, assigned_location_id, is_active, created_at FROM users ORDER BY username").fetchall()
    user_frame = pd.DataFrame([dict(row) for row in users])
    st.dataframe(user_frame, width="stretch", hide_index=True)
    render_frame_visual_summary(user_frame, "User roles and account activity")
    with st.expander("Create a user account"):
        with st.form("create-dineiq-user"):
            username = st.text_input("Username")
            password = st.text_input("Temporary password (12+ characters)", type="password")
            role = st.selectbox("Role", ROLES)
            locations = list_records(database_path, "locations", principal)
            loc_id = st.selectbox("Assigned location for restaurant manager", [""] + [str(r["Location_ID"]) for r in locations])
            submitted = st.form_submit_button("Create account")
        if submitted:
            try:
                create_user(database_path, username, password, role, loc_id or None, actor_role=principal.role)
                st.success("User account created.")
                st.rerun()
            except (ValueError, PermissionError, sqlite3.IntegrityError) as exc:
                st.error(str(exc))


def render_management(database_path: str, principal: Principal) -> None:
    st.header("Operational data management")
    st.caption(f"Signed in as {principal.username} · role: {principal.role.replace('_', ' ').title()}")
    choices = [entity for entity in ENTITY_FIELDS if allowed(principal.role, entity, "read")]
    entity = st.selectbox("Record type", choices, format_func=lambda value: LABELS[value])
    records = list_records(database_path, entity, principal)
    st.subheader(f"{LABELS[entity]} · {len(records):,} shown (maximum 500)")
    record_frame = pd.DataFrame(records)
    st.dataframe(record_frame, width="stretch", hide_index=True)
    render_frame_visual_summary(record_frame, f"{LABELS[entity]} visual summary")
    can_create = allowed(principal.role, entity, "create")
    can_update = allowed(principal.role, entity, "update")
    can_deactivate = allowed(principal.role, entity, "deactivate")
    if can_create:
        with st.expander("Add a record", expanded=not records):
            with st.form(f"create-{entity}"):
                values = _form_values(entity, ENTITY_FIELDS[entity])
                submitted = st.form_submit_button("Save record", type="primary")
            if submitted:
                try:
                    save_record(database_path, entity, values, principal)
                    st.success("Record saved.")
                    st.rerun()
                except (ValueError, PermissionError, sqlite3.IntegrityError) as exc:
                    st.error(f"Record was not saved: {exc}")
    if records and (can_update or can_deactivate):
        ids = [str(row[ENTITY_FIELDS[entity][0]]) for row in records]
        selected = st.selectbox("Existing record", ids, key=f"select-{entity}")
        current = next(row for row in records if str(row[ENTITY_FIELDS[entity][0]]) == selected)
        if can_update:
            with st.expander("Edit selected record"):
                with st.form(f"update-{entity}"):
                    values = _form_values(entity, ENTITY_FIELDS[entity], current, omit_identifier=True)
                    submitted = st.form_submit_button("Update record")
                if submitted:
                    try:
                        save_record(database_path, entity, values, principal, record_id=selected)
                        st.success("Record updated.")
                        st.rerun()
                    except (ValueError, PermissionError, sqlite3.IntegrityError, LookupError) as exc:
                        st.error(f"Record was not updated: {exc}")
        if can_deactivate and "Is_Active" in current and bool(current["Is_Active"]):
            if st.button("Deactivate selected record", key=f"deactivate-{entity}"):
                try:
                    deactivate_record(database_path, entity, selected, principal)
                    st.success("Record deactivated; the record remains in the audit history.")
                    st.rerun()
                except (ValueError, PermissionError, LookupError, sqlite3.IntegrityError) as exc:
                    st.error(f"Record was not deactivated: {exc}")
    _admin_users(database_path, principal)
