from __future__ import annotations

import re

import pandas as pd


def _lookup(frame: pd.DataFrame, keys: tuple[str, ...], names: tuple[str, ...]) -> dict:
    key = next((x for x in keys if x in frame), None)
    name = next((x for x in names if x in frame), None)
    if key is None or name is None:
        return {}
    rows = frame[[key, name]].dropna().drop_duplicates(key)
    return dict(zip(rows[key].astype(str), rows[name]))


def enrich_names(frame: pd.DataFrame, dimensions: dict) -> pd.DataFrame:
    """Map names without changing identifiers, row order, or business values."""
    result = frame.copy()
    definitions = (
        ("items_dim", ("Item_ID", "item_id", "menu_id"), ("Item_Name", "Food_Name", "Dish_Name"), "Item_Name"),
        ("categories_dim", ("Category_ID", "category_id"), ("Category_Name",), "Category_Name"),
        ("locations_dim", ("Location_ID", "Restaurant_ID", "location_id", "branch_id"),
         ("Branch_Name", "Location_Name", "Restaurant_Name"), "Branch_Name"),
        ("channels_dim", ("Channel_ID", "channel_id", "preferred_channel_id"), ("Channel_Name",), "Channel_Name"),
        ("promotions_dim", ("Promotion_ID", "promotion_id"), ("Promotion_Name",), "Promotion_Name"),
        ("customers_dim", ("Customer_ID", "customer_id"), ("Customer_Name", "Customer_Name_Pseudonymous"), "Customer_Name"),
    )
    for dimension, keys, names, output in definitions:
        source = dimensions.get(dimension, pd.DataFrame())
        column = next((x for x in keys if x in result), None)
        values = _lookup(source, keys, names)
        if column is not None and values:
            mapped = result[column].astype(str).map(values)
            target = output if output in result else next((name for name in names if name in result), output)
            result[target] = result[target].where(result[target].notna(), mapped) if target in result else mapped

    locations = dimensions.get("locations_dim", pd.DataFrame())
    location_column = next((x for x in ("Location_ID", "Restaurant_ID", "location_id", "branch_id", "assigned_location_id") if x in result), None)
    if location_column:
        for output, candidates in (
            ("Area", ("Area", "Locality", "area", "locality")),
            ("Latitude", ("Latitude", "latitude", "Lat", "lat")),
            ("Longitude", ("Longitude", "longitude", "Lon", "Lng", "lon", "lng")),
        ):
            values = _lookup(locations, ("Location_ID", "Restaurant_ID"), candidates)
            if values:
                mapped = result[location_column].astype(str).map(values)
                if mapped.notna().any():
                    target = next((name for name in candidates if name in result), output)
                    result[target] = result[target].where(result[target].notna(), mapped) if target in result else mapped
        if "assigned_location_id" in result and "Branch_Name" not in result:
            values = _lookup(locations, ("Location_ID", "Restaurant_ID"), ("Branch_Name", "Location_Name", "Restaurant_Name"))
            if values:
                result["Branch_Name"] = result["assigned_location_id"].astype(str).map(values)

    items = _lookup(dimensions.get("items_dim", pd.DataFrame()), ("Item_ID",), ("Item_Name", "Food_Name", "Dish_Name"))
    for identifier, output in (("antecedent_item_id", "Antecedent_Item_Name"), ("consequent_item_id", "Consequent_Item_Name")):
        if identifier in result and items:
            mapped = result[identifier].astype(str).map(items)
            if mapped.notna().any():
                result[output] = mapped
    if {"entity_id", "entity_type"}.issubset(result) and items:
        eligible = result["entity_type"].isin(("menu_item", "menu_item_location"))
        mapped = result["entity_id"].astype(str).map(items).where(eligible)
        if mapped.notna().any():
            result["Item_Name"] = result["Item_Name"].where(result["Item_Name"].notna(), mapped) if "Item_Name" in result else mapped
        pairs = result["entity_type"].eq("item_pair")
        def pair_name(value):
            ids = str(value).split("→")
            return " + ".join(str(items[key]) for key in ids) if len(ids) == 2 and all(key in items for key in ids) else None
        names = result["entity_id"].map(pair_name).where(pairs)
        if names.notna().any():
            result["Item_Pair_Name"] = names

    # Recommendation artifacts intentionally contain several entity types in
    # one table. Present one populated subject column instead of separate,
    # mostly-empty item/pair/branch columns.
    if {"entity_id", "entity_type"}.issubset(result):
        entity_ids = result["entity_id"].astype("string")
        entity_types = result["entity_type"].fillna("").astype(str)
        subject = pd.Series(pd.NA, index=result.index, dtype="object")
        item_names = entity_ids.astype(str).map(items) if items else pd.Series(pd.NA, index=result.index)
        pair_names = result.get("Item_Pair_Name", pd.Series(pd.NA, index=result.index))
        promotion_names = entity_ids.astype(str).map(_lookup(
            dimensions.get("promotions_dim", pd.DataFrame()), ("Promotion_ID", "promotion_id"), ("Promotion_Name",)
        ))
        location_names = entity_ids.astype(str).map(_lookup(
            locations, ("Location_ID", "Restaurant_ID"), ("Restaurant_Name", "Branch_Name", "Location_Name")
        ))
        subject = subject.mask(entity_types.isin(("menu_item", "menu_item_location")), item_names)
        subject = subject.mask(entity_types.eq("item_pair"), pair_names)
        subject = subject.mask(entity_types.eq("promotion"), promotion_names)
        subject = subject.mask(entity_types.eq("location"), location_names)
        subject = subject.mask(entity_types.eq("customer_segment"), entity_ids)

        # Add the branch to item/location recommendations without creating a
        # separate sparse branch column in the displayed recommendation table.
        if "location_id" in result:
            branch_by_id = _lookup(
                locations, ("Location_ID", "Restaurant_ID"), ("Restaurant_Name", "Branch_Name", "Location_Name")
            )
            branch = result["location_id"].astype("string").astype(str).map(branch_by_id)
            item_location = entity_types.eq("menu_item_location") & subject.notna() & branch.notna()
            subject = subject.mask(item_location, subject.astype(str) + " · " + branch.astype(str))
        if subject.notna().any():
            result["Entity_Name"] = subject

        # Replace opaque identifiers in the action sentence only when the
        # corresponding real dataset name is known. The analytical artifact is
        # copied before this presentation-only transformation.
        if "recommended_action" in result:
            item_map = items
            location_map = _lookup(
                locations, ("Location_ID", "Restaurant_ID"), ("Restaurant_Name", "Branch_Name", "Location_Name")
            )
            promotion_map = _lookup(
                dimensions.get("promotions_dim", pd.DataFrame()), ("Promotion_ID", "promotion_id"), ("Promotion_Name",)
            )

            def readable_action(row: pd.Series):
                value = row.get("recommended_action")
                if pd.isna(value):
                    return value
                text = str(value)
                replacements: dict[str, object] = {}
                entity_id = str(row.get("entity_id", ""))
                entity_type = str(row.get("entity_type", ""))
                if entity_type in {"menu_item", "menu_item_location"} and entity_id in item_map:
                    replacements[entity_id] = item_map[entity_id]
                elif entity_type == "item_pair":
                    for identifier in entity_id.split("→"):
                        if identifier in item_map:
                            replacements[identifier] = item_map[identifier]
                elif entity_type == "promotion" and entity_id in promotion_map:
                    replacements[entity_id] = promotion_map[entity_id]
                elif entity_type == "location" and entity_id in location_map:
                    replacements[entity_id] = location_map[entity_id]
                location_id = row.get("location_id")
                if pd.notna(location_id) and str(location_id) in location_map:
                    replacements[str(location_id)] = location_map[str(location_id)]
                for identifier in sorted(replacements, key=len, reverse=True):
                    text = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(identifier)}(?![A-Za-z0-9_])",
                                  str(replacements[identifier]), text)
                return text

            result["recommended_action"] = result.apply(readable_action, axis=1)
    return result


def _has_display_values(values: pd.Series) -> bool:
    """Return whether a column contains at least one visible value."""
    populated = values.notna()
    if pd.api.types.is_object_dtype(values.dtype) or pd.api.types.is_string_dtype(values.dtype):
        populated &= values.astype("string").str.strip().fillna("").ne("")
    return bool(populated.any())


def _drop_duplicate_name_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Remove only human-readable helper columns that duplicate another name."""
    result = frame.copy()
    for preferred, duplicate in (("Restaurant_Name", "Branch_Name"), ("Area", "Locality")):
        if preferred in result and duplicate in result:
            left = result[preferred].astype("string").fillna("")
            right = result[duplicate].astype("string").fillna("")
            if left.equals(right):
                result = result.drop(columns=[duplicate])
    return result


def display_frame(frame: pd.DataFrame, *, context: str | None = None) -> pd.DataFrame:
    """Show relevant populated columns with each displayed ID beside its name."""
    working = frame.copy()
    working = working.loc[:, [column for column in working if _has_display_values(working[column])]]
    working = _drop_duplicate_name_columns(working)
    context_text = (context or "").casefold()

    if "recommendation" in context_text:
        # One subject/name pair works for menu items, item pairs, locations,
        # promotions and customer segments without sparse helper columns.
        relevant = (
            "entity_id", "Entity_Name", "entity_type", "recommendation_type", "recommended_action",
            "priority", "potential_business_impact", "supporting_evidence",
        )
        selected = [column for column in relevant if column in working and _has_display_values(working[column])]
        working = working[selected]

    aliases = {
        "Item_Name": "Food Item", "Food_Name": "Food Item", "Dish_Name": "Food Item",
        "Antecedent_Item_Name": "Food Item (antecedent)", "Consequent_Item_Name": "Food Item (consequent)",
        "Item_Pair_Name": "Food Items", "Category_Name": "Category", "Branch_Name": "Branch",
        "Restaurant_Name": "Restaurant", "Location_Name": "Location", "Area": "Area", "Locality": "Area",
        "Customer_Name": "Customer", "Channel_Name": "Ordering Channel", "Promotion_Name": "Promotion",
        "entity_id": "Subject ID", "entity_type": "Subject Type", "Entity_Name": "Recommendation Subject",
        "recommendation_type": "Recommendation Type", "recommended_action": "Recommended Action",
        "potential_business_impact": "Potential Business Impact", "supporting_evidence": "Supporting Evidence",
        "priority": "Priority", "priority_percentile": "Priority Percentile",
        "Latitude": "Latitude", "latitude": "Latitude", "lat": "Latitude", "Lat": "Latitude",
        "Longitude": "Longitude", "longitude": "Longitude", "lon": "Longitude", "lng": "Longitude", "Lng": "Longitude",
    }
    if context is not None:
        aliases.update({
            "Item_ID": "Menu Item ID", "item_id": "Menu Item ID", "menu_id": "Menu Item ID",
            "Category_ID": "Category ID", "category_id": "Category ID",
            "Customer_ID": "Customer ID", "customer_id": "Customer ID",
            "Restaurant_ID": "Restaurant ID", "Promotion_ID": "Promotion ID", "promotion_id": "Promotion ID",
            "Wastage_ID": "Wastage ID", "wastage_id": "Wastage ID",
            "price_event_id": "Price Sensitivity ID", "price_sensitivity_id": "Price Sensitivity ID",
            "antecedent_item_id": "Antecedent Menu Item ID", "consequent_item_id": "Consequent Menu Item ID",
            "Location_ID": "Restaurant ID" if "location performance" in context_text else "Location ID",
            "location_id": "Restaurant ID" if "location performance" in context_text else "Location ID",
        })
    labels, used = {}, set()
    for column in working:
        label = aliases.get(column, column)
        if label in used:
            label = f"{label} ({column})"
        used.add(label)
        labels[column] = label
    result = working.rename(columns=labels).copy()
    priority = ("Food Item", "Food Items", "Food Item (antecedent)", "Food Item (consequent)",
                "Category", "Customer", "Branch", "Restaurant", "Location", "Area", "Latitude", "Longitude")
    if context is not None:
        priority = (
            "Wastage ID", "Price Sensitivity ID", "Menu Item ID", "Food Item",
            "Antecedent Menu Item ID", "Food Item (antecedent)",
            "Consequent Menu Item ID", "Food Item (consequent)", "Food Items",
            "Category ID", "Category", "Restaurant ID", "Location ID", "Restaurant", "Branch", "Location", "Area",
            "Customer ID", "Customer", "Promotion ID", "Promotion", "Recommendation Subject", "Subject ID", "Subject Type",
        )
    if "recommendation" in context_text:
        priority = ("Subject ID", "Recommendation Subject", "Subject Type", "Recommendation Type",
                    "Recommended Action", "Priority", "Potential Business Impact", "Supporting Evidence")
    elif "location performance" in context_text:
        priority = ("Restaurant ID", "Restaurant", "Branch", "Area", "City", "Province", "Latitude", "Longitude")
    if "username" in result:
        priority = ("username", "role", "Branch", "Restaurant", "Location", "user_id", "assigned_location_id")
    first = [name for name in priority if name in result]
    return result[first + [name for name in result if name not in first]]
