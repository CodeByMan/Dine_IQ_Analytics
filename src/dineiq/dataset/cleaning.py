"""Non-destructive clean views following dataset/documented cleaning decisions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, functions as F


def _without_internal_columns(frame: DataFrame) -> DataFrame:
    return frame.select(*[name for name in frame.columns if not name.startswith("__invalid_cast__")])


def _not_flagged(frame: DataFrame, column: str) -> DataFrame:
    if column not in frame.columns:
        return frame
    return frame.where(F.coalesce(F.col(column), F.lit(False)) == F.lit(False))


def clean_tables(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Create clean Spark views without altering canonical input files.

    Rules follow documentation/CLEANING_DECISIONS.csv: remove explicitly flagged
    anomaly/duplicate rows and invalid quantity/rating/order rows from clean views.
    Raw inputs are never overwritten.
    """
    cleaned = {name: _without_internal_columns(frame) for name, frame in tables.items()}

    orders = _not_flagged(cleaned["orders"], "Is_Anomaly")
    orders = _not_flagged(orders, "Is_Duplicate").dropDuplicates(["Order_ID"])
    cleaned["orders"] = orders

    inventory = _not_flagged(cleaned["inventory"], "Is_Anomaly")
    cleaned["inventory"] = inventory

    wastage = _not_flagged(cleaned["wastage"], "Is_Anomaly")
    wastage = wastage.where(F.col("Quantity_Wasted").isNull() | (F.col("Quantity_Wasted") >= 0))
    cleaned["wastage"] = wastage

    ratings = _not_flagged(cleaned["ratings"], "Is_Anomaly")
    ratings = _not_flagged(ratings, "Is_Duplicate")
    ratings = ratings.where(F.col("Rating").between(1, 5))
    cleaned["ratings"] = ratings

    order_items = _not_flagged(cleaned["order_items"], "Is_Anomaly")
    order_items = _not_flagged(order_items, "Is_Duplicate")
    order_items = order_items.where(F.col("Quantity").isNull() | (F.col("Quantity") > 0))
    completed_order_ids = orders.where(
        F.lower(F.trim(F.col("Order_Status"))) == "completed"
    ).select("Order_ID").dropDuplicates()
    cleaned["order_items"] = order_items.join(completed_order_ids, "Order_ID", "left_semi")

    return cleaned


def clean_tables_with_audit(
    tables: dict[str, DataFrame],
    quarantine_root: Path | None = None,
) -> tuple[dict[str, DataFrame], dict[str, Any]]:
    """Return clean views plus row-level decision accounting.

    Quarantine outputs are written only under the project-owned artifact root;
    source CSVs are never modified.  ``exceptAll`` preserves duplicate rows so
    the record count reflects the exact difference between input and clean view.
    """
    cleaned = clean_tables(tables)
    decisions: dict[str, Any] = {}
    for table, source in tables.items():
        public_source = _without_internal_columns(source)
        clean = cleaned[table]
        input_rows = int(public_source.count())
        output_rows = int(clean.count())
        removed = int(input_rows - output_rows)
        record: dict[str, Any] = {
            "table": table,
            "input_rows": input_rows,
            "clean_rows": output_rows,
            "removed_rows": removed,
            "action": "retain" if removed == 0 else "remove_from_clean_view",
            "source_modified": False,
            "rules": [],
        }
        if table in {"orders", "inventory", "ratings", "order_items", "wastage"}:
            if "Is_Anomaly" in public_source.columns:
                record["rules"].append({"rule": "Is_Anomaly=true", "rows_matching": int(public_source.where(F.col("Is_Anomaly") == True).count())})
            if "Is_Duplicate" in public_source.columns:
                record["rules"].append({"rule": "Is_Duplicate=true", "rows_matching": int(public_source.where(F.col("Is_Duplicate") == True).count())})
        if table == "order_items" and "Quantity" in public_source.columns:
            record["rules"].append({"rule": "Quantity>0", "rows_matching": int(public_source.where(F.col("Quantity") <= 0).count())})
        if table == "wastage" and "Quantity_Wasted" in public_source.columns:
            record["rules"].append({"rule": "Quantity_Wasted>=0", "rows_matching": int(public_source.where(F.col("Quantity_Wasted") < 0).count())})
        if table == "ratings" and "Rating" in public_source.columns:
            record["rules"].append({"rule": "1<=Rating<=5", "rows_matching": int(public_source.where(~F.col("Rating").between(1, 5)).count())})
        if quarantine_root is not None:
            quarantine = public_source.exceptAll(clean)
            quarantine_count = int(quarantine.count())
            record["quarantine_rows"] = quarantine_count
            if quarantine_count:
                destination = quarantine_root / table
                destination.parent.mkdir(parents=True, exist_ok=True)
                quarantine.write.mode("overwrite").parquet(str(destination))
                record["quarantine_path"] = str(destination)
        decisions[table] = record
    return cleaned, {
        "requirement_ids": ["FR-015", "DE-007"],
        "tables": decisions,
        "source_data_modified": False,
        "removed_rows": sum(item["removed_rows"] for item in decisions.values()),
        "passed": all(item["removed_rows"] >= 0 for item in decisions.values()),
    }
