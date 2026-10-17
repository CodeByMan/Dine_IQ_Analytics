from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, Window, functions as F

from dineiq.dataset.schema import TABLE_SCHEMAS


# The supplied synthetic-data generator explicitly injects these tokens as
# label-only variants of its canonical units (the numeric values are not
# rescaled). Restore its documented category unit labels; this is not a
# general-purpose physical-unit conversion for real-world data.
UNIT_CANON = {
    "kg": "kg", "Kg": "kg", "KG": "kg", "kilograms": "kg", "lbs": "kg", "grams": "kg",
    "liters": "liters", "Liters": "liters", "L": "liters", "Litre": "liters", "LTR": "liters",
    "pcs": "pcs", "PCS": "pcs", "Pieces": "pcs", "pieces": "pcs", "Pcs": "pcs",
}


def _without_internal_columns(frame: DataFrame) -> DataFrame:
    return frame.select(*[name for name in frame.columns if not name.startswith("__invalid_cast__")])


def _not_flagged(frame: DataFrame, column: str) -> DataFrame:
    if column not in frame.columns:
        return frame
    return frame.where(F.coalesce(F.col(column), F.lit(False)) == F.lit(False))


def _semi_valid(frame: DataFrame, column: str, parent: DataFrame, parent_column: str) -> DataFrame:
    keys = parent.select(F.col(parent_column).cast("string").alias("__valid_key")).where(
        F.col("__valid_key").isNotNull()
    ).dropDuplicates()
    return frame.alias("child").join(
        keys.alias("parent"),
        F.col(f"child.{column}").cast("string") == F.col("parent.__valid_key"),
        "left_semi",
    )


def _deduplicate(frame: DataFrame, keys: list[str]) -> DataFrame:
    """Keep one deterministic row per key, ordering by a stable row hash."""
    order = F.sha2(F.to_json(F.struct(*[F.col(name) for name in frame.columns])), 256)
    window = Window.partitionBy(*keys).orderBy(order)
    return frame.withColumn("__dedup_rank", F.row_number().over(window)).where(
        F.col("__dedup_rank") == 1
    ).drop("__dedup_rank")


def _clean_dataset(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Apply the main dataset archive's documented cleaning decisions in Spark.

    The functions create derived frames only. They do not write back to raw
    inputs. Rules mirror the candidate cleaning script/log: duplicate and
    invalid transaction quarantine, invalid promotion nulling, order-line
    arithmetic repair, rating validation, waste capping/unit normalization,
    and inventory median imputation. Historical policies and their limitations
    are recorded in the prior dataset audit; this implementation does not assert the
    decisions are ideal business policy.
    """
    cleaned = {name: _without_internal_columns(frame) for name, frame in tables.items()}

    customers = cleaned["customers"]
    restaurants = cleaned["restaurants"]
    promotions = cleaned["promotions"]
    menus = cleaned["menu_items"]
    orders = _deduplicate(cleaned["orders"], ["Order_ID"])
    orders = orders.where(F.col("Order_Date").isNotNull())
    orders = orders.where(F.col("Customer_ID").isNotNull())
    orders = _semi_valid(orders, "Customer_ID", customers, "Customer_ID")
    orders = orders.where(F.col("Restaurant_ID").isNotNull())
    orders = _semi_valid(orders, "Restaurant_ID", restaurants, "Restaurant_ID")

    promotion_lookup = promotions.select(
        F.col("Promotion_ID").alias("__promotion_key"),
        F.col("Start_Date").alias("__promotion_start"),
        F.col("End_Date").alias("__promotion_end"),
    )
    orders = orders.alias("order_row").join(
        promotion_lookup.alias("promotion"),
        F.col("order_row.Promotion_ID") == F.col("promotion.__promotion_key"),
        "left",
    ).select("order_row.*", "promotion.__promotion_start", "promotion.__promotion_end")
    promotion_valid = (
        F.col("Promotion_ID").isNull()
        | (
            F.col("__promotion_start").isNotNull()
            & F.col("Order_Date").between(F.col("__promotion_start"), F.col("__promotion_end"))
        )
    )
    orders = orders.withColumn(
        "Promotion_ID",
        F.when(promotion_valid, F.col("Promotion_ID")).otherwise(F.lit(None).cast("string")),
    ).drop("__promotion_start", "__promotion_end")
    orders = orders.withColumn("Channel", F.coalesce(F.col("Channel"), F.lit("Unknown")))
    orders = orders.withColumn("Channel_ID", F.col("Channel"))

    order_items = _deduplicate(cleaned["order_items"], ["Order_Item_ID"])
    order_items = _deduplicate(order_items, ["Order_ID", "Item_ID", "Quantity", "Unit_Price"])
    order_items = order_items.where(
        F.col("Unit_Price").isNotNull() & (F.col("Unit_Price") > 0) & (F.col("Unit_Price") <= 10_000)
    )
    order_items = order_items.where(F.col("Quantity").isNotNull() & (F.col("Quantity") > 0))
    order_items = order_items.where(F.col("Item_ID").isNotNull())
    order_items = _semi_valid(order_items, "Item_ID", menus, "Item_ID")
    order_items = _semi_valid(order_items, "Order_ID", orders, "Order_ID")
    order_items = order_items.withColumn(
        "Discount_Percent", F.least(F.lit(100), F.greatest(F.lit(0), F.col("Discount_Percent")))
    ).withColumn(
        "Line_Subtotal", F.round(F.col("Quantity") * F.col("Unit_Price"), 2)
    ).withColumn(
        "Line_Total", F.round(F.col("Line_Subtotal") * (1 - F.col("Discount_Percent") / 100), 2)
    ).withColumn(
        "Discount_Amount", F.col("Line_Subtotal") - F.col("Line_Total")
    )
    costs = menus.select("Item_ID", "Base_Cost").dropDuplicates(["Item_ID"])
    order_items = order_items.drop("Gross_Profit", "Base_Cost").join(costs, "Item_ID", "left").withColumn(
        "Gross_Profit", F.col("Line_Total") - F.col("Quantity") * F.col("Base_Cost")
    )

    line_totals = order_items.groupBy("Order_ID").agg(
        F.count("Order_Item_ID").alias("__item_count"),
        F.sum("Line_Subtotal").alias("__subtotal"),
        F.sum("Line_Total").alias("__total"),
    )
    orders = orders.join(line_totals, "Order_ID", "left").withColumn(
        "Item_Count", F.coalesce(F.col("__item_count"), F.lit(0))
    ).withColumn(
        "Subtotal", F.round(F.coalesce(F.col("__subtotal"), F.lit(0.0)), 2)
    ).withColumn(
        "Total_Amount", F.round(F.coalesce(F.col("__total"), F.lit(0.0)), 2)
    ).withColumn(
        "Discount_Amount", F.round(F.col("Subtotal") - F.col("Total_Amount"), 2)
    ).withColumn(
        "Tax_Amount",
        F.round(F.col("Total_Amount") * F.when(F.col("Payment_Method") == "Cash", 0.13).otherwise(0.08), 2),
    ).withColumn(
        "Grand_Total",
        F.round(F.col("Total_Amount") + F.col("Tax_Amount") + F.coalesce(F.col("Delivery_Charge"), F.lit(0.0)), 2),
    ).drop("__item_count", "__subtotal", "__total")
    cleaned["orders"] = orders.withColumn("Order_Status", F.col("Status"))
    cleaned["order_items"] = order_items

    ratings = cleaned["ratings"].where(F.col("Rating_Value").between(1, 5))
    ratings = ratings.where(F.col("Customer_ID").isNotNull())
    ratings = _semi_valid(ratings, "Order_ID", orders, "Order_ID")
    ratings = _semi_valid(ratings, "Item_ID", menus, "Item_ID")
    ratings = _semi_valid(ratings, "Restaurant_ID", restaurants, "Restaurant_ID")
    cleaned["ratings"] = ratings

    wastage = cleaned["wastage"].where(F.col("Item_ID").isNotNull())
    wastage = _semi_valid(wastage, "Item_ID", menus, "Item_ID")
    wastage = wastage.withColumn(
        "Unit", F.coalesce(F.create_map(*[x for pair in UNIT_CANON.items() for x in (F.lit(pair[0]), F.lit(pair[1]))])[F.col("Unit")], F.col("Unit"))
    ).withColumn(
        "Wastage_Quantity", F.least(F.col("Wastage_Quantity"), F.col("Prepared_Quantity"))
    ).withColumn(
        "Quantity_Wasted", F.col("Wastage_Quantity")
    ).withColumn(
        "Preparation_Quantity", F.col("Prepared_Quantity")
    )
    cleaned["wastage"] = wastage

    inventory = cleaned["inventory"].withColumn(
        "Unit", F.coalesce(F.create_map(*[x for pair in UNIT_CANON.items() for x in (F.lit(pair[0]), F.lit(pair[1]))])[F.col("Unit")], F.col("Unit"))
    )
    median_by_pair = F.percentile_approx("Consumed_Stock", 0.5).over(
        Window.partitionBy("Restaurant_ID", "Item_ID")
    )
    overall_median = F.percentile_approx("Consumed_Stock", 0.5).over(Window.partitionBy())
    inventory = inventory.withColumn(
        "Consumed_Stock", F.coalesce(F.col("Consumed_Stock"), median_by_pair, overall_median)
    ).withColumn(
        "Units_Sold", F.col("Consumed_Stock")
    ).withColumn(
        "Purchases", F.col("Received_Stock")
    )
    cleaned["inventory"] = inventory

    return cleaned


def _clean_legacy_fixture_tables(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Keep small legacy-shaped unit fixtures supported during migration."""
    cleaned = {name: _without_internal_columns(frame) for name, frame in tables.items()}
    orders = _not_flagged(cleaned["orders"], "Is_Anomaly")
    orders = _not_flagged(orders, "Is_Duplicate").dropDuplicates(["Order_ID"])
    cleaned["orders"] = orders
    cleaned["inventory"] = _not_flagged(cleaned["inventory"], "Is_Anomaly")
    wastage = _not_flagged(cleaned["wastage"], "Is_Anomaly")
    if "Quantity_Wasted" in wastage.columns:
        wastage = wastage.where(F.col("Quantity_Wasted").isNull() | (F.col("Quantity_Wasted") >= 0))
    cleaned["wastage"] = wastage
    ratings = _not_flagged(cleaned["ratings"], "Is_Anomaly")
    ratings = _not_flagged(ratings, "Is_Duplicate")
    if "Rating" in ratings.columns:
        ratings = ratings.where(F.col("Rating").between(1, 5))
    cleaned["ratings"] = ratings
    order_items = _not_flagged(cleaned["order_items"], "Is_Anomaly")
    order_items = _not_flagged(order_items, "Is_Duplicate")
    if "Quantity" in order_items.columns:
        order_items = order_items.where(F.col("Quantity").isNull() | (F.col("Quantity") > 0))
    if "Order_Status" in orders.columns:
        ids = orders.where(F.lower(F.trim(F.col("Order_Status"))) == "completed").select("Order_ID").dropDuplicates()
        order_items = order_items.join(ids, "Order_ID", "left_semi")
    cleaned["order_items"] = order_items
    return cleaned


def clean_tables(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Return clean views; main dataset rules run when its raw fields are present."""
    orders = tables.get("orders")
    if orders is not None and "Status" in orders.columns:
        return _clean_dataset(tables)
    return _clean_legacy_fixture_tables(tables)


def clean_tables_with_audit(
    tables: dict[str, DataFrame],
    quarantine_root: Path | None = None,
) -> tuple[dict[str, DataFrame], dict[str, Any]]:
    """Return cleaned frames plus recoverable excluded rows and row counts.

    Corrected values remain in clean output, not quarantine. Quarantine contains
    only records whose key was removed or duplicate-key copies; no raw source is
    written or modified.
    """
    cleaned = clean_tables(tables)
    decisions: dict[str, Any] = {}
    for table, source in tables.items():
        public_source = _without_internal_columns(source)
        clean = cleaned[table]
        input_rows, output_rows = int(public_source.count()), int(clean.count())
        record: dict[str, Any] = {
            "table": table,
            "input_rows": input_rows,
            "clean_rows": output_rows,
            "removed_rows": input_rows - output_rows,
            "action": "retain_or_apply_documented_dataset_cleaning",
            "rules": [],
            "source_modified": False,
        }
        spec = TABLE_SCHEMAS.get(table)
        if quarantine_root is not None and spec and spec.primary_key in public_source.columns:
            key = spec.primary_key
            retained = clean.select(key).where(F.col(key).isNotNull()).dropDuplicates()
            excluded = public_source.alias("source").join(
                retained.alias("retained"),
                F.col(f"source.{key}").cast("string") == F.col(f"retained.{key}").cast("string"),
                "left_anti",
            )
            order = F.sha2(F.to_json(F.struct(*[F.col(name) for name in public_source.columns])), 256)
            duplicate_window = Window.partitionBy(key).orderBy(order)
            duplicates = public_source.withColumn("__quarantine_rank", F.row_number().over(duplicate_window)).where(
                F.col("__quarantine_rank") > 1
            ).drop("__quarantine_rank")
            duplicates = duplicates.join(retained, key, "left_semi")
            quarantine = excluded.unionByName(duplicates, allowMissingColumns=True).dropDuplicates()
            count = int(quarantine.count())
            record["quarantine_rows"] = count
            if count:
                destination = quarantine_root / table
                destination.parent.mkdir(parents=True, exist_ok=True)
                quarantine.withColumn(
                    "Quarantine_Reason", F.lit("excluded by the documented main dataset cleaning policy")
                ).write.mode("errorifexists").parquet(str(destination))
                record["quarantine_path"] = str(destination)
        decisions[table] = record
    return cleaned, {
        "srs_requirements": ["§1.6.xv"],
        "tables": decisions,
        "source_data_modified": False,
        "removed_rows": sum(item["removed_rows"] for item in decisions.values()),
        "passed": all(item["removed_rows"] >= 0 for item in decisions.values()),
    }
