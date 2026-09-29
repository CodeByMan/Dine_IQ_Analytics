"""descriptive analytics descriptive wastage trends and cost summaries (no prediction)."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_wastage_analysis(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    waste = tables["wastage"].where(F.col("Quantity_Wasted").isNull() | (F.col("Quantity_Wasted") >= 0))

    def aggregate(frame: DataFrame, *keys: str) -> DataFrame:
        return frame.groupBy(*keys).agg(
            F.sum("Quantity_Wasted").alias("quantity_wasted"),
            F.sum("Total_Wastage_Cost").alias("wastage_cost"),
            F.sum("Preparation_Quantity").alias("preparation_quantity"),
            F.countDistinct("Wastage_ID").alias("record_count"),
        ).withColumn(
            "wastage_percentage",
            F.when(F.col("preparation_quantity") > 0,
                   F.col("quantity_wasted") / F.col("preparation_quantity") * 100.0),
        )

    monthly = waste.where(F.col("Wastage_Date").isNotNull()).withColumn(
        "year_month", F.date_format("Wastage_Date", "yyyy-MM")
    )
    monthly_trend = monthly.groupBy("year_month").agg(
        F.sum("Quantity_Wasted").alias("quantity_wasted"),
        F.sum("Total_Wastage_Cost").alias("wastage_cost"),
        F.sum("Preparation_Quantity").alias("preparation_quantity"),
        F.countDistinct("Wastage_ID").alias("record_count"),
    ).withColumn(
        "wastage_percentage",
        F.when(F.col("preparation_quantity") > 0, F.col("quantity_wasted") / F.col("preparation_quantity") * 100.0),
    ).orderBy("year_month")

    by_item = aggregate(waste, "Item_ID").orderBy(F.col("wastage_cost").desc_nulls_last())

    by_location = aggregate(waste, "Location_ID").orderBy(F.col("wastage_cost").desc_nulls_last())

    by_reason_shift = waste.groupBy("Wastage_Reason", "Shift").agg(
        F.sum("Quantity_Wasted").alias("quantity_wasted"),
        F.sum("Total_Wastage_Cost").alias("wastage_cost"),
        F.countDistinct("Wastage_ID").alias("record_count"),
    ).orderBy(F.col("wastage_cost").desc_nulls_last())

    # The SRS requires wastage to be inspectable by nine operational
    # dimensions.  Keep the original four artifacts for backward
    # compatibility and add explicit dimension artifacts for the remaining
    # views, each with the same auditable quantity/cost denominator fields.
    menu = tables.get("menu_items")
    if menu is not None and "Category_ID" in menu.columns:
        by_category = aggregate(waste.join(menu.select("Item_ID", "Category_ID"), "Item_ID", "left"), "Category_ID")
    else:
        by_category = aggregate(waste.withColumn("Category_ID", F.lit(None).cast("string")), "Category_ID")

    by_day = aggregate(
        waste.withColumn("wastage_day", F.to_date("Wastage_Date"))
        .withColumn("day_of_week", F.date_format("Wastage_Date", "EEEE")),
        "wastage_day", "day_of_week",
    ).orderBy("wastage_day")

    # Demand context is computed from completed order lines and joined at the
    # item/location grain; null demand is retained rather than converted into
    # a misleading zero.
    orders = tables.get("orders")
    order_items = tables.get("order_items")
    if orders is not None and order_items is not None and {"Order_ID", "Item_ID", "Quantity"}.issubset(order_items.columns):
        demand = order_items.join(
            orders.where(F.lower(F.trim("Order_Status")) == "completed").select("Order_ID", "Location_ID"),
            "Order_ID", "left",
        ).groupBy("Item_ID", "Location_ID").agg(F.sum("Quantity").alias("demand_units"))
        demand_waste = waste.join(demand, ["Item_ID", "Location_ID"], "left").withColumn(
            "demand_bucket",
            F.when(F.col("demand_units").isNull(), "Unknown")
            .when(F.col("demand_units") <= 10, "Low")
            .when(F.col("demand_units") <= 100, "Medium")
            .otherwise("High"),
        )
        by_demand = aggregate(demand_waste, "demand_bucket")
        demand_item = demand_waste.select("Item_ID", "Location_ID", "demand_units").dropDuplicates()
    else:
        by_demand = aggregate(waste.withColumn("demand_bucket", F.lit("Unknown")), "demand_bucket")
        demand_item = None

    inventory = tables.get("inventory")
    if inventory is not None and {"Item_ID", "Location_ID"}.issubset(inventory.columns):
        inventory_columns = [name for name in ("Item_ID", "Location_ID", "Current_Stock", "Reorder_Level", "Stock_Status") if name in inventory.columns]
        inventory_context = inventory.select(*inventory_columns).dropDuplicates(["Item_ID", "Location_ID"])
        inv_waste = waste.join(inventory_context, ["Item_ID", "Location_ID"], "left")
        by_inventory = aggregate(
            inv_waste.withColumn("inventory_status", F.coalesce(F.col("Stock_Status"), F.lit("Unknown")))
            if "Stock_Status" in inv_waste.columns else inv_waste.withColumn("inventory_status", F.lit("Unknown")),
            "inventory_status",
        )
    else:
        by_inventory = aggregate(waste.withColumn("inventory_status", F.lit("Unknown")), "inventory_status")

    if orders is not None and order_items is not None and "Promotion_ID" in orders.columns:
        promotions = order_items.select("Order_ID", "Item_ID").dropDuplicates().join(
            orders.select("Order_ID", "Location_ID", "Promotion_ID"), "Order_ID", "left"
        ).withColumn(
            "promotion_status",
            F.when(F.col("Promotion_ID").isNull() | (F.trim(F.col("Promotion_ID")) == ""), "No promotion")
            .otherwise("Promotion applied"),
        ).select("Item_ID", "Location_ID", "promotion_status").dropDuplicates()
        promo_waste = waste.join(promotions, ["Item_ID", "Location_ID"], "left")
        by_promotion = aggregate(
            promo_waste.withColumn("promotion_status", F.coalesce(F.col("promotion_status"), F.lit("Unknown"))),
            "promotion_status",
        )
    else:
        by_promotion = aggregate(waste.withColumn("promotion_status", F.lit("Unknown")), "promotion_status")

    return {
        "wastage_monthly_trend": monthly_trend,
        "wastage_by_item": by_item,
        "wastage_by_location": by_location,
        "wastage_by_reason_shift": by_reason_shift,
        "wastage_by_category": by_category,
        "wastage_by_day": by_day,
        "wastage_by_demand": by_demand,
        "wastage_by_inventory": by_inventory,
        "wastage_by_promotion": by_promotion,
    }
