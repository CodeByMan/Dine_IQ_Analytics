"""Restaurant comparisons and location-specific menu classes."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


MENU_CLASSES = ("Profit Driver", "Volume Driver", "Hidden Opportunity", "Low Performer")


def _completed_orders(tables: dict[str, DataFrame]) -> DataFrame:
    return tables["orders"].where(
        F.lower(F.trim("Order_Status")) == "completed"
    ).select(
        "Order_ID", "Customer_ID", "Location_ID", "Promotion_ID", "Channel_ID",
        F.to_date("Order_Date").alias("order_date"), F.col("Total_Amount").cast("double").alias("order_revenue"),
    )


def build_location_comparison(tables: dict[str, DataFrame]) -> DataFrame:
    """Compare revenue, margin, customers, repeat, wastage, ratings and promos."""
    orders = _completed_orders(tables)
    lines = tables["order_items"].select("Order_ID", "Line_Total", "Gross_Profit", "Quantity")
    line_totals = lines.groupBy("Order_ID").agg(
        F.sum("Line_Total").alias("line_revenue"), F.sum("Gross_Profit").alias("contribution_margin"),
        F.sum("Quantity").alias("units_sold"),
    )
    order_facts = orders.join(line_totals, "Order_ID", "left").fillna(
        0, subset=["line_revenue", "contribution_margin", "units_sold"]
    )
    base = order_facts.groupBy("Location_ID").agg(
        F.countDistinct("Order_ID").alias("order_count"),
        F.sum("order_revenue").alias("revenue"),
        F.sum("contribution_margin").alias("contribution_margin"),
        F.avg("order_revenue").alias("average_order_value"),
        F.countDistinct("Customer_ID").alias("customer_count"),
        F.sum(F.when(F.col("Promotion_ID").isNotNull() & (F.trim("Promotion_ID") != ""), 1).otherwise(0)).alias("promoted_order_count"),
        F.sum(F.when(F.col("Promotion_ID").isNotNull() & (F.trim("Promotion_ID") != ""), F.col("order_revenue")).otherwise(0)).alias("promoted_revenue"),
        F.sum(F.when(F.col("Promotion_ID").isNotNull() & (F.trim("Promotion_ID") != ""), F.col("contribution_margin")).otherwise(0)).alias("promoted_margin"),
        F.sum(F.when(F.col("Promotion_ID").isNull() | (F.trim("Promotion_ID") == ""), 1).otherwise(0)).alias("nonpromoted_order_count"),
        F.sum(F.when(F.col("Promotion_ID").isNull() | (F.trim("Promotion_ID") == ""), F.col("contribution_margin")).otherwise(0)).alias("nonpromoted_margin"),
    )
    repeats = order_facts.where(F.col("Customer_ID").isNotNull()).groupBy(
        "Location_ID", "Customer_ID"
    ).agg(F.countDistinct("Order_ID").alias("customer_orders")).groupBy("Location_ID").agg(
        F.sum(F.when(F.col("customer_orders") >= 2, 1).otherwise(0)).alias("repeat_customer_count")
    )
    waste = tables["wastage"].groupBy("Location_ID").agg(
        F.sum("Quantity_Wasted").alias("wastage_quantity"),
        F.sum("Total_Wastage_Cost").alias("wastage_cost"),
    )
    ratings = tables["ratings"].where(F.col("Rating").between(1, 5)).groupBy("Location_ID").agg(
        F.avg("Rating").alias("average_rating"), F.count("Rating").alias("rating_count")
    )
    restaurant = tables["restaurants"].select("Location_ID", "Restaurant_Name", "City", "Province")
    result = restaurant.join(base, "Location_ID", "left").join(repeats, "Location_ID", "left").join(
        waste, "Location_ID", "left"
    ).join(ratings, "Location_ID", "left").fillna(0, subset=[
        "order_count", "revenue", "contribution_margin", "customer_count", "promoted_order_count",
        "promoted_revenue", "promoted_margin", "nonpromoted_order_count", "nonpromoted_margin",
        "repeat_customer_count", "wastage_quantity", "wastage_cost", "rating_count",
    ]).withColumn(
        "repeat_customer_rate", F.when(F.col("customer_count") > 0, F.col("repeat_customer_count") / F.col("customer_count"))
    ).withColumn(
        "promotion_order_share", F.when(F.col("order_count") > 0, F.col("promoted_order_count") / F.col("order_count"))
    ).withColumn(
        "promotion_revenue_per_order", F.when(F.col("promoted_order_count") > 0, F.col("promoted_revenue") / F.col("promoted_order_count"))
    ).withColumn(
        "promotion_margin_per_order", F.when(F.col("promoted_order_count") > 0, F.col("promoted_margin") / F.col("promoted_order_count"))
    ).withColumn(
        "nonpromotion_margin_per_order", F.when(F.col("nonpromoted_order_count") > 0, F.col("nonpromoted_margin") / F.col("nonpromoted_order_count"))
    ).withColumn(
        "promotion_margin_per_order_change_pct", F.when(
            F.col("nonpromotion_margin_per_order") != 0,
            (F.col("promotion_margin_per_order") - F.col("nonpromotion_margin_per_order"))
            / F.abs("nonpromotion_margin_per_order") * 100,
        )
    ).withColumn(
        "margin_pct", F.when(F.col("revenue") != 0, F.col("contribution_margin") / F.col("revenue") * 100)
    ).withColumn(
        "wastage_cost_per_order", F.when(F.col("order_count") > 0, F.col("wastage_cost") / F.col("order_count"))
    ).orderBy(F.col("revenue").desc())
    return result


def build_location_menu_performance(tables: dict[str, DataFrame]) -> DataFrame:
    """Classify each sold item at each location using multi-factor percentiles."""
    orders = _completed_orders(tables).select("Order_ID", "Location_ID", "Customer_ID")
    lines = tables["order_items"].select("Order_ID", "Item_ID", "Quantity", "Line_Total", "Gross_Profit")
    sales = lines.where(F.col("Quantity") > 0).join(orders, "Order_ID", "inner")
    item_location = sales.groupBy("Item_ID", "Location_ID").agg(
        F.sum("Quantity").alias("units_sold"), F.sum("Line_Total").alias("revenue"),
        F.sum("Gross_Profit").alias("contribution_margin"), F.countDistinct("Order_ID").alias("order_frequency"),
        F.countDistinct("Customer_ID").alias("customer_count"),
    ).withColumn(
        "profit_pct", F.when(F.col("revenue") != 0, F.col("contribution_margin") / F.col("revenue") * 100)
    )
    repeat = sales.where(F.col("Customer_ID").isNotNull()).groupBy(
        "Item_ID", "Location_ID", "Customer_ID"
    ).agg(F.countDistinct("Order_ID").alias("orders_per_customer")).groupBy("Item_ID", "Location_ID").agg(
        F.countDistinct(F.when(F.col("orders_per_customer") >= 2, F.col("Customer_ID"))).alias("repeat_customers"),
        F.countDistinct("Customer_ID").alias("item_customers"),
    ).withColumn("repeat_purchase_rate", F.when(F.col("item_customers") > 0,
        F.col("repeat_customers") / F.col("item_customers")))
    ratings = tables["ratings"].where(F.col("Rating").between(1, 5)).groupBy("Item_ID", "Location_ID").agg(
        F.avg("Rating").alias("average_rating")
    )
    waste = tables["wastage"].groupBy("Item_ID", "Location_ID").agg(
        F.sum("Quantity_Wasted").alias("wastage_quantity"),
        F.sum("Preparation_Quantity").alias("preparation_quantity"),
        F.sum("Total_Wastage_Cost").alias("wastage_cost"),
    )
    items = tables["menu_items"].select("Item_ID", "Item_Name", "Category_ID")
    result = item_location.join(items, "Item_ID", "left").join(repeat, ["Item_ID", "Location_ID"], "left").join(
        ratings, ["Item_ID", "Location_ID"], "left"
    ).join(waste, ["Item_ID", "Location_ID"], "left").fillna(
        0, subset=["wastage_quantity", "preparation_quantity", "wastage_cost", "repeat_customers", "item_customers"]
    ).withColumn(
        "wastage_pct", F.when(F.col("preparation_quantity") > 0,
            F.col("wastage_quantity") / F.col("preparation_quantity") * 100)
    )
    by_location = Window.partitionBy("Location_ID")
    result = result.withColumn("profitability_score", (
        F.percent_rank().over(by_location.orderBy(F.col("contribution_margin")))
        + F.percent_rank().over(by_location.orderBy(F.col("profit_pct")))
    ) / 2).withColumn("demand_score", (
        F.percent_rank().over(by_location.orderBy(F.col("units_sold")))
        + F.percent_rank().over(by_location.orderBy(F.col("order_frequency")))
    ) / 2).withColumn("opportunity_score", (
        F.coalesce(F.percent_rank().over(by_location.orderBy(F.col("average_rating"))), F.lit(0.5))
        + F.percent_rank().over(by_location.orderBy(F.col("contribution_margin")))
        + F.coalesce(F.col("repeat_purchase_rate"), F.lit(0.5))
    ) / 3).withColumn(
        "performance_class",
        F.when((F.col("profitability_score") >= 0.60) & (F.col("demand_score") >= 0.40), MENU_CLASSES[0])
        .when(F.col("demand_score") >= 0.60, MENU_CLASSES[1])
        .when((F.col("opportunity_score") >= 0.60) & (F.col("demand_score") < 0.60), MENU_CLASSES[2])
        .otherwise(MENU_CLASSES[3]),
    ).withColumn("classification_scope", F.lit("within_location"))
    return result.orderBy("Location_ID", F.col("performance_class"), F.col("revenue").desc())

