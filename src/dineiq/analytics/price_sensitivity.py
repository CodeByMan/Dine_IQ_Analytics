"""Observed historical demand response around effective price changes."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_price_sensitivity(tables: dict[str, DataFrame], window_days: int = 30) -> DataFrame:
    """Estimate item elasticity from daily completed units before/after changes.

    Each price event is compared over equal, configurable windows around its
    effective date. Estimates are descriptive and not causal elasticity claims.
    """
    if window_days < 7 or window_days > 180:
        raise ValueError("window_days must be between 7 and 180")
    orders = tables["orders"].where(
        F.lower(F.trim(F.col("Order_Status"))) == "completed"
    ).select("Order_ID", "Customer_ID", F.to_date("Order_Date").alias("order_date"))
    line_columns = ["Order_ID", "Item_ID", "Quantity", "Line_Total", "Gross_Profit", "Discount_Amount"]
    sales = (
        tables["order_items"].select(*line_columns)
        .where(F.col("Quantity") > 0)
        .join(orders, "Order_ID", "inner")
    )
    daily = sales.groupBy("Item_ID", "order_date").agg(
        F.sum("Quantity").alias("units_sold"),
        F.sum("Line_Total").alias("revenue"),
        F.sum("Gross_Profit").alias("contribution_margin"),
        F.sum("Discount_Amount").alias("discount_amount"),
    )
    customer_item_orders = sales.where(F.col("Customer_ID").isNotNull()).select(
        "Item_ID", "Customer_ID", "Order_ID"
    ).dropDuplicates()
    repeat = customer_item_orders.groupBy("Item_ID", "Customer_ID").agg(
        F.countDistinct("Order_ID").alias("customer_item_order_count")
    ).groupBy("Item_ID").agg(
        F.countDistinct("Customer_ID").alias("item_customers"),
        F.countDistinct(F.when(F.col("customer_item_order_count") >= 2, F.col("Customer_ID"))).alias("repeat_customers"),
    ).withColumn(
        "repeat_purchase_rate", F.when(F.col("item_customers") > 0, F.col("repeat_customers") / F.col("item_customers"))
    ).select("Item_ID", "repeat_purchase_rate")
    rating = tables["ratings"].where(F.col("Rating").between(1, 5)).groupBy("Item_ID").agg(
        F.avg("Rating").alias("average_rating")
    )
    events = tables["pricing_history"].where(
        (F.col("Previous_Price") > 0) & (F.col("New_Price") > 0)
    ).select(
        F.col("Price_History_ID").cast("string").alias("price_event_id"), "Item_ID",
        F.to_date("Effective_From").alias("effective_date"),
        F.col("Previous_Price").cast("double").alias("previous_price"),
        F.col("New_Price").cast("double").alias("new_price"),
    ).where(F.col("effective_date").isNotNull())
    joined = events.join(daily, "Item_ID", "left").where(
        (F.col("order_date") >= F.date_sub(F.col("effective_date"), window_days))
        & (F.col("order_date") < F.date_add(F.col("effective_date"), window_days))
    )
    results = joined.groupBy(
        "price_event_id", "Item_ID", "effective_date", "previous_price", "new_price"
    ).agg(
        F.sum(F.when(F.col("order_date") < F.col("effective_date"), F.col("units_sold")).otherwise(0)).alias("units_before"),
        F.sum(F.when(F.col("order_date") >= F.col("effective_date"), F.col("units_sold")).otherwise(0)).alias("units_after"),
        F.sum(F.when(F.col("order_date") < F.col("effective_date"), F.col("revenue")).otherwise(0)).alias("revenue_before"),
        F.sum(F.when(F.col("order_date") >= F.col("effective_date"), F.col("revenue")).otherwise(0)).alias("revenue_after"),
        F.sum(F.when(F.col("order_date") < F.col("effective_date"), F.col("contribution_margin")).otherwise(0)).alias("margin_before"),
        F.sum(F.when(F.col("order_date") >= F.col("effective_date"), F.col("contribution_margin")).otherwise(0)).alias("margin_after"),
        F.sum(F.when(F.col("order_date") < F.col("effective_date"), F.col("discount_amount")).otherwise(0)).alias("discount_before"),
        F.sum(F.when(F.col("order_date") >= F.col("effective_date"), F.col("discount_amount")).otherwise(0)).alias("discount_after"),
        F.countDistinct(F.when(F.col("order_date") < F.col("effective_date"), F.col("order_date"))).alias("observed_days_before"),
        F.countDistinct(F.when(F.col("order_date") >= F.col("effective_date"), F.col("order_date"))).alias("observed_days_after"),
    ).withColumn(
        "price_change_pct", (F.col("new_price") - F.col("previous_price")) / F.col("previous_price") * 100.0
    ).withColumn(
        "demand_change_pct", F.when(F.col("units_before") > 0, (F.col("units_after") - F.col("units_before")) / F.col("units_before") * 100.0)
    ).withColumn(
        "observed_elasticity", F.when(F.col("price_change_pct") != 0, F.col("demand_change_pct") / F.col("price_change_pct"))
    ).withColumn(
        "revenue_change_pct", F.when(F.col("revenue_before") > 0, (F.col("revenue_after") - F.col("revenue_before")) / F.col("revenue_before") * 100.0)
    ).withColumn(
        "contribution_margin_change_pct", F.when(F.col("margin_before") != 0, (F.col("margin_after") - F.col("margin_before")) / F.abs("margin_before") * 100.0)
    ).withColumn(
        "discount_amount_change", F.col("discount_after") - F.col("discount_before")
    ).withColumn(
        "price_sensitivity_class",
        F.when(F.col("observed_elasticity").isNull(), "Insufficient evidence")
        .when(F.abs("observed_elasticity") >= 1.5, "Highly Price Sensitive")
        .when(F.abs("observed_elasticity") >= 0.75, "Moderately Price Sensitive")
        .otherwise("Low Price Sensitivity"),
    ).withColumn("comparison_window_days", F.lit(window_days))
    return results.join(rating, "Item_ID", "left").join(repeat, "Item_ID", "left")
