"""Ordering-channel behavior and menu preference summaries."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_channel_analysis(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    orders = tables["orders"].where(
        F.lower(F.trim("Order_Status")) == "completed"
    ).select(
        "Order_ID", "Channel_ID", "Customer_ID", "Promotion_ID",
        F.to_date("Order_Date").alias("order_date"), "Order_Time",
        F.col("Total_Amount").cast("double").alias("order_revenue"),
    )
    line_metrics = tables["order_items"].select(
        "Order_ID", "Item_ID", "Quantity", "Line_Total", "Gross_Profit", "Discount_Amount"
    ).groupBy("Order_ID").agg(
        F.countDistinct("Item_ID").alias("distinct_items"), F.sum("Gross_Profit").alias("contribution_margin"),
        F.sum("Line_Total").alias("line_revenue"), F.sum("Discount_Amount").alias("line_discount"),
    )
    facts = orders.join(line_metrics, "Order_ID", "left").fillna(
        0, subset=["distinct_items", "contribution_margin", "line_revenue", "line_discount"]
    ).withColumn("order_hour", F.regexp_extract("Order_Time", r"^(\d{1,2})", 1).cast("int"))
    summary = facts.groupBy("Channel_ID").agg(
        F.countDistinct("Order_ID").alias("order_count"), F.sum("order_revenue").alias("revenue"),
        F.avg("order_revenue").alias("average_order_value"),
        F.sum("contribution_margin").alias("contribution_margin"),
        F.avg("distinct_items").alias("average_basket_size"),
        F.avg("line_discount").alias("average_order_discount"),
        F.avg(F.when(F.col("Promotion_ID").isNotNull() & (F.trim("Promotion_ID") != ""), 1.0).otherwise(0.0)).alias("promotion_order_share"),
        F.countDistinct("Customer_ID").alias("customer_count"),
        F.avg("order_hour").alias("average_order_hour"),
    ).withColumn("margin_pct", F.when(F.col("revenue") != 0,
        F.col("contribution_margin") / F.col("revenue") * 100))
    channel_dim = tables["ordering_channels"].select("Channel_ID", "Channel_Name", "Description")
    channel_summary = channel_dim.join(summary, "Channel_ID", "left").fillna(
        0, subset=["order_count", "revenue", "contribution_margin", "customer_count"]
    ).orderBy(F.col("order_count").desc())

    category_pref = orders.select("Order_ID", "Channel_ID").join(
        tables["order_items"].select("Order_ID", "Item_ID", "Line_Total", "Quantity"), "Order_ID", "inner"
    ).join(tables["menu_items"].select("Item_ID", "Category_ID"), "Item_ID", "inner").groupBy(
        "Channel_ID", "Category_ID"
    ).agg(
        F.sum("Quantity").alias("units_sold"), F.sum("Line_Total").alias("category_revenue"),
        F.countDistinct("Order_ID").alias("order_count"),
    )
    category_dim = tables["menu_categories"].select("Category_ID", "Category_Name")
    channel_category = category_pref.join(channel_dim, "Channel_ID", "left").join(category_dim, "Category_ID", "left")
    from pyspark.sql import Window
    rank_window = Window.partitionBy("Channel_ID").orderBy(F.col("category_revenue").desc(), F.col("Category_ID"))
    channel_category = channel_category.withColumn("preference_rank", F.row_number().over(rank_window))
    peak = facts.where(F.col("order_hour").between(0, 23)).groupBy(
        "Channel_ID", "order_hour"
    ).agg(F.countDistinct("Order_ID").alias("order_count")).withColumn(
        "period_type", F.lit("hour_of_day")
    )
    peak_window = Window.partitionBy("Channel_ID").orderBy(F.col("order_count").desc(), F.col("order_hour"))
    peak = peak.withColumn("peak_rank", F.row_number().over(peak_window))
    return {
        "channel_summary": channel_summary,
        "channel_category_preferences": channel_category,
        "channel_peak_periods": peak,
    }
