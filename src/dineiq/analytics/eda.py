"""Spark EDA tables for the 13 outputs explicitly named in SRS FR-EX-84."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


EDA_OUTPUTS = (
    "top_selling_dishes", "lowest_selling_dishes", "highest_revenue_dishes",
    "highest_profit_dishes", "highest_margin_dishes", "high_wastage_dishes",
    "best_rated_dishes", "poorly_rated_dishes", "popular_menu_categories",
    "peak_ordering_periods", "location_sales_patterns", "channel_ordering_patterns",
    "promotion_driven_sales",
)


def build_eda_outputs(tables: dict[str, DataFrame], cleaned: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Build the named EDA results from cleaned records without collecting to the driver."""
    orders = cleaned["orders"].where(F.lower(F.trim(F.col("Order_Status"))) == "completed")
    line = cleaned["order_items"].where(F.col("Quantity") > 0).join(
        orders.select(
            "Order_ID", "Location_ID", "Channel_ID", "Promotion_ID", "Order_Date",
            "Order_DateTime", "Order_Time",
        ),
        "Order_ID",
        "inner",
    )
    menu = cleaned["menu_items"].select("Item_ID", "Item_Name", "Category_ID")
    item_sales = line.groupBy("Item_ID").agg(
        F.sum("Quantity").alias("units_sold"),
        F.sum("Line_Total").alias("revenue"),
        F.sum("Gross_Profit").alias("profit"),
        F.sum(F.col("Gross_Profit")).alias("contribution_margin"),
    ).withColumn(
        "margin_percentage",
        F.when(F.col("revenue") != 0, F.col("contribution_margin") / F.col("revenue") * 100.0),
    )
    all_items = menu.join(item_sales, "Item_ID", "left").fillna(
        0, subset=["units_sold", "revenue", "profit", "contribution_margin", "margin_percentage"]
    )

    wastage = cleaned["wastage"].groupBy("Item_ID").agg(
        F.sum("Quantity_Wasted").alias("quantity_wasted"),
        F.sum("Preparation_Quantity").alias("preparation_quantity"),
    ).withColumn(
        "wastage_percentage",
        F.when(F.col("preparation_quantity") > 0, F.col("quantity_wasted") / F.col("preparation_quantity") * 100.0),
    )
    ratings = cleaned["ratings"].groupBy("Item_ID").agg(
        F.avg("Rating").alias("average_rating"), F.count("Rating").alias("rating_count")
    )
    categories = cleaned["menu_categories"].select("Category_ID", "Category_Name")

    item_rank = all_items.join(wastage, "Item_ID", "left").join(ratings, "Item_ID", "left")
    top_selling = item_rank.orderBy(F.col("units_sold").desc(), F.col("Item_ID").asc())
    low_selling = item_rank.orderBy(F.col("units_sold").asc(), F.col("Item_ID").asc())
    high_revenue = item_rank.orderBy(F.col("revenue").desc(), F.col("Item_ID").asc())
    high_profit = item_rank.orderBy(F.col("profit").desc(), F.col("Item_ID").asc())
    high_margin = item_rank.orderBy(F.col("margin_percentage").desc_nulls_last(), F.col("Item_ID").asc())
    high_waste = menu.join(wastage, "Item_ID", "left").orderBy(
        F.col("wastage_percentage").desc_nulls_last(), F.col("quantity_wasted").desc_nulls_last()
    )
    best_rated = menu.join(ratings, "Item_ID", "inner").orderBy(
        F.col("average_rating").desc(), F.col("rating_count").desc()
    )
    poorly_rated = menu.join(ratings, "Item_ID", "inner").orderBy(
        F.col("average_rating").asc(), F.col("rating_count").desc()
    )
    popular_categories = item_sales.join(menu, "Item_ID", "inner").groupBy("Category_ID").agg(
        F.sum("units_sold").alias("units_sold"), F.sum("revenue").alias("revenue")
    ).join(categories, "Category_ID", "left").orderBy(F.col("units_sold").desc())

    period_orders = orders.withColumn(
        "order_hour", F.coalesce(F.hour("Order_DateTime"), F.regexp_extract(F.col("Order_Time").cast("string"), r"^(\d{1,2})", 1).cast("int"))
    ).withColumn("day_of_week", F.date_format("Order_Date", "EEEE"))
    by_hour = period_orders.groupBy("order_hour").agg(F.countDistinct("Order_ID").alias("order_count"))
    by_day = period_orders.groupBy("day_of_week").agg(F.countDistinct("Order_ID").alias("order_count"))
    peak_periods = by_hour.select(
        F.col("order_hour").cast("string").alias("period"),
        "order_count",
        F.lit("hour").alias("period_type"),
    ).unionByName(
        by_day.select(
            F.col("day_of_week").cast("string").alias("period"),
            "order_count",
            F.lit("day_of_week").alias("period_type"),
        )
    ).orderBy(F.col("order_count").desc())

    location_patterns = line.groupBy("Location_ID").agg(
        F.sum("Line_Total").alias("revenue"), F.sum("Quantity").alias("units_sold"),
        F.countDistinct("Order_ID").alias("order_count"),
    ).orderBy(F.col("revenue").desc())
    channel_patterns = line.groupBy("Channel_ID").agg(
        F.countDistinct("Order_ID").alias("order_count"), F.sum("Line_Total").alias("revenue"),
        F.sum("Quantity").alias("units_sold"),
    ).orderBy(F.col("order_count").desc())
    promo_sales = line.where(F.col("Promotion_ID").isNotNull() & (F.trim(F.col("Promotion_ID")) != ""))
    promotion_patterns = promo_sales.groupBy("Promotion_ID").agg(
        F.countDistinct("Order_ID").alias("order_count"), F.sum("Line_Total").alias("revenue"),
        F.sum("Quantity").alias("units_sold"),
    ).orderBy(F.col("revenue").desc())

    outputs = dict(zip(EDA_OUTPUTS, (
        top_selling, low_selling, high_revenue, high_profit, high_margin, high_waste,
        best_rated, poorly_rated, popular_categories, peak_periods, location_patterns,
        channel_patterns, promotion_patterns,
    )))
    if set(outputs) != set(EDA_OUTPUTS):
        raise RuntimeError("EDA output catalog mismatch.")
    return outputs
