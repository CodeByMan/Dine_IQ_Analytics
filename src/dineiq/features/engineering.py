"""Spark feature engineering for the 22 feature families named in SRS Step 7."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


SRS_FEATURES = (
    "item_revenue", "cost", "contribution_margin", "profit_percentage", "order_frequency",
    "item_popularity", "repeat_purchase_rate", "average_rating", "rating_trend",
    "wastage_percentage", "promotion_dependency", "discount_percentage", "customer_recency",
    "customer_frequency", "customer_monetary_value", "average_order_value", "peak_hour_frequency",
    "weekend_order_ratio", "location_performance", "channel_preference", "basket_size",
    "price_change_percentage",
)


def _completed_orders(orders: DataFrame) -> DataFrame:
    return orders.where(F.lower(F.trim(F.col("Order_Status"))) == "completed")


def build_feature_tables(tables: dict[str, DataFrame], cleaned: dict[str, DataFrame] | None = None) -> dict[str, DataFrame]:
    """Create item, customer, and item-location features using completed transactions.

    Formulas are explicitly named in the data foundation documentation. Source rows are
    read-only, and this function returns lazy Spark frames rather than collecting
    analytical tables into Python memory.
    """
    source = cleaned or tables
    orders = _completed_orders(source["orders"]).select(
        "Order_ID", "Customer_ID", "Location_ID", "Channel_ID", "Promotion_ID",
        "Order_Date", "Order_DateTime", "Order_Time", "Total_Amount",
    )
    lines = source["order_items"].where(F.col("Quantity") > 0).select(
        "Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Discount_Amount",
        "Line_Total", "Gross_Profit",
    )
    sales = lines.join(orders, "Order_ID", "inner")

    menu = sales.groupBy("Item_ID").agg(
        F.sum("Line_Total").alias("item_revenue"),
        F.sum(F.col("Line_Total") - F.col("Gross_Profit")).alias("cost"),
        F.sum("Gross_Profit").alias("contribution_margin"),
        F.countDistinct("Order_ID").alias("order_frequency"),
        F.sum("Quantity").alias("item_popularity"),
        F.sum("Discount_Amount").alias("total_discount"),
        F.sum(F.col("Quantity") * F.col("Unit_Price")).alias("gross_line_value"),
        F.sum(F.when(F.col("Promotion_ID").isNotNull() & (F.trim(F.col("Promotion_ID")) != ""), F.col("Line_Total")).otherwise(0)).alias("promoted_revenue"),
    ).withColumn(
        "profit_percentage",
        F.when(F.col("item_revenue") != 0, F.col("contribution_margin") / F.col("item_revenue") * 100.0),
    ).withColumn(
        "discount_percentage",
        F.when(F.col("gross_line_value") != 0, F.col("total_discount") / F.col("gross_line_value") * 100.0),
    ).withColumn(
        "promotion_dependency",
        F.when(F.col("item_revenue") != 0, F.col("promoted_revenue") / F.col("item_revenue") * 100.0),
    )

    customer_item_orders = sales.where(F.col("Customer_ID").isNotNull()).select(
        "Customer_ID", "Item_ID", "Order_ID"
    ).dropDuplicates()
    item_customer_counts = customer_item_orders.groupBy("Item_ID").agg(
        F.countDistinct("Customer_ID").alias("unique_customers")
    )
    repeat_customers = customer_item_orders.groupBy("Item_ID", "Customer_ID").agg(
        F.countDistinct("Order_ID").alias("item_order_count")
    ).where(F.col("item_order_count") >= 2).groupBy("Item_ID").agg(
        F.countDistinct("Customer_ID").alias("repeat_customers")
    )
    repeat_rate = item_customer_counts.join(repeat_customers, "Item_ID", "left").withColumn(
        "repeat_purchase_rate",
        F.when(F.col("unique_customers") > 0, F.coalesce(F.col("repeat_customers"), F.lit(0)) / F.col("unique_customers")),
    ).select("Item_ID", "repeat_purchase_rate")

    rating_by_date = source["ratings"].where(F.col("Rating").between(1, 5)).groupBy(
        "Item_ID", "Review_Date"
    ).agg(F.avg("Rating").alias("daily_average_rating"))
    overall_rating = source["ratings"].where(F.col("Rating").between(1, 5)).groupBy(
        "Item_ID"
    ).agg(F.avg("Rating").alias("average_rating"))
    rating_window_asc = Window.partitionBy("Item_ID").orderBy(F.col("Review_Date").asc())
    rating_window_desc = Window.partitionBy("Item_ID").orderBy(F.col("Review_Date").desc())
    first_rating = rating_by_date.withColumn("row_num", F.row_number().over(rating_window_asc)).where(
        F.col("row_num") == 1
    ).select("Item_ID", F.col("daily_average_rating").alias("first_period_rating"))
    last_rating = rating_by_date.withColumn("row_num", F.row_number().over(rating_window_desc)).where(
        F.col("row_num") == 1
    ).select("Item_ID", F.col("daily_average_rating").alias("last_period_rating"))
    rating_trend = last_rating.join(first_rating, "Item_ID", "inner").withColumn(
        "rating_trend", F.col("last_period_rating") - F.col("first_period_rating")
    ).select("Item_ID", "rating_trend")
    rating_features = overall_rating.join(rating_trend, "Item_ID", "inner")

    waste = source["wastage"].groupBy("Item_ID").agg(
        F.sum("Quantity_Wasted").alias("wasted_quantity"),
        F.sum("Preparation_Quantity").alias("prepared_quantity"),
    ).withColumn(
        "wastage_percentage",
        F.when(F.col("prepared_quantity") > 0, F.col("wasted_quantity") / F.col("prepared_quantity") * 100.0),
    ).select("Item_ID", "wastage_percentage")

    by_hour = sales.withColumn(
        "order_hour",
        F.coalesce(F.hour("Order_DateTime"), F.regexp_extract(F.col("Order_Time").cast("string"), r"^(\d{1,2})", 1).cast("int")),
    ).groupBy("Item_ID", "order_hour").agg(F.countDistinct("Order_ID").alias("hour_orders"))
    peak_hour = by_hour.groupBy("Item_ID").agg(F.max("hour_orders").alias("peak_hour_frequency"))

    weekend_orders = sales.withColumn(
        "weekday", F.dayofweek("Order_Date")
    ).groupBy("Item_ID").agg(
        F.countDistinct("Order_ID").alias("all_orders"),
        F.countDistinct(F.when(F.col("weekday").isin(1, 7), F.col("Order_ID"))).alias("weekend_orders"),
    ).withColumn(
        "weekend_order_ratio",
        F.when(F.col("all_orders") > 0, F.col("weekend_orders") / F.col("all_orders")),
    ).select("Item_ID", "weekend_order_ratio")

    order_sizes = lines.groupBy("Order_ID").agg(F.countDistinct("Item_ID").alias("basket_size"))
    basket_features = sales.join(order_sizes, "Order_ID", "inner").groupBy("Item_ID").agg(
        F.avg("basket_size").alias("basket_size")
    )

    channel_counts = sales.where(F.col("Customer_ID").isNotNull()).groupBy(
        "Customer_ID", "Channel_ID"
    ).agg(F.countDistinct("Order_ID").alias("channel_orders"))
    channel_window = Window.partitionBy("Customer_ID").orderBy(
        F.col("channel_orders").desc(), F.col("Channel_ID").asc()
    )
    channel_preference = channel_counts.withColumn("rank", F.row_number().over(channel_window)).where(
        F.col("rank") == 1
    ).select("Customer_ID", F.col("Channel_ID").alias("channel_preference"))

    price_change = source["pricing_history"].groupBy("Item_ID").agg(
        F.avg("Price_Change_Percentage").alias("price_change_percentage")
    )
    feature_frame = menu
    for other in (repeat_rate, rating_features, waste, peak_hour, weekend_orders, basket_features, price_change):
        feature_frame = feature_frame.join(other, "Item_ID", "left")
    feature_frame = feature_frame.select(
        "Item_ID", "item_revenue", "cost", "contribution_margin", "profit_percentage",
        "order_frequency", "item_popularity", "repeat_purchase_rate", "average_rating", "rating_trend",
        "wastage_percentage", "promotion_dependency", "discount_percentage", "peak_hour_frequency",
        "weekend_order_ratio", "basket_size", "price_change_percentage",
    )

    location_revenue = sales.groupBy("Item_ID", "Location_ID").agg(
        F.sum("Line_Total").alias("item_location_revenue")
    )
    location_totals = sales.groupBy("Location_ID").agg(F.sum("Line_Total").alias("location_revenue"))
    location_features = location_revenue.join(location_totals, "Location_ID", "inner").withColumn(
        "location_performance",
        F.when(F.col("location_revenue") != 0, F.col("item_location_revenue") / F.col("location_revenue") * 100.0),
    )

    customer_features = orders.where(F.col("Customer_ID").isNotNull()).groupBy("Customer_ID").agg(
        F.max("Order_Date").alias("last_order_date"),
        F.countDistinct("Order_ID").alias("customer_frequency"),
        F.sum("Total_Amount").alias("customer_monetary_value"),
        F.avg("Total_Amount").alias("average_order_value"),
    )
    dataset_end = orders.agg(F.max("Order_Date").alias("dataset_end"))
    customer_features = customer_features.crossJoin(dataset_end).withColumn(
        "customer_recency", F.datediff(F.col("dataset_end"), F.col("last_order_date"))
    ).drop("dataset_end", "last_order_date").join(channel_preference, "Customer_ID", "left")

    return {
        "menu_features": feature_frame,
        "customer_features": customer_features,
        "menu_location_features": location_features,
    }
