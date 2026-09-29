"""descriptive analytics rating comparisons with menu and restaurant performance."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_rating_analysis(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    ratings = tables["ratings"].where(F.col("Rating").between(1, 5))
    completed = tables["orders"].where(F.lower(F.trim(F.col("Order_Status"))) == "completed")
    order_context = ["Order_ID", "Location_ID"]
    for optional in ("Customer_ID", "Order_Date", "Promotion_ID"):
        if optional in completed.columns:
            order_context.append(optional)
    sales = tables["order_items"].where(F.col("Quantity") > 0).join(
        completed.select(*order_context), "Order_ID", "inner"
    )
    item_sales = sales.groupBy("Item_ID").agg(
        F.sum("Line_Total").alias("item_revenue"),
        F.sum("Gross_Profit").alias("item_contribution_margin"),
        F.countDistinct("Order_ID").alias("item_order_count"),
    )
    location_item_sales = sales.groupBy("Item_ID", "Location_ID").agg(
        F.sum("Line_Total").alias("item_location_revenue"),
        F.countDistinct("Order_ID").alias("item_location_order_count"),
    )
    location_sales = completed.groupBy("Location_ID").agg(
        F.sum("Total_Amount").alias("location_revenue"),
        F.countDistinct("Order_ID").alias("location_order_count"),
    )

    if {"Customer_ID", "Item_ID", "Order_ID"}.issubset(sales.columns):
        customer_item = sales.select("Customer_ID", "Item_ID", "Order_ID").dropDuplicates()
        item_repeat = customer_item.groupBy("Item_ID", "Customer_ID").agg(
            F.countDistinct("Order_ID").alias("item_customer_orders")
        ).groupBy("Item_ID").agg(
            F.countDistinct("Customer_ID").alias("item_customer_count"),
            F.countDistinct(F.when(F.col("item_customer_orders") >= 2, F.col("Customer_ID"))).alias("item_repeat_customer_count"),
        ).withColumn(
            "repeat_purchase_rate",
            F.when(F.col("item_customer_count") > 0,
                   F.col("item_repeat_customer_count") / F.col("item_customer_count")),
        ).select("Item_ID", "repeat_purchase_rate")
    else:
        item_repeat = item_sales.select("Item_ID").withColumn("repeat_purchase_rate", F.lit(None).cast("double"))

    if {"Customer_ID", "Location_ID", "Order_ID", "Item_ID"}.issubset(sales.columns):
        location_customer_item = sales.select("Customer_ID", "Location_ID", "Item_ID", "Order_ID").dropDuplicates()
        location_repeat = location_customer_item.groupBy("Location_ID", "Customer_ID", "Item_ID").agg(
            F.countDistinct("Order_ID").alias("customer_item_orders")
        ).groupBy("Location_ID").agg(
            F.countDistinct(F.struct("Customer_ID", "Item_ID")).alias("location_customer_item_count"),
            F.countDistinct(F.when(F.col("customer_item_orders") >= 2,
                                   F.struct("Customer_ID", "Item_ID"))).alias("location_repeat_customer_item_count"),
        ).withColumn(
            "repeat_purchase_rate",
            F.when(F.col("location_customer_item_count") > 0,
                   F.col("location_repeat_customer_item_count") / F.col("location_customer_item_count")),
        ).select("Location_ID", "repeat_purchase_rate")
    else:
        location_repeat = location_sales.select("Location_ID").withColumn("repeat_purchase_rate", F.lit(None).cast("double"))

    rating_period = ratings.groupBy("Item_ID").agg(
        F.min(F.to_date("Review_Date")).alias("rating_period_start")
    ) if "Review_Date" in ratings.columns else ratings.select("Item_ID").distinct().withColumn(
        "rating_period_start", F.lit(None).cast("date")
    )
    item_info = tables["menu_items"].select("Item_ID", "Item_Name", "Category_ID")
    restaurant_info = tables["restaurants"].select(
        "Location_ID", "Restaurant_Name", F.col("Average_Rating").alias("recorded_restaurant_rating")
    )

    item_ratings = ratings.groupBy("Item_ID").agg(
        F.avg("Rating").alias("average_customer_rating"),
        F.count("Rating").alias("rating_count"),
        F.stddev_pop("Rating").alias("rating_stddev"),
    )
    by_item = item_info.join(item_ratings, "Item_ID", "left").join(item_sales, "Item_ID", "left").join(
        item_repeat, "Item_ID", "left"
    ).join(rating_period, "Item_ID", "left").withColumn(
        "rating_period", F.date_format("rating_period_start", "yyyy-MM")
    ).orderBy(
        F.col("average_customer_rating").desc_nulls_last(), F.col("rating_count").desc_nulls_last()
    )

    location_ratings = ratings.where(F.col("Location_ID").isNotNull()).groupBy("Location_ID").agg(
        F.avg("Rating").alias("average_customer_rating"),
        F.count("Rating").alias("rating_count"),
        F.stddev_pop("Rating").alias("rating_stddev"),
    )
    by_location = restaurant_info.join(location_ratings, "Location_ID", "left").join(
        location_sales, "Location_ID", "left"
    ).join(location_repeat, "Location_ID", "left").withColumn(
        "customer_vs_recorded_rating_difference",
        F.col("average_customer_rating") - F.col("recorded_restaurant_rating"),
    ).withColumn("rating_period", F.lit(None).cast("string")).orderBy(
        F.col("average_customer_rating").desc_nulls_last()
    )

    item_location_ratings = ratings.where(F.col("Location_ID").isNotNull()).groupBy(
        "Item_ID", "Location_ID"
    ).agg(
        F.avg("Rating").alias("average_customer_rating"), F.count("Rating").alias("rating_count")
    )
    by_item_location = item_location_ratings.join(item_info, "Item_ID", "left").join(
        restaurant_info, "Location_ID", "left"
    ).join(location_item_sales, ["Item_ID", "Location_ID"], "left").orderBy(
        F.col("average_customer_rating").desc_nulls_last()
    ).join(item_repeat, "Item_ID", "left").withColumn("promotion_status", F.lit("Unknown"))

    # Promotion status is an observed context, not a causal rating claim.
    if "Promotion_ID" in sales.columns:
        promo_context = sales.groupBy("Item_ID").agg(
            F.max(F.when(F.col("Promotion_ID").isNotNull() & (F.trim(F.col("Promotion_ID").cast("string")) != ""), 1).otherwise(0)).alias("has_promoted_sales")
        )
        by_item = by_item.join(promo_context, "Item_ID", "left").withColumn(
            "promotion_status", F.when(F.col("has_promoted_sales") == 1, "Promotion applied").otherwise("No promotion")
        ).drop("has_promoted_sales")
        by_item_location = by_item_location.join(promo_context, "Item_ID", "left").withColumn(
            "promotion_status", F.when(F.col("has_promoted_sales") == 1, "Promotion applied").otherwise("No promotion")
        ).drop("has_promoted_sales")
        location_promo = sales.groupBy("Location_ID").agg(
            F.max(F.when(F.col("Promotion_ID").isNotNull() & (F.trim(F.col("Promotion_ID").cast("string")) != ""), 1).otherwise(0)).alias("has_promoted_sales")
        )
        by_location = by_location.join(location_promo, "Location_ID", "left").withColumn(
            "promotion_status", F.when(F.col("has_promoted_sales") == 1, "Promotion applied").otherwise("No promotion")
        ).drop("has_promoted_sales")
    else:
        by_item = by_item.withColumn("promotion_status", F.lit("Unknown"))
        by_location = by_location.withColumn("promotion_status", F.lit("Unknown"))
    return {
        "item_rating_performance": by_item,
        "location_rating_performance": by_location,
        "item_location_rating_performance": by_item_location,
    }
