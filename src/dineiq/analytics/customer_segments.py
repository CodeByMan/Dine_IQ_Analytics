"""descriptive analytics customer RFM and behavior-factor segmentation outputs."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


CUSTOMER_SEGMENTS = (
    "High-Value Loyal", "Frequent", "Promotion-Driven", "At-Risk", "New", "Occasional"
)


def build_customer_rfm_and_segments(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Calculate RFM plus all ten stated behavior factors and six segment labels."""
    orders = tables["orders"].where(F.lower(F.trim(F.col("Order_Status"))) == "completed").select(
        "Order_ID", "Customer_ID", "Order_Date", "Order_DateTime", "Order_Time",
        "Total_Amount", "Channel_ID", "Promotion_ID",
    )
    dataset_end = orders.agg(F.max("Order_Date").alias("dataset_end_date"))
    rfm = orders.where(F.col("Customer_ID").isNotNull()).groupBy("Customer_ID").agg(
        F.max("Order_Date").alias("last_order_date"),
        F.countDistinct("Order_ID").alias("frequency"),
        F.sum("Total_Amount").alias("monetary_value"),
        F.avg("Total_Amount").alias("average_order_value"),
        F.countDistinct("Order_Date").alias("visit_frequency"),
    )

    promo = orders.groupBy("Customer_ID").agg(
        F.countDistinct("Order_ID").alias("order_count"),
        F.countDistinct(F.when(F.col("Promotion_ID").isNotNull() & (F.trim(F.col("Promotion_ID")) != ""), F.col("Order_ID"))).alias("promoted_order_count"),
    ).withColumn(
        "promotion_sensitivity", F.when(F.col("order_count") > 0, F.col("promoted_order_count") / F.col("order_count"))
    ).select("Customer_ID", "promotion_sensitivity")

    channel = orders.groupBy("Customer_ID", "Channel_ID").agg(
        F.countDistinct("Order_ID").alias("channel_order_count")
    ).withColumn(
        "rank", F.row_number().over(Window.partitionBy("Customer_ID").orderBy(F.col("channel_order_count").desc(), F.col("Channel_ID").asc()))
    ).where(F.col("rank") == 1).select("Customer_ID", F.col("Channel_ID").alias("preferred_channel_id"))

    with_hour = orders.withColumn(
        "order_hour",
        F.coalesce(
            F.hour("Order_DateTime"),
            F.regexp_extract(F.col("Order_Time").cast("string"), r"^(\d{1,2})", 1).cast("int"),
        ),
    )
    time = with_hour.where(F.col("order_hour").isNotNull()).groupBy("Customer_ID", "order_hour").agg(
        F.countDistinct("Order_ID").alias("hour_order_count")
    ).withColumn(
        "rank", F.row_number().over(Window.partitionBy("Customer_ID").orderBy(F.col("hour_order_count").desc(), F.col("order_hour").asc()))
    ).where(F.col("rank") == 1).select(
        "Customer_ID",
        F.when(F.col("order_hour").between(5, 11), "Morning")
        .when(F.col("order_hour").between(12, 16), "Afternoon")
        .when(F.col("order_hour").between(17, 21), "Evening")
        .otherwise("Night").alias("preferred_time_of_day"),
    )

    lines = tables["order_items"].where(F.col("Quantity") > 0).join(
        orders.select("Order_ID", "Customer_ID"), "Order_ID", "inner"
    )
    categories = lines.join(
        tables["menu_items"].select("Item_ID", "Category_ID"), "Item_ID", "left"
    ).groupBy("Customer_ID", "Category_ID").agg(F.sum("Quantity").alias("category_units"))
    favorite = categories.withColumn(
        "rank", F.row_number().over(Window.partitionBy("Customer_ID").orderBy(F.col("category_units").desc(), F.col("Category_ID").asc_nulls_last()))
    ).where(F.col("rank") == 1).select("Customer_ID", F.col("Category_ID").alias("favorite_category_id"))

    customer_base = tables["customers"].select("Customer_ID", "Signup_Date")
    result = customer_base.crossJoin(dataset_end).join(rfm, "Customer_ID", "left").withColumn(
        "recency_days", F.datediff(F.col("dataset_end_date"), F.col("last_order_date"))
    ).drop("last_order_date").join(promo, "Customer_ID", "left").join(channel, "Customer_ID", "left").join(
        time, "Customer_ID", "left"
    ).join(favorite, "Customer_ID", "left")
    result = result.fillna(0, subset=["frequency", "monetary_value", "average_order_value", "visit_frequency", "promotion_sensitivity"])
    r_window = Window.orderBy(F.col("recency_days").asc_nulls_last(), F.col("Customer_ID").asc())
    f_window = Window.orderBy(F.col("frequency").desc(), F.col("Customer_ID").asc())
    m_window = Window.orderBy(F.col("monetary_value").desc(), F.col("Customer_ID").asc())
    result = result.withColumn("R_score", F.when(F.col("recency_days").isNotNull(), 6 - F.ntile(5).over(r_window)))
    result = result.withColumn("F_score", 6 - F.ntile(5).over(f_window))
    result = result.withColumn("M_score", 6 - F.ntile(5).over(m_window))
    result = result.withColumn(
        "repeat_behavior", F.when(F.col("frequency") >= 2, "Repeat").otherwise("Single/No repeat")
    ).withColumn(
        "is_recent_new_customer",
        (F.col("frequency") <= 1) & (F.col("Signup_Date") >= F.date_sub(F.col("dataset_end_date"), 90)),
    )
    result = result.withColumn(
        "customer_segment",
        F.when(F.col("is_recent_new_customer"), CUSTOMER_SEGMENTS[4])
        .when((F.col("R_score") <= 2) & (F.col("frequency") >= 2), CUSTOMER_SEGMENTS[3])
        .when((F.col("frequency") > 0) & (F.col("R_score") >= 3) & (F.col("F_score") >= 4) & (F.col("M_score") >= 4), CUSTOMER_SEGMENTS[0])
        .when((F.col("promotion_sensitivity") >= 0.50) & (F.col("frequency") >= 2), CUSTOMER_SEGMENTS[2])
        .when((F.col("frequency") > 0) & (F.col("F_score") >= 4), CUSTOMER_SEGMENTS[1])
        .otherwise(CUSTOMER_SEGMENTS[5]),
    )
    rfm_output = result.select(
        "Customer_ID", "recency_days", "frequency", "monetary_value", "R_score", "F_score", "M_score"
    )
    factor_output = result.select(
        "Customer_ID", "recency_days", "frequency", "monetary_value", "average_order_value",
        "visit_frequency", "favorite_category_id", "promotion_sensitivity", "preferred_channel_id",
        "preferred_time_of_day", "repeat_behavior", "customer_segment",
    )
    return {"customer_rfm": rfm_output, "customer_segments": factor_output}
