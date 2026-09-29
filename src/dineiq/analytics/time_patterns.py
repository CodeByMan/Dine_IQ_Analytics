"""descriptive analytics hourly, weekday, and calendar-season ordering patterns."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_time_patterns(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    orders = tables["orders"].where(F.lower(F.trim(F.col("Order_Status"))) == "completed")
    dated = orders.where(F.col("Order_Date").isNotNull()).withColumn(
        "order_hour",
        F.coalesce(
            F.hour("Order_DateTime"),
            F.regexp_extract(F.col("Order_Time").cast("string"), r"^(\d{1,2})", 1).cast("int"),
        ),
    ).withColumn("day_number", F.dayofweek("Order_Date")).withColumn(
        "day_name", F.date_format("Order_Date", "EEEE")
    ).withColumn("month_number", F.month("Order_Date")).withColumn(
        "month_name", F.date_format("Order_Date", "MMMM")
    ).withColumn("year_month", F.date_format("Order_Date", "yyyy-MM")).withColumn(
        "is_weekend", F.dayofweek("Order_Date").isin(1, 7)
    ).withColumn(
        "season",
        F.when(F.month("Order_Date").isin(12, 1, 2), "Winter")
        .when(F.month("Order_Date").isin(3, 4, 5), "Spring")
        .when(F.month("Order_Date").isin(6, 7, 8), "Summer")
        .otherwise("Autumn"),
    )

    hourly = dated.where(F.col("order_hour").isNotNull()).groupBy("order_hour").agg(
        F.countDistinct("Order_ID").alias("order_count"), F.sum("Total_Amount").alias("revenue")
    ).orderBy("order_hour")
    weekday = dated.groupBy("day_number", "day_name").agg(
        F.countDistinct("Order_ID").alias("order_count"), F.sum("Total_Amount").alias("revenue")
    ).orderBy("day_number")
    seasonality = dated.groupBy("month_number", "month_name").agg(
        F.countDistinct("Order_ID").alias("order_count"), F.sum("Total_Amount").alias("revenue"),
        F.countDistinct(F.year("Order_Date")).alias("years_observed"),
    ).orderBy("month_number")
    monthly = dated.groupBy("year_month").agg(
        F.countDistinct("Order_ID").alias("order_count"), F.sum("Total_Amount").alias("revenue")
    ).orderBy("year_month")

    peak_hour = hourly.orderBy(F.col("order_count").desc(), F.col("order_hour").asc()).limit(1).select(
        F.lit("hour").alias("period_type"), F.col("order_hour").cast("string").alias("peak_period"),
        "order_count", "revenue",
    )
    peak_day = weekday.orderBy(F.col("order_count").desc(), F.col("day_number").asc()).limit(1).select(
        F.lit("day_of_week").alias("period_type"), F.col("day_name").alias("peak_period"),
        "order_count", "revenue",
    )
    peak_month = seasonality.orderBy(F.col("order_count").desc(), F.col("month_number").asc()).limit(1).select(
        F.lit("calendar_month").alias("period_type"), F.col("month_name").alias("peak_period"),
        "order_count", "revenue",
    )
    weekend = dated.groupBy("is_weekend").agg(
        F.countDistinct("Order_ID").alias("order_count"), F.sum("Total_Amount").alias("revenue")
    )
    peak_weekend = weekend.orderBy(F.col("order_count").desc(), F.col("is_weekend").desc()).limit(1).select(
        F.lit("weekend").alias("period_type"),
        F.when(F.col("is_weekend"), F.lit("Weekend")).otherwise(F.lit("Weekday")).alias("peak_period"),
        "order_count", "revenue",
    )

    location = dated.where(F.col("Location_ID").isNotNull()).groupBy("Location_ID").agg(
        F.countDistinct("Order_ID").alias("order_count"), F.sum("Total_Amount").alias("revenue")
    ) if "Location_ID" in dated.columns else None
    peak_location = location.orderBy(F.col("order_count").desc(), F.col("Location_ID")).limit(1).select(
        F.lit("restaurant_location").alias("period_type"), F.col("Location_ID").cast("string").alias("peak_period"),
        "order_count", "revenue",
    ) if location is not None else peak_weekend.limit(0)

    if "Channel_ID" in dated.columns:
        channel = dated.where(F.col("Channel_ID").isNotNull()).groupBy("Channel_ID").agg(
            F.countDistinct("Order_ID").alias("order_count"), F.sum("Total_Amount").alias("revenue")
        )
        peak_channel = channel.orderBy(F.col("order_count").desc(), F.col("Channel_ID")).limit(1).select(
            F.lit("ordering_channel").alias("period_type"), F.col("Channel_ID").cast("string").alias("peak_period"),
            "order_count", "revenue",
        )
    else:
        peak_channel = peak_weekend.limit(0)

    peak_periods = (peak_hour.unionByName(peak_day).unionByName(peak_month)
                    .unionByName(peak_weekend).unionByName(peak_location).unionByName(peak_channel))
    return {
        "hourly_order_patterns": hourly,
        "weekday_order_patterns": weekday,
        "seasonal_order_patterns": seasonality,
        "monthly_order_trend": monthly,
        "peak_period_summary": peak_periods,
    }
