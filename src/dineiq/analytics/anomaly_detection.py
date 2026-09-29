"""SRS-named rating and sales anomaly screens with auditable evidence."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


EVENT_COLUMNS = (
    "anomaly_type", "subject_type", "subject_id", "location_id", "event_date",
    "observed_value", "expected_value", "severity", "evidence",
)


def _event(frame: DataFrame, anomaly_type: str, subject_type: str, subject_id, location_id,
           event_date, observed, expected, severity, evidence) -> DataFrame:
    return frame.select(
        F.lit(anomaly_type).alias("anomaly_type"), F.lit(subject_type).alias("subject_type"),
        subject_id.cast("string").alias("subject_id"), location_id.cast("string").alias("location_id"),
        event_date.cast("date").alias("event_date"), observed.cast("double").alias("observed_value"),
        expected.cast("double").alias("expected_value"), F.lit(severity).alias("severity"),
        evidence.cast("string").alias("evidence"),
    )


def build_rating_anomalies(tables: dict[str, DataFrame]) -> DataFrame:
    ratings_source = tables.get("raw_ratings", tables["ratings"])
    ratings = ratings_source.where(F.col("Rating").between(1, 5)).withColumn(
        "review_date", F.to_date("Review_Date")
    ).where(F.col("review_date").isNotNull())
    daily = ratings.groupBy("Item_ID", "review_date").agg(
        F.avg("Rating").alias("daily_average"), F.count("Rating").alias("daily_count")
    ).withColumn("day_number", F.datediff("review_date", F.lit("1970-01-01")))
    by_item = Window.partitionBy("Item_ID").orderBy("day_number").rangeBetween(-7, -1)
    recent = daily.withColumn("prior_average", F.avg("daily_average").over(by_item)).withColumn(
        "prior_rating_count", F.sum("daily_count").over(by_item)
    )
    spike_drop = recent.where(
        (F.col("daily_count") >= 3) & (F.col("prior_rating_count") >= 3)
        & (F.abs(F.col("daily_average") - F.col("prior_average")) >= 1.0)
    )
    spike = _event(spike_drop.where(F.col("daily_average") > F.col("prior_average")),
        "Sudden rating spike", "item", F.col("Item_ID"), F.lit(None).cast("string"), F.col("review_date"),
        F.col("daily_average"), F.col("prior_average"), "High",
        F.concat(F.lit("daily average vs prior 7-day observed baseline; n="), F.col("daily_count")))
    drop = _event(spike_drop.where(F.col("daily_average") < F.col("prior_average")),
        "Sudden rating drop", "item", F.col("Item_ID"), F.lit(None).cast("string"), F.col("review_date"),
        F.col("daily_average"), F.col("prior_average"), "High",
        F.concat(F.lit("daily average vs prior 7-day observed baseline; n="), F.col("daily_count")))

    burst_window = Window.partitionBy("Item_ID").orderBy("day_number").rangeBetween(-30, -1)
    bursts = daily.withColumn("prior_daily_average_count", F.avg("daily_count").over(burst_window)).where(
        (F.col("daily_count") >= 10) & (F.col("prior_daily_average_count") > 0)
        & (F.col("daily_count") >= 5 * F.col("prior_daily_average_count"))
    )
    burst = _event(bursts, "High rating volume in a short period", "item", F.col("Item_ID"),
        F.lit(None).cast("string"), F.col("review_date"), F.col("daily_count"),
        F.col("prior_daily_average_count"), "High", F.lit("daily ratings are at least 5x the prior 30-day observed-day average; n>=10"))

    weekly = ratings.withColumn("review_week", F.to_date(F.date_trunc("week", "review_date"))).groupBy(
        "Item_ID", "review_week", "Rating"
    ).agg(F.count("Rating").alias("identical_rating_count"))
    week_total = weekly.groupBy("Item_ID", "review_week").agg(
        F.sum("identical_rating_count").alias("weekly_rating_count")
    )
    identical = weekly.join(week_total, ["Item_ID", "review_week"], "inner").where(
        (F.col("weekly_rating_count") >= 10)
        & (F.col("identical_rating_count") / F.col("weekly_rating_count") >= 0.90)
    )
    identical_event = _event(identical, "Excessive identical ratings", "item", F.col("Item_ID"),
        F.lit(None).cast("string"), F.col("review_week"), F.col("identical_rating_count"),
        F.col("weekly_rating_count"), "Medium",
        F.concat(F.lit("rating value="), F.col("Rating"), F.lit("; share of weekly item/location ratings >=90%")))

    orders = tables["orders"].where(F.lower(F.trim("Order_Status")) == "completed").select(
        "Order_ID", "Customer_ID"
    )
    purchased = orders.join(tables["order_items"].select("Order_ID", "Item_ID"), "Order_ID", "inner").dropDuplicates(
        ["Order_ID", "Customer_ID", "Item_ID"]
    )
    linked_ratings = ratings.where(F.col("Order_ID").isNotNull() & F.col("Customer_ID").isNotNull()).select(
        "Rating_ID", "Order_ID", "Customer_ID", "Item_ID", "Location_ID", "review_date", "Rating"
    )
    inconsistent = linked_ratings.join(purchased, ["Order_ID", "Customer_ID", "Item_ID"], "left_anti")
    inconsistent_event = _event(inconsistent, "Rating inconsistent with purchasing pattern", "rating",
        F.col("Rating_ID"), F.col("Location_ID"), F.col("review_date"), F.col("Rating"), F.lit(None).cast("double"),
        "High", F.lit("rating's customer/order/item tuple has no matching completed purchase line"))
    return spike.unionByName(drop).unionByName(burst).unionByName(identical_event).unionByName(inconsistent_event)


def build_sales_anomalies(tables: dict[str, DataFrame]) -> DataFrame:
    orders = tables["orders"].where(F.lower(F.trim("Order_Status")) == "completed").select(
        "Order_ID", "Customer_ID", "Location_ID", "Promotion_ID", F.to_date("Order_Date").alias("order_date"),
        F.col("Total_Amount").cast("double").alias("order_value"),
    )
    sales = tables["order_items"].where(F.col("Quantity") > 0).select(
        "Order_ID", "Item_ID", "Quantity", "Line_Total", "Gross_Profit", "Discount_Amount"
    ).join(orders.select("Order_ID", "Location_ID", "order_date"), "Order_ID", "inner")
    daily = sales.groupBy("Item_ID", "order_date").agg(
        F.sum("Quantity").alias("daily_units"), F.sum("Line_Total").alias("daily_revenue"),
        F.countDistinct("Order_ID").alias("daily_order_count"),
    ).withColumn("day_number", F.datediff("order_date", F.lit("1970-01-01")))
    bounds = daily.groupBy("Item_ID").agg(F.min("order_date").alias("first_date"), F.max("order_date").alias("last_date"))
    date_spine = bounds.select("Item_ID", F.explode(F.sequence(
        "first_date", "last_date", F.expr("interval 1 day")
    )).alias("order_date"))
    full_daily = date_spine.join(daily.drop("day_number"), ["Item_ID", "order_date"], "left").fillna(
        0, subset=["daily_units", "daily_revenue", "daily_order_count"]
    ).withColumn("day_number", F.datediff("order_date", F.lit("1970-01-01")))
    history = Window.partitionBy("Item_ID").orderBy("day_number").rangeBetween(-28, -1)
    scored = full_daily.withColumn("baseline_units", F.avg("daily_units").over(history)).withColumn(
        "baseline_stddev", F.stddev_pop("daily_units").over(history)
    ).withColumn("baseline_days", F.count("daily_units").over(history))
    eligible = scored.where((F.col("baseline_days") >= 7) & (F.col("baseline_units") > 0))
    spike_drop = eligible.where(
        F.abs(F.col("daily_units") - F.col("baseline_units")) > 3 * F.coalesce(F.col("baseline_stddev"), F.lit(0.0))
    )
    spike = _event(spike_drop.where(F.col("daily_units") > F.col("baseline_units")),
        "Sudden sales spike", "item", F.col("Item_ID"), F.lit(None).cast("string"), F.col("order_date"),
        F.col("daily_units"), F.col("baseline_units"), "High", F.lit("daily item/location units exceed a trailing 28-day mean by >3 standard deviations"))
    drop = _event(spike_drop.where(F.col("daily_units") < F.col("baseline_units")),
        "Sudden sales drop", "item", F.col("Item_ID"), F.lit(None).cast("string"), F.col("order_date"),
        F.col("daily_units"), F.col("baseline_units"), "High", F.lit("daily item/location units are >3 standard deviations below trailing 28-day mean"))
    unexpected = eligible.where(
        (F.col("daily_units") >= 2 * F.col("baseline_units"))
        & (F.col("daily_units") <= F.col("baseline_units") + 3 * F.coalesce(F.col("baseline_stddev"), F.lit(0.0)))
    )
    unexpected_event = _event(unexpected, "Unexpected demand", "item", F.col("Item_ID"), F.lit(None).cast("string"),
        F.col("order_date"), F.col("daily_units"), F.col("baseline_units"), "Medium",
        F.lit("daily units are at least 2x the trailing 28-day mean, below the separate 3-sigma spike rule"))

    order_thresholds = orders.groupBy("Location_ID").agg(
        F.expr("percentile_approx(order_value, 0.995, 10000)").alias("p995_order_value")
    )
    high_orders = orders.join(order_thresholds, "Location_ID").where(
        F.col("order_value") > F.col("p995_order_value")
    )
    order_event = _event(high_orders, "Abnormally high order value", "order", F.col("Order_ID"),
        F.col("Location_ID"), F.col("order_date"), F.col("order_value"), F.col("p995_order_value"),
        "High", F.lit("order total exceeds location-specific 99.5th percentile"))

    line_source = tables.get("raw_order_items", tables["order_items"])
    line_discount = line_source.select(
        "Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Discount_Amount"
    ).join(orders.select("Order_ID", "Location_ID", "order_date"), "Order_ID", "inner").withColumn(
        "pre_discount_value", F.col("Unit_Price") * F.col("Quantity")
    ).withColumn(
        "discount_pct", F.when(F.col("pre_discount_value") > 0,
            F.col("Discount_Amount") / F.col("pre_discount_value") * 100)
    ).where((F.col("discount_pct") > 50) | (F.col("Discount_Amount") > F.col("pre_discount_value")))
    discount_event = _event(line_discount, "Unusual discount", "order_line", F.col("Order_Item_ID"),
        F.col("Location_ID"), F.col("order_date"), F.col("discount_pct"), F.lit(50.0), "Medium",
        F.lit("line discount exceeds 50% of pre-discount value or exceeds the pre-discount value"))

    raw_orders = tables.get("raw_orders", tables["orders"])
    signature = ["Customer_ID", "Location_ID", "Order_Date", "Order_Time", "Total_Amount", "Payment_Method"]
    duplicate_signatures = raw_orders.groupBy(*signature).agg(
        F.count("Order_ID").alias("signature_count"), F.min("Order_ID").alias("representative_order_id")
    ).where(F.col("signature_count") > 1)
    flagged = raw_orders.where(F.coalesce(F.col("Is_Duplicate"), F.lit(False))).select(
        F.col("Order_ID").alias("representative_order_id"), "Location_ID", "Order_Date",
        F.lit(1).alias("signature_count"),
    )
    duplicate_event = _event(duplicate_signatures, "Duplicate transaction", "order",
        F.col("representative_order_id"), F.lit(None).cast("string"), F.to_date(F.lit(None)),
        F.col("signature_count"), F.lit(1), "High", F.lit("repeated transaction signature in raw source"))
    if "Is_Duplicate" in raw_orders.columns:
        flagged_event = _event(flagged, "Duplicate transaction", "order", F.col("representative_order_id"),
            F.col("Location_ID"), F.to_date("Order_Date"), F.col("signature_count"), F.lit(1), "High",
            F.lit("source transaction is explicitly marked duplicate"))
        duplicate_event = duplicate_event.unionByName(flagged_event)
    duplicate_event = duplicate_event.dropDuplicates(["subject_id", "anomaly_type"])
    return spike.unionByName(drop).unionByName(unexpected_event).unionByName(order_event).unionByName(
        discount_event
    ).unionByName(duplicate_event)
