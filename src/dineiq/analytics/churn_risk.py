"""Transparent customer engagement decline screening."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


CHURN_FACTORS = (
    "increasing_recency", "declining_frequency", "declining_monetary_value",
    "reduced_category_diversity", "lower_visit_frequency",
)


def build_customer_churn_risk(tables: dict[str, DataFrame]) -> DataFrame:
    """Flag engagement decline using equal recent and prior 90-day windows.

    There is no adjudicated churn label in the SRS dataset. This output is a
    transparent rule-based risk screen, not a trained or accuracy-scored model.
    """
    completed = tables["orders"].where(
        F.lower(F.trim("Order_Status")) == "completed"
    ).where(F.col("Customer_ID").isNotNull()).select(
        "Order_ID", "Customer_ID", F.to_date("Order_Date").alias("order_date"),
        F.col("Total_Amount").cast("double").alias("order_value"),
    )
    dataset_end = completed.agg(F.max("order_date").alias("dataset_end_date"))
    windowed = completed.crossJoin(dataset_end)
    recent = F.col("order_date") > F.date_sub(F.col("dataset_end_date"), 90)
    prior = (F.col("order_date") <= F.date_sub(F.col("dataset_end_date"), 90)) & (
        F.col("order_date") > F.date_sub(F.col("dataset_end_date"), 180)
    )
    orders_by_customer = windowed.groupBy("Customer_ID").agg(
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("Order_ID").alias("total_completed_orders"),
        F.countDistinct(F.when(recent, F.col("Order_ID"))).alias("recent_order_count"),
        F.countDistinct(F.when(prior, F.col("Order_ID"))).alias("prior_order_count"),
        F.countDistinct(F.when(recent, F.col("order_date"))).alias("recent_visit_days"),
        F.countDistinct(F.when(prior, F.col("order_date"))).alias("prior_visit_days"),
        F.sum(F.when(recent, F.col("order_value")).otherwise(0.0)).alias("recent_monetary_value"),
        F.sum(F.when(prior, F.col("order_value")).otherwise(0.0)).alias("prior_monetary_value"),
        F.max("dataset_end_date").alias("dataset_end_date"),
    ).withColumn("recency_days", F.datediff("dataset_end_date", "last_order_date"))

    category_lines = completed.select("Order_ID", "Customer_ID", "order_date").join(
        tables["order_items"].select("Order_ID", "Item_ID"), "Order_ID", "inner"
    ).join(tables["menu_items"].select("Item_ID", "Category_ID"), "Item_ID", "inner").crossJoin(dataset_end)
    category_recent = F.col("order_date") > F.date_sub(F.col("dataset_end_date"), 90)
    category_prior = (F.col("order_date") <= F.date_sub(F.col("dataset_end_date"), 90)) & (
        F.col("order_date") > F.date_sub(F.col("dataset_end_date"), 180)
    )
    category_counts = category_lines.groupBy("Customer_ID").agg(
        F.countDistinct(F.when(category_recent, F.col("Category_ID"))).alias("recent_category_count"),
        F.countDistinct(F.when(category_prior, F.col("Category_ID"))).alias("prior_category_count"),
    )
    result = tables["customers"].select("Customer_ID", "Signup_Date").join(
        orders_by_customer, "Customer_ID", "left"
    ).join(category_counts, "Customer_ID", "left").fillna(
        0, subset=["total_completed_orders", "recent_order_count", "prior_order_count",
                   "recent_visit_days", "prior_visit_days", "recent_monetary_value",
                   "prior_monetary_value", "recent_category_count", "prior_category_count"]
    ).withColumn(
        "increasing_recency",
        (F.col("total_completed_orders") >= 2) & (F.col("recency_days") >= 60),
    ).withColumn(
        "declining_frequency",
        (F.col("prior_order_count") >= 2) & (F.col("recent_order_count") < 0.5 * F.col("prior_order_count")),
    ).withColumn(
        "declining_monetary_value",
        (F.col("prior_monetary_value") > 0) & (F.col("recent_monetary_value") < 0.5 * F.col("prior_monetary_value")),
    ).withColumn(
        "reduced_category_diversity",
        (F.col("prior_category_count") > 0) & (F.col("recent_category_count") < F.col("prior_category_count")),
    ).withColumn(
        "lower_visit_frequency",
        (F.col("prior_visit_days") >= 2) & (F.col("recent_visit_days") < 0.5 * F.col("prior_visit_days")),
    )
    score_expr = sum(F.col(name).cast("int") for name in CHURN_FACTORS)
    result = result.withColumn(
        "engagement_trend_pct",
        F.when(F.col("prior_order_count") > 0,
               (F.col("recent_order_count") - F.col("prior_order_count"))
               / F.col("prior_order_count") * 100.0),
    ).withColumn("churn_risk_factor_count", score_expr).withColumn(
        "churn_risk_level",
        F.when(F.col("churn_risk_factor_count") >= 3, "High")
        .when(F.col("churn_risk_factor_count") >= 2, "Medium")
        .otherwise("Low"),
    ).withColumn(
        "churn_signal_summary",
        F.concat_ws("; ",
            F.when(F.col("increasing_recency"), F.lit("recency increased")),
            F.when(F.col("declining_frequency"), F.lit("frequency declined")),
            F.when(F.col("declining_monetary_value"), F.lit("monetary value declined")),
            F.when(F.col("reduced_category_diversity"), F.lit("category diversity reduced")),
            F.when(F.col("lower_visit_frequency"), F.lit("visit frequency declined")),
        ),
    ).withColumn(
        "risk_method", F.lit("rule-based 90-day engagement comparison; not a trained churn classifier")
    ).withColumn(
        "validation_status", F.lit("screening signal; no adjudicated churn label is available in the SRS dataset")
    )
    return result
