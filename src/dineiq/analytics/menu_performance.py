"""descriptive analytics profitability, four-way menu classification and tricky-case flags."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


MENU_CLASSES = ("Profit Driver", "Volume Driver", "Hidden Opportunity", "Low Performer")
TRICKY_CASE_FLAGS = (
    "high_selling_loss_making", "high_profit_rarely_purchased", "popular_high_wastage",
    "high_rating_poor_profitability", "low_rating_high_sales", "promotion_dependent",
    "location_variable_performance", "weekend_only_performance", "seasonal_item",
    "new_item_insufficient_history",
)


def add_performance_class(frame: DataFrame) -> DataFrame:
    """Assign every item using multi-factor percentile scores.

    Quantiles are computed from the current item population. Rule cutoffs and
    precedence are documented in docs/descriptive_analytics_design.md.
    """
    # Keep the public helper usable with compact contract fixtures that only
    # provide the three score columns.  Full pipeline frames carry the
    # calculated acceptable-wastage signal; minimal callers default to the
    # historical class rules rather than failing analysis-time resolution.
    acceptable_wastage = (
        F.coalesce(F.col("acceptable_wastage"), F.lit(True))
        if "acceptable_wastage" in frame.columns else F.lit(True)
    )
    return frame.withColumn(
        "performance_class",
        F.when((F.col("profitability_score") >= 0.60) & (F.col("demand_score") >= 0.40)
               & acceptable_wastage, MENU_CLASSES[0])
        .when(F.col("demand_score") >= 0.60, MENU_CLASSES[1])
        .when((F.col("opportunity_score") >= 0.60) & (F.col("demand_score") < 0.60), MENU_CLASSES[2])
        .otherwise(MENU_CLASSES[3]),
    )


def add_tricky_case_flags(frame: DataFrame) -> DataFrame:
    """Add explicit booleans for the ten named menu-performance edge cases."""
    expressions = {
        "high_selling_loss_making": (F.col("demand_score") >= 0.75) & (F.col("contribution_margin") < 0),
        "high_profit_rarely_purchased": (F.col("profitability_score") >= 0.75) & (F.col("order_frequency") <= 1),
        "popular_high_wastage": (F.col("demand_score") >= 0.75) & (F.col("wastage_percentile") >= 0.75),
        "high_rating_poor_profitability": (F.col("average_rating") >= 4.0) & (F.col("profitability_score") < 0.50),
        "low_rating_high_sales": (F.col("average_rating") <= 2.5) & (F.col("demand_score") >= 0.75),
        "promotion_dependent": F.col("promotion_dependency") >= 50.0,
        "location_variable_performance": (F.col("location_cv_percentile") >= 0.75) & (F.col("location_count") >= 2),
        "weekend_only_performance": F.col("weekend_order_ratio") >= 1.0,
        "seasonal_item": (F.col("active_month_count") >= 4) & (F.col("top_three_month_share") >= 0.75),
        "new_item_insufficient_history": F.col("item_age_days").between(0, 90)
        & (F.coalesce(F.col("active_month_count"), F.lit(0)) <= 3),
    }
    result = frame
    for name, expression in expressions.items():
        result = result.withColumn(name, F.coalesce(expression, F.lit(False)))
    return result


def _percentile_rank(frame: DataFrame, source: str, target: str, ascending: bool = True) -> DataFrame:
    order = F.col(source).asc_nulls_first() if ascending else F.col(source).desc_nulls_last()
    return frame.withColumn(
        target,
        F.when(F.col(source).isNotNull(), F.percent_rank().over(Window.orderBy(order))),
    )


def build_menu_performance(
    tables: dict[str, DataFrame],
    menu_features: DataFrame,
    menu_location_features: DataFrame,
) -> dict[str, DataFrame]:
    """Create item profitability/classification and ten-case evidence outputs."""
    orders = tables["orders"].where(F.lower(F.trim(F.col("Order_Status"))) == "completed")
    # order_items may already contain an Order_Date column in the canonical
    # dataset. Alias the authoritative orders date so the join cannot create
    # ambiguous references.
    lines = tables["order_items"].where(F.col("Quantity") > 0).join(
        orders.select("Order_ID", F.col("Order_Date").alias("_menu_order_date")),
        "Order_ID", "inner",
    )
    monthly = lines.where(F.col("_menu_order_date").isNotNull()).withColumn(
        "sales_month", F.date_format("_menu_order_date", "yyyy-MM")
    ).groupBy("Item_ID", "sales_month").agg(F.sum("Quantity").alias("month_units"))
    monthly_summary = monthly.groupBy("Item_ID").agg(
        F.countDistinct("sales_month").alias("active_month_count"),
        F.sum("month_units").alias("total_month_units"),
    )
    top_months = monthly.withColumn(
        "month_rank", F.row_number().over(Window.partitionBy("Item_ID").orderBy(F.col("month_units").desc(), F.col("sales_month")))
    ).where(F.col("month_rank") <= 3).groupBy("Item_ID").agg(
        F.sum("month_units").alias("top_three_month_units")
    )
    seasonality = monthly_summary.join(top_months, "Item_ID", "left").withColumn(
        "top_three_month_share",
        F.when(F.col("total_month_units") > 0, F.col("top_three_month_units") / F.col("total_month_units")),
    ).drop("top_three_month_units", "total_month_units")

    # Keep a transparent trend signal for the SRS sales-trend dimension.  The
    # comparison is against two equal, trailing 90-day windows, so it remains
    # deterministic and does not use future observations.
    dated_sales = lines.withColumn("order_date", F.to_date("_menu_order_date"))
    end_date = dated_sales.agg(F.max("order_date").alias("end_date"))
    trend = dated_sales.crossJoin(end_date).groupBy("Item_ID").agg(
        F.sum(F.when(F.col("order_date") > F.date_sub(F.col("end_date"), 90), F.col("Quantity")).otherwise(0.0)).alias("recent_units"),
        F.sum(F.when(
            (F.col("order_date") <= F.date_sub(F.col("end_date"), 90))
            & (F.col("order_date") > F.date_sub(F.col("end_date"), 180)),
            F.col("Quantity"),
        ).otherwise(0.0)).alias("prior_units"),
    ).withColumn(
        "sales_trend",
        F.when(F.col("prior_units") > 0,
               (F.col("recent_units") - F.col("prior_units")) / F.col("prior_units") * 100.0)
        .otherwise(F.when(F.col("recent_units") > 0, F.lit(100.0)).otherwise(F.lit(0.0))),
    ).select("Item_ID", "sales_trend")

    location = menu_location_features.groupBy("Item_ID").agg(
        F.countDistinct("Location_ID").alias("location_count"),
        F.avg("item_location_revenue").alias("mean_location_revenue"),
        F.stddev_pop("item_location_revenue").alias("std_location_revenue"),
    ).withColumn(
        "location_revenue_cv",
        F.when(F.col("mean_location_revenue") > 0, F.col("std_location_revenue") / F.col("mean_location_revenue")),
    )

    dataset_end = orders.agg(F.max("Order_Date").alias("dataset_end_date"))
    items = tables["menu_items"].select(
        "Item_ID", "Item_Name", "Category_ID", "Selling_Price", "Ingredient_Cost", "Launch_Date"
    )
    result = items.join(menu_features, "Item_ID", "left").join(location, "Item_ID", "left")
    result = result.join(seasonality, "Item_ID", "left").join(trend, "Item_ID", "left").crossJoin(dataset_end).withColumn(
        "item_age_days", F.datediff(F.col("dataset_end_date"), F.col("Launch_Date"))
    )

    # Small fixtures and older cached feature artifacts may not contain every
    # optional indicator.  Materialize the missing indicators as nulls so the
    # output contract remains stable and the UI can explain unavailable data.
    for column in ("average_rating", "repeat_purchase_rate", "wastage_percentage",
                   "promotion_dependency", "discount_percentage", "sales_trend"):
        if column not in result.columns:
            result = result.withColumn(column, F.lit(None).cast("double"))

    zero_columns = (
        "item_revenue", "cost", "contribution_margin", "profit_percentage", "order_frequency",
        "item_popularity", "promotion_dependency", "weekend_order_ratio",
    )
    result = result.fillna(0, subset=list(zero_columns)).withColumn(
        "wastage_percentile", F.lit(None).cast("double")
    )
    result = _percentile_rank(result, "contribution_margin", "margin_rank")
    result = _percentile_rank(result, "profit_percentage", "profit_rate_rank")
    result = _percentile_rank(result, "item_popularity", "popularity_rank")
    result = _percentile_rank(result, "order_frequency", "frequency_rank")
    result = _percentile_rank(result, "wastage_percentage", "wastage_percentile")
    result = _percentile_rank(result, "location_revenue_cv", "location_cv_percentile")
    result = _percentile_rank(result, "average_rating", "rating_rank")
    result = _percentile_rank(result, "repeat_purchase_rate", "repeat_rank")
    result = result.withColumn(
        "profitability_score", (F.col("margin_rank") + F.col("profit_rate_rank")) / 2.0
    ).withColumn(
        "demand_score", (F.col("popularity_rank") + F.col("frequency_rank")) / 2.0
    ).withColumn(
        "opportunity_score",
        (F.coalesce(F.col("rating_rank"), F.lit(0.5))
         + F.coalesce(F.col("repeat_rank"), F.lit(0.5))
         + F.col("margin_rank")) / 3.0,
    ).withColumn(
        "acceptable_wastage",
        F.col("wastage_percentile").isNull() | (F.col("wastage_percentile") <= F.lit(0.75)),
    )
    classified = add_performance_class(result)
    classified = add_tricky_case_flags(classified)

    profitability = classified.select(
        "Item_ID", "Item_Name", "Category_ID", "item_revenue", "cost", "contribution_margin",
        "profit_percentage", "item_popularity", "order_frequency", "average_rating",
        "repeat_purchase_rate", "wastage_percentage", "promotion_dependency",
        "discount_percentage", "sales_trend", "acceptable_wastage", "profitability_score",
        "demand_score", "opportunity_score", "performance_class",
    )
    # Preserve the established two-identifier-plus-ten-flags contract.  The
    # detailed profitability artifact retains the supporting indicators, while
    # this boolean view remains stable for downstream consumers and tests.
    tricky = classified.select("Item_ID", "Item_Name", *TRICKY_CASE_FLAGS)
    return {"menu_profitability_classification": profitability, "tricky_menu_cases": tricky}
