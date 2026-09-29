"""Nine evidence-linked business recommendations with impact-based priority."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


RECOMMENDATION_TYPES = (
    "Promote high-margin Hidden Opportunities",
    "Reduce preparation of high-wastage dishes",
    "Review pricing of price-sensitive dishes",
    "Bundle frequently purchased items",
    "Remove or redesign persistent Low Performers",
    "Increase stock before predicted peak periods",
    "Target selected customer segments",
    "Review ineffective promotions",
    "Investigate anomalous locations",
)

OUTPUT_COLUMNS = (
    "recommendation_type", "recommended_action", "entity_type", "entity_id", "location_id",
    "potential_business_impact", "priority_percentile", "priority", "supporting_evidence",
)


def _rows(frame: DataFrame, recommendation_type: str, action, entity_type: str,
          entity_id, location_id, impact, evidence) -> DataFrame:
    return frame.select(
        F.lit(recommendation_type).alias("recommendation_type"), action.cast("string").alias("recommended_action"),
        F.lit(entity_type).alias("entity_type"), entity_id.cast("string").alias("entity_id"),
        location_id.cast("string").alias("location_id"),
        F.coalesce(F.abs(impact.cast("double")), F.lit(0.0)).alias("potential_business_impact"),
        evidence.cast("string").alias("supporting_evidence"),
    )


def build_recommendations(
    menu_classes: DataFrame,
    wastage_risk: DataFrame,
    price_sensitivity: DataFrame,
    bundles: DataFrame,
    forecasts: DataFrame,
    customer_segments: DataFrame,
    promotion_traps: DataFrame,
    location_comparison: DataFrame,
    sales_anomalies: DataFrame,
    inventory: DataFrame | None = None,
) -> DataFrame:
    """Create recommendations only where corresponding evidence is present."""
    parts = []
    hidden = menu_classes.where(F.col("performance_class") == "Hidden Opportunity")
    parts.append(_rows(hidden, RECOMMENDATION_TYPES[0],
        F.concat(F.lit("Promote menu item "), F.col("Item_Name")), "menu_item", F.col("Item_ID"),
        F.lit(None).cast("string"), F.col("contribution_margin"),
        F.concat(F.lit("Hidden Opportunity; contribution margin="), F.round("contribution_margin", 2),
                 F.lit("; profit percentage="), F.round("profit_percentage", 2),
                 F.lit("; completed order frequency="), F.col("order_frequency"))))

    high_waste = wastage_risk.where(F.col("high_wastage_risk") == F.lit(True))
    parts.append(_rows(high_waste, RECOMMENDATION_TYPES[1],
        F.concat(F.lit("Reduce preparation quantity for item "), F.col("Item_ID")), "menu_item_location",
        F.col("Item_ID"), F.col("Location_ID"), F.col("historical_wastage_cost"),
        F.concat(F.lit("Historical wastage rate="), F.round("historical_wastage_percentage", 2),
                 F.lit("%; quantity wasted="), F.round("historical_quantity_wasted", 2),
                 F.lit("; percentile cutoff="), F.col("risk_cutoff_value"))))

    sensitive = price_sensitivity.where(F.col("price_sensitivity_class").isin(
        "Highly Price Sensitive", "Moderately Price Sensitive"
    ))
    parts.append(_rows(sensitive, RECOMMENDATION_TYPES[2],
        F.concat(F.lit("Review price for item "), F.col("Item_ID")), "menu_item", F.col("Item_ID"),
        F.lit(None).cast("string"), F.coalesce(F.abs(F.col("revenue_before")) * F.abs(F.col("revenue_change_pct")) / 100,
                                                 F.abs(F.col("demand_change_pct"))),
        F.concat(F.lit("Class="), F.col("price_sensitivity_class"), F.lit("; observed elasticity="),
                 F.round("observed_elasticity", 3), F.lit("; demand change="),
                 F.round("demand_change_pct", 2), F.lit("%; revenue change="),
                 F.round("revenue_change_pct", 2), F.lit("%"))))

    parts.append(_rows(bundles, RECOMMENDATION_TYPES[3],
        F.concat(F.lit("Offer item "), F.col("antecedent_item_id"), F.lit(" with "), F.col("consequent_item_id")),
        "item_pair", F.concat_ws("→", "antecedent_item_id", "consequent_item_id"),
        F.lit(None).cast("string"), F.col("lift") * F.col("pair_order_count"),
        F.concat(F.lit("Pair orders="), F.col("pair_order_count"), F.lit("; support="), F.round("support", 4),
                 F.lit("; confidence="), F.round("confidence", 4), F.lit("; lift="), F.round("lift", 4))))

    low = menu_classes.where(F.col("performance_class") == "Low Performer")
    if "active_month_count" in menu_classes.columns:
        low = low.withColumn(
            "persistent_low_performer",
            F.coalesce(F.col("active_month_count"), F.lit(0)) >= 3,
        ).where(F.col("persistent_low_performer"))
    else:
        low = low.withColumn("persistent_low_performer", F.lit(True))
    parts.append(_rows(low, RECOMMENDATION_TYPES[4],
        F.concat(F.lit("Review persistent Low Performer item "), F.col("Item_Name")), "menu_item",
        F.col("Item_ID"), F.lit(None).cast("string"), F.abs(F.col("contribution_margin")),
        F.concat(F.lit("Low Performer; revenue="), F.round("item_revenue", 2),
                 F.lit("; contribution margin="), F.round("contribution_margin", 2),
                 F.lit("; completed order frequency="), F.col("order_frequency"),
                 F.lit("; persistence evidence="), F.col("persistent_low_performer"))))

    forecast_totals = forecasts.withColumn("Date", F.to_date("Date")).groupBy("Item_ID", "Location_ID").agg(
        F.sum("forecast_quantity").alias("forecast_horizon_demand"),
        F.min("Date").alias("forecast_start"), F.max("Date").alias("forecast_end"),
    )
    peak_forecasts = forecasts
    if "peak_demand_period" in forecasts.columns:
        peak_forecasts = forecasts.where(F.col("peak_demand_period"))
    else:
        peak_cutoff = forecasts.agg(F.expr("percentile_approx(forecast_quantity, 0.75)").alias("cutoff"))
        peak_forecasts = forecasts.crossJoin(peak_cutoff).where(F.col("forecast_quantity") >= F.col("cutoff"))
    peak_totals = peak_forecasts.withColumn("Date", F.to_date("Date")).groupBy("Item_ID", "Location_ID").agg(
        F.sum("forecast_quantity").alias("peak_forecast_demand"),
        F.min("Date").alias("peak_start"), F.max("Date").alias("peak_end"),
    )
    stock_context = peak_totals
    if inventory is not None and {"Item_ID", "Location_ID"}.issubset(inventory.columns):
        inventory_columns = [name for name in ("Item_ID", "Location_ID", "Current_Stock", "Reorder_Level", "Stock_Status") if name in inventory.columns]
        stock_context = peak_totals.join(
            inventory.select(*inventory_columns).dropDuplicates(["Item_ID", "Location_ID"]),
            ["Item_ID", "Location_ID"], "left",
        )
        if {"Current_Stock", "Reorder_Level"}.issubset(stock_context.columns):
            stock_context = stock_context.where(
                F.col("Current_Stock").isNull() | (F.col("Current_Stock") < F.col("Reorder_Level"))
            )
    for optional in ("Current_Stock", "Reorder_Level"):
        if optional not in stock_context.columns:
            stock_context = stock_context.withColumn(optional, F.lit(None).cast("double"))
    stock_evidence = F.concat(
        F.lit("Predicted peak demand="), F.round("peak_forecast_demand", 2),
        F.lit(" units between "), F.col("peak_start"), F.lit(" and "), F.col("peak_end"),
        F.when(F.col("Current_Stock").isNotNull(),
               F.concat(F.lit("; current stock="), F.col("Current_Stock"))).otherwise(F.lit("")),
    )
    parts.append(_rows(
        stock_context.where(F.col("peak_forecast_demand") > 0), RECOMMENDATION_TYPES[5],
        F.concat(F.lit("Plan stock for item "), F.col("Item_ID"), F.lit(" before forecast period")),
        "menu_item_location", F.col("Item_ID"), F.col("Location_ID"), F.col("peak_forecast_demand"),
        stock_evidence,
    ))

    segment_summary = customer_segments.groupBy("customer_segment").agg(
        F.countDistinct("Customer_ID").alias("customer_count"),
        F.avg("monetary_value").alias("average_monetary_value"),
        F.avg("frequency").alias("average_order_frequency"),
    ).where(F.col("customer_count") > 0)
    segment_action = F.when(F.col("customer_segment").isin("At-Risk", "Churn Risk"), "win back with a targeted reactivation offer") \
        .when(F.col("customer_segment").isin("High-Value Loyal", "Frequent"), "protect loyalty with early access and bundles") \
        .when(F.col("customer_segment").isin("Promotion-Driven", "Promotion Sensitive"), "use measured promotions with margin guardrails") \
        .otherwise("tailor content to the segment's observed purchase behavior")
    parts.append(_rows(segment_summary, RECOMMENDATION_TYPES[6],
        F.concat(F.lit("Target the "), F.col("customer_segment"), F.lit(" segment: "), segment_action),
        "customer_segment", F.col("customer_segment"), F.lit(None).cast("string"),
        F.col("customer_count") * F.coalesce(F.col("average_monetary_value"), F.lit(0.0)),
        F.concat(F.lit("Customers="), F.col("customer_count"), F.lit("; average monetary value="),
                 F.round("average_monetary_value", 2), F.lit("; average order frequency="),
                 F.round("average_order_frequency", 2))))

    ineffective = promotion_traps.where(
        F.col("promotion_sales_up_profit_down") | F.col("promotion_customer_up_margin_collapse")
        | F.col("promotion_increased_wastage") | F.col("purchases_only_during_discount_window")
        | F.col("sales_shift_from_more_profitable_product")
    )
    parts.append(_rows(ineffective, RECOMMENDATION_TYPES[7],
        F.concat(F.lit("Review promotion "), F.col("Promotion_Name")), "promotion",
        F.col("Promotion_ID"), F.lit(None).cast("string"),
        F.abs(F.coalesce(F.col("promotion_revenue") - F.col("pre_revenue"), F.lit(0.0)))
        + F.abs(F.coalesce(F.col("promotion_wastage_cost") - F.col("pre_wastage_cost"), F.lit(0.0))),
        F.concat_ws("; ",
            F.when(F.col("promotion_sales_up_profit_down"), F.lit("sales increased while contribution margin did not")),
            F.when(F.col("promotion_customer_up_margin_collapse"), F.lit("customer count rose while margin per order fell at least 20%")),
            F.when(F.col("promotion_increased_wastage"), F.concat(F.lit("wastage cost change="), F.round("wastage_cost_change_pct", 2), F.lit("%"))),
            F.when(F.col("purchases_only_during_discount_window"), F.concat(F.lit("discount-window-only customer/item share="), F.round("discount_window_only_share", 3))),
            F.when(F.col("sales_shift_from_more_profitable_product"), F.concat(F.lit("higher-margin items with declining sales="), F.col("higher_margin_items_with_declining_sales"))),
        )))

    suspicious_locations = sales_anomalies.groupBy("location_id").agg(
        F.countDistinct("anomaly_type", "subject_id", "event_date").alias("anomaly_count")
    ).where(F.col("location_id").isNotNull() & (F.col("anomaly_count") > 0)).join(
        location_comparison.select(F.col("Location_ID").cast("string").alias("location_id"),
                                   "revenue", "wastage_cost", "promotion_margin_per_order_change_pct"),
        "location_id", "left",
    )
    parts.append(_rows(suspicious_locations, RECOMMENDATION_TYPES[8],
        F.concat(F.lit("Investigate operational anomalies at location "), F.col("location_id")),
        "location", F.col("location_id"), F.col("location_id"), F.col("anomaly_count"),
        F.concat(F.lit("Anomaly count="), F.col("anomaly_count"), F.lit("; revenue="),
                 F.round("revenue", 2), F.lit("; wastage cost="), F.round("wastage_cost", 2),
                 F.lit("; promotion margin/order change="), F.round("promotion_margin_per_order_change_pct", 2), F.lit("%"))))

    combined = parts[0]
    for part in parts[1:]:
        combined = combined.unionByName(part)
    priority_window = Window.partitionBy("recommendation_type").orderBy(
        F.col("potential_business_impact").asc_nulls_first()
    )
    return combined.withColumn("priority_percentile", F.percent_rank().over(priority_window)).withColumn(
        "priority",
        F.when(F.col("priority_percentile") >= 0.95, "Critical")
        .when(F.col("priority_percentile") >= 0.75, "High")
        .when(F.col("priority_percentile") >= 0.50, "Medium")
        .otherwise("Low"),
    ).select(*OUTPUT_COLUMNS).where(
        F.col("supporting_evidence").isNotNull() & (F.length(F.trim("supporting_evidence")) > 0)
    )
