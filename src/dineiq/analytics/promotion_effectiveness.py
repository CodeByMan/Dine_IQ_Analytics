"""Multi-KPI descriptive promotion effectiveness analysis."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_promotion_effectiveness(tables: dict[str, DataFrame], post_days: int = 30) -> DataFrame:
    """Compare promotion-linked activity with a prior period and post response.

    This is descriptive, not a causal incrementality estimate. The SRS provides
    no control-group design. Sales alone never determines the assessment: the
    output also considers margin, wastage and post-promotion repeat activity.
    """
    if post_days < 1 or post_days > 180:
        raise ValueError("post_days must be between 1 and 180")
    completed = tables["orders"].where(
        F.lower(F.trim(F.col("Order_Status"))) == "completed"
    ).select(
        "Order_ID", "Customer_ID", "Location_ID", "Promotion_ID",
        F.to_date("Order_Date").alias("order_date"), F.col("Total_Amount").cast("double").alias("revenue"),
    )
    first_order = completed.where(F.col("Customer_ID").isNotNull()).groupBy("Customer_ID").agg(
        F.min("order_date").alias("first_order_date")
    )
    lines = tables["order_items"].where(F.col("Quantity") > 0).groupBy("Order_ID").agg(
        F.sum("Gross_Profit").alias("contribution_margin")
    )
    all_orders = completed.join(lines, "Order_ID", "left").fillna(0, subset=["contribution_margin"])
    promotions = tables["promotions"].select(
        "Promotion_ID", "Promotion_Name", F.to_date("Start_Date").alias("start_date"),
        F.to_date("End_Date").alias("end_date"),
    )
    during_orders = all_orders.where(F.col("Promotion_ID").isNotNull()).join(
        promotions, "Promotion_ID", "inner"
    ).where(F.col("order_date").between(F.col("start_date"), F.col("end_date"))).join(
        first_order, "Customer_ID", "left"
    )
    during = during_orders.groupBy("Promotion_ID", "Promotion_Name", "start_date", "end_date").agg(
        F.countDistinct("Order_ID").alias("promotion_order_volume"),
        F.sum("revenue").alias("promotion_revenue"),
        F.sum("contribution_margin").alias("promotion_contribution_margin"),
        F.countDistinct("Customer_ID").alias("promotion_customers"),
        F.avg("revenue").alias("promotion_average_order_value"),
        F.countDistinct(F.when(F.col("Customer_ID").isNotNull() & (F.col("order_date") == F.col("first_order_date")), F.col("Customer_ID"))).alias("new_customers_during_promotion"),
    )
    # The baseline includes all completed orders during the 30 days before each campaign.
    campaign_windows = promotions.select("Promotion_ID", "Promotion_Name", "start_date", "end_date")
    baseline_events = all_orders.alias("o").crossJoin(campaign_windows.alias("p")).where(
        F.col("o.order_date") >= F.date_sub(F.col("p.start_date"), 30)
    ).where(F.col("o.order_date") < F.col("p.start_date"))
    baseline = baseline_events.groupBy(
        F.col("p.Promotion_ID").alias("Promotion_ID")
    ).agg(
        F.countDistinct("o.Order_ID").alias("pre_order_volume"),
        F.sum("o.revenue").alias("pre_revenue"),
        F.sum("o.contribution_margin").alias("pre_contribution_margin"),
        F.countDistinct("o.Customer_ID").alias("pre_customer_count"),
        F.avg("o.revenue").alias("pre_average_order_value"),
    )
    post_events = all_orders.alias("o").crossJoin(campaign_windows.alias("p")).where(
        F.col("o.order_date") > F.col("p.end_date")
    ).where(F.col("o.order_date") <= F.date_add(F.col("p.end_date"), post_days))
    post = post_events.groupBy(F.col("p.Promotion_ID").alias("Promotion_ID")).agg(
        F.countDistinct("o.Order_ID").alias("post_order_volume"),
        F.sum("o.revenue").alias("post_revenue"),
        F.countDistinct("o.Customer_ID").alias("post_customer_count"),
    )
    participants = during_orders.where(F.col("Customer_ID").isNotNull()).select(
        "Promotion_ID", "Customer_ID", "end_date"
    ).dropDuplicates()
    participant_post = participants.alias("c").join(
        all_orders.alias("o"), F.col("c.Customer_ID") == F.col("o.Customer_ID"), "inner"
    ).where(F.col("o.order_date") > F.col("c.end_date")).where(
        F.col("o.order_date") <= F.date_add(F.col("c.end_date"), post_days)
    )
    repeat_customers = participant_post.groupBy(F.col("c.Promotion_ID").alias("Promotion_ID")).agg(
        F.countDistinct("c.Customer_ID").alias("post_repeat_customers")
    )
    waste = tables["wastage"].groupBy("Wastage_Date").agg(
        F.sum("Total_Wastage_Cost").alias("wastage_cost"),
        F.sum("Quantity_Wasted").alias("wastage_quantity"),
    ).select(F.to_date("Wastage_Date").alias("waste_date"), "wastage_cost", "wastage_quantity")
    waste_campaign = waste.alias("w").crossJoin(campaign_windows.alias("p")).where(
        F.col("w.waste_date").between(F.col("p.start_date"), F.col("p.end_date"))
    ).groupBy(F.col("p.Promotion_ID").alias("Promotion_ID")).agg(
        F.sum("w.wastage_cost").alias("promotion_wastage_cost"),
        F.sum("w.wastage_quantity").alias("promotion_wastage_quantity"),
    )
    waste_pre_campaign = waste.alias("w").crossJoin(campaign_windows.alias("p")).where(
        F.col("w.waste_date") >= F.date_sub(F.col("p.start_date"), 30)
    ).where(F.col("w.waste_date") < F.col("p.start_date")).groupBy(
        F.col("p.Promotion_ID").alias("Promotion_ID")
    ).agg(F.sum("w.wastage_cost").alias("pre_wastage_cost"))
    result = campaign_windows.join(
        during.drop("Promotion_Name", "start_date", "end_date"), "Promotion_ID", "left"
    ).join(baseline, "Promotion_ID", "left").join(post, "Promotion_ID", "left").join(
        repeat_customers, "Promotion_ID", "left"
    ).join(waste_campaign, "Promotion_ID", "left").join(
        waste_pre_campaign, "Promotion_ID", "left"
    ).fillna(0, subset=[
        "promotion_order_volume", "promotion_revenue", "promotion_contribution_margin", "promotion_customers",
        "new_customers_during_promotion", "pre_order_volume", "pre_revenue", "pre_contribution_margin",
        "pre_customer_count", "post_order_volume", "post_revenue", "post_customer_count",
        "post_repeat_customers", "promotion_wastage_cost", "promotion_wastage_quantity", "pre_wastage_cost",
    ])
    result = result.withColumn(
        "revenue_change_pct", F.when(F.col("pre_revenue") != 0, (F.col("promotion_revenue") - F.col("pre_revenue")) / F.col("pre_revenue") * 100.0)
    ).withColumn(
        "margin_change_pct", F.when(F.col("pre_contribution_margin") != 0, (F.col("promotion_contribution_margin") - F.col("pre_contribution_margin")) / F.abs("pre_contribution_margin") * 100.0)
    ).withColumn(
        "order_volume_change_pct", F.when(F.col("pre_order_volume") != 0, (F.col("promotion_order_volume") - F.col("pre_order_volume")) / F.col("pre_order_volume") * 100.0)
    ).withColumn(
        "average_order_value_change_pct", F.when(F.col("pre_average_order_value") > 0, (F.col("promotion_average_order_value") - F.col("pre_average_order_value")) / F.col("pre_average_order_value") * 100.0)
    ).withColumn(
        "wastage_cost_change_pct", F.when(F.col("pre_wastage_cost") > 0, (F.col("promotion_wastage_cost") - F.col("pre_wastage_cost")) / F.col("pre_wastage_cost") * 100.0)
    ).withColumn(
        "multi_kpi_assessment",
        F.when((F.col("revenue_change_pct") > 0) & (F.col("margin_change_pct") > 0)
               & ((F.col("wastage_cost_change_pct") <= 0) | F.col("wastage_cost_change_pct").isNull()),
               "Revenue and contribution margin improved without higher measured wastage cost")
        .when((F.col("revenue_change_pct") > 0) & ((F.col("margin_change_pct") <= 0)
               | (F.col("wastage_cost_change_pct") > 0) | F.col("margin_change_pct").isNull()),
              "Revenue increased with a margin or wastage trade-off")
        .otherwise("No revenue-and-margin improvement demonstrated"),
    ).withColumn("comparison_window_days", F.lit(30)).withColumn("post_window_days", F.lit(post_days)).withColumn(
        "causal_effect_claimed", F.lit(False)
    )
    return result
