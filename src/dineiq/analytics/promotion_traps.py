"""Detect the five promotion-trap patterns named in the DineIQ SRS."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def build_promotion_traps(
    tables: dict[str, DataFrame], effectiveness: DataFrame, min_item_pairs: int = 5
) -> DataFrame:
    """Flag campaign-level warning patterns with their measured evidence.

    The flags are descriptive comparisons, not causal claims. Thresholds that
    the SRS leaves open are explicit parameters or documented rule constants.
    """
    if min_item_pairs < 1:
        raise ValueError("min_item_pairs must be positive")

    promo = effectiveness.withColumn(
        "promotion_sales_up_profit_down",
        (F.col("revenue_change_pct") > 0) & (F.col("margin_change_pct") <= 0),
    ).withColumn(
        "promotion_customer_up_margin_collapse",
        (F.col("promotion_customers") > F.col("pre_customer_count"))
        & (F.col("pre_order_volume") > 0)
        & (F.col("promotion_order_volume") > 0)
        & (
            F.col("promotion_contribution_margin") / F.col("promotion_order_volume")
            <= 0.8 * F.col("pre_contribution_margin") / F.col("pre_order_volume")
        ),
    ).withColumn(
        "promotion_increased_wastage",
        F.coalesce(F.col("wastage_cost_change_pct") > 0, F.lit(False)),
    )

    completed = tables["orders"].where(
        F.lower(F.trim("Order_Status")) == "completed"
    ).select(
        "Order_ID", "Customer_ID", "Promotion_ID",
        F.to_date("Order_Date").alias("order_date"),
    )
    lines = tables["order_items"].select("Order_ID", "Item_ID", "Quantity")
    customer_item_orders = completed.where(F.col("Customer_ID").isNotNull()).join(
        lines, "Order_ID", "inner"
    ).select("Customer_ID", "Item_ID", "Promotion_ID", "order_date")
    windows = promo.select("Promotion_ID", "start_date", "end_date")
    campaign_pairs = customer_item_orders.where(
        F.col("Promotion_ID").isNotNull() & (F.trim("Promotion_ID") != "")
    ).join(windows, "Promotion_ID", "inner").where(
        F.col("order_date").between(F.col("start_date"), F.col("end_date"))
    ).select("Promotion_ID", "Customer_ID", "Item_ID", "start_date", "end_date").dropDuplicates(
        ["Promotion_ID", "Customer_ID", "Item_ID"]
    )
    outside = campaign_pairs.alias("c").join(
        customer_item_orders.alias("o"),
        (F.col("c.Customer_ID") == F.col("o.Customer_ID"))
        & (F.col("c.Item_ID") == F.col("o.Item_ID")),
        "inner",
    ).where(
        (F.col("o.order_date") < F.col("c.start_date"))
        | (F.col("o.order_date") > F.col("c.end_date"))
    ).select(F.col("c.Promotion_ID").alias("Promotion_ID"),
             F.col("c.Customer_ID").alias("Customer_ID"),
             F.col("c.Item_ID").alias("Item_ID")).dropDuplicates()
    outside_counts = outside.groupBy("Promotion_ID").agg(
        F.countDistinct(F.struct("Customer_ID", "Item_ID")).alias("pairs_with_outside_purchase")
    )
    pair_counts = campaign_pairs.groupBy("Promotion_ID").agg(
        F.countDistinct(F.struct("Customer_ID", "Item_ID")).alias("campaign_customer_item_pairs")
    ).join(outside_counts, "Promotion_ID", "left").fillna(0, subset=["pairs_with_outside_purchase"])
    pair_counts = pair_counts.withColumn(
        "discount_window_only_share",
        F.when(F.col("campaign_customer_item_pairs") > 0,
               1 - F.col("pairs_with_outside_purchase") / F.col("campaign_customer_item_pairs")),
    ).withColumn(
        "purchases_only_during_discount_window",
        (F.col("campaign_customer_item_pairs") >= min_item_pairs)
        & (F.col("discount_window_only_share") >= 0.75),
    )

    # Compare same-category item demand around each campaign. A shift flag is
    # raised only when a higher-margin item loses at least 20% of its prior
    # 30-day volume while another item in that category is sold on campaign.
    menu = tables["menu_items"].select("Item_ID", "Category_ID")
    promoted_categories = customer_item_orders.where(
        F.col("Promotion_ID").isNotNull() & (F.trim("Promotion_ID") != "")
    ).join(windows, "Promotion_ID", "inner").where(
        F.col("order_date").between(F.col("start_date"), F.col("end_date"))
    ).join(menu, "Item_ID", "inner").select("Promotion_ID", "Category_ID").dropDuplicates()

    campaign_lines = completed.select("Order_ID", "Promotion_ID", "order_date")
    daily_item_sales = campaign_lines.join(
        tables["order_items"].select("Order_ID", "Item_ID", "Quantity", "Gross_Profit"),
        "Order_ID", "inner",
    ).groupBy("Item_ID", "order_date").agg(
        F.sum("Quantity").alias("units"), F.sum("Gross_Profit").alias("margin")
    )
    window_rows = windows.alias("p").crossJoin(daily_item_sales.alias("d")).where(
        (F.col("d.order_date") >= F.date_sub(F.col("p.start_date"), 30))
        & (F.col("d.order_date") <= F.col("p.end_date"))
    ).groupBy(F.col("p.Promotion_ID").alias("Promotion_ID"), F.col("d.Item_ID").alias("Item_ID")).agg(
        F.sum(F.when(F.col("d.order_date") < F.col("p.start_date"), F.col("d.units")).otherwise(0)).alias("pre_units"),
        F.sum(F.when(F.col("d.order_date").between(F.col("p.start_date"), F.col("p.end_date")), F.col("d.units")).otherwise(0)).alias("campaign_units"),
    )
    margin_per_unit = daily_item_sales.groupBy("Item_ID").agg(
        (F.sum("margin") / F.sum("units")).alias("margin_per_unit")
    ).join(menu, "Item_ID", "inner")
    category_cutoffs = margin_per_unit.groupBy("Category_ID").agg(
        F.expr("percentile_approx(margin_per_unit, 0.75)").alias("high_margin_cutoff")
    )
    high_margin_items = margin_per_unit.join(category_cutoffs, "Category_ID").where(
        F.col("margin_per_unit") >= F.col("high_margin_cutoff")
    ).select("Item_ID", "Category_ID", "margin_per_unit")
    category_candidates = promoted_categories.join(high_margin_items, "Category_ID", "inner")
    promo_item_lines = completed.where(
        F.col("Promotion_ID").isNotNull() & (F.trim("Promotion_ID") != "")
    ).join(windows, "Promotion_ID", "inner").where(
        F.col("order_date").between(F.col("start_date"), F.col("end_date"))
    ).join(tables["order_items"].select("Order_ID", "Item_ID", "Quantity", "Gross_Profit"),
          "Order_ID", "inner").join(menu, "Item_ID", "inner")
    campaign_low_margin_sales = promo_item_lines.groupBy("Promotion_ID", "Category_ID").agg(
        (F.sum("Gross_Profit") / F.sum("Quantity")).alias("promo_category_margin_per_unit")
    )
    shifted = category_candidates.join(window_rows, ["Promotion_ID", "Item_ID"], "inner").join(
        campaign_low_margin_sales, ["Promotion_ID", "Category_ID"], "inner"
    ).where(
        (F.col("campaign_units") > 0)
        & (F.col("pre_units") > 0)
        & (F.col("campaign_units") <= 0.8 * F.col("pre_units"))
        & (F.col("margin_per_unit") > F.col("promo_category_margin_per_unit"))
    ).groupBy("Promotion_ID").agg(
        F.countDistinct("Item_ID").alias("higher_margin_items_with_declining_sales")
    ).withColumn("sales_shift_from_more_profitable_product", F.col("higher_margin_items_with_declining_sales") > 0)

    return promo.join(pair_counts.select(
        "Promotion_ID", "campaign_customer_item_pairs", "discount_window_only_share",
        "purchases_only_during_discount_window",
    ), "Promotion_ID", "left").join(shifted, "Promotion_ID", "left").fillna(
        False, subset=["purchases_only_during_discount_window", "sales_shift_from_more_profitable_product"]
    ).fillna(0, subset=["campaign_customer_item_pairs", "higher_margin_items_with_declining_sales"])
