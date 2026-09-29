"""Market-basket support, confidence, lift and evidence-backed bundles."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def build_market_basket(tables: dict[str, DataFrame], min_pair_orders: int = 2) -> dict[str, DataFrame]:
    """Count unordered item pairs and emit both directed association rules.

    Support uses all completed orders as its denominator. Rule confidence uses
    the antecedent's completed-order frequency; lift is confidence divided by
    consequent support. A configurable minimum pair count limits sparse pairs.
    """
    if min_pair_orders < 1:
        raise ValueError("min_pair_orders must be at least 1")
    orders = tables["orders"].where(
        F.lower(F.trim(F.col("Order_Status"))) == "completed"
    ).select("Order_ID").dropDuplicates()
    transaction_count = orders.agg(F.countDistinct("Order_ID").alias("n")).first()["n"]
    if not transaction_count:
        empty = tables["order_items"].limit(0).select(
            F.col("Item_ID").cast("string").alias("antecedent_item_id"),
            F.col("Item_ID").cast("string").alias("consequent_item_id"),
        )
        rules = empty.withColumn("pair_order_count", F.lit(0).cast("long")) \
            .withColumn("support", F.lit(None).cast("double")) \
            .withColumn("confidence", F.lit(None).cast("double")) \
            .withColumn("lift", F.lit(None).cast("double"))
        recommendations = rules.withColumn("recommendation_rank", F.lit(None).cast("int")) \
            .withColumn("recommendation_type", F.lit("Cross-sell / bundle candidate"))
        return {"association_rules": rules, "bundle_recommendations": recommendations.limit(0)}

    baskets = tables["order_items"].where(F.col("Quantity") > 0).join(
        orders, "Order_ID", "inner"
    ).groupBy("Order_ID").agg(
        F.array_sort(F.collect_set(F.col("Item_ID").cast("string"))).alias("items")
    ).where(F.size("items") >= 2)
    left = baskets.select("Order_ID", F.posexplode("items").alias("pos_a", "item_a"))
    right = baskets.select("Order_ID", F.posexplode("items").alias("pos_b", "item_b"))
    pair_counts = left.join(right, "Order_ID", "inner").where(
        F.col("pos_a") < F.col("pos_b")
    ).groupBy("item_a", "item_b").agg(
        F.countDistinct("Order_ID").alias("pair_order_count")
    ).where(F.col("pair_order_count") >= min_pair_orders)

    singleton = tables["order_items"].where(F.col("Quantity") > 0).join(
        orders, "Order_ID", "inner"
    ).groupBy(F.col("Item_ID").cast("string").alias("item_id")).agg(
        F.countDistinct("Order_ID").alias("item_order_count")
    )
    a = singleton.select(F.col("item_id").alias("item_a"), F.col("item_order_count").alias("count_a"))
    b = singleton.select(F.col("item_id").alias("item_b"), F.col("item_order_count").alias("count_b"))
    metrics = pair_counts.join(a, "item_a", "inner").join(b, "item_b", "inner").withColumn(
        "support", F.col("pair_order_count") / F.lit(float(transaction_count))
    ).withColumn(
        "confidence_a_to_b", F.col("pair_order_count") / F.col("count_a")
    ).withColumn(
        "confidence_b_to_a", F.col("pair_order_count") / F.col("count_b")
    ).withColumn(
        "support_a", F.col("count_a") / F.lit(float(transaction_count))
    ).withColumn(
        "support_b", F.col("count_b") / F.lit(float(transaction_count))
    )
    rules_ab = metrics.select(
        F.col("item_a").alias("antecedent_item_id"),
        F.col("item_b").alias("consequent_item_id"), "pair_order_count", "support",
        F.col("confidence_a_to_b").alias("confidence"),
        (F.col("confidence_a_to_b") / F.col("support_b")).alias("lift"),
    )
    rules_ba = metrics.select(
        F.col("item_b").alias("antecedent_item_id"),
        F.col("item_a").alias("consequent_item_id"), "pair_order_count", "support",
        F.col("confidence_b_to_a").alias("confidence"),
        (F.col("confidence_b_to_a") / F.col("support_a")).alias("lift"),
    )
    rules = rules_ab.unionByName(rules_ba)
    window = Window.partitionBy("antecedent_item_id").orderBy(
        F.col("lift").desc(), F.col("confidence").desc(), F.col("pair_order_count").desc(),
        F.col("consequent_item_id").asc(),
    )
    recommendations = rules.withColumn("recommendation_rank", F.row_number().over(window)).where(
        F.col("recommendation_rank") <= 5
    )
    if "menu_items" in tables:
        menu = tables["menu_items"].select(
            F.col("Item_ID").cast("string").alias("menu_item_id"),
            F.col("Item_Name").alias("menu_item_name"),
            F.col("Category_ID").cast("string").alias("menu_category_id"),
            F.col("Selling_Price").cast("double").alias("menu_price"),
        )
        antecedent = menu.select(
            F.col("menu_item_id").alias("antecedent_item_id"),
            F.col("menu_item_name").alias("antecedent_item_name"),
            F.col("menu_category_id").alias("antecedent_category_id"),
            F.col("menu_price").alias("antecedent_price"),
        )
        consequent = menu.select(
            F.col("menu_item_id").alias("consequent_item_id"),
            F.col("menu_item_name").alias("consequent_item_name"),
            F.col("menu_category_id").alias("consequent_category_id"),
            F.col("menu_price").alias("consequent_price"),
        )
        recommendations = recommendations.join(antecedent, "antecedent_item_id", "left").join(
            consequent, "consequent_item_id", "left"
        ).withColumn(
            "recommendation_type",
            F.when((F.col("antecedent_category_id") == F.col("consequent_category_id"))
                   & (F.col("consequent_price") > F.col("antecedent_price")), "Upsell opportunity")
            .when(F.col("antecedent_category_id") == F.col("consequent_category_id"), "Combo meal / frequently paired dish")
            .otherwise("Cross-sell opportunity"),
        )
    else:
        recommendations = recommendations.withColumn(
            "recommendation_type", F.lit("Cross-sell / bundle candidate")
        )
    return {"association_rules": rules, "bundle_recommendations": recommendations}
