from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


ALIASES: dict[str, dict[str, str]] = {
    "inventory": {
        "Restaurant_ID": "Location_ID",
        "Month": "Inventory_Date",
        "Received_Stock": "Purchases",
        "Consumed_Stock": "Units_Sold",
    },
    "menu_items": {
        "Base_Price": "Selling_Price",
        "Base_Cost": "Ingredient_Cost",
        "Introduced_Date": "Launch_Date",
    },
    "orders": {
        "Restaurant_ID": "Location_ID",
        "Status": "Order_Status",
        "Channel": "Channel_ID",
    },
    "pricing_history": {
        "Pricing_ID": "Price_History_ID",
        "Price": "New_Price",
        "Effective_Date": "Effective_From",
        "End_Date": "Effective_To",
    },
    "ratings": {
        "Restaurant_ID": "Location_ID",
        "Rating_Value": "Rating",
        "Rating_Date": "Review_Date",
    },
    "restaurants": {
        "Restaurant_ID": "Location_ID",
        "Locality": "Area",
        "District": "Region",
    },
    "wastage": {
        "Restaurant_ID": "Location_ID",
        "Date": "Wastage_Date",
        "Prepared_Quantity": "Preparation_Quantity",
        "Wastage_Quantity": "Quantity_Wasted",
        "Wastage_Cost": "Total_Wastage_Cost",
        "Reason": "Wastage_Reason",
    },
}


def normalize_table(name: str, frame: DataFrame) -> DataFrame:
    """Create analytical aliases and derived fields for one main dataset table."""
    result = frame
    for source, target in ALIASES.get(name.lower(), {}).items():
        if source in result.columns and target not in result.columns:
            result = result.withColumn(target, F.col(source))

    if name == "orders":
        if "Order_DateTime" in result.columns and "Order_Time" not in result.columns:
            result = result.withColumn("Order_Time", F.date_format("Order_DateTime", "HH:mm:ss"))
    elif name == "order_items":
        if {"Line_Subtotal", "Line_Total"}.issubset(result.columns):
            result = result.withColumn(
                "Discount_Amount", F.col("Line_Subtotal") - F.col("Line_Total")
            )
    elif name == "promotions" and "Discount_Percent" in result.columns:
        if "Discount_Percentage" not in result.columns:
            result = result.withColumn("Discount_Percentage", F.col("Discount_Percent"))
    elif name == "pricing_history" and {"Price", "Previous_Price"}.issubset(result.columns):
        result = result.withColumn(
            "Price_Change_Percentage",
            F.when(
                F.col("Previous_Price").isNotNull() & (F.col("Previous_Price") != 0),
                (F.col("Price") - F.col("Previous_Price")) / F.col("Previous_Price") * 100,
            ),
        )
    return result


def normalize_tables(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Normalize a table mapping without mutating input frames."""
    result = {name: normalize_table(name, frame) for name, frame in tables.items()}
    if {"menu_items", "pricing_history"}.issubset(result):
        # The item catalog's Base_Price is a fallback/source value. Prefer the
        # latest observed effective-dated chain price for catalog analytics;
        # pricing history has no branch key by design. Refuse ambiguous latest
        # prices instead of choosing a branch- or row-order-specific value.
        history = result["pricing_history"].where(
            F.col("Item_ID").isNotNull()
            & F.col("Effective_From").isNotNull()
            & F.col("New_Price").isNotNull()
        )
        latest_dates = history.groupBy("Item_ID").agg(
            F.max("Effective_From").alias("__latest_effective_from")
        )
        latest_rows = history.join(latest_dates, "Item_ID", "inner").where(
            F.col("Effective_From") == F.col("__latest_effective_from")
        )
        ambiguous = latest_rows.groupBy("Item_ID").agg(
            F.countDistinct("New_Price").alias("__distinct_latest_prices")
        ).where(F.col("__distinct_latest_prices") > 1).limit(1).count()
        if ambiguous:
            raise ValueError(
                "main dataset has conflicting prices for the same item on its latest effective date; "
                "resolve the source evidence before current-price analytics."
            )
        current_prices = latest_rows.groupBy("Item_ID").agg(
            F.first("New_Price", ignorenulls=True).alias("__history_selling_price"),
            F.max("Effective_From").alias("Price_Effective_From"),
        )
        result["menu_items"] = result["menu_items"].join(
            current_prices, "Item_ID", "left"
        ).withColumn(
            "Selling_Price",
            F.coalesce(F.col("__history_selling_price"), F.col("Base_Price")),
        ).withColumn(
            "Selling_Price_Source",
            F.when(F.col("__history_selling_price").isNotNull(), "pricing_history_latest_observed_effective")
            .otherwise("menu_items_base_price_fallback"),
        ).drop("__history_selling_price")
    if {"order_items", "menu_items"}.issubset(result):
        # Base_Cost is a static synthetic estimate, not an effective-dated cost.
        # Keep it explicitly named as an estimated contribution calculation.
        costs = result["menu_items"].select("Item_ID", "Base_Cost").dropDuplicates(["Item_ID"])
        lines = result["order_items"].drop("Base_Cost", "Gross_Profit").alias("line").join(costs.alias("cost"), "Item_ID", "left")
        result["order_items"] = lines.withColumn(
            "Gross_Profit",
            F.col("Line_Total") - F.col("Quantity") * F.col("Base_Cost"),
        )
    return result
