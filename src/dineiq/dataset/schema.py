"""Canonical table schemas and relationships from the supplied data dictionary."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass(frozen=True)
class TableSchema:
    name: str
    filename: str
    primary_key: str
    columns: tuple[str, ...]


TABLE_SCHEMAS: dict[str, TableSchema] = {
    "customers": TableSchema("customers", "customers.csv", "Customer_ID", (
        "Customer_ID", "Customer_Name", "Age", "Gender", "City", "Area", "Signup_Date",
        "Customer_Segment", "Preferred_Channel", "Preferred_Cuisine", "Loyalty_Tier",
        "Income_Band", "Acquisition_Source", "Is_Active", "Last_Order_Date", "Total_Orders",
        "Lifetime_Value", "Average_Order_Value", "Churn_Status", "Synthetic_Contact", "Is_Duplicate",
    )),
    "inventory": TableSchema("inventory", "inventory.csv", "Inventory_ID", (
        "Inventory_ID", "Location_ID", "Item_ID", "Inventory_Date", "Opening_Stock", "Purchases",
        "Units_Sold", "Adjustments", "Closing_Stock", "Reorder_Level", "Wastage_Quantity",
        "Stockout_Flag", "Supplier_ID", "Is_Anomaly",
    )),
    "menu_categories": TableSchema("menu_categories", "menu_categories.csv", "Category_ID", (
        "Category_ID", "Category_Name", "Description",
    )),
    "menu_items": TableSchema("menu_items", "menu_items.csv", "Item_ID", (
        "Item_ID", "Item_Name", "Category_ID", "Cuisine_Type", "Description", "Size",
        "Selling_Price", "Ingredient_Cost", "Estimated_Preparation_Time", "Calories",
        "Is_Vegetarian", "Is_Spicy", "Is_Active", "Launch_Date", "Discontinued_Date", "Base_Cost",
        "Popularity_Index", "Perishability", "Price_Sensitive", "Price_Sensitivity_Elasticity",
        "Observed_Units_Sold", "Contribution_Margin_PKR", "Is_Popular_Low_Margin",
        "Is_Profitable_Low_Selling", "Is_High_Wastage_Item",
    )),
    "order_items": TableSchema("order_items", "order_items.csv", "Order_Item_ID", (
        "Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Discount_Amount",
        "Line_Total", "Item_Cost", "Gross_Profit", "Special_Request", "Preparation_Time",
        "Item_Status", "Is_Anomaly", "Is_Duplicate", "Base_Quantity", "Price_History_ID",
        "Price_Sensitive", "Price_Sensitivity_Applied", "Promotion_Type_Applied", "Promotion_Qualified",
    )),
    "ordering_channels": TableSchema("ordering_channels", "ordering_channels.csv", "Channel_ID", (
        "Channel_ID", "Channel_Name", "Description",
    )),
    "orders": TableSchema("orders", "orders.csv", "Order_ID", (
        "Order_ID", "Customer_ID", "Location_ID", "Promotion_ID", "Channel_ID", "Order_Date",
        "Order_Time", "Order_DateTime", "Order_Status", "Payment_Method", "Delivery_Type", "Subtotal",
        "Discount_Amount", "Tax_Amount", "Delivery_Fee", "Total_Amount", "Order_Source",
        "Cancellation_Reason", "Is_Anomaly", "Is_Duplicate",
    )),
    "pricing_history": TableSchema("pricing_history", "pricing_history.csv", "Price_History_ID", (
        "Price_History_ID", "Item_ID", "Location_ID", "Effective_From", "Effective_To",
        "Previous_Price", "New_Price", "Price_Change_Percentage", "Reason", "Approved_Date",
    )),
    "promotions": TableSchema("promotions", "promotions.csv", "Promotion_ID", (
        "Promotion_ID", "Promotion_Name", "Promotion_Type", "Discount_Percentage", "Fixed_Discount",
        "Minimum_Order_Value", "Start_Date", "End_Date", "Applicable_Locations", "Applicable_Categories",
        "Usage_Limit", "Promotion_Status", "Is_Misleading",
    )),
    "ratings": TableSchema("ratings", "ratings.csv", "Rating_ID", (
        "Rating_ID", "Customer_ID", "Order_ID", "Item_ID", "Location_ID", "Rating", "Review_Text",
        "Review_Date", "Sentiment", "Is_Verified", "Response_Time_Hours", "Is_Anomaly", "Is_Duplicate",
    )),
    "restaurants": TableSchema("restaurants", "restaurants.csv", "Location_ID", (
        "Location_ID", "Restaurant_Name", "City", "Province", "Area", "Latitude", "Longitude",
        "Opening_Date", "Restaurant_Type", "Seating_Capacity", "Delivery_Radius_KM", "Average_Rating",
        "Manager_ID", "Status", "Volume_Index", "Service_Quality",
    )),
    "wastage": TableSchema("wastage", "wastage.csv", "Wastage_ID", (
        "Wastage_ID", "Location_ID", "Item_ID", "Wastage_Date", "Quantity_Wasted", "Unit_Cost",
        "Total_Wastage_Cost", "Wastage_Reason", "Shift", "Recorded_By", "Preventable_Flag",
        "Is_Anomaly", "Preparation_Quantity", "Wastage_Percentage",
    )),
}

DATE_FIELDS = {
    "Signup_Date", "Last_Order_Date", "Inventory_Date", "Launch_Date", "Discontinued_Date",
    "Order_Date", "Approved_Date", "Start_Date", "End_Date", "Review_Date", "Opening_Date", "Wastage_Date",
}
TIMESTAMP_FIELDS = {"Order_DateTime"}
BOOLEAN_FIELDS = {
    "Is_Active", "Churn_Status", "Is_Duplicate", "Stockout_Flag", "Is_Anomaly", "Is_Vegetarian",
    "Is_Spicy", "Price_Sensitive", "Is_Popular_Low_Margin", "Is_Profitable_Low_Selling",
    "Is_High_Wastage_Item", "Price_Sensitivity_Applied", "Promotion_Qualified", "Is_Misleading",
    "Is_Verified", "Preventable_Flag",
}
STRING_ID_FIELDS = {"Supplier_ID", "Manager_ID", "Recorded_By"}
INTEGER_FIELDS = {
    "Age", "Total_Orders", "Opening_Stock", "Purchases", "Units_Sold", "Adjustments", "Closing_Stock",
    "Reorder_Level", "Selling_Price", "Ingredient_Cost", "Estimated_Preparation_Time", "Calories",
    "Base_Cost", "Observed_Units_Sold", "Contribution_Margin_PKR", "Quantity", "Preparation_Time",
    "Base_Quantity", "Discount_Percentage", "Usage_Limit", "Rating", "Seating_Capacity",
}
DOUBLE_FIELDS = {
    "Lifetime_Value", "Average_Order_Value", "Wastage_Quantity", "Popularity_Index", "Perishability",
    "Price_Sensitivity_Elasticity", "Unit_Price", "Discount_Amount", "Line_Total", "Item_Cost",
    "Gross_Profit", "Subtotal", "Tax_Amount", "Delivery_Fee", "Total_Amount", "Previous_Price",
    "New_Price", "Price_Change_Percentage", "Fixed_Discount", "Minimum_Order_Value", "Latitude",
    "Longitude", "Delivery_Radius_KM", "Average_Rating", "Volume_Index", "Service_Quality",
    "Quantity_Wasted", "Unit_Cost", "Total_Wastage_Cost", "Preparation_Quantity", "Wastage_Percentage",
    "Response_Time_Hours",
}


def spark_type_for(column: str, table: str | None = None) -> DataType:
    """Return the declared source type while preserving documented text IDs/dates."""
    if column in BOOLEAN_FIELDS:
        return BooleanType()
    if column in DATE_FIELDS:
        return DateType()
    if column in TIMESTAMP_FIELDS:
        return TimestampType()
    if column in STRING_ID_FIELDS or (column == "Promotion_ID" and table == "orders"):
        return StringType()
    if column.endswith("_ID"):
        return LongType()
    if column in INTEGER_FIELDS:
        return LongType()
    if column in DOUBLE_FIELDS:
        return DoubleType()
    return StringType()


def raw_schema(table: str) -> StructType:
    spec = TABLE_SCHEMAS[table]
    return StructType([StructField(name, StringType(), True) for name in spec.columns])


def expected_schema(table: str) -> StructType:
    spec = TABLE_SCHEMAS[table]
    return StructType([StructField(name, spark_type_for(name, table), True) for name in spec.columns])


RELATIONSHIPS: tuple[tuple[str, str, str, str], ...] = (
    ("orders", "Customer_ID", "customers", "Customer_ID"),
    ("orders", "Order_ID", "order_items", "Order_ID"),
    ("order_items", "Item_ID", "menu_items", "Item_ID"),
    ("menu_items", "Category_ID", "menu_categories", "Category_ID"),
    ("orders", "Location_ID", "restaurants", "Location_ID"),
    ("orders", "Promotion_ID", "promotions", "Promotion_ID"),
    ("menu_items", "Item_ID", "pricing_history", "Item_ID"),
    ("menu_items", "Item_ID", "ratings", "Item_ID"),
    ("menu_items", "Item_ID", "inventory", "Item_ID"),
    ("menu_items", "Item_ID", "wastage", "Item_ID"),
)

# Child table, child key, parent table, parent key. Used for referential checks.
FOREIGN_KEYS: tuple[tuple[str, str, str, str], ...] = (
    ("orders", "Customer_ID", "customers", "Customer_ID"),
    ("orders", "Location_ID", "restaurants", "Location_ID"),
    ("orders", "Promotion_ID", "promotions", "Promotion_ID"),
    ("orders", "Channel_ID", "ordering_channels", "Channel_ID"),
    ("order_items", "Order_ID", "orders", "Order_ID"),
    ("order_items", "Item_ID", "menu_items", "Item_ID"),
    ("menu_items", "Category_ID", "menu_categories", "Category_ID"),
    ("pricing_history", "Item_ID", "menu_items", "Item_ID"),
    ("ratings", "Item_ID", "menu_items", "Item_ID"),
    ("ratings", "Customer_ID", "customers", "Customer_ID"),
    ("ratings", "Order_ID", "orders", "Order_ID"),
    ("inventory", "Item_ID", "menu_items", "Item_ID"),
    ("inventory", "Location_ID", "restaurants", "Location_ID"),
    ("wastage", "Item_ID", "menu_items", "Item_ID"),
    ("wastage", "Location_ID", "restaurants", "Location_ID"),
)
