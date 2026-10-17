from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import DataType, StructType


@dataclass(frozen=True)
class TableSchema:
    name: str
    filename: str
    primary_key: str
    columns: tuple[str, ...]


TABLE_SCHEMAS: dict[str, TableSchema] = {
    "customers": TableSchema("customers", "Customers.csv", "Customer_ID", (
        "Customer_ID", "First_Name", "Last_Name", "Gender", "Age", "City",
        "Locality", "Home_Restaurant_ID", "Signup_Date", "Email", "Phone",
    )),
    "inventory": TableSchema("inventory", "Inventory.csv", "Inventory_ID", (
        "Inventory_ID", "Restaurant_ID", "Item_ID", "Month", "Opening_Stock",
        "Received_Stock", "Consumed_Stock", "Closing_Stock", "Unit",
    )),
    "menu_categories": TableSchema("menu_categories", "Menu_Categories.csv", "Category_ID", (
        "Category_ID", "Category_Name",
    )),
    "menu_item_availability": TableSchema(
        "menu_item_availability", "Menu_Item_Availability.csv", "Availability_ID",
        ("Availability_ID", "Restaurant_ID", "Item_ID", "Is_Available", "Available_From"),
    ),
    "menu_items": TableSchema("menu_items", "Menu_Items.csv", "Item_ID", (
        "Item_ID", "Item_Name", "Category_ID", "Base_Cost", "Base_Price",
        "Introduced_Date", "Is_Active",
    )),
    "order_items": TableSchema("order_items", "Order_Items.csv", "Order_Item_ID", (
        "Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price",
        "Discount_Percent", "Line_Subtotal", "Line_Total",
    )),
    "orders": TableSchema("orders", "Orders.csv", "Order_ID", (
        "Order_ID", "Customer_ID", "Restaurant_ID", "Order_DateTime", "Order_Date",
        "Channel", "Payment_Method", "Promotion_ID", "Status", "Delivery_Charge",
        "Item_Count", "Subtotal", "Total_Amount", "Discount_Amount", "Tax_Amount",
        "Grand_Total",
    )),
    "pricing_history": TableSchema("pricing_history", "Pricing_History.csv", "Pricing_ID", (
        "Pricing_ID", "Item_ID", "Price", "Previous_Price", "Effective_Date", "End_Date",
    )),
    "promotions": TableSchema("promotions", "Promotions.csv", "Promotion_ID", (
        "Promotion_ID", "Promotion_Name", "Discount_Percent", "Start_Date", "End_Date",
        "Applicable_Category_ID", "Applicable_Restaurant_ID",
    )),
    "ratings": TableSchema("ratings", "Ratings.csv", "Rating_ID", (
        "Rating_ID", "Order_ID", "Customer_ID", "Item_ID", "Restaurant_ID",
        "Rating_Value", "Rating_Date", "Comment", "Reviewer_Name",
    )),
    "restaurants": TableSchema("restaurants", "Restaurants.csv", "Restaurant_ID", (
        "Restaurant_ID", "Chain_Name", "Restaurant_Name", "Locality", "District",
        "Address", "City", "Province", "Country", "Location_Type", "Opening_Date",
        "Seating_Capacity",
    )),
    "wastage": TableSchema("wastage", "Wastage.csv", "Wastage_ID", (
        "Wastage_ID", "Restaurant_ID", "Item_ID", "Date", "Prepared_Quantity",
        "Wastage_Quantity", "Unit", "Reason", "Wastage_Cost",
    )),
}

DATE_FIELDS = {
    "Signup_Date", "Month", "Available_From", "Introduced_Date", "Order_Date",
    "Effective_Date", "End_Date", "Start_Date", "Rating_Date", "Opening_Date", "Date",
}
TIMESTAMP_FIELDS = {"Order_DateTime"}
BOOLEAN_FIELDS = {"Is_Available", "Is_Active"}
INTEGER_FIELDS = {"Age", "Item_Count", "Quantity", "Seating_Capacity", "Rating_Value"}
DECIMAL_FIELDS = {
    "Base_Cost", "Base_Price", "Price", "Previous_Price", "Unit_Price",
    "Discount_Percent", "Line_Subtotal", "Line_Total", "Delivery_Charge", "Subtotal",
    "Total_Amount", "Discount_Amount", "Tax_Amount", "Grand_Total", "Opening_Stock",
    "Received_Stock", "Consumed_Stock", "Closing_Stock", "Prepared_Quantity",
    "Wastage_Quantity", "Wastage_Cost", "Latitude", "Longitude",
}
# Compatibility name retained for the reused ingestion module. Values are
# declared DECIMAL(12,2), not binary floating-point doubles.
DOUBLE_FIELDS = DECIMAL_FIELDS
STRING_ID_FIELDS = {
    column
    for spec in TABLE_SCHEMAS.values()
    for column in spec.columns
    if column.endswith("_ID")
}


def spark_type_for(column: str, table: str | None = None) -> "DataType":
    """Return the typed main dataset view type while preserving IDs as strings."""
    from pyspark.sql.types import (
        BooleanType, DateType, DecimalType, IntegerType, StringType, TimestampType,
    )

    if column in STRING_ID_FIELDS:
        return StringType()
    if column in BOOLEAN_FIELDS:
        return BooleanType()
    if column in DATE_FIELDS:
        return DateType()
    if column in TIMESTAMP_FIELDS:
        return TimestampType()
    if column in INTEGER_FIELDS:
        return IntegerType()
    if column in DECIMAL_FIELDS:
        return DecimalType(12, 2)
    return StringType()


def raw_schema(table: str) -> "StructType":
    """Lossless CSV input schema: source values remain nullable strings."""
    from pyspark.sql.types import StringType, StructField, StructType

    spec = TABLE_SCHEMAS[table]
    return StructType([StructField(name, StringType(), True) for name in spec.columns])


def expected_schema(table: str) -> "StructType":
    """Typed, nullable analytical schema for the declared main dataset columns."""
    from pyspark.sql.types import StructField, StructType

    spec = TABLE_SCHEMAS[table]
    return StructType([StructField(name, spark_type_for(name, table), True) for name in spec.columns])


# (child table, child key, parent table, parent key). Nullable foreign keys,
# where allowed by the source schema, are checked by quality rules separately.
FOREIGN_KEYS: tuple[tuple[str, str, str, str], ...] = (
    ("customers", "Home_Restaurant_ID", "restaurants", "Restaurant_ID"),
    ("inventory", "Restaurant_ID", "restaurants", "Restaurant_ID"),
    ("inventory", "Item_ID", "menu_items", "Item_ID"),
    ("menu_item_availability", "Restaurant_ID", "restaurants", "Restaurant_ID"),
    ("menu_item_availability", "Item_ID", "menu_items", "Item_ID"),
    ("menu_items", "Category_ID", "menu_categories", "Category_ID"),
    ("order_items", "Order_ID", "orders", "Order_ID"),
    ("order_items", "Item_ID", "menu_items", "Item_ID"),
    ("orders", "Customer_ID", "customers", "Customer_ID"),
    ("orders", "Restaurant_ID", "restaurants", "Restaurant_ID"),
    ("orders", "Promotion_ID", "promotions", "Promotion_ID"),
    ("pricing_history", "Item_ID", "menu_items", "Item_ID"),
    ("promotions", "Applicable_Category_ID", "menu_categories", "Category_ID"),
    ("promotions", "Applicable_Restaurant_ID", "restaurants", "Restaurant_ID"),
    ("ratings", "Order_ID", "orders", "Order_ID"),
    ("ratings", "Customer_ID", "customers", "Customer_ID"),
    ("ratings", "Item_ID", "menu_items", "Item_ID"),
    ("ratings", "Restaurant_ID", "restaurants", "Restaurant_ID"),
    ("wastage", "Restaurant_ID", "restaurants", "Restaurant_ID"),
    ("wastage", "Item_ID", "menu_items", "Item_ID"),
)

# SRS integration joins; avoid a separate channel dimension because main dataset
# records Channel directly on Orders.
RELATIONSHIPS: tuple[tuple[str, str, str, str], ...] = (
    ("orders", "Customer_ID", "customers", "Customer_ID"),
    ("orders", "Order_ID", "order_items", "Order_ID"),
    ("order_items", "Item_ID", "menu_items", "Item_ID"),
    ("menu_items", "Category_ID", "menu_categories", "Category_ID"),
    ("orders", "Restaurant_ID", "restaurants", "Restaurant_ID"),
    ("orders", "Promotion_ID", "promotions", "Promotion_ID"),
    ("menu_items", "Item_ID", "pricing_history", "Item_ID"),
    ("menu_items", "Item_ID", "ratings", "Item_ID"),
    ("menu_items", "Item_ID", "inventory", "Item_ID"),
    ("menu_items", "Item_ID", "wastage", "Item_ID"),
)
