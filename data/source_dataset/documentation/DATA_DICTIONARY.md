# Data dictionary

All monetary amounts are PKR. Dates are ISO dates; timestamps are local-naive synthetic timestamps. Boolean columns are true/false. Empty optional cells are missing values.

| Table | Primary key | Main columns and meaning | Foreign keys |
|---|---|---|---|
| Customers | Customer_ID | fictional name/contact, age, gender, location, signup, segment, preference, loyalty, income, lifecycle summaries | — |
| Orders | Order_ID | customer, location, optional promotion, channel, date/time, status, payment, delivery, subtotal/discount/tax/fee/total, cancellation and anomaly fields | Customer_ID, Location_ID, Promotion_ID, Channel_ID |
| Order_Items | Order_Item_ID | order/menu item, base and adjusted quantity, effective price-history key, campaign application, line discount/total, cost/profit, request and status | Order_ID, Item_ID, Price_History_ID |
| Menu_Items | Item_ID | name/category/cuisine, selling price/cost, preparation time, price-sensitivity cohort, observed sales/margin and high-wastage profiles | Category_ID |
| Menu_Categories | Category_ID | category name and description | — |
| Restaurants | Location_ID | fictional branch name, city/province/area, coordinates, opening, type, capacity, radius, rating, manager, operating indices | — |
| Pricing_History | Price_History_ID | item/location price validity intervals, previous/new price, change percentage/reason/approval | Item_ID, Location_ID |
| Promotions | Promotion_ID | campaign type, discount, thresholds, date interval, location/category scope, usage limit/status | — |
| Ratings | Rating_ID | customer/order/item/location, stars, review, timestamp, sentiment, verification/response, quality flags | Customer_ID, Order_ID, Item_ID, Location_ID |
| Inventory | Inventory_ID | location/item/date, opening, purchases, sales, adjustments, closing, reorder/waste/stockout/supplier | Location_ID, Item_ID |
| Wastage | Wastage_ID | location/item/date, wasted quantity, distinct preparation quantity and wastage percentage, cost, reason/shift/recorder/preventability | Location_ID, Item_ID |
| Ordering_Channels | Channel_ID | channel name and description | — |

IDs are positive integers. Ratings are 1–5; quantities, prices, costs and durations are in practical synthetic ranges except intentionally flagged anomalies. Inventory closing stock can be negative where stockout anomaly is flagged. `Pricing_History` has multiple non-overlapping intervals per item/location. See source headers for exact types and column names.








# Column-level reference

The tables below enumerate every source column. Sampled types are inferred from the generated CSVs; empty optional cells are nullable. PK/FK fields are positive integer identifiers.

## customers

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Customer_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Customer_Name` | string | Customer Name; see table description above; controlled nulls/noise may occur where documented. |
| `Age` | integer | Age; see table description above; controlled nulls/noise may occur where documented. |
| `Gender` | string | Gender; see table description above; controlled nulls/noise may occur where documented. |
| `City` | string | City; see table description above; controlled nulls/noise may occur where documented. |
| `Area` | string | Area; see table description above; controlled nulls/noise may occur where documented. |
| `Signup_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Customer_Segment` | string | Customer Segment; see table description above; controlled nulls/noise may occur where documented. |
| `Preferred_Channel` | string | Preferred Channel; see table description above; controlled nulls/noise may occur where documented. |
| `Preferred_Cuisine` | string | Preferred Cuisine; see table description above; controlled nulls/noise may occur where documented. |
| `Loyalty_Tier` | string | Loyalty Tier; see table description above; controlled nulls/noise may occur where documented. |
| `Income_Band` | string | Income Band; see table description above; controlled nulls/noise may occur where documented. |
| `Acquisition_Source` | string | Acquisition Source; see table description above; controlled nulls/noise may occur where documented. |
| `Is_Active` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Last_Order_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Total_Orders` | integer | Total Orders; see table description above; controlled nulls/noise may occur where documented. |
| `Lifetime_Value` | decimal | Synthetic PKR amount; anomalies may be flagged. |
| `Average_Order_Value` | decimal | Synthetic PKR amount; anomalies may be flagged. |
| `Churn_Status` | boolean | Churn Status; see table description above; controlled nulls/noise may occur where documented. |
| `Synthetic_Contact` | string | Synthetic Contact; see table description above; controlled nulls/noise may occur where documented. |
| `Is_Duplicate` | boolean | Boolean quality/operational indicator where values are true/false. |

## inventory

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Inventory_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Location_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Item_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Inventory_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Opening_Stock` | integer | Opening Stock; see table description above; controlled nulls/noise may occur where documented. |
| `Purchases` | integer | Purchases; see table description above; controlled nulls/noise may occur where documented. |
| `Units_Sold` | integer | Units Sold; see table description above; controlled nulls/noise may occur where documented. |
| `Adjustments` | integer | Adjustments; see table description above; controlled nulls/noise may occur where documented. |
| `Closing_Stock` | integer | Closing Stock; see table description above; controlled nulls/noise may occur where documented. |
| `Reorder_Level` | integer | Reorder Level; see table description above; controlled nulls/noise may occur where documented. |
| `Wastage_Quantity` | decimal | Wastage Quantity; see table description above; controlled nulls/noise may occur where documented. |
| `Stockout_Flag` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Supplier_ID` | string | String business/staff identifier. |
| `Is_Anomaly` | boolean | Boolean quality/operational indicator where values are true/false. |

## menu_categories

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Category_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Category_Name` | string | Category Name; see table description above; controlled nulls/noise may occur where documented. |
| `Description` | string | Description; see table description above; controlled nulls/noise may occur where documented. |

## menu_items

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Item_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Item_Name` | string | Item Name; see table description above; controlled nulls/noise may occur where documented. |
| `Category_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Cuisine_Type` | string | Cuisine Type; see table description above; controlled nulls/noise may occur where documented. |
| `Description` | string | Description; see table description above; controlled nulls/noise may occur where documented. |
| `Size` | string | Size; see table description above; controlled nulls/noise may occur where documented. |
| `Selling_Price` | integer | Selling Price; see table description above; controlled nulls/noise may occur where documented. |
| `Ingredient_Cost` | integer | Synthetic PKR amount; anomalies may be flagged. |
| `Estimated_Preparation_Time` | integer | Numeric preparation duration in minutes. |
| `Calories` | integer | Calories; see table description above; controlled nulls/noise may occur where documented. |
| `Is_Vegetarian` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Is_Spicy` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Is_Active` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Launch_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Discontinued_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Base_Cost` | integer | Synthetic PKR amount; anomalies may be flagged. |
| `Popularity_Index` | decimal | Popularity Index; see table description above; controlled nulls/noise may occur where documented. |
| `Perishability` | decimal | Perishability; see table description above; controlled nulls/noise may occur where documented. |
| `Price_Sensitive` | boolean | Price Sensitive; see table description above; controlled nulls/noise may occur where documented. |
| `Price_Sensitivity_Elasticity` | decimal | Price Sensitivity Elasticity; see table description above; controlled nulls/noise may occur where documented. |
| `Observed_Units_Sold` | integer | Observed Units Sold; see table description above; controlled nulls/noise may occur where documented. |
| `Contribution_Margin_PKR` | integer | Contribution Margin PKR; see table description above; controlled nulls/noise may occur where documented. |
| `Is_Popular_Low_Margin` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Is_Profitable_Low_Selling` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Is_High_Wastage_Item` | boolean | Boolean quality/operational indicator where values are true/false. |

## order_items

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Order_Item_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Order_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Item_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Quantity` | integer | Quantity; see table description above; controlled nulls/noise may occur where documented. |
| `Unit_Price` | integer | Synthetic PKR amount; anomalies may be flagged. |
| `Discount_Amount` | integer | Synthetic PKR amount; anomalies may be flagged. |
| `Line_Total` | integer | Synthetic PKR amount; anomalies may be flagged. |
| `Item_Cost` | integer | Item Cost; see table description above; controlled nulls/noise may occur where documented. |
| `Gross_Profit` | integer | Gross Profit; see table description above; controlled nulls/noise may occur where documented. |
| `Special_Request` | string | Special Request; see table description above; controlled nulls/noise may occur where documented. |
| `Preparation_Time` | integer | Numeric preparation duration in minutes. |
| `Item_Status` | string | Item Status; see table description above; controlled nulls/noise may occur where documented. |
| `Is_Anomaly` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Is_Duplicate` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Base_Quantity` | integer / nullable integer | Base Quantity; see table description above; controlled nulls/noise may occur where documented. |
| `Price_History_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Price_Sensitive` | boolean | Price Sensitive; see table description above; controlled nulls/noise may occur where documented. |
| `Price_Sensitivity_Applied` | boolean | Price Sensitivity Applied; see table description above; controlled nulls/noise may occur where documented. |
| `Promotion_Type_Applied` | string | Promotion Type Applied; see table description above; controlled nulls/noise may occur where documented. |
| `Promotion_Qualified` | boolean | Promotion Qualified; see table description above; controlled nulls/noise may occur where documented. |

## ordering_channels

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Channel_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Channel_Name` | string | Channel Name; see table description above; controlled nulls/noise may occur where documented. |
| `Description` | string | Description; see table description above; controlled nulls/noise may occur where documented. |

## orders

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Order_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Customer_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Location_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Promotion_ID` | string | Integer identifier / foreign key; nullable only where documented. |
| `Channel_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Order_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Order_Time` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Order_DateTime` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Order_Status` | string | Order Status; see table description above; controlled nulls/noise may occur where documented. |
| `Payment_Method` | string | Payment Method; see table description above; controlled nulls/noise may occur where documented. |
| `Delivery_Type` | string | Delivery Type; see table description above; controlled nulls/noise may occur where documented. |
| `Subtotal` | integer | Synthetic PKR amount; anomalies may be flagged. |
| `Discount_Amount` | integer | Synthetic PKR amount; anomalies may be flagged. |
| `Tax_Amount` | decimal | Synthetic PKR amount; anomalies may be flagged. |
| `Delivery_Fee` | integer | Synthetic PKR amount; anomalies may be flagged. |
| `Total_Amount` | decimal | Synthetic PKR amount; anomalies may be flagged. |
| `Order_Source` | string | Order Source; see table description above; controlled nulls/noise may occur where documented. |
| `Cancellation_Reason` | string | Cancellation Reason; see table description above; controlled nulls/noise may occur where documented. |
| `Is_Anomaly` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Is_Duplicate` | boolean | Boolean quality/operational indicator where values are true/false. |

## pricing_history

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Price_History_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Item_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Location_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Effective_From` | string | Effective From; see table description above; controlled nulls/noise may occur where documented. |
| `Effective_To` | string | Effective To; see table description above; controlled nulls/noise may occur where documented. |
| `Previous_Price` | integer | Previous Price; see table description above; controlled nulls/noise may occur where documented. |
| `New_Price` | integer | New Price; see table description above; controlled nulls/noise may occur where documented. |
| `Price_Change_Percentage` | decimal | Price Change Percentage; see table description above; controlled nulls/noise may occur where documented. |
| `Reason` | string | Reason; see table description above; controlled nulls/noise may occur where documented. |
| `Approved_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |

## promotions

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Promotion_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Promotion_Name` | string | Promotion Name; see table description above; controlled nulls/noise may occur where documented. |
| `Promotion_Type` | string | Promotion Type; see table description above; controlled nulls/noise may occur where documented. |
| `Discount_Percentage` | integer | Discount Percentage; see table description above; controlled nulls/noise may occur where documented. |
| `Fixed_Discount` | integer | Fixed Discount; see table description above; controlled nulls/noise may occur where documented. |
| `Minimum_Order_Value` | integer | Minimum Order Value; see table description above; controlled nulls/noise may occur where documented. |
| `Start_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `End_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Applicable_Locations` | string | Applicable Locations; see table description above; controlled nulls/noise may occur where documented. |
| `Applicable_Categories` | string | Applicable Categories; see table description above; controlled nulls/noise may occur where documented. |
| `Usage_Limit` | integer | Usage Limit; see table description above; controlled nulls/noise may occur where documented. |
| `Promotion_Status` | string | Promotion Status; see table description above; controlled nulls/noise may occur where documented. |
| `Is_Misleading` | boolean | Boolean quality/operational indicator where values are true/false. |

## ratings

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Rating_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Customer_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Order_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Item_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Location_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Rating` | integer | Integer from 1 to 5. |
| `Review_Text` | string | Review Text; see table description above; controlled nulls/noise may occur where documented. |
| `Review_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Sentiment` | string | Sentiment; see table description above; controlled nulls/noise may occur where documented. |
| `Is_Verified` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Response_Time_Hours` | decimal | Numeric response duration in hours. |
| `Is_Anomaly` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Is_Duplicate` | boolean | Boolean quality/operational indicator where values are true/false. |

## restaurants

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Location_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Restaurant_Name` | string | Restaurant Name; see table description above; controlled nulls/noise may occur where documented. |
| `City` | string | City; see table description above; controlled nulls/noise may occur where documented. |
| `Province` | string | Province; see table description above; controlled nulls/noise may occur where documented. |
| `Area` | string | Area; see table description above; controlled nulls/noise may occur where documented. |
| `Latitude` | decimal | Latitude; see table description above; controlled nulls/noise may occur where documented. |
| `Longitude` | decimal | Longitude; see table description above; controlled nulls/noise may occur where documented. |
| `Opening_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Restaurant_Type` | string | Restaurant Type; see table description above; controlled nulls/noise may occur where documented. |
| `Seating_Capacity` | integer | Seating Capacity; see table description above; controlled nulls/noise may occur where documented. |
| `Delivery_Radius_KM` | decimal | Delivery Radius KM; see table description above; controlled nulls/noise may occur where documented. |
| `Average_Rating` | decimal | Average Rating; see table description above; controlled nulls/noise may occur where documented. |
| `Manager_ID` | string | String business/staff identifier. |
| `Status` | string | Status; see table description above; controlled nulls/noise may occur where documented. |
| `Volume_Index` | decimal | Volume Index; see table description above; controlled nulls/noise may occur where documented. |
| `Service_Quality` | decimal | Service Quality; see table description above; controlled nulls/noise may occur where documented. |

## wastage

| Column | Sampled type | Meaning / expected values |
|---|---|---|
| `Wastage_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Location_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Item_ID` | integer / nullable integer | Integer identifier / foreign key; nullable only where documented. |
| `Wastage_Date` | date/timestamp or time string | Synthetic event/effective date or time in the documented 24-month window. |
| `Quantity_Wasted` | decimal | Quantity Wasted; see table description above; controlled nulls/noise may occur where documented. |
| `Unit_Cost` | integer | Unit Cost; see table description above; controlled nulls/noise may occur where documented. |
| `Total_Wastage_Cost` | decimal | Synthetic PKR amount; anomalies may be flagged. |
| `Wastage_Reason` | string | Wastage Reason; see table description above; controlled nulls/noise may occur where documented. |
| `Shift` | string | Shift; see table description above; controlled nulls/noise may occur where documented. |
| `Recorded_By` | string | Recorded By; see table description above; controlled nulls/noise may occur where documented. |
| `Preventable_Flag` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Is_Anomaly` | boolean | Boolean quality/operational indicator where values are true/false. |
| `Preparation_Quantity` | integer | Preparation Quantity; see table description above; controlled nulls/noise may occur where documented. |
| `Wastage_Percentage` | decimal | Wastage Percentage; see table description above; controlled nulls/noise may occur where documented. |
