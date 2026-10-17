# Data Dictionary - Zauq Kitchen, Karachi (PKR, synthetic data)

Current schema, row-count, null, range, and relationship evidence is recorded in
`reports/validation_report.json` and the `reports/data_foundation/` validation reports.
The raw schema is preserved; canonical Spark types/analytical aliases are in
`src/dineiq/dataset/schema.py` and `normalize.py`. Source documentation describes
identity fields as synthetic; this is provenance, not authenticated real-person data.
Declared typical ranges below are business descriptions, not proof every raw
value conforms. Numeric/date anomalies and cleaning effects are recorded in
`reports/data_foundation/data_quality_report.json` and
`reports/data_foundation/cleaning_decisions.json`.

## Customers

**Primary Key:** `Customer_ID`

**Foreign Keys:** `Home_Restaurant_ID` -> `Restaurants.Restaurant_ID`

| Column | Description |
|---|---|
| Customer_ID | PK |
| First_Name | synthetic Pakistani first name |
| Last_Name | synthetic surname |
| Gender | Male/Female |
| Age | years (16-75) |
| City | always Karachi |
| Locality | Karachi locality |
| Home_Restaurant_ID | FK (usual branch) |
| Signup_Date | date |
| Email | synthetic @example.com; some blank (DQ) |
| Phone | synthetic +92-399 (unallocated prefix); some blank (DQ) |

## Restaurants

**Primary Key:** `Restaurant_ID`

| Column | Description |
|---|---|
| Restaurant_ID | PK (22 Karachi branches) |
| Chain_Name | single chain name |
| Restaurant_Name | chain - locality |
| Locality | Karachi locality |
| District | Karachi district |
| Address | synthetic commercial address |
| City | Karachi |
| Province | Sindh |
| Country | Pakistan |
| Location_Type | string |
| Opening_Date | date |
| Seating_Capacity | int |

## Menu_Categories

**Primary Key:** `Category_ID`

| Column | Description |
|---|---|
| Category_ID | PK |
| Category_Name | string (12 categories) |

## Menu_Items

**Primary Key:** `Item_ID`

**Foreign Keys:** `Category_ID` -> `Menu_Categories.Category_ID`

| Column | Description |
|---|---|
| Item_ID | PK (same menu at every branch) |
| Item_Name | string |
| Category_ID | FK |
| Base_Cost | PKR cost per serving |
| Base_Price | PKR initial chain-wide price |
| Introduced_Date | date |
| Is_Active | bool |

## Menu_Item_Availability

**Primary Key:** `Availability_ID`

**Foreign Keys:** `Restaurant_ID` -> `Restaurants.Restaurant_ID`; `Item_ID` -> `Menu_Items.Item_ID`

| Column | Description |
|---|---|
| Availability_ID | PK |
| Restaurant_ID | FK |
| Item_ID | FK |
| Is_Available | bool (all true: identical menu) |
| Available_From | date (supplementary) |

## Pricing_History

**Primary Key:** `Pricing_ID`

**Foreign Keys:** `Item_ID` -> `Menu_Items.Item_ID`

| Column | Description |
|---|---|
| Pricing_ID | PK |
| Item_ID | FK |
| Price | PKR, CHAIN-WIDE price valid from Effective_Date |
| Previous_Price | PKR, NULL for first price |
| Effective_Date | date |
| End_Date | date, NULL = current |

## Promotions

**Primary Key:** `Promotion_ID`

**Foreign Keys:** `Applicable_Category_ID` -> `Menu_Categories (nullable = all)`; `Applicable_Restaurant_ID` -> `Restaurants (nullable = chain-wide)`

| Column | Description |
|---|---|
| Promotion_ID | PK |
| Promotion_Name | string |
| Discount_Percent | int 5-50 |
| Start_Date | date |
| End_Date | date |
| Applicable_Category_ID | nullable FK |
| Applicable_Restaurant_ID | nullable FK |

## Orders

**Primary Key:** `Order_ID`

**Foreign Keys:** `Customer_ID` -> `Customers (DQ: blank / unknown)`; `Restaurant_ID` -> `Restaurants (DQ: invalid)`; `Promotion_ID` -> `Promotions (DQ: invalid / outside window)`

| Column | Description |
|---|---|
| Order_ID | PK (duplicated for ~0.5% - DQ) |
| Customer_ID | FK |
| Restaurant_ID | FK |
| Order_DateTime | datetime |
| Order_Date | date text (some impossible - DQ) |
| Channel | Dine-in/Takeaway/Website/App/Third-Party Delivery/Phone Order (some blank - DQ) |
| Payment_Method | Cash/Card/Online |
| Promotion_ID | nullable FK |
| Status | Completed/Cancelled/Refunded/Pending |
| Delivery_Charge | PKR |
| Item_Count | derived |
| Subtotal | PKR derived |
| Total_Amount | PKR derived (after discount) |
| Discount_Amount | PKR derived |
| Tax_Amount | synthetic sales tax: 13% cash, 8% card/online |
| Grand_Total | Total + Tax + Delivery |

## Order_Items

**Primary Key:** `Order_Item_ID`

**Foreign Keys:** `Order_ID` -> `Orders.Order_ID (DQ: orphan)`; `Item_ID` -> `Menu_Items.Item_ID (DQ: blank / unknown)`

| Column | Description |
|---|---|
| Order_Item_ID | PK (exact + near duplicates - DQ) |
| Order_ID | FK |
| Item_ID | FK |
| Quantity | int (some negative - DQ) |
| Unit_Price | PKR chain-wide price at order date (some invalid - DQ) |
| Discount_Percent | float (some out of range - DQ) |
| Line_Subtotal | derived |
| Line_Total | derived (some incorrect - DQ) |

## Ratings

**Primary Key:** `Rating_ID`

**Foreign Keys:** `Order_ID` -> `Orders`; `Customer_ID` -> `Customers (DQ: blank)`; `Item_ID` -> `Menu_Items (DQ)`; `Restaurant_ID` -> `Restaurants (DQ)`

| Column | Description |
|---|---|
| Rating_ID | PK |
| Order_ID | FK |
| Customer_ID | FK |
| Item_ID | FK |
| Restaurant_ID | FK |
| Rating_Value | int 1-5 expected (some invalid - DQ) |
| Rating_Date | date |
| Comment | synthetic review text, nullable |
| Reviewer_Name | synthetic 'First L.' |

## Inventory

**Primary Key:** `Inventory_ID`

**Foreign Keys:** `Restaurant_ID` -> `Restaurants`; `Item_ID` -> `Menu_Items`

| Column | Description |
|---|---|
| Inventory_ID | PK |
| Restaurant_ID | FK |
| Item_ID | FK |
| Month | date |
| Opening_Stock | float |
| Received_Stock | float |
| Consumed_Stock | float (some blank - DQ) |
| Closing_Stock | float |
| Unit | kg/liters/pcs (variants - DQ) |

## Wastage

**Primary Key:** `Wastage_ID`

**Foreign Keys:** `Restaurant_ID` -> `Restaurants`; `Item_ID` -> `Menu_Items (DQ: blank)`

| Column | Description |
|---|---|
| Wastage_ID | PK |
| Restaurant_ID | FK |
| Item_ID | FK |
| Date | date |
| Prepared_Quantity | preparation quantity (denominator of Wastage %) |
| Wastage_Quantity | may exceed Prepared_Quantity (DQ) |
| Unit | kg/liters/pcs (variants - DQ) |
| Reason | string |
| Wastage_Cost | PKR derived |

