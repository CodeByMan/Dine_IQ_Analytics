# PK / FK Definitions

| Table | Primary Key | Foreign Keys |
|---|---|---|
| Customers | Customer_ID | Home_Restaurant_ID -> Restaurants.Restaurant_ID |
| Restaurants | Restaurant_ID | - |
| Menu_Categories | Category_ID | - |
| Menu_Items | Item_ID | Category_ID -> Menu_Categories.Category_ID |
| Menu_Item_Availability | Availability_ID | Restaurant_ID -> Restaurants.Restaurant_ID; Item_ID -> Menu_Items.Item_ID |
| Pricing_History | Pricing_ID | Item_ID -> Menu_Items.Item_ID |
| Promotions | Promotion_ID | Applicable_Category_ID -> Menu_Categories (nullable = all); Applicable_Restaurant_ID -> Restaurants (nullable = chain-wide) |
| Orders | Order_ID | Customer_ID -> Customers (DQ: blank / unknown); Restaurant_ID -> Restaurants (DQ: invalid); Promotion_ID -> Promotions (DQ: invalid / outside window) |
| Order_Items | Order_Item_ID | Order_ID -> Orders.Order_ID (DQ: orphan); Item_ID -> Menu_Items.Item_ID (DQ: blank / unknown) |
| Ratings | Rating_ID | Order_ID -> Orders; Customer_ID -> Customers (DQ: blank); Item_ID -> Menu_Items (DQ); Restaurant_ID -> Restaurants (DQ) |
| Inventory | Inventory_ID | Restaurant_ID -> Restaurants; Item_ID -> Menu_Items |
| Wastage | Wastage_ID | Restaurant_ID -> Restaurants; Item_ID -> Menu_Items (DQ: blank) |
