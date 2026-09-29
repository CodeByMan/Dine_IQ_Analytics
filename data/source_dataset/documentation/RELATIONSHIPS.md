# Relationships and keys

- `Customers.Customer_ID` 1-to-many `Orders.Customer_ID`.
- `Orders.Order_ID` 1-to-many `Order_Items.Order_ID` and `Ratings.Order_ID`.
- `Order_Items.Item_ID` references `Menu_Items.Item_ID`; lines also carry `Price_History_ID` matching the effective price record.
- `Menu_Items.Category_ID` references `Menu_Categories.Category_ID`.
- `Orders.Location_ID` references `Restaurants.Location_ID`; `Location_ID` also scopes prices, ratings, inventory, and wastage.
- `Orders.Promotion_ID` optionally references `Promotions.Promotion_ID`; null means no applied campaign.
- `Menu_Items.Item_ID` references pricing history, ratings, inventory, and wastage by item; location and effective date narrow the applicable pricing row.
- `Orders.Channel_ID` references the supporting `Ordering_Channels` table.
- `Wastage.Preparation_Quantity` is the production denominator, distinct from `Quantity_Wasted`; `Wastage_Percentage` is calculated from those fields.

Primary keys are unique integer identifiers. Optional campaign identifiers are nullable integer references. The Python and Spark workflows consume the same canonical source snapshots; Parquet conversion is generated from those CSV sources. `scripts/spark_pipeline.py` checks each required cross-table join and orphan condition.
