# Dataset Statistics

| Table (raw) | Rows | Columns |
|---|---:|---:|
| Customers | 55,000 | 11 |
| Restaurants | 22 | 12 |
| Menu_Categories | 12 | 2 |
| Menu_Items | 185 | 7 |
| Menu_Item_Availability | 4,070 | 5 |
| Pricing_History | 555 | 6 |
| Promotions | 45 | 7 |
| Orders | 261,300 | 16 |
| Order_Items | 1,135,614 | 8 |
| Ratings | 105,000 | 9 |
| Inventory | 48,840 | 9 |
| Wastage | 55,096 | 9 |

| Table (processed) | Rows |
|---|---:|
| Customers | 55,000 |
| Restaurants | 22 |
| Menu_Categories | 12 |
| Menu_Items | 185 |
| Menu_Item_Availability | 4,070 |
| Pricing_History | 555 |
| Promotions | 45 |
| Orders | 256,880 |
| Order_Items | 1,105,311 |
| Ratings | 102,604 |
| Inventory | 48,840 |
| Wastage | 54,986 |

- Period: 2025-01-01 to 2025-12-31 (12 months)
- Branches: 22, all in Karachi, Sindh
- Menu: 185 items in 12 categories, identical at every branch
- Ratings reconciliation: 105,000 raw = 102,604 processed + 2,396 quarantined;
  the quarantine includes ratings whose parent order was rejected during cleaning.
- Raw-versus-processed validation and data-quality evidence is available in
  `reports/validation_report.json` and `reports/data_foundation/data_quality_report.json`.
