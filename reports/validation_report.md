# Validation report (measured from files on disk)

## Final flags

```text
RESTAURANT_CHAIN_COUNT=PASS   # 1
NON_KARACHI_BRANCHES=PASS   # 0
KARACHI_ONLY=PASS   # Karachi only
BRANCH_COUNT=PASS   # 22
MENU_IDENTICAL_ALL_BRANCHES=PASS   # identical
PRICE_IDENTICAL_ALL_BRANCHES=PASS   # identical
DEMAND_VARIATION=PASS   # CV=0.31, max/min=3.00
CUSTOMER_MIX_VARIATION=PASS   # age range 7.0y, female range 0.21
WASTAGE_VARIATION=PASS   # 11.0%-17.7%
RATING_VARIATION=PASS   # avg 3.66-3.99
PROMOTION_RESPONSE_VARIATION=PASS   # branch promo share 22.9%-61.8%
ORDER_CHANNEL_VARIATION=PASS   # V=0.154, delivery+phone 19%-38%
INTENTIONAL_RAW_ISSUES_PRESENT=PASS   # 13/13 categories, 29,044 records
ISSUE_LEDGER_MATCHES_RAW_DATA=PASS   # 36,037/36,037 entries verified, counts match=True
RAW_DATA_IMMUTABLE=PASS   # 12 files, checksums match, read-only=True
```

## Supporting checks

- volumes_relationships_structure: PASS
- analytical_cases_emerge_from_data: PASS
- parquet_readback: PASS
- chronological_split: PASS
- project_structure: PASS

## Measured details

```json
{
 "volume": {
  "Customers": {
   "actual": 55000,
   "minimum": 50000,
   "pass": true
  },
  "Orders_unique": {
   "actual": 260000,
   "minimum": 100000,
   "pass": true
  },
  "Order_Items_raw": {
   "actual": 1135614,
   "minimum": 1000000,
   "pass": true
  },
  "Order_Items_cleaned": {
   "actual": 1105311,
   "minimum": 1000000,
   "pass": true
  },
  "Menu_Items": {
   "actual": 185,
   "minimum": 150,
   "pass": true
  },
  "Menu_Categories": {
   "actual": 12,
   "minimum": 10,
   "pass": true
  },
  "Restaurants": {
   "actual": 22,
   "minimum": 20,
   "pass": true
  },
  "Ratings": {
   "actual": 105000,
   "minimum": 100000,
   "pass": true
  },
  "Wastage": {
   "actual": 55096,
   "minimum": 50000,
   "pass": true
  },
  "months_of_history": {
   "actual": 12.0,
   "minimum": 12,
   "pass": true
  },
  "pricing_rows_per_item_min": {
   "actual": 1,
   "minimum": 2,
   "pass": true
  },
  "promotions": {
   "actual": 45,
   "minimum": 2,
   "pass": true
  }
 },
 "structure": {
  "all_11_required_tables_present": true
 },
 "relationships_cleaned": {
  "Orders-Customers": {
   "integrity_pct": 100.0,
   "pass": true
  },
  "Orders-Order_Items": {
   "integrity_pct": 100.0,
   "pass": true
  },
  "Order_Items-Menu_Items": {
   "integrity_pct": 100.0,
   "pass": true
  },
  "Menu_Items-Menu_Categories": {
   "integrity_pct": 100.0,
   "pass": true
  },
  "Orders-Restaurants": {
   "integrity_pct": 100.0,
   "pass": true
  },
  "Orders-Promotions": {
   "integrity_pct": 100.0,
   "pass": true
  },
  "Menu_Items-Pricing_History": {
   "integrity_pct": 100.0,
   "pass": true
  },
  "Menu_Items-Ratings": {
   "integrity_pct": 100.0,
   "pass": true
  },
  "Menu_Items-Inventory": {
   "integrity_pct": 100.0,
   "pass": true
  },
  "Menu_Items-Wastage": {
   "integrity_pct": 100.0,
   "pass": true
  }
 },
 "karachi": {
  "branches": 22,
  "stray_other_city_mentions": 0,
  "customer_cities": [
   "Karachi"
  ]
 },
 "menu": {
  "items": 185,
  "branches_with_full_menu": 22,
  "availability_rows": 4070,
  "items_without_sales_at_some_branch_pct": null,
  "min_menu_items_sold_at_any_branch": 184
 },
 "price": {
  "item_date_groups": 62884,
  "groups_seen_at_multiple_branches": 58454,
  "groups_with_more_than_one_price": 0,
  "order_lines_not_matching_chain_price_list": 0,
  "pricing_history_has_branch_column": false
 },
 "demand": {
  "orders_per_branch_min": 5702,
  "orders_per_branch_max": 17098,
  "cv": 0.308,
  "max_over_min": 3.0,
  "lunch_share_range": [
   0.176,
   0.318
  ],
  "dinner_share_range": [
   0.308,
   0.459
  ],
  "aov_range": [
   4209.2,
   5710.0
  ]
 },
 "customer_mix": {
  "mean_age_range": [
   27.7,
   34.6
  ],
  "female_share_range": [
   0.298,
   0.51
  ],
  "repeat_customer_share_range": [
   0.235,
   0.354
  ],
  "cramers_v_branch_x_age_group": 0.116,
  "cramers_v_branch_x_gender": 0.109
 },
 "wastage": {
  "wastage_pct_min": 10.97,
  "wastage_pct_max": 17.72,
  "max_over_min": 1.62
 },
 "ratings": {
  "avg_rating_range": [
   3.66,
   3.994
  ],
  "share_1_2_star_range": [
   0.06,
   0.135
  ],
  "overall_share_3_to_5_pct": 91.0,
  "overall_share_1_2_pct": 9.0
 },
 "promotion_response": {
  "promo_order_share_by_branch_range": [
   0.2286,
   0.6183
  ],
  "max_over_min": 2.71,
  "promo_order_share_by_age_group": {
   "<25": 0.4465,
   "25-34": 0.4265,
   "35-49": 0.4192,
   "50+": 0.3883
  }
 },
 "channels": {
  "channels": [
   "Dine-in",
   "Phone Order",
   "Takeaway",
   "Third-Party Delivery",
   "Unknown",
   "Website/App"
  ],
  "delivery_plus_phone_share_range": [
   0.188,
   0.383
  ],
  "cramers_v_branch_x_channel": 0.154
 },
 "issue_counts_vs_ledger": {
  "Orders.duplicate_record": {
   "detected_in_raw": 1300,
   "ledger": 1300,
   "match": true
  },
  "Orders.missing_customer_reference": {
   "detected_in_raw": 1300,
   "ledger": 1300,
   "match": true
  },
  "Orders.invalid_customer_fk": {
   "detected_in_raw": 520,
   "ledger": 520,
   "match": true
  },
  "Orders.missing_value": {
   "detected_in_raw": 1040,
   "ledger": 1040,
   "match": true
  },
  "Orders.invalid_branch_reference": {
   "detected_in_raw": 780,
   "ledger": 780,
   "match": true
  },
  "Orders.invalid_date": {
   "detected_in_raw": 520,
   "ledger": 520,
   "match": true
  },
  "Orders.invalid_promotion_reference": {
   "detected_in_raw": 520,
   "ledger": 520,
   "match": true
  },
  "Orders.inconsistent_promotion_reference": {
   "detected_in_raw": 520,
   "ledger": 520,
   "match": true
  },
  "Order_Items.duplicate_record": {
   "detected_in_raw": 3389,
   "ledger": 3389,
   "match": true
  },
  "Order_Items.near_duplicate_record": {
   "detected_in_raw": 2259,
   "ledger": 2259,
   "match": true
  },
  "Order_Items.invalid_price": {
   "detected_in_raw": 3389,
   "ledger": 3389,
   "match": true
  },
  "Order_Items.negative_quantity": {
   "detected_in_raw": 2259,
   "ledger": 2259,
   "match": true
  },
  "Order_Items.missing_menu_reference": {
   "detected_in_raw": 3389,
   "ledger": 3389,
   "match": true
  },
  "Order_Items.invalid_menu_fk": {
   "detected_in_raw": 1129,
   "ledger": 1129,
   "match": true
  },
  "Order_Items.orphan_order_reference": {
   "detected_in_raw": 1129,
   "ledger": 1129,
   "match": true
  },
  "Order_Items.incorrect_discount": {
   "detected_in_raw": 3389,
   "ledger": 3389,
   "match": true
  },
  "Order_Items.incorrect_line_total": {
   "detected_in_raw": 3389,
   "ledger": 3389,
   "match": true
  },
  "Ratings.invalid_rating": {
   "detected_in_raw": 525,
   "ledger": 525,
   "match": true
  },
  "Ratings.missing_customer_reference": {
   "detected_in_raw": 420,
   "ledger": 420,
   "match": true
  },
  "Ratings.invalid_menu_fk": {
   "detected_in_raw": 105,
   "ledger": 105,
   "match": true
  },
  "Ratings.invalid_branch_reference": {
   "detected_in_raw": 105,
   "ledger": 105,
   "match": true
  },
  "Wastage.impossible_wastage": {
   "detected_in_raw": 275,
   "ledger": 275,
   "match": true
  },
  "Wastage.inconsistent_units": {
   "detected_in_raw": 550,
   "ledger": 550,
   "match": true
  },
  "Wastage.missing_menu_reference": {
   "detected_in_raw": 110,
   "ledger": 110,
   "match": true
  },
  "Inventory.inconsistent_units": {
   "detected_in_raw": 488,
   "ledger": 488,
   "match": true
  },
  "Inventory.missing_value": {
   "detected_in_raw": 488,
   "ledger": 488,
   "match": true
  },
  "Customers.missing_value": {
   "detected_in_raw": 2750,
   "ledger": 2750,
   "match": true
  }
 },
 "intentional_issue_categories_detected": {
  "missing values": 4278,
  "invalid foreign keys": 2778,
  "invalid dates": 520,
  "invalid ratings": 525,
  "negative quantities": 2259,
  "negative/invalid prices": 3389,
  "incorrect line totals": 3389,
  "duplicate records": 6948,
  "inconsistent units": 1038,
  "impossible wastage": 275,
  "missing customer references": 1720,
  "invalid branch references": 885,
  "inconsistent promotion references": 1040
 },
 "controlled_rates_pct": {
  "duplicate_orders_pct": 0.498,
  "invalid_price_pct": 0.298,
  "invalid_date_pct": 0.199,
  "invalid_rating_pct": 0.5
 },
 "ledger": {
  "entries": 36037,
  "entries_verified_in_raw": 36037,
  "entries_not_found": 0,
  "issue_types": 19,
  "all_type_counts_match_independent_detection": true,
  "unverified_examples": []
 },
 "raw_immutability": {
  "files": 12,
  "checksums_match": true,
  "all_read_only": true
 },
 "business_cases": {
  "popular_low_margin": {
   "avg_sales_pctile": 83.1,
   "avg_margin_pctile": 24.6,
   "ok": true
  },
  "profitable_low_selling": {
   "avg_sales_pctile": 9.5,
   "avg_margin_pctile": 82.1,
   "ok": true
  },
  "popular_high_wastage_items": {
   "count": 5,
   "ok": true
  },
  "sales_anomalies": {
   "spikes_detectable": "10/10",
   "drops_detectable": "10/10",
   "high_value_orders_above_p99": 187,
   "ok": true
  },
  "price_sensitivity": {
   "flagged_windows": 29,
   "control_windows": 35,
   "flagged_mean_demand_ratio": 0.686,
   "control_mean_demand_ratio": 1.088,
   "ok": true
  },
  "promotions": {
   "orders_per_day_by_quality": {
    "Weak": 42.4,
    "Misleading": 197.5,
    "Effective": 203.5
   },
   "discounted_lines_vs_same_item_non_promo_lines": {
    "Effective": {
     "lines": 47120,
     "units_ratio": 1.233,
     "revenue_ratio": 1.014,
     "profit_ratio": 0.846
    },
    "Misleading": {
     "lines": 20816,
     "units_ratio": 1.493,
     "revenue_ratio": 1.175,
     "profit_ratio": 0.839
    },
    "Weak": {
     "lines": 8081,
     "units_ratio": 0.999,
     "revenue_ratio": 0.823,
     "profit_ratio": 0.703
    }
   },
   "promotion_window_vs_own_28day_baseline_chain_wide": {
    "Misleading": {
     "promotions": 6,
     "median_revenue_ratio_vs_baseline": 1.377,
     "median_profit_ratio_vs_baseline": 1.034,
     "share_with_revenue_up_and_profit_down": 0.33
    },
    "Effective": {
     "promotions": 10,
     "median_revenue_ratio_vs_baseline": 1.553,
     "median_profit_ratio_vs_baseline": 1.518,
     "share_with_revenue_up_and_profit_down": 0.0
    }
   },
   "chain_wide_promo_day_order_uplift": 1.265,
   "misleading_scoped_to_low_margin_categories": true,
   "chain_wide": 26,
   "branch_specific": 19,
   "criterion": "misleading: revenue up, profit growth lags revenue by >=0.20 and >=25% of such promotions lose profit; effective: profit keeps pace and <=10% lose profit",
   "ok": true
  },
  "calendar_patterns": {
   "weekend_share_pct": 52.4,
   "winter_share_pct": 36.4,
   "peak_hours_share_pct": 53.3,
   "ramazan_iftar_share_pct": 47.3,
   "other_months_iftar_share_pct": 25.7,
   "ok": true
  },
  "customers": {
   "top5pct_revenue_share": 0.301,
   "inactive_churn_risk_share": 0.377,
   "new_customers_share": 0.197,
   "one_order_customers_share": 0.243,
   "repeat_customers_share": 0.757,
   "ok": true
  },
  "rating_anomaly_bursts": {
   "detected": "5/5",
   "ok": true
  }
 },
 "parquet_readback_ok": true,
 "chronological_split": {
  "train": [
   "2025-01-01",
   "2025-08-31"
  ],
  "validation": [
   "2025-09-01",
   "2025-10-31"
  ],
  "test": [
   "2025-11-01",
   "2025-12-31"
  ]
 },
 "project_structure": {
  "required_dirs_and_files_present": true
 },
 "supporting_checks": {
  "volumes_relationships_structure": true,
  "analytical_cases_emerge_from_data": true,
  "parquet_readback": true,
  "chronological_split": true,
  "project_structure": true
 },
 "ALL_PASS": true
}
```
