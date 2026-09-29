#!/usr/bin/env python3
"""Validate the isolated hidden-evaluation readiness catalog and case payloads."""
from pathlib import Path
import json
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
REQUIRED={'missing_values','duplicate_orders','unknown_menu_item','new_restaurant_location','price_change','unusual_promotion','extreme_wastage','seasonal_change','unexpected_customer_behavior','outlier'}
df=pd.read_csv(ROOT/'fixtures'/'hidden_data'/'hidden_scenarios.csv')
found=set(df.Case_ID)
payloads={r.Case_ID:json.loads(r.Payload_JSON) for r in df.itertuples(index=False)}
locations=set(pd.read_csv(ROOT/'data'/'restaurants.csv',usecols=['Location_ID']).Location_ID.astype(int))
items=set(pd.read_csv(ROOT/'data'/'menu_items.csv',usecols=['Item_ID']).Item_ID.astype(int))
fixtures_ok=(payloads['missing_values']['Customer_ID'] is None and payloads['unknown_menu_item']['Item_ID'] not in items and payloads['new_restaurant_location']['Location_ID'] not in locations and payloads['price_change']['New_Price']>payloads['price_change']['Old_Price'] and payloads['unusual_promotion']['Discount_Percentage']>50 and payloads['extreme_wastage']['Quantity_Wasted']>payloads['extreme_wastage']['Preparation_Quantity'] and payloads['unexpected_customer_behavior']['Orders_Per_Day']>payloads['unexpected_customer_behavior']['Baseline_Orders_Per_Day'] and payloads['outlier']['Order_Total']>payloads['outlier']['Baseline_P99'])
trap=pd.read_csv(ROOT/'fixtures'/'hidden_data'/'promotion_trap_case.csv')
campaign=pd.read_csv(ROOT/'data'/'promotions.csv').set_index('Promotion_ID').loc[14]
promoted=trap.loc[trap.Promotion_ID.eq(14)].iloc[0]
baseline=trap.loc[trap.Scenario.str.startswith('Baseline'),'Contribution_Profit'].iloc[0]
trapok=(len(trap)==2 and bool(campaign.Is_Misleading) and float(campaign.Discount_Percentage)==20 and float(campaign.Minimum_Order_Value)==4999 and promoted.Basket_Gross_Value>=campaign.Minimum_Order_Value and abs(promoted.Discount_Amount-round(promoted.Basket_Gross_Value*promoted.Discount_Percentage/100,2))<.01 and promoted.Contribution_Profit<baseline)
valid=REQUIRED<=found and all(json.loads(s) for s in df.Payload_JSON) and df.Case_ID.is_unique and trapok and fixtures_ok
print(f'Hidden-data scenarios: {len(found)}/10; payload stress values: {"PASS" if fixtures_ok else "FAIL"}; promotion-trap term/math: {"PASS" if trapok else "FAIL"}; STATUS: '+('PASS' if valid else 'FAIL'))
if not valid: print('Missing: '+', '.join(sorted(REQUIRED-found)))
raise SystemExit(0 if valid else 2)
