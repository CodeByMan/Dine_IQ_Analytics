#!/usr/bin/env python3
"""Execute the ten SRS cross-table join contracts using the canonical Python inputs."""
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]; D=ROOT/'data'
def read(n,cols=None): return pd.read_csv(D/f'{n}.csv',usecols=cols)
cases=[]
def check(label,left,right,key,nullable=False):
    a,b=read(left,[key]),read(right,[key])
    if nullable: a=a.loc[a[key].notna()].copy()
    a[key]=pd.to_numeric(a[key],errors='coerce').astype('Int64')
    b[key]=pd.to_numeric(b[key],errors='coerce').astype('Int64')
    n=len(a.merge(b,on=key,how='inner',validate='many_to_one' if b[key].is_unique else 'many_to_many'))
    orphan=a.loc[~a[key].isin(b[key])]
    ok=n>0 and len(orphan)==0
    cases.append((label,n,len(orphan),ok))
check('Orders-Customers','orders','customers','Customer_ID')
check('Orders-Order_Items','orders','order_items','Order_ID')
check('Order_Items-Menu_Items','order_items','menu_items','Item_ID')
check('Menu_Items-Categories','menu_items','menu_categories','Category_ID')
check('Orders-Restaurants','orders','restaurants','Location_ID')
check('Orders-Promotions','orders','promotions','Promotion_ID',True)
check('Menu_Items-Pricing_History','menu_items','pricing_history','Item_ID')
check('Menu_Items-Ratings','menu_items','ratings','Item_ID')
check('Menu_Items-Inventory','menu_items','inventory','Item_ID')
check('Menu_Items-Wastage','menu_items','wastage','Item_ID')
for label,n,orph,ok in cases: print(f'[{label}] joined_rows={n:,}; orphan_left={orph}; '+('PASS' if ok else 'FAIL'))
print(f'PYTHON JOIN CONTRACTS: {sum(x[3] for x in cases)}/{len(cases)} PASS')
raise SystemExit(0 if all(x[3] for x in cases) else 2)
