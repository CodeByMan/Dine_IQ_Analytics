#!/usr/bin/env python3
"""Spark checks and aggregation over the same canonical table snapshots as Python."""
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
ROOT=Path(__file__).resolve().parents[1]
spark=SparkSession.builder.appName('RestaurantSRSJoinValidation').getOrCreate()
def load(name):
    p=ROOT/'parquet'/f'{name}.parquet'
    return spark.read.parquet(str(p)) if p.exists() else spark.read.option('header',True).option('inferSchema',True).csv(str(ROOT/'data'/f'{name}.csv'))
T={n:load(n) for n in ['orders','customers','order_items','menu_items','menu_categories','restaurants','promotions','pricing_history','ratings','inventory','wastage']}
joins=[
 ('Orders-Customers','orders','Customer_ID','customers','Customer_ID'),
 ('Orders-Order_Items','orders','Order_ID','order_items','Order_ID'),
 ('Order_Items-Menu_Items','order_items','Item_ID','menu_items','Item_ID'),
 ('Menu_Items-Categories','menu_items','Category_ID','menu_categories','Category_ID'),
 ('Orders-Restaurants','orders','Location_ID','restaurants','Location_ID'),
 ('Orders-Promotions','orders','Promotion_ID','promotions','Promotion_ID'),
 ('Menu_Items-Pricing_History','menu_items','Item_ID','pricing_history','Item_ID'),
 ('Menu_Items-Ratings','menu_items','Item_ID','ratings','Item_ID'),
 ('Menu_Items-Inventory','menu_items','Item_ID','inventory','Item_ID'),
 ('Menu_Items-Wastage','menu_items','Item_ID','wastage','Item_ID'),
]
failed=[]
for label,left,lk,right,rk in joins:
    a,b=T[left],T[right]
    if label=='Orders-Promotions': a=a.filter(F.col(lk).isNotNull())
    # Numeric casts normalize nullable CSV promotion identifiers without changing records.
    unmatched=a.select(F.col(lk).cast('long').alias('_key')).join(b.select(F.col(rk).cast('long').alias('_key')).distinct(), '_key','left_anti').count()
    joined=a.join(b,a[lk].cast('long')==b[rk].cast('long'),'inner').count()
    ok=unmatched==0 and joined>0
    print(f'[{label}] joined={joined:,}; orphan_left={unmatched}; '+('PASS' if ok else 'FAIL'))
    if not ok: failed.append(label)
orders=T['orders'].filter(F.col('Order_Status')=='Completed')
daily=(orders.join(T['order_items'],'Order_ID').groupBy(F.to_date('Order_Date').alias('Date'),'Location_ID')
       .agg(F.sum('Line_Total').alias('Item_Revenue'),F.count('*').alias('Line_Count')))
print(f'Completed daily/location aggregates: {daily.count():,}; source tables are the same canonical snapshots used by the Python validation pipeline.')
spark.stop()
raise SystemExit(2 if failed else 0)
