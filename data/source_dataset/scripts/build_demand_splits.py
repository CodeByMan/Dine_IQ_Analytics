"""Build reproducible chronological daily demand train/validation/test CSV splits."""
from pathlib import Path
import csv, json
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]; DATA=ROOT/'data'; OUT=ROOT/'splits'/'demand_forecast'; OUT.mkdir(parents=True,exist_ok=True)
def flag(s): return s.fillna(False).astype(str).str.lower().isin(['true','1'])
o=pd.read_csv(DATA/'orders.csv',usecols=['Order_ID','Order_Date','Location_ID','Order_Status'])
li=pd.read_csv(DATA/'order_items.csv',usecols=['Order_ID','Item_ID','Quantity','Is_Anomaly','Is_Duplicate'])
li=li.loc[~flag(li.Is_Anomaly)&~flag(li.Is_Duplicate)&pd.to_numeric(li.Quantity,errors='coerce').gt(0)]
o=o.loc[o.Order_Status.eq('Completed')]; o['Order_Date']=pd.to_datetime(o.Order_Date).dt.normalize()
demand=li.merge(o,on='Order_ID').groupby(['Order_Date','Item_ID','Location_ID'],as_index=False).Quantity.sum()
start,end=o.Order_Date.min(),o.Order_Date.max(); dates=pd.date_range(start,end,freq='D')
items=pd.read_csv(DATA/'menu_items.csv',usecols=['Item_ID','Launch_Date','Discontinued_Date','Is_Active'])
items['Launch_Date']=pd.to_datetime(items.Launch_Date,errors='coerce'); items['Discontinued_Date']=pd.to_datetime(items.Discontinued_Date,errors='coerce')
locs=np.sort(pd.read_csv(DATA/'restaurants.csv',usecols=['Location_ID']).Location_ID.unique())
ids=np.sort(items.Item_ID.unique()); n=len(dates); ntrain=int(n*.70); nval=int(n*.15)
train_end=dates[ntrain-1]; val_end=dates[ntrain+nval-1]
files={x:OUT/f'{x}.csv' for x in ('train','validation','test')}
for p in files.values():
    with p.open('w',newline='',encoding='utf-8') as f: csv.writer(f).writerow(['Date','Item_ID','Location_ID','Is_Available','Target_Quantity'])
item_meta=items.set_index('Item_ID')
# At most 30 dates x all dimensions are materialized at once (~170k rows), keeping memory bounded.
for offset in range(0,n,30):
    ds=dates[offset:offset+30]
    grid=pd.MultiIndex.from_product([ds,ids,locs],names=['Order_Date','Item_ID','Location_ID']).to_frame(index=False)
    grid=grid.merge(items,on='Item_ID',how='left')
    available=(grid.Is_Active.astype(str).str.lower().isin(['true','1'])) & (grid.Launch_Date.isna() | grid.Order_Date.ge(grid.Launch_Date)) & (grid.Discontinued_Date.isna() | grid.Order_Date.le(grid.Discontinued_Date))
    grid=grid.loc[available,['Order_Date','Item_ID','Location_ID']]
    grid=grid.merge(demand,on=['Order_Date','Item_ID','Location_ID'],how='left',validate='one_to_one')
    grid['Target_Quantity']=grid.Quantity.fillna(0).astype('int64')
    grid['Is_Available']=1
    grid['Date']=grid.Order_Date.dt.strftime('%Y-%m-%d')
    for name,mask in [('train',grid.Order_Date.le(train_end)),('validation',grid.Order_Date.gt(train_end)&grid.Order_Date.le(val_end)),('test',grid.Order_Date.gt(val_end))]:
        part=grid.loc[mask,['Date','Item_ID','Location_ID','Is_Available','Target_Quantity']]
        if len(part): part.to_csv(files[name],mode='a',header=False,index=False)
meta={'Task':'Daily restaurant demand forecast (dataset split preparation only; no model fitted).','Target':'Target_Quantity','Features':['Date','Item_ID','Location_ID','Is_Available'],'Split_Method':'Chronological by date; each date belongs to exactly one partition; no randomization.','Proportions':'70% train / 15% validation / 15% test by distinct calendar dates.','Date_Range':[str(dates.min().date()),str(dates.max().date())],'Boundaries':{'train_end':str(train_end.date()),'validation_start':str((train_end+pd.Timedelta(days=1)).date()),'validation_end':str(val_end.date()),'test_start':str((val_end+pd.Timedelta(days=1)).date())},'Leakage_Prevention':'Partitions are strictly chronological; future dates are never in an earlier partition. Demand is aggregated from completed orders only.','Zero_Demand':'Every active item/location/date combination is emitted; Target_Quantity=0 when no qualifying sale exists.'}
(OUT/'split_metadata.json').write_text(json.dumps(meta,indent=2),encoding='utf-8')
for n,p in files.items(): print(n,p.stat().st_size)
