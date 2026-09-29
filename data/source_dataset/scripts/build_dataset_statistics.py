#!/usr/bin/env python3
"""Write a compact current statistics report for all dataset deliverables."""
from pathlib import Path
import json
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]; D=ROOT/'data'; DOC=ROOT/'documentation'
PK={'customers':'Customer_ID','orders':'Order_ID','order_items':'Order_Item_ID','menu_items':'Item_ID','menu_categories':'Category_ID','restaurants':'Location_ID','pricing_history':'Price_History_ID','promotions':'Promotion_ID','ratings':'Rating_ID','inventory':'Inventory_ID','wastage':'Wastage_ID','ordering_channels':'Channel_ID'}
rows=[]
for path in sorted(D.glob('*.csv')):
    n=0; keys=set(); date_min=None; date_max=None
    for chunk in pd.read_csv(path,chunksize=100000):
        n+=len(chunk)
        key=PK[path.stem]
        keys.update(chunk[key].dropna().astype(str).tolist())
        date_cols=[c for c in chunk.columns if c.endswith('_Date') or c in ('Order_DateTime',)]
        for col in date_cols:
            vals=pd.to_datetime(chunk[col],errors='coerce').dropna()
            if len(vals):
                lo,hi=vals.min().date().isoformat(),vals.max().date().isoformat()
                date_min=lo if date_min is None else min(date_min,lo); date_max=hi if date_max is None else max(date_max,hi)
    rows.append({'table':path.stem,'rows':n,'unique_primary_keys':len(keys),'date_min':date_min,'date_max':date_max})
processed=[]
for p in sorted((ROOT/'processed_data').glob('*.csv')):
    with p.open('rb') as f: n=sum(1 for _ in f)-1
    processed.append({'artifact':str(p.relative_to(ROOT)),'rows':n,'bytes':p.stat().st_size})
splits={}
for name in ('train','validation','test'):
    p=ROOT/'splits'/'demand_forecast'/f'{name}.csv'
    with p.open('rb') as f: splits[name]={'rows':sum(1 for _ in f)-1,'bytes':p.stat().st_size}
out={'tables':rows,'processed_artifacts':processed,'splits':splits}
(DOC/'DATASET_STATISTICS.json').write_text(json.dumps(out,indent=2),encoding='utf-8')
lines=['# Dataset statistics','', '| Table | Rows | Unique primary keys | Date minimum | Date maximum |','|---|---:|---:|---|---|']
for x in rows: lines.append(f"| {x['table']} | {x['rows']:,} | {x['unique_primary_keys']:,} | {x['date_min'] or '—'} | {x['date_max'] or '—'} |")
lines+=['','## Processed artifacts','','| Artifact | Rows | Bytes |','|---|---:|---:|']
for x in processed: lines.append(f"| `{x['artifact']}` | {x['rows']:,} | {x['bytes']:,} |")
lines+=['','## Forecast splits','','| Split | Rows | Bytes |','|---|---:|---:|']
for k,v in splits.items(): lines.append(f"| {k} | {v['rows']:,} | {v['bytes']:,} |")
(DOC/'VALIDATION_REPORT.txt').write_text('\n'.join(lines)+'\n',encoding='utf-8')
print(f"PASS: statistics for {len(rows)} tables, {len(processed)} processed artifacts, and {len(splits)} splits")
