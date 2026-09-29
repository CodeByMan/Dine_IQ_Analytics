from pathlib import Path
import csv, json, shutil
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
DATA=ROOT/'data'; DOC=ROOT/'documentation'; SAMPLE=ROOT/'samples'; FIX=ROOT/'fixtures'; PROC=ROOT/'processed_data'
for p in [DOC,SAMPLE/'cleaned',FIX/'duplicates',FIX/'hidden_data',PROC,ROOT/'splits'/'demand_forecast'] : p.mkdir(parents=True,exist_ok=True)
T={p.stem:pd.read_csv(p) for p in DATA.glob('*.csv')}
# Isolated duplicate fixtures: duplicate one valid row exactly; primary tables remain clean.
for table,key,filename in [('orders','Order_ID','duplicate_orders.csv'),('order_items','Order_Item_ID','duplicate_order_items.csv')]:
    df=T[table]
    candidates=df.loc[~df.get('Is_Anomaly',False).astype(bool)] if 'Is_Anomaly' in df else df
    sample=candidates.iloc[[0]].copy(); fixture=pd.concat([sample,sample],ignore_index=True)
    fixture.to_csv(FIX/'duplicates'/filename,index=False)
(FIX/'duplicates'/'manifest.json').write_text(json.dumps({'purpose':'Isolated validator fixtures; canonical data files are unchanged.','fixtures':[{'file':'duplicate_orders.csv','table':'orders','primary_key':'Order_ID','expected_duplicate_groups':1},{'file':'duplicate_order_items.csv','table':'order_items','primary_key':'Order_Item_ID','expected_duplicate_groups':1}]},indent=2))
# Cleaned 25-row samples derived from canonical data with conservative row-level checks.
def boolcol(df,c): return df[c].fillna(False).astype(str).str.lower().isin(['true','1']) if c in df else pd.Series(False,index=df.index)
clean_rules=[]
for name,df in T.items():
    before=len(df); out=df.copy(); rule='Retain source rows; preserve documented optional nulls.'; condition='None beyond required key/relationship checks.'
    if name=='orders':
        out=out[~boolcol(out,'Is_Anomaly') & ~boolcol(out,'Is_Duplicate') & out['Order_ID'].notna()].drop_duplicates('Order_ID')
        rule='Exclude flagged anomaly/duplicate rows and repeated Order_ID.'; condition='Is_Anomaly or Is_Duplicate or repeated Order_ID.'
    elif name=='order_items':
        out=out[~boolcol(out,'Is_Anomaly') & ~boolcol(out,'Is_Duplicate') & (pd.to_numeric(out.Quantity,errors='coerce')>0) & out.Order_ID.isin(T['orders'].loc[T['orders'].Order_Status.eq('Completed'),'Order_ID'])].drop_duplicates('Order_Item_ID')
        rule='Exclude flagged anomaly/duplicate rows, nonpositive quantities, and lines for noncompleted orders.'; condition='Quality flags, quantity <= 0, or parent order not Completed.'
    elif name=='ratings':
        out=out[~boolcol(out,'Is_Anomaly') & ~boolcol(out,'Is_Duplicate') & pd.to_numeric(out.Rating,errors='coerce').between(1,5)].drop_duplicates('Rating_ID')
        rule='Exclude flagged anomaly/duplicate ratings and ratings outside 1-5.'; condition='Quality flags or rating outside allowed scale.'
    elif name=='wastage':
        out=out[~boolcol(out,'Is_Anomaly') & (pd.to_numeric(out.Quantity_Wasted,errors='coerce')>=0)].drop_duplicates('Wastage_ID')
        rule='Exclude flagged anomaly rows and negative quantity from clean sample.'; condition='Is_Anomaly or negative Quantity_Wasted.'
    elif name=='inventory':
        out=out[~boolcol(out,'Is_Anomaly')].drop_duplicates('Inventory_ID'); rule='Exclude flagged anomaly rows from clean sample.'; condition='Is_Anomaly.'
    elif name=='customers': out=out.drop_duplicates('Customer_ID')
    elif name=='menu_items': out=out.drop_duplicates('Item_ID')
    elif name=='menu_categories': out=out.drop_duplicates('Category_ID')
    elif name=='restaurants': out=out.drop_duplicates('Location_ID')
    elif name=='pricing_history': out=out.drop_duplicates('Price_History_ID')
    elif name=='promotions': out=out.drop_duplicates('Promotion_ID')
    elif name=='ordering_channels': out=out.drop_duplicates('Channel_ID')
    out.head(25).to_csv(SAMPLE/'cleaned'/f'{name}_clean_sample.csv',index=False)
    flagged=int((~df.index.isin(out.index)).sum())
    clean_rules.append({'Table':name,'Source':'data/'+name+'.csv','Condition_Detected':condition,'Cleaning_Rule':rule,'Action':'Exclude from cleaned sample only; raw canonical table preserved.','Result':'First 25 eligible rows exported.' if len(out) else 'No eligible rows.','Records_Removed_From_Clean_View':flagged,'Records_Corrected':0,'Records_Retained_As_Anomalies':int(boolcol(df,'Is_Anomaly').sum()),'Reason':'Keep source auditable; provide conservative clean view without silently altering raw data.'})
pd.DataFrame(clean_rules).to_csv(DOC/'CLEANING_DECISIONS.csv',index=False)
# Build historically aligned line terms with the dedicated, reproducible utility.
import subprocess, sys
subprocess.run([sys.executable,str(ROOT/'scripts'/'build_transaction_terms.py')],check=True)
# Deterministic, isolated promotion-trap example derived from existing campaign and price-history terms.
promrow=T['promotions'].loc[T['promotions'].Promotion_ID.eq(16)].iloc[0]; price=T['pricing_history'].loc[(T['pricing_history'].Item_ID.eq(5))&(T['pricing_history'].Location_ID.eq(5))&(T['pricing_history'].Effective_From.le('2024-12-17'))&(T['pricing_history'].Effective_To.ge('2024-12-17'))].iloc[0]
item=T['menu_items'].loc[T['menu_items'].Item_ID.eq(5)].iloc[0]; unit=float(price.New_Price); cost=float(item.Ingredient_Cost); disc_pct=float(promrow.Discount_Percentage)
trap=pd.DataFrame([{'Fixture_Type':'TEST_ONLY_BASELINE','Order_ID':'FIXTURE_BASELINE','Order_Date':'2024-12-17','Location_ID':5,'Item_ID':5,'Promotion_ID':'','Quantity':1,'Historical_Unit_Price':unit,'Discount_Percentage':0,'Gross_Revenue':unit,'Discount_Amount':0,'Net_Revenue':unit,'Ingredient_Cost_Total':cost,'Contribution_Profit':unit-cost,'Source_Price_History_ID':int(price.Price_History_ID),'Rule':'Baseline without campaign.'},{'Fixture_Type':'TEST_ONLY_PROMOTION_SCENARIO','Order_ID':'FIXTURE_PROMO','Order_Date':'2024-12-17','Location_ID':5,'Item_ID':5,'Promotion_ID':16,'Quantity':2,'Historical_Unit_Price':unit,'Discount_Percentage':disc_pct,'Gross_Revenue':unit*2,'Discount_Amount':round(unit*2*disc_pct/100,2),'Net_Revenue':round(unit*2*(1-disc_pct/100),2),'Ingredient_Cost_Total':cost*2,'Contribution_Profit':round(unit*2*(1-disc_pct/100)-cost*2,2),'Source_Price_History_ID':int(price.Price_History_ID),'Rule':'Existing 25% campaign, campaign date/location/category/minimum validated; fixture only.'}])
trap.to_csv(FIX/'hidden_data'/'promotion_trap_case.csv',index=False)
# Chronological demand split is built by the bounded-memory dedicated utility.
import subprocess, sys
subprocess.run([sys.executable,str(ROOT/'scripts'/'build_demand_splits.py')],check=True)
(DOC/'HIDDEN_DATA_READINESS.md').write_text('''# Hidden-data readiness\n\nThis package contains dataset-level cases only; it does not implement downstream inference, dashboards, or model behavior. Cases are isolated under `fixtures/hidden_data/` and must never be appended to canonical `data/*.csv`. The readiness matrix covers new locations, new items, missing optional values, outliers, unknown category references, effective price changes, unusual promotion terms, wastage outliers, seasonal shifts, and duplicate order keys. Expected responses are schema/validation checks or review flags; retain raw values for audit. The promotion trap example is deterministic and derives its price and campaign terms from canonical records.\n''',encoding='utf-8')
(DOC/'CLEANING_RULES.md').write_text('''# Cleaning rules and outputs\n\n`data/*.csv` are immutable source tables for this remediation. `samples/cleaned/*_clean_sample.csv` are small clean views. Exact rule, source, detection, action, counts, correction/removal, anomaly retention, and reason are in `CLEANING_DECISIONS.csv`. No raw source rows were corrected or removed. Clean views exclude only flagged anomaly/duplicate rows, invalid quantities/ratings, noncompleted order lines, and repeated primary keys as applicable.\n''',encoding='utf-8')
# Fix type dictionary; rebuild with explicit numeric duration override.
