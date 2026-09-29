"""Build a line-level analytical view with historical prices and applicable recorded promotion terms."""
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]; D=ROOT/'data'; OUT=ROOT/'processed_data'; OUT.mkdir(exist_ok=True)
def flag(s): return s.fillna(False).astype(str).str.lower().isin(['true','1'])
o=pd.read_csv(D/'orders.csv',usecols=['Order_ID','Location_ID','Promotion_ID','Order_Date','Order_Status','Subtotal'])
o['Order_Date']=pd.to_datetime(o.Order_Date,errors='coerce')
li=pd.read_csv(D/'order_items.csv')
li=li.drop(columns=[c for c in ['Price_History_ID','Price_Sensitive','Price_Sensitivity_Applied','Promotion_Type_Applied','Promotion_Qualified','Gross_Line_Value','Scenario_Discount'] if c in li],errors='ignore')
li=li.drop(columns=[c for c in ['Price_History_ID','Price_Sensitive','Price_Sensitivity_Applied','Promotion_Type_Applied','Promotion_Qualified','Gross_Line_Value','Scenario_Discount'] if c in li],errors='ignore')
li=li.loc[~flag(li.Is_Anomaly)&~flag(li.Is_Duplicate)&pd.to_numeric(li.Quantity,errors='coerce').gt(0)]
mi=pd.read_csv(D/'menu_items.csv',usecols=['Item_ID','Category_ID','Item_Name'])
ph=pd.read_csv(D/'pricing_history.csv',usecols=['Item_ID','Location_ID','Price_History_ID','Effective_From','Effective_To','New_Price'])
ph['Effective_From']=pd.to_datetime(ph.Effective_From); ph['Effective_To']=pd.to_datetime(ph.Effective_To)
promo=pd.read_csv(D/'promotions.csv'); promo['Start_Date']=pd.to_datetime(promo.Start_Date); promo['End_Date']=pd.to_datetime(promo.End_Date)
base=li.merge(o,on='Order_ID',how='inner'); base=base.loc[base.Order_Status.eq('Completed')].merge(mi,on='Item_ID',how='left').sort_values('Order_Date')
price_rows=ph.sort_values('Effective_From')
base=pd.merge_asof(base,price_rows,left_on='Order_Date',right_on='Effective_From',by=['Item_ID','Location_ID'],direction='backward')
covered=base.Price_History_ID.notna() & base.Order_Date.le(base.Effective_To)
base['Historical_Unit_Price']=base.New_Price.where(covered,base.Unit_Price); base['Price_Source']=covered.map({True:'HISTORICAL_PRICE',False:'RAW_PRICE_FALLBACK'}); base['Price_History_ID']=base.Price_History_ID.where(covered)
base=base.reset_index(drop=True); base['Gross_Line_Value']=(base.Historical_Unit_Price*base.Quantity).round(2)
base['Promotion_Qualified']=False; base['Promotion_Type_Applied']='NONE'; base['Scenario_Discount']=0.0
base['Promotion_Discount_Percentage']=0.0; base['Fixed_Discount_Term']=0.0; base['Order_Minimum_Order_Value']=0.0
for p in promo.itertuples():
    m=base.Promotion_ID.eq(p.Promotion_ID)&base.Order_Date.between(p.Start_Date,p.End_Date)&base.Subtotal.ge(float(p.Minimum_Order_Value or 0))
    if str(p.Applicable_Locations)!='ALL': m &= base.Location_ID.astype(str).isin(str(p.Applicable_Locations).split('|'))
    if str(p.Applicable_Categories)!='ALL': m &= base.Category_ID.astype(str).isin(str(p.Applicable_Categories).split('|'))
    typ=str(p.Promotion_Type).strip().lower()
    if typ not in ('percentage','fixed','bundle','free delivery'): continue
    base.loc[m,'Promotion_Qualified']=True; base.loc[m,'Promotion_Type_Applied']=p.Promotion_Type
    base.loc[m,'Promotion_Discount_Percentage']=float(p.Discount_Percentage or 0); base.loc[m,'Fixed_Discount_Term']=float(p.Fixed_Discount or 0); base.loc[m,'Order_Minimum_Order_Value']=float(p.Minimum_Order_Value or 0)
    if typ=='percentage':
        base.loc[m,'Scenario_Discount']=(base.loc[m,'Gross_Line_Value']*float(p.Discount_Percentage or 0)/100).round(2)
    elif typ in ('fixed','bundle'):
        idx=base.index[m]
        eligible=base.loc[idx,['Order_ID','Gross_Line_Value']].copy()
        eligible['_gross_total']=eligible.groupby('Order_ID').Gross_Line_Value.transform('sum')
        eligible['_order_discount']=eligible['_gross_total'].clip(upper=float(p.Fixed_Discount or 0))
        eligible['_alloc']=(eligible['_order_discount']*eligible.Gross_Line_Value/eligible['_gross_total']).round(2)
        residual=(eligible['_order_discount']-eligible.groupby('Order_ID')['_alloc'].transform('sum')).round(2)
        last=~eligible.Order_ID.duplicated(keep='last')
        eligible.loc[last,'_alloc']=(eligible.loc[last,'_alloc']+residual.loc[last]).round(2)
        base.loc[idx,'Scenario_Discount']=eligible['_alloc']
# For item-level amounts, free-delivery campaign qualifies but has no item discount.
base['Net_Line_Value']=(base.Gross_Line_Value-base.Scenario_Discount).round(2)
base['Transaction_Terms_Consistent']=base.Scenario_Discount.ge(0)&base.Scenario_Discount.le(base.Gross_Line_Value)&((base.Gross_Line_Value-base.Scenario_Discount-base.Net_Line_Value).abs()<.011)
cols=['Order_ID','Order_Date','Location_ID','Item_ID','Item_Name','Category_ID','Quantity','Unit_Price','Historical_Unit_Price','Price_History_ID','Price_Source','Promotion_ID','Promotion_Type_Applied','Promotion_Qualified','Promotion_Discount_Percentage','Fixed_Discount_Term','Order_Minimum_Order_Value','Scenario_Discount','Gross_Line_Value','Net_Line_Value','Transaction_Terms_Consistent']
base[cols].to_csv(OUT/'transaction_terms.csv',index=False)
print(f'PASS: {len(base):,} completed clean order lines written')
