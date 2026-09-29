"""Append column-level headers, sampled types, and generic value guidance to dictionary."""
from pathlib import Path
import pandas as pd
root=Path(__file__).resolve().parents[1]; doc=root/'documentation/DATA_DICTIONARY.md'
base=doc.read_text(encoding='utf-8').split('\n# Column-level reference')[0]
base=base.replace('| Order_Items | Order_Item_ID | order/menu item, quantity, unit price, line discount/total, cost/profit, request, preparation and status | Order_ID, Item_ID |','| Order_Items | Order_Item_ID | order/menu item, base and adjusted quantity, effective price-history key, campaign application, line discount/total, cost/profit, request and status | Order_ID, Item_ID, Price_History_ID |')
base=base.replace('| Menu_Items | Item_ID | name/category/cuisine, size, synthetic selling price/cost, prep time, calories, dietary flags, launch/discontinue | Category_ID |','| Menu_Items | Item_ID | name/category/cuisine, selling price/cost, preparation time, price-sensitivity cohort, observed sales/margin and high-wastage profiles | Category_ID |')
base=base.replace('| Wastage | Wastage_ID | location/item/date, wasted quantity/cost, reason/shift/recorder/preventability | Location_ID, Item_ID |','| Wastage | Wastage_ID | location/item/date, wasted quantity, distinct preparation quantity and wastage percentage, cost, reason/shift/recorder/preventability | Location_ID, Item_ID |')
out=[base,'\n# Column-level reference','\nThe tables below enumerate every source column. Sampled types are inferred from the generated CSVs; empty optional cells are nullable. PK/FK fields are positive integer identifiers.']
def typ(s):
    vals=s.dropna().astype(str)
    if s.name in {'Supplier_ID','Manager_ID'}: return 'string'
    if s.name in {'Estimated_Preparation_Time','Preparation_Time','Preparation_Quantity'}: return 'integer'
    if s.name in {'Response_Time_Hours','Wastage_Percentage','Price_Sensitivity_Elasticity'}: return 'decimal'
    if s.name in {'Price_Sensitive','Price_Sensitivity_Applied','Promotion_Qualified'}: return 'boolean'
    if s.name in {'Base_Quantity','Price_History_ID'} or (s.name.endswith('_ID') and s.name not in {'Supplier_ID','Manager_ID'}):
        idnum=pd.to_numeric(s,errors='coerce').dropna()
        if len(idnum) and ((idnum%1)==0).all(): return 'integer / nullable integer'
    if s.name in {'Estimated_Preparation_Time','Preparation_Time','Response_Time_Hours'}:
        n=pd.to_numeric(s,errors='coerce').dropna()
        return 'integer' if len(n) and ((n%1)==0).all() else 'decimal'
    if len(vals) and vals.isin(['True','False']).all(): return 'boolean'
    if any(t in s.name for t in ['Date','Time']): return 'date/timestamp or time string'
    numeric=pd.to_numeric(s,errors='coerce')
    if len(vals) and numeric.notna().sum()==len(vals):
        return 'integer' if ((numeric.dropna()%1)==0).all() else 'decimal'
    return 'string'

for p in sorted((root/'data').glob('*.csv')):
    df=pd.read_csv(p,nrows=40); out += [f'\n## {p.stem}\n','| Column | Sampled type | Meaning / expected values |','|---|---|---|']
    for c in df.columns:
        meaning=c.replace('_',' ')
        if c.endswith('_ID') and c not in {'Supplier_ID','Manager_ID'}: expected='Integer identifier / foreign key; nullable only where documented.'
        elif c in {'Supplier_ID','Manager_ID'}: expected='String business/staff identifier.'
        elif c=='Rating': expected='Integer from 1 to 5.'
        elif c in ['Unit_Price','Line_Total','Total_Amount','Subtotal','Ingredient_Cost','Base_Cost','Discount_Amount','Tax_Amount','Delivery_Fee','Total_Wastage_Cost','Lifetime_Value','Average_Order_Value']: expected='Synthetic PKR amount; anomalies may be flagged.'
        elif c in ['Estimated_Preparation_Time','Preparation_Time']: expected='Numeric preparation duration in minutes.'
        elif c=='Response_Time_Hours': expected='Numeric response duration in hours.'
        elif 'Date' in c or c.endswith('_Time'): expected='Synthetic event/effective date or time in the documented 24-month window.'
        elif c.startswith('Is_') or c.endswith('_Flag') or c.endswith('_Status')=='never': expected='Boolean quality/operational indicator where values are true/false.'
        else: expected=f'{meaning}; see table description above; controlled nulls/noise may occur where documented.'
        out.append(f'| `{c}` | {typ(df[c])} | {expected} |')
doc.write_text('\n'.join(out)+'\n',encoding='utf-8')
