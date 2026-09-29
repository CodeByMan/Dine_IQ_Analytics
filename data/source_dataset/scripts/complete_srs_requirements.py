#!/usr/bin/env python3
"""Deterministic dataset-level completion pass for the explicit SRS gaps.

Run after generate_dataset.py and before samples, splits, and Parquet conversion.
Canonical anomalies remain in the source tables and are identified by flags;
duplicate/hidden-data test records remain isolated under fixtures/.
"""
from pathlib import Path
import json
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
D = ROOT / 'data'
DOC = ROOT / 'documentation'
FIX = ROOT / 'fixtures'

def bools(s):
    return s.fillna(False).astype(str).str.lower().isin(['true', '1'])

def main():
    print('reading source tables...', flush=True)
    orders = pd.read_csv(D/'orders.csv')
    lines = pd.read_csv(D/'order_items.csv')
    if 'Base_Quantity' not in lines:
        lines['Base_Quantity'] = pd.to_numeric(lines.Quantity,errors='coerce')
    # Remove prior derived columns so the build pass is safe to rerun.
    lines = lines.drop(columns=[c for c in ['Price_History_ID','Price_Sensitive','Price_Sensitivity_Applied','Promotion_Type_Applied','Promotion_Qualified','Gross_Line_Value','Scenario_Discount'] if c in lines], errors='ignore')
    menu = pd.read_csv(D/'menu_items.csv')
    ph = pd.read_csv(D/'pricing_history.csv')
    promos = pd.read_csv(D/'promotions.csv')
    locs = pd.read_csv(D/'restaurants.csv')
    trap = promos.Promotion_ID.eq(14)
    promos.loc[trap,'Promotion_Name']='High Basket Threshold Deal'
    promos.loc[trap,'Promotion_Type']='Percentage'
    promos.loc[trap,'Discount_Percentage']=20
    promos.loc[trap,'Fixed_Discount']=0
    promos.loc[trap,'Minimum_Order_Value']=4999
    promos.loc[trap,'Start_Date']='2026-05-01'
    promos.loc[trap,'End_Date']='2026-07-31'
    promos.loc[trap,'Applicable_Locations']='ALL'
    promos.loc[trap,'Applicable_Categories']='ALL'
    promos.loc[trap,'Promotion_Status']='Active'
    promos.loc[trap,'Is_Misleading']=True
    promos.to_csv(D/'promotions.csv',index=False)

    # Cover the complete transaction date range with effective prices.
    ph['Effective_To'] = pd.to_datetime(ph.Effective_To)
    ph['Effective_From'] = pd.to_datetime(ph.Effective_From)
    last_by_pair = ph.groupby(['Item_ID','Location_ID']).Effective_To.idxmax()
    ph.loc[last_by_pair, 'Effective_To'] = pd.Timestamp('2026-09-24')
    ph.to_csv(D/'pricing_history.csv', index=False)

    # Make an explicit, reproducible price-sensitivity cohort. Higher historical
    # price steps reduce the generated purchase quantity for this cohort.
    menu['Price_Sensitive'] = (pd.to_numeric(menu.Item_ID).astype(int) % 5 == 0)
    menu['Price_Sensitivity_Elasticity'] = np.where(menu.Price_Sensitive, -1.2, -0.2)
    menu.to_csv(D/'menu_items.csv', index=False)

    print('matching historical prices...', flush=True)
    od = orders[['Order_ID','Location_ID','Order_Date','Order_Status','Promotion_ID']].copy()
    od['Order_Date'] = pd.to_datetime(od.Order_Date, errors='coerce')
    li = lines.merge(od, on='Order_ID', how='left', validate='many_to_one')
    li = li.merge(menu[['Item_ID','Category_ID','Ingredient_Cost','Price_Sensitive']], on='Item_ID', how='left', validate='many_to_one')
    ph['Effective_From'] = pd.to_datetime(ph.Effective_From)
    ph['Effective_To'] = pd.to_datetime(ph.Effective_To)
    ph['New_Price'] = pd.to_numeric(ph.New_Price)
    li = li.sort_values('Order_Date')
    price = ph.sort_values('Effective_From')
    li = pd.merge_asof(li, price[['Item_ID','Location_ID','Price_History_ID','Effective_From','Effective_To','New_Price']],
                       left_on='Order_Date', right_on='Effective_From', by=['Item_ID','Location_ID'], direction='backward')
    valid_price = li.Price_History_ID.notna() & li.Order_Date.le(li.Effective_To)
    if not valid_price.all():
        raise RuntimeError(f'Historical prices do not cover {int((~valid_price).sum())} order lines')
    li['Unit_Price'] = li.New_Price.astype(float)
    li['Price_History_ID'] = li.Price_History_ID.astype('int64')
    li['Price_Sensitivity_Applied'] = False
    # Effective price step >= 3 marks later, higher-price periods. Quantity is
    # adjusted by one serving for non-anomaly observations, bounded to 1..3.
    step = li.Price_History_ID.astype(int).map(ph.groupby(['Item_ID','Location_ID']).Price_History_ID.rank(method='dense').to_dict()) if False else None
    # Derive each pair's period rank without relying on non-unique global IDs.
    rank_map = {}
    for key, group in price.groupby(['Item_ID','Location_ID'], sort=False):
        for rank, hid in enumerate(group.Price_History_ID.tolist(), start=1):
            rank_map[int(hid)] = rank
    ranks = li.Price_History_ID.map(rank_map).fillna(1).astype(int)
    sensitive = li.Price_Sensitive.fillna(False).astype(bool) & ~bools(li.Is_Anomaly)
    if 'Base_Quantity' not in li:
        li['Base_Quantity'] = pd.to_numeric(li.Quantity, errors='coerce')
    q = pd.to_numeric(li.Base_Quantity, errors='coerce').fillna(0).astype(int)
    q.loc[sensitive & ranks.eq(1) & q.lt(3)] += 1
    q.loc[sensitive & ranks.ge(3) & q.gt(1)] -= 1
    li['Quantity'] = q
    li['Price_Sensitivity_Applied'] = sensitive & ((ranks.eq(1) & pd.to_numeric(li.Base_Quantity).lt(3)) | (ranks.ge(3) & pd.to_numeric(li.Base_Quantity).gt(1)))

    print('validating recorded promotion eligibility...', flush=True)
    # Determine whether the recorded campaign actually qualifies at the order
    # level; invalid/ineligible references are cleared, never silently honored.
    promo_by_id = promos.set_index('Promotion_ID')
    pmap = promo_by_id.to_dict('index')
    good = ~bools(li.Is_Anomaly) & pd.to_numeric(li.Quantity, errors='coerce').gt(0)
    li['Gross_Line_Value'] = (li.Unit_Price * li.Quantity).round(2)
    gross_by_order = li.loc[good].groupby('Order_ID').Gross_Line_Value.sum()
    orders['_Gross_Check'] = orders.Order_ID.map(gross_by_order).fillna(0)
    orders['_Candidate_Promotion_ID'] = pd.to_numeric(orders.Promotion_ID, errors='coerce')
    orders['_Order_Date'] = pd.to_datetime(orders.Order_Date, errors='coerce')
    orders['Promotion_ID'] = pd.Series(pd.NA, index=orders.index, dtype='Int64')
    for pid, p in pmap.items():
        mask = orders._Candidate_Promotion_ID.eq(int(pid)) & orders.Order_Status.eq('Completed')
        mask &= orders._Order_Date.between(pd.Timestamp(p['Start_Date']), pd.Timestamp(p['End_Date']))
        mask &= orders._Gross_Check.ge(float(p['Minimum_Order_Value'] or 0))
        if str(p['Applicable_Locations']) != 'ALL':
            allowed = {int(x) for x in str(p['Applicable_Locations']).split('|')}
            mask &= pd.to_numeric(orders.Location_ID, errors='coerce').isin(allowed)
        if str(p['Applicable_Categories']) != 'ALL':
            cats = {int(x) for x in str(p['Applicable_Categories']).split('|')}
            eligible_orders = li.loc[good & pd.to_numeric(li.Category_ID, errors='coerce').isin(cats), 'Order_ID'].unique()
            mask &= orders.Order_ID.isin(eligible_orders)
        if str(p['Promotion_Type']).lower() == 'free delivery':
            mask &= orders.Delivery_Type.astype(str).str.lower().eq('delivery')
        orders.loc[mask, 'Promotion_ID'] = int(pid)

    print('recalculating line and order totals...', flush=True)
    # Recompute eligible discounts from the actual campaign rules.
    li['Discount_Amount'] = 0.0
    li['Promotion_Type_Applied'] = 'NONE'
    li['Promotion_Qualified'] = False
    li['Scenario_Discount'] = 0.0
    li['_Order_Promotion_ID'] = li.Order_ID.map(orders.set_index('Order_ID').Promotion_ID)
    for pid, p in pmap.items():
        line_mask = li._Order_Promotion_ID.eq(int(pid)) & good
        if str(p['Applicable_Categories']) != 'ALL':
            cats = {int(x) for x in str(p['Applicable_Categories']).split('|')}
            line_mask &= pd.to_numeric(li.Category_ID, errors='coerce').isin(cats)
        idx = li.index[line_mask]
        if not len(idx): continue
        gross = li.loc[idx, 'Gross_Line_Value']
        typ = str(p['Promotion_Type']).lower()
        if typ == 'percentage':
            disc = (gross * float(p['Discount_Percentage']) / 100).round(2)
        elif typ in ('fixed','bundle'):
            baskets = gross.groupby(li.loc[idx,'Order_ID']).transform('sum')
            amount = baskets.clip(upper=float(p['Fixed_Discount']))
            disc = (amount * gross / baskets).round(2)
            # Allocate any cent rounding remainder to the last eligible line/order.
            alloc = pd.DataFrame({'Order_ID':li.loc[idx,'Order_ID'].to_numpy(),'Discount':disc.to_numpy(),'Amount':amount.to_numpy()},index=idx)
            residual=(alloc.Amount-alloc.groupby('Order_ID').Discount.transform('sum')).round(2)
            last=~alloc.Order_ID.duplicated(keep='last')
            disc.loc[last.index[last]]=(disc.loc[last.index[last]]+residual.loc[last.index[last]]).round(2)
        else:
            disc = pd.Series(0.0, index=idx)
        li.loc[idx,'Discount_Amount'] = disc.to_numpy()
        li.loc[idx,'Promotion_Type_Applied'] = p['Promotion_Type']
        li.loc[idx,'Promotion_Qualified'] = True
    li['Line_Total'] = (li.Gross_Line_Value - li.Discount_Amount).round(2)
    li['Item_Cost'] = (pd.to_numeric(li.Ingredient_Cost)*pd.to_numeric(li.Quantity)).round(2)
    li['Gross_Profit'] = (li.Line_Total-li.Item_Cost).round(2)
    li['Price_Sensitive'] = li.Price_Sensitive.fillna(False)
    li = li.sort_values('Order_Item_ID')
    keep = list(lines.columns)
    for col in ['Base_Quantity','Price_History_ID','Price_Sensitive','Price_Sensitivity_Applied','Promotion_Type_Applied','Promotion_Qualified']:
        if col not in keep: keep.append(col)
    li[keep].to_csv(D/'order_items.csv', index=False)

    # Rebuild order totals from valid billable lines; retain invalid line cases
    # separately in source with their anomaly flags for quality assessment.
    billable = li.loc[~bools(li.Is_Anomaly) & pd.to_numeric(li.Quantity,errors='coerce').gt(0) & li.Order_Status.eq('Completed')].copy()
    totals = billable.groupby('Order_ID').agg(Subtotal=('Gross_Line_Value','sum'), Discount_Amount=('Discount_Amount','sum')).round(2)
    orders = orders.set_index('Order_ID')
    orders['Subtotal'] = totals.Subtotal.reindex(orders.index).fillna(0).round(2)
    orders['Discount_Amount'] = totals.Discount_Amount.reindex(orders.index).fillna(0).round(2)
    free_delivery = orders.Promotion_ID.map(promos.set_index('Promotion_ID').Promotion_Type.to_dict()).eq('Free Delivery')
    orders.loc[free_delivery, 'Delivery_Fee'] = 0
    orders['Tax_Amount'] = ((orders.Subtotal-orders.Discount_Amount).clip(lower=0)*.05).round(2)
    orders['Total_Amount'] = (orders.Subtotal-orders.Discount_Amount+orders.Tax_Amount+orders.Delivery_Fee).round(2)
    orders.drop(columns=['_Gross_Check','_Candidate_Promotion_ID','_Order_Date'],inplace=True)
    orders.reset_index().to_csv(D/'orders.csv',index=False)

    print('adding wastage denominator...', flush=True)
    # Add the denominator needed for a production-based wastage percentage.
    waste = pd.read_csv(D/'wastage.csv').drop(columns=['Preparation_Quantity','Wastage_Percentage'],errors='ignore')
    waste['Wastage_Date'] = pd.to_datetime(waste.Wastage_Date).dt.strftime('%Y-%m-%d')
    sales = billable[['Location_ID','Item_ID','Quantity','Order_Date']].copy()
    sales['Order_Date'] = pd.to_datetime(sales.Order_Date).dt.strftime('%Y-%m-%d')
    daily = sales.groupby(['Location_ID','Item_ID','Order_Date']).Quantity.sum().rename('Units_Sold').reset_index()
    waste = waste.merge(daily, left_on=['Location_ID','Item_ID','Wastage_Date'], right_on=['Location_ID','Item_ID','Order_Date'], how='left')
    waste['Units_Sold'] = pd.to_numeric(waste.Units_Sold,errors='coerce').fillna(0)
    waste['Preparation_Quantity'] = np.ceil(np.maximum(waste.Units_Sold + pd.to_numeric(waste.Quantity_Wasted), waste.Units_Sold*1.08 + 1)).astype(int)
    waste['Wastage_Percentage'] = (100*pd.to_numeric(waste.Quantity_Wasted)/waste.Preparation_Quantity).round(4)
    waste.drop(columns=['Order_Date','Units_Sold'],inplace=True)
    waste.to_csv(D/'wastage.csv',index=False)

    # Explicitly tag the requested dish profiles from observed generated sales,
    # contribution margin, and wastage statistics (quartile definitions).
    clean_sales=li.loc[~bools(li.Is_Anomaly) & pd.to_numeric(li.Quantity,errors='coerce').gt(0)]
    units=clean_sales.groupby('Item_ID').Quantity.sum()
    margin=pd.to_numeric(menu.Selling_Price)-pd.to_numeric(menu.Ingredient_Cost)
    observed=menu.Item_ID.map(units).fillna(0)
    low_margin=margin.le(margin.quantile(.25)); high_margin=margin.ge(margin.quantile(.75))
    high_sales=observed.ge(observed.quantile(.75)); low_sales=observed.le(observed.quantile(.25))
    waste_rate=waste.groupby('Item_ID').Wastage_Percentage.mean()
    high_waste=menu.Item_ID.map(waste_rate).fillna(0).ge(waste_rate.quantile(.75))
    menu['Observed_Units_Sold']=observed.astype('int64')
    menu['Contribution_Margin_PKR']=margin.round(2)
    menu['Is_Popular_Low_Margin']=high_sales & low_margin
    menu['Is_Profitable_Low_Selling']=high_margin & low_sales
    menu['Is_High_Wastage_Item']=high_waste
    if not menu.Is_Popular_Low_Margin.any() or not menu.Is_Profitable_Low_Selling.any():
        raise RuntimeError('Generated tables do not contain both required sales/margin scenarios')
    menu.to_csv(D/'menu_items.csv',index=False)

    # Rebuild transaction terms after canonical transaction corrections.
    build_terms = ROOT/'scripts'/'build_transaction_terms.py'
    import subprocess, sys
    subprocess.run([sys.executable,str(build_terms)],check=True)
    # Keep fixture cases isolated from canonical source tables.
    create_quality_fixtures()
    create_promotion_trap_fixture()
    write_docs()
    print('PASS: historical prices, campaign rules, price-response cohort, wastage denominator, strategic item profiles, and isolated quality fixtures updated')

def create_quality_fixtures():
    out = FIX/'data_quality'; out.mkdir(parents=True,exist_ok=True)
    cases = [
      ('missing_values','customers',{'Customer_ID':1,'Age':None}),
      ('duplicate_orders','orders',{'Order_ID':1,'duplicate_of':1}),
      ('duplicate_order_lines','order_items',{'Order_Item_ID':1,'duplicate_of':1}),
      ('invalid_menu_prices','menu_items',{'Item_ID':-900003,'Selling_Price':-1}),
      ('negative_quantities','order_items',{'Order_Item_ID':-900004,'Quantity':-1}),
      ('invalid_dates','orders',{'Order_ID':-900005,'Order_Date':'2026-02-30'}),
      ('invalid_ratings','ratings',{'Rating_ID':-900006,'Rating':6}),
      ('missing_customer_ids','orders',{'Order_ID':-900007,'Customer_ID':None}),
      ('missing_menu_ids','order_items',{'Order_Item_ID':-900008,'Item_ID':None}),
      ('invalid_restaurant_ids','orders',{'Order_ID':-900009,'Location_ID':999999}),
      ('impossible_wastage_quantities','wastage',{'Wastage_ID':-900010,'Quantity_Wasted':1000,'Preparation_Quantity':20}),
      ('incorrect_discounts','order_items',{'Order_Item_ID':-900011,'Gross_Line_Value':100,'Discount_Amount':125}),
      ('cancelled_transactions','orders',{'Order_ID':-900012,'Order_Status':'Cancelled'}),
      ('inconsistent_units','wastage',{'Wastage_ID':-900013,'Quantity_Unit':'kg','Expected_Unit':'portion'}),
      ('invalid_location_references','inventory',{'Inventory_ID':-900014,'Location_ID':999999}),
    ]
    pd.DataFrame([{'Issue_Type':i,'Table':t,'Fixture_Record_JSON':json.dumps(r,sort_keys=True),'Expected_Detector':i} for i,t,r in cases]).to_csv(out/'quality_issue_cases.csv',index=False)
    (out/'manifest.json').write_text(json.dumps({'isolated_test_fixtures':True,'canonical_tables_modified_with_test_corruption':False,'case_count':len(cases),'cases':[x[0] for x in cases]},indent=2),encoding='utf-8')
    hidden = [
      ('missing_values',{'Customer_ID':None,'Age':None},'retain optional null; flag required-key null'),
      ('duplicate_orders',{'Order_ID':1001,'duplicate_Order_ID':1001},'detect duplicate key; keep fixture isolated'),
      ('unknown_menu_item',{'Order_ID':-1,'Item_ID':999999},'report unknown key; do not crash'),
      ('new_restaurant_location',{'Location_ID':999999,'City':'Test City'},'accept schema-valid new entity and report unseen location'),
      ('price_change',{'Item_ID':25,'Location_ID':1,'Old_Price':1000,'New_Price':1100,'Effective_From':'2026-09-25'},'validate effective date and recompute price features'),
      ('unusual_promotion',{'Promotion_Type':'Percentage','Discount_Percentage':80,'Minimum_Order_Value':0},'flag out-of-policy discount for review'),
      ('extreme_wastage',{'Quantity_Wasted':1000,'Preparation_Quantity':50},'flag wastage above prepared quantity'),
      ('seasonal_change',{'Month':2,'Demand_Multiplier':2.5},'detect distribution shift; do not assume fixed seasonal factor'),
      ('unexpected_customer_behavior',{'Customer_ID':1,'Orders_Per_Day':80,'Baseline_Orders_Per_Day':1},'flag behavior outlier without rejecting valid customer ID'),
      ('outlier',{'Order_Total':1000000,'Baseline_P99':50000},'flag numeric outlier for review'),
    ]
    hdir=FIX/'hidden_data'; hdir.mkdir(parents=True,exist_ok=True)
    pd.DataFrame([{'Case_ID':i,'Payload_JSON':json.dumps(v,sort_keys=True),'Expected_Handling':e} for i,v,e in hidden]).to_csv(hdir/'hidden_scenarios.csv',index=False)

def create_promotion_trap_fixture():
    out=FIX/'hidden_data'; out.mkdir(parents=True,exist_ok=True)
    rows=[
      {'Scenario':'Baseline below threshold','Promotion_ID':'','Order_Date':'2026-06-15','Location_ID':1,'Basket_Gross_Value':4900,'Campaign_Minimum_Order_Value':4999,'Discount_Percentage':0,'Discount_Amount':0,'Net_Value':4900,'Basket_Cost':3400,'Contribution_Profit':1500,'Qualifies':False},
      {'Scenario':'Threshold campaign applied','Promotion_ID':14,'Order_Date':'2026-06-15','Location_ID':1,'Basket_Gross_Value':5100,'Campaign_Minimum_Order_Value':4999,'Discount_Percentage':20,'Discount_Amount':1020,'Net_Value':4080,'Basket_Cost':3600,'Contribution_Profit':480,'Qualifies':True},
    ]
    pd.DataFrame(rows).to_csv(out/'promotion_trap_case.csv',index=False)

def write_docs():
    DOC.mkdir(exist_ok=True)
    (DOC/'DATA_GENERATION.md').write_text('''# Data generation\n\nRun `python scripts/build_submission.py` to produce the complete dataset from the seeded synthetic generator and dataset-level completion scripts. `generate_dataset.py` creates the raw relational source tables from scratch using seed 42 by default; it does not load a ready-made dataset. `complete_srs_requirements.py` deterministically adds the effective-price and eligible-promotion behavior, explicit price-sensitive cohort, observed item-profile flags, preparation denominator, and isolated quality fixtures. `remediate_dataset.py` creates cleaned samples, processed transaction terms, chronological splits, and duplicate fixtures. `create_parquet.py` writes all raw tables and large processed tables to Snappy Parquet.\n\nGeneration uses NumPy/Pandas and remains local. The date window is 24 months ending 2026-09-24. The line-item table is streamed during raw generation. Fixed seed and compatible dependency versions make the generation reproducible. All names, campaign records, amounts, and operational measurements are synthetic and do not describe actual businesses, people, or current price quotes.\n\nRealism elements are represented in code and source data: controlled nulls, isolated duplicate fixtures, flagged invalid transactions/cancellations, effective price changes and a deterministic price-response cohort, seasonal/weekend/peak-hour and location variation, customer segments/lifecycle groups, explicit observed sales/margin profiles, high-wastage item profiles, rating/sales anomalies, and a threshold-promotion case.\n''',encoding='utf-8')
    (DOC/'RELATIONSHIPS.md').write_text('''# Relationships and keys\n\n- `Customers.Customer_ID` 1-to-many `Orders.Customer_ID`.\n- `Orders.Order_ID` 1-to-many `Order_Items.Order_ID` and `Ratings.Order_ID`.\n- `Order_Items.Item_ID` references `Menu_Items.Item_ID`; lines also carry `Price_History_ID` matching the effective price record.\n- `Menu_Items.Category_ID` references `Menu_Categories.Category_ID`.\n- `Orders.Location_ID` references `Restaurants.Location_ID`; `Location_ID` also scopes prices, ratings, inventory, and wastage.\n- `Orders.Promotion_ID` optionally references `Promotions.Promotion_ID`; null means no applied campaign.\n- `Menu_Items.Item_ID` references pricing history, ratings, inventory, and wastage by item; location and effective date narrow the applicable pricing row.\n- `Orders.Channel_ID` references the supporting `Ordering_Channels` table.\n- `Wastage.Preparation_Quantity` is the production denominator, distinct from `Quantity_Wasted`; `Wastage_Percentage` is calculated from those fields.\n\nPrimary keys are unique integer identifiers. Optional campaign identifiers are nullable integer references. The Python and Spark workflows consume the same canonical source snapshots; Parquet conversion is generated from those CSV sources. `scripts/spark_pipeline.py` checks each required cross-table join and orphan condition.\n''',encoding='utf-8')
    (DOC/'PRICE_PROMOTION_RULES.md').write_text('''# Historical price and promotion rules\n\n`order_items.Unit_Price` is set to the effective `Pricing_History.New_Price` for the item's location and order date. The last effective interval covers 2026-09-24. A reproducible sensitivity cohort is `Item_ID % 5 == 0`; for non-anomaly orders its quantity is increased by one serving (capped at 3) in the first price period and reduced by one (floored at 1) from period three onward. `Price_Sensitivity_Elasticity` records the synthetic cohort parameter. This deterministic mechanism supports later price-sensitivity analysis; this package does not fit or report an elasticity model.\n\nOnly completed orders with an active campaign, sufficient gross basket, eligible location, and at least one eligible category retain a `Promotion_ID`. Percentage discounts apply to eligible item lines. Fixed and Bundle discounts use the campaign fixed cap allocated pro rata across eligible lines; this is the package's explicit operational interpretation for the available fields. Free Delivery applies no item discount and sets the delivery fee to zero. Other statuses and ineligible campaigns have no promotion discount. Order totals are recalculated from billable non-anomaly lines. Invalid transaction rows remain in canonical source with flags for data-quality assessment.\n\nPromotion 14 is a deliberately identified high-basket-threshold scenario: 20% discount only when the gross basket reaches PKR 4,999. `fixtures/hidden_data/promotion_trap_case.csv` demonstrates the threshold and contribution-profit effect in isolated test rows; fixture values do not enter canonical order tables.\n''',encoding='utf-8')
    (DOC/'HIDDEN_DATA_READINESS.md').write_text('''# Hidden-data readiness\n\nIsolated, executable fixture cases live under `fixtures/hidden_data/` and `fixtures/data_quality/`; none are appended to canonical tables. The readiness fixture contains missing values, duplicate orders, unknown menu items, new restaurant locations, price changes, unusual promotions, extreme wastage, seasonal changes, unexpected customer behavior, and outliers. Unknown IDs are reported for review rather than crashing a join; nulls are preserved for field-level policy checks; price/promotion terms and numeric ranges are checked explicitly. Run `python scripts/validate_srs_compliance.py` to exercise the deterministic dataset and fixture checks. This is dataset-level validation, not a prediction or hidden-data application implementation.\n''',encoding='utf-8')
    (DOC/'DATA_QUALITY.md').write_text('''# Data-quality coverage\n\nThe SRS text labels the list as 14 types but enumerates 15. This package retains all 15 listed checks and records the discrepancy in `fixtures/data_quality/manifest.json`. Canonical source data contains natural missingness, cancellations, and flagged anomalies. Duplicate keys and malformed/invalid reference cases are kept in isolated fixtures so the analysis tables retain valid primary keys. Run `python scripts/validate_srs_compliance.py` for the quality-fixture detection result.\n''',encoding='utf-8')

if __name__ == '__main__': main()
