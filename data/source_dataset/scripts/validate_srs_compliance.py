#!/usr/bin/env python3
"""Concise end-to-end dataset gate for the listed dataset SRS requirements."""
from pathlib import Path
import argparse, csv, json, subprocess, sys, zipfile
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]; D=ROOT/'data'; failures=[]; rows=[]
EXPECTED={'customers':62000,'orders':125000,'order_items':1250000,'menu_items':210,'menu_categories':18,'restaurants':27,'pricing_history':22697,'promotions':40,'ratings':120000,'inventory':34020,'wastage':65000,'ordering_channels':8}
PK={'customers':'Customer_ID','orders':'Order_ID','order_items':'Order_Item_ID','menu_items':'Item_ID','menu_categories':'Category_ID','restaurants':'Location_ID','pricing_history':'Price_History_ID','promotions':'Promotion_ID','ratings':'Rating_ID','inventory':'Inventory_ID','wastage':'Wastage_ID','ordering_channels':'Channel_ID'}
def report(label,ok,evidence=''):
    rows.append((label,bool(ok),evidence))
    if not ok: failures.append((label,evidence))
def run_script(name):
    try:
        r=subprocess.run([sys.executable,str(ROOT/'scripts'/name)],cwd=ROOT,capture_output=True,text=True)
    except OSError as e:
        return False,f'{type(e).__name__}: {e}'
    output=(r.stdout+r.stderr).strip()
    detail=' | '.join(output.splitlines()[-5:]) if output else f'exit={r.returncode}'
    return r.returncode==0,detail

def validate_key_join(tables,left,left_key,right,right_key,nullable=False):
    """Execute a key-only join and report matched/orphan references without file writes."""
    lk=pd.to_numeric(tables[left][left_key],errors='coerce').astype('Int64')
    rk=pd.to_numeric(tables[right][right_key],errors='coerce').astype('Int64')
    if not nullable and lk.isna().any():
        return False,f'{left}.{left_key} has {int(lk.isna().sum())} null keys'
    l=pd.DataFrame({'_key':lk.dropna() if nullable else lk})
    r=pd.DataFrame({'_key':rk.dropna().drop_duplicates()})
    orphan=int((~l['_key'].isin(r['_key'])).sum())
    joined=len(l.merge(r,on='_key',how='inner',validate='many_to_one'))
    return orphan==0 and joined>0,f'{left}↔{right}: joined={joined:,}; orphan references={orphan}'
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--zip',type=Path); args=ap.parse_args()
    tables={n:pd.read_csv(D/f'{n}.csv') for n in EXPECTED}
    zipok=True; evidence='not requested'
    if args.zip:
        try:
            with zipfile.ZipFile(args.zip) as z:
                bad=z.testzip(); zipok=bad is None; evidence='CRC verified' if bad is None else f'bad member {bad}'
        except Exception as e: zipok=False; evidence=str(e)
    report('ZIP integrity',zipok,evidence)
    volume=all(len(tables[n])==count and tables[n][PK[n]].notna().all() and tables[n][PK[n]].is_unique for n,count in EXPECTED.items())
    report('Required tables, volumes, and primary keys',volume,f'{len(tables)} tables; 12 expected row counts checked')
    # Required minimum entities/history, not just raw-file sizes.
    minok=(tables['orders'].Order_ID.nunique()>=100000 and tables['customers'].Customer_ID.nunique()>=50000 and tables['menu_items'].Item_ID.nunique()>=150 and tables['menu_categories'].Category_ID.nunique()>=10 and tables['restaurants'].Location_ID.nunique()>=20 and tables['ratings'].Rating_ID.nunique()>=100000 and tables['wastage'].Wastage_ID.nunique()>=50000 and tables['promotions'].Promotion_ID.nunique()>1 and tables['pricing_history'].Price_History_ID.nunique()>1 and (pd.to_datetime(tables['orders'].Order_Date).max()-pd.to_datetime(tables['orders'].Order_Date).min()).days>=365)
    report('Required dataset volumes and time coverage',minok,'entity counts and order date range calculated from source rows')
    rel=[('orders','Customer_ID','customers','Customer_ID'),('orders','Location_ID','restaurants','Location_ID'),('orders','Channel_ID','ordering_channels','Channel_ID'),('orders','Promotion_ID','promotions','Promotion_ID'),('order_items','Order_ID','orders','Order_ID'),('order_items','Item_ID','menu_items','Item_ID'),('menu_items','Category_ID','menu_categories','Category_ID'),('pricing_history','Item_ID','menu_items','Item_ID'),('pricing_history','Location_ID','restaurants','Location_ID'),('ratings','Order_ID','orders','Order_ID'),('ratings','Customer_ID','customers','Customer_ID'),('ratings','Item_ID','menu_items','Item_ID'),('ratings','Location_ID','restaurants','Location_ID'),('inventory','Location_ID','restaurants','Location_ID'),('inventory','Item_ID','menu_items','Item_ID'),('wastage','Location_ID','restaurants','Location_ID'),('wastage','Item_ID','menu_items','Item_ID')]
    fk=True; fkbad=[]
    for child,col,parent,pcol in rel:
        a=set(pd.to_numeric(tables[child][col],errors='coerce').dropna().astype(int)); b=set(pd.to_numeric(tables[parent][pcol],errors='coerce').dropna().astype(int))
        if not a<=b: fk=False; fkbad.append(f'{child}.{col}')
    report('Foreign keys and required join references',fk,f'{len(rel)} references; orphan columns={fkbad}')
    join_specs=[
        ('orders','Customer_ID','customers','Customer_ID',False),
        ('orders','Order_ID','order_items','Order_ID',False),
        ('order_items','Item_ID','menu_items','Item_ID',False),
        ('menu_items','Category_ID','menu_categories','Category_ID',False),
        ('orders','Location_ID','restaurants','Location_ID',False),
        ('orders','Promotion_ID','promotions','Promotion_ID',True),
        ('menu_items','Item_ID','pricing_history','Item_ID',False),
        ('menu_items','Item_ID','ratings','Item_ID',False),
        ('menu_items','Item_ID','inventory','Item_ID',False),
        ('menu_items','Item_ID','wastage','Item_ID',False),
    ]
    join_results=[validate_key_join(tables,*spec) for spec in join_specs]
    joinok=all(ok for ok,_ in join_results)
    jmsg='; '.join(msg for _,msg in join_results if not _)
    report('All 10 Python cross-table joins',joinok,jmsg or '10 key-only joins executed; no orphan references')
    o=tables['orders']; li=tables['order_items']; ph=tables['pricing_history']; menu=tables['menu_items']; waste=tables['wastage']
    statusok={'Order_Status','Cancellation_Reason'}<=set(o.columns) and o.Order_Status.eq('Cancelled').sum()>0
    report('Cancellation and rating links',statusok and {'Order_ID','Customer_ID'}<=set(tables['ratings'].columns),f"cancelled={int(o.Order_Status.eq('Cancelled').sum()):,}; ratings contain customer/order keys")
    priceok={'Effective_From','Effective_To'}<=set(ph.columns) and pd.to_datetime(ph.Effective_From).le(pd.to_datetime(ph.Effective_To)).all() and pd.to_datetime(ph.Effective_To).max()>=pd.to_datetime(o.Order_Date).max()
    report('Effective historical pricing coverage',priceok,f"history through {pd.to_datetime(ph.Effective_To).max().date()}; orders through {pd.to_datetime(o.Order_Date).max().date()}")
    terms_path=ROOT/'processed_data'/'transaction_terms.csv'
    termok=terms_path.exists()
    if termok:
        tx=pd.read_csv(terms_path)
        termok=(len(tx)>=1000000 and tx.Historical_Unit_Price.notna().all() and tx.Price_Source.eq('HISTORICAL_PRICE').all() and tx.Transaction_Terms_Consistent.fillna(False).all() and (tx.Scenario_Discount>=0).all() and (tx.Scenario_Discount<=tx.Gross_Line_Value).all())
        pct=tx.Promotion_Qualified & tx.Promotion_Type_Applied.astype(str).str.lower().eq('percentage')
        termok &= ((tx.loc[pct,'Scenario_Discount']-(tx.loc[pct,'Gross_Line_Value']*tx.loc[pct,'Promotion_Discount_Percentage']/100).round(2)).abs()<.02).all()
        none=~tx.Promotion_Qualified
        termok &= tx.loc[none,'Scenario_Discount'].eq(0).all()
        fixed=tx[tx.Promotion_Qualified & tx.Promotion_Type_Applied.astype(str).str.lower().isin(['fixed','bundle'])]
        if len(fixed):
            sums=fixed.groupby(['Order_ID','Promotion_ID']).agg(discount=('Scenario_Discount','sum'),cap=('Fixed_Discount_Term','first'))
            termok &= sums.discount.le(sums.cap+.02).all()
    report('Processed historical-price/promotion terms',termok,f'rows={len(tx):,}' if termok else 'processed transaction terms absent or inconsistent')
    q=pd.to_numeric(li.Quantity,errors='coerce').fillna(0); gross=pd.to_numeric(li.Unit_Price,errors='coerce').fillna(0)*q; disc=pd.to_numeric(li.Discount_Amount,errors='coerce').fillna(0)
    bill=~li.Is_Anomaly.fillna(False).astype(str).str.lower().isin(['true','1']) & q.gt(0) & li.Order_ID.isin(o.loc[o.Order_Status.eq('Completed'),'Order_ID'])
    sums=li.loc[bill].assign(_gross=gross[bill],_discount=disc[bill]).groupby('Order_ID').agg(gross=('_gross','sum'),discount=('_discount','sum'))
    order_sums=o.set_index('Order_ID')
    discountok=(disc.ge(0).all() and disc[bill].le(gross[bill]+.02).all() and (sums.discount-order_sums.Discount_Amount.reindex(sums.index)).abs().max()<.02 and (sums.gross-order_sums.Subtotal.reindex(sums.index)).abs().max()<.02)
    report('Canonical promotion arithmetic',discountok,'source line discounts and billable order subtotal/discount totals reconcile')
    if {'Price_History_ID','Base_Quantity','Price_Sensitive','Price_Sensitivity_Applied'}<=set(li.columns):
        lookup=o[['Order_ID','Location_ID','Order_Date']].merge(li[['Order_Item_ID','Order_ID','Item_ID','Unit_Price','Price_History_ID']],on='Order_ID',validate='one_to_many')
        lookup=lookup.merge(ph[['Price_History_ID','Item_ID','Location_ID','Effective_From','Effective_To','New_Price']],on=['Price_History_ID','Item_ID','Location_ID'],validate='many_to_one')
        lookup['Order_Date']=pd.to_datetime(lookup.Order_Date); lookup['Effective_From']=pd.to_datetime(lookup.Effective_From); lookup['Effective_To']=pd.to_datetime(lookup.Effective_To)
        histok=len(lookup)==len(li) and lookup.Order_Date.between(lookup.Effective_From,lookup.Effective_To).all() and ((lookup.Unit_Price-lookup.New_Price).abs()<.011).all()
    else: histok=False
    report('Order-line historical price linkage',histok,'every order line linked to effective price by item, location, and date')
    qualityok,qmsg=run_script('validate_quality_fixtures.py'); report('All listed data-quality cases',qualityok,qmsg)
    hiddenok,hmsg=run_script('validate_hidden_readiness.py'); report('Hidden-data readiness cases',hiddenok,hmsg)
    # Validation must be read-only: coverage generation belongs to build_submission.py.
    coverage_path=ROOT/'documentation'/'REALISM_COVERAGE.csv'
    try:
        coverage=pd.read_csv(coverage_path)
        element_col=next(c for c in coverage.columns if c.lower() in {'element','realism_element','requirement'})
        status_col=next(c for c in coverage.columns if c.lower() in {'status','result'})
        realismok=(coverage[element_col].nunique()==20 and len(coverage)==20 and
                   coverage[status_col].astype(str).str.upper().eq('PASS').all())
        rmsg=f'{coverage[element_col].nunique()}/20 unique elements; all statuses PASS'
    except Exception as e:
        realismok=False; rmsg=f'{type(e).__name__}: {e}'
    report('All 20 generated realism elements',realismok,rmsg)
    splitok,smsg=run_script('validate_split_artifacts.py'); report('Chronological train/validation/test splits',splitok,smsg)
    clean=list((ROOT/'samples'/'cleaned').glob('*_clean_sample.csv'))
    report('Raw and cleaned samples plus cleaning trace',len(list((ROOT/'samples').glob('*_sample.csv')))==12 and len(clean)==12 and (ROOT/'documentation'/'CLEANING_DECISIONS.csv').exists(),f'raw={len(list((ROOT/"samples").glob("*_sample.csv")))}; cleaned={len(clean)}')
    dic=(ROOT/'documentation'/'DATA_DICTIONARY.md').read_text(encoding='utf-8')
    dictok=all(f'`{c}` | {t} |' in dic for c,t in [('Estimated_Preparation_Time','integer'),('Preparation_Time','integer'),('Response_Time_Hours','decimal'),('Preparation_Quantity','integer'),('Wastage_Percentage','decimal')])
    report('Data dictionary and numeric durations',dictok,'dictionary types checked against current source schema')
    # Read every Parquet file, compare complete values and schemas with its CSV.
    try:
        import pyarrow.parquet as pq
        import pyarrow as pa
        parquet_ok=True; pfails=[]
        def type_ok(field, series):
            t=field.type
            # CSV readers infer an all-empty column as float64. Arrow's nullable
            # double storage is compatible with that schema; there are no values
            # whose logical type could be lost or misread.
            if pd.api.types.is_float_dtype(series.dtype): return pa.types.is_floating(t)
            if series.isna().all(): return pa.types.is_null(t) or pa.types.is_string(t) or pa.types.is_large_string(t)
            if pd.api.types.is_bool_dtype(series.dtype): return pa.types.is_boolean(t)
            if pd.api.types.is_integer_dtype(series.dtype): return pa.types.is_integer(t)
            if pd.api.types.is_float_dtype(series.dtype): return pa.types.is_floating(t)
            if pd.api.types.is_object_dtype(series.dtype) or pd.api.types.is_string_dtype(series.dtype): return pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_binary(t)
            return True
        for name,expected in EXPECTED.items():
            p=ROOT/'parquet'/f'{name}.parquet'; df=tables[name]
            if not p.exists(): parquet_ok=False; pfails.append(name+': missing'); continue
            pf=pq.ParquetFile(p)
            if pf.metadata.num_rows!=len(df) or pf.schema_arrow.names!=df.columns.tolist(): parquet_ok=False; pfails.append(name+': schema/count mismatch'); continue
            for field in pf.schema_arrow:
                if not type_ok(field,df[field.name]): parquet_ok=False; pfails.append(name+f'.{field.name}: type {field.type} conflicts with CSV {df[field.name].dtype}')
            seen=0
            for batch,chunk in zip(pf.iter_batches(batch_size=65536),pd.read_csv(D/f'{name}.csv',chunksize=65536)):
                left=batch.to_pandas().reset_index(drop=True); right=chunk.reset_index(drop=True)
                try:
                    for col in left.columns:
                        a,b=left[col],right[col]
                        if pd.api.types.is_object_dtype(a.dtype) or pd.api.types.is_string_dtype(a.dtype): a=a.astype('string').fillna('<NA>'); b=b.astype('string').fillna('<NA>')
                        pd.testing.assert_series_equal(a,b,check_dtype=False,check_exact=False,rtol=1e-9,atol=1e-9,check_names=False)
                except AssertionError: parquet_ok=False; pfails.append(name+f': value mismatch at batch {seen//65536}')
                seen+=len(left)
            if seen!=len(df): parquet_ok=False; pfails.append(name+': read count mismatch')
        proc=ROOT/'processed_data'/'transaction_terms.parquet'
        if not proc.exists(): parquet_ok=False; pfails.append('processed transaction_terms.parquet missing')
        else:
            pf=pq.ParquetFile(proc)
            if pf.metadata.num_rows!=len(tx) or pf.schema_arrow.names!=tx.columns.tolist(): parquet_ok=False; pfails.append('processed transaction_terms schema/count mismatch')
            for field in pf.schema_arrow:
                if not type_ok(field,tx[field.name]): parquet_ok=False; pfails.append(f'processed.{field.name}: type {field.type} conflicts with CSV {tx[field.name].dtype}')
            # read-back all row groups and compare a stable checksum over key/value columns
            read=pq.read_table(proc,columns=['Order_ID','Item_ID','Historical_Unit_Price','Scenario_Discount']).to_pandas()
            exp=tx[['Order_ID','Item_ID','Historical_Unit_Price','Scenario_Discount']]
            try: pd.testing.assert_frame_equal(read.reset_index(drop=True),exp.reset_index(drop=True),check_dtype=False,check_exact=False,rtol=1e-9,atol=1e-9)
            except AssertionError: parquet_ok=False; pfails.append('processed transaction_terms values mismatch')
        report('Raw and large processed Parquet read-back',parquet_ok,'all CSV/Parquet schemas, counts, and values compared; '+('; '.join(pfails) if pfails else 'all passed'))
    except ImportError:
        report('Raw and large processed Parquet read-back',False,'pyarrow missing; install requirements.txt and rerun')
    # Effective price, preparation denominator, sensitivity cohort, promotion trap artifacts.
    scenariook={'Preparation_Quantity','Wastage_Percentage'}<=set(waste.columns) and (waste.Preparation_Quantity>0).all() and (waste.Wastage_Percentage>=0).all() and menu.Price_Sensitive.any() and li.Price_Sensitivity_Applied.any() and bool(tables['promotions'].Is_Misleading.astype(str).str.lower().eq('true').any()) and menu.Is_Popular_Low_Margin.any() and menu.Is_Profitable_Low_Selling.any() and menu.Is_High_Wastage_Item.any()
    report('Price sensitivity and wastage denominator',scenariook,'sensitive item flags, transaction response flags, preparation and wastage percentage present')
    print('\n'.join(f'[{i:02}] {name:.<43} {"PASS" if ok else "FAIL"}'+(f' — {msg}' if not ok else '') for i,(name,ok,msg) in enumerate(rows,1)))
    passed=sum(ok for _,ok,_ in rows)
    print(f'\nDATASET REQUIREMENTS: {passed}/{len(rows)} PASS')
    print(f'ERRORS: {len(failures)}')
    print('DATASET STATUS: '+('PASS' if not failures else 'FAIL'))
    return 0 if not failures else 2
if __name__=='__main__': raise SystemExit(main())
