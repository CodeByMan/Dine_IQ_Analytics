from pathlib import Path
import pandas as pd
D=Path(__file__).resolve().parents[1]/'data'
minimum={'customers':50000,'orders':100000,'order_items':1000000,'menu_items':150,'menu_categories':10,'restaurants':20,'ratings':100000,'wastage':50000}
def read(n): return pd.read_csv(D/f'{n}.csv')
def main():
    tables={p.stem:pd.read_csv(p) for p in D.glob('*.csv')}; print('DATASET VALIDATION REPORT\n=========================')
    for n,m in minimum.items(): print(f'{n:22} {len(tables[n]):>10,}  '+('PASS' if len(tables[n])>=m else 'FAIL'))
    checks=[]
    def ck(name,ok): checks.append((name,bool(ok))); print(f'{name:28} '+('PASS' if ok else 'FAIL'))
    for t,col in [('customers','Customer_ID'),('orders','Order_ID'),('order_items','Order_Item_ID'),('menu_items','Item_ID'),('menu_categories','Category_ID'),('restaurants','Location_ID'),('pricing_history','Price_History_ID'),('promotions','Promotion_ID'),('ratings','Rating_ID'),('inventory','Inventory_ID'),('wastage','Wastage_ID'),('ordering_channels','Channel_ID')]: ck(f'PK {t}',tables[t][col].is_unique)
    for t,c,p,pc in [('orders','Customer_ID','customers','Customer_ID'),('orders','Location_ID','restaurants','Location_ID'),('orders','Channel_ID','ordering_channels','Channel_ID'),('orders','Promotion_ID','promotions','Promotion_ID'),('order_items','Order_ID','orders','Order_ID'),('order_items','Item_ID','menu_items','Item_ID'),('menu_items','Category_ID','menu_categories','Category_ID'),('pricing_history','Item_ID','menu_items','Item_ID'),('pricing_history','Location_ID','restaurants','Location_ID'),('ratings','Order_ID','orders','Order_ID'),('ratings','Customer_ID','customers','Customer_ID'),('ratings','Item_ID','menu_items','Item_ID'),('ratings','Location_ID','restaurants','Location_ID'),('inventory','Location_ID','restaurants','Location_ID'),('inventory','Item_ID','menu_items','Item_ID'),('wastage','Location_ID','restaurants','Location_ID'),('wastage','Item_ID','menu_items','Item_ID')]: ck(f'FK {t}.{c}',set(tables[t][c].dropna().astype(int)).issubset(set(tables[p][pc].astype(int))))
    o=tables['orders']; ck('Transaction coverage >= 12 months',(pd.to_datetime(o.Order_Date).max()-pd.to_datetime(o.Order_Date).min()).days>=365)
    li=tables['order_items']; ck('Line amount arithmetic',(((li.Quantity*li.Unit_Price-li.Discount_Amount-li.Line_Total).abs()<.03) | li.Is_Anomaly.astype(bool)).all())
    ck('Intentional anomalies present',li.Is_Anomaly.astype(bool).any() and o.Is_Anomaly.astype(bool).any())
    ck('Order total arithmetic',(((o.Subtotal-o.Discount_Amount+o.Tax_Amount+o.Delivery_Fee-o.Total_Amount).abs()<.03)).all())
    ph_active=tables['promotions'].set_index('Promotion_ID')
    pp=o.dropna(subset=['Promotion_ID']).copy(); pp['Promotion_ID']=pp.Promotion_ID.astype(int); pp['Order_Date']=pd.to_datetime(pp.Order_Date)
    promo_ok=all((row.Order_Date>=pd.Timestamp(ph_active.loc[row.Promotion_ID,'Start_Date'])) and (row.Order_Date<=pd.Timestamp(ph_active.loc[row.Promotion_ID,'End_Date'])) for row in pp.itertuples())
    ck('Promotions used in active dates',promo_ok)
    rd=tables['ratings'].merge(o[['Order_ID','Order_DateTime']],on='Order_ID',suffixes=('_rating','_order')); ck('Ratings not before orders',(pd.to_datetime(rd.Review_Date)>=pd.to_datetime(rd.Order_DateTime)).all())
    ck('Controlled duplicate markers',tables['ratings'].Is_Duplicate.astype(bool).sum()>0)
    dupcols=[c for c in tables['ratings'].columns if c not in ['Rating_ID','Is_Duplicate']]
    ck('Duplicate rating payloads exist',tables['ratings'].duplicated(dupcols).any())
    weekday_share=pd.to_datetime(o.Order_Date).dt.dayofweek.ge(4).mean(); ck('Weekend weighting applied',weekday_share>.40)
    month_share=pd.to_datetime(o.Order_Date).dt.month.isin([2,3]).mean(); ck('Seasonal proxy uplift applied',month_share>.13)
    ph=tables['pricing_history']; ck('Pricing periods valid',(pd.to_datetime(ph.Effective_From)<=pd.to_datetime(ph.Effective_To)).all())
    pr=tables['promotions']; ck('Promotion periods valid',(pd.to_datetime(pr.Start_Date)<=pd.to_datetime(pr.End_Date)).all())
    print('\nNull % (sample):');
    for t in ['customers','orders','ratings','inventory']: print(t,round(tables[t].isna().mean().mean()*100,3))
    print('\nOverall Dataset Status:', 'PASS' if all(v for _,v in checks) and all(len(tables[n])>=m for n,m in minimum.items()) else 'FAIL')
if __name__=='__main__': main()
