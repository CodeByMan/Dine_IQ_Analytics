#!/usr/bin/env python3
"""Reproducible, synthetic Pakistani restaurant data generator. No existing dataset is reused."""
from pathlib import Path
import argparse, csv, random
import numpy as np
import pandas as pd
SEED=42
NUM_CUSTOMERS=62000; NUM_ORDERS=125000; NUM_ORDER_ITEMS=1250000; NUM_MENU_ITEMS=210; NUM_LOCATIONS=27; NUM_RATINGS=120000; NUM_WASTAGE_RECORDS=65000
END=pd.Timestamp('2026-09-24'); START=END-pd.DateOffset(months=24)
ROOT=Path(__file__).resolve().parents[1]; DATA=ROOT/'data'
rng=np.random.default_rng(SEED)
def put(name,rows): pd.DataFrame(rows).to_csv(DATA/f'{name}.csv',index=False)
def randdates(n,a=START,b=END): return a+pd.to_timedelta(rng.integers(0,max(1,(b-a).days),n),unit='D')
def generate_channels():
    return put('ordering_channels',[{'Channel_ID':i+1,'Channel_Name':n,'Description':d} for i,(n,d) in enumerate([('In-Store','Dine-in or counter'),('Restaurant Website','Direct web orders'),('Mobile App','First-party app'),('Food Delivery Platform','Third-party delivery'),('Phone','Telephone orders'),('WhatsApp','WhatsApp orders'),('Walk-In','Takeaway walk-in'),('Drive-Thru','Drive-through where available')])])
def generate_categories():
    names=['Pakistani Main Course','BBQ','Biryani & Rice','Burgers','Pizza','Fried Chicken','Chinese','Fast Food','Desi Breakfast','Karahi','Handi','Seafood','Desserts','Beverages','Tea & Coffee','Rolls & Wraps','Salads','Kids Meals']; put('menu_categories',[{'Category_ID':i+1,'Category_Name':n,'Description':f'{n} menu selection'} for i,n in enumerate(names)]); return names
CITIES=[('Karachi','Sindh',['Clifton','DHA','Gulshan-e-Iqbal','North Nazimabad','PECHS','Bahadurabad','Saddar','Gulistan-e-Johar']),('Lahore','Punjab',['Gulberg','DHA','Johar Town','Model Town','Bahria Town','Wapda Town']),('Islamabad','Islamabad Capital Territory',['F-6','F-7','F-8','G-9','Blue Area']),('Rawalpindi','Punjab',['Saddar Rawalpindi','Bahria Town']),('Faisalabad','Punjab',['D Ground','People’s Colony']),('Multan','Punjab',['Cantt','Gulgasht']),('Peshawar','Khyber Pakhtunkhwa',['University Road','Hayatabad']),('Quetta','Balochistan',['Jinnah Road','Satellite Town']),('Hyderabad','Sindh',['Latifabad','Qasimabad']),('Gujranwala','Punjab',['Satellite Town']),('Sialkot','Punjab',['Cantt']),('Bahawalpur','Punjab',['Model Town A']),('Sukkur','Sindh',['Military Road']),('Abbottabad','Khyber Pakhtunkhwa',['Mandian']),('Sargodha','Punjab',['Satellite Town'])]
def generate_restaurants():
    rows=[]
    for i in range(NUM_LOCATIONS):
        city,prov,areas=CITIES[i%len(CITIES)]; area=areas[i%len(areas)]
        rows.append(dict(Location_ID=i+1,Restaurant_Name=f"{['Dastarkhwan','Karachi Grill','Lahori Zaika','Chai Junction','Desi Courtyard','Spice Route'][i%6]} {area}",City=city,Province=prov,Area=area,Latitude=round(24.8+(i%11)*.7+rng.normal(0,.08),5),Longitude=round(67+(i%9)*.7+rng.normal(0,.08),5),Opening_Date=(START-pd.Timedelta(days=int(rng.integers(60,2200)))).date(),Restaurant_Type=['Quick Service','Family Dining','Premium Casual','Cafe'][i%4],Seating_Capacity=int(rng.integers(35,180)),Delivery_Radius_KM=round(float(rng.uniform(3,12)),1),Average_Rating=round(float(rng.uniform(3.5,4.8)),2),Manager_ID=f'MGR{i+1:04d}',Status='Active',Volume_Index=round(float(rng.uniform(.65,1.45)),2),Service_Quality=round(float(rng.uniform(-.3,.4)),2)))
    put('restaurants',rows); return rows
FIRST='Muhammad Ali Usman Hamza Ahmed Bilal Hassan Ayesha Fatima Hira Sana Zainab Maryam Omar Daniyal Abdullah Laiba Iqra Saad'.split(); LAST='Ahmed Khan Sheikh Malik Raza Hussain Iqbal Qureshi Butt Chaudhry Siddiqui Mirza Shah Akhtar Javed'.split()
def generate_customers(restaurants):
    segments=['New Customers','Regular Customers','High-Value Customers','Price-Sensitive Customers','Occasional Customers','Churn-Risk Customers','Churned Customers','Loyal Customers','Promotion-Driven Customers']; probs=[.08,.21,.07,.16,.17,.1,.08,.08,.05]
    seg=rng.choice(segments,NUM_CUSTOMERS,p=probs); target=rng.integers(0,NUM_LOCATIONS,NUM_CUSTOMERS); signup=randdates(NUM_CUSTOMERS,START-pd.Timedelta(days=365),END-pd.Timedelta(days=40)); rows=[]
    for i in range(NUM_CUSTOMERS):
        r=restaurants[target[i]]
        rows.append(dict(Customer_ID=i+1,Customer_Name=f'{random.choice(FIRST)} {random.choice(LAST)}',Age=int(rng.integers(18,67)) if rng.random()>.035 else '',Gender=rng.choice(['Female','Male','Prefer not to say']),City=r['City'],Area=r['Area'],Signup_Date=signup[i].date(),Customer_Segment=seg[i],Preferred_Channel=rng.choice(['Mobile App','Food Delivery Platform','Restaurant Website','In-Store','WhatsApp','Phone']),Preferred_Cuisine=rng.choice(['Pakistani','BBQ','Chinese','Fast Food','Continental','Desi']),Loyalty_Tier=rng.choice(['Standard','Silver','Gold','Platinum'],p=[.57,.23,.15,.05]),Income_Band=rng.choice(['Entry','Lower-Middle','Middle','Upper-Middle','High']) if rng.random()>.04 else '',Acquisition_Source=rng.choice(['Social Media','Referral','Walk-In','Search','Delivery Platform','University Offer']),Is_Active=seg[i]!='Churned Customers',Last_Order_Date='',Total_Orders=0,Lifetime_Value=0,Average_Order_Value=0,Churn_Status=seg[i] in ['Churn-Risk Customers','Churned Customers'],Synthetic_Contact=f'cust{i+1:06d}@example.invalid',Is_Duplicate=False))
    put('customers',rows); return rows,seg
BASE=[('Chicken Biryani','Biryani & Rice',520,260,False),('Beef Biryani','Biryani & Rice',680,390,False),('Mutton Biryani','Biryani & Rice',980,620,False),('Chicken Karahi','Karahi',1450,880,False),('Mutton Karahi','Karahi',2450,1640,False),('Chicken Handi','Handi',1550,940,False),('Chicken Tikka','BBQ',620,340,False),('Malai Boti','BBQ',780,420,False),('Seekh Kebab','BBQ',430,210,False),('Reshmi Kebab','BBQ',560,300,False),('Beef Burger','Burgers',690,340,False),('Zinger Burger','Burgers',620,310,False),('Chicken Burger','Burgers',540,250,False),('Chicken Shawarma','Rolls & Wraps',390,170,False),('Beef Shawarma','Rolls & Wraps',490,240,False),('Chicken Roll','Rolls & Wraps',360,155,False),('Chicken Cheese Roll','Rolls & Wraps',430,205,False),('BBQ Platter','BBQ',2650,1480,False),('Chicken Wings','Fried Chicken',720,350,False),('Fried Chicken','Fried Chicken',680,390,False),('Chicken Chow Mein','Chinese',760,390,False),('Chicken Manchurian','Chinese',890,470,False),('Egg Fried Rice','Chinese',420,180,True),('Chicken Fried Rice','Chinese',650,300,False),('Margherita Pizza','Pizza',1090,440,True),('Chicken Fajita Pizza','Pizza',1690,790,False),('Chicken Tikka Pizza','Pizza',1590,730,False),('Pepperoni Pizza','Pizza',1790,910,False),('Beef Pizza','Pizza',1890,960,False),('Alfredo Pasta','Fast Food',990,430,False),('Chicken Pasta','Fast Food',890,390,False),('Loaded Fries','Fast Food',590,240,True),('French Fries','Fast Food',320,110,True),('Gulab Jamun','Desserts',280,90,True),('Kheer','Desserts',320,125,True),('Brownie','Desserts',390,160,True),('Chocolate Cake','Desserts',520,245,True),('Kunafa','Desserts',690,330,True),('Mango Lassi','Beverages',290,105,True),('Sweet Lassi','Beverages',220,75,True),('Salted Lassi','Beverages',200,65,True),('Doodh Patti','Tea & Coffee',160,45,True),('Kashmiri Chai','Tea & Coffee',240,85,True),('Green Tea','Tea & Coffee',120,30,True),('Fresh Lime','Beverages',190,55,True),('Soft Drink','Beverages',150,85,True),('Mineral Water','Beverages',100,42,True)]
def generate_menu(cats):
    variants=['Regular','Large','Family','Single','Half','Full','Combo','Platter']; mults=[1,1.4,2.7,.86,.72,1.6,1.55,2.3]; rows=[]
    for i in range(NUM_MENU_ITEMS):
        name,cat,p,c,veg=BASE[i%len(BASE)]; v=variants[(i//len(BASE))%8]; mul=mults[(i//len(BASE))%8]; rows.append(dict(Item_ID=i+1,Item_Name=name if i<len(BASE) else f'{v} {name}',Category_ID=cats.index(cat)+1,Cuisine_Type='Pakistani' if cat not in ['Pizza','Burgers','Chinese','Fast Food','Tea & Coffee'] else cat,Description=f'{v} serving of {name}, prepared fresh to order.',Size=v,Selling_Price=round(p*mul/10)*10,Ingredient_Cost=round(c*mul/10)*10,Estimated_Preparation_Time=int(rng.integers(8,35)),Calories=int(rng.integers(120,1250)),Is_Vegetarian=veg,Is_Spicy=cat in ['Karahi','BBQ','Pakistani Main Course'],Is_Active=True,Launch_Date=(START-pd.Timedelta(days=int(rng.integers(30,1300)))).date(),Discontinued_Date='',Base_Cost=round(c*mul/10)*10,Popularity_Index=round(float(rng.lognormal(0,.65)),3),Perishability=round(float(rng.uniform(.04,.22)),3)))
    put('menu_items',rows); return rows
def generate_pricing(menu,restaurants):
    rows=[]; k=0
    for m in menu:
        for r in restaurants:
            base=m['Selling_Price']*rng.uniform(.91,1.16); n=int(rng.integers(3,6)); ds=pd.date_range(START,END,periods=n+1)
            for j in range(n):
                new=round(base*(1+.045*j)/10)*10; old=round(base*(1+.045*(j-1))/10)*10 if j else round(base*.94/10)*10; k+=1; rows.append(dict(Price_History_ID=k,Item_ID=m['Item_ID'],Location_ID=r['Location_ID'],Effective_From=ds[j].date(),Effective_To=(ds[j+1]-pd.Timedelta(days=1)).date(),Previous_Price=old,New_Price=new,Price_Change_Percentage=round(100*(new-old)/old,2),Reason=rng.choice(['Ingredient Cost Increase','Inflation','Seasonal Pricing','Supplier Change','Competitor Adjustment','Menu Repricing']),Approved_Date=(ds[j]-pd.Timedelta(days=int(rng.integers(0,10)))).date()))
    put('pricing_history',rows)
def generate_promotions(restaurants,cats):
    names=['Friday Family Deal','Ramadan Iftar Deal','Eid Family Feast','Weekend Burger Deal','Buy 1 Get 1 Free','Student Discount','Lunch Special','Karachi Food Festival','Independence Day Offer','New Customer Discount','Free Delivery Weekend','Winter BBQ Deal','Pizza Tuesday','High Basket Threshold Deal','App Loyalty Bonus']; rows=[]
    for i in range(40):
        a=START+pd.Timedelta(days=int(rng.integers(0,(END-START).days-100))); b=min(END,a+pd.Timedelta(days=int(rng.integers(10,70))))
        rows.append(dict(Promotion_ID=i+1,Promotion_Name=names[i%len(names)]+(f' {i//len(names)+1}' if i>=len(names) else ''),Promotion_Type=rng.choice(['Percentage','Fixed','Bundle','Free Delivery']),Discount_Percentage=int(rng.choice([0,5,10,15,20,25])),Fixed_Discount=int(rng.choice([0,100,150,250,400])),Minimum_Order_Value=int(rng.choice([0,799,1199,1999,2999,4999])),Start_Date=a.date(),End_Date=b.date(),Applicable_Locations='ALL' if i%3 else '|'.join(str(int(x)) for x in rng.choice([r['Location_ID'] for r in restaurants],5,replace=False)),Applicable_Categories='ALL' if i%4 else str(int(rng.integers(1,len(cats)+1))),Usage_Limit=int(rng.integers(500,12000)),Promotion_Status='Expired' if b<END else 'Active',Is_Misleading=i==13))
    put('promotions',rows); return rows
def generate_orders(customers,segments,restaurants,promos,menu):
    cw=np.array([2.8 if x in ['High-Value Customers','Loyal Customers'] else 1.6 if x in ['Regular Customers','Promotion-Driven Customers'] else .55 if x=='Churned Customers' else 1.0 for x in segments]); ci=rng.choice(len(customers),NUM_ORDERS,p=cw/cw.sum()); lw=np.array([r['Volume_Index'] for r in restaurants]); li=rng.choice(len(restaurants),NUM_ORDERS,p=lw/lw.sum()); days=pd.date_range(START,END,freq='D'); dw=np.ones(len(days)); dw[np.array(days.dayofweek)>=4]*=1.28; dw[np.isin(np.array(days.month),[2,3])]*=1.16; od=np.array(rng.choice(days.values,size=NUM_ORDERS,p=dw/dw.sum())); churn=np.array([segments[i]=='Churned Customers' for i in ci]); od[churn]=np.array(randdates(churn.sum(),START,START+pd.Timedelta(days=365))); od=np.maximum(od,np.array([customers[x]['Signup_Date'] for x in ci],dtype='datetime64[ns]')); od=np.maximum(od,np.array([restaurants[x]['Opening_Date'] for x in li],dtype='datetime64[ns]')); hours=rng.choice([8,9,10,12,13,14,17,18,19,20,21,22,23,0,1],NUM_ORDERS,p=np.array([.025,.025,.015,.09,.075,.035,.06,.1,.12,.15,.12,.08,.06,.025,.015])/0.995); dt=od+pd.to_timedelta(hours,unit='h')+pd.to_timedelta(rng.integers(0,60,NUM_ORDERS),unit='m'); status=rng.choice(['Completed','Cancelled','Refunded','Failed','Pending'],NUM_ORDERS,p=[.91,.045,.018,.017,.01]); channels=rng.choice(np.arange(1,9),NUM_ORDERS,p=[.13,.08,.24,.28,.07,.08,.1,.02]); promotion=np.zeros(NUM_ORDERS,dtype=int)
    for pr in promos:
        active=(od>=np.datetime64(pr['Start_Date']))&(od<=np.datetime64(pr['End_Date']))&(rng.random(NUM_ORDERS)<.02)
        promotion[active]=pr['Promotion_ID']
    counts=np.maximum(2,rng.poisson(10,NUM_ORDERS)); counts[promotion>0]+=1; diff=NUM_ORDER_ITEMS-counts.sum(); counts[:abs(diff)]+=1 if diff>0 else -1
    subs=np.zeros(NUM_ORDERS); discounts=np.zeros(NUM_ORDERS); first_item=np.zeros(NUM_ORDERS,dtype=int); costs=np.zeros(NUM_ORDERS); pop=np.array([m['Popularity_Index'] for m in menu]); pop/=pop.sum(); rowid=1
    with open(DATA/'order_items.csv','w',newline='',encoding='utf-8') as f:
        w=csv.writer(f); w.writerow(['Order_Item_ID','Order_ID','Item_ID','Quantity','Unit_Price','Discount_Amount','Line_Total','Item_Cost','Gross_Profit','Special_Request','Preparation_Time','Item_Status','Is_Anomaly','Is_Duplicate'])
        for o,n in enumerate(counts):
            for z in range(int(n)):
                ix=int(rng.choice(len(menu),p=pop));
                if z==0: first_item[o]=ix+1
                m=menu[ix]; q=int(rng.choice([1,1,1,2,2,3]));
                if rowid%4000==0: q=10
                price=m['Selling_Price']*rng.uniform(.9,1.16); dis=price*q*(.10 if promotion[o] else 0); total=round(price*q-dis,2); cost=m['Ingredient_Cost']*q; subs[o]+=price*q; discounts[o]+=dis; costs[o]+=cost; bad=rowid%1800==0 or rowid%4000==0
                w.writerow([rowid,o+1,ix+1,-1 if bad else q,round(price,2),round(dis,2),-abs(total) if bad else total,round(cost,2),round(total-cost,2),rng.choice(['','Extra spicy','Less spicy','No onions','Sauce on side']),int(m['Estimated_Preparation_Time']+rng.integers(-3,10)),'Completed' if status[o]=='Completed' or (status[o]=='Cancelled' and rowid%19==0) else status[o],bad,False]); rowid+=1
    rows=[]
    for i in range(NUM_ORDERS):
        sub=subs[i]; disc=round(discounts[i],2); tax=round(max(0,sub-disc)*.05,2); fee=0 if channels[i] in [1,7,8] else int(rng.choice([0,80,120,150])); total=round(sub-disc+tax+fee,2)
        rows.append(dict(Order_ID=i+1,Customer_ID=int(ci[i]+1),Location_ID=int(li[i]+1),Promotion_ID=int(promotion[i]) or '',Channel_ID=int(channels[i]),Order_Date=pd.Timestamp(od[i]).date(),Order_Time=pd.Timestamp(dt[i]).strftime('%H:%M:%S'),Order_DateTime=pd.Timestamp(dt[i]),Order_Status=status[i],Payment_Method=rng.choice(['Cash','Card','JazzCash','Easypaisa','Bank Transfer']),Delivery_Type='Dine-in' if channels[i] in [1,7,8] else ('Delivery' if rng.random()>.015 else ''),Subtotal=round(sub,2),Discount_Amount=disc,Tax_Amount=tax,Delivery_Fee=fee,Total_Amount=total,Order_Source='Direct' if channels[i]!=4 else 'Third-Party',Cancellation_Reason=rng.choice(['Customer changed mind','Long wait','Address issue','Restaurant capacity']) if status[i]=='Cancelled' else '',Is_Anomaly=i%2300==0,Is_Duplicate=False))
    put('orders',rows)
    sums=np.zeros(len(customers)); norders=np.zeros(len(customers),dtype=int); last=[None]*len(customers)
    for z,row in enumerate(rows):
        ix=row['Customer_ID']-1; norders[ix]+=1; last[ix]=max(last[ix] or row['Order_Date'],row['Order_Date']); sums[ix]+=row['Total_Amount'] if row['Order_Status']=='Completed' else 0
    for z,c in enumerate(customers): c['Total_Orders']=int(norders[z]); c['Lifetime_Value']=round(float(sums[z]),2); c['Average_Order_Value']=round(float(sums[z]/max(1,norders[z])),2); c['Last_Order_Date']=last[z] or ''
    put('customers',customers); return ci,li,dt,status,first_item
REVIEWS=['Biryani was aromatic and rice perfectly cooked.','Great taste and generous portion for the price.','Delivery was late and food arrived cold.','Fresh ingredients, good packaging and polite service.','Spice level was higher than expected.','Excellent BBQ flavour, though the portion was small.','Food was average; value for money could improve.','Order was delayed but staff handled the issue well.','Tea was hot and snack was crisp.','Packaging leaked and meal was lukewarm.']
def generate_ratings(customers,restaurants,order_info,menu):
    ci,li,dt,status,item_for=order_info; eligible=np.where(status=='Completed')[0]; picks=rng.choice(eligible,NUM_RATINGS,replace=True); rows=[]
    for i,o in enumerate(picks):
        rating=int(rng.choice([1,2,3,4,5],p=[.035,.055,.15,.36,.4])); text=random.choice(REVIEWS)
        if i%997==0: rating,text=1,'Loved it, excellent taste and service.'
        if i%1103==0: rating,text=5,'Arrived cold and delivery took too long.'
        row=dict(Rating_ID=i+1,Customer_ID=int(ci[o]+1),Order_ID=int(o+1),Item_ID=int(item_for[o]),Location_ID=int(li[o]+1),Rating=rating,Review_Text='' if i%45==0 else text,Review_Date=dt[o]+pd.Timedelta(hours=int(rng.integers(1,96))),Sentiment='Positive' if rating>=4 else 'Negative' if rating<=2 else 'Neutral',Is_Verified=True,Response_Time_Hours=round(float(rng.uniform(1,72)),1),Is_Anomaly=i%997==0 or i%1103==0,Is_Duplicate=False)
        if i>0 and i%2000==0: row=rows[-1].copy(); row['Rating_ID']=i+1; row['Is_Duplicate']=True
        rows.append(row)
    put('ratings',rows)
def generate_inventory_wastage(restaurants,menu):
    wast=[]; inv=[]; dr=pd.date_range(START,END,freq='D'); pop=np.array([m['Popularity_Index'] for m in menu]);
    for j in range(NUM_WASTAGE_RECORDS):
        loc=int(rng.integers(1,len(restaurants)+1)); item=int(rng.choice(np.arange(1,len(menu)+1),p=pop/pop.sum())); m=menu[item-1]; date=dr[int(rng.integers(0,len(dr)))]; q=round(float(rng.gamma(1.2,.45)*(1.5 if m['Perishability']>.15 else .7)),2); reason=rng.choice(['Expired','Overproduction','Burnt','Damaged','Wrong Order','Customer Return','Spoilage','Preparation Error','Storage Issue','Quality Rejection'])
        wast.append(dict(Wastage_ID=j+1,Location_ID=loc,Item_ID=item,Wastage_Date=date.date(),Quantity_Wasted=q,Unit_Cost=m['Ingredient_Cost'],Total_Wastage_Cost=round(q*m['Ingredient_Cost'],2),Wastage_Reason=reason,Shift=rng.choice(['Breakfast','Lunch','Evening','Dinner','Late Night']),Recorded_By=f'STAFF{int(rng.integers(1,700)):04d}',Preventable_Flag=reason not in ['Customer Return','Quality Rejection'],Is_Anomaly=j%1500==0))
    k=0
    for r in restaurants:
        for d in pd.date_range(START,END,freq='7D'):
            for item in rng.choice(np.arange(1,len(menu)+1),12,replace=False):
                k+=1; sold=int(rng.poisson(20*menu[item-1]['Popularity_Index']*r['Volume_Index'])); op=int(rng.integers(10,100)); pur=int(rng.integers(0,80)); adj=int(rng.integers(-4,5)) if rng.random()>.02 else ''; close=op+pur-sold+(adj if adj!='' else 0)
                inv.append(dict(Inventory_ID=k,Location_ID=r['Location_ID'],Item_ID=int(item),Inventory_Date=d.date(),Opening_Stock=op,Purchases=pur,Units_Sold=sold,Adjustments=adj,Closing_Stock=close,Reorder_Level=int(rng.integers(8,30)),Wastage_Quantity=round(float(rng.uniform(0,3)),2),Stockout_Flag=close<0,Supplier_ID=f'SUP{int(rng.integers(1,80)):03d}',Is_Anomaly=close<0))
    put('inventory',inv); put('wastage',wast)
def main():
    global rng
    p=argparse.ArgumentParser(); p.add_argument('--seed',type=int,default=SEED); a=p.parse_args(); rng=np.random.default_rng(a.seed); random.seed(a.seed); DATA.mkdir(parents=True,exist_ok=True)
    generate_channels(); cats=generate_categories(); rs=generate_restaurants(); cs,segs=generate_customers(rs); menu=generate_menu(cats); generate_pricing(menu,rs); promo=generate_promotions(rs,cats); oi=generate_orders(cs,segs,rs,promo,menu); generate_ratings(cs,rs,oi,menu); generate_inventory_wastage(rs,menu); print(f'Generated all tables at {DATA} with seed {a.seed}')
if __name__=='__main__': main()
