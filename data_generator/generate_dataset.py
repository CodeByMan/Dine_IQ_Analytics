import os, json, shutil, hashlib, stat
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

if __name__ != "__main__":
    raise RuntimeError("The generator is an explicit offline command, not an importable data loader")

SEED = 42
rng = np.random.default_rng(SEED)
ROOT = os.environ.get("DINEIQ_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if "DINEIQ_ROOT" not in os.environ:
    raise RuntimeError("Generation requires an explicit fresh DINEIQ_ROOT; never regenerate the main dataset")
_generation_root = Path(ROOT).resolve()
for _name in ("raw_data", "processed_data", "parquet_data", "sample_data", "documentation", "reports", "config", "database", "tests"):
    _destination = _generation_root / _name
    if _destination.exists() and (not _destination.is_dir() or any(_destination.iterdir())):
        raise FileExistsError(f"Refusing generation over existing artifacts: {_destination}")
for _name in ("README.md", "AI_USAGE.md", "requirements.txt", "LICENSE"):
    if (_generation_root / _name).exists():
        raise FileExistsError("Generation root contains existing project files; choose a fresh root")
P = {k: os.path.join(ROOT, k) for k in ["raw_data", "processed_data", "parquet_data", "sample_data", "documentation",
                                         "reports", "config", "database", "tests"]}
for v in P.values():
    os.makedirs(v, exist_ok=True)
for sub in ["quarantine", "splits"]:
    os.makedirs(os.path.join(P["processed_data"], sub), exist_ok=True)
for sub in ["tables", "splits"]:
    os.makedirs(os.path.join(P["parquet_data"], sub), exist_ok=True)

CHAIN_NAME = "Zauq Kitchen"
N_CUSTOMERS, N_ORDERS, N_RATINGS, N_WASTAGE = 55_000, 260_000, 105_000, 55_000
START_DATE, END_DATE = datetime(2025, 1, 1), datetime(2025, 12, 31)
TOTAL_DAYS = (END_DATE - START_DATE).days + 1
MAX_VALID_PRICE = 10_000
D = datetime.fromisoformat

# ===========================================================================
# 1. BRANCHES - 22 locations
# ===========================================================================

CHANNELS = ["Dine-in", "Takeaway", "Website/App", "Third-Party Delivery", "Phone Order"]
B = [
 ("Clifton Block 5", "South", "Do Talwar Road", "Seaview", 1.60, "family", 0.85, 32, .50, .70, 1.15, 0.90, +.12, .80, [.38,.16,.18,.24,.04], {"C06":1.3,"C09":1.2,"C10":1.2,"C02":.9}),
 ("Boat Basin, Clifton", "South", "Khayaban-e-Roomi", "Uptown", 1.35, "family", 0.85, 30, .52, .70, 1.10, 0.95, +.10, .80, [.45,.12,.16,.23,.04], {"C03":1.1,"C11":1.3}),
 ("DHA Phase 6", "South", "Bukhari Commercial Lane", "Uptown", 1.70, "family", 0.95, 35, .50, .60, 1.25, 0.85, +.15, .70, [.30,.15,.22,.28,.05], {"C06":1.3,"C09":1.2,"C11":1.2}),
 ("DHA Phase 2 Extension", "South", "Khayaban-e-Ittehad", "Uptown", 1.45, "family", 0.90, 34, .50, .60, 1.20, 0.90, +.12, .70, [.32,.16,.20,.27,.05], {"C06":1.2,"C03":1.1}),
 ("Sea View", "South", "Beach Avenue", "Waterfront", 1.20, "family", 0.75, 28, .48, .90, 1.00, 1.20, -.05, 1.10, [.55,.20,.08,.12,.05], {"C03":1.2,"C06":1.2,"C10":1.3}),
 ("Saddar", "South", "Zaibunnisa Street", "Downtown", 1.40, "office", 0.55, 36, .30, 1.20, 0.90, 1.10, -.08, 1.30, [.50,.22,.04,.08,.16], {"C02":1.4,"C12":1.5,"C01":1.1}),
 ("Gulshan-e-Iqbal Block 13-D", "East", "Main Rashid Minhas Road", "Downtown", 1.65, "family", 0.65, 31, .52, 1.00, 1.10, 1.00, 0.00, 1.00, [.40,.18,.14,.22,.06], {"C03":1.2,"C02":1.1}),
 ("Gulistan-e-Jauhar Block 12", "East", "Abul Hasan Isphahani Road", "Suburban", 1.45, "family", 0.60, 30, .50, 1.10, 1.05, 1.05, -.02, 1.10, [.38,.20,.14,.22,.06], {}),
 ("Johar Chowrangi, University Road", "East", "University Road", "Downtown", 1.30, "student", 0.50, 24, .42, 1.40, 0.85, 1.10, -.05, 1.50, [.30,.20,.20,.26,.04], {"C06":1.5,"C10":1.2,"C03":1.1}),
 ("PECHS Block 2", "East", "Tariq Road Link", "Uptown", 1.25, "mixed", 0.75, 36, .50, .85, 1.05, 0.95, +.05, .90, [.42,.18,.12,.22,.06], {}),
 ("Bahadurabad", "East", "Bahadur Yar Jung Road", "Downtown", 1.10, "student", 0.60, 27, .45, 1.20, 0.95, 1.00, 0.00, 1.30, [.35,.20,.16,.24,.05], {}),
 ("Tariq Road", "East", "Tariq Road Main", "Downtown", 1.50, "office", 0.60, 33, .38, 1.10, 0.95, 1.15, -.04, 1.20, [.48,.22,.06,.14,.10], {"C03":1.4,"C02":1.2}),
 ("North Nazimabad Block H", "Central", "Hyderi Road", "Suburban", 1.30, "family", 0.60, 33, .52, 1.00, 1.10, 1.00, +.02, 1.00, [.40,.20,.10,.20,.10], {"C01":1.2,"C12":1.2}),
 ("Nazimabad No. 2", "Central", "Abbasi Shaheed Road", "Suburban", 1.00, "mixed", 0.45, 35, .45, 1.30, 0.95, 1.15, -.06, 1.40, [.38,.22,.04,.14,.22], {"C02":1.2,"C12":1.3}),
 ("Federal B Area Block 16", "Central", "Karimabad Main", "Suburban", 1.15, "family", 0.55, 32, .50, 1.10, 1.00, 1.00, -.01, 1.10, [.40,.20,.10,.22,.08], {}),
 ("Hyderi Market", "Central", "Hyderi Market Road", "Downtown", 0.90, "mixed", 0.55, 34, .50, 1.10, 1.00, 1.00, 0.00, 1.10, [.40,.22,.08,.20,.10], {}),
 ("North Karachi Sector 11-A", "Central", "Sector 11-A Commercial Lane", "Suburban", 0.85, "mixed", 0.40, 31, .40, 1.50, 0.90, 1.30, -.10, 1.50, [.34,.26,.04,.12,.24], {"C02":1.3,"C06":1.1}),
 ("Shahrah-e-Faisal, Karsaz", "East", "Shahrah-e-Faisal Service Road", "Highway", 1.35, "office", 0.70, 35, .35, .90, 1.00, 0.95, +.05, .90, [.36,.24,.14,.20,.06], {}),
 ("Korangi Industrial Area", "Korangi", "Korangi Road", "Industrial", 0.95, "industrial", 0.35, 33, .18, 1.50, 0.90, 1.35, -.12, 1.60, [.30,.38,.04,.06,.22], {"C02":1.5,"C05":1.3,"C04":1.3}),
 ("Landhi", "Korangi", "Landhi Main Bazaar", "Industrial", 0.75, "industrial", 0.30, 31, .22, 1.60, 0.85, 1.40, -.15, 1.70, [.30,.34,.04,.08,.24], {"C02":1.4,"C04":1.3,"C05":1.2}),
 ("Malir Halt", "Malir", "Malir Halt Road", "Suburban", 0.80, "mixed", 0.40, 32, .45, 1.40, 0.90, 1.20, -.08, 1.40, [.34,.26,.06,.14,.20], {}),
 ("Scheme 33", "Malir", "Scheme 33 Main Boulevard", "Suburban", 0.70, "mixed", 0.45, 29, .46, 1.30, 0.95, 1.20, -.05, 1.30, [.36,.22,.12,.20,.10], {}),
]
N_REST = len(B)
rest_ids = np.array([f"R{i+1:03d}" for i in range(N_REST)])
rest_idx_of = {r: i for i, r in enumerate(rest_ids)}
rest_busy = np.array([b[4] for b in B]); rest_profile = [b[5] for b in B]
rest_aff = np.array([b[6] for b in B]); rest_age = np.array([b[7] for b in B]); rest_fem = np.array([b[8] for b in B])
rest_psens = np.array([b[9] for b in B]); rest_basket = np.array([b[10] for b in B]); rest_wmult = np.array([b[11] for b in B])
rest_rbias = np.array([b[12] for b in B]); rest_promo = np.array([b[13] for b in B])
rest_chan = np.array([b[14] for b in B]); rest_catpref = [b[15] for b in B]
rest_locality = np.array([b[0] for b in B])
restaurants_df = pd.DataFrame([{
    "Restaurant_ID": rest_ids[i], "Chain_Name": CHAIN_NAME, "Restaurant_Name": f"{CHAIN_NAME} - {b[0]}",
    "Locality": b[0], "District": b[1] + (" Karachi" if b[1] in ("South", "East", "Central") else ""),
    "Address": f"Shop {int(rng.integers(1, 45))}, {b[2]}, {b[0]}, Karachi",
    "City": "Karachi", "Province": "Sindh", "Country": "Pakistan", "Location_Type": b[3],
    "Opening_Date": (datetime(2024, 6, 1) - timedelta(days=int(rng.integers(60, 3000)))).date().isoformat(),
    "Seating_Capacity": int(rng.integers(40, 180)),
} for i, b in enumerate(B)])
print(f"Restaurants: {N_REST} (all Karachi) | chain: {CHAIN_NAME}")

# ===========================================================================
# 2. MENU - ONE catalogue, identical at every branch (no regional / branch-specific items)
# ===========================================================================
CATEGORIES = [("C01","Karahi, Handi & Curries"),("C02","Biryani & Rice"),("C03","BBQ & Grill"),("C04","Daal & Vegetables"),
              ("C05","Bread & Tandoor"),("C06","Fast Food, Rolls & Chinese"),("C07","Starters & Soups"),("C08","Sides & Salads"),
              ("C09","Desserts"),("C10","Beverages"),("C11","Seafood"),("C12","Desi Breakfast")]
categories_df = pd.DataFrame(CATEGORIES, columns=["Category_ID", "Category_Name"])
COST_RATIO_BAND = {"C01":(.42,.58),"C02":(.30,.42),"C03":(.40,.55),"C04":(.25,.38),"C05":(.15,.28),"C06":(.35,.48),
                   "C07":(.30,.42),"C08":(.18,.30),"C09":(.25,.38),"C10":(.12,.25),"C11":(.45,.60),"C12":(.28,.40)}
LOW_MARGIN_CATEGORIES = ["C01", "C03", "C11"]
UNIT_BY_CAT = {c: "kg" for c, _ in CATEGORIES}; UNIT_BY_CAT["C10"] = "liters"; UNIT_BY_CAT["C05"] = "pcs"
MENU = {
 "C01": [("Chicken Karahi",1450),("Mutton Karahi",2600),("Beef Karahi",1900),("Chicken Handi",1350),("Mutton Handi",2500),
         ("Beef Handi",1800),("Chicken White Karahi",1550),("Mutton White Karahi",2700),("Chicken Kali Mirch Karahi",1500),
         ("Chicken Achari Handi",1400),("Chicken Jalfrezi",1300),("Butter Chicken Handi",1450),("Beef Nihari",1100),("Haleem",650),
         ("Siri Paya",900),("Chicken Qorma",1250),("Mutton Qorma",2300),("Chicken Masala",1200),("Chicken Nihari",950),
         ("Mutton Nihari",1800),("Chicken Ginger Karahi",1500),("Chicken Lemon Karahi",1500),("Mutton Rogan Josh",2400),
         ("Beef Pasanda",1700),("Chicken Shahi Korma",1350),("Keema Matar",1100)],
 "C02": [("Chicken Biryani",550),("Mutton Biryani",950),("Beef Biryani",650),("Chicken Pulao",500),("Mutton Pulao",900),
         ("Plain Rice",250),("Zeera Rice",300),("Vegetable Pulao",350),("Chicken Fried Rice",520),("Egg Fried Rice",400),
         ("Sindhi Biryani",600),("Bombay Biryani",580),("Chicken Tikka Biryani",650),("Beef Pulao",600),("Karahi Rice Bowl",520)],
 "C03": [("Chicken Tikka",600),("Chicken Tikka Boneless",750),("Malai Boti",800),("Seekh Kebab",700),("Chicken Seekh Kebab",680),
         ("Mutton Seekh Kebab",950),("Chicken BBQ",850),("Chicken BBQ Full",1600),("Reshmi Kebab",780),("Beef Bihari Boti",1000),
         ("Chicken Boti",700),("Mutton Chops",1600),("Afghani Chicken Tikka",900),("Gola Kebab",750),("Chicken Cheese Kebab",820),
         ("Chicken Chargha",1200),("Chapli Kebab",750),("Chicken Malai Tikka",780),("Beef Tikka",900),("Mutton Boti",1000),
         ("BBQ Platter for Two",2200),("BBQ Family Platter",3800)],
 "C04": [("Daal",400),("Chana",420),("Mixed Vegetable Sabzi",480),("Daal Mash",450),("Daal Chana",400),("Daal Makhni",500),
         ("Daal Tarka",420),("Aloo Gobi",450),("Palak Paneer",650),("Bhindi Masala",480),("Aloo Matar",430),("Baingan Bharta",500),
         ("Karhi Pakora",520),("Daal Palak",430),("Kofta Curry",800),("Aloo Palak",430),("Karhi Chawal",520)],
 "C05": [("Plain Naan",60),("Garlic Naan",120),("Roghni Naan",90),("Roti",40),("Tandoori Roti",50),("Paratha",100),
         ("Lachha Paratha",130),("Kulcha",80),("Cheese Naan",220),("Aloo Paratha",180),("Sheermal",150),("Keema Naan",260),
         ("Chicken Paratha",250),("Puri",50)],
 "C06": [("Chicken Burger",650),("Chicken Zinger Burger",750),("Chicken Sandwich",550),("Chicken Wrap",600),("Club Sandwich",800),
         ("Beef Burger",800),("Chicken Nuggets",550),("Chicken Shawarma",450),("Chicken Paratha Roll",420),("Chicken Fajita Wrap",650),
         ("Beef Seekh Roll",450),("Chicken Tikka Roll",450),("Zinger Paratha Roll",480),("Chicken Chowmein",600),
         ("Chicken Manchurian",900),("Chicken Chilli Dry",950),("Egg Chowmein",480),("Bun Kebab",250),("Zinger Burger Meal Deal",850)],
 "C07": [("Chicken Corn Soup",350),("Hot & Sour Soup",350),("Chicken Samosa",220),("Vegetable Samosa",160),("Chicken Wings",650),
         ("Pakora Platter",400),("Chana Chaat",300),("Dahi Bhalla",320),("Chicken Spring Rolls",420),("Dahi Papri Chaat",300),
         ("Fruit Chaat",350),("Chicken Fingers",600)],
 "C08": [("French Fries",300),("Raita",120),("Salad",150),("Chutney",80),("Russian Salad",350),("Masala Fries",350),("Achar",80),
         ("Papad",60),("Mint Raita",140),("Kachumber Salad",180),("Loaded Fries",420),("Coleslaw",160)],
 "C09": [("Kheer",280),("Gulab Jamun",260),("Gajar Halwa",380),("Chocolate Cake",450),("Kulfi",300),("Ras Malai",350),("Firni",270),
         ("Falooda",420),("Shahi Tukray",350),("Ice Cream Scoop",200),("Jalebi",250),("Rabri Falooda",450),("Mango Kulfi",320),
         ("Sohan Halwa",380),("Brownie with Ice Cream",480)],
 "C10": [("Chai",150),("Kashmiri Chai",250),("Sweet Lassi",280),("Salted Lassi",250),("Fresh Lemonade",250),("Soft Drinks",150),
         ("Bottled Water",80),("Mint Margarita",330),("Mango Shake",380),("Fresh Orange Juice",400),("Doodh Soda",200),
         ("Rooh Afza Sharbat",220),("Mango Lassi",320),("Doodh Patti",160),("Kashmiri Pink Chai",260),("Peach Iced Tea",300),
         ("Fresh Mint Lemonade",270),("Strawberry Shake",380),("Cold Coffee",420)],
 "C11": [("Fried Fish",1200),("Fish Tikka",1100),("Prawn Karahi",2400),("Fish Curry",1100),("Grilled Fish",1300),("Fish and Chips",950),
         ("Prawn Masala",2300)],
 "C12": [("Halwa Puri Breakfast",450),("Anda Paratha",350),("Chana Puri",420),("Omelette",250),("Aloo Anda Paratha",380),
         ("Paya Breakfast",650),("Lassi Breakfast Combo",500)],
}
FLAGSHIP = {"Chicken Biryani","Chicken Karahi","Chicken Tikka","Seekh Kebab","Plain Naan","Roti","Chicken Burger","Chai","Soft Drinks",
            "Fresh Lemonade","French Fries","Raita","Chicken Handi","Malai Boti","Mutton Karahi","Chicken Pulao","Garlic Naan",
            "Daal Mash","Sweet Lassi","Gulab Jamun"}
raw_items = [dict(name=n, cat=c, price=p) for c, lst in MENU.items() for n, p in lst]
assert len({r["name"] for r in raw_items}) == len(raw_items), "duplicate menu item names"
POP_WEIGHT = {"Hot": 9.0, "Popular": 4.0, "Regular": 1.6, "Niche": 0.5, "Rare": 0.15}
rows = []
for k, r in enumerate(raw_items):
    tier = "Hot" if r["name"] in FLAGSHIP else str(rng.choice(["Popular", "Regular", "Niche", "Rare"], p=[.26, .45, .24, .05]))
    lo, hi = COST_RATIO_BAND[r["cat"]]
    tend = float(np.clip(rng.normal(0.10 if r["cat"] in ("C11","C09","C08","C07") else (0.07 if r["cat"] in ("C02","C05") else 0.06), 0.03), 0.01, 0.28))
    rows.append({"Item_ID": f"M{k+1:04d}", "Item_Name": r["name"], "Category_ID": r["cat"],
                 "Base_Cost": round(r["price"] * float(rng.uniform(lo, hi)), 2), "Base_Price": float(r["price"]),
                 "Introduced_Date": (START_DATE - timedelta(days=int(rng.integers(100, 1500)))).date().isoformat(),
                 "Is_Active": True, "_pop_tier": tier, "_wastage_tendency": tend})
items_df = pd.DataFrame(rows)
N_ITEMS = len(items_df); item_ids = items_df["Item_ID"].values; item_cat = items_df["Category_ID"].values
items_df["_pop_weight"] = items_df["_pop_tier"].map(POP_WEIGHT)
print(f"Menu: {N_ITEMS} items in {len(categories_df)} categories (identical at all {N_REST} branches)")

# ---- engineered analytical cases (they must EMERGE from transactions; nothing is labelled in the data) ----
all_idx = np.arange(N_ITEMS)
non_flag = np.array([i for i in all_idx if items_df.loc[i, "Item_Name"] not in FLAGSHIP])
hot_pop = np.where(items_df["_pop_tier"].isin(["Hot", "Popular"]).values)[0]
hi_waste_idx = np.concatenate([rng.choice(hot_pop, 6, replace=False), rng.choice(np.setdiff1d(all_idx, hot_pop), 6, replace=False)])
items_df.loc[hi_waste_idx, "_wastage_tendency"] = rng.uniform(0.22, 0.35, 12)       # incl. popular high-wastage dishes
pop_low_margin_idx = rng.choice(non_flag, 10, replace=False)
items_df.loc[pop_low_margin_idx, "_pop_tier"] = rng.choice(["Hot", "Popular"], 10)
items_df.loc[pop_low_margin_idx, "_pop_weight"] = items_df.loc[pop_low_margin_idx, "_pop_tier"].map(POP_WEIGHT)
for i in pop_low_margin_idx:
    lo, hi = COST_RATIO_BAND[items_df.loc[i, "Category_ID"]]
    items_df.loc[i, "Base_Cost"] = round(items_df.loc[i, "Base_Price"] * float(rng.uniform(hi, min(hi + .15, .78))), 2)
prof_low_sell_idx = rng.choice(np.setdiff1d(non_flag, pop_low_margin_idx), 10, replace=False)
items_df.loc[prof_low_sell_idx, "_pop_tier"] = rng.choice(["Niche", "Rare"], 10)
items_df.loc[prof_low_sell_idx, "_pop_weight"] = items_df.loc[prof_low_sell_idx, "_pop_tier"].map(POP_WEIGHT)
for i in prof_low_sell_idx:
    lo, hi = COST_RATIO_BAND[items_df.loc[i, "Category_ID"]]
    items_df.loc[i, "Base_Cost"] = round(items_df.loc[i, "Base_Price"] * float(rng.uniform(max(lo - .08, .06), lo)), 2)
ps_idx = rng.choice(all_idx, 20, replace=False)
items_df["_price_sensitive"] = False; items_df.loc[ps_idx, "_price_sensitive"] = True
engineered = set(pop_low_margin_idx) | set(prof_low_sell_idx) | set(ps_idx)
new_idx = rng.choice(np.array([i for i in non_flag if i not in engineered]), 6, replace=False)
for i in new_idx:
    items_df.loc[i, "Introduced_Date"] = (datetime(2025, 10, 15) + timedelta(days=int(rng.integers(0, 45)))).date().isoformat()
    items_df.loc[i, "_pop_tier"] = "Regular"; items_df.loc[i, "_pop_weight"] = POP_WEIGHT["Regular"]
item_intro_off = np.array([(D(s) - START_DATE).days for s in items_df["Introduced_Date"]])
item_rating_mean = np.clip(rng.normal(4.0, .30, N_ITEMS), 3.4, 4.6)
poor_idx = np.concatenate([rng.choice(hot_pop, 3, replace=False), rng.choice(np.setdiff1d(all_idx, hot_pop), 5, replace=False)])
item_rating_mean[poor_idx] = rng.uniform(2.9, 3.3, len(poor_idx))

# ---- identical menu at every branch (availability table = proof of identical catalogue) ----
A = np.ones((N_REST, N_ITEMS), dtype=bool)
av_r, av_i = np.where(A)
availability_df = pd.DataFrame({
    "Availability_ID": [f"AV{k+1:06d}" for k in range(len(av_r))], "Restaurant_ID": rest_ids[av_r], "Item_ID": item_ids[av_i],
    "Is_Available": True,
    "Available_From": [max(D(items_df.loc[i, "Introduced_Date"]), START_DATE).date().isoformat() for i in av_i]})
print(f"Menu_Item_Availability: {len(availability_df)} rows = {N_ITEMS} items x {N_REST} branches")

# ===========================================================================
# 3. CUSTOMERS (synthetic Pakistani identities; age/gender/locality depend on home branch)
# ===========================================================================
MALE = ["Muhammad","Ahmed","Ali","Hassan","Hussain","Usman","Bilal","Hamza","Fahad","Zain","Imran","Kamran","Asad","Farhan","Saad",
        "Umar","Tariq","Nadeem","Shahid","Junaid","Faisal","Waqas","Adeel","Noman","Rizwan","Arslan","Danish","Haris","Talha","Shoaib",
        "Waleed","Zeeshan","Salman","Naveed","Atif","Kashif","Owais","Daniyal","Murtaza","Abdullah"]
FEMALE = ["Ayesha","Fatima","Zainab","Maryam","Sana","Hina","Amna","Mehwish","Noor","Iqra","Bushra","Saima","Rabia","Sadia","Nimra",
          "Laiba","Areeba","Hira","Mahnoor","Komal","Anam","Samina","Farah","Rukhsar","Sidra","Maham","Aliza","Khadija","Hafsa","Zara",
          "Kinza","Mariam","Aiman","Dua","Sumaiya","Rida"]
SURNAMES = ["Khan","Ahmed","Memon","Sheikh","Qureshi","Siddiqui","Ansari","Baloch","Soomro","Jamali","Hashmi","Rizvi","Naqvi","Zaidi",
            "Malik","Mirza","Baig","Chaudhry","Butt","Rajput","Bhatti","Abbasi","Lakhani","Dawood","Haroon","Ismail","Hussain","Iqbal",
            "Akhtar","Javed","Shah","Syed","Lodhi","Niazi","Afridi","Yousufzai","Kapadia","Bhutto","Shaikh","Usmani"]
is_female = rng.random(N_CUSTOMERS) < 0  # placeholder, set after home branch is drawn
rest_w = rest_busy ** 1.5
home_rest_idx = rng.choice(N_REST, N_CUSTOMERS, p=rest_w / rest_w.sum())
is_female = rng.random(N_CUSTOMERS) < rest_fem[home_rest_idx]
age = np.clip(np.rint(rng.normal(rest_age[home_rest_idx], 9)), 16, 75).astype(int)
first_names = np.where(is_female, rng.choice(FEMALE, N_CUSTOMERS), rng.choice(MALE, N_CUSTOMERS))
last_names = rng.choice(SURNAMES, N_CUSTOMERS)
cust_loc = np.where(rng.random(N_CUSTOMERS) < 0.70, rest_locality[home_rest_idx], rng.choice(rest_locality, N_CUSTOMERS))
cust_digital = np.clip(1.4 - age / 50 + rng.normal(0, .2, N_CUSTOMERS), 0.2, 1.6)
cust_promo_sens = rng.lognormal(0, .45, N_CUSTOMERS) * rest_psens[home_rest_idx]

is_new_customer = rng.random(N_CUSTOMERS) < 0.20
new_signup_day = rng.integers(0, TOTAL_DAYS - 60, N_CUSTOMERS)
signup_dates = [(START_DATE + timedelta(days=int(new_signup_day[i]))) if is_new_customer[i]
                else START_DATE - timedelta(days=int(rng.integers(60, 1500))) for i in range(N_CUSTOMERS)]
cust_signup_day = np.where(is_new_customer, new_signup_day, 0)
cust_activity_weight = rng.lognormal(0.0, 1.15, N_CUSTOMERS)
cust_is_churn_prone = rng.random(N_CUSTOMERS) < 0.18
customers_df = pd.DataFrame({
    "Customer_ID": [f"CUS{i+1:06d}" for i in range(N_CUSTOMERS)], "First_Name": first_names, "Last_Name": last_names,
    "Gender": np.where(is_female, "Female", "Male"), "Age": age, "City": "Karachi", "Locality": cust_loc,
    "Home_Restaurant_ID": rest_ids[home_rest_idx], "Signup_Date": [d.date().isoformat() for d in signup_dates],
    "Email": [f"{f.lower()}.{l.lower()}{i+1:06d}@example.com" for i, (f, l) in enumerate(zip(first_names, last_names))],
    "Phone": [f"+92-399-{int(x):07d}" for x in rng.integers(0, 9999999, N_CUSTOMERS)],   # unallocated prefix = non-contactable
})
cust_fullname_short = np.array([f"{f} {l[0]}." for f, l in zip(first_names, last_names)])
print(f"Customers: {len(customers_df)}")

# ===========================================================================
# 4. PROMOTIONS (Pakistani calendar; chain-wide AND branch-specific)
# ===========================================================================
FIXED = [("Ramazan Iftar Deal", 15, "2025-03-01", "2025-03-29"), ("Pakistan Day Offer", 14, "2025-03-21", "2025-03-23"),
         ("Eid ul Fitr Special Platter", 20, "2025-03-31", "2025-04-03"), ("Eid ul Adha BBQ Offer", 18, "2025-06-06", "2025-06-09"),
         ("Independence Day Offer", 14, "2025-08-12", "2025-08-14"), ("Winter Karahi Fest", 15, "2025-12-01", "2025-12-20")]
LOCAL = [("Sea View Sunday Special", 15, "Sea View"), ("Clifton Dinner Deal", 15, "Clifton Block 5"), ("Saddar Lunch Deal", 12, "Saddar"),
         ("Tariq Road BBQ Night", 20, "Tariq Road"), ("Korangi Workers Lunch Offer", 15, "Korangi Industrial Area"),
         ("University Road Student Offer", 12, "Johar Chowrangi, University Road")]
GENERIC = [("Weekend Family Offer", 20), ("Jummah Special", 10), ("Karachi Foodie Weekend", 18), ("Weekend Biryani Offer", 15),
           ("Lunch Deal", 12), ("Summer Cooler Offer", 18), ("Mango Season Shake Deal", 20), ("Free Delivery Weekend", 10),
           ("Wedding Season Platter", 22), ("Buy 1 Get 1 Naan", 50), ("Late Night Deal", 15), ("Loyalty Member Discount", 10),
           ("App-Only Flash Sale", 30), ("Clearance: Slow Movers", 40), ("Mega Family Combo", 22), ("New Item Launch Promo", 15),
           ("Winter Haleem Deal", 15), ("Student Card Discount", 12)]
N_PROMOS = 45
promos = []
for i in range(N_PROMOS):
    scope_rest = None
    if i < len(FIXED):
        name, bd, fs, fe = FIXED[i]; start, end = D(fs), D(fe)
    else:
        if i < len(FIXED) + len(LOCAL):
            name, bd, loc = LOCAL[i - len(FIXED)]; scope_rest = str(rest_ids[list(rest_locality).index(loc)])
        else:
            name, bd = GENERIC[(i - len(FIXED) - len(LOCAL)) % len(GENERIC)]
        start = START_DATE + timedelta(days=int(rng.integers(0, TOTAL_DAYS - 20)))
        end = min(start + timedelta(days=int(rng.integers(5, 30))), END_DATE)
    quality = str(rng.choice(["Effective", "Weak", "Misleading"], p=[.45, .30, .25]))
    if quality == "Misleading":
        cat, usage = str(rng.choice(LOW_MARGIN_CATEGORIES)), 0.55
        if scope_rest is None and rng.random() >= 0.8:
            scope_rest = str(rng.choice(rest_ids))
    elif quality == "Weak":
        cat = rng.choice([c for c, _ in CATEGORIES] + [None]); usage = 0.10
        if scope_rest is None and rng.random() < 0.3: scope_rest = str(rng.choice(rest_ids))
    else:
        cat = rng.choice([c for c, _ in CATEGORIES] + [None, None]); usage = 0.40
        if scope_rest is None and rng.random() < 0.2: scope_rest = str(rng.choice(rest_ids))
    promos.append({"Promotion_ID": f"PROMO{i+1:03d}", "Promotion_Name": f"{name} #{i+1}",
                   "Discount_Percent": int(np.clip(bd + int(rng.integers(-3, 4)) + (8 if quality == "Misleading" else 0), 5, 50)),
                   "Start_Date": start.date().isoformat(), "End_Date": end.date().isoformat(),
                   "Applicable_Category_ID": None if cat is None else str(cat), "Applicable_Restaurant_ID": scope_rest,
                   "_quality": quality, "_usage_prob": usage})
promotions_df = pd.DataFrame(promos)
print(f"Promotions: {len(promotions_df)} ({promotions_df['Applicable_Restaurant_ID'].isna().sum()} chain-wide, "
      f"{promotions_df['Applicable_Restaurant_ID'].notna().sum()} branch-specific)")

# ===========================================================================
# 5. PRICING_HISTORY - CHAIN-WIDE ONLY (one price per item per effective date, identical at every branch)
# ===========================================================================
def round5(x): return max(10, int(round(x / 5.0)) * 5)
ph_rows, pid = [], 1
for i in range(N_ITEMS):
    intro = max(D(items_df.loc[i, "Introduced_Date"]), START_DATE)
    sens = bool(items_df.loc[i, "_price_sensitive"])
    sched, cur, p = [[intro, round5(items_df.loc[i, "Base_Price"])]], intro, float(items_df.loc[i, "Base_Price"])
    for _ in range(int(rng.choice([1, 2, 3, 4], p=[.30, .35, .25, .10]))):
        cur = cur + timedelta(days=int(rng.integers(25, 90)))
        if cur > END_DATE - timedelta(days=5): break
        p = p * (1 + (1 if rng.random() < 0.75 else -1) * float(rng.uniform(.04, .18 if sens else .12)))
        sched.append([cur, round5(p)])
    for k in range(1, len(sched)):
        if sched[k][1] == sched[k - 1][1]: sched[k][1] += 5
    for k, (eff, price) in enumerate(sched):
        ph_rows.append({"Pricing_ID": f"PH{pid:06d}", "Item_ID": item_ids[i], "Price": float(price),
                        "Previous_Price": float(sched[k - 1][1]) if k else None, "Effective_Date": eff.date().isoformat(),
                        "End_Date": (sched[k + 1][0] - timedelta(days=1)).date().isoformat() if k + 1 < len(sched) else None})
        pid += 1
pricing_history_df = pd.DataFrame(ph_rows)
print(f"Pricing_History: {len(pricing_history_df)} chain-wide price rows for {N_ITEMS} items")

# ===========================================================================
# 6. ORDERS
# ===========================================================================
w = cust_activity_weight
counts = np.floor(w / w.sum() * N_ORDERS).astype(int)
rem = N_ORDERS - counts.sum()
if rem > 0: np.add.at(counts, rng.choice(N_CUSTOMERS, size=rem, p=w / w.sum()), 1)
cust_row_idx = np.repeat(np.arange(N_CUSTOMERS), counts)
order_customer_id = customers_df["Customer_ID"].values[cust_row_idx]
order_signup_day = cust_signup_day[cust_row_idx]; order_churn_prone = cust_is_churn_prone[cust_row_idx]
churn_cut = order_signup_day + ((TOTAL_DAYS - 1 - order_signup_day) * rng.uniform(.25, .75, N_ORDERS)).astype(int)
active_end = np.maximum(np.where(order_churn_prone, churn_cut, TOTAL_DAYS - 1), order_signup_day)

all_dates = pd.date_range(START_DATE, END_DATE); wd = all_dates.dayofweek.values; mon_arr = all_dates.month.values
day_w = np.select([wd == 4, wd == 5, wd == 6], [1.3, 1.6, 1.4], 1.0).astype(float)
day_w *= np.select([np.isin(mon_arr, [11, 12, 1, 2]), np.isin(mon_arr, [6, 7, 8])], [1.2, .95], 1.0)
def day_range(s, e): return (all_dates >= pd.Timestamp(s)) & (all_dates <= pd.Timestamp(e))
day_w[day_range("2025-03-01", "2025-03-30")] *= .95; day_w[day_range("2025-03-31", "2025-04-02")] *= 1.9
day_w[day_range("2025-06-06", "2025-06-09")] *= 1.4; day_w[day_range("2025-08-14", "2025-08-14")] *= 1.4
day_w[day_range("2025-12-25", "2025-12-25")] *= 1.2
chain_promo_days = np.zeros(TOTAL_DAYS, bool)
for _, _p in promotions_df.iterrows():
    if pd.isna(_p["Applicable_Restaurant_ID"]) and _p["_quality"] in ("Effective", "Misleading"):
        m_ = day_range(_p["Start_Date"], _p["End_Date"])
        day_w[m_] *= 1.12 if _p["_quality"] == "Misleading" else 1.20; chain_promo_days |= m_
day_norm = day_w / day_w.max()
day_off = rng.integers(order_signup_day, active_end + 1)
for _ in range(3):
    acc = rng.random(N_ORDERS) < day_norm[day_off]
    if (~acc).sum() == 0: break
    day_off[~acc] = rng.integers(order_signup_day[~acc], active_end[~acc] + 1)
order_month = mon_arr[day_off]

other = rng.random(N_ORDERS) < 0.20
order_rest_idx = np.where(other, rng.choice(N_REST, N_ORDERS, p=rest_w / rest_w.sum()), home_rest_idx[cust_row_idx])
# promotion windows pull extra traffic towards promo-responsive branches
in_promo = chain_promo_days[day_off] & (rng.random(N_ORDERS) < 0.25)
pw_ = rest_busy * rest_promo; order_rest_idx[in_promo] = rng.choice(N_REST, in_promo.sum(), p=pw_ / pw_.sum())
order_restaurant_id = rest_ids[order_rest_idx]

HOURS = np.arange(7, 23)
HP = {"family": [1,2,3,4,6,10,11,6,3,4,7,11,16,17,14,7], "office": [2,3,4,6,10,18,17,7,3,4,6,9,12,12,8,4],
      "industrial": [3,4,5,6,9,15,13,6,3,4,5,7,9,10,9,10], "student": [1,1,2,3,5,8,9,6,5,7,9,12,14,15,15,11],
      "mixed": [1,2,3,4,7,12,13,7,4,5,7,10,14,15,12,6]}
HP = {k: np.array(v, float) / sum(v) for k, v in HP.items()}
HP_RAM = np.array([1,.5,.5,.5,.5,1,1,1,1.5,3,9,16,12,8,11,12], float); HP_RAM /= HP_RAM.sum()
order_hour = np.zeros(N_ORDERS, dtype=int)
prof_arr = np.array(rest_profile)[order_rest_idx]
for pf, pp in HP.items():
    m_ = prof_arr == pf; order_hour[m_] = rng.choice(HOURS, m_.sum(), p=pp)
ram = order_month == 3; order_hour[ram] = rng.choice(HOURS, ram.sum(), p=HP_RAM)
order_dt = (pd.Timestamp(START_DATE) + pd.to_timedelta(day_off, unit="D") + pd.to_timedelta(order_hour, unit="h")
            + pd.to_timedelta(rng.integers(0, 60, N_ORDERS), unit="m"))

# channel: branch mix x customer digital affinity
Pch = rest_chan[order_rest_idx].copy(); dm = .6 + .8 * cust_digital[cust_row_idx]
Pch[:, 2] *= dm; Pch[:, 3] *= dm; Pch /= Pch.sum(1, keepdims=True)
order_channel = np.array(CHANNELS)[(rng.random(N_ORDERS)[:, None] > Pch.cumsum(1)).sum(1).clip(0, 4)]
pay = np.empty(N_ORDERS, dtype=object)
PAYP = {"Dine-in": [.40,.45,.15], "Takeaway": [.50,.35,.15], "Website/App": [.05,.15,.80], "Third-Party Delivery": [.45,0,.55], "Phone Order": [.85,0,.15]}
for ch, pp in PAYP.items():
    m_ = order_channel == ch; pay[m_] = rng.choice(["Cash", "Card", "Online"], m_.sum(), p=pp)
dcharge = np.zeros(N_ORDERS)
dl = np.isin(order_channel, ["Third-Party Delivery", "Phone Order"]) | ((order_channel == "Website/App") & (rng.random(N_ORDERS) < .55))
dcharge[dl] = rng.choice([0, 99, 149, 199, 249], dl.sum(), p=[.15, .20, .25, .25, .15])

# promotion usage: base x branch response x customer promo-sensitivity
promo_id_col = np.array([None] * N_ORDERS, dtype=object); promo_disc_col = np.zeros(N_ORDERS)
promo_cat_col = np.array([None] * N_ORDERS, dtype=object); assigned = np.zeros(N_ORDERS, bool)
resp = rest_promo[order_rest_idx] * cust_promo_sens[cust_row_idx]
for _, pr in promotions_df.iterrows():
    sd = (D(pr["Start_Date"]) - START_DATE).days; ed = (D(pr["End_Date"]) - START_DATE).days
    rid = pr["Applicable_Restaurant_ID"]
    rmask = np.ones(N_ORDERS, bool) if pd.isna(rid) else (order_restaurant_id == rid)
    use = (day_off >= sd) & (day_off <= ed) & rmask & (~assigned) & (rng.random(N_ORDERS) < np.clip(pr["_usage_prob"] * resp, 0, .92))
    promo_id_col[use] = pr["Promotion_ID"]; promo_disc_col[use] = pr["Discount_Percent"]
    promo_cat_col[use] = None if pd.isna(pr["Applicable_Category_ID"]) else pr["Applicable_Category_ID"]; assigned |= use
status = np.array(["Completed"] * N_ORDERS, dtype=object); sr = rng.random(N_ORDERS)
status[sr < .05] = "Cancelled"; status[(sr >= .05) & (sr < .07)] = "Refunded"
status[(day_off >= TOTAL_DAYS - 3) & (rng.random(N_ORDERS) < .35)] = "Pending"
order_ids = np.array([f"ORD{i+1:07d}" for i in range(N_ORDERS)])
orders_df = pd.DataFrame({"Order_ID": order_ids, "Customer_ID": order_customer_id, "Restaurant_ID": order_restaurant_id,
                          "Order_DateTime": order_dt.strftime("%Y-%m-%d %H:%M:%S"), "Order_Date": order_dt.strftime("%Y-%m-%d"),
                          "Channel": order_channel, "Payment_Method": pay, "Promotion_ID": promo_id_col, "Status": status,
                          "Delivery_Charge": dcharge})
print(f"Orders: {len(orders_df)}")

# ===========================================================================
# 7. ORDER_ITEMS - same menu everywhere; branch taste / seasonality shape DEMAND only
# ===========================================================================
branch_item_noise = rng.lognormal(0, .20, (N_REST, N_ITEMS))
pop_w = items_df["_pop_weight"].values.astype(float)
BW = np.zeros((N_REST, N_ITEMS))
for r in range(N_REST):
    cp = rest_catpref[r]
    BW[r] = pop_w * np.array([cp.get(c, 1.0) for c in item_cat]) * branch_item_noise[r]
BW[:, new_idx] = 0.0

def seasonal(name):
    n, m = name.lower(), np.ones(12)
    if "gajar halwa" in n: m = np.array([4,3,1.5,.3,.15,.1,.1,.1,.15,.3,2.5,4.])
    elif "mango" in n: m = np.array([.3,.3,.6,1.5,3,4,4,3,1.2,.5,.3,.3])
    elif any(k in n for k in ["lassi","lemonade","doodh soda","rooh afza","margarita","falooda","kulfi","ice cream","iced tea","cold coffee"]):
        m = np.array([.5,.5,1.4,1.5,1.8,2,2,1.9,1.5,1,.6,.5])
        if "rooh afza" in n or "lemonade" in n: m[2] = 2.5
    elif any(k in n for k in ["chai","doodh patti"]): m = np.array([1.8,1.6,1,.8,.6,.5,.5,.5,.7,1,1.6,1.9])
    elif any(k in n for k in ["haleem","nihari","paya"]): m = np.array([1.7,1.5,1.2,.8,.5,.4,.4,.4,.6,.9,1.5,1.8])
    elif any(k in n for k in ["samosa","pakora","dahi bhalla","chana chaat","dahi papri","fruit chaat"]): m = np.array([1,1,3.5,1,1,1,1,1,1,1,1,1.])
    return m
SEAS = np.stack([seasonal(n) for n in items_df["Item_Name"]], axis=1)

basket = np.clip(rng.poisson(3.4 * rest_basket[order_rest_idx]) + 1, 1, 15)
total_rows = int(basket.sum())
oi_order_idx = np.repeat(np.arange(N_ORDERS), basket)
oi_rest_idx = order_rest_idx[oi_order_idx]; oi_month = order_month[oi_order_idx]
oi_item_idx = np.zeros(total_rows, dtype=int)
key = oi_rest_idx * 13 + oi_month; srt = np.argsort(key, kind="stable")
uk, st, ct = np.unique(key[srt], return_index=True, return_counts=True)
for k, s, c in zip(uk, st, ct):
    rr, mm = k // 13, k % 13
    ww = BW[rr] * SEAS[mm - 1]; oi_item_idx[srt[s:s + c]] = rng.choice(N_ITEMS, size=c, p=ww / ww.sum())
oi_day = day_off[oi_order_idx]
for i in new_idx:
    sel = (oi_day >= item_intro_off[i]) & (rng.random(total_rows) < 0.04); oi_item_idx[sel] = i
qty = rng.choice([1, 2, 3, 4, 5], size=total_rows, p=[.62, .23, .09, .04, .02]); cr = item_cat[oi_item_idx]
m5 = cr == "C05"; qty[m5] = rng.choice([1,2,3,4,5,6], m5.sum(), p=[.25,.30,.20,.12,.08,.05])
m10 = cr == "C10"; qty[m10] = rng.choice([1,2,3,4], m10.sum(), p=[.45,.30,.15,.10])
oi = pd.DataFrame({"Order_ID": order_ids[oi_order_idx], "Item_ID": item_ids[oi_item_idx], "Restaurant_ID": order_restaurant_id[oi_order_idx],
                   "Quantity": qty, "_dt": pd.Timestamp(START_DATE) + pd.to_timedelta(oi_day, unit="D"),
                   "_promo_id": promo_id_col[oi_order_idx], "_promo_disc": promo_disc_col[oi_order_idx],
                   "_promo_cat": promo_cat_col[oi_order_idx]})
oi = oi.drop_duplicates(subset=["Order_ID", "Item_ID"], keep="first").reset_index(drop=True)
ph = pricing_history_df.copy(); ph["Effective_Date"] = pd.to_datetime(ph["Effective_Date"])
oi = pd.merge_asof(oi.sort_values("_dt"), ph.sort_values("Effective_Date")[["Item_ID", "Effective_Date", "Price"]],
                   left_on="_dt", right_on="Effective_Date", by="Item_ID", direction="backward")
miss = oi["Price"].isna()
if miss.any(): oi.loc[miss, "Price"] = oi.loc[miss, "Item_ID"].map(dict(zip(item_ids, items_df["Base_Price"])))
oi = oi.rename(columns={"Price": "Unit_Price"}).drop(columns=["Effective_Date"]).reset_index(drop=True)
oi["_cat"] = oi["Item_ID"].map(dict(zip(item_ids, item_cat)))
applies = oi["_promo_id"].notna() & (oi["_promo_cat"].isna() | (oi["_promo_cat"] == oi["_cat"]))
oi["Discount_Percent"] = np.where(applies, oi["_promo_disc"], 0.0)
_q = promotions_df.set_index("Promotion_ID")["_quality"]
_pq = oi["_promo_id"].map(_q)
_pb = np.where(_pq == "Misleading", .80, np.where(_pq == "Effective", .40, 0.0))        # promo-driven extra units on discounted lines
_boost = applies.values & (rng.random(len(oi)) < _pb)
oi.loc[_boost, "Quantity"] = oi.loc[_boost, "Quantity"].values + 1
print(f"Order lines sampled: {len(oi)}")

# ---- 7b. sales anomalies (real, detectable patterns) ----
anomaly_records = {"spikes": [], "drops": [], "high_value_orders": []}
orders_by_key = orders_df.groupby(["Restaurant_ID", "Order_Date"])["Order_ID"].apply(np.array).to_dict()
hot_items = np.where(items_df["_pop_tier"].isin(["Hot", "Popular"]).values)[0]
spike_items = rng.choice(hot_items, 10, replace=False); spike_rows = []
for i in spike_items:
    r = int(rng.integers(0, N_REST)); d_ = START_DATE + timedelta(days=int(rng.integers(20, TOTAL_DAYS - 20)))
    cands = orders_by_key.get((rest_ids[r], d_.date().isoformat()))
    if cands is None or len(cands) == 0: continue
    chosen = rng.choice(cands, size=min(40, len(cands)), replace=False)
    _ph = pricing_history_df[(pricing_history_df["Item_ID"] == item_ids[i]) & (pd.to_datetime(pricing_history_df["Effective_Date"]) <= d_)]
    price = float(_ph.sort_values("Effective_Date").iloc[-1]["Price"]) if len(_ph) else float(items_df.loc[i, "Base_Price"])   # chain-wide price in effect that day
    for oid in chosen:
        spike_rows.append({"Order_ID": oid, "Item_ID": item_ids[i], "Restaurant_ID": rest_ids[r], "Quantity": int(rng.integers(4, 9)),
                           "_dt": pd.Timestamp(d_), "_promo_id": None, "_promo_disc": 0.0, "_promo_cat": None, "Unit_Price": price,
                           "_cat": item_cat[i], "Discount_Percent": 0.0})
    anomaly_records["spikes"].append({"Item_ID": item_ids[i], "Restaurant_ID": rest_ids[r], "Date": d_.date().isoformat(), "Extra_Lines": len(chosen)})
if spike_rows: oi = pd.concat([oi, pd.DataFrame(spike_rows)], ignore_index=True)
drop_items = rng.choice(np.setdiff1d(hot_items, spike_items), 10, replace=False); drop_mask = np.zeros(len(oi), bool)
for i in drop_items:
    r = int(rng.integers(0, N_REST)); ws = START_DATE + timedelta(days=int(rng.integers(20, TOTAL_DAYS - 30))); we = ws + timedelta(days=6)
    m = ((oi["Item_ID"] == item_ids[i]) & (oi["Restaurant_ID"] == rest_ids[r]) & (oi["_dt"] >= ws) & (oi["_dt"] <= we)).values
    idx = np.where(m)[0]; nd = int(len(idx) * .85)
    if nd: drop_mask[rng.choice(idx, nd, replace=False)] = True
    anomaly_records["drops"].append({"Item_ID": item_ids[i], "Restaurant_ID": rest_ids[r], "Window_Start": ws.date().isoformat(),
                                     "Window_End": we.date().isoformat(), "Rows_Suppressed": nd, "Rows_Available_Before": int(m.sum())})
oi = oi[~drop_mask].reset_index(drop=True)

# ---- 7c. price sensitivity: flagged items react to CHAIN-WIDE price changes (all branches) ----
flag_ids = set(items_df.loc[items_df["_price_sensitive"], "Item_ID"])
sub = oi[oi["Item_ID"].isin(flag_ids)]; grp = sub.groupby("Item_ID").indices
sub_label, sub_dt = sub.index.values, sub["_dt"].values
elasticity_log, drop_labels, add_rows = [], [], []
for _, c in pricing_history_df[pricing_history_df["Item_ID"].isin(flag_ids) & pricing_history_df["Previous_Price"].notna()].iterrows():
    if c["Item_ID"] not in grp: continue
    pos = grp[c["Item_ID"]]; eff = np.datetime64(c["Effective_Date"])
    labels = sub_label[pos[(sub_dt[pos] >= eff) & (sub_dt[pos] <= eff + np.timedelta64(30, "D"))]]
    pct = c["Price"] / c["Previous_Price"] - 1
    if len(labels) == 0 or abs(pct) < .02: continue
    if pct > 0:
        n = int(len(labels) * float(np.clip(pct * 3.0, .12, .70)))
        if n: drop_labels.extend(rng.choice(labels, n, replace=False).tolist())
        elasticity_log.append({"Item_ID": c["Item_ID"], "Pct_Price_Change": round(pct * 100, 1), "Direction": "increase", "Rows_Removed": n})
    else:
        n = int(len(labels) * float(np.clip(-pct * 2.4, .12, .60)))
        if n:
            src = oi.loc[rng.choice(labels, n, replace=True)].copy()
            keys_ = [(r_, t.strftime("%Y-%m-%d")) for r_, t in zip(src["Restaurant_ID"], src["_dt"])]
            src["Order_ID"] = [rng.choice(orders_by_key[k]) if k in orders_by_key else None for k in keys_]
            src["Discount_Percent"] = 0.0; src["_promo_id"] = None; add_rows.append(src[src["Order_ID"].notna()])
        elasticity_log.append({"Item_ID": c["Item_ID"], "Pct_Price_Change": round(pct * 100, 1), "Direction": "decrease", "Rows_Added": n})
if drop_labels: oi = oi.drop(index=list(set(drop_labels)))
if add_rows: oi = pd.concat([oi] + add_rows, ignore_index=True)
oi = oi.reset_index(drop=True).drop_duplicates(subset=["Order_ID", "Item_ID"], keep="first").reset_index(drop=True)
hv_orders = rng.choice(order_ids, 250, replace=False); hv = oi["Order_ID"].isin(hv_orders).values
oi.loc[hv, "Quantity"] = np.round(oi.loc[hv, "Quantity"].values * rng.uniform(5.0, 9.0, hv.sum())).astype(int)
anomaly_records["high_value_orders"] = [str(x) for x in hv_orders]

oi = oi.sort_values(["Order_ID", "Item_ID"]).reset_index(drop=True)
oi["Order_Item_ID"] = [f"OI{i+1:08d}" for i in range(len(oi))]
oi["Line_Subtotal"] = (oi["Quantity"] * oi["Unit_Price"]).round(2)
oi["Line_Total"] = (oi["Line_Subtotal"] * (1 - oi["Discount_Percent"] / 100.0)).round(2)
order_items_df = oi[["Order_Item_ID", "Order_ID", "Item_ID", "Quantity", "Unit_Price", "Discount_Percent", "Line_Subtotal", "Line_Total"]].copy()
order_items_df["Quantity"] = order_items_df["Quantity"].astype(int)
print(f"Order_Items: {len(order_items_df)} (avg basket {len(order_items_df)/N_ORDERS:.2f}); anomalies: "
      f"{len(anomaly_records['spikes'])} spikes, {len(anomaly_records['drops'])} drops, 250 high-value orders")

def order_totals(orders, lines):
    a = lines.groupby("Order_ID").agg(Item_Count=("Order_Item_ID", "count"), Subtotal=("Line_Subtotal", "sum"), Total_Amount=("Line_Total", "sum")).reset_index()
    o = orders.drop(columns=[c for c in ["Item_Count", "Subtotal", "Total_Amount", "Discount_Amount", "Tax_Amount", "Grand_Total"] if c in orders.columns]).merge(a, on="Order_ID", how="left")
    o["Item_Count"] = o["Item_Count"].fillna(0).astype(int)
    o["Subtotal"] = o["Subtotal"].fillna(0.0).round(2); o["Total_Amount"] = o["Total_Amount"].fillna(0.0).round(2)
    o["Discount_Amount"] = (o["Subtotal"] - o["Total_Amount"]).round(2)
    o["Tax_Amount"] = (o["Total_Amount"] * np.where(o["Payment_Method"] == "Cash", 0.13, 0.08)).round(2)   # synthetic tax rule
    o["Grand_Total"] = (o["Total_Amount"] + o["Tax_Amount"] + o["Delivery_Charge"]).round(2)
    return o
orders_df = order_totals(orders_df, order_items_df)

# ===========================================================================
# 8. RATINGS + SYNTHETIC REVIEWS (branch-specific bias / spread / delivery experience)
# ===========================================================================
pool_orders = orders_df.loc[orders_df["Status"] == "Completed", ["Order_ID", "Customer_ID", "Restaurant_ID", "Order_Date", "Channel"]]
pool = order_items_df[["Order_ID", "Item_ID"]].merge(pool_orders, on="Order_ID")
pick = pool.iloc[rng.choice(len(pool), N_RATINGS, replace=False)].reset_index(drop=True)
it_pos = {k: n for n, k in enumerate(item_ids)}
r_pos = pick["Restaurant_ID"].map(rest_idx_of).values; it_p = pick["Item_ID"].map(it_pos).values
branch_sd = rng.uniform(.85, 1.20, N_REST); branch_dq = rng.normal(0, .20, N_REST)
rdate = pd.to_datetime(pick["Order_Date"]) + pd.to_timedelta(rng.integers(0, 5, N_RATINGS), unit="D")
trend = (rdate.dt.dayofyear.values / 365.0 - .5) * .15
is_deliv = pick["Channel"].isin(["Third-Party Delivery", "Phone Order", "Website/App"]).values
mu = item_rating_mean[it_p] + rest_rbias[r_pos] + trend + np.where(is_deliv, branch_dq[r_pos], 0)
rv = np.clip(np.rint(rng.normal(mu, branch_sd[r_pos])), 1, 5).astype(int)
POS_FOOD = ["The {item} was absolutely delicious", "Loved the {item}, cooked perfectly", "{item} had amazing flavour",
            "Best {item} I have had in Karachi", "The {item} was fresh and piping hot", "Really enjoyed the {item}, great taste"]
POS_X = ["Staff was very courteous.", "Service was quick and friendly.", "Our table was ready on time.", "The branch was spotless.",
         "Good value for money.", "Portion size is great for the price.", "Will definitely order again."]
POS_D = ["The rider arrived on time.", "Delivery was fast and the food stayed hot.", "Packaging was neat and secure."]
NEU_FOOD = ["The {item} was okay, nothing special", "{item} was decent but a bit oily", "Average {item}, expected more flavour"]
NEU_X = ["Service was acceptable.", "Prices are fair but not cheap.", "Wait time was a little long."]
NEG_FOOD = ["The {item} was cold and bland", "{item} was far too salty", "Very disappointed with the {item}",
            "The {item} was undercooked", "The {item} was oily and tasted stale"]
NEG_X = ["Service was very slow.", "The staff was rude.", "We waited over 40 minutes.", "Our order was incomplete.",
         "Overpriced for the portion.", "Not worth the price."]
NEG_D = ["Delivery took more than an hour.", "Rider was late and the food arrived cold.", "Packaging was leaking."]
item_name_arr = items_df["Item_Name"].values
def make_review(item, rt, dl, a, b, c):
    first, ex = ((POS_FOOD, POS_X + (POS_D if dl else [])) if rt >= 4 else ((NEU_FOOD, NEU_X) if rt == 3 else (NEG_FOOD, NEG_X + (NEG_D if dl else []))))
    s = first[a % len(first)].format(item=item)
    return s + ". " + ex[b % len(ex)] if c % 10 < 7 else s + "."
ra, rb, rc_ = rng.integers(0, 1000, N_RATINGS), rng.integers(0, 1000, N_RATINGS), rng.integers(0, 1000, N_RATINGS)
has_c = rng.random(N_RATINGS) < .60
comments = np.array([make_review(item_name_arr[it_p[k]], rv[k], is_deliv[k], ra[k], rb[k], rc_[k]) if has_c[k] else None
                     for k in range(N_RATINGS)], dtype=object)
ratings_df = pd.DataFrame({"Rating_ID": [f"RT{i+1:07d}" for i in range(N_RATINGS)], "Order_ID": pick["Order_ID"].values,
                           "Customer_ID": pick["Customer_ID"].values, "Item_ID": pick["Item_ID"].values,
                           "Restaurant_ID": pick["Restaurant_ID"].values, "Rating_Value": rv,
                           "Rating_Date": rdate.dt.strftime("%Y-%m-%d").values, "Comment": comments,
                           "Reviewer_Name": pick["Customer_ID"].map(dict(zip(customers_df["Customer_ID"], cust_fullname_short))).values})
print(f"Ratings: {len(ratings_df)} (1-2 star share {(rv <= 2).mean()*100:.1f}%, 3-5 star {(rv >= 3).mean()*100:.1f}%)")

# ===========================================================================
# 9. INVENTORY + 10. WASTAGE (same menu everywhere; wastage differs by branch practices / demand)
# ===========================================================================
months = pd.date_range(START_DATE, END_DATE, freq="MS"); pr, pi = np.where(A)
base_cons = np.maximum(3.0, BW[pr, pi] * rest_busy[pr] * 1.5 + 2.0); parts = []
for mth in months:
    op = np.round(rng.normal(base_cons * 1.3, base_cons * .2), 1); rc2 = np.round(rng.normal(base_cons * 1.1, base_cons * .15), 1)
    cs = np.round(np.clip(rng.normal(base_cons, base_cons * .2), 1, None), 1)
    parts.append(pd.DataFrame({"Restaurant_ID": rest_ids[pr], "Item_ID": item_ids[pi], "Month": mth.date().isoformat(), "Opening_Stock": op,
                               "Received_Stock": rc2, "Consumed_Stock": cs, "Closing_Stock": np.round(np.maximum(op + rc2 - cs, 0), 1),
                               "Unit": [UNIT_BY_CAT[c] for c in item_cat[pi]]}))
inventory_df = pd.concat(parts, ignore_index=True)
inventory_df.insert(0, "Inventory_ID", [f"INV{i+1:07d}" for i in range(len(inventory_df))])
tend = items_df["_wastage_tendency"].values
tend_eff = tend[pi] * rest_wmult[pr]
pair_w = BW[pr, pi] * tend_eff; pair_w = .85 * pair_w / pair_w.sum() + .15 / len(pair_w)
sel = rng.choice(len(pr), N_WASTAGE, p=pair_w); w_r, w_i = pr[sel], pi[sel]
w_day = np.minimum(np.maximum(rng.integers(0, TOTAL_DAYS, N_WASTAGE), item_intro_off[w_i]), TOTAL_DAYS - 1)
mean_prep = 2.0 + BW[w_r, w_i] * .5 * np.sqrt(rest_busy[w_r])
prep = np.round(np.clip(rng.normal(mean_prep, mean_prep * .25), .5, None), 2)
wq = np.round(np.clip(prep * tend[w_i] * rest_wmult[w_r] * rng.uniform(.6, 1.4, N_WASTAGE), .1, None), 2)
wq = np.minimum(wq, np.round(prep * .9, 2))
wastage_df = pd.DataFrame({"Wastage_ID": [f"WST{i+1:07d}" for i in range(N_WASTAGE)], "Restaurant_ID": rest_ids[w_r], "Item_ID": item_ids[w_i],
                           "Date": (pd.Timestamp(START_DATE) + pd.to_timedelta(w_day, unit="D")).strftime("%Y-%m-%d"),
                           "Prepared_Quantity": prep, "Wastage_Quantity": wq, "Unit": [UNIT_BY_CAT[c] for c in item_cat[w_i]],
                           "Reason": rng.choice(["Overproduction", "Spoilage", "Preparation Waste", "Customer Return", "Expired", "Trimming Loss"], N_WASTAGE)})
extra, nxt = [], N_WASTAGE + 1
for _, p_ in promotions_df[promotions_df["_quality"] == "Misleading"].iterrows():
    if pd.isna(p_["Applicable_Category_ID"]): continue
    scope = [rest_idx_of[p_["Applicable_Restaurant_ID"]]] if pd.notna(p_["Applicable_Restaurant_ID"]) else list(range(N_REST))
    s_, e_ = D(p_["Start_Date"]), D(p_["End_Date"]); nd = max((e_ - s_).days, 1); cat_items = np.where(item_cat == p_["Applicable_Category_ID"])[0]
    for _ in range(min(8, nd)):
        r = int(rng.choice(scope)); i = int(rng.choice(cat_items)); m_ = 2.0 + BW[r, i] * .5 * np.sqrt(rest_busy[r])
        pq = round(float(np.clip(rng.normal(m_, m_ * .25), .5, None)), 2)
        wv = min(round(float(pq * tend[i] * rest_wmult[r] * rng.uniform(1.6, 2.4)), 2), round(pq * .85, 2))
        extra.append({"Wastage_ID": f"WST{nxt:07d}", "Restaurant_ID": rest_ids[r], "Item_ID": item_ids[i],
                      "Date": (s_ + timedelta(days=int(rng.integers(0, nd + 1)))).date().isoformat(), "Prepared_Quantity": pq,
                      "Wastage_Quantity": wv, "Unit": UNIT_BY_CAT[item_cat[i]], "Reason": "Overproduction"}); nxt += 1
if extra: wastage_df = pd.concat([wastage_df, pd.DataFrame(extra)], ignore_index=True)
SERVING = {"kg": .4, "liters": .3, "pcs": 1.0}
wastage_df["Wastage_Cost"] = (wastage_df["Wastage_Quantity"] * wastage_df["Item_ID"].map(dict(zip(item_ids, items_df["Base_Cost"]))) / wastage_df["Unit"].map(SERVING)).round(2)
print(f"Inventory: {len(inventory_df)} | Wastage: {len(wastage_df)}")
menu_items_export = items_df[["Item_ID", "Item_Name", "Category_ID", "Base_Cost", "Base_Price", "Introduced_Date", "Is_Active"]].copy()
promotions_export = promotions_df.drop(columns=["_quality", "_usage_prob"]).copy()

# ===========================================================================
# 11. RATING-ANOMALY BURSTS (analytical anomalies; logged in the manifest, not DQ issues)
# ===========================================================================
rating_bursts = []
for k in range(5):
    b = int(rng.integers(0, N_REST)); ip = int(rng.choice(hot_items)); v = 5 if k < 3 else 1
    date = (START_DATE + timedelta(days=int(rng.integers(30, 330)))).date().isoformat()
    rows_ = ratings_df.index[(ratings_df["Restaurant_ID"] == rest_ids[b]).values]; sel_ = rng.choice(rows_, 120, replace=False)
    ratings_df.loc[sel_, ["Item_ID", "Rating_Value", "Rating_Date"]] = [item_ids[ip], v, date]
    ratings_df.loc[sel_, "Comment"] = "Best food ever!" if v == 5 else "Terrible, never again."
    rating_bursts.append({"Item_ID": item_ids[ip], "Restaurant_ID": rest_ids[b], "Value": v, "Date": date, "Rows": 120})

# ===========================================================================
# 12. INTENTIONAL DATA-QUALITY ISSUES + ISSUE LEDGER (one row per corrupted record; <=1 issue per row)
# ===========================================================================
ledger, touched = [], {t: set() for t in ["Customers", "Orders", "Order_Items", "Ratings", "Wastage", "Inventory"]}
def pick(table, df, n, mask=None):
    ok = np.ones(len(df), bool) if mask is None else np.asarray(mask)
    if touched[table]:
        ok = ok & ~np.isin(df.index.values, np.fromiter(touched[table], dtype=np.int64, count=len(touched[table])))
    return rng.choice(df.index.values[ok], size=n, replace=False)
def corrupt(table, df, keycol, ix, col, new, itype, rule):
    ix = list(ix); old = df.loc[ix, col].tolist(); keys = df.loc[ix, keycol].tolist()
    newv = list(new) if isinstance(new, (list, np.ndarray)) else [new] * len(ix)
    df[col] = df[col].astype(object) if any(v is None for v in newv) or isinstance(newv[0], str) else df[col]
    df.loc[ix, col] = pd.Series(newv, index=ix, dtype=object) if df[col].dtype == object else newv
    for k, o, nv in zip(keys, old, newv):
        ledger.append({"Table": table, "Key_Column": keycol, "Key_Value": k, "Column": col, "Issue_Type": itype,
                       "Original_Value": o, "Raw_Value": nv, "Rule": rule})
    touched[table].update(int(i) for i in ix)
def add_dups(table, df, keycol, n_exact, n_near, near_id_fn=None):
    src = pick(table, df, n_exact + n_near); ex, nr = src[:n_exact], src[n_exact:]
    copies = df.loc[src].copy(); base = len(df)
    if n_near:
        copies.iloc[n_exact:, copies.columns.get_loc(keycol)] = [near_id_fn(k) for k in range(n_near)]
    copies.index = np.arange(base, base + len(copies))
    for j, (lbl, row) in enumerate(copies.iterrows()):
        orig_key = df.at[src[j], keycol]; is_near = j >= n_exact
        ledger.append({"Table": table, "Key_Column": keycol, "Key_Value": row[keycol], "Column": "(entire row)",
                       "Issue_Type": "near_duplicate_record" if is_near else "duplicate_record", "Original_Value": "",
                       "Raw_Value": f"{'near_' if is_near else ''}duplicate_of:{orig_key}",
                       "Rule": "same content, new key" if is_near else "exact duplicate row"})
    touched[table].update(int(i) for i in src); touched[table].update(range(base, base + len(copies)))
    return pd.concat([df, copies])

# --- Customers: missing contact values ---
n = len(customers_df)
corrupt("Customers", customers_df, "Customer_ID", pick("Customers", customers_df, int(.03 * n)), "Email", None, "missing_value", "Email left blank")
corrupt("Customers", customers_df, "Customer_ID", pick("Customers", customers_df, int(.02 * n)), "Phone", None, "missing_value", "Phone left blank")
# --- Orders ---
n = len(orders_df)
orders_df = add_dups("Orders", orders_df, "Order_ID", int(.005 * n), 0)
nullpromo = orders_df["Promotion_ID"].isna().values
corrupt("Orders", orders_df, "Order_ID", pick("Orders", orders_df, int(.005 * n)), "Customer_ID", None, "missing_customer_reference", "Customer_ID blank")
ix = pick("Orders", orders_df, int(.002 * n))
corrupt("Orders", orders_df, "Order_ID", ix, "Customer_ID", [f"CUS99{k:04d}" for k in range(len(ix))], "invalid_customer_fk", "Customer_ID not in Customers")
corrupt("Orders", orders_df, "Order_ID", pick("Orders", orders_df, int(.004 * n)), "Channel", None, "missing_value", "Channel blank")
corrupt("Orders", orders_df, "Order_ID", pick("Orders", orders_df, int(.003 * n)), "Restaurant_ID", "R999", "invalid_branch_reference", "Restaurant_ID R999 does not exist")
ix = pick("Orders", orders_df, int(.002 * n))
corrupt("Orders", orders_df, "Order_ID", ix, "Order_Date", list(rng.choice(["2025-02-30", "2025-13-05", "0000-00-00", "2025-04-31"], len(ix))), "invalid_date", "impossible calendar date")
corrupt("Orders", orders_df, "Order_ID", pick("Orders", orders_df, int(.002 * n), nullpromo), "Promotion_ID", "PROMO999", "invalid_promotion_reference", "Promotion_ID not in Promotions")
ix = pick("Orders", orders_df, int(.002 * n), nullpromo); wrong = []
pr_start = dict(zip(promotions_df["Promotion_ID"], promotions_df["Start_Date"])); pr_end = dict(zip(promotions_df["Promotion_ID"], promotions_df["End_Date"]))
for od_ in orders_df.loc[ix, "Order_Date"]:
    while True:
        cand = str(rng.choice(promotions_df["Promotion_ID"]))
        if not (pr_start[cand] <= od_ <= pr_end[cand]): break
    wrong.append(cand)
corrupt("Orders", orders_df, "Order_ID", ix, "Promotion_ID", wrong, "inconsistent_promotion_reference", "valid Promotion_ID but order date outside its campaign window")
# --- Order_Items ---
n = len(order_items_df); nxt_oi = n + 1
order_items_df = add_dups("Order_Items", order_items_df, "Order_Item_ID", int(.003 * n), int(.002 * n), lambda k: f"OI{nxt_oi + k:08d}")
ix = pick("Order_Items", order_items_df, int(.003 * n))
corrupt("Order_Items", order_items_df, "Order_Item_ID", ix, "Unit_Price", list(rng.choice([-500.0, -150.0, 0.0, 99999.99], len(ix))), "invalid_price", "negative/zero/absurd price")
ix = pick("Order_Items", order_items_df, int(.002 * n))
corrupt("Order_Items", order_items_df, "Order_Item_ID", ix, "Quantity", list(-rng.integers(1, 4, len(ix))), "negative_quantity", "quantity < 0")
corrupt("Order_Items", order_items_df, "Order_Item_ID", pick("Order_Items", order_items_df, int(.003 * n)), "Item_ID", None, "missing_menu_reference", "Item_ID blank")
ix = pick("Order_Items", order_items_df, int(.001 * n))
corrupt("Order_Items", order_items_df, "Order_Item_ID", ix, "Item_ID", "M9999", "invalid_menu_fk", "Item_ID not in Menu_Items")
ix = pick("Order_Items", order_items_df, int(.001 * n))
corrupt("Order_Items", order_items_df, "Order_Item_ID", ix, "Order_ID", [f"ORD99{k:05d}" for k in range(len(ix))], "orphan_order_reference", "Order_ID not in Orders")
ix = pick("Order_Items", order_items_df, int(.003 * n))
corrupt("Order_Items", order_items_df, "Order_Item_ID", ix, "Discount_Percent", list(rng.choice([-10.0, 150.0, 200.0], len(ix))), "incorrect_discount", "discount outside 0-100")
ix = pick("Order_Items", order_items_df, int(.003 * n))
corrupt("Order_Items", order_items_df, "Order_Item_ID", ix, "Line_Total",
        [round(float(v) * float(f), 2) for v, f in zip(order_items_df.loc[ix, "Line_Total"], rng.choice([.5, .8, 1.2, 1.5], len(ix)))],
        "incorrect_line_total", "Line_Total != Quantity x Unit_Price x (1 - discount)")
# --- Ratings ---
n = len(ratings_df)
ix = pick("Ratings", ratings_df, int(.005 * n))
corrupt("Ratings", ratings_df, "Rating_ID", ix, "Rating_Value", list(rng.choice([0, -1, 6, 10], len(ix))), "invalid_rating", "rating outside 1-5")
corrupt("Ratings", ratings_df, "Rating_ID", pick("Ratings", ratings_df, int(.004 * n)), "Customer_ID", None, "missing_customer_reference", "Customer_ID blank")
corrupt("Ratings", ratings_df, "Rating_ID", pick("Ratings", ratings_df, int(.001 * n)), "Item_ID", "M9998", "invalid_menu_fk", "Item_ID not in Menu_Items")
corrupt("Ratings", ratings_df, "Rating_ID", pick("Ratings", ratings_df, int(.001 * n)), "Restaurant_ID", "R998", "invalid_branch_reference", "Restaurant_ID does not exist")
# --- Wastage / Inventory ---
n = len(wastage_df)
ix = pick("Wastage", wastage_df, int(.005 * n))
corrupt("Wastage", wastage_df, "Wastage_ID", ix, "Wastage_Quantity", (wastage_df.loc[ix, "Prepared_Quantity"] * rng.uniform(1.15, 1.6, len(ix))).round(2).tolist(), "impossible_wastage", "Wastage_Quantity > Prepared_Quantity")
UNIT_VARIANTS = {"kg": ["Kg", "KG", "kilograms", "lbs", "grams"], "liters": ["Liters", "L", "Litre", "LTR"], "pcs": ["PCS", "Pieces", "pieces", "Pcs"]}
for tname, df_, key_ in [("Wastage", wastage_df, "Wastage_ID"), ("Inventory", inventory_df, "Inventory_ID")]:
    ix = pick(tname, df_, int(.01 * len(df_)))
    corrupt(tname, df_, key_, ix, "Unit", [str(rng.choice(UNIT_VARIANTS[u])) for u in df_.loc[ix, "Unit"]], "inconsistent_units", "unit label variant")
corrupt("Wastage", wastage_df, "Wastage_ID", pick("Wastage", wastage_df, int(.002 * len(wastage_df))), "Item_ID", None, "missing_menu_reference", "Item_ID blank")
corrupt("Inventory", inventory_df, "Inventory_ID", pick("Inventory", inventory_df, int(.01 * len(inventory_df))), "Consumed_Stock", None, "missing_value", "Consumed_Stock blank")
ledger_df = pd.DataFrame(ledger); ledger_df.insert(0, "Issue_ID", [f"ISS{i+1:06d}" for i in range(len(ledger_df))])
ledger_df.to_csv(os.path.join(P["reports"], "issue_ledger.csv"), index=False)
print(f"Issue ledger: {len(ledger_df)} intentional issue records"); print(ledger_df.groupby("Issue_Type").size().to_dict())

# ===========================================================================
# 13. RAW DATA - written ONCE, checksummed, made read-only
# ===========================================================================
RAW_TABLES = {"Customers": customers_df, "Restaurants": restaurants_df, "Menu_Categories": categories_df, "Menu_Items": menu_items_export,
              "Menu_Item_Availability": availability_df, "Pricing_History": pricing_history_df, "Promotions": promotions_export,
              "Orders": orders_df, "Order_Items": order_items_df, "Ratings": ratings_df, "Inventory": inventory_df, "Wastage": wastage_df}
for f in os.listdir(P["raw_data"]):
    fp = os.path.join(P["raw_data"], f); os.chmod(fp, 0o644); os.remove(fp)
sha = {}
for t, df in RAW_TABLES.items():
    fp = os.path.join(P["raw_data"], f"{t}.csv"); df.to_csv(fp, index=False)
    sha[f"{t}.csv"] = hashlib.sha256(open(fp, "rb").read()).hexdigest()
    print(f"RAW {t}.csv: {len(df)} rows x {df.shape[1]} cols")
json.dump(sha, open(os.path.join(P["reports"], "raw_data_checksums.json"), "w"), indent=2)
for t in RAW_TABLES: os.chmod(os.path.join(P["raw_data"], f"{t}.csv"), 0o444)

# ===========================================================================
# 14. CLEANING PIPELINE - reads RAW FROM DISK; bad records are QUARANTINED (not silently deleted)
# ===========================================================================
R = {t: pd.read_csv(os.path.join(P["raw_data"], f"{t}.csv"), low_memory=False) for t in RAW_TABLES}
clog, Q = [], {}
def lc(t, rule, n): clog.append({"Table": t, "Rule": rule, "Records_Affected": int(n)})
def quarantine(table, df, mask, reason):
    mask = pd.Series(mask, index=df.index) if not isinstance(mask, pd.Series) else mask
    if mask.sum():
        q = df[mask].copy(); q["Quarantine_Reason"] = reason; Q.setdefault(table, []).append(q)
    lc(table, f"Quarantined: {reason}", mask.sum()); return df[~mask]
UNIT_CANON = {v: k for k, vs in UNIT_VARIANTS.items() for v in vs}

c_orders = R["Orders"].copy()
c_orders = quarantine("Orders", c_orders, c_orders.duplicated("Order_ID", keep="first"), "duplicate Order_ID")
c_orders = quarantine("Orders", c_orders, pd.to_datetime(c_orders["Order_Date"], errors="coerce").isna(), "invalid/impossible Order_Date")
c_orders = quarantine("Orders", c_orders, c_orders["Customer_ID"].isna(), "missing Customer_ID")
c_orders = quarantine("Orders", c_orders, ~c_orders["Customer_ID"].isin(R["Customers"]["Customer_ID"]), "Customer_ID not in Customers (invalid FK)")
c_orders = quarantine("Orders", c_orders, ~c_orders["Restaurant_ID"].isin(R["Restaurants"]["Restaurant_ID"]), "invalid branch reference")
bad_ref = c_orders["Promotion_ID"].notna() & ~c_orders["Promotion_ID"].isin(R["Promotions"]["Promotion_ID"])
lc("Orders", "Fixed: invalid Promotion_ID reference set to NULL", bad_ref.sum()); c_orders.loc[bad_ref, "Promotion_ID"] = np.nan
pm = R["Promotions"].set_index("Promotion_ID")
has_p = c_orders["Promotion_ID"].notna()
outside = has_p & ((c_orders["Order_Date"] < c_orders["Promotion_ID"].map(pm["Start_Date"])) | (c_orders["Order_Date"] > c_orders["Promotion_ID"].map(pm["End_Date"])))
lc("Orders", "Fixed: Promotion_ID outside campaign window set to NULL", outside.sum()); c_orders.loc[outside, "Promotion_ID"] = np.nan
lc("Orders", "Fixed: missing Channel imputed as 'Unknown'", c_orders["Channel"].isna().sum()); c_orders["Channel"] = c_orders["Channel"].fillna("Unknown")

c_oi = R["Order_Items"].copy()
c_oi = quarantine("Order_Items", c_oi, c_oi.duplicated("Order_Item_ID", keep="first"), "exact duplicate order line")
c_oi = quarantine("Order_Items", c_oi, c_oi.duplicated(["Order_ID", "Item_ID", "Quantity", "Unit_Price"], keep="first"), "near-duplicate order line (new key, same content)")
c_oi = quarantine("Order_Items", c_oi, (c_oi["Unit_Price"] <= 0) | (c_oi["Unit_Price"] > MAX_VALID_PRICE), "invalid Unit_Price")
c_oi = quarantine("Order_Items", c_oi, c_oi["Quantity"] <= 0, "negative/zero Quantity")
c_oi = quarantine("Order_Items", c_oi, c_oi["Item_ID"].isna(), "missing Item_ID")
c_oi = quarantine("Order_Items", c_oi, ~c_oi["Item_ID"].isin(R["Menu_Items"]["Item_ID"]), "Item_ID not in Menu_Items (invalid FK)")
c_oi = quarantine("Order_Items", c_oi, ~c_oi["Order_ID"].isin(R["Orders"]["Order_ID"]), "orphan Order_ID (not in raw Orders)")
c_oi = quarantine("Order_Items", c_oi, ~c_oi["Order_ID"].isin(c_orders["Order_ID"]), "parent order quarantined")
exp_total = (c_oi["Quantity"] * c_oi["Unit_Price"] * (1 - c_oi["Discount_Percent"] / 100)).round(2)
valid_disc = c_oi["Discount_Percent"].between(0, 100)
lc("Order_Items", "Fixed: incorrect Line_Total recomputed", ((c_oi["Line_Total"] - exp_total).abs() > 0.02)[valid_disc].sum())
lc("Order_Items", "Fixed: Discount_Percent clipped to [0,100]", (~valid_disc).sum())
c_oi["Discount_Percent"] = c_oi["Discount_Percent"].clip(0, 100)
c_oi["Line_Subtotal"] = (c_oi["Quantity"] * c_oi["Unit_Price"]).round(2)
c_oi["Line_Total"] = (c_oi["Line_Subtotal"] * (1 - c_oi["Discount_Percent"] / 100)).round(2)
c_orders = order_totals(c_orders, c_oi); lc("Orders", "Recomputed Item_Count/Subtotal/Total/Tax/Grand_Total from cleaned lines", len(c_orders))

c_rt = R["Ratings"].copy()
c_rt = quarantine("Ratings", c_rt, ~c_rt["Rating_Value"].between(1, 5), "invalid rating (outside 1-5)")
c_rt = quarantine("Ratings", c_rt, c_rt["Customer_ID"].isna(), "missing Customer_ID")
c_rt = quarantine("Ratings", c_rt, ~c_rt["Order_ID"].isin(c_orders["Order_ID"]), "Order_ID not in cleaned Orders (invalid FK)")
c_rt = quarantine("Ratings", c_rt, ~c_rt["Item_ID"].isin(R["Menu_Items"]["Item_ID"]), "Item_ID not in Menu_Items (invalid FK)")
c_rt = quarantine("Ratings", c_rt, ~c_rt["Restaurant_ID"].isin(R["Restaurants"]["Restaurant_ID"]), "invalid branch reference")
c_wa = R["Wastage"].copy()
imp = c_wa["Wastage_Quantity"] > c_wa["Prepared_Quantity"]; lc("Wastage", "Fixed: impossible Wastage_Quantity capped at Prepared_Quantity", imp.sum())
c_wa["Wastage_Quantity"] = np.minimum(c_wa["Wastage_Quantity"], c_wa["Prepared_Quantity"])
lc("Wastage", "Fixed: inconsistent Unit labels standardised", c_wa["Unit"].isin(UNIT_CANON).sum()); c_wa["Unit"] = c_wa["Unit"].replace(UNIT_CANON)
c_wa = quarantine("Wastage", c_wa, c_wa["Item_ID"].isna(), "missing Item_ID")
c_iv = R["Inventory"].copy()
lc("Inventory", "Fixed: inconsistent Unit labels standardised", c_iv["Unit"].isin(UNIT_CANON).sum()); c_iv["Unit"] = c_iv["Unit"].replace(UNIT_CANON)
lc("Inventory", "Fixed: missing Consumed_Stock imputed (item-branch median)", c_iv["Consumed_Stock"].isna().sum())
c_iv["Consumed_Stock"] = c_iv.groupby(["Restaurant_ID", "Item_ID"])["Consumed_Stock"].transform(lambda s: s.fillna(s.median()))
c_iv["Consumed_Stock"] = c_iv["Consumed_Stock"].fillna(c_iv["Consumed_Stock"].median())
CLEANED = dict(R); CLEANED.update({"Orders": c_orders, "Order_Items": c_oi, "Ratings": c_rt, "Wastage": c_wa, "Inventory": c_iv})
for t, df in CLEANED.items(): df.to_csv(os.path.join(P["processed_data"], f"{t}.csv"), index=False)
for t, lst in Q.items(): pd.concat(lst).to_csv(os.path.join(P["processed_data"], "quarantine", f"{t}_quarantine.csv"), index=False)
pd.DataFrame(clog).to_csv(os.path.join(P["reports"], "cleaning_log.csv"), index=False)
print(f"Cleaned: orders {len(c_orders):,} | lines {len(c_oi):,} | ratings {len(c_rt):,} | wastage {len(c_wa):,}; quarantined tables: {list(Q)}")

# ===========================================================================
# 15. PARQUET + CHRONOLOGICAL SPLIT + SAMPLES
# ===========================================================================
pq = {}
for t, df in CLEANED.items():
    fp = os.path.join(P["parquet_data"], "tables", f"{t}.parquet"); df.to_parquet(fp, index=False)
    rb = pd.read_parquet(fp); pq[t] = {"rows_written": len(df), "rows_read_back": len(rb), "ok": bool(len(rb) == len(df) and list(rb.columns) == list(df.columns))}
json.dump(pq, open(os.path.join(P["reports"], "parquet_verification.json"), "w"), indent=2)
co = c_orders.assign(_dt=pd.to_datetime(c_orders["Order_Date"]))
for s, so in {"train": co[co["_dt"] < "2025-09-01"], "validation": co[(co["_dt"] >= "2025-09-01") & (co["_dt"] < "2025-11-01")],
              "test": co[co["_dt"] >= "2025-11-01"]}.items():
    so = so.drop(columns=["_dt"]); li = c_oi[c_oi["Order_ID"].isin(set(so["Order_ID"]))]
    for base, ext in [(os.path.join(P["processed_data"], "splits", s), "csv"), (os.path.join(P["parquet_data"], "splits", s), "parquet")]:
        os.makedirs(base, exist_ok=True)
        for nm, d_ in [("Orders", so), ("Order_Items", li)]:
            (d_.to_csv(os.path.join(base, f"{nm}.csv"), index=False) if ext == "csv" else d_.to_parquet(os.path.join(base, f"{nm}.parquet"), index=False))
    print(f"split {s}: {len(so):,} orders / {len(li):,} lines")
for sub in ["raw_sample", "cleaned_sample"]: os.makedirs(os.path.join(P["sample_data"], sub), exist_ok=True)
keycol_of = {"Orders": "Order_ID", "Order_Items": "Order_Item_ID", "Ratings": "Rating_ID", "Wastage": "Wastage_ID", "Inventory": "Inventory_ID", "Customers": "Customer_ID"}
for t, df in R.items():
    smp = df.sample(min(500, len(df)), random_state=SEED)
    if t in keycol_of:   # include a slice of known-dirty records so the sample shows the real messiness
        dirty = df[df[keycol_of[t]].isin(set(ledger_df.loc[ledger_df["Table"] == t, "Key_Value"]))].head(100)
        smp = pd.concat([smp, dirty]).drop_duplicates()
    smp.to_csv(os.path.join(P["sample_data"], "raw_sample", f"{t}_sample.csv"), index=False)
    CLEANED[t].sample(min(500, len(CLEANED[t])), random_state=SEED).to_csv(os.path.join(P["sample_data"], "cleaned_sample", f"{t}_sample.csv"), index=False)
print("Parquet, splits and samples written.")

# ===========================================================================
# 16. DOCUMENTATION, DATABASE SCHEMA, CONFIG, MANIFEST, ROOT FILES
# ===========================================================================
S = {
 "Customers": ("Customer_ID", {"Home_Restaurant_ID": "Restaurants.Restaurant_ID"}, {"Customer_ID": "PK", "First_Name": "synthetic Pakistani first name",
   "Last_Name": "synthetic surname", "Gender": "Male/Female", "Age": "years (16-75)", "City": "always Karachi", "Locality": "Karachi locality",
   "Home_Restaurant_ID": "FK (usual branch)", "Signup_Date": "date", "Email": "synthetic @example.com; some blank (DQ)", "Phone": "synthetic +92-399 (unallocated prefix); some blank (DQ)"}),
 "Restaurants": ("Restaurant_ID", {}, {"Restaurant_ID": "PK (22 Karachi branches)", "Chain_Name": "single chain name", "Restaurant_Name": "chain - locality",
   "Locality": "Karachi locality", "District": "Karachi district", "Address": "synthetic commercial address", "City": "Karachi", "Province": "Sindh",
   "Country": "Pakistan", "Location_Type": "string", "Opening_Date": "date", "Seating_Capacity": "int"}),
 "Menu_Categories": ("Category_ID", {}, {"Category_ID": "PK", "Category_Name": "string (12 categories)"}),
 "Menu_Items": ("Item_ID", {"Category_ID": "Menu_Categories.Category_ID"}, {"Item_ID": "PK (same menu at every branch)", "Item_Name": "string",
   "Category_ID": "FK", "Base_Cost": "PKR cost per serving", "Base_Price": "PKR initial chain-wide price", "Introduced_Date": "date", "Is_Active": "bool"}),
 "Menu_Item_Availability": ("Availability_ID", {"Restaurant_ID": "Restaurants.Restaurant_ID", "Item_ID": "Menu_Items.Item_ID"},
   {"Availability_ID": "PK", "Restaurant_ID": "FK", "Item_ID": "FK", "Is_Available": "bool (all true: identical menu)", "Available_From": "date (supplementary)"}),
 "Pricing_History": ("Pricing_ID", {"Item_ID": "Menu_Items.Item_ID"}, {"Pricing_ID": "PK", "Item_ID": "FK", "Price": "PKR, CHAIN-WIDE price valid from Effective_Date",
   "Previous_Price": "PKR, NULL for first price", "Effective_Date": "date", "End_Date": "date, NULL = current"}),
 "Promotions": ("Promotion_ID", {"Applicable_Category_ID": "Menu_Categories (nullable = all)", "Applicable_Restaurant_ID": "Restaurants (nullable = chain-wide)"},
   {"Promotion_ID": "PK", "Promotion_Name": "string", "Discount_Percent": "int 5-50", "Start_Date": "date", "End_Date": "date",
    "Applicable_Category_ID": "nullable FK", "Applicable_Restaurant_ID": "nullable FK"}),
 "Orders": ("Order_ID", {"Customer_ID": "Customers (DQ: blank / unknown)", "Restaurant_ID": "Restaurants (DQ: invalid)", "Promotion_ID": "Promotions (DQ: invalid / outside window)"},
   {"Order_ID": "PK (duplicated for ~0.5% - DQ)", "Customer_ID": "FK", "Restaurant_ID": "FK", "Order_DateTime": "datetime", "Order_Date": "date text (some impossible - DQ)",
    "Channel": "Dine-in/Takeaway/Website/App/Third-Party Delivery/Phone Order (some blank - DQ)", "Payment_Method": "Cash/Card/Online", "Promotion_ID": "nullable FK",
    "Status": "Completed/Cancelled/Refunded/Pending", "Delivery_Charge": "PKR", "Item_Count": "derived", "Subtotal": "PKR derived", "Total_Amount": "PKR derived (after discount)",
    "Discount_Amount": "PKR derived", "Tax_Amount": "synthetic sales tax: 13% cash, 8% card/online", "Grand_Total": "Total + Tax + Delivery"}),
 "Order_Items": ("Order_Item_ID", {"Order_ID": "Orders.Order_ID (DQ: orphan)", "Item_ID": "Menu_Items.Item_ID (DQ: blank / unknown)"},
   {"Order_Item_ID": "PK (exact + near duplicates - DQ)", "Order_ID": "FK", "Item_ID": "FK", "Quantity": "int (some negative - DQ)", "Unit_Price": "PKR chain-wide price at order date (some invalid - DQ)",
    "Discount_Percent": "float (some out of range - DQ)", "Line_Subtotal": "derived", "Line_Total": "derived (some incorrect - DQ)"}),
 "Ratings": ("Rating_ID", {"Order_ID": "Orders", "Customer_ID": "Customers (DQ: blank)", "Item_ID": "Menu_Items (DQ)", "Restaurant_ID": "Restaurants (DQ)"},
   {"Rating_ID": "PK", "Order_ID": "FK", "Customer_ID": "FK", "Item_ID": "FK", "Restaurant_ID": "FK", "Rating_Value": "int 1-5 expected (some invalid - DQ)",
    "Rating_Date": "date", "Comment": "synthetic review text, nullable", "Reviewer_Name": "synthetic 'First L.'"}),
 "Inventory": ("Inventory_ID", {"Restaurant_ID": "Restaurants", "Item_ID": "Menu_Items"}, {"Inventory_ID": "PK", "Restaurant_ID": "FK", "Item_ID": "FK", "Month": "date",
   "Opening_Stock": "float", "Received_Stock": "float", "Consumed_Stock": "float (some blank - DQ)", "Closing_Stock": "float", "Unit": "kg/liters/pcs (variants - DQ)"}),
 "Wastage": ("Wastage_ID", {"Restaurant_ID": "Restaurants", "Item_ID": "Menu_Items (DQ: blank)"}, {"Wastage_ID": "PK", "Restaurant_ID": "FK", "Item_ID": "FK", "Date": "date",
   "Prepared_Quantity": "preparation quantity (denominator of Wastage %)", "Wastage_Quantity": "may exceed Prepared_Quantity (DQ)", "Unit": "kg/liters/pcs (variants - DQ)",
   "Reason": "string", "Wastage_Cost": "PKR derived"}),
}
for t_, (pk_, fk_, cols_) in S.items():
    assert set(cols_) == set(RAW_TABLES[t_].columns), f"schema doc mismatch in {t_}: {set(cols_) ^ set(RAW_TABLES[t_].columns)}"
with open(os.path.join(P["documentation"], "data_dictionary.md"), "w") as f:
    f.write(f"# Data Dictionary - {CHAIN_NAME}, Karachi (PKR, synthetic data)\n\n")
    for t_, (pk_, fk_, cols_) in S.items():
        f.write(f"## {t_}\n\n**Primary Key:** `{pk_}`\n\n")
        if fk_: f.write("**Foreign Keys:** " + "; ".join(f"`{c}` -> `{r}`" for c, r in fk_.items()) + "\n\n")
        f.write("| Column | Description |\n|---|---|\n" + "".join(f"| {c} | {d} |\n" for c, d in cols_.items()) + "\n")
with open(os.path.join(P["documentation"], "pk_fk_definitions.md"), "w") as f:
    f.write("# PK / FK Definitions\n\n| Table | Primary Key | Foreign Keys |\n|---|---|---|\n")
    for t_, (pk_, fk_, _) in S.items(): f.write(f"| {t_} | {pk_} | {'; '.join(f'{c} -> {r}' for c, r in fk_.items()) or '-'} |\n")
with open(os.path.join(P["documentation"], "dataset_statistics.md"), "w") as f:
    f.write("# Dataset Statistics\n\n| Table (raw) | Rows | Columns |\n|---|---:|---:|\n")
    for t_, df_ in RAW_TABLES.items(): f.write(f"| {t_} | {len(df_):,} | {df_.shape[1]} |\n")
    f.write("\n| Table (processed) | Rows |\n|---|---:|\n" + "".join(f"| {t_} | {len(d_):,} |\n" for t_, d_ in CLEANED.items()))
    f.write(f"\n- Period: {START_DATE.date()} to {END_DATE.date()} (12 months)\n- Branches: {N_REST}, all in Karachi, Sindh\n- Menu: {N_ITEMS} items in {len(categories_df)} categories, identical at every branch\n"
            f"- Issue ledger: {len(ledger_df):,} intentional raw-data issue records (reports/issue_ledger.csv)\n")
with open(os.path.join(P["documentation"], "karachi_branches.md"), "w") as f:
    f.write(f"# {CHAIN_NAME} - Karachi branches (synthetic commercial addresses)\n\n| ID | Branch | District | Address |\n|---|---|---|---|\n")
    for _, r_ in restaurants_df.iterrows(): f.write(f"| {r_['Restaurant_ID']} | {r_['Locality']} | {r_['District']} | {r_['Address']} |\n")
    f.write("\nAll branches share ONE menu and ONE chain-wide price list. Branches differ only through demand, customers, channels, wastage, ratings and promotion response.\n")
with open(os.path.join(P["documentation"], "issue_ledger.md"), "w") as f:
    f.write("# Issue ledger\n\n`reports/issue_ledger.csv` has one row per intentionally corrupted raw record (Issue_ID, Table, Key_Column, Key_Value, Column, Issue_Type, "
            "Original_Value, Raw_Value, Rule). Each raw row carries at most one injected issue. `raw_data/` is read-only and checksummed "
            "(`reports/raw_data_checksums.json`); cleaning reads it and writes `processed_data/` (fixes) and `processed_data/quarantine/` (removed records with reasons). "
            "`reports/cleaning_log.csv` lists every cleaning rule and its record count.\n")
with open(os.path.join(P["documentation"], "hidden_data_readiness.md"), "w") as f:
    f.write("# Hidden-Data Readiness\n\nThe generator is seed-controlled and parameterised (branches, menu, DQ rates, promotions). Re-running with another seed or extra branches/items/rates "
            "simulates a hidden dataset; schema, PK/FK structure and the issue types stay stable. Pipelines must tolerate unknown items/branches, price changes, unusual promotions, "
            "extreme wastage, seasonal shifts, outliers and duplicates.\n")
def ddl_type(c):
    if c.endswith("_ID"): return "VARCHAR(20)"
    if c in ("Is_Available", "Is_Active"): return "BOOLEAN"
    if c == "Order_DateTime": return "TIMESTAMP"
    if c.endswith("Date") or c in ("Date", "Month"): return "DATE"
    if c in ("Quantity", "Age", "Item_Count", "Seating_Capacity", "Rating_Value"): return "INTEGER"
    if any(k in c for k in ("Price", "Cost", "Amount", "Total", "Subtotal", "Charge", "Stock", "Quantity", "Percent")): return "NUMERIC(12,2)"
    return "VARCHAR(255)"
with open(os.path.join(P["database"], "schema.sql"), "w") as f:
    f.write(f"-- {CHAIN_NAME} (Karachi) - schema for the CLEANED data (raw_data holds intentionally invalid values)\n")
    for t_, (pk_, fk_, cols_) in S.items():
        lines = [f"  {c} {ddl_type(c)}" + (" PRIMARY KEY" if c == pk_ else "") for c in cols_]
        lines += [f"  FOREIGN KEY ({c}) REFERENCES {r.split(' ')[0].split('.')[0]}({S[r.split(' ')[0].split('.')[0]][0]})" for c, r in fk_.items()]
        f.write(f"\nCREATE TABLE {t_} (\n" + ",\n".join(lines) + "\n);\n")
json.dump({"seed": SEED, "chain": CHAIN_NAME, "branches": N_REST, "customers": N_CUSTOMERS, "orders": N_ORDERS, "ratings": N_RATINGS, "wastage": N_WASTAGE,
           "period": [START_DATE.date().isoformat(), END_DATE.date().isoformat()], "max_valid_price_pkr": MAX_VALID_PRICE,
           "tax_rule": {"cash": 0.13, "card_or_online": 0.08}, "split": {"train": "< 2025-09-01", "validation": "2025-09-01..2025-10-31", "test": ">= 2025-11-01"},
           "dq_rates": "0.1%-3% per issue type; see reports/issue_ledger.csv"}, open(os.path.join(P["config"], "dataset_config.json"), "w"), indent=2)
json.dump({"NOTE": "INSTRUCTOR ANSWER KEY - generation parameters used only for validation; the data itself carries no analytical labels.",
           "popular_low_margin": item_ids[pop_low_margin_idx].tolist(), "profitable_low_selling": item_ids[prof_low_sell_idx].tolist(),
           "high_wastage_items": item_ids[hi_waste_idx].tolist(), "price_sensitive": sorted(flag_ids), "new_items": item_ids[new_idx].tolist(),
           "poorly_rated": item_ids[poor_idx].tolist(), "promotion_quality": dict(zip(promotions_df["Promotion_ID"], promotions_df["_quality"])),
           "sales_anomalies": anomaly_records, "rating_bursts": rating_bursts, "price_elasticity_windows": elasticity_log[:300],
           "branch_parameters": {rest_ids[i]: {"locality": rest_locality[i], "busy": rest_busy[i], "profile": rest_profile[i], "age_mean": int(rest_age[i]),
                                               "female_share": rest_fem[i], "price_sensitivity": rest_psens[i], "wastage_multiplier": rest_wmult[i],
                                               "rating_bias": rest_rbias[i], "promo_response": rest_promo[i], "channel_mix": rest_chan[i].tolist()} for i in range(N_REST)}},
          open(os.path.join(P["reports"], "generation_manifest.json"), "w"), indent=1, default=str)
for d_ in ["src", "templates", "static", "spark_jobs", "spark_sql", "python_pipeline", "notebooks", "models", "screenshots"]:
    os.makedirs(os.path.join(ROOT, d_), exist_ok=True); open(os.path.join(ROOT, d_, ".gitkeep"), "w").close()
open(os.path.join(ROOT, "requirements.txt"), "w").write(f"pandas=={pd.__version__}\nnumpy=={np.__version__}\npyarrow>=14\n")
open(os.path.join(ROOT, "LICENSE"), "w").write("""MIT License

Copyright (c) 2025 DineIQ Analytics Team

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
""")
open(os.path.join(ROOT, "AI_USAGE.md"), "w").write("""# AI Usage Declaration

| Field | Declaration |
|---|---|
| Name of AI tool | Claude (Anthropic) |
| Purpose of use | Design and writing of the synthetic dataset generator, validator, data-quality/cleaning pipeline and dataset documentation |
| Type of assistance requested | Code generation, schema design, synthetic-data design, validation logic, documentation |
| Files / modules affected | `data_generator/generate_dataset.py`, `data_generator/validate_dataset.py`, `tests/test_dataset_validation.py`, `documentation/*`, `database/schema.sql`, `config/dataset_config.json`, `README.md`, `requirements.txt`, `LICENSE` |
| Modifications performed | Generator and validator were iterated several times after validation failures (e.g. price-elasticity strength, wastage coverage, promotion demand lift, spike pricing) |
| Testing completed | `python data_generator/validate_dataset.py` (see `reports/validation_report.md`); `pytest tests/` |
| Team member who verified the output | **TO BE COMPLETED BY THE TEAM** - a team member must review, understand and test this code before submission (SRS 1.8 items 15 and 17) |

AI-generated code must be independently reviewed, modified where required, understood and tested by team members. No external generative-AI API is used to produce any analytics or data.
""")
open(os.path.join(ROOT, "README.md"), "w").write(f"""# DineIQ Analytics - {CHAIN_NAME} (Karachi) synthetic dataset

ONE Pakistani restaurant chain, {N_REST} branches, all in Karachi. **Same menu + same chain-wide prices at every branch; different demand, customers, wastage, ratings,
promotion response and order channels per branch.** All data is synthetic (names, e-mails @example.com, phones +92-399 unallocated).

## Layout
- `raw_data/` - 12 tables, intentionally messy, READ-ONLY, checksummed (`reports/raw_data_checksums.json`)
- `processed_data/` - cleaned tables, `quarantine/` (removed records + reason), `splits/` (chronological train/validation/test CSV)
- `parquet_data/` - cleaned tables and splits in Parquet
- `sample_data/` - raw and cleaned samples (raw sample includes known-dirty records)
- `data_generator/` - `generate_dataset.py` (seeded), `validate_dataset.py` (independent validator reading files from disk)
- `reports/` - `issue_ledger.csv`, `cleaning_log.csv`, validation reports, checksums, instructor manifest (answer key)
- `documentation/`, `database/schema.sql`, `config/`, `tests/`; `src/ templates/ static/ spark_jobs/ spark_sql/ python_pipeline/ notebooks/ models/ screenshots/` are reserved for the application deliverables

## Run
```
pip install -r requirements.txt
python data_generator/generate_dataset.py      # regenerate everything (same seed = same data)
python data_generator/validate_dataset.py      # measured validation + final flags
```
Raw files are made read-only; the generator replaces them only when you re-run it deliberately.
""")
print("Documentation, schema, config, manifest and root files written.")
