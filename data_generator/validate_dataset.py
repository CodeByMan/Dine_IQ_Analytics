import os, re, json, hashlib, stat, sys, tempfile
import numpy as np, pandas as pd

ROOT = os.environ.get("DINEIQ_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAWD, PROC, REP = (os.path.join(ROOT, d) for d in ("raw_data", "processed_data", "reports"))
MAX_PRICE = 10_000
TABLES = ["Customers", "Restaurants", "Menu_Categories", "Menu_Items", "Menu_Item_Availability", "Pricing_History", "Promotions",
          "Orders", "Order_Items", "Ratings", "Inventory", "Wastage"]
REQ11 = ["Customers", "Orders", "Order_Items", "Menu_Items", "Menu_Categories", "Restaurants", "Pricing_History", "Promotions", "Ratings", "Inventory", "Wastage"]

def read_json(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)

def cramers_v(a, b):
    ct = pd.crosstab(a, b).values.astype(float); n = ct.sum()
    exp = ct.sum(1, keepdims=True) * ct.sum(0, keepdims=True) / n
    chi2 = ((ct - exp) ** 2 / exp).sum(); k = min(ct.shape) - 1
    return float(np.sqrt(chi2 / (n * k))) if k > 0 else 0.0

def main():
    R = {t: pd.read_csv(os.path.join(RAWD, f"{t}.csv"), low_memory=False) for t in TABLES}
    C = {t: pd.read_csv(os.path.join(PROC, f"{t}.csv"), low_memory=False) for t in TABLES}
    led = pd.read_csv(os.path.join(REP, "issue_ledger.csv"), dtype=str, keep_default_na=False)
    man = read_json(os.path.join(REP, "generation_manifest.json"))
    res, flags = {}, {}
    rs, items, av, ph = R["Restaurants"], R["Menu_Items"], R["Menu_Item_Availability"], R["Pricing_History"]

    # ---------------- volumes / structure / relationships ----------------
    o_raw = R["Orders"]; vol = {
        "Customers": (len(R["Customers"]), 50_000), "Orders_unique": (o_raw["Order_ID"].nunique(), 100_000),
        "Order_Items_raw": (len(R["Order_Items"]), 1_000_000), "Order_Items_cleaned": (len(C["Order_Items"]), 1_000_000),
        "Menu_Items": (len(items), 150), "Menu_Categories": (len(R["Menu_Categories"]), 10), "Restaurants": (len(rs), 20),
        "Ratings": (len(R["Ratings"]), 100_000), "Wastage": (len(R["Wastage"]), 50_000)}
    months = (pd.to_datetime(C["Orders"]["Order_Date"]).max() - pd.to_datetime(C["Orders"]["Order_Date"]).min()).days / 30.44
    res["volume"] = {k: {"actual": int(a), "minimum": m, "pass": bool(a >= m)} for k, (a, m) in vol.items()}
    res["volume"]["months_of_history"] = {"actual": round(months, 1), "minimum": 12, "pass": bool(months >= 11.9)}
    pricing_counts = ph.groupby("Item_ID").size()
    menu_item_ids = set(items["Item_ID"])
    pricing_item_ids = set(pricing_counts.index)
    all_items_have_price = menu_item_ids.issubset(pricing_item_ids)

    introduced_dates = pd.to_datetime(
        items.set_index("Item_ID")["Introduced_Date"],
        errors="coerce",
    )
    latest_order_date = pd.to_datetime(
        C["Orders"]["Order_Date"],
        errors="coerce",
    ).max()
    history_cutoff = latest_order_date - pd.Timedelta(days=90)
    history_eligible_ids = introduced_dates[
        introduced_dates <= history_cutoff
    ].index
    eligible_counts = pricing_counts.reindex(
        history_eligible_ids
    ).fillna(0)
    eligible_history_ok = bool((eligible_counts >= 1).all())

    res["volume"]["pricing_rows_per_item_min"] = {
        "actual": int(pricing_counts.min()),
        "minimum": 1,
        "history_cutoff": str(history_cutoff.date()),
        "late_introduction_items": int(
            (introduced_dates > history_cutoff).sum()
        ),
        "all_items_have_price": bool(all_items_have_price),
        "eligible_history_coverage": bool(eligible_history_ok),
        "pass": bool(all_items_have_price and eligible_history_ok),
    }
    res["volume"]["promotions"] = {"actual": len(R["Promotions"]), "minimum": 2, "pass": len(R["Promotions"]) > 1}
    res["structure"] = {"all_11_required_tables_present": all(t in R for t in REQ11)}
    cm, cc, ci, cr, cw = C["Menu_Items"], C["Customers"], C["Order_Items"], C["Ratings"], C["Wastage"]; co = C["Orders"]
    rel = {"Orders-Customers": co["Customer_ID"].isin(cc["Customer_ID"]), "Orders-Order_Items": ci["Order_ID"].isin(co["Order_ID"]),
           "Order_Items-Menu_Items": ci["Item_ID"].isin(cm["Item_ID"]), "Menu_Items-Menu_Categories": cm["Category_ID"].isin(C["Menu_Categories"]["Category_ID"]),
           "Orders-Restaurants": co["Restaurant_ID"].isin(C["Restaurants"]["Restaurant_ID"]),
           "Orders-Promotions": co.loc[co["Promotion_ID"].notna(), "Promotion_ID"].isin(C["Promotions"]["Promotion_ID"]),
           "Menu_Items-Pricing_History": C["Pricing_History"]["Item_ID"].isin(cm["Item_ID"]),
           "Ratings-Orders": cr["Order_ID"].isin(co["Order_ID"]),
           "Ratings-Customers": cr["Customer_ID"].isin(cc["Customer_ID"]),
           "Menu_Items-Ratings": cr["Item_ID"].isin(cm["Item_ID"]),
           "Ratings-Restaurants": cr["Restaurant_ID"].isin(C["Restaurants"]["Restaurant_ID"]),
           "Menu_Items-Inventory": C["Inventory"]["Item_ID"].isin(cm["Item_ID"]), "Menu_Items-Wastage": cw["Item_ID"].isin(cm["Item_ID"])}
    res["relationships_cleaned"] = {k: {"integrity_pct": round(float(v.mean()) * 100, 4), "pass": bool(v.mean() > .9999)} for k, v in rel.items()}

    # ---------------- 1-4: chain / Karachi ----------------
    flags["RESTAURANT_CHAIN_COUNT"] = (int(rs["Chain_Name"].nunique()), rs["Chain_Name"].nunique() == 1 and rs["Restaurant_Name"].str.startswith(rs["Chain_Name"].iloc[0]).all())
    other_cities = r"\b(?:Lahore|Islamabad|Rawalpindi|Peshawar|Quetta|Multan|Faisalabad|Hyderabad|Sukkur|Sialkot|Gujranwala|Sargodha|Bahawalpur|Abbottabad|Mardan)\b"
    txt = pd.concat([rs[["Restaurant_Name", "Locality", "District", "Address", "City", "Province"]].astype(str).agg(" ".join, axis=1),
                     cc[["City", "Locality"]].astype(str).agg(" ".join, axis=1), R["Promotions"]["Promotion_Name"], items["Item_Name"]])
    stray = int(txt.str.contains(other_cities, flags=re.I, regex=True).sum())
    non_k = int((rs["City"] != "Karachi").sum() + (rs["Province"] != "Sindh").sum())
    flags["NON_KARACHI_BRANCHES"] = (non_k, non_k == 0)
    flags["KARACHI_ONLY"] = ("Karachi only" if non_k == 0 and stray == 0 and (cc["City"] == "Karachi").all() else "violations", non_k == 0 and stray == 0 and bool((cc["City"] == "Karachi").all()))
    res["karachi"] = {"branches": len(rs), "stray_other_city_mentions": stray, "customer_cities": cc["City"].unique().tolist()}
    flags["BRANCH_COUNT"] = (len(rs), len(rs) >= 20)

    # ---------------- 5-6: identical menu / identical price ----------------
    sets = av.groupby("Restaurant_ID")["Item_ID"].apply(frozenset); full = frozenset(items["Item_ID"])
    menu_ok = bool((sets == full).all() and len(sets) == len(rs) and av["Is_Available"].all()
                   and not {"Menu_Type", "Specialty_Region", "Region_Group"} & set(items.columns))
    res["menu"] = {"items": len(items), "branches_with_full_menu": int((sets == full).sum()), "availability_rows": len(av),
                   "items_without_sales_at_some_branch_pct": None}
    flags["MENU_IDENTICAL_ALL_BRANCHES"] = ("identical" if menu_ok else "differs", menu_ok)
    ov = o_raw.drop_duplicates("Order_ID"); ov = ov[pd.to_datetime(ov["Order_Date"], errors="coerce").notna() & ov["Restaurant_ID"].isin(rs["Restaurant_ID"])]
    oi = R["Order_Items"]; oi = oi[(oi["Unit_Price"] > 0) & (oi["Unit_Price"] <= MAX_PRICE) & oi["Item_ID"].isin(items["Item_ID"])].drop_duplicates("Order_Item_ID")
    oi = oi.merge(ov[["Order_ID", "Order_Date", "Restaurant_ID"]], on="Order_ID")
    g = oi.groupby(["Item_ID", "Order_Date"]).agg(p=("Unit_Price", "nunique"), b=("Restaurant_ID", "nunique"))
    oi["_d"] = pd.to_datetime(oi["Order_Date"]); phs = ph.assign(Effective_Date=pd.to_datetime(ph["Effective_Date"])).sort_values("Effective_Date")
    oi = pd.merge_asof(oi.sort_values("_d"), phs[["Item_ID", "Effective_Date", "Price"]], left_on="_d", right_on="Effective_Date", by="Item_ID")
    mism = int(((oi["Unit_Price"] - oi["Price"]).abs() > .001).sum())
    price_ok = bool("Restaurant_ID" not in ph.columns and not ph.duplicated(["Item_ID", "Effective_Date"]).any() and (g["p"] == 1).all() and mism == 0)
    res["price"] = {"item_date_groups": len(g), "groups_seen_at_multiple_branches": int((g["b"] > 1).sum()), "groups_with_more_than_one_price": int((g["p"] > 1).sum()),
                    "order_lines_not_matching_chain_price_list": mism, "pricing_history_has_branch_column": "Restaurant_ID" in ph.columns}
    flags["PRICE_IDENTICAL_ALL_BRANCHES"] = ("identical" if price_ok else "differs", price_ok)

    # ---------------- 7-12: branch variation (measured on cleaned data) ----------------
    co = co.assign(_d=pd.to_datetime(co["Order_Date"]), _h=pd.to_datetime(co["Order_DateTime"]).dt.hour)
    byb = co.groupby("Restaurant_ID").agg(orders=("Order_ID", "count"), revenue=("Total_Amount", "sum"), aov=("Total_Amount", "mean"))
    lunch = co.assign(l=co["_h"].between(12, 14)).groupby("Restaurant_ID")["l"].mean(); dinner = co.assign(l=co["_h"].between(19, 22)).groupby("Restaurant_ID")["l"].mean()
    cv = float(byb["orders"].std() / byb["orders"].mean()); ratio = float(byb["orders"].max() / byb["orders"].min())
    res["demand"] = {"orders_per_branch_min": int(byb["orders"].min()), "orders_per_branch_max": int(byb["orders"].max()), "cv": round(cv, 3), "max_over_min": round(ratio, 2),
                     "lunch_share_range": [round(float(lunch.min()), 3), round(float(lunch.max()), 3)], "dinner_share_range": [round(float(dinner.min()), 3), round(float(dinner.max()), 3)],
                     "aov_range": [round(float(byb["aov"].min()), 1), round(float(byb["aov"].max()), 1)]}
    flags["DEMAND_VARIATION"] = (f"CV={cv:.2f}, max/min={ratio:.2f}", bool(cv > .25 and ratio > 1.8 and lunch.max() - lunch.min() > .05))
    oc = co.merge(cc[["Customer_ID", "Age", "Gender"]], on="Customer_ID")
    oc["ag"] = pd.cut(oc["Age"], [0, 24, 34, 49, 200], labels=["<25", "25-34", "35-49", "50+"])
    mix = oc.groupby("Restaurant_ID").agg(age=("Age", "mean"), fem=("Gender", lambda s: (s == "Female").mean()))
    rep_ = oc.groupby(["Restaurant_ID", "Customer_ID"]).size().groupby("Restaurant_ID").apply(lambda s: (s > 1).mean())
    v_age, v_gen = cramers_v(oc["Restaurant_ID"], oc["ag"]), cramers_v(oc["Restaurant_ID"], oc["Gender"])
    res["customer_mix"] = {"mean_age_range": [round(float(mix["age"].min()), 1), round(float(mix["age"].max()), 1)], "female_share_range": [round(float(mix["fem"].min()), 3), round(float(mix["fem"].max()), 3)],
                           "repeat_customer_share_range": [round(float(rep_.min()), 3), round(float(rep_.max()), 3)], "cramers_v_branch_x_age_group": round(v_age, 3), "cramers_v_branch_x_gender": round(v_gen, 3)}
    flags["CUSTOMER_MIX_VARIATION"] = (f"age range {mix['age'].max()-mix['age'].min():.1f}y, female range {mix['fem'].max()-mix['fem'].min():.2f}",
                                       bool(mix["age"].max() - mix["age"].min() >= 3 and mix["fem"].max() - mix["fem"].min() >= .10 and v_age >= .04))
    waste_by_branch = cw.groupby("Restaurant_ID")[["Wastage_Quantity", "Prepared_Quantity"]].sum()
    wb = waste_by_branch["Wastage_Quantity"] / waste_by_branch["Prepared_Quantity"]
    res["wastage"] = {"wastage_pct_min": round(float(wb.min()) * 100, 2), "wastage_pct_max": round(float(wb.max()) * 100, 2), "max_over_min": round(float(wb.max() / wb.min()), 2)}
    flags["WASTAGE_VARIATION"] = (f"{wb.min()*100:.1f}%-{wb.max()*100:.1f}%", bool(wb.max() / wb.min() >= 1.3))
    rb = cr.groupby("Restaurant_ID")["Rating_Value"].agg(["mean", lambda s: (s <= 2).mean()]); rb.columns = ["mean", "low"]
    res["ratings"] = {"avg_rating_range": [round(float(rb["mean"].min()), 3), round(float(rb["mean"].max()), 3)], "share_1_2_star_range": [round(float(rb["low"].min()), 3), round(float(rb["low"].max()), 3)],
                      "overall_share_3_to_5_pct": round(float((cr["Rating_Value"] >= 3).mean()) * 100, 1), "overall_share_1_2_pct": round(float((cr["Rating_Value"] <= 2).mean()) * 100, 1)}
    flags["RATING_VARIATION"] = (f"avg {rb['mean'].min():.2f}-{rb['mean'].max():.2f}", bool(rb["mean"].max() - rb["mean"].min() >= .25 and .02 < (cr["Rating_Value"] <= 2).mean() < .20))
    co["promo"] = co["Promotion_ID"].notna(); ps = co.groupby("Restaurant_ID")["promo"].mean()
    oc2 = co.merge(cc[["Customer_ID", "Age"]], on="Customer_ID"); oc2["ag"] = pd.cut(oc2["Age"], [0, 24, 34, 49, 200], labels=["<25", "25-34", "35-49", "50+"])
    pa = oc2.groupby("ag", observed=True)["promo"].mean()
    res["promotion_response"] = {"promo_order_share_by_branch_range": [round(float(ps.min()), 4), round(float(ps.max()), 4)], "max_over_min": round(float(ps.max() / ps.min()), 2),
                                 "promo_order_share_by_age_group": {str(k): round(float(v), 4) for k, v in pa.items()}}
    flags["PROMOTION_RESPONSE_VARIATION"] = (f"branch promo share {ps.min()*100:.1f}%-{ps.max()*100:.1f}%", bool(ps.max() / ps.min() >= 1.3 and pa.max() - pa.min() >= .01))
    ch = pd.crosstab(co["Restaurant_ID"], co["Channel"], normalize="index"); v_ch = cramers_v(co["Restaurant_ID"], co["Channel"])
    dl = ch["Third-Party Delivery"] + ch.get("Phone Order", 0)
    res["channels"] = {"channels": sorted(co["Channel"].unique().tolist()), "delivery_plus_phone_share_range": [round(float(dl.min()), 3), round(float(dl.max()), 3)], "cramers_v_branch_x_channel": round(v_ch, 3)}
    flags["ORDER_CHANNEL_VARIATION"] = (f"V={v_ch:.3f}, delivery+phone {dl.min()*100:.0f}%-{dl.max()*100:.0f}%", bool(dl.max() - dl.min() >= .10 and v_ch >= .05))

    # ---------------- 13-14: intentional raw issues vs ledger ----------------
    cust_ids, item_ids, rest_ids, promo_ids = set(R["Customers"]["Customer_ID"]), set(items["Item_ID"]), set(rs["Restaurant_ID"]), set(R["Promotions"]["Promotion_ID"])
    O, I, T, W, V, U = R["Orders"], R["Order_Items"], R["Ratings"], R["Wastage"], R["Inventory"], R["Customers"]
    units_ok = {"kg", "liters", "pcs"}
    exact = I.duplicated("Order_Item_ID").sum(); I2 = I.drop_duplicates("Order_Item_ID")
    vp = (I2["Unit_Price"] > 0) & (I2["Unit_Price"] <= MAX_PRICE); vq = I2["Quantity"] > 0; vd = I2["Discount_Percent"].between(0, 100)
    expt = (I2["Quantity"] * I2["Unit_Price"] * (1 - I2["Discount_Percent"] / 100)).round(2)
    pm = R["Promotions"].set_index("Promotion_ID"); hp = O["Promotion_ID"].isin(promo_ids) & pd.to_datetime(O["Order_Date"], errors="coerce").notna()
    outside = hp & ((O["Order_Date"] < O["Promotion_ID"].map(pm["Start_Date"])) | (O["Order_Date"] > O["Promotion_ID"].map(pm["End_Date"])))
    det = {
     ("Orders", "duplicate_record"): len(O) - O["Order_ID"].nunique(), ("Orders", "missing_customer_reference"): O["Customer_ID"].isna().sum(),
     ("Orders", "invalid_customer_fk"): (O["Customer_ID"].notna() & ~O["Customer_ID"].isin(cust_ids)).sum(), ("Orders", "missing_value"): O["Channel"].isna().sum(),
     ("Orders", "invalid_branch_reference"): (~O["Restaurant_ID"].isin(rest_ids)).sum(), ("Orders", "invalid_date"): pd.to_datetime(O["Order_Date"], errors="coerce").isna().sum(),
     ("Orders", "invalid_promotion_reference"): (O["Promotion_ID"].notna() & ~O["Promotion_ID"].isin(promo_ids)).sum(), ("Orders", "inconsistent_promotion_reference"): outside.sum(),
     ("Order_Items", "duplicate_record"): exact, ("Order_Items", "near_duplicate_record"): I2.duplicated(["Order_ID", "Item_ID", "Quantity", "Unit_Price"]).sum(),
     ("Order_Items", "invalid_price"): (~vp).sum(), ("Order_Items", "negative_quantity"): (~vq).sum(), ("Order_Items", "missing_menu_reference"): I2["Item_ID"].isna().sum(),
     ("Order_Items", "invalid_menu_fk"): (I2["Item_ID"].notna() & ~I2["Item_ID"].isin(item_ids)).sum(), ("Order_Items", "orphan_order_reference"): (~I2["Order_ID"].isin(set(O["Order_ID"]))).sum(),
     ("Order_Items", "incorrect_discount"): (~vd).sum(), ("Order_Items", "incorrect_line_total"): ((I2["Line_Total"] - expt).abs() > .02)[vp & vq & vd].sum(),
     ("Ratings", "invalid_rating"): (~T["Rating_Value"].between(1, 5)).sum(), ("Ratings", "missing_customer_reference"): T["Customer_ID"].isna().sum(),
     ("Ratings", "invalid_menu_fk"): (~T["Item_ID"].isin(item_ids)).sum(), ("Ratings", "invalid_branch_reference"): (~T["Restaurant_ID"].isin(rest_ids)).sum(),
     ("Wastage", "impossible_wastage"): (W["Wastage_Quantity"] > W["Prepared_Quantity"]).sum(), ("Wastage", "inconsistent_units"): (~W["Unit"].isin(units_ok)).sum(),
     ("Wastage", "missing_menu_reference"): W["Item_ID"].isna().sum(), ("Inventory", "inconsistent_units"): (~V["Unit"].isin(units_ok)).sum(),
     ("Inventory", "missing_value"): V["Consumed_Stock"].isna().sum(), ("Customers", "missing_value"): U["Email"].isna().sum() + U["Phone"].isna().sum()}
    led_cnt = led.groupby(["Table", "Issue_Type"]).size().to_dict()
    cmp_ = {f"{t}.{i}": {"detected_in_raw": int(d), "ledger": int(led_cnt.get((t, i), 0)), "match": int(d) == int(led_cnt.get((t, i), 0))} for (t, i), d in det.items()}
    extra_types = [k for k in led_cnt if k not in det]
    res["issue_counts_vs_ledger"] = cmp_
    # categories requested in the brief -> each must be present in the RAW data (independent detection)
    brief = {"missing values": det[("Orders", "missing_value")] + det[("Customers", "missing_value")] + det[("Inventory", "missing_value")],
             "invalid foreign keys": det[("Orders", "invalid_customer_fk")] + det[("Order_Items", "invalid_menu_fk")] + det[("Order_Items", "orphan_order_reference")],
             "invalid dates": det[("Orders", "invalid_date")], "invalid ratings": det[("Ratings", "invalid_rating")],
             "negative quantities": det[("Order_Items", "negative_quantity")], "negative/invalid prices": det[("Order_Items", "invalid_price")],
             "incorrect line totals": det[("Order_Items", "incorrect_line_total")], "duplicate records": det[("Orders", "duplicate_record")] + det[("Order_Items", "duplicate_record")] + det[("Order_Items", "near_duplicate_record")],
             "inconsistent units": det[("Wastage", "inconsistent_units")] + det[("Inventory", "inconsistent_units")], "impossible wastage": det[("Wastage", "impossible_wastage")],
             "missing customer references": det[("Orders", "missing_customer_reference")] + det[("Ratings", "missing_customer_reference")],
             "invalid branch references": det[("Orders", "invalid_branch_reference")] + det[("Ratings", "invalid_branch_reference")],
             "inconsistent promotion references": det[("Orders", "invalid_promotion_reference")] + det[("Orders", "inconsistent_promotion_reference")]}
    rates = {k: round(100 * v / n_, 3) for k, v, n_ in [("duplicate_orders_pct", det[("Orders", "duplicate_record")], len(O)), ("invalid_price_pct", det[("Order_Items", "invalid_price")], len(I)),
             ("invalid_date_pct", det[("Orders", "invalid_date")], len(O)), ("invalid_rating_pct", det[("Ratings", "invalid_rating")], len(T))]}
    res["intentional_issue_categories_detected"] = {k: int(v) for k, v in brief.items()}; res["controlled_rates_pct"] = rates
    flags["INTENTIONAL_RAW_ISSUES_PRESENT"] = (f"{sum(v > 0 for v in brief.values())}/{len(brief)} categories, {int(sum(brief.values())):,} records", all(v > 0 for v in brief.values()) and max(rates.values()) < 2.0)
    # ledger entry-by-entry verification against the raw files
    def same(a, b):
        if (pd.isna(a) or a == "") and (pd.isna(b) or b == ""): return True
        if pd.isna(a) or pd.isna(b) or a == "" or b == "": return False
        try: return abs(float(a) - float(b)) < 1e-6
        except ValueError: return str(a) == str(b)
    verified = unverified = 0; bad_examples = []
    keymap = {t: led.loc[led["Table"] == t, "Key_Column"].iloc[0] for t in led["Table"].unique()}
    for (t, col), sub in led[led["Column"] != "(entire row)"].groupby(["Table", "Column"]):
        k = keymap[t]; rawv = R[t][[k, col]].rename(columns={k: "Key_Value", col: "_raw"}); rawv["Key_Value"] = rawv["Key_Value"].astype(str)
        m = sub.merge(rawv, on="Key_Value", how="left"); m["ok"] = [same(a, b) for a, b in zip(m["Raw_Value"], m["_raw"])]
        okk = m.groupby("Issue_ID")["ok"].any(); verified += int(okk.sum()); unverified += int((~okk).sum())
        if (~okk).any(): bad_examples.append((t, col, sub.loc[sub["Issue_ID"].isin(okk[~okk].index), "Key_Value"].head(3).tolist()))
    for t, sub in led[led["Column"] == "(entire row)"].groupby("Table"):
        k = keymap[t]; cnt = R[t][k].astype(str).value_counts()
        for _, r in sub.iterrows():
            src = r["Raw_Value"].split(":", 1)[1]
            ok = (cnt.get(r["Key_Value"], 0) >= 2) if r["Issue_Type"] == "duplicate_record" else (cnt.get(r["Key_Value"], 0) >= 1 and cnt.get(src, 0) >= 1)
            verified += int(ok); unverified += int(not ok)
    counts_match = all(v["match"] for v in cmp_.values()) and not extra_types
    res["ledger"] = {"entries": len(led), "entries_verified_in_raw": verified, "entries_not_found": unverified, "issue_types": len({k[1] for k in led_cnt}),
                     "all_type_counts_match_independent_detection": counts_match, "unverified_examples": bad_examples[:3]}
    flags["ISSUE_LEDGER_MATCHES_RAW_DATA"] = (f"{verified:,}/{len(led):,} entries verified, counts match={counts_match}", unverified == 0 and counts_match)
    sums = read_json(os.path.join(REP, "raw_data_checksums.json"))
    now = {f: hashlib.sha256(open(os.path.join(RAWD, f), "rb").read()).hexdigest() for f in sums}
    ro = all(os.stat(os.path.join(RAWD, f)).st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH) == 0 for f in sums)
    res["raw_immutability"] = {"files": len(sums), "checksums_match": all(now[f] == sums[f] for f in sums), "all_read_only": ro}
    flags["RAW_DATA_IMMUTABLE"] = (f"{len(sums)} files, checksums match, read-only={ro}", res["raw_immutability"]["checksums_match"] and ro)

    # ---------------- analytical difficulty (emerges from transactions) ----------------
    cl = ci.merge(co[["Order_ID", "Restaurant_ID", "_d", "Promotion_ID"]], on="Order_ID").merge(cm[["Item_ID", "Item_Name", "Base_Cost", "Base_Price"]], on="Item_ID")
    cl["margin"] = (cl["Line_Total"] - cl["Quantity"] * cl["Base_Cost"]) / cl["Line_Total"].replace(0, np.nan)
    qty = cl.groupby("Item_ID")["Quantity"].sum().reindex(cm["Item_ID"]).fillna(0); mg = ((cm["Base_Price"] - cm["Base_Cost"]) / cm["Base_Price"]); mg.index = cm["Item_ID"]
    qr, mr = qty.rank(pct=True), mg.rank(pct=True); biz = {}
    for nm, ids_, hi in [("popular_low_margin", man["popular_low_margin"], True), ("profitable_low_selling", man["profitable_low_selling"], False)]:
        a, b = float(qr[ids_].mean()), float(mr[ids_].mean()); biz[nm] = {"avg_sales_pctile": round(a * 100, 1), "avg_margin_pctile": round(b * 100, 1), "ok": bool((a > .55 and b < .45) if hi else (a < .45 and b > .55))}
    waste_by_item = cw.groupby("Item_ID")[["Wastage_Quantity", "Prepared_Quantity"]].sum()
    wi = waste_by_item["Wastage_Quantity"] / waste_by_item["Prepared_Quantity"]
    pop_hw = [i for i in wi.index if qr.get(i, 0) >= .75 and wi[i] >= wi.quantile(.85)]
    biz["popular_high_wastage_items"] = {"count": len(pop_hw), "ok": len(pop_hw) >= 3}
    d_ = cl.groupby(["Item_ID", "Restaurant_ID", "_d"])["Quantity"].sum(); sh = dh = 0
    for r in man["sales_anomalies"]["spikes"]:
        s = d_.xs((r["Item_ID"], r["Restaurant_ID"]), level=(0, 1)); t_ = pd.Timestamp(r["Date"])
        sh += int(t_ in s.index and len(s) > 1 and s.loc[t_] > 3 * s.drop(t_).mean())
    for r in man["sales_anomalies"]["drops"]:
        s = d_.xs((r["Item_ID"], r["Restaurant_ID"]), level=(0, 1)); w_ = pd.date_range(r["Window_Start"], r["Window_End"])
        inw, out = s.reindex(w_).fillna(0).mean(), s[~s.index.isin(w_)].mean(); dh += int(out > 0 and inw < .4 * out)
    hv = co[co["Order_ID"].isin(man["sales_anomalies"]["high_value_orders"])]["Total_Amount"]
    biz["sales_anomalies"] = {"spikes_detectable": f"{sh}/{len(man['sales_anomalies']['spikes'])}", "drops_detectable": f"{dh}/{len(man['sales_anomalies']['drops'])}",
                              "high_value_orders_above_p99": int((hv > co["Total_Amount"].quantile(.99)).sum()), "ok": bool(sh > 0 and dh > 0)}
    flag_ids = set(man["price_sensitive"]); ctrl = set(np.random.default_rng(1).choice([i for i in cm["Item_ID"] if i not in flag_ids and i not in man["new_items"]], 20, replace=False))
    dq_ = cl.groupby(["Item_ID", "_d"])["Quantity"].sum(); dqd = {k: (v.index.values, v.values) for k, v in dq_.groupby(level=0)} if False else {i: (g_.index.get_level_values(1).values, g_.values) for i, g_ in dq_.groupby(level=0)}
    def ratios(ids):
        out_ = []
        for _, c in ph[ph["Item_ID"].isin(ids) & ph["Previous_Price"].notna()].iterrows():
            if c["Price"] / c["Previous_Price"] - 1 < .05 or c["Item_ID"] not in dqd: continue
            e = np.datetime64(c["Effective_Date"]); dd, qq = dqd[c["Item_ID"]]
            b_ = qq[(dd >= e - np.timedelta64(30, "D")) & (dd < e)].sum(); a_ = qq[(dd >= e) & (dd < e + np.timedelta64(30, "D"))].sum()
            if b_ >= 20 and e - np.timedelta64(30, "D") >= np.datetime64("2025-01-01") and e + np.timedelta64(30, "D") <= np.datetime64("2025-12-31"): out_.append(a_ / b_)
        return out_
    rf, rc = ratios(flag_ids), ratios(ctrl)
    biz["price_sensitivity"] = {"flagged_windows": len(rf), "control_windows": len(rc), "flagged_mean_demand_ratio": round(float(np.mean(rf)), 3), "control_mean_demand_ratio": round(float(np.mean(rc)), 3),
                                "ok": bool(len(rf) >= 5 and len(rc) >= 5 and np.mean(rf) < np.mean(rc) - .05)}
    q = man["promotion_quality"]; pdays = {r["Promotion_ID"]: max((pd.Timestamp(r["End_Date"]) - pd.Timestamp(r["Start_Date"])).days, 1) for _, r in R["Promotions"].iterrows()}
    pc = co[co["Promotion_ID"].notna()].groupby("Promotion_ID").size(); byq = {}
    for pid, ql in q.items(): byq.setdefault(ql, []).append(pc.get(pid, 0) / pdays[pid])
    mis = R["Promotions"][R["Promotions"]["Promotion_ID"].map(q) == "Misleading"]
    cl["profit"] = cl["Line_Total"] - cl["Quantity"] * cl["Base_Cost"]
    base = cl[cl["Promotion_ID"].isna()].groupby("Item_ID").agg(bq=("Quantity", "mean"), br=("Line_Total", "mean"), bp=("profit", "mean"))
    disc = cl[(cl["Discount_Percent"] > 0) & cl["Promotion_ID"].notna()].assign(q=lambda x: x["Promotion_ID"].map(q)).join(base, on="Item_ID")
    lr = {}
    for ql, d_ in disc.groupby("q"):
        lr[ql] = {"lines": int(len(d_)), "units_ratio": round(float(d_["Quantity"].sum() / d_["bq"].sum()), 3), "revenue_ratio": round(float(d_["Line_Total"].sum() / d_["br"].sum()), 3),
                  "profit_ratio": round(float(d_["profit"].sum() / d_["bp"].sum()), 3)}
    chain_days = pd.Series(False, index=pd.date_range("2025-01-01", "2025-12-31"))
    for _, r in R["Promotions"].iterrows():
        if pd.isna(r["Applicable_Restaurant_ID"]) and q[r["Promotion_ID"]] in ("Effective", "Misleading"): chain_days[pd.date_range(r["Start_Date"], r["End_Date"])] = True
    daily = co.groupby("_d").size().reindex(chain_days.index).fillna(0); mq = lr.get("Misleading", {}); eq_ = lr.get("Effective", {})
    cl = cl.merge(cm[["Item_ID", "Category_ID"]], on="Item_ID")
    dcat = cl.groupby(["_d", "Category_ID"]).agg(rev=("Line_Total", "sum"), prof=("profit", "sum")); dall = cl.groupby("_d").agg(rev=("Line_Total", "sum"), prof=("profit", "sum"))
    win_res = {"Misleading": [], "Effective": []}
    for _, r in R["Promotions"].iterrows():
        ql = q[r["Promotion_ID"]]
        if ql not in win_res or pd.notna(r["Applicable_Restaurant_ID"]): continue
        s_, e_ = pd.Timestamp(r["Start_Date"]), pd.Timestamp(r["End_Date"]); win = pd.date_range(s_, e_)
        base_days = [d for d in list(pd.date_range(s_ - pd.Timedelta(days=28), s_ - pd.Timedelta(days=1))) + list(pd.date_range(e_ + pd.Timedelta(days=1), e_ + pd.Timedelta(days=28)))
                     if d in chain_days.index and not chain_days[d]]
        ser = dall if pd.isna(r["Applicable_Category_ID"]) else dcat.xs(r["Applicable_Category_ID"], level=1)
        wv, bv = ser.reindex(win).dropna(), ser.reindex(base_days).dropna()
        if len(base_days) >= 10 and len(wv) >= 2 and bv["rev"].mean() > 0:
            win_res[ql].append((wv["rev"].mean() / bv["rev"].mean(), wv["prof"].mean() / bv["prof"].mean()))
    wsum = {k: {"promotions": len(v), "median_revenue_ratio_vs_baseline": round(float(np.median([a for a, _ in v])), 3), "median_profit_ratio_vs_baseline": round(float(np.median([b for _, b in v])), 3),
                "share_with_revenue_up_and_profit_down": round(float(np.mean([(a > 1.02 and b < 1.0) for a, b in v])), 2)} for k, v in win_res.items() if v}
    mw, ew = wsum.get("Misleading", {}), wsum.get("Effective", {})
    biz["promotions"] = {"orders_per_day_by_quality": {k: round(float(np.mean(v)), 1) for k, v in byq.items()},
                         "discounted_lines_vs_same_item_non_promo_lines": lr, "promotion_window_vs_own_28day_baseline_chain_wide": wsum,
                         "chain_wide_promo_day_order_uplift": round(float(daily[chain_days].mean() / daily[~chain_days].mean()), 3),
                         "misleading_scoped_to_low_margin_categories": bool(mis["Applicable_Category_ID"].isin(["C01", "C03", "C11"]).all()),
                         "chain_wide": int(R["Promotions"]["Applicable_Restaurant_ID"].isna().sum()), "branch_specific": int(R["Promotions"]["Applicable_Restaurant_ID"].notna().sum()),
                         "criterion": "misleading: revenue up, profit growth lags revenue by >=0.20 and >=25% of such promotions lose profit; effective: profit keeps pace and <=10% lose profit",
                         "ok": bool(mw and ew and mw["median_revenue_ratio_vs_baseline"] > 1.05 and (mw["median_revenue_ratio_vs_baseline"] - mw["median_profit_ratio_vs_baseline"]) >= .20
                                    and mw["share_with_revenue_up_and_profit_down"] >= .25 and ew["median_revenue_ratio_vs_baseline"] > 1.05 and ew["median_profit_ratio_vs_baseline"] > 1.0
                                    and ew["share_with_revenue_up_and_profit_down"] <= .10 and mq.get("units_ratio", 0) > 1.2 and np.mean(byq["Misleading"]) > 1.5 * np.mean(byq["Weak"]))}
    hr = co["_h"]; ram = co["_d"].dt.month == 3; ev = lambda m: float(((hr[m] >= 17) & (hr[m] <= 19)).mean())
    biz["calendar_patterns"] = {"weekend_share_pct": round(float(co["_d"].dt.dayofweek.isin([4, 5, 6]).mean()) * 100, 1), "winter_share_pct": round(float(co["_d"].dt.month.isin([11, 12, 1, 2]).mean()) * 100, 1),
                                "peak_hours_share_pct": round(float(hr.isin([12, 13, 19, 20, 21]).mean()) * 100, 1), "ramazan_iftar_share_pct": round(ev(ram) * 100, 1), "other_months_iftar_share_pct": round(ev(~ram) * 100, 1),
                                "ok": bool(co["_d"].dt.dayofweek.isin([4, 5, 6]).mean() > 3 / 7 * 1.05 and co["_d"].dt.month.isin([11, 12, 1, 2]).mean() > 4 / 12 * 1.05 and hr.isin([12, 13, 19, 20, 21]).mean() > 5 / 16 * 1.3 and ev(ram) > 1.4 * ev(~ram))}
    cust = co.groupby("Customer_ID").agg(n=("Order_ID", "count"), rev=("Total_Amount", "sum"), first=("_d", "min"), last=("_d", "max"))
    top = cust["rev"].sort_values(ascending=False).head(int(.05 * len(cust))).sum() / cust["rev"].sum()
    churn = float(((cust["first"] < "2025-07-01") & (cust["last"] < "2025-10-01")).mean()); newc = float((cc.set_index("Customer_ID").loc[cust.index, "Signup_Date"] >= "2025-01-01").mean())
    biz["customers"] = {"top5pct_revenue_share": round(float(top), 3), "inactive_churn_risk_share": round(churn, 3), "new_customers_share": round(newc, 3), "one_order_customers_share": round(float((cust["n"] == 1).mean()), 3), "repeat_customers_share": round(float((cust["n"] > 1).mean()), 3),
                         "ok": bool(top > .15 and churn > .08 and newc > .08)}
    bursts = sum(1 for b in man["rating_bursts"] if ((cr["Item_ID"] == b["Item_ID"]) & (cr["Restaurant_ID"] == b["Restaurant_ID"]) & (cr["Rating_Date"] == b["Date"]) & (cr["Rating_Value"] == b["Value"])).sum() >= 100)
    biz["rating_anomaly_bursts"] = {"detected": f"{bursts}/{len(man['rating_bursts'])}", "ok": bursts == len(man["rating_bursts"])}
    sold = cl.groupby("Restaurant_ID")["Item_ID"].nunique(); res["menu"]["min_menu_items_sold_at_any_branch"] = int(sold.min())
    res["business_cases"] = biz
    clean_ok = bool(all(v["pass"] for v in res["volume"].values()) and all(v["pass"] for v in res["relationships_cleaned"].values()) and res["structure"]["all_11_required_tables_present"])
    biz_ok = all(v.get("ok", True) for v in biz.values())
    pqv = read_json(os.path.join(REP, "parquet_verification.json"))
    res["parquet_readback_ok"] = all(v["ok"] for v in pqv.values())
    res["chronological_split"] = {s: [str(pd.read_parquet(os.path.join(ROOT, "parquet_data", "splits", s, "Orders.parquet"))["Order_Date"].min()), str(pd.read_parquet(os.path.join(ROOT, "parquet_data", "splits", s, "Orders.parquet"))["Order_Date"].max())] for s in ["train", "validation", "test"]}
    sp = list(res["chronological_split"].values()); split_ok = sp[0][1] < sp[1][0] and sp[1][1] < sp[2][0]
    top_dirs = ["src", "templates", "static", "data_generator", "raw_data", "processed_data", "parquet_data", "spark_jobs", "spark_sql", "python_pipeline", "notebooks", "models", "database", "tests", "sample_data", "documentation", "screenshots", "reports", "config"]
    struct_ok = all(os.path.isdir(os.path.join(ROOT, d)) for d in top_dirs) and all(os.path.isfile(os.path.join(ROOT, f)) for f in ["README.md", "AI_USAGE.md", "requirements.txt", "LICENSE"])
    res["project_structure"] = {"required_dirs_and_files_present": struct_ok}
    res["supporting_checks"] = {"volumes_relationships_structure": clean_ok, "analytical_cases_emerge_from_data": biz_ok, "parquet_readback": res["parquet_readback_ok"], "chronological_split": split_ok, "project_structure": struct_ok}
    allpass = all(ok for _, ok in flags.values()) and all(res["supporting_checks"].values())
    res["flags"] = {k: {"measured": str(v), "status": "PASS" if ok else "FAIL"} for k, (v, ok) in flags.items()}; res["ALL_PASS"] = allpass
    output_dir = tempfile.mkdtemp(prefix="dataset_validation_", dir=REP)
    with open(os.path.join(output_dir, "validation_report.json"), "x", encoding="utf-8") as stream:
        json.dump(res, stream, indent=2, default=str)
    with open(os.path.join(output_dir, "validation_report.md"), "x", encoding="utf-8") as f:
        f.write("# Validation report (measured from files on disk)\n\n## Final flags\n\n```text\n")
        for k, (v, ok) in flags.items(): f.write(f"{k}={'PASS' if ok else 'FAIL'}   # {v}\n")
        f.write("```\n\n## Supporting checks\n\n" + "\n".join(f"- {k}: {'PASS' if v else 'FAIL'}" for k, v in res["supporting_checks"].items()) + "\n\n## Measured details\n\n```json\n" + json.dumps({k: v for k, v in res.items() if k not in ("flags",)}, indent=1, default=str) + "\n```\n")
    print("=========== FINAL VALIDATION (measured from disk) ===========")
    print("REPORT_DIR=" + output_dir)
    names = {"RESTAURANT_CHAIN_COUNT": None, "BRANCH_COUNT": None, "NON_KARACHI_BRANCHES": None}
    for k, (v, ok) in flags.items():
        print(f"{k}={v if k in names else ('PASS' if ok else 'FAIL')}" + (f"   [{'PASS' if ok else 'FAIL'}]" if k in names else f"   # {v}"))
    for k, v in res["supporting_checks"].items(): print(f"  support {k}: {'PASS' if v else 'FAIL'}")
    print("ALL_PASS =", allpass); return 0 if allpass else 1

if __name__ == "__main__":
    sys.exit(main())
