from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from dineiq.config import load_settings


def prepare(cleaned_root: Path, output_root: Path) -> dict:
    raw = load_settings().raw_data_dir.resolve()
    if output_root.resolve() == raw or raw in output_root.resolve().parents:
        raise ValueError("Demand splits cannot be written inside immutable raw_data")
    if output_root.exists():
        raise FileExistsError(f"Refusing to replace existing demand splits: {output_root}")
    orders = pd.read_csv(cleaned_root / "Orders.csv", dtype={"Order_ID": str, "Restaurant_ID": str})
    if orders["Order_ID"].isna().any() or orders["Order_ID"].duplicated().any():
        raise ValueError("Cleaned orders require unique nonnull Order_ID values")
    orders["Order_Date"] = pd.to_datetime(orders["Order_Date"], errors="raise").dt.normalize()
    if orders["Restaurant_ID"].isna().any():
        raise ValueError("Cleaned orders contain blank branch identifiers")
    bounds = {}
    split_ids = {}
    for split in ("train", "validation", "test"):
        source = cleaned_root / "splits" / split / "Orders.csv"
        frame = pd.read_csv(source, dtype={"Order_ID": str}, usecols=["Order_ID", "Order_Date"])
        if frame.empty or frame["Order_ID"].duplicated().any() or frame["Order_ID"].isna().any():
            raise ValueError(f"Invalid transaction split: {split}")
        dates = pd.to_datetime(frame["Order_Date"], errors="raise")
        bounds[split] = (dates.min().normalize(), dates.max().normalize())
        split_ids[split] = set(frame["Order_ID"])
    if not (bounds["train"][1] < bounds["validation"][0] and bounds["validation"][1] < bounds["test"][0]):
        raise ValueError("Transaction splits are not strictly chronological")
    if any(split_ids[a] & split_ids[b] for a, b in (("train", "validation"), ("train", "test"), ("validation", "test"))):
        raise ValueError("Transaction IDs overlap across splits")
    if set.union(*split_ids.values()) != set(orders["Order_ID"]):
        raise ValueError("Transaction splits do not cover exactly the cleaned orders")
    for split, ids in split_ids.items():
        selected = orders.loc[orders["Order_ID"].isin(ids), "Order_Date"]
        if not selected.between(*bounds[split]).all():
            raise ValueError(f"Split {split} dates do not match cleaned order dates")

    availability = pd.read_csv(cleaned_root / "Menu_Item_Availability.csv", dtype={"Item_ID": str, "Restaurant_ID": str})
    items = pd.read_csv(cleaned_root / "Menu_Items.csv", dtype={"Item_ID": str})
    if items["Item_ID"].duplicated().any():
        raise ValueError("Menu item identifiers are not unique")
    if availability.duplicated(["Item_ID", "Restaurant_ID"]).any():
        raise ValueError("Availability contains duplicate item/branch pairs")
    if not availability["Item_ID"].isin(items["Item_ID"]).all():
        raise ValueError("Availability contains unknown menu items")
    availability = availability.merge(items[["Item_ID", "Introduced_Date"]], on="Item_ID", validate="many_to_one")
    availability["Available_From"] = pd.to_datetime(availability["Available_From"], errors="raise")
    availability["Introduced_Date"] = pd.to_datetime(availability["Introduced_Date"], errors="raise")
    availability["start"] = availability[["Available_From", "Introduced_Date"]].max(axis=1)
    availability = availability.loc[availability["Is_Available"].astype(str).str.lower().eq("true")]
    completed = orders.loc[orders["Status"].str.lower().eq("completed"), ["Order_ID", "Restaurant_ID", "Order_Date"]]
    aggregates = []
    line_rows = 0
    seen_line_ids = set()
    for chunk in pd.read_csv(cleaned_root / "Order_Items.csv", dtype={"Order_Item_ID": str, "Order_ID": str, "Item_ID": str}, chunksize=100000):
        if chunk["Order_Item_ID"].isna().any() or chunk["Order_Item_ID"].duplicated().any() or seen_line_ids.intersection(chunk["Order_Item_ID"]):
            raise ValueError("Order lines contain null or duplicate primary keys")
        seen_line_ids.update(chunk["Order_Item_ID"])
        if not chunk["Order_ID"].isin(orders["Order_ID"]).all() or not chunk["Item_ID"].isin(items["Item_ID"]).all():
            raise ValueError("Cleaned order lines contain orphan order or item keys")
        quantity = pd.to_numeric(chunk["Quantity"], errors="raise")
        if quantity.isna().any() or (quantity <= 0).any():
            raise ValueError("Cleaned order lines contain invalid quantities")
        chunk["Quantity"] = quantity
        rows = chunk.merge(completed, on="Order_ID", how="inner", validate="many_to_one")
        line_rows += len(rows)
        aggregates.append(rows.groupby(["Order_Date", "Item_ID", "Restaurant_ID"])["Quantity"].sum())
    if not aggregates:
        raise ValueError("No completed order lines available")
    daily = pd.concat(aggregates).groupby(level=[0, 1, 2]).sum().reset_index().rename(
        columns={"Order_Date": "Date", "Restaurant_ID": "Location_ID", "Quantity": "Target_Quantity"})
    calendar = pd.DataFrame({"Date": pd.date_range(orders["Order_Date"].min(), orders["Order_Date"].max())})
    pairs = availability.rename(columns={"Restaurant_ID": "Location_ID"})[["Item_ID", "Location_ID", "start"]]
    panel = calendar.merge(pairs, how="cross")
    panel = panel.loc[panel["Date"] >= panel["start"]].drop(columns="start")
    keys = ["Date", "Item_ID", "Location_ID"]
    if not daily.merge(panel[keys], on=keys, how="left", indicator=True)["_merge"].eq("both").all():
        raise ValueError("Completed sales conflict with effective item/branch availability")
    panel = panel.merge(daily, on=keys, how="left", validate="one_to_one")
    panel["Target_Quantity"] = panel["Target_Quantity"].fillna(0)
    panel["Is_Available"] = 1
    columns = ["Date", "Item_ID", "Location_ID", "Is_Available", "Target_Quantity"]
    panel = panel.sort_values(keys)[columns]
    partitions = {split: panel.loc[panel["Date"].between(*interval)] for split, interval in bounds.items()}
    if sum(len(frame) for frame in partitions.values()) != len(panel):
        raise ValueError("Daily panel contains dates outside the supplied split boundaries")
    output_root.mkdir(parents=True)
    summary = {"task": "rolling one-day-ahead demand", "completed_line_rows": line_rows,
               "rows": {}, "boundaries": {}, "source": str(cleaned_root),
               "features": "calendar, item, branch, availability; causal lag features built by each independent model",
               "evaluation_policy": "prior test-day actuals become available only after that day; not a fixed-origin multi-day evaluation",
               "pricing_features": "not used", "source_modified": False}
    for split, frame in partitions.items():
        frame.to_csv(output_root / (split + ".csv"), index=False, date_format="%Y-%m-%d")
        summary["rows"][split] = len(frame)
        summary["boundaries"][split] = [str(v.date()) for v in bounds[split]]
    (output_root / "manifest.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleaned-root", type=Path)
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args()
    settings = load_settings()
    result = prepare(args.cleaned_root or settings.processed_data_dir,
                     args.output_root or settings.splits_dir / "demand_forecast")
    for split, rows in result["rows"].items():
        print(f"{split.upper()}_ROWS={rows}")
    print("DEMAND_PREPARATION_STATUS=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
