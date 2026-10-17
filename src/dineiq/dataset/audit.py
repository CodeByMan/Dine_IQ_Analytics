from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from dineiq.config import load_settings
from dineiq.dataset.schema import TABLE_SCHEMAS, FOREIGN_KEYS, DATE_FIELDS, TIMESTAMP_FIELDS, DECIMAL_FIELDS, INTEGER_FIELDS


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_source(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")


def profile(frame: pd.DataFrame, key: str) -> dict:
    missing = frame.eq("")
    dates = {}
    for col in set(frame.columns) & (DATE_FIELDS | TIMESTAMP_FIELDS):
        values = pd.to_datetime(frame[col], errors="coerce", format="mixed")
        dates[col] = {"invalid_nonblank": int((values.isna() & ~missing[col]).sum()),
                      "minimum": str(values.min()), "maximum": str(values.max())}
    numeric = {}
    for col in set(frame.columns) & (DECIMAL_FIELDS | INTEGER_FIELDS):
        values = pd.to_numeric(frame[col].where(~missing[col]), errors="coerce")
        numeric[col] = {"invalid_nonblank": int((values.isna() & ~missing[col]).sum()),
                        "minimum": float(values.min()) if values.notna().any() else None,
                        "maximum": float(values.max()) if values.notna().any() else None,
                        "median": float(values.median()) if values.notna().any() else None}
    safe_categories = {"City", "Province", "Country", "Gender", "Channel", "Payment_Method", "Status",
                       "Unit", "Is_Available", "Is_Active", "Location_Type"}
    distributions = {col: {str(k): int(v) for k, v in frame[col].value_counts().items()}
                     for col in set(frame.columns) & safe_categories}
    return {"rows": len(frame), "columns": list(frame.columns),
            "distinct_counts": {col: int(frame[col].nunique(dropna=False)) for col in frame},
            "numeric_ranges": numeric, "categorical_distributions": distributions,
            "blank_counts": {c: int(v) for c, v in missing.sum().items()},
            "duplicate_rows": int(frame.duplicated().sum()),
            "duplicate_primary_keys": int(frame.duplicated(key).sum()),
            "blank_primary_keys": int(missing[key].sum()), "dates": dates}


def pricing_intervals(history: pd.DataFrame) -> dict:
    """Inclusive end-date audit, comparing all overlapping same-item periods."""
    invalid = reversed_count = overlaps = conflicting_pairs = gaps = 0
    affected_items = set()
    for item, rows in history.groupby("Item_ID"):
        starts = pd.to_datetime(rows["Effective_Date"], errors="coerce")
        # Blank End_Date explicitly denotes the current open-ended price in
        # the supplied generator and dictionary, not an invalid date.
        ends = pd.to_datetime(rows["End_Date"], errors="coerce")
        ends = ends.where(rows["End_Date"].ne(""), pd.Timestamp.max.normalize())
        prices = pd.to_numeric(rows["Price"], errors="coerce")
        invalid += int((starts.isna() | ends.isna() | prices.isna() | (prices <= 0)).sum())
        reversed_count += int((starts > ends).sum())
        valid = pd.DataFrame({"start": starts, "end": ends, "price": prices}).dropna().sort_values("start")
        records = list(valid.itertuples(index=False))
        previous_end = None
        for i, a in enumerate(records):
            if a.start > a.end:
                continue
            if previous_end is not None and a.start > previous_end + pd.Timedelta(days=1):
                gaps += 1
            previous_end = a.end if previous_end is None else max(previous_end, a.end)
            for b in records[i + 1:]:
                if b.start > a.end:
                    break
                if b.start <= b.end and b.end >= a.start:
                    overlaps += 1
                    if abs(a.price - b.price) > 0.005:
                        conflicting_pairs += 1
                        affected_items.add(str(item))
    return {"invalid_records": invalid, "reversed_records": reversed_count,
            "overlapping_interval_pairs": overlaps, "conflicting_interval_pairs": conflicting_pairs,
            "affected_items": len(affected_items), "gaps_between_intervals": gaps,
            "interval_end_semantics": "inclusive; blank end is open-ended", "branch_specific_price_field": "Restaurant_ID" in history,
            "passed": not (invalid or reversed_count or conflicting_pairs or gaps)}


def audit_dataset(root: Path) -> dict:
    root = root.resolve()
    raw_root, cleaned_root = root / "raw_data", root / "processed_data"
    baseline = {p.name: sha256(p) for p in sorted(raw_root.glob("*.csv"))}
    report = {"source": str(raw_root), "raw_tables": {}, "cleaned_tables": {},
              "relationships": {}, "reconciliation": {}, "parquet": {}}
    raw, cleaned = {}, {}
    for name, spec in TABLE_SCHEMAS.items():
        for representation, directory, store in (("raw", raw_root, raw), ("cleaned", cleaned_root, cleaned)):
            path = directory / spec.filename
            if not path.is_file():
                report[representation + "_tables"][name] = {"status": "MISSING"}
                continue
            frame = read_source(path)
            if spec.primary_key not in frame:
                report[representation + "_tables"][name] = {"status": "INVALID", "error": "primary key absent"}
                continue
            store[name] = frame
            measured = profile(frame, spec.primary_key)
            measured.update({"path": str(path.relative_to(root)), "bytes": path.stat().st_size,
                             "sha256": sha256(path), "missing_columns": sorted(set(spec.columns) - set(frame.columns)),
                             "extra_columns": sorted(set(frame.columns) - set(spec.columns)), "status": "MEASURED"})
            report[representation + "_tables"][name] = measured
        if name in raw and name in cleaned:
            a, b = raw[name], cleaned[name]
            ak, bk = set(a[spec.primary_key]), set(b[spec.primary_key])
            unique_a = a.drop_duplicates(spec.primary_key, keep=False).set_index(spec.primary_key)
            unique_b = b.drop_duplicates(spec.primary_key, keep=False).set_index(spec.primary_key)
            common = unique_a.index.intersection(unique_b.index)
            columns = unique_a.columns.intersection(unique_b.columns)
            changes = unique_a.loc[common, columns].ne(unique_b.loc[common, columns]).sum()
            report["reconciliation"][name] = {
                "raw_rows": len(a), "cleaned_rows": len(b), "removed_keys": len(ak - bk), "added_keys": len(bk - ak),
                "missing_cleaned_columns": sorted(set(a.columns) - set(b.columns)),
                "changed_cells_by_column_unique_keys_only": {c: int(v) for c, v in changes.items() if v},
                "comparison_limitation": "Duplicate raw keys excluded from cell comparison, not silently assigned identity."}
    for label, tables in (("raw", raw), ("cleaned", cleaned)):
        relations = []
        for child, child_key, parent, parent_key in FOREIGN_KEYS:
            if child not in tables or parent not in tables:
                continue
            values = tables[child][child_key]
            nonblank = values.ne("")
            matched = values.isin(set(tables[parent][parent_key])) & nonblank
            relations.append({"child": child, "field": child_key, "parent": parent, "parent_field": parent_key,
                              "matched_rows": int(matched.sum()), "blank_rows": int((~nonblank).sum()),
                              "orphan_rows": int((nonblank & ~matched).sum())})
        report["relationships"][label] = relations
    if {"restaurants", "orders", "menu_items", "menu_item_availability", "pricing_history"}.issubset(raw):
        branches = raw["restaurants"]
        activity = cleaned.get("orders", raw["orders"])
        availability = raw["menu_item_availability"]
        expected_pairs = len(branches) * len(raw["menu_items"])
        report["business"] = {
            "brands": int(branches["Chain_Name"].nunique()), "registered_branches": len(branches),
            "branches_with_cleaned_orders": int(activity["Restaurant_ID"].nunique()),
            "non_karachi_rows": int((branches["City"] != "Karachi").sum()),
            "non_sindh_rows": int((branches["Province"] != "Sindh").sum()),
            "non_pakistan_rows": int((branches["Country"] != "Pakistan").sum()),
            "menu_items": len(raw["menu_items"]), "expected_branch_item_pairs": expected_pairs,
            "actual_branch_item_pairs": len(availability.drop_duplicates(["Restaurant_ID", "Item_ID"])),
            "unavailable_records": int((availability["Is_Available"].str.lower() != "true").sum()),
            "pricing": pricing_intervals(raw["pricing_history"]),
            "customer_identity_provenance": "synthetic per supplied generation documentation; no contact values reported"}
    quality = {}
    if {"pricing_history", "orders", "order_items"}.issubset(cleaned):
        history = cleaned["pricing_history"].copy()
        history["effective"] = pd.to_datetime(history["Effective_Date"], errors="raise")
        history["end"] = pd.to_datetime(history["End_Date"].replace("", None), errors="coerce").fillna(pd.Timestamp.max.normalize())
        history["base_price"] = pd.to_numeric(history["Price"], errors="raise")
        orders = cleaned["orders"][["Order_ID", "Order_Date"]].copy()
        orders["date"] = pd.to_datetime(orders["Order_Date"], errors="raise")
        lines = cleaned["order_items"][["Order_ID", "Item_ID", "Unit_Price"]].merge(orders, on="Order_ID", validate="many_to_one")
        matched = pd.merge_asof(lines.sort_values("date"), history.sort_values("effective"),
                                left_on="date", right_on="effective", by="Item_ID", direction="backward")
        covered = matched["effective"].notna() & (matched["date"] <= matched["end"])
        quality["cleaned_historical_price"] = {
            "line_rows": len(matched), "covered_rows": int(covered.sum()),
            "uncovered_rows": int((~covered).sum()),
            "unit_base_price_mismatches": int((covered & ((pd.to_numeric(matched["Unit_Price"], errors="coerce") - matched["base_price"]).abs() > 0.005)).sum()),
            "meaning": "Unit_Price compared to effective chain-wide base price; line discounts are not alternate base prices."}
    for name, tables in (("raw", raw), ("cleaned", cleaned)):
        if "order_items" in tables:
            lines = tables["order_items"]
            q, p, d, total = (pd.to_numeric(lines[c], errors="coerce") for c in ("Quantity", "Unit_Price", "Discount_Percent", "Line_Total"))
            expected = (q * p * (1 - d / 100)).round(2)
            quality[name + "_lines"] = {"invalid_price": int((p.isna() | (p <= 0) | (p > 10000)).sum()),
                                         "invalid_quantity": int((q.isna() | (q <= 0)).sum()),
                                         "invalid_discount": int((d.isna() | ~d.between(0, 100)).sum()),
                                         "line_total_mismatch": int(((total - expected).abs() > 0.02).sum())}
        if "inventory" in tables:
            inv = tables["inventory"]
            a, b, c, end = (pd.to_numeric(inv[x], errors="coerce") for x in ("Opening_Stock", "Received_Stock", "Consumed_Stock", "Closing_Stock"))
            quality[name + "_inventory_balance_mismatch"] = int(((a + b - c - end).abs() > 0.11).sum())
        if {"orders", "order_items"}.issubset(tables):
            quality[name + "_orders_without_lines"] = int((~tables["orders"]["Order_ID"].isin(tables["order_items"]["Order_ID"])).sum())
    report["quality"] = quality
    split_orders = {}
    split_report = {}
    for split in ("train", "validation", "test"):
        path = cleaned_root / "splits" / split / "Orders.csv"
        line_path = cleaned_root / "splits" / split / "Order_Items.csv"
        if not path.is_file() or not line_path.is_file():
            split_report[split] = {"status": "MISSING"}
            continue
        frame, lines = read_source(path), read_source(line_path)
        dates = pd.to_datetime(frame["Order_Date"], errors="coerce")
        split_orders[split] = set(frame["Order_ID"])
        split_report[split] = {"rows": len(frame), "line_rows": len(lines),
                              "minimum": str(dates.min()), "maximum": str(dates.max()),
                              "invalid_dates": int(dates.isna().sum()), "duplicate_order_keys": int(frame.duplicated("Order_ID").sum()),
                              "orphan_lines": int((~lines["Order_ID"].isin(frame["Order_ID"])).sum())}
    overlap = {}
    for a, b in (("train", "validation"), ("train", "test"), ("validation", "test")):
        if a in split_orders and b in split_orders:
            overlap[a + "_" + b] = len(split_orders[a] & split_orders[b])
    report["splits"] = {"tables": split_report, "order_key_overlap": overlap,
                        "all_cleaned_orders_covered": set.union(*split_orders.values()) == set(cleaned["orders"]["Order_ID"]) if len(split_orders) == 3 and "orders" in cleaned else False,
                        "task_scope": "Transaction chronology verified; task-specific daily demand preparation and validation have separate artifacts."}
    try:
        import pyarrow.parquet as pq
    except ImportError:
        report["parquet"] = {"status": "BLOCKED", "reason": "pyarrow unavailable", "files_present": len(list((root / "parquet_data").rglob("*.parquet")))}
    else:
        for name, frame in cleaned.items():
            path = root / "parquet_data" / "tables" / (TABLE_SCHEMAS[name].filename.removesuffix(".csv") + ".parquet")
            if not path.is_file():
                report["parquet"][name] = {"status": "MISSING"}
                continue
            try:
                actual = pq.read_table(path)
                report["parquet"][name] = {"rows": actual.num_rows, "schema": str(actual.schema),
                                          "row_parity": actual.num_rows == len(frame),
                                          "column_parity": actual.column_names == list(frame.columns), "readable": True}
            except Exception as exc:
                report["parquet"][name] = {"status": "FAILED", "error": type(exc).__name__}
    after = {p.name: sha256(p) for p in sorted(raw_root.glob("*.csv"))}
    report["raw_hashes"] = baseline
    report["raw_unchanged"] = baseline == after
    report["raw_total_rows"] = sum(t.get("rows", 0) for t in report["raw_tables"].values())
    report["cleaned_total_rows"] = sum(t.get("rows", 0) for t in report["cleaned_tables"].values())
    report["total_definition"] = "Sum of table rows, not distinct real-world entities."
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    settings = load_settings()
    result = audit_dataset(settings.project_root)
    target = args.output or settings.reports_root / "main_dataset_audit.json"
    if settings.raw_data_dir == target.resolve() or settings.raw_data_dir in target.resolve().parents:
        parser.error("Audit output must not be inside raw_data")
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
    print("RAW_TOTAL_ROWS=" + str(result["raw_total_rows"]))
    print("CLEANED_TOTAL_ROWS=" + str(result["cleaned_total_rows"]))
    print("RAW_UNCHANGED=" + str(result["raw_unchanged"]).upper())
    print("AUDIT_STATUS=PASS" if result["raw_unchanged"] else "AUDIT_STATUS=FAIL")
    return 0 if result["raw_unchanged"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
