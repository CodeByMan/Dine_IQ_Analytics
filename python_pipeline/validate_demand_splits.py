from __future__ import annotations

import argparse
import json
from pathlib import Path
import pandas as pd
from dineiq.config import load_settings
from dineiq.analytics.python_demand_model import INPUT_COLUMNS


def validate(root: Path):
    results, bounds = {}, []
    for split in ("train", "validation", "test"):
        path = root / (split + ".csv")
        frame = pd.read_csv(path, dtype={"Item_ID": str, "Location_ID": str})
        if list(frame.columns) != list(INPUT_COLUMNS):
            raise ValueError(f"Unexpected {split} demand schema")
        dates = pd.to_datetime(frame["Date"], errors="raise")
        quantities = pd.to_numeric(frame["Target_Quantity"], errors="raise")
        invalid = (dates.isna() | quantities.isna() | (quantities < 0) | ~frame["Is_Available"].eq(1)
                   | frame["Item_ID"].isna() | frame["Location_ID"].isna())
        duplicates = frame.duplicated(["Date", "Item_ID", "Location_ID"]).sum()
        passed = bool(len(frame)) and not invalid.any() and duplicates == 0 and dates.is_monotonic_increasing
        results[split] = {"rows": len(frame), "invalid_rows": int(invalid.sum()),
                          "duplicate_events": int(duplicates), "minimum_date": str(dates.min()),
                          "maximum_date": str(dates.max()), "chronologically_sorted": dates.is_monotonic_increasing,
                          "passed": bool(passed)}
        bounds.append((dates.min(), dates.max()))
    chronological = bounds[0][1] < bounds[1][0] and bounds[1][1] < bounds[2][0]
    return {"splits": results, "strict_chronology_and_event_separation": bool(chronological),
            "passed": bool(chronological and all(r["passed"] for r in results.values())),
            "scope": "Rolling one-day-ahead demand; customers/branches may recur, but dated target events do not overlap. Not a universal churn/classification split."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--splits", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    settings = load_settings()
    result = validate(args.splits or settings.splits_dir / "demand_forecast")
    if args.output:
        raw = settings.raw_data_dir.resolve()
        if args.output.resolve() == raw or raw in args.output.resolve().parents:
            raise ValueError("Validation reports cannot enter immutable raw_data")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2)
    for split, record in result["splits"].items():
        print(f"{split.upper()}_ROWS={record['rows']}")
    print("DEMAND_SPLIT_STATUS=" + ("PASS" if result["passed"] else "FAIL"))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
