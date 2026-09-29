"""Verify the dataset files required by DS-EX-13 and retain an inventory."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from dineiq.config import load_settings


def verify() -> dict:
    settings = load_settings()
    root = settings.data_root
    expected_tables = (
        "customers", "orders", "order_items", "menu_items", "menu_categories",
        "restaurants", "pricing_history", "promotions", "ratings", "inventory",
        "wastage", "ordering_channels",
    )
    checks = {
        "dataset_generation_script": (root / "scripts" / "generate_dataset.py").is_file(),
        "data_dictionary": (root / "documentation" / "DATA_DICTIONARY.md").is_file(),
        "pk_fk_definitions": (
            (root / "documentation" / "RELATIONSHIPS.md").is_file()
            and (settings.project_root / "src/dineiq/dataset/schema.py").is_file()
            and (settings.project_root / "src/dineiq/dataset/catalog.py").is_file()
        ),
        "dataset_statistics": (root / "documentation" / "DATASET_STATISTICS.json").is_file(),
        "hidden_data_readiness": (root / "documentation" / "HIDDEN_DATA_READINESS.md").is_file(),
        "raw_csv_tables": all((root / "data" / f"{table}.csv").is_file() for table in expected_tables),
        "parquet_tables": all((root / "parquet" / f"{table}.parquet").is_file() for table in expected_tables),
        "raw_samples": len(list((root / "samples").glob("*_sample.csv"))) >= len(expected_tables),
        "cleaned_samples": len(list((root / "samples/cleaned").glob("*_clean_sample.csv"))) >= len(expected_tables),
        "chronological_splits": all(
            (root / "splits/demand_forecast" / f"{name}.csv").is_file()
            for name in ("train", "validation", "test")
        ),
    }
    stats_path = root / "documentation" / "DATASET_STATISTICS.json"
    try:
        stats = json.loads(stats_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        checks["dataset_statistics"] = False
        stats = {"error": f"{type(exc).__name__}: {exc}"}

    inventory = {
        "requirement_id": "DS-EX-13",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_root": str(root),
        "required_tables": list(expected_tables),
        "checks": checks,
        "statistics": stats,
        "passed": all(checks.values()),
    }
    output = settings.reports_root / "operations" / "dataset_submission_inventory.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for name, passed in checks.items():
        print(f"[{'PASS' if passed else 'FAIL'}] {name}")
    print(f"DS-EX-13 DATASET INVENTORY: {sum(checks.values())}/{len(checks)} "
          f"{'PASS' if inventory['passed'] else 'FAIL'}")
    print(f"REPORT: {output}")
    return inventory


if __name__ == "__main__":
    raise SystemExit(0 if verify()["passed"] else 2)
