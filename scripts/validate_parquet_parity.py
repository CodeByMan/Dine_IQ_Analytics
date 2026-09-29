"""Validate canonical CSV/Parquet row-count and column parity."""

from __future__ import annotations

import json

from dineiq.config import load_settings
from dineiq.dataset.parity import check_all_parity


def main() -> int:
    settings = load_settings()
    report = check_all_parity(settings.data_root)
    output = settings.reports_root / "operations" / "parquet_csv_parity.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for table, check in report["tables"].items():
        status = "PASS" if check["passed"] else "FAIL"
        detail = check.get("error") or f"CSV={check.get('csv_rows')} Parquet={check.get('parquet_rows')}"
        print(f"[{status}] {table}: {detail}")
    print(f"PARQUET/CSV PARITY: {'PASS' if report['passed'] else 'FAIL'}")
    print(f"REPORT: {output}")
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
