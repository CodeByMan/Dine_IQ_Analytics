"""Audit requirement IDs and statuses across all work-area matrices."""

from __future__ import annotations

import csv
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
VALID_STATUSES = {"VERIFIED", "BLOCKED", "NOT_STARTED", "IN_PROGRESS", "IMPLEMENTED", "TESTED"}


def main() -> int:
    matrices = sorted((ROOT / "docs").glob("*_traceability.csv"))
    if not matrices:
        print("TRACEABILITY AUDIT: FAIL — no requirement matrices found")
        return 2
    all_rows = []
    for path in matrices:
        if re.search(r"^(?:batch|stage)[_-]?[1-4][ab](?:[_-]|\.)", path.name.lower()):
            print(f"TRACEABILITY AUDIT: FAIL — legacy workflow label in {path.name}")
            return 2
        with path.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            required = {"Requirement ID", "Status"}
            if not reader.fieldnames or not required.issubset(reader.fieldnames):
                print(f"TRACEABILITY AUDIT: FAIL — invalid columns in {path.name}")
                return 2
            rows = list(reader)
        if not rows:
            print(f"TRACEABILITY AUDIT: FAIL — empty matrix {path.name}")
            return 2
        all_rows.extend((path.name, row) for row in rows)
    ids = [row["Requirement ID"].strip() for _path, row in all_rows]
    if any(not value for value in ids) or len(ids) != len(set(ids)):
        print("TRACEABILITY AUDIT: FAIL — blank or duplicate requirement IDs")
        return 2
    invalid = [(path, row["Requirement ID"], row["Status"]) for path, row in all_rows
               if row["Status"] not in VALID_STATUSES]
    if invalid:
        print(f"TRACEABILITY AUDIT: FAIL — invalid statuses: {invalid}")
        return 2
    verified = sum(row["Status"] == "VERIFIED" for _path, row in all_rows)
    blocked = [(row["Requirement ID"], row["Status"]) for _path, row in all_rows
               if row["Status"] != "VERIFIED"]
    print(f"TRACEABILITY MATRICES: {len(matrices)}")
    print(f"REQUIREMENTS: {len(all_rows)}")
    print(f"VERIFIED: {verified}/{len(all_rows)}")
    for requirement_id, status in blocked:
        print(f"[{status}] {requirement_id}")
    print(f"TRACEABILITY AUDIT: {'PASS' if not blocked else 'INCOMPLETE'}")
    return 0 if not blocked else 1


if __name__ == "__main__":
    raise SystemExit(main())
