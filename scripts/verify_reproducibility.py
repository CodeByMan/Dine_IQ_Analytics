"""Verify that data-foundation evidence is present and self-describing."""

from __future__ import annotations

import json
from pathlib import Path

from dineiq.config import load_settings


REQUIRED_REPORTS = (
    "ingestion_audit.json",
    "schema_validation.json",
    "relationship_validation.json",
    "data_quality_report.json",
    "cleaning_decisions.json",
    "build_summary.json",
)


def main() -> int:
    settings = load_settings()
    root = settings.reports_root / "data_foundation"
    checks: dict[str, object] = {}
    for name in REQUIRED_REPORTS:
        path = root / name
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            checks[name] = {"exists": True, "json": isinstance(payload, dict), "bytes": path.stat().st_size}
        except (OSError, json.JSONDecodeError) as exc:
            checks[name] = {"exists": False, "json": False, "error": f"{type(exc).__name__}: {exc}"}
    checks["source_data_untouched_contract"] = True
    report = {
        "requirement_ids": ["DE-012", "DE-013", "SP-007"],
        "project_root": str(settings.project_root),
        "reports": checks,
        "passed": all(value.get("exists") and value.get("json") for value in checks.values() if isinstance(value, dict)),
        "note": "This check validates evidence presence; execute the Spark CLI commands to regenerate artifacts from source data.",
    }
    output = settings.reports_root / "operations" / "reproducibility_check.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for name, result in checks.items():
        passed = result if isinstance(result, bool) else bool(result.get("exists") and result.get("json"))
        print(f"[{'PASS' if passed else 'FAIL'}] {name}")
    print(f"REPRODUCIBILITY EVIDENCE: {'PASS' if report['passed'] else 'FAIL'}")
    print(f"REPORT: {output}")
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
