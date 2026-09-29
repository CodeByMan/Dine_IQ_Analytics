"""Compare a captured machine report with the SRS hardware/interface baseline."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIN_MEMORY_BYTES = 8 * 1024**3
MIN_FREE_DISK_BYTES = 500 * 1024**3


def main() -> int:
    capture = ROOT / "reports/operations/environment_baseline.json"
    if not capture.is_file():
        raise SystemExit("HARDWARE BASELINE: BLOCKED — run scripts/capture_environment.py first")
    source = json.loads(capture.read_text(encoding="utf-8"))
    memory = source.get("physical_memory_bytes")
    free_disk = source.get("project_disk_free_bytes")
    checks = {
        "ram_minimum_8_gib": {
            "status": "PASS" if isinstance(memory, int) and memory >= MIN_MEMORY_BYTES else "FAIL",
            "observed_bytes": memory,
            "minimum_bytes": MIN_MEMORY_BYTES,
        },
        "free_disk_minimum_500_gib": {
            "status": "PASS" if isinstance(free_disk, int) and free_disk >= MIN_FREE_DISK_BYTES else "FAIL",
            "observed_bytes": free_disk,
            "minimum_bytes": MIN_FREE_DISK_BYTES,
        },
        "processor_class_i5_or_i7": {
            "status": "UNVERIFIED",
            "reason": "The captured report does not identify the processor model/class.",
        },
        "color_svga_display": {
            "status": "UNVERIFIED",
            "reason": "The captured report does not record display capability/resolution.",
        },
        "stable_internet": {
            "status": "UNVERIFIED" if source.get("internet_reachable") else "FAIL",
            "reason": "One connectivity probe does not establish a stable connection over time.",
        },
    }
    verified = all(item["status"] == "PASS" for item in checks.values())
    result = {
        "requirement_id": "NFR-EX-08",
        "source_report": str(capture.relative_to(ROOT)),
        "captured_platform": source.get("platform"),
        "checks": checks,
        "overall_status": "PASS" if verified else "NOT_VERIFIED",
        "note": "The included capture describes the development WSL environment; capture the actual evaluation machine before final sign-off.",
    }
    output = ROOT / "reports/operations/hardware_baseline_comparison.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for name, item in checks.items():
        print(f"[{item['status']}] {name}")
    print(f"NFR-EX-08 HARDWARE BASELINE: {result['overall_status']}")
    print(f"REPORT: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
