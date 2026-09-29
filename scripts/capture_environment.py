"""Record target-machine details for comparison with the SRS baseline."""
from __future__ import annotations

import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import urllib.request


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    try:
        memory_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, ValueError, OSError):
        memory_bytes = None
    usage = shutil.disk_usage(root)
    try:
        java = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=5)
        java_version = (java.stderr or java.stdout).splitlines()[0] if java.returncode == 0 else "unavailable"
    except (OSError, subprocess.TimeoutExpired):
        java_version = "unavailable"
    try:
        with urllib.request.urlopen("https://pypi.org", timeout=5) as response:
            internet = 200 <= response.status < 400
    except Exception:
        internet = False
    report = {
        "requirement_id": "NFR-EX-08", "platform": platform.platform(),
        "python_version": sys.version.split()[0], "logical_cpu_count": os.cpu_count(),
        "physical_memory_bytes": memory_bytes, "project_disk_free_bytes": usage.free,
        "java_version": java_version, "internet_reachable": internet,
        "srs_baseline_comparison": "PENDING: compare these measurements with the exact SRS thresholds.",
    }
    output = root / "reports" / "operations" / "environment_baseline.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"REPORT: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
