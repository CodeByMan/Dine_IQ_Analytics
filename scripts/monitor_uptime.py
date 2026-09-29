"""Measure HTTP availability for a specified SRS evaluation window."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from urllib.error import URLError
from urllib.request import urlopen


def probe(url: str, timeout: float) -> tuple[bool, str]:
    try:
        with urlopen(url, timeout=timeout) as response:
            code = int(response.status)
            return 200 <= code < 400, str(code)
    except Exception as exc:  # capture failures as availability samples
        return False, type(exc).__name__


def availability_percent(successes: int, total: int) -> float:
    return (100.0 * successes / total) if total else 0.0


def run(url: str, duration: float, interval: float, timeout: float, output: Path) -> dict:
    if duration <= 0 or interval <= 0 or timeout <= 0:
        raise ValueError("duration, interval, and timeout must be positive")
    started = datetime.now(timezone.utc)
    deadline = time.monotonic() + duration
    samples = []
    while True:
        ok, detail = probe(url, timeout)
        samples.append({"timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "available": ok, "response": detail})
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(interval, remaining))
    successes = sum(sample["available"] for sample in samples)
    percent = availability_percent(successes, len(samples))
    report = {
        "requirement_id": "NFR-EX-05", "url": url,
        "started_at_utc": started.isoformat(),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": duration, "interval_seconds": interval,
        "sample_count": len(samples), "successful_samples": successes,
        "failed_samples": len(samples) - successes,
        "availability_percent": round(percent, 5), "target_percent": 99.0,
        "passed": percent >= 99.0, "samples": samples,
        "note": "Pass applies only to the recorded window; compare its duration with the SRS evaluation period.",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"UPTIME SAMPLES: {successes}/{len(samples)} succeeded")
    print(f"AVAILABILITY: {percent:.3f}% (target 99.000%)")
    print(f"NFR-EX-05 WINDOW RESULT: {'PASS' if report['passed'] else 'FAIL'}")
    print(f"REPORT: {output}")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8501/")
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=3.0)
    parser.add_argument("--output", type=Path, default=Path("reports/operations/uptime_evidence.json"))
    args = parser.parse_args()
    try:
        report = run(args.url, args.duration_seconds, args.interval_seconds,
                     args.timeout_seconds, args.output)
    except ValueError as exc:
        parser.error(str(exc))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
