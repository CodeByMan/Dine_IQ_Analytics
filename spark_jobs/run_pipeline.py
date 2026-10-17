from __future__ import annotations

import argparse
import sys

from dineiq.cli import main as cli_main


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("validate-data", "build-data-foundation", "build-descriptive-analytics",
                                         "build-demand-planning", "build-operational-intelligence", "build-demand-models"))
    parser.add_argument("--horizon-days", type=int, default=30)
    args = parser.parse_args()
    original = sys.argv
    try:
        sys.argv = ["dineiq", args.stage]
        if args.stage == "build-demand-planning":
            if not 1 <= args.horizon_days <= 366:
                parser.error("--horizon-days must be between 1 and 366")
            sys.argv.extend(["--horizon-days", str(args.horizon_days)])
        status = cli_main()
    except Exception as exc:
        print(f"SPARK_JOB_ERROR={type(exc).__name__}: {exc}", file=sys.stderr)
        status = 1
    finally:
        sys.argv = original
    print("SPARK_JOB_STATUS=" + ("PASS" if status == 0 else "FAIL"))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
