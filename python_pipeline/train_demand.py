from __future__ import annotations

import argparse
import json
from pathlib import Path

from dineiq.analytics.python_demand_model import train_python_demand_model
from dineiq.config import load_settings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--splits", type=Path)
    parser.add_argument("--output", type=Path, required=True, help="Fresh, nonexisting run directory")
    args = parser.parse_args()
    settings = load_settings()
    splits = args.splits or settings.splits_dir / "demand_forecast"
    if args.output.exists():
        raise FileExistsError("Choose a fresh model run output; existing evidence is not overwritten")
    if settings.raw_data_dir == args.output.resolve() or settings.raw_data_dir in args.output.resolve().parents:
        raise ValueError("Model outputs cannot be placed in raw_data")
    for split in ("train", "validation", "test"):
        if not (splits / (split + ".csv")).is_file():
            raise FileNotFoundError(f"Missing demand split: {split}")
    result = train_python_demand_model(
        splits / "train.csv", splits / "validation.csv", splits / "test.csv",
        args.output / "selected_model.joblib", args.output / "test_predictions.csv")
    (args.output / "model_report.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print("TEST_RMSE=" + str(result["test_metrics"]["rmse"]))
    print("PYTHON_MODEL_STATUS=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
