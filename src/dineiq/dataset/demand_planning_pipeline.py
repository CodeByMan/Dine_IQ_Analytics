"""demand planning SRS analytics pipeline, gated on verified data-foundation evidence."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession, functions as F

from dineiq.analytics.demand_forecast import build_demand_forecasts
from dineiq.analytics.market_basket import build_market_basket
from dineiq.analytics.price_sensitivity import build_price_sensitivity
from dineiq.analytics.promotion_effectiveness import build_promotion_effectiveness
from dineiq.analytics.wastage_risk import build_wastage_risk
from dineiq.analytics.wastage_risk_model import train_wastage_risk_model
from dineiq.config import Settings
from dineiq.dataset.schema import TABLE_SCHEMAS


DEMAND_PLANNING_REQUIREMENTS = (
    "FR-EX-25", "FR-EX-26", "FR-EX-27", "FR-EX-28", "FR-EX-29", "FR-EX-31",
    "FR-EX-32", "FR-EX-33", "FR-EX-73", "FR-EX-74", "FR-EX-75", "FR-IN-03",
)
OUTPUTS = (
    "association_rules", "bundle_recommendations", "forecast_evaluation",
    "forecast_item_location_daily", "forecast_category_daily", "forecast_location_daily",
    "wastage_risk_prediction", "price_sensitivity", "promotion_effectiveness",
)


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _require_prior_evidence(settings: Settings) -> None:
    for stage in ("data_foundation", "descriptive_analytics"):
        report = settings.reports_root / stage / "build_summary.json"
        if not report.is_file() or not _json(report).get("passed"):
            raise RuntimeError(f"Verified {stage} build evidence is required before demand planning.")
        trace = settings.project_root / "docs" / f"{stage}_traceability.csv"
        if not trace.is_file():
            raise FileNotFoundError(f"Verified {stage} traceability is missing: {trace}")
        with trace.open("r", newline="", encoding="utf-8-sig") as stream:
            rows = list(csv.DictReader(stream))
        if not rows or any(row.get("Status") != "VERIFIED" for row in rows):
            raise RuntimeError(f"{stage} traceability is not fully VERIFIED.")


def _load_demand_split(spark: SparkSession, path: Path):
    if not path.is_file():
        raise FileNotFoundError(f"Required demand split is missing: {path}")
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Item_ID", StringType(), True),
        StructField("Location_ID", StringType(), True),
        StructField("Is_Available", IntegerType(), True),
        StructField("Target_Quantity", DoubleType(), True),
    ])
    return spark.read.schema(schema).option("header", "true").csv(str(path))


def build_demand_planning(spark: SparkSession, settings: Settings, horizon_days: int = 30) -> dict[str, Any]:
    """Build only demand planning analytical outputs from immutable earlier artifacts."""
    _require_prior_evidence(settings)
    data_foundation_root = settings.artifacts_root / "data_foundation"
    cleaned_root = data_foundation_root / "cleaned_parquet"
    missing = [table for table in TABLE_SCHEMAS if not (cleaned_root / table).is_dir()]
    if missing:
        raise FileNotFoundError(f"Verified data foundation cleaned Parquet missing: {', '.join(missing)}")
    tables = {name: spark.read.parquet(str(cleaned_root / name)) for name in TABLE_SCHEMAS}
    menu_features = spark.read.parquet(str(data_foundation_root / "menu_features"))
    split_root = settings.splits_dir / "demand_forecast"
    train = _load_demand_split(spark, split_root / "train.csv")
    validation = _load_demand_split(spark, split_root / "validation.csv")
    test = _load_demand_split(spark, split_root / "test.csv")

    wastage_model = train_wastage_risk_model(
        tables,
        menu_features,
        settings.models_dir / "wastage_risk" / "selected_model.joblib",
        settings.reports_root / "demand_planning" / "wastage_model.json",
    )
    outputs = {}
    outputs.update(build_market_basket(tables))
    outputs.update(build_demand_forecasts(train, validation, test, tables["menu_items"], horizon_days))
    outputs["wastage_risk_prediction"] = build_wastage_risk(
        tables, menu_features, horizon_days, model_predictions=wastage_model["predictions"]
    )
    outputs["price_sensitivity"] = build_price_sensitivity(tables)
    outputs["promotion_effectiveness"] = build_promotion_effectiveness(tables)

    output_root = settings.artifacts_root / "demand_planning"
    counts: dict[str, int] = {}
    readbacks: dict[str, int] = {}
    verified: dict[str, Any] = {}
    for name in OUTPUTS:
        cached = outputs[name].persist()
        try:
            counts[name] = int(cached.count())
            target = output_root / name
            cached.write.mode("overwrite").option("compression", "snappy").parquet(str(target))
            verified[name] = spark.read.parquet(str(target))
            readbacks[name] = int(verified[name].count())
            if counts[name] != readbacks[name]:
                raise RuntimeError(f"demand planning Parquet read-back row mismatch: {name}")
        finally:
            cached.unpersist()

    metric_names = {row["metric"] for row in verified["forecast_evaluation"].select("metric").distinct().collect()}
    expected_metrics = {"MAE", "RMSE", "MAPE_nonzero_actuals", "R2"}
    classes = {row["price_sensitivity_class"] for row in verified["price_sensitivity"].select("price_sensitivity_class").distinct().collect()}
    expected_classes = {"Highly Price Sensitive", "Moderately Price Sensitive", "Low Price Sensitivity", "Insufficient evidence"}
    split_dates = {
        "train_max": str(train.select(F.max(F.to_date("Date"))).first()[0]),
        "validation_min": str(validation.select(F.min(F.to_date("Date"))).first()[0]),
        "validation_max": str(validation.select(F.max(F.to_date("Date"))).first()[0]),
        "test_min": str(test.select(F.min(F.to_date("Date"))).first()[0]),
    }
    chronological = (
        split_dates["train_max"] < split_dates["validation_min"]
        and split_dates["validation_max"] < split_dates["test_min"]
    )
    passed = (
        len(outputs) == len(OUTPUTS) and len(readbacks) == len(OUTPUTS)
        and all(counts[name] == readbacks[name] for name in OUTPUTS)
        and expected_metrics.issubset(metric_names) and classes.issubset(expected_classes) and bool(classes)
        and chronological and horizon_days >= 1 and wastage_model["summary"]["passed"]
    )
    summary = {
        "requirement_ids": list(DEMAND_PLANNING_REQUIREMENTS), "output_rows": counts,
        "parquet_readback_rows": readbacks, "forecast_horizon_days": horizon_days,
        "chronological_split_boundaries": split_dates, "chronological_splits": chronological,
        "forecast_metrics": sorted(metric_names), "price_sensitivity_classes": sorted(classes),
        "wastage_model": wastage_model["summary"],
        "source_data_modified": False, "passed": bool(passed),
    }
    report = settings.reports_root / "demand_planning" / "build_summary.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary
