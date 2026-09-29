"""operational intelligence anomaly, location, channel, churn and recommendation pipeline."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F

from dineiq.analytics.anomaly_detection import build_rating_anomalies, build_sales_anomalies
from dineiq.analytics.channel_analysis import build_channel_analysis
from dineiq.analytics.churn_risk import CHURN_FACTORS, build_customer_churn_risk
from dineiq.analytics.location_intelligence import MENU_CLASSES, build_location_comparison, build_location_menu_performance
from dineiq.analytics.promotion_traps import build_promotion_traps
from dineiq.analytics.recommendations import RECOMMENDATION_TYPES, build_recommendations
from dineiq.config import Settings
from dineiq.dataset.ingest import read_table
from dineiq.dataset.schema import TABLE_SCHEMAS


OPERATIONAL_INTELLIGENCE_REQUIREMENTS = (
    "FR-EX-34", "FR-EX-36", "FR-EX-37", "FR-EX-38", "FR-EX-39", "FR-EX-40",
    "FR-EX-41", "FR-EX-47", "FR-EX-76", "FR-EX-77", "FR-EX-78", "FR-EX-79", "FR-EX-80",
)
OUTPUTS = (
    "promotion_trap_detection", "rating_anomalies", "sales_anomalies", "location_comparison",
    "location_menu_performance", "channel_summary", "channel_category_preferences",
    "channel_peak_periods", "customer_churn_risk", "recommendations",
)
RATING_ANOMALIES = (
    "Sudden rating spike", "Sudden rating drop", "Excessive identical ratings",
    "High rating volume in a short period", "Rating inconsistent with purchasing pattern",
)
SALES_ANOMALIES = (
    "Sudden sales spike", "Sudden sales drop", "Abnormally high order value",
    "Unusual discount", "Unexpected demand", "Duplicate transaction",
)
PROMOTION_FLAGS = (
    "promotion_sales_up_profit_down", "promotion_customer_up_margin_collapse",
    "promotion_increased_wastage", "purchases_only_during_discount_window",
    "sales_shift_from_more_profitable_product",
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _require_verified_build(settings: Settings, stage: str) -> dict[str, Any]:
    report_path = settings.reports_root / stage / "build_summary.json"
    if not report_path.is_file():
        raise FileNotFoundError(f"Verified {stage} build report is required: {report_path}")
    report = _read_json(report_path)
    if not report.get("passed"):
        raise RuntimeError(f"{stage} build report is not PASS.")
    trace_name = f"{stage}_traceability.csv"
    trace = settings.project_root / "docs" / trace_name
    if not trace.is_file():
        raise FileNotFoundError(f"Verified {stage} traceability is required: {trace}")
    with trace.open("r", newline="", encoding="utf-8-sig") as stream:
        rows = list(csv.DictReader(stream))
    if not rows or any(row.get("Status") != "VERIFIED" for row in rows):
        raise RuntimeError(f"{stage} traceability is not fully VERIFIED.")
    return report


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_operational_intelligence(spark: SparkSession, settings: Settings) -> dict[str, Any]:
    """Build only operational intelligence outputs from prior verified Parquet and source inputs."""
    for stage in ("data_foundation", "descriptive_analytics", "demand_planning"):
        _require_verified_build(settings, stage)
    cleaned_root = settings.artifacts_root / "data_foundation" / "cleaned_parquet"
    missing = [name for name in TABLE_SCHEMAS if not (cleaned_root / name).is_dir()]
    if missing:
        raise FileNotFoundError(f"Verified cleaned Parquet missing: {', '.join(missing)}")
    tables = {name: spark.read.parquet(str(cleaned_root / name)) for name in TABLE_SCHEMAS}
    # The cleaned view intentionally removes known duplicates. Read raw order
    # headers and raw order lines so discount and duplicate detectors inspect
    # the source evidence rather than a cleaned view that may have removed it.
    tables["raw_orders"] = read_table(spark, settings.data_root, "orders")
    tables["raw_order_items"] = read_table(spark, settings.data_root, "order_items")
    tables["raw_ratings"] = read_table(spark, settings.data_root, "ratings")

    descriptive_analytics_root = settings.artifacts_root / "descriptive_analytics"
    demand_planning_root = settings.artifacts_root / "demand_planning"
    menu_classes = spark.read.parquet(str(descriptive_analytics_root / "menu_profitability_classification"))
    customer_segments = spark.read.parquet(str(descriptive_analytics_root / "customer_segments"))
    effectiveness = spark.read.parquet(str(demand_planning_root / "promotion_effectiveness"))
    wastage_risk = spark.read.parquet(str(demand_planning_root / "wastage_risk_prediction"))
    price_sensitivity = spark.read.parquet(str(demand_planning_root / "price_sensitivity"))
    bundles = spark.read.parquet(str(demand_planning_root / "bundle_recommendations"))
    forecasts = spark.read.parquet(str(demand_planning_root / "forecast_item_location_daily"))

    outputs: dict[str, DataFrame] = {
        "promotion_trap_detection": build_promotion_traps(tables, effectiveness),
        "rating_anomalies": build_rating_anomalies(tables),
        "sales_anomalies": build_sales_anomalies(tables),
        "location_comparison": build_location_comparison(tables),
        "location_menu_performance": build_location_menu_performance(tables),
        **build_channel_analysis(tables),
        "customer_churn_risk": build_customer_churn_risk(tables),
    }
    outputs["recommendations"] = build_recommendations(
        menu_classes, wastage_risk, price_sensitivity, bundles, forecasts,
        customer_segments, outputs["promotion_trap_detection"],
        outputs["location_comparison"], outputs["sales_anomalies"],
        inventory=tables.get("inventory"),
    )

    if set(outputs) != set(OUTPUTS):
        raise RuntimeError(f"operational intelligence output contract mismatch: {sorted(set(OUTPUTS) ^ set(outputs))}")
    output_root = settings.artifacts_root / "operational_intelligence"
    counts: dict[str, int] = {}
    readbacks: dict[str, int] = {}
    verified: dict[str, DataFrame] = {}
    for name in OUTPUTS:
        cached = outputs[name].persist()
        try:
            counts[name] = int(cached.count())
            target = output_root / name
            cached.write.mode("overwrite").option("compression", "snappy").parquet(str(target))
            verified[name] = spark.read.parquet(str(target))
            readbacks[name] = int(verified[name].count())
            if counts[name] != readbacks[name]:
                raise RuntimeError(f"operational intelligence Parquet read-back row mismatch: {name}")
        finally:
            cached.unpersist()

    rating_types = {row["anomaly_type"] for row in verified["rating_anomalies"].select("anomaly_type").distinct().collect()}
    sales_types = {row["anomaly_type"] for row in verified["sales_anomalies"].select("anomaly_type").distinct().collect()}
    recommendation_types = {row["recommendation_type"] for row in verified["recommendations"].select("recommendation_type").distinct().collect()}
    promotion_flags = sorted(set(PROMOTION_FLAGS) - set(verified["promotion_trap_detection"].columns))
    churn_missing = sorted(set(CHURN_FACTORS) - set(verified["customer_churn_risk"].columns))
    invalid_classes = verified["location_menu_performance"].where(
        F.col("performance_class").isNull() | ~F.col("performance_class").isin(*MENU_CLASSES)
    ).limit(1).count()
    missing_evidence = verified["recommendations"].where(
        F.col("supporting_evidence").isNull() | (F.length(F.trim("supporting_evidence")) == 0)
        | F.col("priority").isNull()
    ).limit(1).count()
    customer_count = tables["customers"].count()
    churn_count = verified["customer_churn_risk"].count()
    location_count = tables["restaurants"].count()
    comparison_count = verified["location_comparison"].count()
    passed = (
        len(readbacks) == len(OUTPUTS)
        and all(counts[name] == readbacks[name] for name in OUTPUTS)
        and not promotion_flags and not churn_missing and not invalid_classes and not missing_evidence
        and churn_count == customer_count and comparison_count == location_count
        and recommendation_types.issubset(set(RECOMMENDATION_TYPES))
    )
    summary = {
        "requirement_ids": list(OPERATIONAL_INTELLIGENCE_REQUIREMENTS), "output_rows": counts,
        "parquet_readback_rows": readbacks, "rating_anomaly_types_detected": sorted(rating_types),
        "sales_anomaly_types_detected": sorted(sales_types),
        "rating_anomaly_types_supported": list(RATING_ANOMALIES),
        "sales_anomaly_types_supported": list(SALES_ANOMALIES),
        "promotion_pattern_flags": list(PROMOTION_FLAGS),
        "recommendation_types_generated": sorted(recommendation_types),
        "churn_factors": list(CHURN_FACTORS), "customer_rows": customer_count,
        "churn_rows": churn_count, "location_source_rows": location_count,
        "location_comparison_rows": comparison_count,
        "recommendations_have_evidence_and_priority": not bool(missing_evidence),
        "source_data_modified": False, "passed": bool(passed),
    }
    _write_json(settings.reports_root / "operational_intelligence" / "build_summary.json", summary)
    return summary
