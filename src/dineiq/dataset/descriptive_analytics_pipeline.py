"""descriptive analytics analytics pipeline consuming verified data foundation outputs."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, functions as F

from dineiq.analytics.customer_segments import CUSTOMER_SEGMENTS, build_customer_rfm_and_segments
from dineiq.analytics.menu_performance import MENU_CLASSES, TRICKY_CASE_FLAGS, build_menu_performance
from dineiq.analytics.rating_analysis import build_rating_analysis
from dineiq.analytics.time_patterns import build_time_patterns
from dineiq.analytics.wastage_analysis import build_wastage_analysis
from dineiq.config import Settings
from dineiq.dataset.schema import TABLE_SCHEMAS


DESCRIPTIVE_ANALYTICS_REQUIREMENTS = (
    "FR-EX-20", "FR-EX-21", "FR-EX-22", "FR-EX-23", "FR-EX-24", "FR-EX-30",
    "FR-EX-35", "FR-EX-70", "FR-EX-71", "FR-EX-72", "FR-EX-88", "FR-IN-01",
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _require_data_foundation(settings: Settings) -> dict[str, Any]:
    validation_path = settings.reports_root / "data_foundation" / "build_summary.json"
    if not validation_path.is_file():
        raise FileNotFoundError("data foundation build report not found; data foundation must pass first.")
    report = _read_json(validation_path)
    if not report.get("passed"):
        raise RuntimeError("data foundation build report is not PASS; descriptive analytics is blocked.")
    traceability = settings.project_root / "docs" / "data_foundation_traceability.csv"
    if not traceability.is_file():
        raise FileNotFoundError(f"Verified data foundation traceability matrix not found: {traceability}")
    with traceability.open("r", newline="", encoding="utf-8-sig") as stream:
        rows = list(csv.DictReader(stream))
    expected = {f"FR-EX-{number}" for number in range(12, 20)} | {"FR-EX-82", "FR-EX-83", "FR-EX-84"}
    statuses = {row.get("Requirement ID"): row.get("Status") for row in rows}
    if any(statuses.get(requirement) != "VERIFIED" for requirement in expected):
        raise RuntimeError("data foundation traceability is not VERIFIED for all 11 requirements.")
    return report


def _write_parquet(frame, path: Path) -> None:
    frame.write.mode("overwrite").option("compression", "snappy").parquet(str(path))


def build_descriptive_analytics(spark: SparkSession, settings: Settings) -> dict[str, Any]:
    """Build requirement-scoped analytics from immutable data foundation processed tables."""
    data_foundation_report = _require_data_foundation(settings)
    root = settings.artifacts_root / "data_foundation"
    cleaned_root = root / "cleaned_parquet"
    missing = [name for name in TABLE_SCHEMAS if not (cleaned_root / name).is_dir()]
    if missing:
        raise FileNotFoundError(f"data foundation cleaned Parquet missing: {', '.join(missing)}")
    tables = {name: spark.read.parquet(str(cleaned_root / name)) for name in TABLE_SCHEMAS}
    menu_features = spark.read.parquet(str(root / "menu_features"))
    location_features = spark.read.parquet(str(root / "menu_location_features"))

    outputs: dict[str, Any] = {}
    outputs.update(build_menu_performance(tables, menu_features, location_features))
    outputs.update(build_customer_rfm_and_segments(tables))
    outputs.update(build_time_patterns(tables))
    outputs.update(build_wastage_analysis(tables))
    outputs.update(build_rating_analysis(tables))

    output_root = settings.artifacts_root / "descriptive_analytics"
    counts: dict[str, int] = {}
    readback: dict[str, int] = {}
    verified_outputs: dict[str, Any] = {}
    for name, frame in outputs.items():
        cached = frame.persist()
        try:
            counts[name] = int(cached.count())
            _write_parquet(cached, output_root / name)
            verified_outputs[name] = spark.read.parquet(str(output_root / name))
            readback[name] = int(verified_outputs[name].count())
            if readback[name] != counts[name]:
                raise RuntimeError(f"descriptive analytics Parquet read-back row mismatch: {name}")
        finally:
            cached.unpersist()

    class_counts = {
        row["performance_class"]: int(row["count"])
        for row in verified_outputs["menu_profitability_classification"].groupBy("performance_class").count().collect()
    }
    segment_counts = {
        row["customer_segment"]: int(row["count"])
        for row in verified_outputs["customer_segments"].groupBy("customer_segment").count().collect()
    }
    bad_class_rows = verified_outputs["menu_profitability_classification"].where(
        F.col("performance_class").isNull()
        | ~F.col("performance_class").isin(*MENU_CLASSES)
    ).limit(1).count()
    bad_segment_rows = verified_outputs["customer_segments"].where(
        F.col("customer_segment").isNull()
        | ~F.col("customer_segment").isin(*CUSTOMER_SEGMENTS)
    ).limit(1).count()
    missing_factors = sorted(set((
        "recency_days", "frequency", "monetary_value", "average_order_value", "visit_frequency",
        "favorite_category_id", "promotion_sensitivity", "preferred_channel_id",
        "preferred_time_of_day", "repeat_behavior",
    )) - set(outputs["customer_segments"].columns))
    missing_cases = sorted(set(TRICKY_CASE_FLAGS) - set(outputs["tricky_menu_cases"].columns))
    class_row_count = counts["menu_profitability_classification"]
    customer_row_count = counts["customer_segments"]
    menu_source_count = tables["menu_items"].count()
    customer_source_count = tables["customers"].count()

    passed = (
        data_foundation_report.get("passed", False)
        and len(outputs) == 21
        and len(readback) == 21
        and all(readback[name] == counts[name] for name in outputs)
        and class_row_count == menu_source_count
        and customer_row_count == customer_source_count
        and not bad_class_rows and not bad_segment_rows
        and not missing_factors and not missing_cases
        and set(class_counts).issubset(set(MENU_CLASSES))
        and set(segment_counts).issubset(set(CUSTOMER_SEGMENTS))
    )
    summary = {
        "requirement_ids": list(DESCRIPTIVE_ANALYTICS_REQUIREMENTS),
        "output_rows": counts,
        "parquet_readback_rows": readback,
        "menu_class_counts": class_counts,
        "customer_segment_counts": segment_counts,
        "menu_classified_rows": class_row_count,
        "menu_source_rows": menu_source_count,
        "customer_segmented_rows": customer_row_count,
        "customer_source_rows": customer_source_count,
        "menu_classes": list(MENU_CLASSES),
        "customer_segments": list(CUSTOMER_SEGMENTS),
        "tricky_case_flags": list(TRICKY_CASE_FLAGS),
        "missing_customer_factors": missing_factors,
        "missing_tricky_case_flags": missing_cases,
        "source_data_modified": False,
        "passed": bool(passed),
    }
    _write_json(settings.reports_root / "descriptive_analytics" / "build_summary.json", summary)
    return summary
