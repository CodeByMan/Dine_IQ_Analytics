"""Data-foundation commands; generated outputs never enter the source dataset."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, functions as F

from dineiq.config import Settings
from dineiq.analytics.eda import EDA_OUTPUTS, build_eda_outputs
from dineiq.dataset.cleaning import clean_tables_with_audit
from dineiq.features.engineering import SRS_FEATURES, build_feature_tables
from dineiq.dataset.ingest import invalid_cast_counts, read_table, validate_schema
from dineiq.dataset.ingestion_audit import build_ingestion_audit, resolve_source_paths, schema_inference_demo, write_ingestion_audit
from dineiq.dataset.integrate import build_required_joins, build_sql_aggregates, validate_foreign_keys
from dineiq.dataset.quality import analyze_quality, write_quality_report
from dineiq.dataset.schema import TABLE_SCHEMAS


def create_local_spark(settings: Settings, app_name: str) -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        .config("spark.sql.warehouse.dir", str(settings.artifacts_root / "spark-warehouse"))
        .getOrCreate()
    )


def load_source_tables(spark: SparkSession, settings: Settings) -> dict[str, Any]:
    return {name: read_table(spark, settings.data_root, name) for name in TABLE_SCHEMAS}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_data_foundation(spark: SparkSession, settings: Settings) -> dict[str, Any]:
    ingestion = build_ingestion_audit(settings.data_root)
    inference = next((name for name in TABLE_SCHEMAS if resolve_source_paths(settings.data_root, name)), None)
    if inference:
        try:
            ingestion["inference_demo"] = schema_inference_demo(spark, settings.data_root, inference)
        except Exception as exc:
            ingestion["inference_demo"] = {
                "table": inference,
                "available": False,
                "passed": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
    report_root = settings.reports_root / "data_foundation"
    write_ingestion_audit(ingestion, report_root / "ingestion_audit.json")
    tables = load_source_tables(spark, settings)
    schema_checks = tuple(validate_schema(name, frame, check_types=True) for name, frame in tables.items())
    cast_findings = {
        name: invalid_cast_counts(frame, name)
        for name, frame in tables.items()
    }
    fk_checks = validate_foreign_keys(tables)
    quality = analyze_quality(tables, settings.fixtures_dir)
    schema_result = {
        "requirement_ids": ["FR-EX-12", "FR-EX-13"],
        "checks": [check.as_dict() for check in schema_checks],
        "invalid_nonblank_casts_by_table": cast_findings,
        "passed": all(check.passed for check in schema_checks),
    }
    relationship_result = {
        "requirement_ids": ["FR-EX-13"],
        "checks": [check.as_dict() for check in fk_checks],
        "passed": all(check.passed for check in fk_checks),
    }
    _write_json(report_root / "schema_validation.json", schema_result)
    _write_json(report_root / "relationship_validation.json", relationship_result)
    write_quality_report(quality, report_root / "data_quality_report.json")
    result = {
        "schema": schema_result,
        "relationships": relationship_result,
        "quality": quality,
        "ingestion": ingestion,
        "passed": schema_result["passed"] and relationship_result["passed"] and quality["passed"],
    }
    _write_json(report_root / "validation_summary.json", result)
    return result


def _write_parquet(frame, path: Path, partition_column: str | None = None) -> None:
    writer = frame.write.mode("overwrite").option("compression", "snappy")
    if partition_column:
        writer = writer.partitionBy(partition_column)
    writer.parquet(str(path))


def build_data_foundation(spark: SparkSession, settings: Settings) -> dict[str, Any]:
    validation_path = settings.reports_root / "data_foundation" / "validation_summary.json"
    if not validation_path.is_file():
        raise FileNotFoundError("Run the validate-data command successfully before build-data-foundation.")
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    if not validation.get("passed"):
        raise RuntimeError("data foundation validation summary is not PASS; build is blocked.")

    tables = load_source_tables(spark, settings)
    cleaned, cleaning_audit = clean_tables_with_audit(
        tables,
        quarantine_root=settings.artifacts_root / "data_foundation" / "quarantine",
    )
    output_root = settings.artifacts_root / "data_foundation"
    cleaned_root = output_root / "cleaned_parquet"
    counts: dict[str, int] = {}
    for name, frame in cleaned.items():
        if name == "order_items":
            order_dates = cleaned["orders"].select("Order_ID", "Order_Date").dropDuplicates(["Order_ID"])
            frame = frame.join(order_dates, "Order_ID", "inner").withColumn(
                "Order_Year", F.year("Order_Date")
            )
            counts[name] = frame.count()
            _write_parquet(frame, cleaned_root / name, "Order_Year")
        else:
            counts[name] = frame.count()
            _write_parquet(frame, cleaned_root / name)

    joined = build_required_joins(cleaned)
    join_counts = {name: int(frame.count()) for name, frame in joined.items()}
    if len(joined) != 10:
        raise RuntimeError(f"Expected 10 required Spark joins, found {len(joined)}.")

    order_line_join = joined["orders__order_items"]
    sql_aggregates = build_sql_aggregates(spark, order_line_join)
    sql_summary = sql_aggregates["location"]
    sql_summary_path = output_root / "spark_sql_location_summary"
    _write_parquet(sql_summary, sql_summary_path)
    sql_paths: dict[str, str] = {"location": str(sql_summary_path)}
    sql_counts: dict[str, int] = {"location": int(sql_summary.count())}
    for name, frame in sql_aggregates.items():
        if name == "location":
            continue
        path = output_root / "spark_sql" / name
        _write_parquet(frame, path)
        sql_paths[name] = str(path)
        sql_counts[name] = int(frame.count())

    features = build_feature_tables(tables, cleaned)
    feature_counts: dict[str, int] = {}
    for name, frame in features.items():
        feature_counts[name] = int(frame.count())
        _write_parquet(frame, output_root / name)

    eda_outputs = build_eda_outputs(tables, cleaned)
    eda_counts: dict[str, int] = {}
    for name, frame in eda_outputs.items():
        eda_counts[name] = int(frame.count())
        _write_parquet(frame, output_root / "eda" / name)

    readback_counts = {
        "cleaned_order_items": int(spark.read.parquet(str(cleaned_root / "order_items")).count()),
    }
    for name, path in sql_paths.items():
        readback_counts[f"spark_sql/{name}"] = int(spark.read.parquet(path).count())
        if readback_counts[f"spark_sql/{name}"] != sql_counts[name]:
            raise RuntimeError(f"Parquet read-back row mismatch for Spark SQL aggregate {name}.")
    for name in features:
        readback_counts[name] = int(spark.read.parquet(str(output_root / name)).count())
        if readback_counts[name] != feature_counts[name]:
            raise RuntimeError(f"Parquet read-back row mismatch for {name}.")
    for name in eda_outputs:
        readback_counts[f"eda/{name}"] = int(spark.read.parquet(str(output_root / "eda" / name)).count())
        if readback_counts[f"eda/{name}"] != eda_counts[name]:
            raise RuntimeError(f"Parquet read-back row mismatch for EDA output {name}.")
    if readback_counts["cleaned_order_items"] != counts["order_items"]:
        raise RuntimeError("Partitioned clean order_items Parquet read-back row mismatch.")

    summary = {
        "requirement_ids": ["FR-EX-15", "FR-EX-16", "FR-EX-17", "FR-EX-18", "FR-EX-19", "FR-EX-83", "FR-EX-84"],
        "cleaned_rows_by_table": counts,
        "required_join_rows": join_counts,
        "cleaning_decisions": cleaning_audit,
        "spark_sql_aggregates": sql_counts,
        "spark_sql_paths": sql_paths,
        "feature_rows": feature_counts,
        "eda_rows": eda_counts,
        "parquet_readback_rows": readback_counts,
        "feature_names": list(SRS_FEATURES),
        "partitioned_output": str(cleaned_root / "order_items"),
        "parquet_outputs": len(cleaned) + len(features) + len(eda_outputs) + len(sql_paths),
        "source_data_modified": False,
        "passed": all(count >= 0 for count in counts.values()) and len(join_counts) == 10
        and readback_counts["cleaned_order_items"] == counts["order_items"]
        and all(readback_counts[f"spark_sql/{name}"] == sql_counts[name] for name in sql_counts)
        and all(readback_counts[name] == feature_counts[name] for name in features)
        and all(readback_counts[f"eda/{name}"] == eda_counts[name] for name in eda_outputs)
        and set(features) == {"menu_features", "customer_features", "menu_location_features"},
    }
    summary["run_metadata"] = {
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_master": spark.sparkContext.master,
        "spark_version": spark.version,
        "source_data_modified": False,
    }
    _write_json(settings.reports_root / "data_foundation" / "cleaning_decisions.json", cleaning_audit)
    _write_json(settings.reports_root / "data_foundation" / "build_summary.json", summary)
    return summary
