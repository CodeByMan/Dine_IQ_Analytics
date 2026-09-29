"""DineIQ setup, data validation, analytics, modeling and application commands."""

from __future__ import annotations

import argparse
import platform
import sys
import time

from dineiq.config import load_settings
from dineiq.error_reporting import operation_error_message
from dineiq.db.auth import create_first_admin_interactive
from dineiq.db.schema import initialize_database
from dineiq.dataset.catalog import TABLES, inspect_dataset
from dineiq.paths import ensure_runtime_dirs
from dineiq.db.operations import finish_job, start_job
from dineiq.api.server import run_server


def _tracked_job(settings, name, operation):
    """Persist Spark processing state so the UI can show success/failure."""
    job_id = None
    started = time.perf_counter()
    try:
        ensure_runtime_dirs(settings)
        initialize_database(settings.database_path)
        job_id = start_job(str(settings.database_path), name)
        result = operation()
        duration = round(time.perf_counter() - started, 3)
        finish_job(str(settings.database_path), job_id, success=True,
                   message="Completed successfully",
                   details={"duration_seconds": duration, "status": "succeeded"})
    except Exception as exc:
        if job_id is not None:
            try:
                finish_job(str(settings.database_path), job_id, success=False,
                           message=f"{type(exc).__name__}: {str(exc)[:500]}",
                           details={"duration_seconds": round(time.perf_counter() - started, 3),
                                    "status": "failed", "exception_type": type(exc).__name__})
            except Exception as audit_exc:
                operation_error_message(
                    f"Recording failed processing-job status for {name}",
                    audit_exc,
                    settings.logs_dir / "application.log",
                )
        message = operation_error_message(
            f"Processing job {name}", exc, settings.logs_dir / "application.log"
        )
        prefix = f"PROCESSING JOB {job_id}: " if job_id is not None else "PROCESSING JOB: "
        print(f"{prefix}FAILED — {message}")
        raise
    print(f"PROCESSING JOB {job_id}: SUCCEEDED ({round(time.perf_counter() - started, 3)}s)")
    return result


def _doctor() -> int:
    ok = sys.version_info[:2] == (3, 13)
    print(f"Python: {platform.python_version()} — {'PASS' if ok else 'FAIL (requires Python 3.13)'}")
    settings = load_settings()
    print(f"Project root: {settings.project_root}")
    print(f"Dataset root: {settings.data_root} — {'FOUND' if settings.data_root.is_dir() else 'NOT FOUND'}")
    print(f"DINEIQ ENVIRONMENT CHECK: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 2


def _dataset_check() -> int:
    settings = load_settings()
    checks = inspect_dataset(settings.raw_data_dir)
    for check in checks:
        spec = TABLES[check.table]
        detail = f"{check.rows:,} rows; minimum {spec.minimum_rows:,}"
        if check.error:
            detail += f"; {check.error}"
        status = "PASS" if check.passed else "FAIL"
        print(f"[{status}] {check.table}: {detail}")
    passed = sum(check.passed for check in checks)
    print(f"DATASET CSV CHECK: {passed}/{len(checks)} {'PASS' if passed == len(checks) else 'FAIL'}")
    print("NOTE: CSV presence/count checks do not verify Parquet schema or value parity.")
    return 0 if passed == len(checks) else 2


def _init() -> int:
    settings = load_settings()
    created = ensure_runtime_dirs(settings)
    for path in created:
        print(f"[READY] {path}")
    print("PROJECT INITIALIZATION: PASS")
    return 0


def _data_foundation_validate() -> int:
    from dineiq.dataset.pipeline import create_local_spark, validate_data_foundation

    settings = load_settings()
    spark = create_local_spark(settings, "DineIQ-DataFoundation-Validation")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = _tracked_job(settings, "data_foundation-validation", lambda: validate_data_foundation(spark, settings))
    finally:
        spark.stop()
    schemas = result["schema"]["checks"]
    relationships = result["relationships"]["checks"]
    fixtures = result["quality"]["isolated_fixture_coverage"]
    print(f"SCHEMA CHECKS: {sum(c['passed'] for c in schemas)}/{len(schemas)} PASS")
    print(f"FOREIGN-KEY CHECKS: {sum(c['passed'] for c in relationships)}/{len(relationships)} PASS")
    print(f"QUALITY FIXTURES: {fixtures['fixture_types_detected']}/{fixtures['expected_issue_types']} PASS")
    print(f"QUALITY REPORT: {settings.reports_root / 'data_foundation' / 'data_quality_report.json'}")
    print(f"DATA FOUNDATION VALIDATION: {'PASS' if result['passed'] else 'FAIL'}")
    return 0 if result["passed"] else 2


def _data_foundation_build() -> int:
    from dineiq.analytics.eda import EDA_OUTPUTS
    from dineiq.dataset.pipeline import build_data_foundation, create_local_spark

    settings = load_settings()
    spark = create_local_spark(settings, "DineIQ-DataFoundation-Build")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = _tracked_job(settings, "build-data-foundation", lambda: build_data_foundation(spark, settings))
    finally:
        spark.stop()
    print(f"CLEANED TABLES: {len(result['cleaned_rows_by_table'])}/12")
    print(f"REQUIRED SPARK JOINS: {len(result['required_join_rows'])}/10")
    print(f"FEATURE TABLES: {len(result['feature_rows'])}/3")
    print(f"SRS FEATURES: {len(result['feature_names'])}/22")
    print(f"EDA OUTPUTS: {len(result['eda_rows'])}/{len(EDA_OUTPUTS)}")
    expected_readbacks = len(result['feature_rows']) + len(result['eda_rows']) + 1 + len(result.get('spark_sql_aggregates', {}))
    print(f"PARQUET READ-BACK: {len(result['parquet_readback_rows'])}/{expected_readbacks} PASS")
    print(f"DATA FOUNDATION BUILD: {'PASS' if result['passed'] else 'FAIL'}")
    return 0 if result["passed"] else 2


def _descriptive_analytics_build() -> int:
    from dineiq.dataset.descriptive_analytics_pipeline import build_descriptive_analytics
    from dineiq.dataset.pipeline import create_local_spark

    settings = load_settings()
    spark = create_local_spark(settings, "DineIQ-DescriptiveAnalytics-Analytics")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = _tracked_job(settings, "build-descriptive-analytics", lambda: build_descriptive_analytics(spark, settings))
    finally:
        spark.stop()
    output_total = len(result["output_rows"])
    readback_total = len(result["parquet_readback_rows"])
    print(f"ANALYTICS OUTPUTS: {output_total}/{output_total}")
    print(f"MENU ITEMS CLASSIFIED: {result['menu_classified_rows']}/{result['menu_source_rows']}")
    print(f"CUSTOMERS SEGMENTED: {result['customer_segmented_rows']}/{result['customer_source_rows']}")
    print(f"MENU CLASS LABELS SUPPORTED: {len(result['menu_classes'])}/4")
    print(f"CUSTOMER SEGMENT LABELS SUPPORTED: {len(result['customer_segments'])}/6")
    print(f"TRICKY CASE FLAGS: {len(result['tricky_case_flags'])}/10")
    print(f"PARQUET READ-BACK: {readback_total}/{output_total} PASS")
    print(f"DESCRIPTIVE ANALYTICS BUILD: {'PASS' if result['passed'] else 'FAIL'}")
    return 0 if result["passed"] else 2


def _demand_planning_build(horizon_days: int) -> int:
    from dineiq.dataset.demand_planning_pipeline import build_demand_planning
    from dineiq.dataset.pipeline import create_local_spark

    settings = load_settings()
    spark = create_local_spark(settings, "DineIQ-DemandPlanning-Analytics")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = _tracked_job(settings, "build-demand-planning", lambda: build_demand_planning(spark, settings, horizon_days=horizon_days))
    finally:
        spark.stop()
    print(f"ANALYTICS OUTPUTS: {len(result['output_rows'])}/9")
    print(f"PARQUET READ-BACK: {len(result['parquet_readback_rows'])}/9 PASS")
    print(f"FORECAST HORIZON: {result['forecast_horizon_days']} days")
    print(f"CHRONOLOGICAL SPLITS: {'PASS' if result['chronological_splits'] else 'FAIL'}")
    print(f"FORECAST METRIC FAMILIES: {len(result['forecast_metrics'])}/4")
    print(f"PRICE-SENSITIVITY CLASSES: {len(result['price_sensitivity_classes'])} observed")
    wastage_model = result.get("wastage_model", {})
    print(f"WASTAGE MODEL: {wastage_model.get('algorithm', 'Unavailable')} — {'PASS' if wastage_model.get('passed') else 'FAIL'}")
    print(f"WASTAGE MODEL INFERENCE ROWS: {wastage_model.get('inference_records', 0)}")
    print(f"DEMAND PLANNING BUILD: {'PASS' if result['passed'] else 'FAIL'}")
    return 0 if result["passed"] else 2


def _operational_intelligence_build() -> int:
    from dineiq.dataset.operational_intelligence_pipeline import build_operational_intelligence
    from dineiq.dataset.pipeline import create_local_spark

    settings = load_settings()
    spark = create_local_spark(settings, "DineIQ-OperationalIntelligence-Analytics")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = _tracked_job(settings, "build-operational-intelligence", lambda: build_operational_intelligence(spark, settings))
    finally:
        spark.stop()
    print(f"ANALYTICS OUTPUTS: {len(result['output_rows'])}/10")
    print(f"PARQUET READ-BACK: {len(result['parquet_readback_rows'])}/10 PASS")
    print(f"RATING ANOMALY TYPES: {len(result['rating_anomaly_types_detected'])}/5")
    print(f"SALES ANOMALY TYPES: {len(result['sales_anomaly_types_detected'])}/6")
    print(f"LOCATION ROWS: {result['location_comparison_rows']}/{result['location_source_rows']}")
    print(f"CUSTOMER CHURN ROWS: {result['churn_rows']}/{result['customer_rows']}")
    print(f"RECOMMENDATION TYPES GENERATED: {len(result['recommendation_types_generated'])}/9")
    print(f"OPERATIONAL INTELLIGENCE BUILD: {'PASS' if result['passed'] else 'FAIL'}")
    return 0 if result["passed"] else 2


def _demand_models_build() -> int:
    from dineiq.dataset.demand_models_pipeline import build_demand_models
    from dineiq.dataset.pipeline import create_local_spark

    settings = load_settings()
    spark = create_local_spark(settings, "DineIQ-DemandModels-Dual-Models")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = _tracked_job(settings, "build-demand-models", lambda: build_demand_models(spark, settings))
    finally:
        spark.stop()
    print(f"SPARK ALGORITHMS TRAINED: {len(result['spark_algorithms_trained'])}/3")
    print(f"SPARK SELECTED MODEL: {result['spark_selected_model']}")
    print(f"SPARK TEST RMSE: {result['spark_test_metrics']['rmse']:.4f}")
    print(f"PYTHON MODEL: {result['python_selected_model']} (epoch {result['python_best_epoch']})")
    print(f"PYTHON TEST RMSE: {result['python_test_metrics']['rmse']:.4f}")
    print(f"UNSEEN COMPARISON RECORDS: {result['comparison_records']} (minimum 100)")
    print(f"DUAL-PIPELINE AGREEMENT: {result['agreement_percentage']:.2f}%")
    print(f"SPARK RMSE IMPROVEMENT VS SEASONAL-NAIVE: {result['spark_rmse_improvement_over_seasonal_naive_pct']:.2f}%")
    print(f"PYTHON RMSE IMPROVEMENT VS SEASONAL-NAIVE: {result['python_rmse_improvement_over_seasonal_naive_pct']:.2f}%")
    print(f"NFR-EX-04 BASELINE IMPROVEMENT: {'PASS' if result['nfr_ex_04_forecast_baseline_improvement_verified'] else 'FAIL'}")
    print(f"COMPARISON PARQUET READ-BACK: {result['comparison_parquet_readback_records']}/{result['comparison_records']} PASS")
    print(f"DEMAND MODELS BUILD: {'PASS' if result['passed'] else 'FAIL'}")
    return 0 if result["passed"] else 2


def _serve_api(host: str, port: int) -> int:
    """Run the local integration API over the existing domain services."""
    settings = load_settings()
    ensure_runtime_dirs(settings)
    run_server(settings, host=host, port=port)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="dineiq", description=__doc__)
    parser.add_argument(
        "command",
        choices=("doctor", "dataset-check", "init", "create-admin", "validate-data", "build-data-foundation", "build-descriptive-analytics", "build-demand-planning", "build-operational-intelligence", "build-demand-models", "serve-api"),
    )
    parser.add_argument("--horizon-days", type=int, default=30,
                        help="Configurable future daily demand forecast horizon (1-366; default: 30)")
    parser.add_argument("--host", default="127.0.0.1", help="API bind host (serve-api only).")
    parser.add_argument("--port", type=int, default=8502, help="API bind port (serve-api only).")
    args = parser.parse_args()
    commands = {
        "doctor": _doctor,
        "dataset-check": _dataset_check,
        "init": _init,
        "validate-data": _data_foundation_validate,
        "build-data-foundation": _data_foundation_build,
        "build-descriptive-analytics": _descriptive_analytics_build,
    }
    if args.command == "create-admin":
        settings = load_settings()
        ensure_runtime_dirs(settings)
        initialize_database(settings.database_path)
        try:
            user_id = create_first_admin_interactive(str(settings.database_path))
        except (RuntimeError, ValueError) as exc:
            print(f"ADMIN ACCOUNT: FAIL — {exc}")
            return 2
        print(f"FIRST ADMINISTRATOR CREATED: user_id={user_id}")
        return 0
    if args.command == "build-demand-planning":
        return _demand_planning_build(args.horizon_days)
    if args.command == "build-operational-intelligence":
        return _operational_intelligence_build()
    if args.command == "build-demand-models":
        return _demand_models_build()
    if args.command == "serve-api":
        return _serve_api(args.host, args.port)
    return commands[args.command]()


if __name__ == "__main__":
    raise SystemExit(main())
