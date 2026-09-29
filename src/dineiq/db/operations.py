"""Durable audit, job, model-version and result records."""
from __future__ import annotations

import json
import uuid
from typing import Any

from dineiq.db.permissions import Principal
from dineiq.db.schema import connect_database


def audit_event(database_path: str, actor: Principal | None, action: str,
                entity_type: str, entity_id: str | None = None,
                *, outcome: str = "success", details: dict[str, Any] | None = None) -> None:
    if outcome not in {"success", "failure"}:
        raise ValueError("Invalid audit outcome.")
    with connect_database(database_path) as db:
        db.execute(
            "INSERT INTO audit_events(actor_user_id,actor_name,action,entity_type,entity_id,outcome,details) VALUES(?,?,?,?,?,?,?)",
            (actor.user_id if actor else None, actor.username if actor else "system", action,
             entity_type, entity_id, outcome, json.dumps(details or {}, sort_keys=True)),
        )


# The 12 report types required by SRS FR-EX-86, plus dashboard-view exports.
SRS_REPORT_EXPORTS = frozenset({
    "menu_performance", "profitability", "customer_segmentation", "market_basket",
    "demand_forecast", "wastage", "promotions", "pricing", "location_performance",
    "anomalies", "recommendations", "spark_python_comparison",
})
EXPORT_REPORTS = SRS_REPORT_EXPORTS | frozenset({
    "overview_summary", "menu_intelligence", "customer_intelligence",
    "location_intelligence", "model_comparison", "data_management",
})

def record_export(database_path: str, actor: Principal, report: str, row_count: int) -> None:
    """Persist an authenticated successful CSV export without storing report data."""
    if not isinstance(report, str) or report not in EXPORT_REPORTS:
        raise ValueError("Unknown report export type.")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or not 1 <= row_count <= 1_000_000:
        raise ValueError("Export row count must be between 1 and 1,000,000.")
    audit_event(database_path, actor, "export", "report", report,
                details={"format": "csv", "row_count": row_count})


def start_job(database_path: str, job_name: str, details: dict[str, Any] | None = None) -> str:
    job_id = str(uuid.uuid4())
    with connect_database(database_path) as db:
        db.execute("INSERT INTO processing_jobs(job_id,job_name,status,details) VALUES(?,?,'running',?)",
                   (job_id, job_name, json.dumps(details or {}, sort_keys=True)))
    return job_id


def finish_job(database_path: str, job_id: str, *, success: bool, message: str,
               details: dict[str, Any] | None = None) -> None:
    with connect_database(database_path) as db:
        cursor = db.execute(
            "UPDATE processing_jobs SET finished_at=CURRENT_TIMESTAMP,status=?,message=?,details=COALESCE(?,details) WHERE job_id=?",
            ("succeeded" if success else "failed", message,
             json.dumps(details, sort_keys=True) if details is not None else None, job_id),
        )
        if cursor.rowcount != 1:
            raise LookupError("Processing job was not found.")


def record_model_run(database_path: str, *, pipeline: str, model_name: str,
                     model_version: str, artifact_path: str,
                     metrics: dict[str, Any], record_count: int) -> str:
    run_id = str(uuid.uuid4())
    with connect_database(database_path) as db:
        db.execute("INSERT INTO model_runs(run_id,pipeline,model_name,model_version,artifact_path,metrics_json,record_count) VALUES(?,?,?,?,?,?,?)",
                   (run_id, pipeline, model_name, model_version, artifact_path,
                    json.dumps(metrics, sort_keys=True, allow_nan=False), int(record_count)))
        db.execute("INSERT INTO stored_results(run_id,result_type,model_version,value_json) VALUES(?,?,?,?)",
                   (run_id, "evaluation", model_version,
                    json.dumps(metrics, sort_keys=True, allow_nan=False)))
        db.execute(
            "INSERT INTO audit_events(actor_user_id,actor_name,action,entity_type,entity_id,outcome,details) VALUES(NULL,'system','live_prediction_request','model_run',?,'success',?)",
            (run_id, json.dumps({"model_version": model_version, "record_count": int(record_count)})),
        )
    return run_id


def record_prediction_results(database_path: str, result: dict[str, Any]) -> None:
    """Persist both live predictions linked to their exact trained model versions."""
    with connect_database(database_path) as db:
        for pipeline, field, version_field in (
            ("Spark", "spark_prediction", "spark_model_version"),
            ("Python", "python_prediction", "python_model_version"),
        ):
            version = str(result[version_field])
            run = db.execute("SELECT run_id FROM model_runs WHERE model_version=? ORDER BY created_at DESC LIMIT 1",
                             (version,)).fetchone()
            if not run:
                raise LookupError(f"No persisted model run is registered for version {version}.")
            value = {k: result[k] for k in ("date", "item_id", "location_id", field)}
            db.execute("INSERT INTO stored_results(run_id,result_type,entity_id,model_version,value_json) VALUES(?,?,?,?,?)",
                       (run["run_id"], "live_prediction", str(result["request_id"]), version,
                        json.dumps(value, sort_keys=True, allow_nan=False)))
        db.execute(
            "INSERT INTO audit_events(actor_user_id,actor_name,action,entity_type,entity_id,outcome,details) VALUES(NULL,'system','prediction','dual_model',?,'success',?)",
            (str(result["request_id"]), json.dumps({"spark_model_version": result["spark_model_version"],
             "python_model_version": result["python_model_version"], "latency_ms": result["latency_ms"]})),
        )


def recent_jobs(database_path: str, limit: int = 100) -> list[dict[str, Any]]:
    with connect_database(database_path) as db:
        rows = db.execute("SELECT job_id,started_at,finished_at,job_name,status,message FROM processing_jobs ORDER BY started_at DESC LIMIT ?",
                          (max(1, min(int(limit), 500)),)).fetchall()
    return [dict(row) for row in rows]


def recent_audit_events(database_path: str, limit: int = 200) -> list[dict[str, Any]]:
    with connect_database(database_path) as db:
        rows = db.execute("SELECT occurred_at,actor_name,action,entity_type,entity_id,outcome,details FROM audit_events ORDER BY event_id DESC LIMIT ?",
                          (max(1, min(int(limit), 1000)),)).fetchall()
    return [dict(row) for row in rows]
