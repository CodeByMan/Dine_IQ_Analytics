"""operations persistence and report-contract tests."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from dineiq.db.operations import (
    audit_event, finish_job, recent_audit_events, recent_jobs, record_model_run,
    record_prediction_results, record_export, start_job,
)
from dineiq.db.permissions import Principal
from dineiq.db.auth import create_user
from dineiq.db.schema import initialize_database
from dineiq.ui.reports import REPORT_REQUIREMENTS, build_report_frames, available_date_bounds
from dineiq.analytics.model_serving import DemandRequest, python_feature_record


class OperationsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = str(Path(self.temp.name) / "dineiq.sqlite3")
        initialize_database(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def test_processing_jobs_keep_running_and_terminal_states(self):
        job_id = start_job(self.database, "test-job", {"input": "synthetic"})
        self.assertEqual(recent_jobs(self.database)[0]["status"], "running")
        finish_job(self.database, job_id, success=True, message="ok")
        self.assertEqual(recent_jobs(self.database)[0]["status"], "succeeded")

    def test_model_versions_and_evaluation_results_persist(self):
        run_id = record_model_run(self.database, pipeline="Python", model_name="SGDRegressor",
                                  model_version="python-v1", artifact_path="models/python.joblib",
                                  metrics={"rmse": 1.25}, record_count=100)
        from dineiq.db.schema import connect_database
        with connect_database(self.database) as db:
            model = db.execute("SELECT model_version,record_count FROM model_runs WHERE run_id=?", (run_id,)).fetchone()
            result = db.execute("SELECT model_version,value_json FROM stored_results WHERE run_id=?", (run_id,)).fetchone()
        self.assertEqual(model["model_version"], "python-v1")
        self.assertEqual(model["record_count"], 100)
        self.assertEqual(result["model_version"], "python-v1")
        self.assertEqual(json.loads(result["value_json"])["rmse"], 1.25)

    def test_audit_log_stores_actor_and_action_without_secrets(self):
        user_id = create_user(self.database, "admin", "a-secure-test-password", "administrator")
        principal = Principal(user_id, "admin", "administrator")
        audit_event(self.database, principal, "export", "report", "Wastage", details={"rows": 3})
        event = recent_audit_events(self.database)[0]
        self.assertEqual((event["actor_name"], event["action"], event["entity_id"]),
                         ("admin", "export", "Wastage"))
        self.assertNotIn("password", event["details"].lower())

    def test_all_twelve_srs_report_exports_are_audited_with_actor_and_safe_metadata(self):
        from dineiq.db.operations import SRS_REPORT_EXPORTS
        user_id = create_user(self.database, "reporter", "a-secure-test-password", "analyst")
        principal = Principal(user_id, "reporter", "analyst")
        for report in sorted(SRS_REPORT_EXPORTS):
            record_export(self.database, principal, report, 12)
        events = recent_audit_events(self.database, limit=50)
        export_events = [event for event in events if event["action"] == "export"]
        self.assertEqual(len(SRS_REPORT_EXPORTS), 12)
        self.assertEqual({event["entity_id"] for event in export_events}, set(SRS_REPORT_EXPORTS))
        for event in export_events:
            self.assertEqual((event["actor_name"], event["entity_type"], event["outcome"]),
                             ("reporter", "report", "success"))
            self.assertEqual(json.loads(event["details"]), {"format": "csv", "row_count": 12})
        with self.assertRaises(ValueError):
            record_export(self.database, principal, "unknown_report", 12)
        with self.assertRaises(ValueError):
            record_export(self.database, principal, "demand_forecast", 0)

    def test_forecast_filter_bounds_include_future_forecast_rows(self):
        orders = pd.DataFrame({"Order_Date": ["2026-09-24"]})
        forecast = pd.DataFrame({"Date": ["2026-09-25", "2026-10-24"],
                                 "forecast_quantity": [12.0, 15.0]})
        bounds = available_date_bounds(orders, forecast)
        self.assertEqual(str(bounds[0]), "2026-09-24")
        self.assertEqual(str(bounds[1]), "2026-10-24")
        self.assertTrue(all(bounds[0] <= pd.Timestamp(value).date() <= bounds[1]
                            for value in forecast["Date"]))

    def test_twelve_named_srs_report_frames_are_available(self):
        frame = pd.DataFrame({"metric": [1]})
        outputs = build_report_frames({key: frame for key in (
            "menu", "customers", "market_basket", "forecast", "wastage", "promotion_traps",
            "price_sensitivity", "location", "recommendations", "dual_rows", "sales_anomalies",
            "rating_anomalies")})
        self.assertEqual(set(outputs), set(REPORT_REQUIREMENTS))
        self.assertEqual(len(outputs), 12)
        self.assertEqual(len(outputs["Anomalies"]), 2)

    def test_live_predictions_persist_with_their_model_versions(self):
        record_model_run(self.database, pipeline="Spark", model_name="LinearRegression",
                         model_version="spark-v1", artifact_path="spark-model",
                         metrics={"rmse": 1.1}, record_count=200)
        record_model_run(self.database, pipeline="Python", model_name="SGDRegressor",
                         model_version="python-v1", artifact_path="python-model",
                         metrics={"rmse": 1.2}, record_count=200)
        record_prediction_results(self.database, {
            "request_id": "request-1", "date": "2026-09-27", "item_id": "I1",
            "location_id": "L1", "spark_prediction": 9.5, "python_prediction": 10.2,
            "spark_model_version": "spark-v1", "python_model_version": "python-v1",
            "latency_ms": 90.0,
        })
        from dineiq.db.schema import connect_database
        with connect_database(self.database) as db:
            rows = db.execute("SELECT result_type,model_version,value_json FROM stored_results WHERE result_type='live_prediction' ORDER BY result_id").fetchall()
        self.assertEqual([row["model_version"] for row in rows], ["spark-v1", "python-v1"])
        self.assertTrue(all(row["result_type"] == "live_prediction" for row in rows))

    def test_live_inference_features_match_python_training_contract(self):
        request = DemandRequest("2026-09-27", "I1", "L1", 1, 8, 6, 7)
        record = python_feature_record(request)
        self.assertEqual(record["item=I1"], 1.0)
        self.assertEqual(record["location=L1"], 1.0)
        self.assertEqual(record["lag_1"], 8.0)
        self.assertEqual(record["lag_7"], 6.0)
        self.assertEqual(record["trailing_7_mean"], 7.0)
        with self.assertRaises(ValueError):
            python_feature_record(DemandRequest("not-a-date", "I1", "L1", 1, 8, 6, 7))


if __name__ == "__main__":
    unittest.main()
