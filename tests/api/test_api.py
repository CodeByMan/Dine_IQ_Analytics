from __future__ import annotations

import base64
import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import urlopen

from dineiq.api.server import ApiApplication, create_server
from dineiq.config import Settings
from dineiq.db.auth import create_user


class FakeDemandPredictor:
    def predict(self, request):
        request.validated()
        return {
            "request_id": "test-request",
            "date": request.forecast_date,
            "item_id": request.item_id,
            "location_id": request.location_id,
            "spark_prediction": 10.0,
            "python_prediction": 10.5,
            "spark_model_version": "spark-test",
            "python_model_version": "python-test",
            "latency_ms": 1.0,
        }


def _settings(root: Path) -> Settings:
    return Settings(root, root / "data", root / "artifacts", root / "reports")


class ApiContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.settings = _settings(root)
        self.app = ApiApplication(self.settings, demand_predictor=FakeDemandPredictor())
        create_user(str(self.settings.database_path), "admin", "test-password-123", "administrator")
        token = base64.b64encode(b"admin:test-password-123").decode("ascii")
        self.auth = {"Authorization": f"Basic {token}"}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_health_is_public_and_structured(self):
        response = self.app.dispatch("GET", "/api/v1/health")
        self.assertEqual(response.status, 200)
        self.assertTrue(response.payload["ok"])
        self.assertEqual(response.payload["service"], "dineiq-api")
        self.assertFalse(response.payload["dataset_available"])

    def test_authentication_and_real_sqlite_crud_workflow(self):
        unauthorized = self.app.dispatch("GET", "/api/v1/entities/locations")
        self.assertEqual(unauthorized.status, 401)

        saved = self.app.dispatch(
            "POST", "/api/v1/entities/locations", headers=self.auth,
            body=json.dumps({"values": {"Location_ID": "L1", "Restaurant_Name": "Central"}}),
        )
        self.assertEqual(saved.status, 201)
        rows = self.app.dispatch("GET", "/api/v1/entities/locations?limit=10", headers=self.auth)
        self.assertEqual(rows.status, 200)
        self.assertEqual(rows.payload["records"][0]["Location_ID"], "L1")

        invalid = self.app.dispatch(
            "POST", "/api/v1/entities/locations", headers=self.auth,
            body=json.dumps({"values": {"Location_ID": "L2"}}),
        )
        self.assertEqual(invalid.status, 422)

    def test_report_catalog_exposes_all_twelve_srs_categories(self):
        response = self.app.dispatch("GET", "/api/v1/reports", headers=self.auth)
        self.assertEqual(response.status, 200)
        reports = response.payload["reports"]
        self.assertEqual(len(reports), 12)
        self.assertEqual({item["export_key"] for item in reports}, {
            "menu_performance", "profitability", "customer_segmentation", "market_basket",
            "demand_forecast", "wastage", "promotions", "pricing", "location_performance",
            "anomalies", "recommendations", "spark_python_comparison",
        })

    def test_demand_prediction_validates_input_and_calls_persisted_service_boundary(self):
        missing = self.app.dispatch(
            "POST", "/api/v1/predictions/demand", headers=self.auth,
            body=json.dumps({"item_id": "I1"}),
        )
        self.assertEqual(missing.status, 422)

        valid = {
            "forecast_date": "2026-09-27", "item_id": "I1", "location_id": "L1",
            "is_available": 1, "lag_1": 8, "lag_7": 6, "trailing_7_mean": 7,
        }
        response = self.app.dispatch(
            "POST", "/api/v1/predictions/demand", headers=self.auth, body=json.dumps(valid)
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(response.payload["prediction"]["spark_model_version"], "spark-test")

    def test_http_server_starts_and_serves_health(self):
        server = create_server(application=self.app, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urlopen(f"http://127.0.0.1:{server.server_address[1]}/health", timeout=3) as result:
                payload = json.loads(result.read().decode("utf-8"))
            self.assertEqual(result.status, 200)
            self.assertEqual(payload["status"], "ok")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
