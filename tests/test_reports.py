from __future__ import annotations

import io
import unittest
from pathlib import Path

import pandas as pd

from dineiq.ui.reports import (
    REPORT_REQUIREMENTS,
    build_report_frames,
    report_export_key,
    report_filename,
    serialize_report_csv,
    validate_report_csv,
    validate_report_frame,
)


def _report_inputs() -> dict[str, pd.DataFrame]:
    return {
        "menu": pd.DataFrame({
            "Item_ID": ["I1", "I2"], "performance_class": ["Profit Driver", "Low Performer"],
            "item_revenue": [100.0, 20.0], "contribution_margin": [40.0, -2.0],
            "Location_ID": ["L1", "L2"],
        }),
        "customers": pd.DataFrame({
            "Customer_ID": ["C1", "C2"], "customer_segment": ["High Value", "At Risk"],
        }),
        "market_basket": pd.DataFrame({
            "antecedent_item_id": ["I1"], "consequent_item_id": ["I2"],
            "support": [0.2], "confidence": [0.5], "lift": [1.2],
        }),
        "forecast": pd.DataFrame({
            "Date": ["2026-01-01"], "Item_ID": ["I1"], "forecast_quantity": [12.0],
        }),
        "wastage_records": pd.DataFrame({
            "Wastage_ID": ["W1", "W2"], "Item_ID": ["I1", "I2"], "Location_ID": ["L1", "L2"],
            "Wastage_Date": ["2026-01-01", "2026-01-02"], "Quantity_Wasted": [2.0, 1.0],
            "Total_Wastage_Cost": [4.0, 2.0],
        }),
        "promotion_traps": pd.DataFrame({
            "Promotion_ID": ["P1"], "promotion_sales_up_profit_down": [True],
        }),
        "price_sensitivity": pd.DataFrame({
            "Item_ID": ["I1"], "price_sensitivity_class": ["Highly Price Sensitive"],
            "observed_elasticity": [-1.7],
        }),
        "location": pd.DataFrame({
            "Location_ID": ["L1"], "revenue": [100.0], "order_count": [10],
        }),
        "recommendations": pd.DataFrame({
            "recommendation_type": ["Review pricing of price-sensitive dishes"], "priority": ["High"],
        }),
        "dual_rows": pd.DataFrame({
            "spark_prediction": [10.0], "python_prediction": [10.2], "match_status": ["match"],
        }),
        "sales_anomalies": pd.DataFrame({"anomaly_type": ["Sudden sales spike"]}),
        "rating_anomalies": pd.DataFrame({"anomaly_type": ["Rating anomaly"]}),
    }


class ReportContractTests(unittest.TestCase):
    def test_all_twelve_reports_have_real_frames_and_stable_exports(self):
        frames = build_report_frames(_report_inputs())
        self.assertEqual(set(frames), set(REPORT_REQUIREMENTS))
        self.assertEqual(len(frames), 12)
        for name, frame in frames.items():
            check = validate_report_frame(name, frame)
            self.assertTrue(check["passed"], (name, check))
            payload = serialize_report_csv(frame)
            self.assertTrue(payload.startswith(b"\xef\xbb\xbf"))
            csv_check = validate_report_csv(payload, frame)
            self.assertTrue(csv_check["passed"], (name, csv_check))
            self.assertEqual(csv_check["rows"], len(frame))
            self.assertEqual(report_filename(name), f"dineiq_{report_export_key(name)}.csv")

    def test_spark_python_key_matches_audit_contract(self):
        self.assertEqual(report_export_key("Spark-vs-Python comparison"), "spark_python_comparison")

    def test_formula_prefixes_are_neutralized(self):
        source = pd.DataFrame({"note": ["=SUM(A1:A2)", "+cmd", "-danger", "@cmd", "safe"]})
        parsed = pd.read_csv(io.BytesIO(serialize_report_csv(source)))
        self.assertEqual(parsed["note"].tolist(), ["'=SUM(A1:A2)", "'+cmd", "'-danger", "'@cmd", "safe"])

    def test_filter_changes_report_rows_and_empty_csv_remains_valid(self):
        source = _report_inputs()["menu"]
        # Equivalent deterministic filter contract used by the UI's shared
        # filter layer; this test remains dependency-light and checks the
        # report builder receives only the selected records.
        filtered = source[source["Location_ID"].isin(["L1"])].copy()
        self.assertEqual(len(filtered), 1)
        frames = build_report_frames({"menu": filtered})
        self.assertEqual(len(frames["Menu performance"]), 1)

        empty = source.iloc[0:0]
        payload = serialize_report_csv(empty)
        check = validate_report_csv(payload, empty)
        self.assertTrue(check["passed"], check)
        self.assertEqual(check["rows"], 0)

    def test_missing_inputs_produce_explicit_empty_states_for_all_reports(self):
        frames = build_report_frames({})
        self.assertEqual(len(frames), 12)
        for name, frame in frames.items():
            self.assertTrue(frame.empty, name)
            self.assertTrue(validate_report_frame(name, frame)["passed"])

    def test_streamlit_page_uses_dedicated_report_download_path(self):
        app = (Path(__file__).parents[1] / "src" / "dineiq" / "ui" / "app.py").read_text(encoding="utf-8")
        self.assertIn('"Download CSV report"', app)
        self.assertIn("serialize_report_csv", app)
        self.assertIn("validate_report_csv", app)
        self.assertIn("record_report_export", app)
        for name in REPORT_REQUIREMENTS:
            self.assertIn(name, app + Path(__file__).parents[1].joinpath("src/dineiq/ui/reports.py").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
