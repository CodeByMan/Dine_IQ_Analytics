import tempfile
import unittest
from pathlib import Path
import pandas as pd
import numpy as np
from dineiq.analytics.python_demand_model import _CausalHistory, _metrics, train_python_demand_model, transform_feature_record


def series(dates, values):
    return pd.DataFrame({"Date": dates, "Item_ID": "M1", "Location_ID": "R1",
                         "Is_Available": 1, "Target_Quantity": values})


class PythonDemandContractTests(unittest.TestCase):
    def test_log_feature_transform_is_stateless_and_preserves_input(self):
        original = {"lag_1": 100, "lag_7": 10, "trailing_7_mean": 0, "available": 1}
        result = transform_feature_record(original)
        self.assertEqual(original["lag_1"], 100)
        self.assertAlmostEqual(result["lag_1"], np.log1p(100))
        self.assertEqual(result["trailing_7_mean"], 0)
    def test_lag_excludes_the_current_target(self):
        history = _CausalHistory()
        records, targets, indices = history.encode_and_update(series(pd.date_range("2025-01-01", periods=8), range(8)))
        self.assertEqual(indices, [7])
        self.assertEqual(records[0]["lag_1"], 6)
        self.assertEqual(records[0]["lag_7"], 0)
        self.assertEqual(targets[0], 7)

    def test_duplicate_entity_date_cannot_leak_a_same_day_target(self):
        with self.assertRaisesRegex(ValueError, "one observation"):
            _CausalHistory().encode_and_update(series(["2025-01-01", "2025-01-01"], [1, 10]))

    def test_reverse_chronology_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "chronologically"):
            _CausalHistory().encode_and_update(series(["2025-01-02", "2025-01-01"], [1, 2]))

    def test_zero_actual_mape_is_explicitly_unavailable(self):
        result = _metrics(np.array([0, 0]), np.array([0, 0]), "test", "test")
        self.assertEqual(result["rmse"], 0)
        self.assertIsNone(result["mape_nonzero_actuals"])

    def test_nonfinite_targets_and_missing_entities_are_rejected(self):
        for value in (float("inf"), -1, float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                _CausalHistory().encode_and_update(series(["2025-01-01"], [value]))
        frame = series(["2025-01-01"], [1])
        frame["Item_ID"] = None
        with self.assertRaises(ValueError):
            _CausalHistory().encode_and_update(frame)

    def test_real_training_saves_predictions_and_baseline_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            dates = pd.date_range("2025-01-01", periods=35)
            for split, lo, hi in (("train", 0, 21), ("validation", 21, 28), ("test", 28, 35)):
                series(dates[lo:hi], [float(3 + i % 7) for i in range(lo, hi)]).to_csv(root / (split + ".csv"), index=False)
            result = train_python_demand_model(root / "train.csv", root / "validation.csv", root / "test.csv",
                                              root / "model.joblib", root / "predictions.csv")
            self.assertTrue((root / "model.joblib").is_file())
            self.assertEqual(result["test_metrics"]["records"], 7)
            self.assertIn("baseline_test_metrics", result)
            self.assertIn("hyperparameters", result)
