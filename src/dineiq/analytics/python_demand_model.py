from __future__ import annotations

import copy
from contextlib import nullcontext
import math
import platform
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.feature_extraction import FeatureHasher
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from scipy.sparse import csr_matrix

INPUT_COLUMNS = ("Date", "Item_ID", "Location_ID", "Is_Available", "Target_Quantity")
CHUNK_ROWS = 20_000
EPOCHS = 3
HASH_FEATURES = 2048
FEATURE_VERSION = "log1p-lags-v2"
MODEL_VERSION = "sklearn-sgdregressor-demand-v2"


def transform_feature_record(record: dict[str, float]) -> dict[str, float]:
    """Stabilize count-valued lags on the same log1p scale as the target.

    This stateless transform is shared with inference; no test-set statistic is
    fitted and raw causal-history records remain available for audit.
    """
    transformed = dict(record)
    for name in ("lag_1", "lag_7", "trailing_7_mean"):
        transformed[name] = math.log1p(transformed[name])
    return transformed


class _CausalHistory:
    """Keep only seven previous observed targets per item/location series."""

    def __init__(self) -> None:
        self.values = defaultdict(lambda: deque(maxlen=7))
        self.last_date = None
        self.last_series_dates = {}
        self.last_baselines = []

    def encode_and_update(self, frame: pd.DataFrame) -> tuple[list[dict[str, float]], np.ndarray, list[int]]:
        dates = pd.to_datetime(frame["Date"], errors="coerce")
        if dates.isna().any():
            raise ValueError("Python model input contains invalid dates")
        available = pd.to_numeric(frame["Is_Available"], errors="coerce").to_numpy(dtype=np.float64)
        target = pd.to_numeric(frame["Target_Quantity"], errors="coerce").to_numpy(dtype=np.float64)
        if not np.isfinite(available).all() or not np.isfinite(target).all() or (target < 0).any() or not np.isin(available, [0, 1]).all():
            raise ValueError("Python model input has missing features or negative demand targets")
        if frame[["Item_ID", "Location_ID"]].isna().any().any() or any(
            frame[c].astype(str).str.strip().eq("").any() for c in ("Item_ID", "Location_ID")
        ):
            raise ValueError("Python model input requires nonblank item and branch identifiers")

        records: list[dict[str, float]] = []
        self.last_baselines = []
        eligible_targets: list[float] = []
        eligible_indices: list[int] = []
        items = frame["Item_ID"].astype(str).to_numpy()
        locations = frame["Location_ID"].astype(str).to_numpy()
        for index, timestamp in enumerate(dates):
            current_date = timestamp.date()
            if self.last_date is not None and current_date < self.last_date:
                raise ValueError("Python model split rows must be ordered chronologically")
            self.last_date = current_date
            key = (items[index], locations[index])
            if key in self.last_series_dates and current_date <= self.last_series_dates[key]:
                raise ValueError("Python model requires one observation per item/location/date")
            self.last_series_dates[key] = current_date
            previous = self.values[key]
            value = float(target[index])
            if len(previous) == 7:
                doy = int(timestamp.dayofyear)
                records.append({
                    f"item={items[index]}": 1.0,
                    f"location={locations[index]}": 1.0,
                    f"weekday={timestamp.weekday()}": 1.0,
                    f"month={timestamp.month}": 1.0,
                    "available": float(available[index]),
                    "year_offset": float((timestamp.year - 2000.0) / 10.0),
                    "annual_sin": float(math.sin(2.0 * math.pi * doy / 365.25)),
                    "annual_cos": float(math.cos(2.0 * math.pi * doy / 365.25)),
                    "lag_1": float(previous[-1]),
                    "lag_7": float(previous[0]),
                    "trailing_7_mean": float(sum(previous) / 7.0),
                })
                eligible_targets.append(value)
                eligible_indices.append(index)
                self.last_baselines.append(float(previous[0]))
            previous.append(value)
        return records, np.asarray(eligible_targets, dtype=np.float64), eligible_indices


def _read_chunks(source: Path):
    with pd.read_csv(
        source, usecols=list(INPUT_COLUMNS),
        dtype={"Item_ID": "string", "Location_ID": "string"}, chunksize=CHUNK_ROWS,
    ) as reader:
        yield from reader


def _encode(frame: pd.DataFrame, hasher: FeatureHasher, history: _CausalHistory):
    records, target, indices = history.encode_and_update(frame)
    if not records:
        return csr_matrix((0, HASH_FEATURES), dtype=np.float64), np.log1p(target), indices
    return hasher.transform([transform_feature_record(r) for r in records]), np.log1p(target), indices


def _metrics(actual: np.ndarray, predicted: np.ndarray, model: str, split: str) -> dict[str, Any]:
    prediction = np.maximum(0.0, np.asarray(predicted, dtype=np.float64))
    truth = np.asarray(actual, dtype=np.float64)
    nonzero = truth != 0
    mape = float(np.mean(np.abs((truth[nonzero] - prediction[nonzero]) / truth[nonzero])) * 100.0) if nonzero.any() else None
    return {
        "model": model, "split": split, "records": int(len(truth)),
        "mae": float(mean_absolute_error(truth, prediction)),
        "rmse": float(np.sqrt(mean_squared_error(truth, prediction))),
        "mape_nonzero_actuals": mape,
        "r2": float(r2_score(truth, prediction)) if len(truth) > 1 and np.var(truth) > 0 else None,
    }


def _predict_csv(
    model: SGDRegressor, source: Path, output: Path | None, history: _CausalHistory
) -> dict[str, Any]:
    hasher = FeatureHasher(n_features=HASH_FEATURES, input_type="dict", alternate_sign=False)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
    header = True
    all_actual, all_predictions, all_baselines = [], [], []
    with (output.open("w", encoding="utf-8", newline="") if output is not None else nullcontext(None)) as stream:
        for chunk in _read_chunks(source):
            matrix, target_log, indices = _encode(chunk, hasher, history)
            if not len(target_log):
                continue
            predicted_log = model.predict(matrix)
            predicted = np.maximum(0.0, np.expm1(np.clip(predicted_log, -20.0, 20.0)))
            eligible = chunk.iloc[indices].loc[:, ["Date", "Item_ID", "Location_ID", "Is_Available", "Target_Quantity"]].copy()
            eligible["python_prediction"] = predicted
            if stream is not None:
                eligible.to_csv(stream, index=False, header=header)
            header = False
            all_actual.append(np.expm1(target_log))
            all_predictions.append(predicted)
            all_baselines.append(np.asarray(history.last_baselines))
    if not all_actual:
        raise ValueError(f"{source.stem} split contains no records with seven prior observations")
    result = _metrics(np.concatenate(all_actual), np.concatenate(all_predictions), "SGDRegressor", source.stem)
    baseline = _metrics(np.concatenate(all_actual), np.concatenate(all_baselines), "Seasonal naive lag 7", source.stem)
    result["seasonal_naive_metrics"] = baseline
    result["improves_baseline_rmse"] = result["rmse"] < baseline["rmse"]
    return result


def train_python_demand_model(
    train_csv: Path, validation_csv: Path, test_csv: Path,
    model_path: Path, test_predictions_path: Path,
) -> dict[str, Any]:
    """Train/evaluate by chronological CSV chunks without materializing the full dataset."""
    if model_path.exists() or test_predictions_path.exists():
        raise FileExistsError("Model and prediction outputs must be fresh; prior evidence is preserved")
    model = SGDRegressor(loss="squared_error", penalty="l2", alpha=0.0001,
                         learning_rate="invscaling", eta0=0.01, max_iter=1,
                         tol=None, random_state=42)
    hasher = FeatureHasher(n_features=HASH_FEATURES, input_type="dict", alternate_sign=False)
    best_rmse, best_epoch, best_model = float("inf"), 0, None
    validation_metrics = []
    final_history = None
    training_rows = 0
    for epoch in range(1, EPOCHS + 1):
        history = _CausalHistory()
        trained_rows = 0
        for chunk in _read_chunks(train_csv):
            matrix, target_log, _ = _encode(chunk, hasher, history)
            if len(target_log):
                model.partial_fit(matrix, target_log)
                trained_rows += len(target_log)
        if trained_rows == 0:
            raise ValueError("Python training split has no rows with seven prior observations")
        training_rows = trained_rows
        validation_path = model_path.parent / f"validation_epoch_{epoch}.csv"
        metrics = _predict_csv(model, validation_csv, validation_path, history)
        validation_path.unlink(missing_ok=True)
        metrics["epoch"] = epoch
        validation_metrics.append(metrics)
        if metrics["rmse"] < best_rmse:
            best_rmse, best_epoch, best_model = metrics["rmse"], epoch, copy.deepcopy(model)
        final_history = history
    if best_model is None or final_history is None:
        raise RuntimeError("Python model selection did not retain a validation model/history")
    best_model.dineiq_feature_version_ = FEATURE_VERSION
    best_model.dineiq_model_version_ = MODEL_VERSION
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(best_model, model_path, compress=3)
    test_metrics = _predict_csv(best_model, test_csv, test_predictions_path, final_history)
    training_metrics = _predict_csv(best_model, train_csv, None, _CausalHistory())
    return {
        "selected_model": "SGDRegressor", "model_version": MODEL_VERSION,
        "feature_version": FEATURE_VERSION,
        "runtime": {"python": platform.python_version(), "scikit_learn": sklearn.__version__,
                    "pandas": pd.__version__, "numpy": np.__version__},
        "best_epoch": best_epoch, "epochs_evaluated": EPOCHS, "hash_features": HASH_FEATURES,
        "causal_lag_features": ["lag_1", "lag_7", "trailing_7_mean"],
        "validation_metrics": validation_metrics, "test_metrics": test_metrics,
        "training_metrics": training_metrics,
        "model_path": str(model_path), "test_predictions_path": str(test_predictions_path),
        "training_rows": training_rows,
        "hyperparameters": best_model.get_params(),
        "evaluation_protocol": "rolling one-day-ahead, with prior actuals only; validation selects the epoch, test does not",
        "baseline_test_metrics": test_metrics["seasonal_naive_metrics"],
        "baseline_improvement_verified": test_metrics["improves_baseline_rmse"],
    }
