from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.pipeline import Pipeline

from pyspark.sql import DataFrame, functions as F


MODEL_VERSION = "sklearn-randomforest-wastage-risk-v1"
MIN_TEST_ACCURACY = 0.85
MIN_TEST_MACRO_F1 = 0.80
FEATURE_COLUMNS = (
    "historical_wastage_rate",
    "historical_quantity_wasted",
    "historical_preparation_quantity",
    "historical_wastage_cost",
    "closing_stock",
    "reorder_level",
    "units_sold",
    "stockout_flag",
    "item_popularity",
    "month",
    "day_of_week",
)
KEY_COLUMNS = ("Item_ID", "Location_ID")
MODEL_FEATURE_PREFIX = "model_feature_"


class WastageRiskPredictor:
    """Load and apply the persisted wastage model without retraining."""

    def __init__(self, payload: dict[str, Any], model_path: Path):
        model = payload.get("model")
        features = tuple(payload.get("feature_columns", ()))
        if model is None or features != FEATURE_COLUMNS:
            raise ValueError("Persisted wastage model has an incompatible feature contract")
        if payload.get("model_version") != MODEL_VERSION:
            raise ValueError("Persisted wastage model version is unsupported")
        self.model = model
        self.model_version = str(payload["model_version"])
        self.risk_threshold = float(payload["risk_threshold"])
        self.model_path = model_path

    @classmethod
    def load(cls, model_path: Path) -> "WastageRiskPredictor":
        if not model_path.is_file():
            raise FileNotFoundError(f"Persisted wastage model is missing: {model_path}")
        return cls(joblib.load(model_path), model_path)

    def _features(self, frame: pd.DataFrame) -> pd.DataFrame:
        source = frame.copy()
        prefixed = [f"{MODEL_FEATURE_PREFIX}{column}" for column in FEATURE_COLUMNS]
        if all(column in source.columns for column in prefixed):
            # Persisted risk outputs carry the exact model input contract under
            # model_feature_* names.  Select those columns explicitly instead
            # of renaming the complete frame: UI joins may also contain a
            # display column with the same canonical name, and pandas returns
            # a DataFrame (rather than a Series) for duplicate labels.
            source = source[prefixed].copy()
            source.columns = list(FEATURE_COLUMNS)
            return _numeric(source, list(FEATURE_COLUMNS))[list(FEATURE_COLUMNS)]
        # Defensive protection for frames assembled by display joins.  This
        # preserves the first canonical column while preventing duplicate
        # labels from reaching pandas.to_numeric.
        source = source.loc[:, ~source.columns.duplicated(keep="first")]
        missing = sorted(set(FEATURE_COLUMNS) - set(source.columns))
        if missing:
            raise ValueError(f"Wastage inference input missing features: {missing}")
        return _numeric(source, list(FEATURE_COLUMNS))[list(FEATURE_COLUMNS)]

    def predict(self, frame: pd.DataFrame) -> pd.DataFrame:
        features = self._features(frame)
        probability = self.model.predict_proba(features)[:, 1]
        return pd.DataFrame({
            "model_risk_probability": probability.astype(float),
            "model_high_wastage_risk": probability >= 0.5,
            "model_version": self.model_version,
        }, index=frame.index)


def _numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = frame.loc[:, ~frame.columns.duplicated(keep="first")].copy()
    for column in columns:
        if column not in result:
            result[column] = np.nan
        values = result[column]
        # A defensive fallback for externally assembled frames that still
        # contain duplicate labels after copying.
        if isinstance(values, pd.DataFrame):
            values = values.iloc[:, 0]
        result[column] = pd.to_numeric(values, errors="coerce")
    return result


def _to_pandas(tables: dict[str, DataFrame], menu_features: DataFrame) -> pd.DataFrame:
    """Create daily causal features from source tables without using future labels."""
    wastage_columns = [
        name for name in (
            "Item_ID", "Location_ID", "Wastage_Date", "Quantity_Wasted",
            "Preparation_Quantity", "Total_Wastage_Cost",
        ) if name in tables["wastage"].columns
    ]
    required = {"Item_ID", "Location_ID", "Wastage_Date", "Quantity_Wasted", "Preparation_Quantity", "Total_Wastage_Cost"}
    if not required.issubset(wastage_columns):
        raise ValueError(f"wastage model requires columns: {sorted(required)}")
    waste = tables["wastage"].select(*wastage_columns).toPandas()
    waste["Wastage_Date"] = pd.to_datetime(waste["Wastage_Date"], errors="coerce")
    waste = waste.dropna(subset=["Wastage_Date", "Item_ID", "Location_ID"])
    for column in ("Quantity_Wasted", "Preparation_Quantity", "Total_Wastage_Cost"):
        waste[column] = pd.to_numeric(waste[column], errors="coerce").fillna(0.0)
    waste["Item_ID"] = waste["Item_ID"].astype(str)
    waste["Location_ID"] = waste["Location_ID"].astype(str)
    daily = waste.groupby(["Item_ID", "Location_ID", "Wastage_Date"], as_index=False).agg(
        historical_quantity_wasted=("Quantity_Wasted", "sum"),
        historical_preparation_quantity=("Preparation_Quantity", "sum"),
        historical_wastage_cost=("Total_Wastage_Cost", "sum"),
    )
    daily["historical_wastage_rate"] = np.where(
        daily["historical_preparation_quantity"] > 0,
        daily["historical_quantity_wasted"] / daily["historical_preparation_quantity"],
        np.nan,
    )

    if "inventory" in tables:
        inventory_columns = [
            name for name in (
                "Item_ID", "Location_ID", "Inventory_Date", "Closing_Stock",
                "Reorder_Level", "Units_Sold", "Stockout_Flag",
            ) if name in tables["inventory"].columns
        ]
        inventory = tables["inventory"].select(*inventory_columns).toPandas()
        if "Inventory_Date" in inventory:
            inventory["Inventory_Date"] = pd.to_datetime(inventory["Inventory_Date"], errors="coerce")
            inventory = inventory.rename(columns={"Inventory_Date": "Wastage_Date"})
            inventory["Item_ID"] = inventory["Item_ID"].astype(str)
            inventory["Location_ID"] = inventory["Location_ID"].astype(str)
            inventory = inventory.drop_duplicates(["Item_ID", "Location_ID", "Wastage_Date"])
            inventory = inventory.rename(columns={
                "Closing_Stock": "closing_stock", "Reorder_Level": "reorder_level",
                "Units_Sold": "units_sold", "Stockout_Flag": "stockout_flag",
            })
            daily = daily.merge(
                inventory[[name for name in (
                    "Item_ID", "Location_ID", "Wastage_Date", "closing_stock",
                    "reorder_level", "units_sold", "stockout_flag",
                ) if name in inventory]],
                on=["Item_ID", "Location_ID", "Wastage_Date"], how="left",
            )

    popularity_columns = [name for name in ("Item_ID", "item_popularity", "Popularity_Index") if name in menu_features.columns]
    popularity = menu_features.select(*popularity_columns).toPandas()
    if "Popularity_Index" in popularity and "item_popularity" not in popularity:
        popularity = popularity.rename(columns={"Popularity_Index": "item_popularity"})
    popularity["Item_ID"] = popularity["Item_ID"].astype(str)
    popularity = popularity.drop_duplicates(["Item_ID"])
    daily = daily.merge(popularity[[name for name in ("Item_ID", "item_popularity") if name in popularity]], on="Item_ID", how="left")
    daily = daily.sort_values(["Wastage_Date", "Item_ID", "Location_ID"]).reset_index(drop=True)
    daily["month"] = daily["Wastage_Date"].dt.month.astype(float)
    daily["day_of_week"] = daily["Wastage_Date"].dt.dayofweek.astype(float)
    for column in ("closing_stock", "reorder_level", "units_sold", "stockout_flag", "item_popularity"):
        if column not in daily:
            daily[column] = np.nan
    daily = _numeric(daily, list(FEATURE_COLUMNS))
    return daily


def _feature_rows(daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return causal training rows and latest item/location inference rows."""
    work = daily.sort_values(["Item_ID", "Location_ID", "Wastage_Date"]).copy()
    grouped = work.groupby(["Item_ID", "Location_ID"], sort=False)
    for column in ("historical_wastage_rate", "historical_quantity_wasted", "historical_preparation_quantity", "historical_wastage_cost", "closing_stock", "reorder_level", "units_sold", "stockout_flag"):
        work[column] = grouped[column].transform(lambda values: values.shift(1))
    work["target_rate"] = grouped["historical_wastage_rate"].transform(lambda values: values.shift(-1))
    latest = work.sort_values("Wastage_Date").groupby(list(KEY_COLUMNS), as_index=False).tail(1).copy()
    training = work.dropna(subset=["target_rate"]).copy()
    return training, latest


def _metrics(model: Pipeline, frame: pd.DataFrame, threshold: float, split: str) -> dict[str, Any]:
    if frame.empty:
        return {"split": split, "records": 0, "accuracy": None, "macro_f1": None, "roc_auc": None}
    actual = (frame["target_rate"].to_numpy(dtype=float) >= threshold).astype(int)
    predicted = model.predict(frame[list(FEATURE_COLUMNS)])
    probabilities = model.predict_proba(frame[list(FEATURE_COLUMNS)])[:, 1]
    result: dict[str, Any] = {
        "split": split, "records": int(len(frame)),
        "accuracy": float(accuracy_score(actual, predicted)),
        "macro_f1": float(f1_score(actual, predicted, average="macro", zero_division=0)),
        "roc_auc": None,
    }
    if len(np.unique(actual)) == 2:
        result["roc_auc"] = float(roc_auc_score(actual, probabilities))
    return result


def _classification_acceptance(metrics: dict[str, Any]) -> dict[str, Any]:
    """Enforce SRS §1.7.4 on held-out test metrics, never training scores."""
    def meets(name: str, minimum: float) -> bool:
        value = metrics.get(name)
        return bool(
            isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and minimum <= value <= 1.0
        )

    accuracy_met = meets("accuracy", MIN_TEST_ACCURACY)
    macro_f1_met = meets("macro_f1", MIN_TEST_MACRO_F1)
    return {
        "requirement": "§1.7.4",
        "split": "test",
        "minimum_accuracy": MIN_TEST_ACCURACY,
        "minimum_macro_f1": MIN_TEST_MACRO_F1,
        "rule": "test accuracy >= 0.85 OR test macro_f1 >= 0.80",
        "accuracy_met": accuracy_met,
        "macro_f1_met": macro_f1_met,
        "passed": bool(
            metrics.get("split") == "test" and metrics.get("records", 0) > 0
            and (accuracy_met or macro_f1_met)
        ),
    }


def train_wastage_risk_model(
    tables: dict[str, DataFrame],
    menu_features: DataFrame,
    model_path: Path,
    report_path: Path,
) -> dict[str, Any]:
    """Train, persist, reload, and use a supervised high-wastage classifier."""
    daily = _to_pandas(tables, menu_features)
    training, latest = _feature_rows(daily)
    if len(training) < 20:
        raise ValueError(f"Wastage model requires at least 20 causal training rows; got {len(training)}")
    dates = pd.to_datetime(training["Wastage_Date"])
    train_cutoff = dates.quantile(0.70)
    validation_cutoff = dates.quantile(0.85)
    train_frame = training[dates <= train_cutoff].copy()
    validation_frame = training[(dates > train_cutoff) & (dates <= validation_cutoff)].copy()
    test_frame = training[dates > validation_cutoff].copy()
    if train_frame.empty or validation_frame.empty or test_frame.empty:
        raise ValueError("Wastage model requires non-empty chronological train/validation/test windows")
    # Fit the threshold only from the training window; no validation/test
    # target values influence the decision boundary.
    threshold = float(train_frame["target_rate"].quantile(0.75))
    train_labels = (train_frame["target_rate"] >= threshold).astype(int)
    if train_labels.nunique() < 2:
        threshold = float(train_frame["target_rate"].median())
        train_labels = (train_frame["target_rate"] >= threshold).astype(int)
    if train_labels.nunique() < 2:
        raise ValueError("Wastage training window has only one risk class")
    model = Pipeline([
        ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
        ("classifier", RandomForestClassifier(
            n_estimators=96, max_depth=10, min_samples_leaf=2,
            class_weight="balanced", random_state=42, n_jobs=1,
        )),
    ])
    model.fit(train_frame[list(FEATURE_COLUMNS)], train_labels)
    validation_metrics = _metrics(model, validation_frame, threshold, "validation")
    test_metrics = _metrics(model, test_frame, threshold, "test")
    acceptance = _classification_acceptance(test_metrics)
    if not acceptance["passed"]:
        # Keep the previous selected model untouched. Retain the actual failed
        # metrics so a failed build cannot be mistaken for a successful model.
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps({
            "model_version": MODEL_VERSION,
            "algorithm": "RandomForestClassifier",
            "train_records": int(len(train_frame)),
            "validation_records": int(len(validation_frame)),
            "test_records": int(len(test_frame)),
            "validation_metrics": validation_metrics,
            "test_metrics": test_metrics,
            "classification_acceptance": acceptance,
            "artifact_path": str(model_path),
            "selected_model_updated": False,
            "source_data_modified": False,
            "passed": False,
        }, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        raise ValueError(
            "Wastage model failed SRS §1.7.4: test accuracy must be >= 0.85 "
            "or test macro-F1 >= 0.80. "
            f"Observed accuracy={test_metrics['accuracy']:.4f}, "
            f"macro-F1={test_metrics['macro_f1']:.4f}. "
            f"The previous selected model was retained; metrics: {report_path}"
        )
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "model_version": MODEL_VERSION, "feature_columns": list(FEATURE_COLUMNS), "risk_threshold": threshold}, model_path, compress=3)
    loaded_predictor = WastageRiskPredictor.load(model_path)
    loaded_model: Pipeline = loaded_predictor.model
    latest_features = latest[list(KEY_COLUMNS) + list(FEATURE_COLUMNS)].copy()
    predictions = loaded_predictor.predict(latest_features)
    for column in FEATURE_COLUMNS:
        latest_features[f"{MODEL_FEATURE_PREFIX}{column}"] = latest_features[column]
    latest_features = pd.concat([latest_features, predictions], axis=1)
    prediction_frame = latest_features[list(KEY_COLUMNS) +
                                       [f"{MODEL_FEATURE_PREFIX}{column}" for column in FEATURE_COLUMNS] +
                                       ["model_risk_probability", "model_high_wastage_risk", "model_version"]]
    prediction_frame = prediction_frame.drop_duplicates(list(KEY_COLUMNS))
    summary: dict[str, Any] = {
        "model_version": MODEL_VERSION,
        "algorithm": "RandomForestClassifier",
        "task": "high-wastage-risk classification",
        "target": "next observed wastage rate >= training 75th-percentile threshold",
        "features": list(FEATURE_COLUMNS),
        "risk_threshold": threshold,
        "train_records": int(len(train_frame)),
        "validation_records": int(len(validation_frame)),
        "test_records": int(len(test_frame)),
        "inference_records": int(len(prediction_frame)),
        "chronological_boundaries": {
            "train_max": str(train_frame["Wastage_Date"].max().date()),
            "validation_min": str(validation_frame["Wastage_Date"].min().date()),
            "validation_max": str(validation_frame["Wastage_Date"].max().date()),
            "test_min": str(test_frame["Wastage_Date"].min().date()),
        },
        "validation_metrics": _metrics(loaded_model, validation_frame, threshold, "validation"),
        "test_metrics": _metrics(loaded_model, test_frame, threshold, "test"),
        "classification_acceptance": acceptance,
        "artifact_path": str(model_path),
        "loaded_persisted_model_for_inference": loaded_predictor.model_path == model_path,
        "source_data_modified": False,
    }
    summary["passed"] = bool(
        summary["train_records"] > 0 and summary["validation_records"] > 0
        and summary["test_records"] > 0 and summary["inference_records"] > 0
        and summary["loaded_persisted_model_for_inference"] and model_path.is_file()
        and summary["classification_acceptance"]["passed"]
    )
    spark_predictions = tables["wastage"].sparkSession.createDataFrame(prediction_frame)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"predictions": spark_predictions, "summary": summary}
