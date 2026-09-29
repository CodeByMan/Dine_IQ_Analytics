"""Warm, persisted Spark/Python demand-model inference for live user requests."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math
from pathlib import Path
import time
import uuid

import joblib
from sklearn.feature_extraction import FeatureHasher

from dineiq.analytics.python_demand_model import HASH_FEATURES
from dineiq.config import Settings
from dineiq.db.operations import record_prediction_results


SPARK_MODEL_VERSION = "selected-spark-mllib-demand_models-v1"
PYTHON_MODEL_VERSION = "sklearn-sgdregressor-demand_models-v1"


@dataclass(frozen=True)
class DemandRequest:
    forecast_date: str
    item_id: str
    location_id: str
    is_available: float
    lag_1: float
    lag_7: float
    trailing_7_mean: float

    def validated(self) -> tuple[date, str, str, float, float, float, float]:
        try:
            day = date.fromisoformat(self.forecast_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("Forecast date must use YYYY-MM-DD format.") from exc
        item, location = self.item_id.strip(), self.location_id.strip()
        if not item or not location:
            raise ValueError("Menu item and restaurant location are required.")
        numbers = (self.is_available, self.lag_1, self.lag_7, self.trailing_7_mean)
        try:
            values = tuple(float(x) for x in numbers)
        except (TypeError, ValueError) as exc:
            raise ValueError("Availability and demand history must be numeric.") from exc
        if values[0] not in (0.0, 1.0) or any(not math.isfinite(x) or x < 0 for x in values[1:]):
            raise ValueError("Availability must be 0/1 and historical demand values must be finite and non-negative.")
        return day, item, location, values[0], values[1], values[2], values[3]


def python_feature_record(request: DemandRequest) -> dict[str, float]:
    day, item, location, available, lag1, lag7, trailing = request.validated()
    doy = day.timetuple().tm_yday
    return {
        f"item={item}": 1.0, f"location={location}": 1.0,
        f"weekday={day.weekday()}": 1.0, f"month={day.month}": 1.0,
        "available": available, "year_offset": (day.year - 2000.0) / 10.0,
        "annual_sin": math.sin(2.0 * math.pi * doy / 365.25),
        "annual_cos": math.cos(2.0 * math.pi * doy / 365.25),
        "lag_1": lag1, "lag_7": lag7, "trailing_7_mean": trailing,
    }


class DualDemandPredictor:
    """Load trained artifacts once; each predict call runs inference only."""

    def __init__(self, spark: SparkSession, settings: Settings):
        from pyspark.ml import PipelineModel
        root = settings.models_dir / "demand_models"
        spark_path = root / "spark" / "selected_model"
        python_path = root / "python" / "selected_model.joblib"
        if not spark_path.is_dir() or not python_path.is_file():
            raise FileNotFoundError("Both selected demand models model artifacts must exist before live predictions.")
        self.spark = spark
        self._database_path = settings.database_path
        self.spark_model = PipelineModel.load(str(spark_path))
        self.python_model = joblib.load(python_path)
        self.hasher = FeatureHasher(n_features=HASH_FEATURES, input_type="dict", alternate_sign=False)
        self._warm_models()

    def _spark_feature_frame(self, request: DemandRequest):
        from pyspark.sql import functions as F
        from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType
        day, item, location, available, lag1, lag7, trailing = request.validated()
        schema = StructType([
            StructField("Date", DateType(), False), StructField("Item_ID", StringType(), False),
            StructField("Location_ID", StringType(), False), StructField("Is_Available", DoubleType(), False),
            StructField("lag_1", DoubleType(), False), StructField("lag_7", DoubleType(), False),
            StructField("trailing_7_mean", DoubleType(), False),
        ])
        row = (day, item, location, available, lag1, lag7, trailing)
        return self.spark.createDataFrame([row], schema=schema).withColumn(
            "day_of_week", F.dayofweek("Date").cast("double")
        ).withColumn("month", F.month("Date").cast("double")).withColumn(
            "year", F.year("Date").cast("double")
        ).withColumn("day_sin", F.sin(F.lit(2.0 * math.pi / 365.25) * F.dayofyear("Date"))).withColumn(
            "day_cos", F.cos(F.lit(2.0 * math.pi / 365.25) * F.dayofyear("Date"))
        )

    def _warm_models(self) -> None:
        """Run one discarded prediction per model before serving timed requests."""
        warm_request = DemandRequest("2026-01-01", "__warmup_item__", "__warmup_location__", 1.0, 1.0, 1.0, 1.0)
        self.spark_model.transform(self._spark_feature_frame(warm_request)).select("prediction").first()
        vector = self.hasher.transform([python_feature_record(warm_request)])
        self.python_model.predict(vector)

    def predict(self, request: DemandRequest) -> dict[str, object]:
        from pyspark.sql import functions as F
        started = time.perf_counter()
        day, item, location, available, lag1, lag7, trailing = request.validated()
        frame = self._spark_feature_frame(request)
        spark_value = self.spark_model.transform(frame).select(
            F.greatest(F.lit(0.0), F.col("prediction").cast("double")).alias("prediction")
        ).first()["prediction"]
        vector = self.hasher.transform([python_feature_record(request)])
        python_log = float(self.python_model.predict(vector)[0])
        python_value = max(0.0, math.expm1(max(-20.0, min(20.0, python_log))))
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        request_id = str(uuid.uuid4())
        result = {
            "request_id": request_id, "date": day.isoformat(), "item_id": item, "location_id": location,
            "spark_prediction": float(spark_value), "python_prediction": float(python_value),
            "spark_model_version": SPARK_MODEL_VERSION, "python_model_version": PYTHON_MODEL_VERSION,
            "latency_ms": elapsed_ms,
        }
        record_prediction_results(str(self._database_path), result)
        return result

    @classmethod
    def load(cls, spark: SparkSession, settings: Settings) -> "DualDemandPredictor":
        return cls(spark, settings)
