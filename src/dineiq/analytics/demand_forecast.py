"""Leakage-safe short-horizon demand forecasting and chronological evaluation."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def build_demand_forecasts(
    train: DataFrame, validation: DataFrame, test: DataFrame,
    menu_items: DataFrame, horizon_days: int = 30,
) -> dict[str, DataFrame]:
    """Evaluate causal seasonal-naive and trailing-week forecasts, then project.

    All lag/rolling predictors look strictly backward within each item/location
    series. Validation and test are required to be chronological and disjoint.
    The trailing-week average is compared with a seasonal-naive (lag-7) baseline.
    """
    if horizon_days < 1 or horizon_days > 366:
        raise ValueError("horizon_days must be between 1 and 366")
    def normalize(frame: DataFrame, split_name: str) -> DataFrame:
        return frame.select(
            F.to_date("Date").alias("Date"), F.col("Item_ID").cast("string").alias("Item_ID"),
            F.col("Location_ID").cast("string").alias("Location_ID"),
            F.col("Target_Quantity").cast("double").alias("actual"),
        ).withColumn("split", F.lit(split_name))

    tr, va, te = normalize(train, "train"), normalize(validation, "validation"), normalize(test, "test")
    bounds = [frame.agg(F.min("Date").alias("lo"), F.max("Date").alias("hi")).first() for frame in (tr, va, te)]
    if any(row["lo"] is None for row in bounds):
        raise ValueError("train, validation, and test demand splits must all contain dated rows")
    if not (bounds[0]["hi"] < bounds[1]["lo"] and bounds[1]["hi"] < bounds[2]["lo"]):
        raise ValueError("forecast splits must be strictly chronological and non-overlapping")

    history = tr.unionByName(va).unionByName(te)
    series = Window.partitionBy("Item_ID", "Location_ID").orderBy("Date")
    past_week = series.rowsBetween(-7, -1)
    evaluated = history.withColumn("seasonal_naive", F.lag("actual", 7).over(series)).withColumn(
        "trailing_week_mean", F.avg("actual").over(past_week)
    ).where((F.col("split") == "test") & F.col("seasonal_naive").isNotNull() & F.col("trailing_week_mean").isNotNull())

    def metric_rows(method: str, prediction: str) -> DataFrame:
        base = evaluated.withColumn("prediction", F.greatest(F.lit(0.0), F.col(prediction)))
        stats = base.agg(
            F.avg(F.abs(F.col("actual") - F.col("prediction"))).alias("mae"),
            F.sqrt(F.avg(F.pow(F.col("actual") - F.col("prediction"), 2))).alias("rmse"),
            F.avg(F.when(F.col("actual") != 0, F.abs(F.col("actual") - F.col("prediction")) / F.abs("actual") * 100.0)).alias("mape"),
            F.sum(F.pow(F.col("actual") - F.col("prediction"), 2)).alias("sse"),
            F.var_pop("actual").alias("variance"), F.count(F.lit(1)).alias("n"),
        ).withColumn("r2", F.when(F.col("variance") > 0, 1.0 - F.col("sse") / (F.col("variance") * F.col("n"))))
        return stats.select(
            F.lit(method).alias("method"),
            F.explode(F.array(
                F.struct(F.lit("MAE").alias("metric"), F.col("mae").alias("value")),
                F.struct(F.lit("RMSE").alias("metric"), F.col("rmse").alias("value")),
                F.struct(F.lit("MAPE_nonzero_actuals").alias("metric"), F.col("mape").alias("value")),
                F.struct(F.lit("R2").alias("metric"), F.col("r2").alias("value")),
            )).alias("metric_value"),
        ).select("method", "metric_value.metric", "metric_value.value")

    metrics = metric_rows("Seasonal naive (lag 7)", "seasonal_naive").unionByName(
        metric_rows("Trailing 7-day mean", "trailing_week_mean")
    )
    last_window = Window.partitionBy("Item_ID", "Location_ID").orderBy(F.col("Date").desc())
    latest = history.withColumn("row_num", F.row_number().over(last_window)).where(
        F.col("row_num") == 1
    ).select("Item_ID", "Location_ID", "Date")
    # Use a max-date window against a renamed series date to avoid self-join ambiguity.
    hist_dates = history.select("Item_ID", "Location_ID", F.col("Date").alias("history_date"), "actual")
    recent = latest.select("Item_ID", "Location_ID", F.col("Date").alias("last_date")).join(
        hist_dates, ["Item_ID", "Location_ID"], "inner"
    ).where(F.col("history_date") > F.date_sub(F.col("last_date"), 7)).groupBy(
        "Item_ID", "Location_ID", "last_date"
    ).agg(F.avg("actual").alias("weekly_mean"))
    future = recent.crossJoin(
        train.sparkSession.range(1, horizon_days + 1).select(F.col("id").cast("int").alias("horizon_day"))
    ).withColumn("Date", F.date_add("last_date", F.col("horizon_day"))).withColumn(
        "forecast_quantity", F.greatest(F.lit(0.0), F.col("weekly_mean"))
    )
    item_category = menu_items.select(
        F.col("Item_ID").cast("string").alias("Item_ID"), F.col("Category_ID").cast("string").alias("Category_ID")
    )
    item_future = future.join(item_category, "Item_ID", "left").select(
        "Date", "horizon_day", "Item_ID", "Category_ID", "Location_ID", "forecast_quantity"
    ).withColumn("is_weekend", F.dayofweek("Date").isin(1, 7)).withColumn(
        "season",
        F.when(F.month("Date").isin(12, 1, 2), "Winter")
        .when(F.month("Date").isin(3, 4, 5), "Spring")
        .when(F.month("Date").isin(6, 7, 8), "Summer")
        .otherwise("Autumn"),
    ).withColumn(
        "forecast_period",
        F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday"),
    ).withColumn(
        "peak_demand_period",
        F.col("forecast_quantity") >= F.percentile_approx("forecast_quantity", 0.75).over(
            Window.partitionBy("Item_ID", "Location_ID")
        ),
    )
    category_future = item_future.groupBy("Date", "horizon_day", "Category_ID").agg(
        F.sum("forecast_quantity").alias("forecast_quantity")
    )
    location_future = item_future.groupBy("Date", "horizon_day", "Location_ID").agg(
        F.sum("forecast_quantity").alias("forecast_quantity")
    )
    return {
        "forecast_evaluation": metrics,
        "forecast_item_location_daily": item_future,
        "forecast_category_daily": category_future,
        "forecast_location_daily": location_future,
    }
