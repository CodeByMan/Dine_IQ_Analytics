"""Run a synthetic five-million-row Spark aggregation benchmark without touching source data."""
from __future__ import annotations

import json
import time

from pyspark.sql import SparkSession, functions as F

from dineiq.config import load_settings
from dineiq.dataset.pipeline import create_local_spark
from dineiq.paths import ensure_runtime_dirs


ROWS = 5_000_000


def main() -> int:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    spark = create_local_spark(settings, "DineIQ-Scale-Benchmark")
    spark.sparkContext.setLogLevel("ERROR")
    started = time.perf_counter()
    try:
        records = spark.range(ROWS).select(
            F.col("id").alias("Order_Item_ID"),
            F.pmod(F.col("id"), F.lit(1_200_000)).alias("Order_ID"),
            F.concat(F.lit("I"), F.lpad(F.pmod(F.col("id"), F.lit(210)).cast("string"), 3, "0")).alias("Item_ID"),
            F.concat(F.lit("L"), F.lpad(F.pmod(F.col("id"), F.lit(27)).cast("string"), 2, "0")).alias("Location_ID"),
            (F.pmod(F.col("id"), F.lit(5)) + F.lit(1)).cast("double").alias("Quantity"),
            ((F.pmod(F.col("id"), F.lit(5)) + F.lit(1)) * F.lit(12.5)).alias("Line_Total"),
        )
        source_rows = records.count()
        aggregates = records.groupBy("Item_ID", "Location_ID").agg(
            F.sum("Quantity").alias("units"), F.sum("Line_Total").alias("revenue"),
            F.countDistinct("Order_ID").alias("orders"),
        )
        aggregate_rows = aggregates.count()
        elapsed = time.perf_counter() - started
        passed = source_rows == ROWS and aggregate_rows > 0
        report = {
            "requirement_id": "NFR-EX-02", "passed": passed,
            "synthetic_order_line_rows": source_rows, "aggregated_item_location_groups": aggregate_rows,
            "elapsed_seconds": round(elapsed, 3), "operation": "Spark group aggregation with distinct-order metric",
            "source_dataset_modified": False,
        }
        target = settings.reports_root / "operations" / "scale_benchmark.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"SYNTHETIC ORDER LINES: {source_rows:,}/{ROWS:,}")
        print(f"ITEM/LOCATION GROUPS: {aggregate_rows:,}")
        print(f"ELAPSED: {elapsed:.2f} seconds")
        print(f"SOURCE DATA MODIFIED: NO")
        print(f"NFR-EX-02 SCALE BENCHMARK: {'PASS' if passed else 'FAIL'}")
        print(f"REPORT: {target}")
        return 0 if passed else 2
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
