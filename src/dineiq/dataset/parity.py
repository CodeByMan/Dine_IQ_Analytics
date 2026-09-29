"""CSV/Parquet parity checks without mutating either representation."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from dineiq.dataset.ingestion_audit import TABLE_FILENAMES, resolve_source_paths


def _csv_columns_and_rows(paths: tuple[Path, ...]) -> tuple[tuple[str, ...], int]:
    columns: tuple[str, ...] = ()
    rows = 0
    for path in paths:
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            current = tuple(reader.fieldnames or ())
            if columns and current != columns:
                raise ValueError(f"CSV part schema mismatch: {path}")
            columns = current
            rows += sum(1 for _ in reader)
    return columns, rows


def check_table_parity(data_root: Path, table: str) -> dict[str, Any]:
    """Check source row count/schema against ``parquet/<table>.parquet``."""
    paths = resolve_source_paths(data_root, table)
    parquet_path = data_root / "parquet" / f"{table}.parquet"
    result: dict[str, Any] = {"table": table, "csv_paths": [str(p) for p in paths], "parquet_path": str(parquet_path)}
    if not paths or not parquet_path.exists():
        result.update({"available": False, "passed": False, "error": "CSV or Parquet path missing"})
        return result
    try:
        csv_columns, csv_rows = _csv_columns_and_rows(paths)
        import pyarrow.parquet as pq

        metadata = pq.read_metadata(parquet_path)
        parquet_schema = pq.read_schema(parquet_path)
        parquet_columns = tuple(parquet_schema.names)
        parquet_rows = int(metadata.num_rows)
        result.update({
            "available": True,
            "csv_rows": csv_rows,
            "parquet_rows": parquet_rows,
            "csv_columns": list(csv_columns),
            "parquet_columns": list(parquet_columns),
            "row_count_match": csv_rows == parquet_rows,
            "schema_match": csv_columns == parquet_columns,
            "passed": csv_rows == parquet_rows and csv_columns == parquet_columns,
        })
    except (ImportError, OSError, ValueError) as exc:
        result.update({"available": False, "passed": False, "error": f"{type(exc).__name__}: {exc}"})
    return result


def check_all_parity(data_root: Path) -> dict[str, Any]:
    checks = {name: check_table_parity(data_root, name) for name in TABLE_FILENAMES}
    return {
        "requirement_ids": ["DATA-032", "DE-004", "SP-003"],
        "tables": checks,
        "passed": all(item["passed"] for item in checks.values()),
    }
