"""Read-only source-file inventory and reproducibility evidence.

The canonical CSV package is external to the application archive.  This module
therefore records what is actually available at runtime without copying,
rewriting, or normalising the source files.  It also supports split-table
layouts used by hidden-data submissions (``data/<table>/*.csv`` or
``data/<table>_*.csv``).
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable



TABLE_FILENAMES = {
    "customers": "customers.csv",
    "inventory": "inventory.csv",
    "menu_categories": "menu_categories.csv",
    "menu_items": "menu_items.csv",
    "order_items": "order_items.csv",
    "ordering_channels": "ordering_channels.csv",
    "orders": "orders.csv",
    "pricing_history": "pricing_history.csv",
    "promotions": "promotions.csv",
    "ratings": "ratings.csv",
    "restaurants": "restaurants.csv",
    "wastage": "wastage.csv",
}


@dataclass(frozen=True)
class SourceFile:
    path: str
    bytes: int
    sha256: str
    rows: int
    columns: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self) | {"columns": list(self.columns)}


def resolve_source_paths(data_root: Path, table: str) -> tuple[Path, ...]:
    """Resolve one canonical CSV or a deterministic split-file set.

    A canonical ``data/<table>.csv`` wins over split files so a submission
    containing both representations is never double-counted.  If the
    canonical file is absent, split files are accepted in a stable lexical
    order.
    """
    if table not in TABLE_FILENAMES:
        raise KeyError(f"Unknown source table: {table}")
    data_dir = data_root / "data"
    canonical = data_dir / TABLE_FILENAMES[table]
    if canonical.is_file():
        return (canonical,)
    directory_parts = sorted((data_dir / table).glob("*.csv"))
    named_parts = sorted(data_dir.glob(f"{table}_*.csv"))
    return tuple(path for path in (*directory_parts, *named_parts) if path.is_file())


def _scan_csv(path: Path) -> SourceFile:
    digest = hashlib.sha256()
    size = path.stat().st_size
    with path.open("rb") as raw:
        for block in iter(lambda: raw.read(1024 * 1024), b""):
            digest.update(block)
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        columns = tuple(reader.fieldnames or ())
        rows = sum(1 for _ in reader)
    return SourceFile(str(path), int(size), digest.hexdigest(), int(rows), columns)


def scan_source_files(data_root: Path, tables: Iterable[str] | None = None) -> dict[str, Any]:
    """Inventory CSV files, row counts, columns, sizes and content hashes."""
    names = tuple(tables or TABLE_FILENAMES)
    table_records: dict[str, list[dict[str, Any]]] = {}
    missing: list[str] = []
    for table in names:
        paths = resolve_source_paths(data_root, table)
        if not paths:
            missing.append(table)
            table_records[table] = []
            continue
        table_records[table] = [_scan_csv(path).as_dict() for path in paths]
    return {
        "dataset_root": str(data_root),
        "data_directory": str(data_root / "data"),
        "tables": table_records,
        "table_count": len(names),
        "missing_tables": missing,
        "source_file_count": sum(len(files) for files in table_records.values()),
        "total_rows": sum(item["rows"] for files in table_records.values() for item in files),
        "passed": not missing,
    }


def schema_inference_demo(spark, data_root: Path, table: str) -> dict[str, Any]:
    """Compare Spark's inferred schema with the declared schema for one table.

    The canonical ingestion path remains explicit-schema/cast based.  Inference
    is deliberately isolated to an evidence call so it cannot silently change
    production types.
    """
    from dineiq.dataset.schema import expected_schema

    paths = resolve_source_paths(data_root, table)
    if not paths:
        return {"table": table, "available": False, "passed": False, "error": "source file missing"}
    inferred = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .csv([str(path) for path in paths])
    )
    declared = {field.name: field.dataType.simpleString() for field in expected_schema(table).fields}
    inferred_types = {field.name: field.dataType.simpleString() for field in inferred.schema.fields}
    missing = sorted(set(declared) - set(inferred_types))
    return {
        "table": table,
        "available": True,
        "source_files": [str(path) for path in paths],
        "declared_types": declared,
        "inferred_types": inferred_types,
        "missing_columns": missing,
        "inference_row_count": inferred.limit(1000).count(),
        "passed": not missing,
        "note": "Inference is evidence only; canonical reads use explicit schema plus casts.",
    }


def build_ingestion_audit(data_root: Path, inference: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a JSON-safe ingestion evidence record."""
    inventory = scan_source_files(data_root)
    return {
        "requirement_ids": ["DE-002", "DE-004", "SP-003", "DATA-032"],
        "explicit_schema_path": "src/dineiq/dataset/schema.py::expected_schema",
        "canonical_ingestion_path": "src/dineiq/dataset/ingest.py::read_table",
        "inference_demo": inference or {"available": False, "note": "Spark inference not executed in this run."},
        "multiple_file_support": True,
        "large_file_strategy": "Spark CSV reader with deterministic file list and bounded Spark partitions.",
        **inventory,
    }


def write_ingestion_audit(audit: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
