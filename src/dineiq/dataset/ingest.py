"""Typed, read-only Spark ingestion and source schema validation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from dineiq.dataset.schema import (
    BOOLEAN_FIELDS,
    DATE_FIELDS,
    DOUBLE_FIELDS,
    INTEGER_FIELDS,
    STRING_ID_FIELDS,
    TABLE_SCHEMAS,
    TIMESTAMP_FIELDS,
    expected_schema,
    spark_type_for,
)
from dineiq.dataset.ingestion_audit import resolve_source_paths


@dataclass(frozen=True)
class SchemaCheck:
    table: str
    passed: bool
    missing_columns: tuple[str, ...]
    unexpected_columns: tuple[str, ...]
    type_mismatches: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _cast_column(column: str, table: str):
    source = F.trim(F.col(column))
    if column in BOOLEAN_FIELDS:
        normalized = F.lower(source)
        return F.when(normalized.isin("true", "1", "yes"), F.lit(True)).when(
            normalized.isin("false", "0", "no"), F.lit(False)
        ).otherwise(F.lit(None).cast("boolean"))
    if column in DATE_FIELDS:
        return F.expr(f"try_cast(`{column}` AS DATE)")
    if column in TIMESTAMP_FIELDS:
        return F.expr(f"try_cast(`{column}` AS TIMESTAMP)")
    if column == "Promotion_ID" and table == "orders":
        return F.col(column)
    if column in STRING_ID_FIELDS:
        return F.col(column)
    if column in INTEGER_FIELDS or column.endswith("_ID"):
        return F.expr(f"try_cast(`{column}` AS BIGINT)")
    if column in DOUBLE_FIELDS:
        return F.expr(f"try_cast(`{column}` AS DOUBLE)")
    return F.col(column)


def read_table(spark: SparkSession, data_root: Path, table: str) -> DataFrame:
    """Read canonical or split CSVs, then cast to declared data-dictionary types."""
    if table not in TABLE_SCHEMAS:
        raise KeyError(f"Unknown source table: {table}")
    paths = resolve_source_paths(data_root, table)
    if not paths:
        raise FileNotFoundError(f"Canonical or split CSV not found for {table}: {data_root / 'data'}")
    raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .csv([str(path) for path in paths])
    )
    check = validate_schema(table, raw, check_types=False)
    if check.missing_columns or check.unexpected_columns:
        raise ValueError(
            f"Schema columns invalid for {table}: missing={check.missing_columns}; "
            f"unexpected={check.unexpected_columns}"
        )
    typed = raw
    for field in expected_schema(table).fields:
        converted = _cast_column(field.name, table)
        if field.dataType.simpleString() != "string":
            raw_value = F.trim(F.col(field.name))
            typed = typed.withColumn(
                f"__invalid_cast__{field.name}",
                raw_value.isNotNull() & (raw_value != "") & converted.isNull(),
            )
        typed = typed.withColumn(field.name, converted)
    return typed


def read_table_inferred(spark: SparkSession, data_root: Path, table: str) -> DataFrame:
    """Read a table with Spark inference for an explicit-schema comparison only."""
    if table not in TABLE_SCHEMAS:
        raise KeyError(f"Unknown source table: {table}")
    paths = resolve_source_paths(data_root, table)
    if not paths:
        raise FileNotFoundError(f"Canonical or split CSV not found for {table}: {data_root / 'data'}")
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .csv([str(path) for path in paths])
    )


def validate_schema(table: str, frame: DataFrame, check_types: bool = True) -> SchemaCheck:
    """Compare a Spark frame's columns and declared Spark types with the data dictionary."""
    if table not in TABLE_SCHEMAS:
        raise KeyError(f"Unknown source table: {table}")
    spec = TABLE_SCHEMAS[table]
    expected = set(spec.columns)
    actual = {name for name in frame.columns if not name.startswith("__invalid_cast__")}
    missing = tuple(sorted(expected - actual))
    unexpected = tuple(sorted(actual - expected))
    mismatches: list[str] = []
    if check_types:
        actual_types = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
        for field in expected_schema(table).fields:
            if field.name in actual_types and actual_types[field.name] != field.dataType.simpleString():
                mismatches.append(
                    f"{field.name}: expected {field.dataType.simpleString()}, "
                    f"got {actual_types[field.name]}"
                )
    return SchemaCheck(
        table=table,
        passed=not missing and not unexpected and not mismatches,
        missing_columns=missing,
        unexpected_columns=unexpected,
        type_mismatches=tuple(mismatches),
    )


def validate_source_schemas(spark: SparkSession, data_root: Path) -> tuple[SchemaCheck, ...]:
    """Read each canonical source and return a per-table schema result."""
    checks: list[SchemaCheck] = []
    for table in TABLE_SCHEMAS:
        frame = read_table(spark, data_root, table)
        checks.append(validate_schema(table, frame, check_types=True))
    return tuple(checks)


def invalid_cast_counts(frame: DataFrame, table: str) -> dict[str, int]:
    """Count nonblank typed fields that became null during explicit casting."""
    expressions = [
        F.sum(F.when(F.col(name), 1).otherwise(0)).alias(name.removeprefix("__invalid_cast__"))
        for name in frame.columns
        if name.startswith("__invalid_cast__")
    ]
    if not expressions:
        return {}
    row = frame.agg(*expressions).first()
    return {name: int(row[name] or 0) for name in row.asDict()}
