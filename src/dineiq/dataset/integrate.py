"""Spark SQL joins and FK validation for the ten SRS integration relations."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame, functions as F

from dineiq.dataset.schema import FOREIGN_KEYS, RELATIONSHIPS


@dataclass(frozen=True)
class RelationshipCheck:
    child_table: str
    child_key: str
    parent_table: str
    parent_key: str
    unmatched_rows: int

    @property
    def passed(self) -> bool:
        return self.unmatched_rows == 0

    def as_dict(self) -> dict[str, object]:
        return asdict(self) | {"passed": self.passed}


def validate_foreign_keys(tables: dict[str, DataFrame]) -> tuple[RelationshipCheck, ...]:
    checks: list[RelationshipCheck] = []
    for child_table, child_key, parent_table, parent_key in FOREIGN_KEYS:
        child = tables[child_table].where(F.col(child_key).isNotNull()).alias("child")
        parent = tables[parent_table].select(F.col(parent_key).alias("__parent_key")).dropDuplicates().alias("parent")
        unmatched = child.join(
            parent,
            F.col(f"child.{child_key}").cast("string") == F.col("parent.__parent_key").cast("string"),
            "left_anti",
        ).count()
        checks.append(RelationshipCheck(child_table, child_key, parent_table, parent_key, int(unmatched)))
    return tuple(checks)


def _join(left: DataFrame, left_key: str, right: DataFrame, right_key: str, right_name: str) -> DataFrame:
    left_alias = left.alias("left_table")
    right_alias = right.alias("right_table")
    condition = F.col(f"left_table.{left_key}").cast("string") == F.col(f"right_table.{right_key}").cast("string")
    right_fields = [
        F.col(f"right_table.{column}").alias(f"{right_name}__{column}")
        for column in right.columns
        if column != right_key and not column.startswith("__invalid_cast__")
    ]
    left_fields = [F.col(f"left_table.{column}") for column in left.columns if not column.startswith("__invalid_cast__")]
    return left_alias.join(right_alias, condition, "left").select(*left_fields, *right_fields)


def build_required_joins(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Build each required relation independently; avoids a row-multiplying mega-join."""
    result: dict[str, DataFrame] = {}
    for left_table, left_key, right_table, right_key in RELATIONSHIPS:
        name = f"{left_table}__{right_table}"
        result[name] = _join(
            tables[left_table], left_key, tables[right_table], right_key, right_table
        )
    return result


def sql_aggregate(spark, frame: DataFrame, view_name: str, query: str) -> DataFrame:
    """Register a temporary view and run a caller-supplied Spark SQL query."""
    frame.createOrReplaceTempView(view_name)
    return spark.sql(query)


def build_sql_aggregates(spark, frame: DataFrame, view_name: str = "data_foundation_order_line_join") -> dict[str, DataFrame]:
    """Run a deterministic catalog of business aggregates through Spark SQL."""
    frame.createOrReplaceTempView(view_name)
    queries = {
        "location": f"""SELECT Location_ID,
            SUM(COALESCE(`order_items__Line_Total`, 0)) AS revenue,
            COUNT(DISTINCT Order_ID) AS order_count,
            SUM(COALESCE(`order_items__Quantity`, 0)) AS units_sold
          FROM {view_name} GROUP BY Location_ID""",
        "channel": f"""SELECT Channel_ID,
            SUM(COALESCE(`order_items__Line_Total`, 0)) AS revenue,
            COUNT(DISTINCT Order_ID) AS order_count
          FROM {view_name} GROUP BY Channel_ID""",
        "weekday": f"""SELECT date_format(Order_Date, 'EEEE') AS weekday,
            SUM(COALESCE(`order_items__Line_Total`, 0)) AS revenue,
            COUNT(DISTINCT Order_ID) AS order_count
          FROM {view_name} GROUP BY date_format(Order_Date, 'EEEE')""",
    }
    return {name: spark.sql(query) for name, query in queries.items()}
