"""SRS report catalog, validation, and safe CSV generation."""
from __future__ import annotations

import io
import re

import pandas as pd


REPORT_REQUIREMENTS = {
    "Menu performance": "FR-EX-86",
    "Profitability": "FR-EX-86",
    "Customer segmentation": "FR-EX-86",
    "Market-basket analysis": "FR-EX-86",
    "Demand forecast": "FR-EX-86",
    "Wastage": "FR-EX-86",
    "Promotions": "FR-EX-86",
    "Pricing": "FR-EX-86",
    "Location performance": "FR-EX-86",
    "Anomalies": "FR-EX-86",
    "Recommendations": "FR-EX-86",
    "Spark-vs-Python comparison": "FR-EX-86",
}

# Keep display labels separate from the stable audit/database identifiers.  In
# particular, the SRS label contains "vs" while the persisted operation key is
# intentionally ``spark_python_comparison``.
REPORT_EXPORT_KEYS = {
    "Menu performance": "menu_performance",
    "Profitability": "profitability",
    "Customer segmentation": "customer_segmentation",
    "Market-basket analysis": "market_basket",
    "Demand forecast": "demand_forecast",
    "Wastage": "wastage",
    "Promotions": "promotions",
    "Pricing": "pricing",
    "Location performance": "location_performance",
    "Anomalies": "anomalies",
    "Recommendations": "recommendations",
    "Spark-vs-Python comparison": "spark_python_comparison",
}


REPORT_INPUTS = {
    "Menu performance": "menu",
    "Profitability": "menu",
    "Customer segmentation": "customers",
    "Market-basket analysis": "market_basket",
    "Demand forecast": "forecast",
    "Wastage": "wastage_records",
    "Promotions": "promotion_traps",
    "Pricing": "price_sensitivity",
    "Location performance": "location",
    "Anomalies": "anomalies",
    "Recommendations": "recommendations",
    "Spark-vs-Python comparison": "dual_rows",
}


# Each group is an evidence check, not a fabricated schema. A populated report
# should contain at least one column from every group listed for its category.
REPORT_COLUMN_GROUPS: dict[str, tuple[tuple[str, ...], ...]] = {
    "Menu performance": (("Item_ID", "item_id"), ("performance_class", "Performance_Class")),
    "Profitability": (("Item_ID", "item_id"), ("item_revenue", "contribution_margin", "profit_percentage")),
    "Customer segmentation": (("Customer_ID", "customer_id"), ("customer_segment", "Customer_Segment")),
    "Market-basket analysis": (("antecedent_item_id", "antecedent", "antecedents", "items", "itemset"), ("consequent_item_id", "consequent", "consequents", "rule")),
    "Demand forecast": (("Date", "date"), ("forecast_quantity", "prediction", "actual")),
    "Wastage": (("Wastage_ID", "wastage_id", "Item_ID"), ("Quantity_Wasted", "quantity_wasted"), ("Total_Wastage_Cost", "wastage_cost")),
    "Promotions": (("promotion_id", "Promotion_ID", "promotion_flag", "promotion_sales_up_profit_down"),),
    "Pricing": (("Item_ID", "item_id"), ("price_sensitivity_class", "observed_elasticity", "elasticity")),
    "Location performance": (("Location_ID", "location_id"), ("revenue", "order_count", "contribution_margin")),
    "Anomalies": (("anomaly_type", "Anomaly_Type"),),
    "Recommendations": (("recommendation_type", "Recommendation_Type"), ("priority", "Priority")),
    "Spark-vs-Python comparison": (("spark_prediction",), ("python_prediction",), ("match_status", "agreement")),
}


def report_export_key(report_name: str) -> str:
    """Return the stable database/audit key for a display report name."""
    if report_name in REPORT_EXPORT_KEYS:
        return REPORT_EXPORT_KEYS[report_name]
    return re.sub(r"[^a-z0-9]+", "_", report_name.casefold()).strip("_")


def report_filename(report_name: str) -> str:
    """Return a stable, meaningful CSV filename for a report category."""
    return f"dineiq_{report_export_key(report_name)}.csv"


def _first_available(frame: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def validate_report_frame(report_name: str, frame: pd.DataFrame) -> dict[str, object]:
    """Validate a generated report's evidence columns and row count."""
    if report_name not in REPORT_REQUIREMENTS:
        raise KeyError(f"Unknown SRS report: {report_name}")
    groups = REPORT_COLUMN_GROUPS[report_name]
    missing = [group for group in groups if _first_available(frame, group) is None]
    return {
        "report": report_name,
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "missing_column_groups": [list(group) for group in missing],
        "passed": bool(frame.empty or not missing),
    }


def _formula_safe(frame: pd.DataFrame) -> pd.DataFrame:
    """Neutralize spreadsheet formula prefixes in text cells without changing numerics."""
    result = frame.copy()
    dangerous = ("=", "+", "-", "@")
    for column in result.select_dtypes(include=["object", "string"]).columns:
        result[column] = result[column].map(
            lambda value: ("'" + value) if isinstance(value, str) and value.startswith(dangerous) else value
        )
    return result


def serialize_report_csv(frame: pd.DataFrame) -> bytes:
    """Serialize a report as UTF-8 BOM CSV suitable for spreadsheet clients."""
    return _formula_safe(frame).to_csv(index=False).encode("utf-8-sig")


def validate_report_csv(payload: bytes, expected: pd.DataFrame) -> dict[str, object]:
    """Read a generated CSV back and verify columns and row count."""
    try:
        parsed = pd.read_csv(io.BytesIO(payload))
    except pd.errors.EmptyDataError:
        # pandas treats a header-only CSV as empty input.  Preserve the schema
        # check so an intentionally empty filtered report remains verifiable.
        try:
            header = payload.decode("utf-8-sig").splitlines()[0].split(",")
        except (UnicodeError, IndexError):
            return {"passed": False, "error": "EmptyDataError", "rows": 0, "columns": []}
        return {
            "passed": expected.empty and header == list(expected.columns),
            "rows": 0,
            "columns": header,
        }
    except (OSError, UnicodeError, ValueError, pd.errors.ParserError) as exc:
        return {"passed": False, "error": type(exc).__name__, "rows": 0, "columns": []}
    return {
        "passed": list(parsed.columns) == list(expected.columns) and len(parsed) == len(expected),
        "rows": int(len(parsed)),
        "columns": list(parsed.columns),
    }


def build_report_frames(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Return the twelve SRS report datasets without mutating source frames."""
    result = {
        label: data.get(key, pd.DataFrame()).copy()
        for label, key in REPORT_INPUTS.items()
        if key != "anomalies"
    }
    result["Anomalies"] = pd.concat(
        [data.get("sales_anomalies", pd.DataFrame()), data.get("rating_anomalies", pd.DataFrame())],
        ignore_index=True,
    )
    return {label: result.get(label, pd.DataFrame()) for label in REPORT_REQUIREMENTS}



def available_date_bounds(*frames: pd.DataFrame):
    """Return a date range covering the history and forecast frames."""
    candidates = []
    for frame in frames:
        column = next((name for name in ("Order_Date", "Date", "date", "order_date")
                       if name in frame.columns), None)
        if column is None or frame.empty:
            continue
        parsed = pd.to_datetime(frame[column], errors="coerce").dropna()
        if not parsed.empty:
            candidates.extend((parsed.min(), parsed.max()))
    if not candidates:
        return None
    return min(candidates).date(), max(candidates).date()
