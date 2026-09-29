"""Bounded-memory loading and SRS filter handling for dashboard artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

# Keep the module importable when the optional Parquet runtime is unavailable.
# The application can then render its normal, actionable load-error state
# instead of failing during Python module import.  Tests and production may
# still patch/use this module-level name directly.
try:  # pragma: no cover - availability depends on the execution environment
    import pyarrow.dataset as ds
except ImportError:  # pragma: no cover - exercised by dependency-failure smoke
    ds = None


FILTER_LABELS = (
    "Date range", "Location", "Menu item", "Menu category", "Customer segment",
    "Ordering channel", "Promotion", "Performance class", "Price range", "Rating", "Wastage range",
)

DASHBOARD_LABELS = (
    "Executive", "Menu", "Customers", "Wastage", "Forecast", "Spark vs Python",
)

RECOMMENDATION_REQUIREMENT_TYPES = {
    "FR-EX-48": {
        "Promote high-margin Hidden Opportunities",
        "Review pricing of price-sensitive dishes",
        "Bundle frequently purchased items",
        "Remove or redesign persistent Low Performers",
    },
    "FR-EX-49": {
        "Reduce preparation of high-wastage dishes",
        "Increase stock before predicted peak periods",
    },
    "FR-EX-50": {"Target selected customer segments"},
}

DASHBOARD_REQUIRED_CONTENT = {
    "FR-EX-89 Executive": ("total revenue", "total profit", "total orders", "average order value",
        "active customers", "repeat customers", "wastage", "forecast demand", "critical recommendations", "anomalies"),
    "FR-EX-89 Menu": ("menu-item performance", "Profit Drivers", "Volume Drivers", "Hidden Opportunities",
        "Low Performers", "slow-moving items", "ratings", "margins", "wastage"),
    "FR-EX-89 Customer": ("customer segments", "RFM distribution", "high-value customers",
        "at-risk customers", "promotion-sensitive customers", "customer trends"),
    "FR-EX-89 Wastage": ("total wastage", "wastage cost", "high-wastage items", "high-wastage locations",
        "wastage trends", "wastage-risk predictions"),
    "FR-EX-89 Forecast": ("historical demand", "forecast demand", "actual versus predicted",
        "forecast errors", "high-risk demand periods"),
    "FR-EX-89 Dual-Pipeline": ("Spark prediction", "Python prediction", "match status",
        "numerical difference", "agreement percentage", "disagreement count"),
}

FIELD_ALIASES = {
    "location": ("Location_ID", "location_id", "Location", "location"),
    "item": ("Item_ID", "item_id"),
    "category": ("Category_ID", "category_id"),
    "segment": ("customer_segment", "Customer_Segment"),
    "channel": ("Channel_ID", "channel_id", "preferred_channel_id"),
    "promotion": ("Promotion_ID", "promotion_id"),
    "performance": ("performance_class", "Performance_Class"),
    "price": ("Selling_Price", "selling_price", "unit_price", "New_Price", "previous_price"),
    "rating": ("average_rating", "average_customer_rating", "Rating"),
    "wastage": ("wastage_percentage", "historical_wastage_percentage", "Wastage_Percentage"),
    "dates": ("Date", "date", "Order_Date", "order_date", "Review_Date", "Wastage_Date"),
}


def read_parquet(path: Path, *, columns: list[str] | None = None, max_rows: int | None = None) -> pd.DataFrame:
    """Read a Spark Parquet directory, optionally stopping after a row limit."""
    if not path.is_dir():
        return pd.DataFrame()
    global ds
    if ds is None:
        try:
            import pyarrow.dataset as pyarrow_dataset
        except ImportError as exc:
            raise RuntimeError(
                "Parquet outputs are available, but PyArrow is not installed. "
                "Install the project requirements before loading analytics."
            ) from exc
        ds = pyarrow_dataset
    dataset = ds.dataset(str(path), format="parquet")
    if columns is not None:
        columns = [name for name in columns if name in dataset.schema.names]
        if not columns:
            return pd.DataFrame()
    scanner = dataset.scanner(columns=columns, batch_size=16_384)
    if max_rows is None:
        return scanner.to_table().to_pandas()
    frames: list[pd.DataFrame] = []
    remaining = max_rows
    for batch in scanner.to_batches():
        frame = batch.to_pandas()
        frames.append(frame.iloc[:remaining])
        remaining -= min(len(frame), remaining)
        if remaining <= 0:
            break
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)


def read_csv(path: Path, *, usecols: list[str] | None = None) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path, usecols=usecols, low_memory=False)


def _series_name(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    return next((name for name in aliases if name in frame.columns), None)


def apply_filters(frame: pd.DataFrame, selected: dict[str, Any]) -> pd.DataFrame:
    """Apply selected SRS filter dimensions where a view contains that field."""
    result = frame
    search = selected.get("search")
    if search and not result.empty:
        searchable = result.astype(str).apply(lambda column: column.str.contains(str(search), case=False, regex=False))
        result = result.loc[searchable.any(axis=1)]
    for key, aliases in FIELD_ALIASES.items():
        values = selected.get(key)
        column = _series_name(result, aliases)
        if column is None or values is None:
            continue
        if isinstance(values, str) and not values:
            continue
        if isinstance(values, (list, tuple, set)) and not values:
            continue
        if key == "dates" and len(values) == 2:
            dates = pd.to_datetime(result[column], errors="coerce").dt.date
            result = result.loc[(dates >= values[0]) & (dates <= values[1])]
        elif key == "price" and len(values) == 2:
            numbers = pd.to_numeric(result[column], errors="coerce")
            result = result.loc[(numbers >= values[0]) & (numbers <= values[1])]
        elif key == "rating" and len(values) == 2:
            numbers = pd.to_numeric(result[column], errors="coerce")
            result = result.loc[(numbers >= values[0]) & (numbers <= values[1])]
        elif key == "wastage" and len(values) == 2:
            numbers = pd.to_numeric(result[column], errors="coerce")
            result = result.loc[(numbers >= values[0]) & (numbers <= values[1])]
        elif isinstance(values, (list, tuple, set)):
            result = result.loc[result[column].astype(str).isin({str(value) for value in values})]
    return result


def high_value_customer_count(customers: pd.DataFrame) -> int:
    """Count segments whose documented label denotes high-value loyal customers."""
    if "customer_segment" not in customers:
        return 0
    labels = customers["customer_segment"].fillna("").astype(str).str.casefold()
    return int(labels.str.contains("high-value loyal", regex=False).sum())


def repeat_customer_count(customers: pd.DataFrame) -> int:
    """Count customer rows explicitly classified as repeat purchasers."""
    if "repeat_behavior" not in customers:
        return 0
    return int(customers["repeat_behavior"].fillna("").astype(str).eq("Repeat").sum())


def recency_bucket_counts(customers: pd.DataFrame, *, bins: int = 10) -> pd.Series:
    """Return chart-safe integer bucket counts for customer recency days."""
    if "recency_days" not in customers:
        return pd.Series(dtype="int64", name="Customers")
    recency = pd.to_numeric(customers["recency_days"], errors="coerce").dropna()
    if recency.empty:
        return pd.Series(dtype="int64", name="Customers")
    bucket_ids = pd.cut(recency, bins=bins, labels=False, include_lowest=True)
    counts = bucket_ids.value_counts().sort_index()
    counts.index = counts.index.astype(int)
    counts.index.name = "Recency bucket"
    counts.name = "Customers"
    return counts


def dataframe_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Serialize UI data rows safely for Streamlit JSON/chart APIs."""
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def missing_recommendation_requirement_types(frame: pd.DataFrame) -> dict[str, list[str]]:
    """Return absent evidence-linked recommendation families by SRS requirement."""
    found = set(frame.get("recommendation_type", pd.Series(dtype=str)).dropna().astype(str))
    return {requirement: sorted(types - found)
            for requirement, types in RECOMMENDATION_REQUIREMENT_TYPES.items()
            if types - found}


def build_scenario_baselines(
    menu_items: pd.DataFrame,
    menu_features: pd.DataFrame,
    forecast: pd.DataFrame,
    wastage_risk: pd.DataFrame,
    price_sensitivity: pd.DataFrame,
) -> pd.DataFrame:
    """Join verified model inputs into one estimated item/location context."""
    required_menu = {"Item_ID", "Selling_Price", "Ingredient_Cost"}
    if not required_menu.issubset(menu_items.columns):
        return pd.DataFrame()
    if not {"Item_ID", "Location_ID", "forecast_quantity"}.issubset(forecast.columns):
        return pd.DataFrame()
    result = menu_items.copy()
    result["Item_ID"] = result["Item_ID"].astype(str)
    if {"Item_ID", "discount_percentage"}.issubset(menu_features.columns):
        features = menu_features.copy()
        features["Item_ID"] = features["Item_ID"].astype(str)
        result = result.merge(features[["Item_ID", "discount_percentage"]], on="Item_ID", how="left")
    forecast = forecast.copy()
    forecast["Item_ID"] = forecast["Item_ID"].astype(str)
    forecast["Location_ID"] = forecast["Location_ID"].astype(str)
    demand = forecast.groupby(["Item_ID", "Location_ID"], as_index=False)["forecast_quantity"].sum()
    # Forecast demand is item/location grain.  Merge on both keys so a
    # location-specific what-if context cannot silently receive another
    # location's forecast.
    result = demand.merge(result, on="Item_ID", how="inner")
    risk_columns = {"Item_ID", "Location_ID", "historical_wastage_percentage",
                    "historical_wastage_cost", "historical_quantity_wasted"}
    if risk_columns.issubset(wastage_risk.columns):
        risks = wastage_risk[list(risk_columns)].copy()
        risks["Item_ID"] = risks["Item_ID"].astype(str)
        risks["Location_ID"] = risks["Location_ID"].astype(str)
        result = result.merge(risks, on=["Item_ID", "Location_ID"], how="left")
    else:
        result["historical_wastage_percentage"] = 0.0
        result["historical_wastage_cost"] = 0.0
        result["historical_quantity_wasted"] = 0.0
    if {"Item_ID", "observed_elasticity"}.issubset(price_sensitivity.columns):
        sensitivity = price_sensitivity.copy()
        sensitivity["Item_ID"] = sensitivity["Item_ID"].astype(str)
        sensitivity = sensitivity.groupby("Item_ID", as_index=False)["observed_elasticity"].mean()
        result = result.merge(sensitivity, on="Item_ID", how="left")
    else:
        result["observed_elasticity"] = pd.NA
    numeric = ("Selling_Price", "Ingredient_Cost", "forecast_quantity", "historical_wastage_percentage",
               "historical_wastage_cost", "historical_quantity_wasted", "discount_percentage",
               "observed_elasticity")
    for column in numeric:
        if column in result:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    result["scenario_wastage_rate"] = (result["historical_wastage_percentage"].fillna(0).clip(0, 100) / 100)
    result["scenario_wastage_unit_cost"] = (
        result["historical_wastage_cost"].fillna(0)
        / result["historical_quantity_wasted"].replace(0, pd.NA)
    ).fillna(0)
    result["scenario_preparation_quantity"] = result["forecast_quantity"].fillna(0) / (
        1 - result["scenario_wastage_rate"].clip(upper=0.99)
    )
    return result
