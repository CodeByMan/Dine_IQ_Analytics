"""Pure, data-backed metrics used by the Streamlit dashboard pages.

The UI deliberately keeps these calculations outside Streamlit callbacks.  This
allows dashboard values to be tested against small fixtures and makes the
lineage explicit: each metric is derived from the already filtered pipeline
artifact supplied by ``ui.app``.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def _sum(frame: pd.DataFrame, *columns: str) -> float:
    for column in columns:
        values = _numeric(frame, column)
        if not values.empty:
            return float(values.sum())
    return 0.0


def _mean(frame: pd.DataFrame, *columns: str) -> float:
    for column in columns:
        values = _numeric(frame, column)
        if not values.empty:
            return float(values.mean())
    return 0.0


def _count(frame: pd.DataFrame, *columns: str) -> int:
    for column in columns:
        if column in frame:
            values = frame[column].dropna()
            return int(values.nunique())
    return int(len(frame))


def _contains_count(frame: pd.DataFrame, column: str, text: str) -> int:
    if column not in frame:
        return 0
    return int(frame[column].fillna("").astype(str).str.contains(text, case=False, regex=False).sum())


def executive_kpis(
    menu: pd.DataFrame,
    location: pd.DataFrame,
    customers: pd.DataFrame,
    wastage: pd.DataFrame,
    forecast: pd.DataFrame,
    recommendations: pd.DataFrame,
    anomalies: pd.DataFrame,
) -> dict[str, float | int]:
    """Return the ten executive dashboard KPIs from filtered artifacts."""
    return {
        "total_revenue": _sum(menu, "item_revenue") or _sum(location, "revenue"),
        "total_profit": _sum(menu, "contribution_margin") or _sum(location, "contribution_margin"),
        "total_orders": _sum(location, "completed_orders", "order_count"),
        "average_order_value": _mean(location, "average_order_value"),
        "active_customers": _count(customers, "Customer_ID"),
        "repeat_customers": int(
            customers.get("repeat_behavior", pd.Series(dtype=str)).fillna("").astype(str).eq("Repeat").sum()
        ),
        "wastage": _sum(wastage, "wastage_cost", "Total_Wastage_Cost"),
        "forecast_demand": _sum(forecast, "forecast_quantity"),
        "critical_recommendations": int(
            recommendations.get("priority", pd.Series(dtype=str)).fillna("").astype(str).eq("Critical").sum()
        ),
        "anomalies": int(len(anomalies)),
    }


def menu_kpis(menu: pd.DataFrame) -> dict[str, float | int]:
    """Return named menu intelligence counts and measures."""
    classes = menu.get("performance_class", pd.Series(dtype=str)).fillna("").astype(str)
    return {
        "items": int(len(menu)),
        "profit_drivers": int(classes.eq("Profit Driver").sum()),
        "volume_drivers": int(classes.eq("Volume Driver").sum()),
        "hidden_opportunities": int(classes.eq("Hidden Opportunity").sum()),
        "low_performers": int(classes.eq("Low Performer").sum()),
        "slow_movers": int((_numeric(menu, "order_frequency") <= 1).sum()),
        "average_rating": _mean(menu, "average_rating"),
        "contribution_margin": _sum(menu, "contribution_margin"),
        "wastage_cost": _sum(menu, "wastage_cost", "historical_wastage_cost"),
    }


def customer_kpis(customers: pd.DataFrame, rfm: pd.DataFrame | None = None) -> dict[str, float | int]:
    """Return segment, RFM, value, risk, sensitivity and trend measures."""
    segments = customers.get("customer_segment", pd.Series(dtype=str)).fillna("").astype(str)
    return {
        "customers": int(len(customers)),
        "high_value": _contains_count(customers, "customer_segment", "High-Value"),
        "at_risk": _contains_count(customers, "customer_segment", "At-Risk"),
        "promotion_sensitive": _contains_count(customers, "customer_segment", "Promotion-Driven"),
        "repeat": int(customers.get("repeat_behavior", pd.Series(dtype=str)).fillna("").astype(str).eq("Repeat").sum()),
        "average_monetary_value": _mean(rfm if rfm is not None else customers, "monetary_value"),
        "average_frequency": _mean(rfm if rfm is not None else customers, "frequency"),
        "segment_count": int(segments.nunique()),
    }


def wastage_kpis(wastage: pd.DataFrame, risk: pd.DataFrame) -> dict[str, float | int]:
    """Return wastage totals and model-risk counts from filtered artifacts."""
    high_risk = risk.get("high_wastage_risk", pd.Series(dtype=bool)).fillna(False).astype(bool)
    items = risk.loc[high_risk, "Item_ID"].nunique() if high_risk.any() and "Item_ID" in risk else 0
    locations = risk.loc[high_risk, "Location_ID"].nunique() if high_risk.any() and "Location_ID" in risk else 0
    probability = _numeric(risk, "model_risk_probability")
    return {
        "quantity": _sum(wastage, "quantity_wasted", "wasted_quantity", "Quantity_Wasted"),
        "cost": _sum(wastage, "wastage_cost", "Total_Wastage_Cost"),
        "high_risk_rows": int(high_risk.sum()),
        "high_risk_items": int(items),
        "high_risk_locations": int(locations),
        "mean_model_probability": float(probability.mean()) if not probability.empty else 0.0,
    }


def forecast_kpis(forecast: pd.DataFrame, evaluation: pd.DataFrame) -> dict[str, float | int]:
    """Return forecast demand, high-risk periods, and evaluation measures."""
    quantity = _numeric(forecast, "forecast_quantity")
    high_cutoff = float(quantity.quantile(0.9)) if not quantity.empty else 0.0
    high_risk = int((quantity >= high_cutoff).sum()) if not quantity.empty else 0
    metrics = {
        str(row.get("metric")): float(row.get("value"))
        for row in evaluation.to_dict(orient="records")
        if row.get("metric") is not None and pd.notna(row.get("value"))
    }
    return {"forecast_demand": float(quantity.sum()) if not quantity.empty else 0.0,
            "high_risk_periods": high_risk, "mae": metrics.get("MAE", 0.0),
            "rmse": metrics.get("RMSE", 0.0), "mape": metrics.get("MAPE_nonzero_actuals", 0.0),
            "r2": metrics.get("R2", 0.0)}


def dual_kpis(summary: dict[str, Any] | None, comparison: pd.DataFrame) -> dict[str, float | int]:
    """Return comparison KPIs, preferring complete build-report totals."""
    summary = summary or {}
    agreement = pd.to_numeric(comparison.get("agreement", pd.Series(dtype=float)), errors="coerce")
    differences = _numeric(comparison, "numerical_difference")
    return {
        "agreement_percentage": float(summary.get("agreement_percentage", agreement.mean() * 100 if not agreement.empty else 0.0)),
        "disagreement_count": int(summary.get("disagreement_count", (~agreement.astype(bool)).sum() if not agreement.empty else 0)),
        "comparison_records": int(summary.get("comparison_records", len(comparison))),
        "mean_numerical_difference": float(differences.abs().mean()) if not differences.empty else 0.0,
    }


def grouped_sum(frame: pd.DataFrame, dimension: str, measure: str, *, limit: int = 20) -> pd.DataFrame:
    """Build a deterministic chart table and never invent missing values."""
    if frame.empty or dimension not in frame or measure not in frame:
        return pd.DataFrame(columns=[dimension, measure])
    result = frame[[dimension, measure]].copy()
    result[measure] = pd.to_numeric(result[measure], errors="coerce")
    result = result.dropna(subset=[measure])
    if result.empty:
        return pd.DataFrame(columns=[dimension, measure])
    result[dimension] = result[dimension].fillna("Unknown").astype(str)
    return (result.groupby(dimension, as_index=False, dropna=False)[measure].sum()
            .sort_values(measure, ascending=False).head(limit).reset_index(drop=True))


def grouped_mean(frame: pd.DataFrame, dimension: str, measure: str, *, limit: int = 20) -> pd.DataFrame:
    if frame.empty or dimension not in frame or measure not in frame:
        return pd.DataFrame(columns=[dimension, measure])
    result = frame[[dimension, measure]].copy()
    result[measure] = pd.to_numeric(result[measure], errors="coerce")
    result = result.dropna(subset=[measure])
    if result.empty:
        return pd.DataFrame(columns=[dimension, measure])
    result[dimension] = result[dimension].fillna("Unknown").astype(str)
    return (result.groupby(dimension, as_index=False, dropna=False)[measure].mean()
            .sort_values(measure, ascending=False).head(limit).reset_index(drop=True))


def grouped_count(frame: pd.DataFrame, dimension: str, *, limit: int = 20) -> pd.DataFrame:
    """Count non-null records by a categorical dimension for charting."""
    if frame.empty or dimension not in frame:
        return pd.DataFrame(columns=[dimension, "count"])
    values = frame[dimension].fillna("Unknown").astype(str)
    return (values.value_counts().rename_axis(dimension).reset_index(name="count")
            .head(limit).reset_index(drop=True))


def dated_sum(frame: pd.DataFrame, date_column: str, measure: str) -> pd.DataFrame:
    if frame.empty or date_column not in frame or measure not in frame:
        return pd.DataFrame(columns=["date", measure])
    result = frame[[date_column, measure]].copy()
    result["date"] = pd.to_datetime(result[date_column], errors="coerce")
    result[measure] = pd.to_numeric(result[measure], errors="coerce")
    result = result.dropna(subset=["date", measure])
    if result.empty:
        return pd.DataFrame(columns=["date", measure])
    return result.groupby("date", as_index=False)[measure].sum().sort_values("date")
