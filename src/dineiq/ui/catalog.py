"""Canonical mapping from UI data keys to generated project artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dineiq.config import Settings


OUTPUT_PATHS: dict[str, Path] = {
    "menu": Path("descriptive_analytics/menu_profitability_classification"),
    "features": Path("data_foundation/menu_features"),
    "customers": Path("descriptive_analytics/customer_segments"),
    "rfm": Path("descriptive_analytics/customer_rfm"),
    "wastage": Path("descriptive_analytics/wastage_by_item"),
    "wastage_monthly": Path("descriptive_analytics/wastage_monthly_trend"),
    "wastage_category": Path("descriptive_analytics/wastage_by_category"),
    "wastage_day": Path("descriptive_analytics/wastage_by_day"),
    "wastage_demand": Path("descriptive_analytics/wastage_by_demand"),
    "wastage_inventory": Path("descriptive_analytics/wastage_by_inventory"),
    "wastage_promotion": Path("descriptive_analytics/wastage_by_promotion"),
    "waste_risk": Path("demand_planning/wastage_risk_prediction"),
    "forecast": Path("demand_planning/forecast_item_location_daily"),
    "forecast_eval": Path("demand_planning/forecast_evaluation"),
    "location": Path("operational_intelligence/location_comparison"),
    "channel": Path("operational_intelligence/channel_summary"),
    "ratings": Path("descriptive_analytics/item_rating_performance"),
    "recommendations": Path("operational_intelligence/recommendations"),
    "sales_anomalies": Path("operational_intelligence/sales_anomalies"),
    "rating_anomalies": Path("operational_intelligence/rating_anomalies"),
    "price_sensitivity": Path("demand_planning/price_sensitivity"),
    "promotion_traps": Path("operational_intelligence/promotion_trap_detection"),
    "dual_rows": Path("demand_models/dual_pipeline_comparison"),
    "market_basket": Path("demand_planning/association_rules"),
}

REPORT_PATHS: dict[str, Path] = {
    "data_foundation": Path("data_foundation/build_summary.json"),
    "descriptive_analytics": Path("descriptive_analytics/build_summary.json"),
    "demand_planning": Path("demand_planning/build_summary.json"),
    "operational_intelligence": Path("operational_intelligence/build_summary.json"),
    "demand_models": Path("demand_models/build_summary.json"),
}


def output_path(settings: Settings, key: str) -> Path:
    """Return the canonical artifact directory for a known UI data key."""
    try:
        relative = OUTPUT_PATHS[key]
    except KeyError as exc:
        raise KeyError(f"Unknown DineIQ UI output key: {key}") from exc
    return settings.artifacts_root / relative


def report_path(settings: Settings, key: str) -> Path:
    """Return the canonical build-summary path for a known pipeline."""
    try:
        relative = REPORT_PATHS[key]
    except KeyError as exc:
        raise KeyError(f"Unknown DineIQ pipeline report key: {key}") from exc
    return settings.reports_root / relative


def load_report(settings: Settings, key: str) -> dict[str, Any]:
    """Load a retained build summary, returning an explicit unavailable state."""
    path = report_path(settings, key)
    if not path.is_file():
        return {"passed": False, "available": False, "pipeline": key, "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"passed": False, "available": False, "pipeline": key,
                "path": str(path), "error": type(exc).__name__}
    payload.setdefault("available", True)
    payload.setdefault("pipeline", key)
    payload.setdefault("path", str(path))
    return payload


def available_output_keys(settings: Settings) -> tuple[str, ...]:
    """Return output keys whose Parquet directory currently exists."""
    return tuple(key for key in OUTPUT_PATHS if output_path(settings, key).is_dir())
