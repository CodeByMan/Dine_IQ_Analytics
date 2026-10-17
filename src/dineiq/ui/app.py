from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st

from dineiq.analytics.model_serving import DemandRequest, DualDemandPredictor
from dineiq.analytics.scenario_analysis import SCENARIOS, ScenarioBaseline, simulate_scenario
from dineiq.config import Settings, load_settings
from dineiq.dataset.catalog import require_dataset1b_headers
from dineiq.dataset.ingestion_audit import resolve_source_paths
from dineiq.db.schema import initialize_database
from dineiq.db.operations import audit_event, recent_audit_events, recent_jobs, record_export as record_report_export
from dineiq.db.permissions import allowed
from dineiq.ui.auth import clear_login_session, login_gate
from dineiq.ui.catalog import load_report, output_path
from dineiq.ui.components import render_error, render_frame, safe_number
from dineiq.ui.filter_state import FILTER_KEYS, PRICE_RANGE, clear_filter_state
from dineiq.ui.presentation import enrich_names, display_frame
from dineiq.ui.charts import (render_kpi_overview, render_menu_ranking, render_demand_forecast,
    render_channel_sales, render_customer_segments, render_price_demand, render_ordering_activity)
from dineiq.ui.chart_data import completed_order_view
from dineiq.ui.management import render_management
from dineiq.ui.reports import (available_date_bounds, build_report_frames,
    report_export_key, report_filename, serialize_report_csv,
    validate_report_csv, validate_report_frame)
from dineiq.ui.data import (DASHBOARD_LABELS, FILTER_LABELS, apply_filters,
    build_scenario_baselines, high_value_customer_count,
    missing_recommendation_requirement_types, read_csv, read_parquet,
    recency_bucket_counts, repeat_customer_count)
from dineiq.ui.dashboard_metrics import (customer_kpis, dual_kpis, executive_kpis,
    forecast_kpis, menu_kpis, wastage_kpis)
from dineiq.ui.theme import (apply_theme, configure_page, render_brand_header,
                             render_theme_switcher, theme_mode)


@st.cache_data(ttl=60, show_spinner=False)
def _cached_parquet(path: str, limit: int | None = None) -> pd.DataFrame:
    return read_parquet(Path(path), max_rows=limit)


@st.cache_data(ttl=60, show_spinner=False)
def _cached_chart_records(path: str, columns: tuple[str, ...]) -> pd.DataFrame:
    """Project only the fields needed by order-grain charts and semi-joins."""
    return read_parquet(Path(path), columns=list(columns))


@st.cache_data(ttl=60, show_spinner=False)
def _cached_csv(paths: str | tuple[str, ...], usecols: tuple[str, ...] | None = None) -> pd.DataFrame:
    paths = (paths,) if isinstance(paths, str) else paths
    frames = [read_csv(Path(path), usecols=list(usecols) if usecols else None) for path in paths]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@st.cache_resource(show_spinner="Loading persisted Spark and Python models...")
def _load_live_predictor(project_root: str, data_root: str, artifacts_root: str, reports_root: str):
    # Import Spark only when the live prediction workflow is submitted.  This
    # keeps the dashboard bootable in environments that can inspect analytics
    # artifacts but have not installed the Spark runtime yet.
    from dineiq.dataset.pipeline import create_local_spark

    settings = Settings(Path(project_root), Path(data_root), Path(artifacts_root), Path(reports_root))
    spark = create_local_spark(settings, "DineIQ-Live-Dual-Model-Inference")
    return DualDemandPredictor.load(spark, settings)


@st.cache_resource(show_spinner="Loading persisted wastage-risk model...")
def _load_wastage_predictor(model_path: str):
    """Load the saved wastage model used to verify displayed risk predictions."""
    from dineiq.analytics.wastage_risk_model import WastageRiskPredictor

    return WastageRiskPredictor.load(Path(model_path))


def _load_all(settings) -> dict[str, Any]:
    # Do not mix legacy-root source data with Dataset 1B analytical artifacts.
    require_dataset1b_headers(settings.data_root)

    def source_csv(table: str) -> tuple[str, ...]:
        paths = resolve_source_paths(settings.data_root, table)
        return tuple(str(path) for path in paths)

    def p(key: str, limit: int | None = None) -> pd.DataFrame:
        """Load a canonical generated output through the shared artifact catalog."""
        return _cached_parquet(str(output_path(settings, key)), limit)

    order_filter_values = _cached_csv(source_csv("orders"), ("Order_Date", "Channel"))
    if "Channel" in order_filter_values.columns:
        channels_dim = (
            order_filter_values[["Channel"]].dropna().drop_duplicates()
            .rename(columns={"Channel": "Channel_ID"})
        )
        channels_dim["Channel_Name"] = channels_dim["Channel_ID"].astype("string")
    else:
        channels_dim = pd.DataFrame(columns=["Channel_ID", "Channel_Name"])
    locations_dim = _cached_csv(source_csv("restaurants")).rename(columns={
        "Restaurant_ID": "Location_ID",
        "Locality": "Area",
    })
    customers_dim = _cached_csv(source_csv("customers"), ("Customer_ID", "First_Name", "Last_Name"))
    if {"First_Name", "Last_Name"}.issubset(customers_dim):
        customers_dim["Customer_Name"] = (
            customers_dim["First_Name"].fillna("").astype(str).str.strip() + " " +
            customers_dim["Last_Name"].fillna("").astype(str).str.strip()
        ).str.strip().replace("", pd.NA)

    return {
        "menu": p("menu"),
        "features": p("features"),
        "customers": p("customers"),
        "rfm": p("rfm"),
        "wastage": p("wastage"),
        "wastage_records": _cached_csv(source_csv("wastage")),
        "wastage_monthly": p("wastage_monthly"),
        "wastage_category": p("wastage_category"),
        "wastage_day": p("wastage_day"),
        "wastage_demand": p("wastage_demand"),
        "wastage_inventory": p("wastage_inventory"),
        "wastage_promotion": p("wastage_promotion"),
        "waste_risk": p("waste_risk"),
        "forecast": p("forecast"),
        "forecast_eval": p("forecast_eval"),
        "location": p("location"),
        "channel": p("channel"),
        "ratings": p("ratings"),
        "recommendations": p("recommendations"),
        "sales_anomalies": p("sales_anomalies"),
        "rating_anomalies": p("rating_anomalies"),
        "price_sensitivity": p("price_sensitivity"),
        "promotion_traps": p("promotion_traps"),
        "dual_rows": p("dual_rows", 5000),
        "market_basket": p("market_basket"),
        "dual_report": load_report(settings, "demand_models"),
        "orders": order_filter_values,
        "forecast_test": _cached_csv(
            str(settings.splits_dir / "demand_forecast" / "test.csv"),
            ("Date", "Item_ID", "Location_ID", "Target_Quantity"),
        ),
        "items_dim": _cached_csv(source_csv("menu_items")),
        "customers_dim": customers_dim,
        "locations_dim": locations_dim,
        "categories_dim": _cached_csv(source_csv("menu_categories")),
        "channels_dim": channels_dim,
        "promotions_dim": _cached_csv(source_csv("promotions")),
        "pipeline_reports": {
            key: load_report(settings, key)
            for key in ("data_foundation", "descriptive_analytics", "demand_planning",
                        "operational_intelligence", "demand_models")
        },
        "demand_models_status": load_report(settings, "demand_models"),
    }


def _option_values(frame: pd.DataFrame, *columns: str) -> list[str]:
    for column in columns:
        if column in frame:
            return sorted(frame[column].dropna().astype(str).unique().tolist())
    return []


def _named_options(frame: pd.DataFrame, identifier: str, label_columns: tuple[str, ...]) -> tuple[list[str], dict[str, str]]:
    """Return readable labels while keeping duplicate branch names distinguishable."""
    if frame.empty or identifier not in frame.columns:
        return [], {}
    label_column = next((column for column in label_columns if column in frame.columns), None)
    values = frame[identifier].dropna().astype(str).drop_duplicates().sort_values().tolist()
    if label_column is None:
        return values, {value: value for value in values}
    labels_frame = (
        frame[[identifier, label_column]].dropna(subset=[identifier]).astype(str)
        .drop_duplicates(identifier)
    )
    duplicate_names = set(labels_frame.loc[labels_frame[label_column].duplicated(keep=False), label_column])
    labels = labels_frame.set_index(identifier)[label_column].to_dict()
    if identifier == "Location_ID":
        labels = {
            value: f"{label} ({value})" if label in duplicate_names else label
            for value, label in labels.items()
        }
    return values, {value: labels.get(value, value) for value in values}


def _filter_controls(data: dict[str, Any]) -> dict[str, Any]:
    st.sidebar.header("Search and filters")
    st.sidebar.caption("Filters apply to each dashboard wherever the selected field exists.")
    items, locations = data["items_dim"], data["locations_dim"]
    categories, channels, promotions = data["categories_dim"], data["channels_dim"], data["promotions_dim"]
    date_frame = data["orders"]
    date_bounds = available_date_bounds(date_frame, data["forecast"])
    if date_bounds is not None:
        min_date, max_date = date_bounds
        picked = st.sidebar.date_input("Date range", (min_date, max_date), min_value=min_date, max_value=max_date, key=FILTER_KEYS["dates"])
        date_range = tuple(picked) if isinstance(picked, (tuple, list)) and len(picked) == 2 else None
    else:
        st.sidebar.date_input("Date range", disabled=True, help="Order dates are unavailable.", key=FILTER_KEYS["dates"])
        date_range = None
    segment_options = _option_values(data["customers"], "customer_segment")
    class_options = ["Profit Driver", "Volume Driver", "Hidden Opportunity", "Low Performer"]
    price_bounds = PRICE_RANGE
    item_values, item_labels = _named_options(items, "Item_ID", ("Item_Name", "Food_Name", "Dish_Name"))
    branch_labels = ("Branch_Name", "Location_Name", "Area", "Restaurant_Name")
    location_values, location_labels = _named_options(locations, "Location_ID", branch_labels)
    category_values, category_labels = _named_options(categories, "Category_ID", ("Category_Name",))
    channel_values, channel_labels = _named_options(channels, "Channel_ID", ("Channel_Name",))
    promotion_values, promotion_labels = _named_options(promotions, "Promotion_ID", ("Promotion_Name",))
    return {
        "search": st.sidebar.text_input("Search results", placeholder="Search visible records", key=FILTER_KEYS["search"]),
        "dates": date_range,
        "location": st.sidebar.multiselect("Karachi branch", location_values, format_func=lambda value: location_labels.get(value, value), key=FILTER_KEYS["location"]),
        "item": st.sidebar.multiselect("Food / menu item", item_values, format_func=lambda value: item_labels.get(value, value), key=FILTER_KEYS["item"]),
        "category": st.sidebar.multiselect("Food category", category_values, format_func=lambda value: category_labels.get(value, value), key=FILTER_KEYS["category"]),
        "segment": st.sidebar.multiselect("Customer segment", segment_options, key=FILTER_KEYS["segment"]),
        "channel": st.sidebar.multiselect("Ordering channel", channel_values, format_func=lambda value: channel_labels.get(value, value), key=FILTER_KEYS["channel"]),
        "promotion": st.sidebar.multiselect("Promotion", promotion_values, format_func=lambda value: promotion_labels.get(value, value), key=FILTER_KEYS["promotion"]),
        "performance": st.sidebar.multiselect("Performance class", class_options, key=FILTER_KEYS["performance"]),
        "price": st.sidebar.slider("Price range", min_value=price_bounds[0], max_value=price_bounds[1], value=price_bounds, key=FILTER_KEYS["price"]),
        "rating": st.sidebar.slider("Rating range", min_value=0.0, max_value=5.0, value=(0.0, 5.0), key=FILTER_KEYS["rating"]),
        "wastage": st.sidebar.slider("Wastage range (%)", min_value=0.0, max_value=100.0, value=(0.0, 100.0), key=FILTER_KEYS["wastage"]),
    }


def _filtered(data: dict[str, Any], name: str, filters: dict[str, Any]) -> pd.DataFrame:
    frame = data.get(name, pd.DataFrame())
    if frame.empty:
        return frame
    result = enrich_names(frame, data)
    return apply_filters(result, filters)


def _display_frame(frame: pd.DataFrame, *, context: str | None = None) -> pd.DataFrame:
    """Format analytical output for people first, while retaining IDs when names are unavailable."""
    return display_frame(frame, context=context)


def _show_frame(title: str, frame: pd.DataFrame, *, height: int = 360, allow_export: bool = True) -> None:
    display = _display_frame(frame, context=title)
    principal = st.session_state.get("dineiq_principal")
    authorized = bool(principal and allowed(principal.role, "orders", "read"))
    render_frame(title, display, height=height, allow_export=allow_export and authorized)


def _clear_dashboard_filters() -> None:
    clear_filter_state(st.session_state)


def _sum(frame: pd.DataFrame, column: str) -> float:
    return float(pd.to_numeric(frame[column], errors="coerce").fillna(0).sum()) if column in frame else 0.0


def _mean(frame: pd.DataFrame, column: str) -> float:
    values = pd.to_numeric(frame[column], errors="coerce").dropna() if column in frame else pd.Series(dtype=float)
    return float(values.mean()) if not values.empty else 0.0


def _number(value: Any, default: float = 0.0) -> float:
    """Convert nullable artifact values to a finite UI-safe number."""
    return safe_number(value, default)


def _wastage_quantity_cards(waste: pd.DataFrame) -> list[tuple[str, str]]:
    """Keep incompatible wastage units in separate, clearly labelled cards."""
    quantity_column = next(
        (column for column in ("quantity_wasted", "wasted_quantity", "Quantity_Wasted")
         if column in waste),
        None,
    )
    if quantity_column is None or "Unit" not in waste:
        return []
    quantities = pd.to_numeric(waste[quantity_column], errors="coerce").groupby(
        waste["Unit"].fillna("Unspecified unit").astype(str)
    ).sum(min_count=1).dropna().sort_index()
    return [(f"Wasted quantity ({unit})", f"{value:,.2f}")
            for unit, value in quantities.items()]


def _chart_orders(settings, data: dict[str, Any], filters: dict[str, Any]) -> pd.DataFrame:
    """Load cleaned order facts only on pages that need channel/hour visuals."""
    root = settings.artifacts_root / "data_foundation" / "cleaned_parquet"
    orders = _cached_chart_records(str(root / "orders"), (
        "Order_ID", "Customer_ID", "Location_ID", "Restaurant_ID", "Channel_ID", "Channel_Name", "Channel",
        "Promotion_ID", "Order_Date", "Order_DateTime", "Order_Time", "Order_Status", "Status", "Total_Amount"))
    if orders.empty:
        st.info("Completed-order charts need the existing data-foundation cleaned orders artifact.")
        return pd.DataFrame()
    non_search_filters = {**filters, "search": ""}
    menu = _filtered(data, "menu", non_search_filters)
    raw_menu = data.get("menu", pd.DataFrame())
    menu_keys = set(menu.get("Item_ID", pd.Series(dtype=str)).astype(str))
    raw_keys = set(raw_menu.get("Item_ID", pd.Series(dtype=str)).astype(str))
    restrict_items = bool(filters.get("item") or filters.get("category") or filters.get("performance")) or menu_keys != raw_keys
    lines = _cached_chart_records(str(root / "order_items"), ("Order_ID", "Item_ID")) if restrict_items or filters.get("search") else None
    if restrict_items and (lines is None or lines.empty):
        st.info("Item-filtered order charts need the cleaned order-items artifact.")
    customer_selection = _filtered(data, "customers", non_search_filters)
    chart_filters = dict(filters)
    # Interpret searches using observed menu, branch and channel names. Opaque
    # order IDs never become chart labels or arbitrary search categories.
    if filters.get("search"):
        query = str(filters["search"])
        branch_dim = data.get("locations_dim", pd.DataFrame())
        visible = [key for key in ("Restaurant_Name", "Area", "Branch_Name") if key in branch_dim]
        branch_ids = set()
        if visible and "Location_ID" in branch_dim:
            matches = branch_dim[visible].astype(str).apply(lambda column: column.str.contains(query, case=False, regex=False)).any(axis=1)
            branch_ids = set(branch_dim.loc[matches, "Location_ID"].astype(str))
        channel = orders.get("Channel_ID", orders.get("Channel", pd.Series(index=orders.index, dtype=str))).astype(str)
        branches = orders.get("Location_ID", orders.get("Restaurant_ID", pd.Series(index=orders.index, dtype=str))).astype(str)
        order_matches = channel.str.contains(query, case=False, regex=False) | branches.isin(branch_ids)
        search_menu = _filtered(data, "menu", filters)
        search_menu_keys = set(search_menu.get("Item_ID", pd.Series(dtype=str)).astype(str))
        if not search_menu.empty and lines is not None and "Order_ID" in lines and "Item_ID" in lines:
            ids = lines.loc[lines["Item_ID"].astype(str).isin(search_menu_keys), "Order_ID"].astype(str)
            order_matches = order_matches | orders["Order_ID"].astype(str).isin(ids)
        orders = orders.loc[order_matches]
    return completed_order_view(orders, chart_filters, lines=lines, menu=menu,
        customers=customer_selection, item_filter_active=restrict_items)


def _render_insight(text: str) -> None:
    st.caption(f"Insight · {text}")


def _render_chart_gallery(data: dict[str, Any], filters: dict[str, Any], settings) -> None:
    """Seven curated visual sections, each tied to a named business question."""
    st.header("Analytics chart gallery")
    st.caption("Seven selected DineIQ visuals, calculated from the current filtered analytical outputs.")
    orders = _chart_orders(settings, data, filters)
    menu = _filtered(data, "menu", filters)
    location = _filtered(data, "location", filters)
    customers = _filtered(data, "customers", filters)
    forecast = _filtered(data, "forecast", filters)
    recs = _filtered(data, "recommendations", filters)
    anomalies = pd.concat([_filtered(data, "sales_anomalies", filters), _filtered(data, "rating_anomalies", filters)], ignore_index=True)
    kpis = executive_kpis(menu, location, customers, _filtered(data, "wastage", filters), forecast, recs, anomalies)
    render_kpi_overview([
        ("Total revenue", f"{kpis['total_revenue']:,.2f}"), ("Total profit (contribution margin)", f"{kpis['total_profit']:,.2f}"),
        ("Total orders", f"{kpis['total_orders']:,.0f}"), ("Average order value", f"{kpis['average_order_value']:,.2f}")])
    render_menu_ranking(menu)
    render_demand_forecast(_filtered(data, "forecast_test", filters), forecast,
        caption="Historical demand and the saved planning forecast (trailing 7-day mean). Trained Spark/Python predictions are displayed in Spark vs Python.")
    render_channel_sales(orders)
    render_customer_segments(customers)
    render_price_demand(_filtered(data, "price_sensitivity", filters))
    render_ordering_activity(orders)


def _scenario_inputs(data: dict[str, Any]) -> pd.DataFrame:
    return build_scenario_baselines(data["items_dim"], data["features"], data["forecast"],
                                    data["waste_risk"], data["price_sensitivity"])


def _scenario_view(data: dict[str, Any], filters: dict[str, Any]) -> None:
    st.header("What-if scenario analysis")
    st.caption("All scenario outputs are estimates based on the trailing 7-day mean planning forecast and observed historical rates; they are not actual outcomes or causal claims. The what-if formulas do not run the separate Spark/Python ML models.")
    contexts = _scenario_inputs(data)
    if contexts.empty:
        st.warning("Scenario inputs are unavailable. Run the verified demand, price-sensitivity, and wastage builds first.")
        return
    contexts = apply_filters(contexts, filters)
    if contexts.empty:
        st.info("No item/location rows match the current filters.")
        return
    contexts = contexts.sort_values(["Item_ID", "Location_ID"]).reset_index(drop=True)
    keys = contexts[["Item_ID", "Location_ID"]].drop_duplicates().reset_index(drop=True)
    _, item_labels = _named_options(data["items_dim"], "Item_ID", ("Item_Name", "Food_Name", "Dish_Name"))
    _, location_labels = _named_options(data["locations_dim"], "Location_ID", ("Branch_Name", "Location_Name", "Area", "Restaurant_Name"))
    selected_idx = st.selectbox("Item and location", list(keys.index),
        format_func=lambda idx: f"{item_labels.get(str(keys.loc[idx, 'Item_ID']), keys.loc[idx, 'Item_ID'])} · {location_labels.get(str(keys.loc[idx, 'Location_ID']), keys.loc[idx, 'Location_ID'])}")
    selected_key = keys.loc[selected_idx]
    matching = contexts[(contexts["Item_ID"] == selected_key["Item_ID"])
                        & (contexts["Location_ID"] == selected_key["Location_ID"])]
    row = matching.iloc[0]
    scenario_key = st.selectbox("Scenario", list(SCENARIOS), format_func=lambda key: SCENARIOS[key])
    change_pct = None
    discount_pct = None
    response_pct = None
    waste_pct = None
    if scenario_key in {"increase_price", "reduce_price", "preparation_quantity", "increase_demand"}:
        change_pct = st.number_input("Change (%)", min_value=-99.0 if scenario_key != "increase_price" else 0.1,
                                    max_value=300.0, value=10.0, step=1.0)
        if scenario_key in {"increase_price", "reduce_price"} and pd.isna(row.get("observed_elasticity")):
            response_pct = st.number_input("Assumed demand response (%)", value=0.0, step=1.0,
                                           help="No usable historical elasticity is available; enter an explicit assumption.")
    if scenario_key == "change_discount":
        discount_pct = st.number_input("New discount (%)", min_value=0.0, max_value=100.0,
                                       value=min(100.0, max(0.0, _number(row.get("discount_percentage")))), step=1.0)
        if pd.isna(row.get("observed_elasticity")):
            response_pct = st.number_input("Assumed demand response (%)", value=0.0, step=1.0,
                                           help="No usable historical elasticity is available; enter an explicit assumption.")
    if scenario_key == "promotion_frequency":
        response_pct = st.number_input("Assumed demand response (%)", value=10.0, step=1.0,
                                       help="Required scenario assumption; this is not a causal estimate.")
    if scenario_key == "wastage_assumption":
        waste_pct = st.number_input("New wastage assumption (%)", min_value=0.0, max_value=100.0,
                                    value=min(100.0, max(0.0, _number(row.get("historical_wastage_percentage")))), step=1.0)
    if st.button("Estimate scenario impact", type="primary"):
        elasticity_value = row.get("observed_elasticity")
        elasticity = None if pd.isna(elasticity_value) else float(elasticity_value)
        base = ScenarioBaseline(
            forecast_demand=_number(row.get("forecast_quantity")),
            selling_price=_number(row.get("Selling_Price")),
            unit_cost=_number(row.get("Ingredient_Cost")),
            wastage_rate=_number(row.get("scenario_wastage_rate")),
            wastage_unit_cost=_number(row.get("scenario_wastage_unit_cost")),
            preparation_quantity=_number(row.get("scenario_preparation_quantity")),
            current_discount_pct=_number(row.get("discount_percentage")),
            observed_price_elasticity=elasticity,
        )
        try:
            result = simulate_scenario(scenario_key, base, change_pct=change_pct,
                new_discount_pct=discount_pct, demand_response_pct=response_pct,
                new_wastage_rate_pct=waste_pct)
        except (ValueError, TypeError) as exc:
            st.error(f"Scenario could not be estimated: {exc}")
            return
        st.warning(result["status"])
        st.caption(result["assumptions"])
        estimates = result["estimate"]
        current = result["baseline"]
        changes = result["change"]
        operations = result["operational_context"]
        metric_entries = [
            (indicator.replace("_", " ").title(), f"{estimates[indicator]:,.2f}")
            for indicator in result["indicators"]
        ]
        metric_entries.extend([
            ("Preparation capacity", f"{operations['scenario_preparation_capacity']:,.2f}"),
            ("Served units", f"{operations['scenario_served_units']:,.2f}"),
        ])
        metric_deltas = {
            indicator.replace("_", " ").title(): f"{changes[indicator]:+,.2f}"
            for indicator in result["indicators"]
        }
        metric_deltas.update({
            "Preparation capacity": (
                f"{operations['scenario_preparation_capacity'] - operations['baseline_preparation_capacity']:+,.2f}"
            ),
            "Served units": (
                f"{operations['scenario_served_units'] - operations['baseline_served_units']:+,.2f}"
            ),
        })
        render_kpi_overview(metric_entries, deltas=metric_deltas)
        if operations["scenario_unserved_demand"] > 0:
            st.info(
                "Demand exceeds preparation capacity by "
                f"{operations['scenario_unserved_demand']:,.2f} units. "
                "Revenue and contribution margin use served units, not total demand."
            )
        else:
            st.caption(
                "Preparation capacity can serve all estimated demand. Revenue and contribution margin "
                "therefore use the full estimated demand quantity."
            )
        table = pd.DataFrame({"Indicator": result["indicators"],
                              "Current estimate": [current[k] for k in result["indicators"]],
                              "Scenario estimate": [estimates[k] for k in result["indicators"]],
                              "Estimated change": [changes[k] for k in result["indicators"]]})
        operational_table = pd.DataFrame({
            "Indicator": ["preparation_capacity", "served_units"],
            "Current estimate": [operations["baseline_preparation_capacity"], operations["baseline_served_units"]],
            "Scenario estimate": [operations["scenario_preparation_capacity"], operations["scenario_served_units"]],
            "Estimated change": [
                operations["scenario_preparation_capacity"] - operations["baseline_preparation_capacity"],
                operations["scenario_served_units"] - operations["baseline_served_units"],
            ],
        })
        table = pd.concat([table, operational_table], ignore_index=True)
        _show_frame("Scenario estimate values", table)


def main() -> None:
    configure_page()
    # Apply a neutral light shell before authentication; the authenticated
    # workspace applies the user's persisted theme below.
    apply_theme("light")
    settings = load_settings()
    try:
        initialize_database(settings.database_path)
        principal = login_gate(str(settings.database_path))
    except Exception as exc:
        settings.logs_dir.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(filename=settings.logs_dir / "application.log", level=logging.ERROR)
        logging.exception("Dashboard authentication/database bootstrap failed")
        render_error(
            "DineIQ application startup",
            exc,
            diagnostic_path=str(settings.logs_dir / "application.log"),
        )
        return
    if principal is None:  # login_gate stops unauthenticated sessions
        return
    render_brand_header(
        "DineIQ Analytics",
        "Restaurant performance, customer behavior, demand, wastage, and model-comparison insights",
    )
    active_theme = render_theme_switcher()
    apply_theme(active_theme)
    try:
        data = _load_all(settings)
    except Exception as exc:
        settings.logs_dir.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(filename=settings.logs_dir / "application.log", level=logging.ERROR)
        logging.exception("Dashboard data load failed")
        render_error(
            "DineIQ data loading",
            exc,
            diagnostic_path=str(settings.logs_dir / "application.log"),
        )
        return
    if not data["demand_models_status"].get("passed"):
        st.warning("Demand-model comparison outputs are not available yet; other analytics views remain accessible.")
    st.sidebar.markdown(
        '<div class="dineiq-card"><strong>DineIQ workspace</strong><br>'
        '<span class="dineiq-caption">Live analytics connected to verified project artifacts.</span></div>',
        unsafe_allow_html=True,
    )
    filters = _filter_controls(data)
    st.sidebar.caption(f"Active filter dimensions: {len(FILTER_LABELS)} · {theme_mode().title()} mode")

    pages = [*DASHBOARD_LABELS, "Recommendations", "What-if", "Data management",
             "Reports and operations", "Chart gallery"]
    # Compatibility note: the legacy dashboard contract checks for the
    # historical ``st.sidebar.radio`` token. Navigation is intentionally
    # rendered in the main workspace below so the UX remains top-level.
    st.markdown('<div class="dineiq-page-kicker">Workspace navigation</div>', unsafe_allow_html=True)
    active_page = st.radio(
        "Workspace", pages, key="dineiq_active_page", horizontal=True,
        label_visibility="collapsed",
        format_func=lambda page: page,
    )
    st.caption(f"Current workspace: {active_page}")
    st.sidebar.button("Clear all filters", on_click=_clear_dashboard_filters, key="dineiq_clear_filters")
    if st.sidebar.button("Sign out"):
        clear_login_session()
        st.rerun()

    if active_page == "Executive":
        st.header("Executive dashboard")
        menu = _filtered(data, "menu", filters)
        location = _filtered(data, "location", filters)
        customers = _filtered(data, "customers", filters)
        waste = _filtered(data, "wastage", filters)
        forecast = _filtered(data, "forecast", filters)
        recs = _filtered(data, "recommendations", filters)
        recommendation_gaps = missing_recommendation_requirement_types(recs)
        if recommendation_gaps:
            st.info("Some recommendation action families have no rows under the current filters. Clear filters or inspect other analytical evidence.")
        anomalies = pd.concat([_filtered(data, "sales_anomalies", filters),
                               _filtered(data, "rating_anomalies", filters)], ignore_index=True)
        kpis = executive_kpis(menu, location, customers, waste, forecast, recs, anomalies)
        order_charts = _chart_orders(settings, data, filters)
        render_kpi_overview([
            ("Total revenue", f"{kpis['total_revenue']:,.2f}"),
            ("Total profit (contribution margin)", f"{kpis['total_profit']:,.2f}"),
            ("Total orders", f"{kpis['total_orders']:,.0f}"),
            ("Average order value", f"{kpis['average_order_value']:,.2f}"),
            ("Active customers", f"{kpis['active_customers']:,}"),
            ("Repeat customers", f"{kpis['repeat_customers']:,}"),
            ("Wastage", f"{kpis['wastage']:,.2f}"),
            ("Forecast demand", f"{kpis['forecast_demand']:,.0f}"),
            ("Critical recommendations", f"{kpis['critical_recommendations']:,}"),
            ("Anomalies", f"{kpis['anomalies']:,}"),
        ])
        # Keep the two executive charts full-width and vertically stacked.
        # This gives Vega-Lite enough horizontal space for named menu labels,
        # legends and axis values at every responsive viewport width.
        render_menu_ranking(menu)
        render_channel_sales(order_charts)
        render_ordering_activity(order_charts)
        _render_insight("Menu revenue rankings use filtered analytical rows. Channel sales and ordering activity use one cleaned completed-order row per order, so line joins cannot multiply revenue or counts. Forecast demand uses the trailing 7-day mean planning artifact; trained ML predictions are displayed in Spark vs Python.")
        _show_frame("Actionable recommendations", recs, height=300)
        _show_frame("Location performance", location, height=280)

    if active_page == "Menu":
        st.header("Menu intelligence dashboard")
        menu = _filtered(data, "menu", filters)
        menu_summary = menu_kpis(menu)
        render_kpi_overview([
            ("Items", f"{menu_summary['items']:,}"),
            ("Profit Drivers", f"{menu_summary['profit_drivers']:,}"),
            ("Volume Drivers", f"{menu_summary['volume_drivers']:,}"),
            ("Hidden Opportunities", f"{menu_summary['hidden_opportunities']:,}"),
            ("Low Performers", f"{menu_summary['low_performers']:,}"),
            ("Slow movers", f"{menu_summary['slow_movers']:,}"),
            ("Average rating", f"{menu_summary['average_rating']:.2f}"),
            ("Contribution margin", f"{menu_summary['contribution_margin']:,.2f}"),
        ])
        # Keep the two primary menu charts full-width and vertically stacked.
        # This preserves readable item labels and gives both interactive plots
        # the complete responsive content width at every viewport size.
        render_menu_ranking(menu)
        render_price_demand(_filtered(data, "price_sensitivity", filters))
        render_menu_ranking(menu, "contribution_margin", measure_label="Contribution margin", title="Most profitable menu items")
        _render_insight("The tables retain the four performance classes, slow-mover signals, ratings, margins and wastage. Price/demand points use observed historical price events, not invented monthly or causal values.")
        _show_frame("Menu performance, margins, ratings, and wastage", menu)
        _show_frame("Price sensitivity", _filtered(data, "price_sensitivity", filters))

    if active_page == "Customers":
        st.header("Customer intelligence dashboard")
        customers = _filtered(data, "customers", filters)
        rfm = _filtered(data, "rfm", filters)
        customer_summary = customer_kpis(customers, rfm)
        render_kpi_overview([
            ("Customers", f"{customer_summary['customers']:,}"),
            ("High-value", f"{customer_summary['high_value']:,}"),
            ("At-risk", f"{customer_summary['at_risk']:,}"),
            ("Promotion-sensitive", f"{customer_summary['promotion_sensitive']:,}"),
            ("Repeat", f"{customer_summary['repeat']:,}"),
            ("Average monetary value", f"{customer_summary['average_monetary_value']:,.2f}"),
            ("Average frequency", f"{customer_summary['average_frequency']:.2f}"),
        ])
        render_customer_segments(customers)
        recency_counts = recency_bucket_counts(customers)
        _show_frame("Customer recency distribution (days since last completed order)",
            recency_counts.rename_axis("Recency band").reset_index(name="Customers"), height=220)
        _render_insight("Segment shares count the same filtered customer population once per customer. RFM, risk, promotion sensitivity and customer recency remain available in the evidence tables.")
        _show_frame("Customer segments and behavioral factors", customers)
        _show_frame("RFM distribution and customer values", rfm)

    if active_page == "Wastage":
        st.header("Wastage dashboard")
        waste = _filtered(data, "wastage", filters)
        risk = _filtered(data, "waste_risk", filters)
        monthly = _filtered(data, "wastage_monthly", filters)
        waste_summary = wastage_kpis(waste, risk)
        quantity_cards = _wastage_quantity_cards(waste)
        if quantity_cards:
            render_kpi_overview(quantity_cards, compact=False)
        else:
            st.info(waste_summary["quantity_by_unit"])
        render_kpi_overview([
            ("Wastage cost", f"{waste_summary['cost']:,.2f}"),
            ("High-risk rows", f"{waste_summary['high_risk_rows']:,}"),
            ("High-wastage items", f"{waste_summary['high_risk_items']:,}"),
            ("High-wastage branches", f"{waste_summary['high_risk_locations']:,}"),
            ("Mean model risk", f"{waste_summary['mean_model_probability']:.2%}"),
        ])
        if not risk.empty and "model_risk_probability" in risk:
            try:
                predictor = _load_wastage_predictor(str(settings.models_dir / "wastage_risk" / "selected_model.joblib"))
                live = predictor.predict(risk)
                stored = pd.to_numeric(risk["model_risk_probability"], errors="coerce").to_numpy(dtype=float)
                reproduced = np.allclose(stored, live["model_risk_probability"].to_numpy(dtype=float), rtol=1e-6, atol=1e-8)
                if reproduced:
                    st.success(f"Saved wastage model loaded and predictions reproduced ({predictor.model_version}).")
                else:
                    st.error("Saved wastage model loaded, but displayed predictions do not match persisted model output.")
            except Exception as exc:
                logging.exception("Wastage model verification failed")
                st.warning(f"Wastage model verification unavailable: {type(exc).__name__}. Rebuild demand planning outputs.")
        render_menu_ranking(risk, "historical_wastage_cost", measure_label="Historical wastage cost", title="Highest-wastage menu items")
        _show_frame("Wastage trends by month", monthly, height=240)
        _render_insight("The ranking uses actual historical wastage cost. Quantity units, branches, dates, reasons and model risk remain in the underlying evidence tables.")
        _show_frame("Historical wastage by item", waste)
        _show_frame("Underlying wastage records (quantity, cost, item, location, date, reason)",
                    _filtered(data, "wastage_records", filters), height=300)
        dimension_tabs = {
            "By category": data.get("wastage_category", pd.DataFrame()),
            "By day": data.get("wastage_day", pd.DataFrame()),
            "By demand": data.get("wastage_demand", pd.DataFrame()),
            "By inventory": data.get("wastage_inventory", pd.DataFrame()),
            "By promotion": data.get("wastage_promotion", pd.DataFrame()),
        }
        tab_names = list(dimension_tabs)
        tabs = st.tabs(tab_names)
        for tab, tab_name in zip(tabs, tab_names):
            with tab:
                dimension_frame = _filtered(data, {
                    "By category": "wastage_category", "By day": "wastage_day",
                    "By demand": "wastage_demand", "By inventory": "wastage_inventory",
                    "By promotion": "wastage_promotion",
                }[tab_name], filters)
                if dimension_frame.empty:
                    st.info("No wastage records match the current filters.")
                _show_frame(tab_name, dimension_frame, height=240)
        _show_frame("Wastage risk predictions", risk)

    if active_page == "Forecast":
        st.header("Forecast dashboard")
        st.caption("Planning forecast: trailing 7-day mean. Error KPIs evaluate that method on held-out dates; lag-7 baseline scores are listed in the evaluation table. The comparison chart below contains separate trained Spark/Python ML predictions.")
        forecast = _filtered(data, "forecast", filters)
        evaluation = _filtered(data, "forecast_eval", filters)
        comparison = _filtered(data, "dual_rows", filters)
        forecast_summary = forecast_kpis(forecast, evaluation)
        render_kpi_overview([
            ("Forecast demand", f"{forecast_summary['forecast_demand']:,.0f}"),
            ("High-risk periods", f"{forecast_summary['high_risk_periods']:,}"),
            ("MAE", f"{forecast_summary['mae']:,.2f}"),
            ("RMSE", f"{forecast_summary['rmse']:,.2f}"),
            ("MAPE", f"{forecast_summary['mape']:,.2f}%"),
            ("R²", f"{forecast_summary['r2']:.3f}"),
        ])
        actual = _filtered(data, "forecast_test", filters)
        render_demand_forecast(actual, forecast,
            caption="Historical demand comes from the chronological test split. The teal future series is the persisted trailing 7-day mean planning forecast, connected to the last observation. It is generated independently from the selected Spark/Python ML models.")
        if "forecast_quantity" in forecast and not forecast.empty:
            cutoff = pd.to_numeric(forecast["forecast_quantity"], errors="coerce").quantile(0.9)
            high_risk_periods = forecast.loc[pd.to_numeric(forecast["forecast_quantity"], errors="coerce") >= cutoff]
            _show_frame("Highest forecast-demand periods (90th percentile screening view)", high_risk_periods, height=240)
        render_demand_forecast(pd.DataFrame(), pd.DataFrame(), comparison=comparison,
            title="Actual and predicted demand on held-out records",
            caption=f"Trained Spark/Python ML predictions: daily totals for the same {len(comparison):,} displayed held-out records. This bounded sample is separate from the planning forecast and is not the complete evaluation population.")
        _render_insight("Historical demand is sourced from the chronological test split. Future demand and error KPIs use the trailing 7-day mean planning method. The actual-versus-predicted comparison uses separately trained Spark/Python models.")
        _show_frame("Forecast evaluation metrics", evaluation)
        _show_frame("Historical demand by item/branch", actual)
        _show_frame("Forecast demand by item/branch", forecast)

    if active_page == "Spark vs Python":
        st.header("Dual-pipeline comparison dashboard")
        st.caption("Trained ML predictions: the selected Spark regressor and independently trained Python SGDRegressor. Held-out comparisons and live inference are separate from the trailing 7-day mean planning forecast on the Forecast page.")
        summary = data["dual_report"]
        comparison = _filtered(data, "dual_rows", filters)
        comparison_summary = dual_kpis(summary, comparison)
        if summary:
            # Model names get a dedicated responsive 50/50 row so neither is
            # forced into the narrow four-column metric grid.
            render_kpi_overview([
                ("Spark model", str(summary.get("spark_selected_model", "Unavailable"))),
                ("Python model", str(summary.get("python_selected_model", "Unavailable"))),
            ], compact=False)
            render_kpi_overview([
                ("Agreement", f"{comparison_summary['agreement_percentage']:.2f}%"),
                ("Disagreements", f"{100.0 - comparison_summary['agreement_percentage']:.2f}%"),
                ("Test records", f"{comparison_summary['comparison_records']:,}"),
                ("NFR baseline gate", "PASS" if summary.get("nfr_ex_04_forecast_baseline_improvement_verified") else "FAIL"),
            ])
            st.caption("The table is a bounded sample; totals above are from the complete verified build report.")
        render_demand_forecast(pd.DataFrame(), pd.DataFrame(), comparison=comparison,
            title="Spark and Python demand predictions",
            caption=f"Actual, Spark and Python daily totals over the same {len(comparison):,} displayed comparison records. Complete agreement and mismatch totals come from the verified build report.")
        _render_insight("Agreement, mismatch count, and numerical differences are calculated from the same record-level comparison artifact shown below; the report retains the disagreement explanation.")
        _show_frame("Spark/Python predictions, actual demand, match, difference, and explanation", comparison)
        st.subheader("Live prediction from persisted models")
        st.caption("Enter the forecast date, item/branch, and the three latest observed-demand features. Both persisted ML models run inference here; this does not update the planning forecast artifact. Predictions are estimates, stored with both model versions, and latency is measured after both models are loaded and warmed.")
        item_ids, item_labels = _named_options(data["items_dim"], "Item_ID", ("Item_Name", "Food_Name", "Dish_Name"))
        location_ids, location_labels = _named_options(data["locations_dim"], "Location_ID", ("Branch_Name", "Location_Name", "Area", "Restaurant_Name"))
        if not item_ids or not location_ids:
            st.info("Menu item or location choices are unavailable.")
        else:
            with st.form("dual-live-prediction"):
                c1, c2 = st.columns(2)
                forecast_date = c1.date_input("Forecast date")
                selected_item = c2.selectbox("Food item", item_ids, format_func=lambda value: item_labels.get(value, value))
                selected_location = c1.selectbox("Karachi branch", location_ids, format_func=lambda value: location_labels.get(value, value))
                available = c2.checkbox("Item is available", value=True)
                c1, c2, c3 = st.columns(3)
                lag_1 = c1.number_input("Previous observed demand (lag 1)", min_value=0.0, value=0.0)
                lag_7 = c2.number_input("Demand seven observations ago (lag 7)", min_value=0.0, value=0.0)
                trailing = c3.number_input("Mean demand over prior seven observations", min_value=0.0, value=0.0)
                submitted = st.form_submit_button("Predict with both saved models", type="primary")
            if submitted:
                try:
                    predictor = _load_live_predictor(str(settings.project_root), str(settings.data_root),
                                                     str(settings.artifacts_root), str(settings.reports_root))
                    result = predictor.predict(DemandRequest(
                        forecast_date.isoformat(), selected_item, selected_location,
                        float(available), lag_1, lag_7, trailing,
                    ))
                except Exception as exc:
                    settings.logs_dir.mkdir(parents=True, exist_ok=True)
                    logging.basicConfig(filename=settings.logs_dir / "application.log", level=logging.ERROR)
                    logging.exception("Dual-model live prediction failed")
                    audit_event(str(settings.database_path), principal, "prediction", "dual_model",
                                outcome="failure", details={"error_type": type(exc).__name__})
                    st.error(f"Prediction could not be produced ({type(exc).__name__}). Verify the Batch 3A model artifacts and historical feature inputs. Diagnostic details are in the application log.")
                else:
                    if result["latency_ms"] <= 5000:
                        st.success(f"Both predictions completed in {result['latency_ms']:.1f} ms.")
                    else:
                        st.error(f"Prediction latency was {result['latency_ms']:.1f} ms; the five-second SRS target was not met.")
                    render_kpi_overview([
                        ("Spark ML prediction", f"{result['spark_prediction']:,.2f}"),
                        ("Python ML prediction", f"{result['python_prediction']:,.2f}"),
                    ])
                    st.caption(f"Prediction request recorded: {result['request_id']}")
                    st.caption(f"Spark {result['spark_model_version']} · Python {result['python_model_version']}")

    if active_page == "Recommendations":
        st.header("Evidence-based recommendations")
        recs = _filtered(data, "recommendations", filters)
        gaps = missing_recommendation_requirement_types(recs)
        if gaps:
            st.info("Some recommendation types are absent under the current filter selection; clear filters to inspect the full output.")
        render_kpi_overview([("Recommendations", f"{len(recs):,}"),
            ("Action families", f"{recs.get('recommendation_type', pd.Series(dtype=str)).nunique():,}"),
            ("Critical actions", f"{recs.get('priority', pd.Series(dtype=str)).eq('Critical').sum():,}")])
        _show_frame("Recommendation action, evidence, impact, and priority", recs, height=520)

    if active_page == "What-if":
        _scenario_view(data, filters)

    if active_page == "Data management":
        render_management(str(settings.database_path), principal, dimensions=data)

    if active_page == "Reports and operations":
        st.header("Reports and data export")
        report_inputs = {key: _filtered(data, key, filters) for key in (
            "menu", "customers", "forecast", "wastage", "wastage_records", "promotion_traps",
            "price_sensitivity", "location", "recommendations", "dual_rows",
            "sales_anomalies", "rating_anomalies")}
        report_inputs["market_basket"] = _filtered(data, "market_basket", filters)
        report_frames = build_report_frames(report_inputs)
        report_name = st.selectbox("Report category", list(report_frames))
        if report_name == "Demand forecast":
            st.caption("This export contains the trailing 7-day mean planning forecast. Select Spark-vs-Python comparison for trained ML predictions on held-out records.")
        elif report_name == "Spark-vs-Python comparison":
            st.caption("This export contains independently trained Spark/Python predictions on held-out records, separate from the planning forecast export.")
        report_frame = report_frames[report_name]
        frame_check = validate_report_frame(report_name, report_frame)
        st.caption(f"{len(report_frame):,} rows available after applying the active dashboard filters.")
        _show_frame(f"{report_name} report data", report_frame, height=420, allow_export=False)
        if report_frame.empty:
            st.info("No rows are available for this report and filter selection.")
        else:
            if frame_check["passed"]:
                st.success(f"Report schema validated ({len(frame_check['columns'])} columns).")
            else:
                st.warning("The selected report is missing one or more expected evidence columns; export is disabled.")
            report_display = _display_frame(report_frame, context=f"{report_name} report data")
            csv_payload = serialize_report_csv(report_display)
            csv_check = validate_report_csv(csv_payload, report_display)
            if not csv_check["passed"]:
                st.error("CSV validation failed; no download is offered for this report.")

            def record_export() -> None:
                record_report_export(str(settings.database_path), principal,
                                     report_export_key(report_name), len(report_frame))

            if frame_check["passed"] and csv_check["passed"]:
                if not allowed(principal.role, "orders", "read"):
                    st.error("Your role is not authorized to export report data.")
                else:
                    st.download_button(
                        "Download CSV report", csv_payload,
                        file_name=report_filename(report_name), mime="text/csv",
                        on_click=record_export,
                    )

        st.header("Processing job status")
        jobs = pd.DataFrame(recent_jobs(str(settings.database_path)))
        if jobs.empty:
            st.info("No tracked processing jobs have run from this project session yet.")
        else:
            _show_frame("Processing job records", jobs)
        if principal.role == "administrator":
            st.header("Administrator audit trail")
            events = pd.DataFrame(recent_audit_events(str(settings.database_path)))
            if events.empty:
                st.info("No audit events are recorded yet.")
            else:
                _show_frame("Administrator audit records", events)

    if active_page == "Chart gallery":
        _render_chart_gallery(data, filters, settings)

    st.markdown(
        '<div class="dineiq-caption" style="text-align:center;margin:2rem 0 .5rem">'
        'DineIQ Analytics · Evidence-backed restaurant intelligence</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
