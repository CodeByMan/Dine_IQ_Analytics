"""Streamlit restaurant-intelligence dashboards over verified project outputs."""

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
from dineiq.db.schema import initialize_database
from dineiq.db.operations import audit_event, recent_audit_events, recent_jobs, record_export as record_report_export
from dineiq.db.permissions import allowed
from dineiq.ui.auth import login_gate
from dineiq.ui.catalog import load_report, output_path
from dineiq.ui.components import chart_card, render_error, render_frame, render_frame_visual_summary, render_kpis, safe_number
from dineiq.ui.management import render_management
from dineiq.ui.reports import (available_date_bounds, build_report_frames,
    report_export_key, report_filename, serialize_report_csv,
    validate_report_csv, validate_report_frame)
from dineiq.ui.data import (DASHBOARD_LABELS, FILTER_LABELS, apply_filters,
    build_scenario_baselines, high_value_customer_count,
    missing_recommendation_requirement_types, read_csv, read_parquet,
    recency_bucket_counts, repeat_customer_count)
from dineiq.ui.dashboard_metrics import (customer_kpis, dual_kpis, executive_kpis,
    forecast_kpis, grouped_count, grouped_mean, grouped_sum, menu_kpis, dated_sum, wastage_kpis)
from dineiq.ui.theme import (ACCENT, CHART_COLORS, LIGHT_BG, OFF_WHITE, PRIMARY,
                             SECONDARY, apply_theme, configure_page, render_brand_header,
                             render_theme_switcher, theme_mode)


@st.cache_data(ttl=60, show_spinner=False)
def _cached_parquet(path: str, limit: int | None = None) -> pd.DataFrame:
    return read_parquet(Path(path), max_rows=limit)


@st.cache_data(ttl=60, show_spinner=False)
def _cached_csv(path: str, usecols: tuple[str, ...] | None = None) -> pd.DataFrame:
    return read_csv(Path(path), usecols=list(usecols) if usecols else None)


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
    d = settings.data_root

    def p(key: str, limit: int | None = None) -> pd.DataFrame:
        """Load a canonical generated output through the shared artifact catalog."""
        return _cached_parquet(str(output_path(settings, key)), limit)

    return {
        "menu": p("menu"),
        "features": p("features"),
        "customers": p("customers"),
        "rfm": p("rfm"),
        "wastage": p("wastage"),
        "wastage_records": _cached_csv(str(d / "data" / "wastage.csv")),
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
        "orders": _cached_csv(str(d / "data" / "orders.csv"), ("Order_Date",)),
        "forecast_test": _cached_csv(
            str(d / "splits" / "demand_forecast" / "test.csv"),
            ("Date", "Item_ID", "Location_ID", "Target_Quantity"),
        ),
        "items_dim": _cached_csv(str(d / "data" / "menu_items.csv")),
        "locations_dim": _cached_csv(str(d / "data" / "restaurants.csv")),
        "categories_dim": _cached_csv(str(d / "data" / "menu_categories.csv")),
        "channels_dim": _cached_csv(str(d / "data" / "ordering_channels.csv")),
        "promotions_dim": _cached_csv(str(d / "data" / "promotions.csv")),
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
    """Return stable filter values plus human-readable labels when dimensions provide them."""
    if frame.empty or identifier not in frame.columns:
        return [], {}
    label_column = next((column for column in label_columns if column in frame.columns), None)
    values = frame[identifier].dropna().astype(str).drop_duplicates().sort_values().tolist()
    if label_column is None:
        return values, {value: value for value in values}
    labels = (
        frame[[identifier, label_column]].dropna(subset=[identifier]).astype(str)
        .drop_duplicates(identifier).set_index(identifier)[label_column].to_dict()
    )
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
        picked = st.sidebar.date_input("Date range", (min_date, max_date), min_value=min_date, max_value=max_date)
        date_range = tuple(picked) if isinstance(picked, (tuple, list)) and len(picked) == 2 else None
    else:
        st.sidebar.date_input("Date range", disabled=True, help="Order dates are unavailable.")
        date_range = None
    segment_options = _option_values(data["customers"], "customer_segment")
    class_options = ["Profit Driver", "Volume Driver", "Hidden Opportunity", "Low Performer"]
    price = pd.to_numeric(items.get("Selling_Price", pd.Series(dtype=float)), errors="coerce").dropna()
    price_bounds = (float(price.min()), float(price.max())) if not price.empty else (0.0, 0.0)
    item_values, item_labels = _named_options(items, "Item_ID", ("Item_Name", "Food_Name", "Dish_Name"))
    location_values, location_labels = _named_options(locations, "Location_ID", ("Restaurant_Name", "Location_Name"))
    category_values, category_labels = _named_options(categories, "Category_ID", ("Category_Name",))
    channel_values, channel_labels = _named_options(channels, "Channel_ID", ("Channel_Name",))
    promotion_values, promotion_labels = _named_options(promotions, "Promotion_ID", ("Promotion_Name",))
    return {
        "search": st.sidebar.text_input("Search results", placeholder="Search visible records"),
        "dates": date_range,
        "location": st.sidebar.multiselect("Restaurant / location", location_values, format_func=lambda value: location_labels.get(value, value)),
        "item": st.sidebar.multiselect("Food / menu item", item_values, format_func=lambda value: item_labels.get(value, value)),
        "category": st.sidebar.multiselect("Food category", category_values, format_func=lambda value: category_labels.get(value, value)),
        "segment": st.sidebar.multiselect("Customer segment", segment_options),
        "channel": st.sidebar.multiselect("Ordering channel", channel_values, format_func=lambda value: channel_labels.get(value, value)),
        "promotion": st.sidebar.multiselect("Promotion", promotion_values, format_func=lambda value: promotion_labels.get(value, value)),
        "performance": st.sidebar.multiselect("Performance class", class_options),
        "price": st.sidebar.slider("Price range", min_value=price_bounds[0], max_value=price_bounds[1], value=price_bounds) if price_bounds[0] < price_bounds[1] else price_bounds,
        "rating": st.sidebar.slider("Rating range", min_value=0.0, max_value=5.0, value=(0.0, 5.0)),
        "wastage": st.sidebar.slider("Wastage range (%)", min_value=0.0, max_value=100.0, value=(0.0, 100.0)),
    }


def _filtered(data: dict[str, Any], name: str, filters: dict[str, Any]) -> pd.DataFrame:
    frame = data.get(name, pd.DataFrame())
    if frame.empty:
        return frame
    result = frame.copy()
    # Add dimension names to analytical artifacts for human-readable charts and tables.
    # The identifier columns remain in the working frame so filter and model contracts
    # continue to use the canonical keys.
    dimensions = (
        ("items_dim", "Item_ID", ("Item_Name", "Food_Name", "Dish_Name")),
        ("locations_dim", "Location_ID", ("Restaurant_Name", "Location_Name")),
        ("categories_dim", "Category_ID", ("Category_Name",)),
        ("channels_dim", "Channel_ID", ("Channel_Name",)),
        ("promotions_dim", "Promotion_ID", ("Promotion_Name",)),
        ("customers", "Customer_ID", ("Customer_Name", "Customer_Name_Pseudonymous")),
    )
    for dimension_key, identifier, label_columns in dimensions:
        lookup = data.get(dimension_key, pd.DataFrame())
        if identifier not in result.columns or lookup.empty or identifier not in lookup.columns:
            continue
        label_column = next((column for column in label_columns if column in lookup.columns), None)
        if label_column and label_column not in result.columns:
            result = result.merge(lookup[[identifier, label_column]].drop_duplicates(identifier), on=identifier, how="left")
    return apply_filters(result, filters)


def _display_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Format analytical output for people first, while retaining IDs when names are unavailable."""
    if frame.empty:
        return frame
    result = frame.copy()
    replacements = {
        "Item_Name": "Food item", "Food_Name": "Food item", "Dish_Name": "Food item",
        "Restaurant_Name": "Restaurant", "Location_Name": "Location",
        "Category_Name": "Food category", "Channel_Name": "Ordering channel",
        "Promotion_Name": "Promotion", "Customer_Name": "Customer",
    }
    for source, display in replacements.items():
        if source in result.columns:
            result = result.rename(columns={source: display})
    id_to_name = {
        "Item_ID": "Food item", "Location_ID": "Restaurant", "Category_ID": "Food category",
        "Channel_ID": "Ordering channel", "Promotion_ID": "Promotion", "Customer_ID": "Customer",
    }
    for identifier, display in id_to_name.items():
        if display in result.columns and identifier in result.columns:
            result = result.drop(columns=[identifier])
        elif identifier in result.columns:
            result = result.rename(columns={identifier: f"{display} ID"})
    return result


def _show_frame(title: str, frame: pd.DataFrame, *, height: int = 360) -> None:
    display = _display_frame(frame)
    render_frame(title, display, height=height)
    _render_table_visual_summary(display)


def _sum(frame: pd.DataFrame, column: str) -> float:
    return float(pd.to_numeric(frame[column], errors="coerce").fillna(0).sum()) if column in frame else 0.0


def _mean(frame: pd.DataFrame, column: str) -> float:
    values = pd.to_numeric(frame[column], errors="coerce").dropna() if column in frame else pd.Series(dtype=float)
    return float(values.mean()) if not values.empty else 0.0


def _number(value: Any, default: float = 0.0) -> float:
    """Convert nullable artifact values to a finite UI-safe number."""
    return safe_number(value, default)


def _kpi_row(entries: list[tuple[str, str]]) -> None:
    render_kpis(entries)


def _plotly_pie(title: str, counts: pd.Series) -> None:
    """Render a themed Plotly pie from a categorical backend series."""
    values = counts.dropna()
    if values.empty:
        return
    try:
        import plotly.express as px
        pie_data = values.rename_axis("Category").reset_index(name="Count")
        fig = px.pie(pie_data, names="Category", values="Count", hole=.28,
                     color_discrete_sequence=CHART_COLORS)
        fig.update_traces(textposition="inside", textinfo="percent+label")
        dark = theme_mode() == "dark"
        surface = "#0F2745" if dark else LIGHT_BG
        plot_surface = "#0B1F3A" if dark else OFF_WHITE
        fig.update_layout(title=title, template="plotly_dark" if dark else "plotly_white", height=360,
                          margin=dict(l=30,r=30,t=60,b=30),
                          plot_bgcolor=plot_surface, paper_bgcolor=surface,
                          font_color="#F8FAFC" if dark else PRIMARY)
        with chart_card():
            st.plotly_chart(fig, width="stretch", theme=None)
    except ImportError:
        pie_spec = {"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
                    "data":{"values":pd.DataFrame({"Category":values.index.astype(str), "Count":values.values}).to_dict(orient="records")},
                    "mark":{"type":"arc","innerRadius":55},
                    "encoding":{"theta":{"field":"Count","type":"quantitative"},
                                "color":{"field":"Category","type":"nominal","scale":{"range":CHART_COLORS}},
                                "tooltip":[{"field":"Category","type":"nominal"},{"field":"Count","type":"quantitative"}]},
                    "title":title,"height":320}
        with chart_card(title):
            st.vega_lite_chart(pie_spec, width="stretch")


def _chart_message(frame: pd.DataFrame, message: str = "No chart data is available for the current filters.") -> bool:
    """Render one deterministic empty state and report whether a chart can render."""
    if frame.empty:
        st.info(message)
        return False
    return True


def _chart_dimension(frame: pd.DataFrame, identifier: str) -> str:
    """Prefer a real dimension name over its technical key in visual labels."""
    candidates = {
        "Item_ID": ("Item_Name", "Food_Name", "Dish_Name"),
        "Location_ID": ("Restaurant_Name", "Location_Name"),
        "Category_ID": ("Category_Name",),
        "Channel_ID": ("Channel_Name",),
        "Promotion_ID": ("Promotion_Name",),
        "Customer_ID": ("Customer_Name", "Customer_Name_Pseudonymous"),
    }.get(identifier, ())
    return next((column for column in candidates if column in frame.columns), identifier)


def _render_bar(title: str, frame: pd.DataFrame, dimension: str, measure: str) -> None:
    dimension = _chart_dimension(frame, dimension)
    chart = grouped_sum(frame, dimension, measure)
    if not _chart_message(chart):
        return
    with chart_card(title):
        st.bar_chart(chart.set_index(dimension)[[measure]], color=CHART_COLORS[3], horizontal=True, width="stretch")


def _render_mean_bar(title: str, frame: pd.DataFrame, dimension: str, measure: str) -> None:
    dimension = _chart_dimension(frame, dimension)
    chart = grouped_mean(frame, dimension, measure)
    if not _chart_message(chart):
        return
    with chart_card(title):
        st.bar_chart(chart.set_index(dimension)[[measure]], color=CHART_COLORS[1], horizontal=True, width="stretch")


def _render_count_bar(title: str, frame: pd.DataFrame, dimension: str) -> None:
    dimension = _chart_dimension(frame, dimension)
    chart = grouped_count(frame, dimension)
    if not _chart_message(chart):
        return
    with chart_card(title):
        st.bar_chart(chart.set_index(dimension)[["count"]], color=CHART_COLORS[4], horizontal=True, width="stretch")


def _render_line(title: str, frame: pd.DataFrame, date_column: str, measure: str, *, area: bool = False) -> None:
    chart = dated_sum(frame, date_column, measure)
    if not _chart_message(chart):
        return
    indexed = chart.set_index("date")[[measure]]
    with chart_card(title):
        if area:
            st.area_chart(indexed, color=CHART_COLORS[2], width="stretch")
        else:
            st.line_chart(indexed, color=CHART_COLORS[3], width="stretch")


def _render_scatter(title: str, frame: pd.DataFrame, x: str, y: str) -> None:
    if frame.empty or x not in frame or y not in frame:
        st.info("The selected analytical output has no valid scatter values for the current filters.")
        return
    points = frame[[x, y]].copy()
    points[x] = pd.to_numeric(points[x], errors="coerce")
    points[y] = pd.to_numeric(points[y], errors="coerce")
    points = points.dropna()
    if not _chart_message(points):
        return
    with chart_card(title):
        st.scatter_chart(points, x=x, y=y, color=CHART_COLORS[0], width="stretch")


def _render_gauge(title: str, value: float, *, maximum: float = 100.0, suffix: str = "%", color: str = CHART_COLORS[4]) -> None:
    """Render a real Vega-Lite gauge without requiring a chart dependency."""
    if not np.isfinite(value):
        st.info(f"{title} is unavailable for the current filters.")
        return
    bounded = max(0.0, min(float(maximum), float(value)))
    with chart_card(title):
        st.vega_lite_chart({
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": [{"value": bounded, "maximum": float(maximum)}]},
        "layer": [
            {"mark": {"type": "arc", "innerRadius": 62, "outerRadius": 92, "startAngle": -2.36, "endAngle": 2.36, "color": color},
             "encoding": {"theta": {"field": "value", "type": "quantitative", "scale": {"domain": [0, float(maximum)]}},
                          "tooltip": [{"field": "value", "type": "quantitative"}]}},
            {"mark": {"type": "text", "fontSize": 20, "fontWeight": "bold", "color": PRIMARY},
             "encoding": {"text": {"field": "value", "type": "quantitative", "format": ".2f"}}},
        ],
            "view": {"stroke": None}, "width": "container", "height": 180,
        }, width="stretch")
    st.caption(f"{bounded:.2f}{suffix} of {maximum:.0f}{suffix}")


def _render_radar(title: str, frame: pd.DataFrame, fields: list[str]) -> None:
    """Render a normalized multi-measure profile from the current filtered rows."""
    fields = [field for field in fields if field in frame]
    if len(fields) < 3 or frame.empty:
        return
    values = frame[fields].apply(pd.to_numeric, errors="coerce").mean().dropna()
    if len(values) < 3:
        return
    # Normalize each measure so revenue, ratings, and rates can share one radar.
    low = frame[values.index].apply(pd.to_numeric, errors="coerce").min()
    high = frame[values.index].apply(pd.to_numeric, errors="coerce").max()
    normalized = ((values - low) / (high - low).replace(0, 1) * 100).fillna(0)
    try:
        import plotly.graph_objects as go
        labels = list(normalized.index)
        radius = normalized.astype(float).tolist()
        figure = go.Figure(go.Scatterpolar(r=radius + [radius[0]], theta=labels + [labels[0]], fill="toself",
                                           line=dict(color=CHART_COLORS[3], width=3), fillcolor="rgba(37,99,235,.25)"))
        dark = theme_mode() == "dark"
        surface = "#0F2745" if dark else LIGHT_BG
        figure.update_layout(template="plotly_dark" if dark else "plotly_white", height=390,
                             margin=dict(l=36, r=36, t=48, b=30), paper_bgcolor=surface,
                             font_color="#F8FAFC" if dark else PRIMARY, showlegend=False,
                             polar=dict(radialaxis=dict(visible=True, range=[0, 100])))
        with chart_card(title):
            st.plotly_chart(figure, width="stretch", theme=None)
    except ImportError:
        with chart_card(title):
            st.bar_chart(normalized, color=CHART_COLORS[3], width="stretch")


def _render_table_visual_summary(frame: pd.DataFrame) -> None:
    """Add a compact, data-backed visual summary beside every rendered table."""
    if frame.empty:
        return
    numeric = [c for c in frame.columns if pd.api.types.is_numeric_dtype(frame[c])]
    if not numeric:
        return
    dimension = next((c for c in frame.columns if c not in numeric and frame[c].nunique(dropna=True) <= 30), None)
    if dimension is None:
        dimension = "Record"
        plot = frame[numeric[:1]].copy()
        plot[dimension] = [f"{i + 1}" for i in range(len(plot))]
    else:
        plot = frame[[dimension, numeric[0]]].copy()
    plot[numeric[0]] = pd.to_numeric(plot[numeric[0]], errors="coerce")
    plot = plot.dropna(subset=[numeric[0]]).head(40)
    if plot.empty:
        return
    st.markdown(f'<div class="dineiq-chart-title">📊 {numeric[0].replace("_", " ").title()} by {dimension.replace("_", " ").title()}</div>', unsafe_allow_html=True)
    st.caption(f"Dynamic view from the same filtered rows as the table.")
    chart = plot.groupby(dimension, as_index=False)[numeric[0]].sum().sort_values(numeric[0], ascending=False).head(20)
    if chart.empty:
        return
    with chart_card():
        st.bar_chart(chart.set_index(dimension)[[numeric[0]]], color=CHART_COLORS[3], horizontal=True, width="stretch")


def _render_insight(text: str) -> None:
    st.caption(f"Insight · {text}")


def _render_chart_gallery(data: dict[str, Any], filters: dict[str, Any]) -> None:
    """Explore existing backend outputs with Streamlit charts and native integrations."""
    st.header("Analytics chart gallery")
    st.caption("Charts use the loaded DineIQ analytics outputs and respect the dashboard filters. Choose a dataset, measure, and visualization.")
    keys = ["menu", "customers", "rfm", "wastage", "wastage_monthly", "wastage_category", "wastage_day",
            "wastage_demand", "wastage_inventory", "wastage_promotion", "forecast", "location", "channel",
            "ratings", "recommendations", "sales_anomalies", "rating_anomalies", "price_sensitivity", "dual_rows"]
    frames = {key: _filtered(data, key, filters) for key in keys if isinstance(data.get(key), pd.DataFrame) and not data[key].empty}
    if not frames:
        st.info("No analytics output is available to chart yet.")
        return
    dataset = st.selectbox("Analytics dataset", list(frames), format_func=lambda k: k.replace("_", " ").title())
    frame = frames[dataset]
    numeric = [c for c in frame.columns if pd.api.types.is_numeric_dtype(frame[c])]
    for col in frame.columns:
        if col not in numeric and pd.to_numeric(frame[col], errors="coerce").notna().mean() > .8:
            numeric.append(col)
    dimensions = [c for c in frame.columns if c not in numeric or any(t in c.lower() for t in ("date", "month", "day", "hour"))]
    if not numeric:
        st.info("The selected backend output has no numeric field to chart.")
        return
    c1, c2 = st.columns(2)
    dimension = c1.selectbox("Category or time field", ["Row order"] + dimensions, index=1 if dimensions else 0)
    measure = c2.selectbox("Measure", numeric)
    chart_type = st.selectbox("Visualization", [
        "Streamlit line", "Streamlit bar", "Streamlit area", "Streamlit scatter", "Streamlit map",
        "Gauge chart", "Histogram chart", "Radar chart", "Altair chart", "Vega-Lite chart", "Plotly chart", "Plotly pie chart", "Plotly doughnut chart", "Bokeh chart", "PyDeck map",
    ])
    plot_frame = frame.copy()
    if dimension == "Row order":
        plot_frame["Chart record"] = [f"Record {i+1}" for i in range(len(plot_frame))]
        dimension = "Chart record"
    plot_frame[measure] = pd.to_numeric(plot_frame[measure], errors="coerce")
    plot_frame = plot_frame.dropna(subset=[measure]).head(500)
    if dimension not in plot_frame:
        st.info("The selected dimension is not present in the filtered output.")
        return
    plot_frame[dimension] = plot_frame[dimension].fillna("Unknown").astype(str)
    grouped = plot_frame.groupby(dimension, as_index=False, dropna=False)[measure].sum().sort_values(measure, ascending=False).head(50)
    if grouped.empty:
        st.info("The selected measure contains no chartable numeric values.")
        return
    # Keep chart labels consistent in every supported renderer.
    chart_data = grouped.rename(columns={dimension: "category", measure: "value"})
    if chart_type in {"Streamlit line", "Streamlit bar", "Streamlit area"}:
        indexed = chart_data.set_index("category")[["value"]]
        with chart_card(f"{chart_type} · {dataset.replace('_', ' ').title()}"):
            if chart_type == "Streamlit line": st.line_chart(indexed, color=CHART_COLORS[3], width="stretch")
            elif chart_type == "Streamlit bar": st.bar_chart(indexed, color=CHART_COLORS[0], horizontal=True, width="stretch")
            else: st.area_chart(indexed, color=CHART_COLORS[1], width="stretch")
    elif chart_type == "Gauge chart":
        value = float(pd.to_numeric(chart_data["value"], errors="coerce").mean())
        maximum = float(pd.to_numeric(chart_data["value"], errors="coerce").max()) or 1.0
        _render_gauge(f"{dataset.replace('_', ' ').title()} · {measure}", value, maximum=maximum, suffix="")
    elif chart_type == "Histogram chart":
        values = pd.to_numeric(frame[measure], errors="coerce").dropna()
        if values.empty:
            st.info("No numeric values are available for a histogram.")
        else:
            histogram = pd.cut(values, bins=min(12, max(3, values.nunique())), include_lowest=True).value_counts().sort_index()
            with chart_card(f"Histogram · {measure.replace('_', ' ')}"):
                st.bar_chart(histogram.rename("count"), color=CHART_COLORS[2], width="stretch")
    elif chart_type == "Radar chart":
        radar_fields = st.multiselect("Radar measures", numeric, default=numeric[:min(5, len(numeric))])
        if len(radar_fields) < 3:
            st.info("Radar charts need at least three numeric measures.")
        else:
            values = frame[radar_fields].apply(pd.to_numeric, errors="coerce").mean().dropna()
            if len(values) < 3:
                st.info("The selected measures contain insufficient numeric values.")
            else:
                try:
                    import plotly.graph_objects as go
                    labels = list(values.index)
                    vals = values.astype(float).tolist()
                    fig = go.Figure(go.Scatterpolar(r=vals + [vals[0]], theta=labels + [labels[0]], fill="toself", line_color=ACCENT))
                    dark = theme_mode() == "dark"
                    fig.update_layout(template="plotly_dark" if dark else "plotly_white", height=420,
                                      margin=dict(l=36, r=36, t=48, b=30), paper_bgcolor="#0F2745" if dark else LIGHT_BG,
                                      font_color="#F8FAFC" if dark else PRIMARY, showlegend=False)
                    with chart_card(f"Radar · {dataset.replace('_', ' ').title()}"):
                        st.plotly_chart(fig, width="stretch", theme=None)
                except ImportError:
                    with chart_card(f"Radar fallback · {dataset.replace('_', ' ').title()}"):
                        st.bar_chart(values, color=CHART_COLORS[3], width="stretch")
    elif chart_type == "Streamlit scatter":
        if len(numeric) < 2:
            st.info("Scatter needs two numeric fields; choose a dataset with two measures.")
        else:
            x_col = st.selectbox("X axis", [c for c in numeric if c != measure])
            scatter = frame[[x_col, measure]].apply(pd.to_numeric, errors="coerce").dropna()
            if scatter.empty: st.info("No valid numeric pairs are available for scatter plotting.")
            else:
                with chart_card(f"Scatter · {x_col} vs {measure}"):
                    st.scatter_chart(scatter, x=x_col, y=measure, color=CHART_COLORS[0], width="stretch")
    elif chart_type == "Streamlit map":
        lat = next((c for c in frame.columns if c.lower() in {"latitude", "lat"}), None)
        lon = next((c for c in frame.columns if c.lower() in {"longitude", "lon", "lng"}), None)
        if not lat or not lon:
            st.info("Choose a location output with latitude and longitude fields to display the map.")
        else:
            points = frame[[lat, lon]].rename(columns={lat:"latitude", lon:"longitude"}).apply(pd.to_numeric, errors="coerce").dropna()
            if points.empty: st.info("The selected output has no valid map coordinates.")
            else:
                with chart_card("Location map"):
                    st.map(points, color=CHART_COLORS[3], size=100, width="stretch")
    elif chart_type == "Vega-Lite chart":
        spec = {"$schema":"https://vega.github.io/schema/vega-lite/v5.json", "data":{"values":chart_data.to_dict(orient="records")},
                "mark":{"type":"bar","cornerRadiusEnd":4}, "encoding":{"x":{"field":"category","type":"nominal","sort":"-y","axis":{"labelAngle":-35}},
                "y":{"field":"value","type":"quantitative"}, "color":{"field":"category","type":"nominal","legend":None}}, "height":360}
        with chart_card(f"Vega-Lite · {dataset.replace('_', ' ').title()}"):
            st.vega_lite_chart(spec, width="stretch")
    elif chart_type == "Altair chart":
        try:
            import altair as alt
            spec = alt.Chart(chart_data).mark_bar(color=CHART_COLORS[3], cornerRadiusEnd=4).encode(
                x=alt.X("category:N", sort="-y", axis=alt.Axis(labelAngle=-35)), y="value:Q",
                tooltip=["category:N", "value:Q"]).properties(height=360)
            with chart_card(f"Altair · {dataset.replace('_', ' ').title()}"):
                st.altair_chart(spec, width="stretch")
        except ImportError:
            st.warning("Altair is not installed; install it to enable this chart.")
    elif chart_type in {"Plotly chart", "Plotly pie chart", "Plotly doughnut chart"}:
        try:
            import plotly.express as px
            if chart_type in {"Plotly pie chart", "Plotly doughnut chart"}:
                hole = .28 if chart_type == "Plotly doughnut chart" else 0
                fig = px.pie(chart_data, names="category", values="value", color_discrete_sequence=CHART_COLORS, hole=hole)
                fig.update_traces(textposition="inside", textinfo="percent+label")
            else:
                fig = px.bar(chart_data, x="category", y="value", color="category", color_discrete_sequence=CHART_COLORS)
                fig.update_layout(showlegend=False)
            dark = theme_mode() == "dark"
            fig.update_layout(template="plotly_dark" if dark else "plotly_white", height=420,
                              margin=dict(l=30,r=30,t=50,b=30), paper_bgcolor="#0F2745" if dark else LIGHT_BG,
                              plot_bgcolor="#0B1F3A" if dark else OFF_WHITE, font_color="#F8FAFC" if dark else PRIMARY)
            with chart_card(f"{chart_type} · {dataset.replace('_', ' ').title()}"):
                st.plotly_chart(fig, width="stretch", theme=None)
        except ImportError:
            if chart_type in {"Plotly pie chart", "Plotly doughnut chart"}:
                pie_spec = {"$schema":"https://vega.github.io/schema/vega-lite/v5.json",
                            "data":{"values":chart_data.to_dict(orient="records")},
                            "mark":{"type":"arc", "innerRadius":55 if chart_type == "Plotly doughnut chart" else 0},
                            "encoding":{"theta":{"field":"value","type":"quantitative"},
                                        "color":{"field":"category","type":"nominal","scale":{"range":CHART_COLORS}},
                                        "tooltip":[{"field":"category","type":"nominal"},{"field":"value","type":"quantitative"}]},
                            "title":"Pie chart · " + dataset,"height":360}
                with chart_card("Pie chart fallback"):
                    st.vega_lite_chart(pie_spec, width="stretch")
            else:
                st.warning("Plotly is not installed; install it to enable this chart.")
    elif chart_type == "Bokeh chart":
        try:
            from bokeh.plotting import figure
            fig = figure(x_range=chart_data["category"].tolist(), height=420, sizing_mode="stretch_width",
                         x_axis_label=dimension, y_axis_label=measure, toolbar_location="above")
            fig.vbar(x="category", top="value", width=.72, source=chart_data, color=CHART_COLORS[4])
            fig.xaxis.major_label_orientation = .8
            with chart_card(f"Bokeh · {dataset.replace('_', ' ').title()}"):
                st.bokeh_chart(fig, width="stretch")
        except ImportError:
            st.warning("Bokeh is not installed; install it to enable this chart.")
    elif chart_type == "PyDeck map":
        lat = next((c for c in frame.columns if c.lower() in {"latitude", "lat"}), None)
        lon = next((c for c in frame.columns if c.lower() in {"longitude", "lon", "lng"}), None)
        if not lat or not lon:
            st.info("Choose a location output with latitude and longitude fields to display the PyDeck map.")
        else:
            try:
                import pydeck as pdk
                geo = frame[[lat, lon, measure]].copy(); geo.columns = ["latitude", "longitude", "value"]
                geo["latitude"] = pd.to_numeric(geo["latitude"], errors="coerce")
                geo["longitude"] = pd.to_numeric(geo["longitude"], errors="coerce")
                geo["value"] = pd.to_numeric(geo["value"], errors="coerce").fillna(0)
                geo = geo.dropna(subset=["latitude", "longitude"])
                if geo.empty: st.info("The selected output has no valid map coordinates.")
                else:
                    layer = pdk.Layer("ScatterplotLayer", data=geo, get_position="[longitude, latitude]",
                                      get_radius=220, get_fill_color=[37,99,235,190], pickable=True)
                    view = pdk.ViewState(latitude=float(geo.latitude.mean()), longitude=float(geo.longitude.mean()), zoom=5)
                    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view,
                                            tooltip={"html":"<b>Value:</b> {value}"}), width="stretch")
            except ImportError:
                st.warning("PyDeck is not installed; install it to enable this map.")
    st.caption(f"Chart is based on {len(plot_frame):,} filtered rows from {dataset}.")


def _scenario_inputs(data: dict[str, Any]) -> pd.DataFrame:
    return build_scenario_baselines(data["items_dim"], data["features"], data["forecast"],
                                    data["waste_risk"], data["price_sensitivity"])


def _scenario_view(data: dict[str, Any], filters: dict[str, Any]) -> None:
    st.header("What-if scenario analysis")
    st.caption("All scenario outputs are estimates based on forecast demand and observed historical rates; they are not actual outcomes or causal claims.")
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
    _, location_labels = _named_options(data["locations_dim"], "Location_ID", ("Restaurant_Name", "Location_Name"))
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
        cols = st.columns(len(result["indicators"]))
        for col, indicator in zip(cols, result["indicators"]):
            label = indicator.replace("_", " ").title()
            col.metric(label, f"{estimates[indicator]:,.2f}", f"{changes[indicator]:+,.2f}")
        table = pd.DataFrame({"Indicator": result["indicators"],
                              "Current estimate": [current[k] for k in result["indicators"]],
                              "Scenario estimate": [estimates[k] for k in result["indicators"]],
                              "Estimated change": [changes[k] for k in result["indicators"]]})
        st.dataframe(table, width="stretch", hide_index=True)
        render_frame_visual_summary(table, "Scenario estimate comparison")


def main() -> None:
    configure_page()
    # Apply a neutral light shell before authentication; the authenticated
    # workspace applies the user's persisted theme below.
    apply_theme("light")
    render_brand_header(
        "DineIQ Analytics",
        "Restaurant performance, customer behavior, demand, wastage, and model-comparison insights",
    )
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
        '<div class="dineiq-card"><strong>🍽️ DineIQ workspace</strong><br>'
        '<span class="dineiq-caption">Live analytics connected to verified project artifacts.</span></div>',
        unsafe_allow_html=True,
    )
    filters = _filter_controls(data)
    st.sidebar.caption(f"✨ Active filter dimensions: {len(FILTER_LABELS)} · {theme_mode().title()} mode")

    pages = [*DASHBOARD_LABELS, "Recommendations", "What-if", "Data management",
             "Reports and operations", "Chart gallery"]
    page_icons = {
        "Executive": "📈", "Menu": "🍴", "Customers": "👥", "Wastage": "♻️",
        "Forecast": "🔮", "Spark vs Python": "⚡", "Recommendations": "💡",
        "What-if": "🧪", "Data management": "🗂️", "Reports and operations": "📄",
        "Chart gallery": "🎨",
    }
    st.markdown('<div class="dineiq-page-kicker">Workspace navigation</div>', unsafe_allow_html=True)
    active_page = st.radio(
        "Workspace", pages, key="dineiq_active_page", horizontal=True,
        label_visibility="collapsed",
        format_func=lambda page: f"{page_icons.get(page, '•')}  {page}",
    )
    st.caption(f"Current workspace: {page_icons.get(active_page, '•')} {active_page}")
    if st.sidebar.button("Sign out"):
        st.session_state.pop("dineiq_principal", None)
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
        _kpi_row([
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
        chart_left, chart_right = st.columns(2)
        with chart_left:
            _render_bar("Revenue by location", location, "Location_ID", "revenue")
        with chart_right:
            _render_bar("Contribution margin by location", location, "Location_ID", "contribution_margin")
        trend = data.get("location", pd.DataFrame())
        if "revenue" in trend:
            _render_insight("Location ranking is recalculated after every active filter; use it to compare comparable locations rather than a hard-coded leaderboard.")
        if "priority" in recs and not recs.empty:
            _plotly_pie("Recommendation priority mix", recs["priority"].value_counts())
        if "anomaly_type" in anomalies and not anomalies.empty:
            _render_count_bar("Anomalies by type", anomalies, "anomaly_type")
        _show_frame("Actionable recommendations", recs, height=300)
        _show_frame("Location performance", location, height=280)

    if active_page == "Menu":
        st.header("Menu intelligence dashboard")
        menu = _filtered(data, "menu", filters)
        menu_summary = menu_kpis(menu)
        _kpi_row([
            ("Items", f"{menu_summary['items']:,}"),
            ("Profit Drivers", f"{menu_summary['profit_drivers']:,}"),
            ("Volume Drivers", f"{menu_summary['volume_drivers']:,}"),
            ("Hidden Opportunities", f"{menu_summary['hidden_opportunities']:,}"),
            ("Low Performers", f"{menu_summary['low_performers']:,}"),
            ("Slow movers", f"{menu_summary['slow_movers']:,}"),
            ("Average rating", f"{menu_summary['average_rating']:.2f}"),
            ("Contribution margin", f"{menu_summary['contribution_margin']:,.2f}"),
            ("Wastage cost", f"{menu_summary['wastage_cost']:,.2f}"),
        ])
        if "performance_class" in menu:
            counts = menu["performance_class"].value_counts().rename_axis("Performance class").to_frame("Items")
            chart_left, chart_right = st.columns(2)
            with chart_left:
                with chart_card("Menu classification counts"):
                    st.bar_chart(counts, color=CHART_COLORS[3], width="stretch")
            with chart_right:
                _plotly_pie("Menu performance mix", menu["performance_class"].value_counts())
        chart_left, chart_right = st.columns(2)
        with chart_left:
            _render_mean_bar("Average rating by performance class", menu, "performance_class", "average_rating")
        with chart_right:
            _render_bar("Contribution margin by performance class", menu, "performance_class", "contribution_margin")
        scatter_cols = [c for c in ("item_revenue", "contribution_margin") if c in menu]
        if len(scatter_cols) == 2:
            _render_scatter("Revenue versus contribution margin", menu, scatter_cols[0], scatter_cols[1])
        _render_radar("Menu health profile · normalized filtered averages", menu,
                      ["item_revenue", "contribution_margin", "average_rating", "wastage_percentage", "order_frequency"])
        if "wastage_percentage" in menu:
            _render_bar("Wastage percentage by performance class", menu, "performance_class", "wastage_percentage")
        _render_insight("Each item retains its four-way classification, slow-mover signal, rating, margin, and wastage evidence in the table below; charts are recomputed from the filtered item rows.")
        _show_frame("Menu performance, margins, ratings, and wastage", menu)
        _show_frame("Price sensitivity", _filtered(data, "price_sensitivity", filters))

    if active_page == "Customers":
        st.header("Customer intelligence dashboard")
        customers = _filtered(data, "customers", filters)
        rfm = _filtered(data, "rfm", filters)
        customer_summary = customer_kpis(customers, rfm)
        _kpi_row([
            ("Customers", f"{customer_summary['customers']:,}"),
            ("High-value", f"{customer_summary['high_value']:,}"),
            ("At-risk", f"{customer_summary['at_risk']:,}"),
            ("Promotion-sensitive", f"{customer_summary['promotion_sensitive']:,}"),
            ("Repeat", f"{customer_summary['repeat']:,}"),
            ("Average monetary value", f"{customer_summary['average_monetary_value']:,.2f}"),
            ("Average frequency", f"{customer_summary['average_frequency']:.2f}"),
        ])
        if "customer_segment" in customers:
            segment_counts = customers["customer_segment"].value_counts()
            chart_left, chart_right = st.columns(2)
            with chart_left:
                with chart_card("Customer segment counts"):
                    st.bar_chart(segment_counts, color=CHART_COLORS[1], width="stretch")
            with chart_right:
                _plotly_pie("Customer segment mix", segment_counts)
        if "recency_days" in customers:
            recency_counts = recency_bucket_counts(customers)
            if not recency_counts.empty:
                st.caption("Customer trend proxy: distribution of days since each customer’s last completed order.")
                with chart_card("Customer recency distribution"):
                    st.area_chart(recency_counts.to_frame("customers"), color=CHART_COLORS[2], width="stretch")
        if {"monetary_value", "frequency"}.issubset(rfm.columns):
            _render_scatter("RFM monetary value versus order frequency", rfm, "frequency", "monetary_value")
        if customer_summary["customers"]:
            _render_gauge("High-value customer share", customer_summary["high_value"] / customer_summary["customers"] * 100)
        _render_insight("RFM distribution, segment mix, risk, promotion sensitivity, and recency trend all use the same filtered customer population.")
        _show_frame("Customer segments and behavioral factors", customers)
        _show_frame("RFM distribution and customer values", rfm)

    if active_page == "Wastage":
        st.header("Wastage dashboard")
        waste = _filtered(data, "wastage", filters)
        risk = _filtered(data, "waste_risk", filters)
        monthly = _filtered(data, "wastage_monthly", filters)
        waste_summary = wastage_kpis(waste, risk)
        _kpi_row([("Wasted quantity", f"{waste_summary['quantity']:,.0f}"),
                  ("Wastage cost", f"{waste_summary['cost']:,.2f}"),
                  ("High-risk rows", f"{waste_summary['high_risk_rows']:,}"),
                  ("High-wastage items", f"{waste_summary['high_risk_items']:,}"),
                  ("High-wastage locations", f"{waste_summary['high_risk_locations']:,}"),
                  ("Mean model risk", f"{waste_summary['mean_model_probability']:.2%}")])
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
        if "wastage_cost" in monthly and "year_month" in monthly:
            trend = monthly.copy()
            trend["year_month"] = pd.to_datetime(trend["year_month"], errors="coerce")
            trend = trend.dropna(subset=["year_month"])
            if not trend.empty:
                with chart_card("Wastage cost trend"):
                    st.line_chart(trend.groupby("year_month")["wastage_cost"].sum(), color=CHART_COLORS[0], width="stretch")
        chart_left, chart_right = st.columns(2)
        with chart_left:
            _render_bar("High-wastage items", risk, "Item_ID", "historical_wastage_cost")
        with chart_right:
            _render_bar("High-wastage locations", risk, "Location_ID", "historical_wastage_cost")
        if waste_summary["mean_model_probability"]:
            _render_gauge("Mean model high-wastage probability", waste_summary["mean_model_probability"] * 100)
        if {"historical_wastage_percentage", "model_risk_probability"}.issubset(risk.columns):
            _render_scatter("Historical wastage rate versus model probability", risk,
                            "historical_wastage_percentage", "model_risk_probability")
        _render_insight("Historical quantity, cost, item, location, date, and reason fields remain available below; model risk is displayed alongside the historical evidence.")
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
                elif tab_name == "By category" and "Category_ID" in dimension_frame:
                    _render_bar("Wastage cost by category", dimension_frame, "Category_ID", "wastage_cost")
                elif tab_name == "By day" and "wastage_day" in dimension_frame:
                    _render_line("Wastage cost by day", dimension_frame, "wastage_day", "wastage_cost", area=True)
                elif tab_name == "By demand" and "demand_bucket" in dimension_frame:
                    _render_bar("Wastage cost by demand band", dimension_frame, "demand_bucket", "wastage_cost")
                elif tab_name == "By inventory" and "inventory_status" in dimension_frame:
                    _render_bar("Wastage cost by inventory status", dimension_frame, "inventory_status", "wastage_cost")
                elif tab_name == "By promotion" and "promotion_status" in dimension_frame:
                    _render_bar("Wastage cost by promotion status", dimension_frame, "promotion_status", "wastage_cost")
                _show_frame(tab_name, dimension_frame, height=240)
        _show_frame("Wastage risk predictions", risk)

    if active_page == "Forecast":
        st.header("Forecast dashboard")
        forecast = _filtered(data, "forecast", filters)
        evaluation = _filtered(data, "forecast_eval", filters)
        comparison = _filtered(data, "dual_rows", filters)
        forecast_summary = forecast_kpis(forecast, evaluation)
        _kpi_row([
            ("Forecast demand", f"{forecast_summary['forecast_demand']:,.0f}"),
            ("High-risk periods", f"{forecast_summary['high_risk_periods']:,}"),
            ("MAE", f"{forecast_summary['mae']:,.2f}"),
            ("RMSE", f"{forecast_summary['rmse']:,.2f}"),
            ("MAPE", f"{forecast_summary['mape']:,.2f}%"),
            ("R²", f"{forecast_summary['r2']:.3f}"),
        ])
        actual = _filtered(data, "forecast_test", filters)
        if {"Date", "Target_Quantity"}.issubset(actual.columns) and not actual.empty:
            actual_trend = actual.copy()
            actual_trend["Date"] = pd.to_datetime(actual_trend["Date"], errors="coerce")
            actual_trend["Target_Quantity"] = pd.to_numeric(actual_trend["Target_Quantity"], errors="coerce")
            actual_trend = actual_trend.dropna(subset=["Date", "Target_Quantity"])
            if not actual_trend.empty:
                with chart_card("Historical demand"):
                    st.line_chart(actual_trend.groupby("Date")["Target_Quantity"].sum(), color=CHART_COLORS[1], width="stretch")
        if {"Date", "forecast_quantity"}.issubset(forecast.columns):
            _render_line("Forecast demand", forecast, "Date", "forecast_quantity", area=True)
        if "forecast_quantity" in forecast and not forecast.empty:
            cutoff = pd.to_numeric(forecast["forecast_quantity"], errors="coerce").quantile(0.9)
            high_risk_periods = forecast.loc[pd.to_numeric(forecast["forecast_quantity"], errors="coerce") >= cutoff]
            _show_frame("Highest forecast-demand periods (90th percentile screening view)", high_risk_periods, height=240)
        if {"metric", "value"}.issubset(evaluation.columns):
            _render_bar("Forecast error metrics", evaluation, "metric", "value")
        if {"actual_demand", "spark_prediction"}.issubset(comparison.columns):
            _render_scatter("Actual versus predicted demand", comparison, "actual_demand", "spark_prediction")
        _render_gauge("Forecast R²", forecast_summary["r2"], maximum=1.0, suffix="")
        _render_insight("Historical demand is sourced from the chronological test split; future demand is sourced from the persisted forecast artifact, while error KPIs come from the held-out evaluation output.")
        _show_frame("Forecast evaluation metrics", evaluation)
        _show_frame("Historical and forecast demand by item/location", forecast)

    if active_page == "Spark vs Python":
        st.header("Dual-pipeline comparison dashboard")
        summary = data["dual_report"]
        comparison = _filtered(data, "dual_rows", filters)
        comparison_summary = dual_kpis(summary, comparison)
        if summary:
            _kpi_row([
                ("Spark model", str(summary.get("spark_selected_model", "Unavailable"))),
                ("Python model", str(summary.get("python_selected_model", "Unavailable"))),
                ("Agreement", f"{comparison_summary['agreement_percentage']:.2f}%"),
                ("Disagreements", f"{comparison_summary['disagreement_count']:,}"),
                ("Test records", f"{comparison_summary['comparison_records']:,}"),
                ("NFR baseline gate", "PASS" if summary.get("nfr_ex_04_forecast_baseline_improvement_verified") else "FAIL"),
            ])
            st.caption("The table is a bounded sample; totals above are from the complete verified build report.")
        chart_left, chart_right = st.columns(2)
        with chart_left:
            _render_gauge("Prediction agreement", comparison_summary["agreement_percentage"])
        with chart_right:
            if "match_status" in comparison:
                _render_count_bar("Match versus mismatch", comparison, "match_status")
            elif "absolute_prediction_difference" in comparison:
                _render_bar("Prediction difference", comparison, "Date", "absolute_prediction_difference")
        prediction_columns = [column for column in ("actual_demand", "spark_prediction", "python_prediction") if column in comparison]
        if len(prediction_columns) >= 2:
            _render_scatter("Actual demand versus Spark prediction", comparison, "actual_demand", prediction_columns[1])
        _render_insight("Agreement, mismatch count, and numerical differences are calculated from the same record-level comparison artifact shown below; the report retains the disagreement explanation.")
        _show_frame("Spark/Python predictions, actual demand, match, difference, and explanation", comparison)
        st.subheader("Live prediction from persisted models")
        st.caption("Enter the forecast date, item/location, and the three latest observed-demand features. Predictions are estimates, stored with both model versions, and latency is measured after both persisted models are loaded and warmed.")
        item_ids, item_labels = _named_options(data["items_dim"], "Item_ID", ("Item_Name", "Food_Name", "Dish_Name"))
        location_ids, location_labels = _named_options(data["locations_dim"], "Location_ID", ("Restaurant_Name", "Location_Name"))
        if not item_ids or not location_ids:
            st.info("Menu item or location choices are unavailable.")
        else:
            with st.form("dual-live-prediction"):
                c1, c2 = st.columns(2)
                forecast_date = c1.date_input("Forecast date")
                selected_item = c2.selectbox("Food item", item_ids, format_func=lambda value: item_labels.get(value, value))
                selected_location = c1.selectbox("Restaurant location", location_ids, format_func=lambda value: location_labels.get(value, value))
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
                    _kpi_row([
                        ("Spark forecast", f"{result['spark_prediction']:,.2f}"),
                        ("Python forecast", f"{result['python_prediction']:,.2f}"),
                        ("Request ID", str(result["request_id"])),
                    ])
                    st.caption(f"Spark {result['spark_model_version']} · Python {result['python_model_version']}")

    if active_page == "Recommendations":
        st.header("Evidence-based recommendations")
        recs = _filtered(data, "recommendations", filters)
        gaps = missing_recommendation_requirement_types(recs)
        if gaps:
            st.info("Some recommendation types are absent under the current filter selection; clear filters to inspect the full output.")
        if not recs.empty and "recommendation_type" in recs:
            with chart_card("Recommendation types"):
                st.bar_chart(recs["recommendation_type"].value_counts(), color=CHART_COLORS[4], width="stretch")
        _show_frame("Recommendation action, evidence, impact, and priority", recs, height=520)
        st.caption("Menu optimization actions, inventory planning, and segment targeting use the verified Batch 2B recommendation evidence.")

    if active_page == "What-if":
        _scenario_view(data, filters)

    if active_page == "Data management":
        render_management(str(settings.database_path), principal)

    if active_page == "Reports and operations":
        st.header("Reports and data export")
        report_inputs = {key: _filtered(data, key, filters) for key in (
            "menu", "customers", "forecast", "wastage", "wastage_records", "promotion_traps",
            "price_sensitivity", "location", "recommendations", "dual_rows",
            "sales_anomalies", "rating_anomalies")}
        report_inputs["market_basket"] = _filtered(data, "market_basket", filters)
        report_frames = build_report_frames(report_inputs)
        report_name = st.selectbox("Report category", list(report_frames))
        report_frame = report_frames[report_name]
        frame_check = validate_report_frame(report_name, report_frame)
        st.caption(f"{len(report_frame):,} rows available after applying the active dashboard filters.")
        _show_frame(f"{report_name} report data", report_frame, height=420)
        if report_frame.empty:
            st.info("No rows are available for this report and filter selection.")
        else:
            if frame_check["passed"]:
                st.success(f"Report schema validated ({len(frame_check['columns'])} columns).")
            else:
                st.warning("The selected report is missing one or more expected evidence columns; export is disabled.")
            csv_payload = serialize_report_csv(report_frame)
            csv_check = validate_report_csv(csv_payload, report_frame)
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
            st.dataframe(jobs, width="stretch", hide_index=True)
            render_frame_visual_summary(jobs, "Processing job duration and status")
        if principal.role == "administrator":
            st.header("Administrator audit trail")
            events = pd.DataFrame(recent_audit_events(str(settings.database_path)))
            if events.empty:
                st.info("No audit events are recorded yet.")
            else:
                st.dataframe(events, width="stretch", hide_index=True)
                render_frame_visual_summary(events, "Audit activity summary")

    if active_page == "Chart gallery":
        _render_chart_gallery(data, filters)

    st.markdown(
        '<div class="dineiq-caption" style="text-align:center;margin:2rem 0 .5rem">'
        'DineIQ Analytics · Evidence-backed restaurant intelligence</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
