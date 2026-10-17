from __future__ import annotations

from decimal import Decimal, InvalidOperation
from html import escape
from hashlib import sha256
import json
import re
from typing import Iterable

import pandas as pd

from dineiq.ui.chart_data import (channel_sales, customer_segments, demand_series,
    menu_ranking, ordering_activity, price_demand_points)
from dineiq.ui.reports import report_filename

NAVY, TEAL, GOLD, BLUE, PALE = "#07334F", "#009C9F", "#F3AC22", "#80BCEB", "#BED2E2"
PALETTE = [NAVY, TEAL, GOLD, BLUE, PALE, "#53A9CD", "#93D3D1", "#416B8C"]
SELECTED_VISUALS = ("KPI metrics overview", "Ranked menu item bar chart",
    "Demand forecast line chart", "Sales by channel area chart",
    "Customer segment donut chart", "Price versus demand scatter plot", "Ordering activity heatmap")


_METRIC_NUMBER = re.compile(r"^\s*([+-]?)([^0-9+-]*?)(\d[\d,]*(?:\.\d+)?)(%)?\s*$")
_COMPACT_UNITS = (
    (Decimal("1000000000000"), "T"),
    (Decimal("1000000000"), "B"),
    (Decimal("1000000"), "M"),
    (Decimal("1000"), "K"),
)
def compact_metric_value(value: object) -> str:
    """Shorten a card value without changing its underlying analytical value."""
    text = "" if value is None else str(value).strip()
    match = _METRIC_NUMBER.fullmatch(text)
    if match:
        sign, prefix, number_text, percent = match.groups()
        try:
            number = Decimal(number_text.replace(",", ""))
        except InvalidOperation:
            number = Decimal(0)
        absolute = abs(number)
        for threshold, unit in _COMPACT_UNITS:
            if absolute >= threshold:
                shortened = f"{absolute / threshold:.2f}".rstrip("0").rstrip(".")
                return f"{sign}{prefix}{shortened}{unit}{percent or ''}"
        if len(text) <= 22:
            return text
        shortened = f"{absolute:.4f}".rstrip("0").rstrip(".")
        return f"{sign}{prefix}{shortened}{percent or ''}"
    if len(text) <= 24:
        return text
    return text[:23].rstrip() + "…"


def _metric_help(value: object, delta: object | None) -> str:
    lines = [f"Complete value: {value}"]
    if delta is not None:
        lines.append(f"Complete change: {delta}")
    return "\n\n".join(lines)


def _base(frame: pd.DataFrame, height: int = 330) -> dict:
    return {"$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": json.loads(frame.to_json(orient="records", date_format="iso"))},
        "width": "container", "height": height, "background": "transparent",
        "autosize": {"type": "fit-x", "contains": "padding"},
        "padding": {"left": 10, "right": 25, "top": 20, "bottom": 10},
        "config": {"view": {"stroke": None},
            "axis": {"labelColor": NAVY, "titleColor": NAVY, "gridColor": "#E7EDF2",
                     "domainColor": "#ACBDCB", "tickColor": "#ACBDCB", "titlePadding": 12},
            "legend": {"labelColor": NAVY, "titleColor": NAVY, "symbolType": "circle"}}}


def ranking_spec(rows: pd.DataFrame, measure_label: str) -> dict:
    values = rows.copy()
    values["Bar color"] = [GOLD if i == 0 else TEAL if i == 1 else PALETTE[i % len(PALETTE)] for i in range(len(values))]
    # Keep the menu ranking and channel chart visually aligned in the
    # executive full-width stack. The chart remains width-responsive while
    # retaining enough vertical room for all named menu items and labels.
    spec = _base(values, max(420, min(560, 42 * len(values))))
    # The Y-axis is categorical, so only the continuous revenue/value axis can
    # be scale-bound.  Binding the ordinal Y-axis makes the layered bar spec
    # invalid in Vega-Lite and results in a blank chart.
    spec["params"] = [{
        "name": "menu_value_zoom",
        "select": {"type": "interval", "encodings": ["x"], "zoom": "wheel!"},
        "bind": "scales",
    }]
    spec["encoding"] = {
        "y": {"field": "Food item", "type": "nominal", "sort": {"field": "Value", "order": "descending"}, "title": None,
              "axis": {"labelLimit": 260, "ticks": False, "domain": False}},
        "x": {"field": "Value", "type": "quantitative", "title": measure_label, "axis": {"format": "~s"}},
        "tooltip": [{"field": "Food item", "type": "nominal"},
                    {"field": "Value", "type": "quantitative", "title": measure_label, "format": ",.2f"}]}
    spec["layer"] = [
        {"mark": {"type": "bar", "cornerRadiusEnd": 4, "size": 23},
         "encoding": {"color": {"field": "Bar color", "type": "nominal", "scale": None, "legend": None}}},
        {"transform": [{"filter": "datum.Value >= 0"}], "mark": {"type": "text", "align": "left", "dx": 6, "color": NAVY},
         "encoding": {"text": {"field": "Value", "type": "quantitative", "format": ",.0f"}}},
        {"transform": [{"filter": "datum.Value < 0"}], "mark": {"type": "text", "align": "right", "dx": -6, "color": NAVY},
         "encoding": {"text": {"field": "Value", "type": "quantitative", "format": ",.0f"}}}]
    return spec


def ranking_echarts_spec(rows: pd.DataFrame, measure_label: str) -> dict:
    """Interactive full-width ranking chart backed only by supplied rows."""
    values = rows.copy()
    names = values["Food item"].astype(str).tolist()
    numbers = pd.to_numeric(values["Value"], errors="coerce").fillna(0.0).astype(float).tolist()
    points = [
        {"value": value, "itemStyle": {"color": GOLD if index == 0 else TEAL if index == 1
                                        else PALETTE[index % len(PALETTE)]}}
        for index, value in enumerate(numbers)
    ]
    return {
        "animation": False,
        "color": PALETTE,
        "aria": {"enabled": True},
        "grid": {"left": 24, "right": 72, "top": 24, "bottom": 50, "containLabel": True},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "xAxis": {"type": "value", "name": measure_label, "nameLocation": "middle", "nameGap": 34,
                  "axisLabel": {"color": NAVY}, "nameTextStyle": {"color": NAVY, "fontWeight": "bold"},
                  "splitLine": {"lineStyle": {"color": "#E7EDF2"}}},
        "yAxis": {"type": "category", "data": names, "inverse": True,
                  "axisLabel": {"color": NAVY, "width": 220, "overflow": "truncate"},
                  "axisTick": {"show": False}, "axisLine": {"show": False}},
        # Both inside controls listen to the pointer wheel while it is over
        # the chart. This zooms the numeric scale and the visible category
        # window; dragging inside pans the zoomed view.
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0], "filterMode": "none",
             "zoomOnMouseWheel": True, "moveOnMouseMove": True, "moveOnMouseWheel": False},
            {"type": "inside", "yAxisIndex": [0], "filterMode": "filter",
             "zoomOnMouseWheel": True, "moveOnMouseMove": True, "moveOnMouseWheel": False},
        ],
        "series": [{"name": measure_label, "type": "bar", "data": points, "barMaxWidth": 28,
                    "itemStyle": {"borderRadius": [0, 4, 4, 0]},
                    "label": {"show": True, "position": "right", "color": NAVY}}],
    }


def forecast_spec(rows: pd.DataFrame) -> dict:
    # Display a constant future forecast once, at its first forecast date.
    # This is chart-only: the input rows and complete export data are retained.
    future = rows.loc[rows["Series"].eq("Future forecast")].sort_values("Date")
    if len(future) > 1:
        demands = pd.to_numeric(future["Demand"], errors="coerce")
        tolerance = 1e-9 * max(1.0, abs(float(demands.iloc[0])))
        if demands.notna().all() and demands.max() - demands.min() <= tolerance:
            rows = pd.concat([
                rows.loc[~rows["Series"].eq("Future forecast")], future.head(1),
            ], ignore_index=True)
    spec = _base(rows, 350)
    labels = rows["Series"].drop_duplicates().tolist()
    colors = {"Historical demand": NAVY, "Future forecast": TEAL,
              "Actual demand": NAVY, "Spark prediction": GOLD, "Python prediction": TEAL}
    spec["mark"] = {"type": "line", "strokeWidth": 2.7, "point": {"size": 18}, "clip": True}
    spec["encoding"] = {
        "x": {"field": "Date", "type": "temporal", "title": "Date", "axis": {"format": "%d %b", "labelAngle": 0}},
        "y": {"field": "Demand", "type": "quantitative", "title": "Demand (units)", "scale": {"zero": True}},
        "color": {"field": "Series", "type": "nominal", "scale": {"domain": labels, "range": [colors.get(label, BLUE) for label in labels]}, "legend": {"orient": "top", "title": None}},
        "strokeDash": {"field": "Series", "type": "nominal", "scale": {"domain": labels, "range": [[6, 4] if label == "Future forecast" else [1, 0] for label in labels]}, "legend": None},
        "tooltip": [{"field": "Date", "type": "temporal", "format": "%d %b %Y"}, {"field": "Series", "type": "nominal"},
                    {"field": "Demand", "type": "quantitative", "format": ",.2f"}]}
    historical = rows.loc[rows["Series"].eq("Historical demand")].sort_values("Date")
    future = rows.loc[rows["Series"].eq("Future forecast")].sort_values("Date")
    if not historical.empty and not future.empty and historical.iloc[-1]["Date"] < future.iloc[0]["Date"]:
        # Visual connector only: retain all original rows, marks, prediction
        # values, colours, dash styles and tooltip/export data.
        connector = pd.concat([historical.tail(1), future.head(1)], ignore_index=True)
        connector["Series"] = "Future forecast"
        original_mark = spec.pop("mark")
        spec["layer"] = [
            {"mark": original_mark},
            {"data": {"values": json.loads(connector.to_json(orient="records", date_format="iso"))},
             "mark": {"type": "line", "strokeWidth": 2.7, "clip": True},
             "encoding": {"tooltip": None}},
        ]
    return spec


def channel_spec(rows: pd.DataFrame) -> dict:
    # Match the menu-ranking chart height so the two executive charts have a
    # consistent visual baseline at every responsive width.
    spec = _base(rows, 420)
    # Both axes are continuous (time and revenue), so pointer-wheel zoom can
    # safely update both visible domains. Dragging inside the plot pans them.
    spec["params"] = [{
        "name": "channel_scale_zoom",
        "select": {"type": "interval", "encodings": ["x", "y"], "zoom": "wheel!"},
        "bind": "scales",
    }]
    names = rows["Channel"].drop_duplicates().sort_values().tolist()
    spec["mark"] = {"type": "area", "opacity": 0.95, "clip": True}
    spec["encoding"] = {
        "x": {"field": "Date", "type": "temporal", "title": "Order date", "axis": {"format": "%d %b"}},
        "y": {"field": "Revenue", "type": "quantitative", "stack": "zero", "title": "Order revenue", "axis": {"format": "~s"}},
        "color": {"field": "Channel", "type": "nominal", "scale": {"domain": names, "range": [PALETTE[i % len(PALETTE)] for i in range(len(names))]}, "legend": {"orient": "top", "title": None}},
        "order": {"field": "Channel", "type": "nominal"},
        "tooltip": [{"field": "Date", "type": "temporal", "format": "%d %b %Y"}, {"field": "Channel", "type": "nominal"},
                    {"field": "Revenue", "type": "quantitative", "format": ",.2f"}]}
    return spec


def channel_echarts_spec(rows: pd.DataFrame) -> dict:
    """Interactive stacked channel area chart backed only by supplied rows."""
    values = rows.copy()
    values["Date"] = pd.to_datetime(values["Date"], errors="coerce")
    values["Revenue"] = pd.to_numeric(values["Revenue"], errors="coerce")
    values = values.dropna(subset=["Date", "Channel", "Revenue"])
    dates = sorted(values["Date"].drop_duplicates().tolist())
    channels = sorted(values["Channel"].astype(str).drop_duplicates().tolist())
    labels = [date.strftime("%d %b %Y") for date in dates]
    lookup = values.assign(Channel=values["Channel"].astype(str)).set_index(["Date", "Channel"])["Revenue"]
    series = []
    for index, channel in enumerate(channels):
        channel_values = [float(lookup.get((date, channel), 0.0)) for date in dates]
        series.append({
            "name": channel,
            "type": "line",
            "stack": "Completed-order revenue",
            "data": channel_values,
            "showSymbol": False,
            "symbol": "none",
            "lineStyle": {"width": 1, "color": PALETTE[index % len(PALETTE)]},
            "areaStyle": {"opacity": 0.92, "color": PALETTE[index % len(PALETTE)]},
            "emphasis": {"focus": "series"},
        })
    daily_totals = values.groupby("Date")["Revenue"].sum()
    tick_chars = max((len(f"{float(value) * 1.25:,.0f}") for value in daily_totals), default=1)
    revenue_name_gap = max(66, tick_chars * 7 + 18)
    return {
        "animation": False,
        "color": [PALETTE[index % len(PALETTE)] for index in range(len(channels))],
        "aria": {"enabled": True},
        "legend": {"data": channels, "top": 0, "type": "scroll", "icon": "circle", "textStyle": {"color": NAVY}},
        "grid": {"left": 44, "right": 30, "top": 56, "bottom": 58, "containLabel": True},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "line"}},
        "xAxis": {"type": "category", "data": labels, "boundaryGap": False,
                  "name": "Order date", "nameLocation": "middle", "nameGap": 34,
                  "axisLabel": {"color": NAVY, "hideOverlap": True},
                  "nameTextStyle": {"color": NAVY, "fontWeight": "bold"}},
        "yAxis": {"type": "value", "name": "Order revenue", "nameLocation": "middle", "nameGap": revenue_name_gap,
                  "axisLabel": {"color": NAVY}, "nameTextStyle": {"color": NAVY, "fontWeight": "bold"},
                  "splitLine": {"lineStyle": {"color": "#E7EDF2"}}},
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0], "filterMode": "none",
             "zoomOnMouseWheel": True, "moveOnMouseMove": True, "moveOnMouseWheel": False},
            {"type": "inside", "yAxisIndex": [0], "filterMode": "none",
             "zoomOnMouseWheel": True, "moveOnMouseMove": True, "moveOnMouseWheel": False},
        ],
        "series": series,
    }


def segment_spec(rows: pd.DataFrame) -> dict:
    spec = _base(rows, 360)
    spec["layer"] = [
        {"mark": {"type": "arc", "innerRadius": 80, "outerRadius": 132, "stroke": "white", "strokeWidth": 2},
         "encoding": {"theta": {"field": "Customers", "type": "quantitative", "stack": True},
                      "color": {"field": "Segment", "type": "nominal", "scale": {"range": PALETTE}, "legend": {"orient": "bottom", "columns": 1, "title": None, "labelLimit": 300}},
                      "tooltip": [{"field": "Segment", "type": "nominal"}, {"field": "Customers", "type": "quantitative", "format": ",d"},
                                  {"field": "Share", "type": "quantitative", "format": ".1%"}]}},
        {"mark": {"type": "text", "radius": 106, "color": "white", "fontWeight": "bold", "fontSize": 12},
         "encoding": {"theta": {"field": "Customers", "type": "quantitative", "stack": True},
                      "text": {"field": "Share", "type": "quantitative", "format": ".0%"}}},
        {"data": {"values": [{"text": f"{int(rows['Customers'].sum()):,}"}]},
         "mark": {"type": "text", "fontSize": 26, "fontWeight": "bold", "color": NAVY, "dy": -7}, "encoding": {"text": {"field": "text", "type": "nominal"}}},
        {"data": {"values": [{"text": "Customers"}]}, "mark": {"type": "text", "fontSize": 12, "color": NAVY, "dy": 19},
         "encoding": {"text": {"field": "text", "type": "nominal"}}}]
    return spec


def segment_echarts_spec(rows: pd.DataFrame) -> dict:
    """Responsive customer donut with a visible percentage for every segment."""
    values = rows.copy()
    values["Customers"] = pd.to_numeric(values["Customers"], errors="coerce")
    values = values.dropna(subset=["Segment", "Customers"])
    values = values.loc[values["Customers"] > 0].reset_index(drop=True)
    data = [
        {
            "name": str(row["Segment"]),
            "value": int(row["Customers"]),
            "itemStyle": {"color": PALETTE[index % len(PALETTE)]},
        }
        for index, row in values.iterrows()
    ]
    total = int(values["Customers"].sum())
    return {
        "animation": False,
        "color": PALETTE,
        "aria": {"enabled": True},
        "title": {
            "text": f"{total:,}", "subtext": "Customers", "left": "center", "top": "37%",
            "textStyle": {"color": NAVY, "fontSize": 26, "fontWeight": "bold"},
            "subtextStyle": {"color": NAVY, "fontSize": 12},
        },
        "tooltip": {"trigger": "item", "formatter": "{b}<br/>{c} customers ({d}%)"},
        "legend": {
            "type": "scroll", "orient": "horizontal", "left": "center", "bottom": 0,
            "textStyle": {"color": NAVY},
        },
        "series": [{
            "name": "Customer segments",
            "type": "pie",
            "radius": ["40%", "66%"],
            "center": ["50%", "45%"],
            "avoidLabelOverlap": False,
            "data": data,
            "itemStyle": {"borderColor": "#FFFFFF", "borderWidth": 2},
            "label": {
                "show": True, "position": "outside", "formatter": "{d}%",
                "color": NAVY, "fontWeight": "bold", "fontSize": 12,
            },
            "labelLine": {"show": True, "length": 16, "length2": 12},
            "labelLayout": {"hideOverlap": False, "moveOverlap": "shiftY"},
            "emphasis": {"scale": True, "scaleSize": 5},
        }],
    }


def price_spec(rows: pd.DataFrame) -> dict:
    spec = _base(rows, 360)
    spec["encoding"] = {
        "x": {"field": "Price", "type": "quantitative", "title": "Observed price", "scale": {"zero": False}},
        "y": {"field": "Daily demand", "type": "quantitative", "title": "Units per observed trading day", "scale": {"zero": True}},
        "color": {"field": "Food item", "type": "nominal", "scale": {"range": PALETTE}, "legend": None},
        "tooltip": [{"field": "Food item", "type": "nominal"}, {"field": "Price", "type": "quantitative", "format": ",.2f"},
                    {"field": "Daily demand", "type": "quantitative", "format": ",.2f"},
                    {"field": "Effective date", "type": "temporal", "format": "%d %b %Y"}]}
    names = rows.sort_values("Daily demand", ascending=False).head(5)
    spec["layer"] = [
        {"mark": {"type": "circle", "size": 82, "opacity": 0.88, "stroke": "white", "strokeWidth": 0.5}},
        {"data": {"values": json.loads(names.to_json(orient="records", date_format="iso"))},
         "mark": {"type": "text", "align": "left", "dx": 8, "dy": -7, "fontSize": 10},
         "encoding": {"text": {"field": "Food item", "type": "nominal"}, "color": {"value": NAVY}}}]
    return spec


def price_echarts_spec(rows: pd.DataFrame) -> dict:
    """Interactive price/demand scatter backed only by supplied artifact rows."""
    values = rows.copy()
    values["Price"] = pd.to_numeric(values["Price"], errors="coerce")
    values["Daily demand"] = pd.to_numeric(values["Daily demand"], errors="coerce")
    values = values.dropna(subset=["Food item", "Price", "Daily demand"])
    points = []
    for index, row in values.reset_index(drop=True).iterrows():
        point = {
            "name": str(row["Food item"]),
            "value": [float(row["Price"]), float(row["Daily demand"])],
            "itemStyle": {"color": PALETTE[index % len(PALETTE)]},
        }
        effective_date = pd.to_datetime(row.get("Effective date"), errors="coerce")
        if pd.notna(effective_date):
            point["effectiveDate"] = effective_date.strftime("%d %b %Y")
        points.append(point)
    return {
        "animation": False,
        "color": PALETTE,
        "aria": {"enabled": True},
        "grid": {"left": 24, "right": 34, "top": 28, "bottom": 58, "containLabel": True},
        "tooltip": {"trigger": "item", "axisPointer": {"type": "cross"}},
        "xAxis": {
            "type": "value", "name": "Observed price", "nameLocation": "middle", "nameGap": 36,
            "scale": True, "axisLabel": {"color": NAVY},
            "nameTextStyle": {"color": NAVY, "fontWeight": "bold"},
            "splitLine": {"lineStyle": {"color": "#E7EDF2"}},
        },
        "yAxis": {
            "type": "value", "name": "Units per observed trading day",
            "nameLocation": "middle", "nameGap": 54, "scale": True,
            "axisLabel": {"color": NAVY},
            "nameTextStyle": {"color": NAVY, "fontWeight": "bold"},
            "splitLine": {"lineStyle": {"color": "#E7EDF2"}},
        },
        # Pointer-wheel zoom applies to both numeric axes while the cursor is
        # inside the plot. Dragging pans the zoomed view.
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0], "filterMode": "none",
             "zoomOnMouseWheel": True, "moveOnMouseMove": True, "moveOnMouseWheel": False},
            {"type": "inside", "yAxisIndex": [0], "filterMode": "none",
             "zoomOnMouseWheel": True, "moveOnMouseMove": True, "moveOnMouseWheel": False},
        ],
        "series": [{
            "name": "Price versus demand", "type": "scatter", "data": points,
            "symbolSize": 11, "emphasis": {"focus": "series", "scale": 1.5},
        }],
    }


def activity_spec(rows: pd.DataFrame) -> dict:
    spec = _base(rows, 300)
    spec["mark"] = {"type": "rect", "stroke": "#FFFFFF", "strokeWidth": 1}
    spec["encoding"] = {
        "x": {"field": "Hour", "type": "ordinal", "sort": list(range(24)), "title": "Hour of day",
              "axis": {"labelAngle": 0, "labelExpr": "datum.value % 3 == 0 ? (datum.value == 0 ? '12am' : datum.value < 12 ? datum.value + 'am' : datum.value == 12 ? '12pm' : (datum.value - 12) + 'pm') : ''"}},
        "y": {"field": "Day", "type": "ordinal", "sort": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], "title": None},
        "color": {"field": "Orders", "type": "quantitative", "scale": {"range": [PALE, BLUE, TEAL, "#FFF1BA", GOLD]}, "legend": {"orient": "bottom", "direction": "horizontal", "title": "Completed orders"}},
        "tooltip": [{"field": "Day", "type": "nominal"}, {"field": "Hour", "type": "ordinal", "title": "Hour (0–23)"},
                    {"field": "Orders", "type": "quantitative", "format": ",d"}]}
    return spec


def _echarts_view_controls(panel_key: str, graph_key: str, data_key: str) -> str:
    """Extend native toolbar rows without changing their capsule or positioning.

    Both original elements remain mounted. The browser switches their visibility
    locally, just as the heatmap does; native download/fullscreen handlers remain
    on their original buttons. Only the additional view button is a clone.
    """
    style = f"""<style>
    .st-key-{panel_key} {{ position: relative; }}
    .st-key-{panel_key} [data-testid="stElementContainer"]:has([data-testid="stHtml"]),
    .st-key-{panel_key} [data-testid="stElementContainer"]:has(.stHtml) {{
        display: none !important;
    }}
    .st-key-{data_key} {{ display: none !important; }}
    .st-key-{panel_key} .dineiq-data-slot {{
        position: absolute !important; top: 0; left: 0; width: 100%;
        visibility: hidden; pointer-events: none;
    }}
    .st-key-{panel_key}.dineiq-show-data .dineiq-graph-slot {{
        visibility: hidden !important; pointer-events: none !important;
    }}
    .st-key-{panel_key}.dineiq-show-data .dineiq-data-slot {{
        visibility: visible !important; pointer-events: auto !important; z-index: 2;
    }}
    .st-key-{panel_key}.dineiq-show-data .st-key-{data_key} {{ display: flex !important; }}
    .st-key-{panel_key} .dineiq-unrequested-action {{ display: none !important; }}
    </style>"""
    script = r"""<script>
    (() => {
        const panelSelector = '.st-key-__PANEL__';
        const graphSelector = '.st-key-__GRAPH__';
        const dataSelector = '.st-key-__DATA__';
        const registry = window.__dineiqBackupViewControls ||= new Map();
        registry.get(panelSelector)?.();
        let stopped = false, frame = null, panel = null;
        const inserted = new Set(), hidden = new Set(), slots = new Set();
        const icons = window.__dineiqBackupNativeIcons ||= new Map();
        function label(button) {
            return [button.getAttribute('aria-label'), button.getAttribute('title'),
                button.getAttribute('data-testid'), button.textContent].join(' ');
        }
        function slotFor(element, owner) {
            let slot = element;
            while (slot.parentElement && slot.parentElement !== owner) slot = slot.parentElement;
            return slot;
        }
        function commonRow(buttons, toolbar) {
            let row = buttons[0].parentElement;
            while (row && row !== toolbar && !buttons.every(b => row.contains(b))) row = row.parentElement;
            return row || toolbar;
        }
        function collectIcons() {
            for (const button of document.querySelectorAll('[data-testid="stElementToolbar"] button')) {
                if (button.hasAttribute('data-dineiq-native-view')) continue;
                const name = /show\s+data/i.test(label(button)) ? 'Show data'
                    : /show\s+chart/i.test(label(button)) ? 'Show chart' : null;
                const svg = button.querySelector('svg');
                if (name && svg) icons.set(name, svg.cloneNode(true));
            }
        }
        function viewIcon(button, name) {
            const donor = icons.get(name);
            const signature = name + (donor?.outerHTML || 'material');
            if (button.getAttribute('data-dineiq-icon') === signature) return;
            button.setAttribute('data-dineiq-icon', signature);
            const old = button.querySelector('svg');
            const svg = donor ? donor.cloneNode(true) : old?.cloneNode(false);
            if (!svg) return;
            if (!donor) {
                svg.setAttribute('viewBox', '0 0 24 24');
                const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                path.setAttribute('d', name === 'Show chart'
                    ? 'M3.5 18.49l6-6.01 4 4L22 6.92 20.59 5.5l-7.09 7.97-4-4L2 16.99z'
                    : 'M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 4H5V5h14v2zM5 9h6v10H5V9zm8 10V9h6v10h-6z');
                svg.appendChild(path);
            }
            svg.setAttribute('aria-hidden', 'true');
            old?.replaceWith(svg);
        }
        function fullscreenButton(owner) {
            const buttons = [...owner.querySelectorAll('[data-testid="stElementToolbar"] button')]
                .filter(b => !b.hasAttribute('data-dineiq-native-view') &&
                    !/search|show\s*\/\s*hide|column\s*visibility|copy.*spec/i.test(label(b)));
            return buttons.find(b => /full\s*screen|expand|collapse/i.test(label(b))) || buttons.at(-1);
        }
        function switchView(sourceButton, sourceFullscreen, showData) {
            let expanded = false;
            for (let node = sourceButton; node && node !== document.body; node = node.parentElement) {
                if (getComputedStyle(node).position === 'fixed' || node.getAttribute('aria-modal') === 'true') {
                    expanded = true; break;
                }
            }
            if (!expanded) { panel.classList.toggle('dineiq-show-data', showData); return; }
            // Let the original native fullscreen handlers move between views.
            // Closing the source first avoids leaving a hidden fullscreen frame.
            sourceFullscreen?.click();
            requestAnimationFrame(() => {
                if (stopped) return;
                panel.classList.toggle('dineiq-show-data', showData);
                requestAnimationFrame(() => {
                    if (stopped) return;
                    const target = panel.querySelector(showData ? dataSelector : graphSelector);
                    if (target) fullscreenButton(target)?.click();
                });
            });
        }
        function installToolbar(owner, name, showData) {
            const toolbar = owner.querySelector('[data-testid="stElementToolbar"]');
            if (!toolbar) return;
            const buttons = [...toolbar.querySelectorAll('button')].filter(b =>
                !b.hasAttribute('data-dineiq-native-view'));
            if (buttons.length < 2) return;
            const row = commonRow(buttons, toolbar);
            // This request specifies three controls. The data grid's search
            // and column-menu shortcuts are excluded from this chart toolbar.
            const actions = buttons.filter(b => {
                if (/search|show\s*\/\s*hide|column\s*visibility|copy.*spec/i.test(label(b))) {
                    const slot = slotFor(b, row);
                    slot.classList.add('dineiq-unrequested-action'); hidden.add(slot);
                    return false;
                }
                return true;
            });
            if (actions.length < 2) return;
            let view = row.querySelector('[data-dineiq-native-view]');
            if (!view) {
                view = actions[0].cloneNode(true);
                view.removeAttribute('id'); view.removeAttribute('disabled');
                view.setAttribute('data-dineiq-native-view', name);
                view.setAttribute('data-testid', 'dineiqNativeView');
                view.setAttribute('aria-label', name); view.setAttribute('title', name);
                view.type = 'button';
                view.addEventListener('click', event => {
                    event.preventDefault(); event.stopPropagation();
                    switchView(view, fullscreenButton(owner), showData);
                    // Both views stay mounted: graph zoom and native table
                    // column widths/sorting survive repeated switches.
                });
                row.insertBefore(view, slotFor(actions[0], row));
                inserted.add(view);
            }
            viewIcon(view, name);
        }
        function install() {
            if (stopped) return;
            panel = document.querySelector(panelSelector);
            const graph = panel?.querySelector(graphSelector);
            const data = panel?.querySelector(dataSelector);
            if (!panel || !graph || !data) return;
            const graphSlot = slotFor(graph, panel), dataSlot = slotFor(data, panel);
            graphSlot.classList.add('dineiq-graph-slot'); dataSlot.classList.add('dineiq-data-slot');
            slots.add(graphSlot); slots.add(dataSlot);
            collectIcons();
            installToolbar(graph, 'Show data', true);
            installToolbar(data, 'Show chart', false);
        }
        function schedule() {
            if (stopped || frame !== null) return;
            frame = requestAnimationFrame(() => { frame = null; install(); });
        }
        const observer = new MutationObserver(schedule);
        observer.observe(document.body, {childList: true, subtree: true});
        registry.set(panelSelector, () => {
            stopped = true;
            if (frame !== null) cancelAnimationFrame(frame);
            observer.disconnect();
            for (const button of inserted) button.remove();
            for (const slot of hidden) slot.classList.remove('dineiq-unrequested-action');
            for (const slot of slots) { slot.classList.remove('dineiq-graph-slot'); slot.classList.remove('dineiq-data-slot'); }
            // Keep the selected view while a Streamlit rerun updates the rows.
        });
        schedule();
    })();
    </script>"""
    return (style + script.replace('__PANEL__', panel_key)
            .replace('__GRAPH__', graph_key).replace('__DATA__', data_key))


def _native_chart_csv_name(card_key: str, title: str) -> str:
    """Change only native CSV download names, retaining their original payloads."""
    filename = report_filename(title + " chart data")
    # The title is reduced to a filename by the existing report naming helper;
    # JSON encoding protects both static identifiers when inserted into JS.
    script = r"""<script>
    (() => {
        const rootSelector = __ROOT__;
        const filename = __FILENAME__;
        const state = window.__dineiqCsvDownloadNames ||= {
            installed: false, roots: new Map(), toolbarNames: new WeakMap(),
            anchorNames: new WeakMap(), pending: null, frame: null
        };
        state.roots.set(rootSelector, filename);
        function markRoots() {
            for (const [selector, name] of state.roots) {
                const root = document.querySelector(selector);
                if (!root) continue;
                if (root.getAttribute('data-dineiq-csv-name') !== name) root.setAttribute('data-dineiq-csv-name', name);
                for (const toolbar of root.querySelectorAll('[data-testid="stElementToolbar"]')) {
                    state.toolbarNames.set(toolbar, name);
                }
            }
        }
        function schedule() {
            if (state.frame !== null) return;
            state.frame = requestAnimationFrame(() => { state.frame = null; markRoots(); });
        }
        if (!state.installed) {
            state.installed = true;
            function namedDownload(anchor, value) {
                // Leave PNGs, explicit report downloads and unrelated links alone.
                if (!/\.csv$/i.test(String(value))) {
                    if (/\.(?:png|svg|jpe?g|pdf)$/i.test(String(value))) state.pending = null;
                    return value;
                }
                const pending = state.pending;
                if (pending && Date.now() - pending.started <= 5000) {
                    state.anchorNames.set(anchor, {original: String(value), name: pending.name, started: pending.started});
                    state.pending = null;
                    return pending.name;
                }
                const assigned = state.anchorNames.get(anchor);
                return assigned && assigned.original === String(value) && Date.now() - assigned.started <= 5000
                    ? assigned.name : value;
            }
            const prototype = HTMLAnchorElement.prototype;
            const download = Object.getOwnPropertyDescriptor(prototype, 'download');
            if (download?.set && download.configurable) {
                Object.defineProperty(prototype, 'download', {
                    ...download,
                    set(value) { download.set.call(this, namedDownload(this, value)); }
                });
            }
            const setAttribute = prototype.setAttribute;
            prototype.setAttribute = function(name, value) {
                return setAttribute.call(this, name,
                    String(name).toLowerCase() === 'download' ? namedDownload(this, value) : value);
            };
            document.addEventListener('click', event => {
                const button = event.target instanceof Element ? event.target.closest('button') : null;
                if (button?.closest('[data-testid="stDownloadButton"]')) {
                    state.pending = null;
                    state.anchorNames = new WeakMap();
                    return;
                }
                if (button?.hasAttribute('data-dineiq-native-view')) return;
                const toolbar = button?.closest('[data-testid="stElementToolbar"]');
                if (!toolbar) return;
                const label = [button.getAttribute('aria-label'), button.getAttribute('title'),
                    button.getAttribute('data-testid'), button.textContent,
                    button.querySelector('svg')?.getAttribute('data-testid')].join(' ');
                if (!/download/i.test(label) || /image|png|jpeg|svg/i.test(label)) return;
                const name = state.toolbarNames.get(toolbar) ||
                    toolbar.closest('[data-dineiq-csv-name]')?.getAttribute('data-dineiq-csv-name');
                if (name) state.pending = {name, started: Date.now()};
            }, true);
            state.observer = new MutationObserver(schedule);
            state.observer.observe(document.body, {childList: true, subtree: true});
        }
        schedule();
    })();
    </script>"""
    style = f"""<style>
    .st-key-{card_key} [data-testid="stElementContainer"]:has(> [data-testid="stHtml"]),
    .st-key-{card_key} [data-testid="stElementContainer"]:has(> .stHtml) {{ display: none !important; }}
    </style>"""
    return style + script.replace('__ROOT__', json.dumps('.st-key-' + card_key)).replace('__FILENAME__', json.dumps(filename))


def _draw(title: str, caption: str, rows: pd.DataFrame, spec_factory, *,
          echarts_factory=None, chart_height: int = 420) -> None:
    import streamlit as st
    key = "dineiq_backup_" + sha256(title.encode()).hexdigest()[:16]
    with st.container(border=True, key=key + "_card"):
        st.markdown(f'<div style="background:{NAVY};color:white;padding:12px 16px;border-radius:10px;margin-bottom:12px;font-weight:600">{escape(title)}</div>', unsafe_allow_html=True)
        st.caption(caption)
        if rows.empty:
            st.info("No matching chart data is available for the current filters.")
            return
        st.html(_native_chart_csv_name(key + "_card", title), unsafe_allow_javascript=True)
        if echarts_factory is not None:
            if not hasattr(st, "echarts_chart"):
                st.error("These charts require Streamlit 1.64 or newer for native ECharts support.")
                return
            panel_key, graph_key, data_key = key + "_panel", key + "_graph", key + "_data"
            with st.container(key=panel_key):
                st.html(_echarts_view_controls(panel_key, graph_key, data_key), unsafe_allow_javascript=True)
                with st.container(key=graph_key):
                    st.echarts_chart(echarts_factory(rows), width="stretch", height=chart_height,
                                     theme=None, renderer="canvas", key=key + "_figure")
                with st.container(key=data_key):
                    st.dataframe(rows, hide_index=True, width="stretch", height=chart_height,
                                 key=key + "_rows")
        else:
            # Only the heatmap and forecast use Vega-Lite.
            st.vega_lite_chart(spec=spec_factory(rows), width="stretch", theme=None)


def render_kpi_overview(entries: Iterable[tuple[str, str]], *,
                        deltas: dict[str, str] | None = None,
                        compact: bool = True) -> None:
    import streamlit as st
    entries = list(entries)
    for offset in range(0, len(entries), 4):
        columns = st.columns(min(4, len(entries) - offset))
        for column, (label, value) in zip(columns, entries[offset:offset + 4]):
            with column:
                full_delta = (deltas or {}).get(label)
                st.metric(
                    label,
                    compact_metric_value(value) if compact else str(value),
                    delta=(compact_metric_value(full_delta) if compact else str(full_delta))
                          if full_delta is not None else None,
                    help=_metric_help(value, full_delta),
                )


def render_menu_ranking(frame: pd.DataFrame, measure: str = "item_revenue", *, measure_label: str = "Revenue",
                        title: str = "Top menu items", limit: int = 10) -> None:
    _draw(title, f"Top {limit} named menu items by {measure_label.lower()} within the selected analytical output.",
          menu_ranking(frame, measure, limit), lambda rows: ranking_spec(rows, measure_label),
          echarts_factory=lambda rows: ranking_echarts_spec(rows, measure_label), chart_height=470)


def render_demand_forecast(history: pd.DataFrame, forecast: pd.DataFrame, *, comparison: pd.DataFrame | None = None,
                           title: str = "Demand forecast", caption: str = "Daily totals from selected observed and persisted forecast rows.") -> None:
    _draw(title, caption, demand_series(history, forecast, comparison), forecast_spec)


def render_channel_sales(orders: pd.DataFrame) -> None:
    _draw("Sales by channel", "Completed-order revenue by date and observed channel; Total_Amount matches the channel-analysis contract.",
          channel_sales(orders), channel_spec, echarts_factory=channel_echarts_spec, chart_height=470)


def render_customer_segments(customers: pd.DataFrame) -> None:
    _draw(
        "Customer segments",
        "Segment shares from the filtered customer population; one count per customer. Every populated segment is labelled with its percentage.",
        customer_segments(customers),
        segment_spec,
        echarts_factory=segment_echarts_spec,
        chart_height=500,
    )


def render_price_demand(frame: pd.DataFrame) -> None:
    _draw(
        "Price versus demand",
        "Each point is a menu item's latest usable price event: units after the change divided by observed trading days. Association is not a causal estimate.",
        price_demand_points(frame),
        price_spec,
        echarts_factory=price_echarts_spec,
        chart_height=470,
    )


def render_ordering_activity(orders: pd.DataFrame) -> None:
    _draw("Ordering activity heatmap", "Distinct completed orders by weekday and recorded hour. Blank timestamps are excluded; zero cells mean no matching recorded orders.", ordering_activity(orders), activity_spec)
