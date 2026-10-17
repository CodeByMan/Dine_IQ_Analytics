from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from dineiq.ui.data import apply_filters


def _numbers(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan)


def menu_ranking(frame: pd.DataFrame, measure: str = "item_revenue", limit: int = 10) -> pd.DataFrame:
    """Rank real menu names; unknown labels are omitted rather than showing IDs."""
    output = pd.DataFrame(columns=["Food item", "Value"])
    name = next((key for key in ("Item_Name", "Food_Name", "Dish_Name") if key in frame), None)
    if name is None or measure not in frame:
        return output
    values = frame[[name, measure]].copy()
    values.columns = ["Food item", "Value"]
    values["Value"] = _numbers(values["Value"])
    values["Food item"] = values["Food item"].astype("string").str.strip()
    values = values.dropna()
    values = values.loc[values["Food item"].ne("")]
    return (values.groupby("Food item", as_index=False)["Value"].sum(min_count=1)
            .sort_values(["Value", "Food item"], ascending=[False, True]).head(limit).reset_index(drop=True))


def demand_series(history: pd.DataFrame, forecast: pd.DataFrame,
                  comparison: pd.DataFrame | None = None) -> pd.DataFrame:
    """Daily observed demand and independent persisted forecast/prediction series."""
    records = []
    inputs = [(history, "Target_Quantity", "Historical demand"),
              (forecast, "forecast_quantity", "Future forecast")]
    if comparison is not None:
        inputs.extend((comparison, column, label) for column, label in (
            ("actual_demand", "Actual demand"), ("spark_prediction", "Spark prediction"),
            ("python_prediction", "Python prediction")))
    for frame, measure, label in inputs:
        if "Date" not in frame or measure not in frame:
            continue
        values = pd.DataFrame({"Date": pd.to_datetime(frame["Date"], errors="coerce").dt.normalize(),
                               "Demand": _numbers(frame[measure])})
        values = values.dropna()
        if not values.empty:
            daily = values.groupby("Date", as_index=False)["Demand"].sum(min_count=1)
            daily["Series"] = label
            records.append(daily)
    return (pd.concat(records, ignore_index=True).sort_values(["Series", "Date"]).reset_index(drop=True)
            if records else pd.DataFrame(columns=["Date", "Demand", "Series"]))


def customer_segments(frame: pd.DataFrame) -> pd.DataFrame:
    column = next((key for key in ("customer_segment", "Customer_Segment") if key in frame), None)
    if column is None:
        return pd.DataFrame(columns=["Segment", "Customers", "Share"])
    names = frame[column].fillna("Unclassified").astype(str)
    if "Customer_ID" in frame:
        rows = pd.DataFrame({"Segment": names, "Customer": frame["Customer_ID"]})
        rows = rows.dropna(subset=["Customer"]).drop_duplicates("Customer")
        names = rows["Segment"]
    counts = names.value_counts().rename_axis("Segment").reset_index(name="Customers")
    total = counts["Customers"].sum()
    counts["Share"] = counts["Customers"] / total if total else 0.0
    return counts


def price_demand_points(frame: pd.DataFrame) -> pd.DataFrame:
    """Latest usable price event per menu item; demand divided by observed days."""
    columns = ["Food item", "Price", "Daily demand", "Effective date"]
    required = {"Item_ID", "new_price", "units_after", "observed_days_after", "effective_date"}
    name = next((key for key in ("Item_Name", "Food_Name", "Dish_Name") if key in frame), None)
    if name is None or not required.issubset(frame.columns):
        return pd.DataFrame(columns=columns)
    values = frame.copy()
    values["Price"] = _numbers(values["new_price"])
    days = _numbers(values["observed_days_after"])
    values["Daily demand"] = _numbers(values["units_after"]) / days.where(days > 0)
    values["Effective date"] = pd.to_datetime(values["effective_date"], errors="coerce")
    values["Food item"] = values[name].astype("string").str.strip()
    values = values.dropna(subset=["Item_ID", *columns])
    values = values.loc[(values["Price"] > 0) & (values["Daily demand"] >= 0) & values["Food item"].ne("")]
    # Stable ordering resolves identical-date events without relying on row order.
    values = values.sort_values(["Item_ID", "Effective date", "Price", "Daily demand"])
    values = values.drop_duplicates("Item_ID", keep="last")
    return values[columns].sort_values("Food item").reset_index(drop=True)


def completed_order_view(orders: pd.DataFrame, filters: dict[str, Any], *,
                         lines: pd.DataFrame | None = None, menu: pd.DataFrame | None = None,
                         customers: pd.DataFrame | None = None,
                         item_filter_active: bool = False) -> pd.DataFrame:
    """One cleaned completed-order row per key, with supported dashboard filters.

Item filters select eligible order IDs through a semi-join, avoiding line
multiplication. Revenue stays at order grain and uses normalized Total_Amount,
the same field used by the existing channel-analysis pipeline.
"""
    required = {"Order_ID", "Order_Date", "Total_Amount"}
    status = next((key for key in ("Order_Status", "Status") if key in orders), None)
    if not required.issubset(orders.columns) or status is None:
        return pd.DataFrame()
    view = orders.copy()
    for source, target in (("Restaurant_ID", "Location_ID"), ("Channel", "Channel_ID")):
        if target not in view and source in view:
            view[target] = view[source]
    view = view.loc[view[status].fillna("").astype(str).str.strip().str.casefold().eq("completed")]
    view = view.dropna(subset=["Order_ID"]).drop_duplicates("Order_ID")
    if customers is not None and "Customer_ID" in view and filters.get("segment"):
        if "Customer_ID" not in customers:
            return pd.DataFrame()
        view = view.loc[view["Customer_ID"].astype(str).isin(customers["Customer_ID"].astype(str))]
    if item_filter_active:
        if lines is None or menu is None or not {"Order_ID", "Item_ID"}.issubset(lines.columns) or "Item_ID" not in menu:
            return pd.DataFrame()
        relevant = lines.loc[lines["Item_ID"].astype(str).isin(menu["Item_ID"].astype(str)), "Order_ID"]
        view = view.loc[view["Order_ID"].astype(str).isin(relevant.astype(str))]
    # Search is interpreted on visible dimension names before this function is
    # called. Apply record-specific filters without searching opaque order IDs.
    view = apply_filters(view, {key: value for key, value in filters.items() if key not in {"search", "segment"}})
    view["Date"] = pd.to_datetime(view["Order_Date"], errors="coerce").dt.normalize()
    timestamp = (pd.to_datetime(view["Order_DateTime"], errors="coerce") if "Order_DateTime" in view
                 else pd.Series(pd.NaT, index=view.index, dtype="datetime64[ns]"))
    if "Order_Time" in view:
        fallback = pd.to_datetime(view["Order_Date"].astype(str).str[:10] + " " + view["Order_Time"].astype(str), errors="coerce")
        timestamp = timestamp.fillna(fallback)
    view["Order hour"] = timestamp.dt.hour
    view["Revenue"] = _numbers(view["Total_Amount"])
    view["Channel"] = view.get("Channel_Name", view.get("Channel_ID", pd.Series(index=view.index, dtype=str))).fillna("Unspecified")
    return view.dropna(subset=["Date"])


def channel_sales(orders: pd.DataFrame) -> pd.DataFrame:
    if not {"Date", "Channel", "Revenue"}.issubset(orders.columns):
        return pd.DataFrame(columns=["Date", "Channel", "Revenue"])
    daily = orders.dropna(subset=["Revenue"]).groupby(["Date", "Channel"], as_index=False)["Revenue"].sum(min_count=1)
    if daily.empty:
        return daily
    # All channels at every observed date keeps area layers aligned. A missing
    # channel/date means no completed orders in that cell, hence a zero total.
    grid = pd.MultiIndex.from_product([sorted(daily["Date"].unique()), sorted(daily["Channel"].unique())], names=["Date", "Channel"])
    return daily.set_index(["Date", "Channel"]).reindex(grid, fill_value=0).reset_index()


def ordering_activity(orders: pd.DataFrame) -> pd.DataFrame:
    columns = ["Day", "Hour", "Orders"]
    if not {"Date", "Order hour", "Order_ID"}.issubset(orders.columns):
        return pd.DataFrame(columns=columns)
    view = orders.loc[orders["Order hour"].between(0, 23)].copy()
    if view.empty:
        return pd.DataFrame(columns=columns)
    view["weekday"] = view["Date"].dt.dayofweek
    counts = view.groupby(["weekday", "Order hour"])["Order_ID"].nunique()
    grid = pd.MultiIndex.from_product([range(7), range(24)], names=["weekday", "Order hour"])
    result = counts.reindex(grid, fill_value=0).rename("Orders").reset_index()
    result["Day"] = result["weekday"].map(dict(enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])))
    result["Hour"] = result["Order hour"].astype(int)
    return result[columns]


def executive_trends(orders: pd.DataFrame) -> dict[str, list[float]]:
    if orders.empty or not {"Date", "Order_ID", "Revenue"}.issubset(orders.columns):
        return {}
    groups = orders.groupby("Date", sort=True)
    daily = groups.agg(revenue=("Revenue", "sum"), orders=("Order_ID", "nunique"))
    daily["aov"] = daily["revenue"] / daily["orders"].where(daily["orders"] > 0)
    trends = {"Total revenue": daily.revenue.tolist(), "Total orders": daily.orders.tolist(),
              "Average order value": daily.aov.dropna().tolist()}
    if "Customer_ID" in orders:
        trends["Active customers"] = groups["Customer_ID"].nunique().tolist()
    return trends
