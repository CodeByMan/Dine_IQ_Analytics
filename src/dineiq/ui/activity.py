"""Data-backed calendar heatmap and customer-cohort summaries for the UI."""
from __future__ import annotations

from typing import Any

import pandas as pd


def order_activity_heatmap(orders: pd.DataFrame) -> list[dict[str, Any]]:
    """Return completed-order counts by weekday and two-hour time bucket."""
    needed = {"Order_Date", "Order_Time", "Order_Status"}
    if not needed.issubset(orders.columns):
        return []
    frame = orders.loc[:, ["Order_Date", "Order_Time", "Order_Status"]].copy()
    frame = frame[frame["Order_Status"].astype(str).str.strip().str.casefold().eq("completed")]
    dates = pd.to_datetime(frame["Order_Date"], errors="coerce")
    times = pd.to_datetime(frame["Order_Time"].astype(str), errors="coerce", format="mixed")
    frame = frame.assign(
        day_index=dates.dt.dayofweek,
        day_name=dates.dt.day_name(),
        hour_bucket=(times.dt.hour // 2) * 2,
    ).dropna(subset=["day_index", "hour_bucket"])
    counts = frame.groupby(["day_index", "day_name", "hour_bucket"], sort=True).size()
    return [
        {"day_name": day, "hour_bucket": hour, "order_count": int(counts.get((idx, day, hour), 0))}
        for idx, day in enumerate(("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))
        for hour in range(0, 24, 2)
    ]


def customer_cohort_heatmap(orders: pd.DataFrame, max_cohorts: int = 7, periods: int = 10) -> list[dict[str, Any]]:
    """Return monthly customer retention; future/incomplete periods stay null."""
    needed = {"Customer_ID", "Order_Date", "Order_Status"}
    if not needed.issubset(orders.columns):
        return []
    frame = orders.loc[:, ["Customer_ID", "Order_Date", "Order_Status"]].copy()
    frame = frame[frame["Order_Status"].astype(str).str.strip().str.casefold().eq("completed")]
    frame["Order_Date"] = pd.to_datetime(frame["Order_Date"], errors="coerce")
    frame = frame.dropna(subset=["Customer_ID", "Order_Date"])
    if frame.empty:
        return []
    frame["period"] = frame["Order_Date"].dt.to_period("M")
    frame = frame.drop_duplicates(["Customer_ID", "period"])
    first = frame.groupby("Customer_ID")["period"].min().rename("cohort")
    frame = frame.join(first, on="Customer_ID")
    frame["period_index"] = (
        (frame["period"].dt.year - frame["cohort"].dt.year) * 12
        + frame["period"].dt.month - frame["cohort"].dt.month
    )
    cohort_sizes = frame.groupby("cohort")["Customer_ID"].nunique()
    observed = frame.groupby(["cohort", "period_index"])["Customer_ID"].nunique()
    latest = frame["period"].max()
    cohorts = list(cohort_sizes.index.sort_values(ascending=False)[:max_cohorts])
    result: list[dict[str, Any]] = []
    for cohort in sorted(cohorts):
        label = str(cohort)
        for index in range(periods):
            period = cohort + index
            active = observed.get((cohort, index))
            retention = None if period > latest or active is None else round(float(active / cohort_sizes[cohort] * 100), 2)
            result.append({"cohort_month": label, "period_index": index, "retention_pct": retention})
    return result
