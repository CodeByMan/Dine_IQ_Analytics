"""Reusable, data-safe Streamlit presentation components."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterable

import pandas as pd
import streamlit as st

from dineiq.ui.theme import ACCENT, CHART_COLORS, OFF_WHITE, PRIMARY, SECONDARY


def render_error(context: str, error: Exception, *, diagnostic_path: str | None = None) -> None:
    """Show safe user guidance while keeping sensitive details out of the page."""
    st.error(f"{context} could not complete.")
    detail = f"Diagnostic details were written to {diagnostic_path}." if diagnostic_path else "Check the configured inputs and application logs."
    st.caption(f"{type(error).__name__}: {detail}")


def render_empty(message: str = "No matching records are available for this view and filter selection.") -> None:
    """Render the common empty-state treatment used by all pages."""
    st.info(message)


def render_frame(title: str, frame: pd.DataFrame, *, height: int = 360, limit: int = 2000) -> None:
    """Render a bounded dataframe with a deterministic empty state."""
    st.subheader(title)
    if frame.empty:
        render_empty()
        return
    st.dataframe(frame.head(limit), width="stretch", height=height, hide_index=True)
    if len(frame) > limit:
        st.caption(f"Showing the first {limit:,} of {len(frame):,} matching rows for browser responsiveness.")


def render_kpis(entries: Iterable[tuple[str, str]]) -> None:
    """Render a responsive row of KPI cards."""
    entries = list(entries)
    if not entries:
        return
    columns = st.columns(min(len(entries), 4))
    for index, (label, value) in enumerate(entries):
        columns[index % len(columns)].metric(label, value)


def render_status(label: str, passed: bool | None, *, detail: str = "") -> None:
    """Render a compact operational status indicator."""
    if passed is True:
        st.success(f"{label}: PASS")
    elif passed is False:
        st.error(f"{label}: FAIL")
    else:
        st.warning(f"{label}: NOT AVAILABLE")
    if detail:
        st.caption(detail)


def safe_number(value: Any, default: float = 0.0) -> float:
    """Convert nullable artifact values into finite UI-safe numbers."""
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(number) if pd.notna(number) else default


def frame_download_bytes(frame: pd.DataFrame) -> bytes:
    """Serialize a report frame with a stable UTF-8 BOM for spreadsheet clients."""
    return frame.to_csv(index=False).encode("utf-8-sig")


@contextmanager
def chart_card(title: str | None = None):
    """Provide consistent breathing room around every premium chart."""
    with st.container(border=True):
        if title:
            st.markdown(f'<div class="dineiq-chart-title">{title}</div>', unsafe_allow_html=True)
        yield


def render_frame_visual_summary(frame: pd.DataFrame, title: str = "Visual summary") -> None:
    """Render a visible, data-backed chart for an arbitrary table frame."""
    if frame is None or frame.empty:
        return
    numeric = [column for column in frame.columns if pd.api.types.is_numeric_dtype(frame[column])]
    if not numeric:
        return
    measure = numeric[0]
    dimension = next((column for column in frame.columns if column != measure and frame[column].nunique(dropna=True) <= 30), None)
    plot = frame[[measure]].copy() if dimension is None else frame[[dimension, measure]].copy()
    if dimension is None:
        dimension = "Record"
        plot[dimension] = [str(index + 1) for index in range(len(plot))]
    plot[measure] = pd.to_numeric(plot[measure], errors="coerce")
    plot = plot.dropna(subset=[measure])
    if plot.empty:
        return
    chart = plot.groupby(dimension, as_index=False)[measure].sum().sort_values(measure, ascending=False).head(20)
    if chart.empty:
        return
    with chart_card(title):
        st.bar_chart(chart.set_index(dimension)[[measure]], color=CHART_COLORS[3], horizontal=True, width="stretch")
