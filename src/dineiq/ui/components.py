from __future__ import annotations

from typing import Any
from hashlib import sha256
from html import escape
import math

import pandas as pd
import streamlit as st
from dineiq.ui.reports import report_filename, serialize_report_csv



def render_error(context: str, error: Exception, *, diagnostic_path: str | None = None) -> None:
    """Show safe user guidance while keeping sensitive details out of the page."""
    st.error(f"{context} could not complete.")
    detail = f"Diagnostic details were written to {diagnostic_path}." if diagnostic_path else "Check the configured inputs and application logs."
    st.caption(f"{type(error).__name__}: {detail}")


def render_empty(message: str = "No matching records are available for this view and filter selection.") -> None:
    """Render the common empty-state treatment used by all pages."""
    st.info(message)


def _cell_text(value: Any) -> str:
    if pd.api.types.is_scalar(value) and pd.isna(value):
        return ""
    return str(value)


def complete_table_html(frame: pd.DataFrame, height: int) -> str:
    """Escape all values, retaining full precision and wrapping long text."""
    header = "".join(f"<th scope='col'>{escape(str(column))}</th>" for column in frame)
    rows = "".join("<tr>" + "".join(f"<td>{escape(_cell_text(value))}</td>" for value in row) + "</tr>"
                   for row in frame.itertuples(index=False, name=None))
    return (f"<div class='dineiq-table-scroll' tabindex='0' role='region' aria-label='Table values' style='max-height:{int(height)}px'>"
            f"<table class='dineiq-data-table'><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table></div>")


def render_frame(title: str, frame: pd.DataFrame, *, height: int = 360, limit: int = 2000,
                 allow_export: bool = True, key: str | None = None) -> None:
    """Show complete cells with local scrolling; sort/page without discarding rows."""
    st.subheader(title)
    key = key or "dineiq_table_" + sha256(title.encode()).hexdigest()[:16]
    if frame.empty:
        render_empty()
        if allow_export and len(frame.columns):
            st.download_button("Download table CSV", frame_download_bytes(frame),
                               file_name=report_filename(title), mime="text/csv", key=key + "_csv")
        return
    st.html(f"""<style>
    .st-key-{key}_descending label[data-baseweb="checkbox"] {{
        min-height: 48px; align-items: center; margin-bottom: 0;
    }}
    .st-key-{key}_descending label[data-baseweb="checkbox"] > div:first-child,
    .st-key-{key}_descending label[data-baseweb="checkbox"] > span:first-child {{
        width: 22px !important; height: 22px !important;
        min-width: 22px !important; flex-shrink: 0;
    }}
    </style>""")
    left, right = st.columns([3, 1], vertical_alignment="bottom")
    choices = ["Original order", *list(frame.columns)]
    if st.session_state.get(key + "_sort") not in choices:
        st.session_state.pop(key + "_sort", None)
    sort_column = left.selectbox("Sort table by", choices, key=key + "_sort")
    descending = right.checkbox("Descending", key=key + "_descending")
    ordered = frame
    if sort_column != "Original order":
        try:
            ordered = frame.sort_values(sort_column, ascending=not descending, kind="stable", na_position="last")
        except TypeError:
            ordered = frame.sort_values(sort_column, ascending=not descending, kind="stable", na_position="last", key=lambda values: values.astype(str))
    page_size = max(1, min(int(limit), 200))
    pages = math.ceil(len(ordered) / page_size)
    page = 1
    if pages > 1:
        old_page = st.session_state.get(key + "_page", 1)
        if not isinstance(old_page, (int, float)) or not 1 <= old_page <= pages:
            st.session_state[key + "_page"] = 1
        page = int(st.number_input("Table page", min_value=1, max_value=pages, value=1, step=1, key=key + "_page"))
    start = (page - 1) * page_size
    visible = ordered.iloc[start:start + page_size]
    st.markdown(complete_table_html(visible, max(320, min(int(height), 760))), unsafe_allow_html=True)
    st.caption(f"Rows {start + 1:,}–{start + len(visible):,} of {len(ordered):,}. Complete cell values wrap; scroll inside the table for more columns.")
    if allow_export:
        st.download_button("Download table CSV", frame_download_bytes(ordered),
                           file_name=report_filename(title), mime="text/csv", key=key + "_csv")
        st.caption(f"CSV includes all {len(ordered):,} available matching rows, including every table page.")


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
    return serialize_report_csv(frame)
