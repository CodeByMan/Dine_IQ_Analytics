"""Central Streamlit theme and page configuration for DineIQ."""

from __future__ import annotations

from typing import Final

import streamlit as st


# Neutral enterprise palette: white light mode only.
PRIMARY: Final = "#0B1F3A"
ACCENT: Final = "#2563EB"
SECONDARY: Final = "#64748B"
LIGHT_BG: Final = "#FFFFFF"
OFF_WHITE: Final = "#F8FAFC"
BODY_TEXT: Final = "#0F172A"
HEADER_TEXT: Final = "#64748B"
CHART_COLORS: Final = ("#DC2626", "#16A34A", "#FACC15", "#2563EB", "#F97316", "#7C3AED", "#0891B2")


def configure_page() -> None:
    """Configure Streamlit once, before any other UI element is emitted."""
    st.set_page_config(page_title="DineIQ Analytics", page_icon="🍽️", layout="wide",
                       initial_sidebar_state="collapsed")


def theme_mode() -> str:
    """Return the single supported visual mode.

    This compatibility helper remains available to chart modules, but the
    application no longer exposes a dark/light switcher.
    """
    return "light"


def render_theme_switcher() -> str:
    """Compatibility no-op for callers from the earlier dual-theme shell."""
    return "light"


def apply_theme(mode: str | None = None) -> None:
    """Apply the responsive light-only enterprise theme."""
    canvas = LIGHT_BG
    panel = OFF_WHITE
    text = BODY_TEXT
    border = "#E2E8F0"
    muted = HEADER_TEXT
    st.markdown(
        f"""
        <style>
        :root {{
            --dineiq-primary: {PRIMARY};
            --dineiq-accent: {ACCENT};
            --dineiq-secondary: {SECONDARY};
            --dineiq-light: {canvas};
            --dineiq-off-white: {panel};
            --dineiq-text: {text};
            --dineiq-header: {muted};
        }}
        [data-testid="stAppViewContainer"] {{ background: {canvas}; color: {text}; }}
        [data-testid="stHeader"] {{ background: {canvas}; }}
        [data-testid="stSidebar"] {{ background: {panel}; border-right: 1px solid {border}; }}
        [data-testid="stSidebar"] * {{ color: {text}; }}
        h1, h2, h3 {{ color: {text} !important; letter-spacing: -0.025em; }}
        h4, h5, h6 {{ color: var(--dineiq-secondary) !important; }}
        p, label, [data-testid="stMarkdownContainer"] {{ color: {text}; }}
        [data-testid="stMetric"] {{ background: {panel}; border: 1px solid {border}; padding: 18px 20px; border-radius: 15px; box-shadow: 0 5px 20px rgba(2,24,52,.12); min-height: 106px; overflow: visible; }}
        [data-testid="stMetricLabel"] {{ color: var(--dineiq-secondary) !important; }}
        [data-testid="stMetricValue"] {{ color: {text} !important; font-weight: 700; overflow-wrap: anywhere; white-space: normal !important; line-height: 1.18; }}
        [data-testid="stMetricValue"] > div {{ white-space: normal !important; overflow-wrap: anywhere; }}
        .stButton button, [data-testid="stDownloadButton"] button {{ border-radius: 9px; border: 1px solid var(--dineiq-accent); color: white; background: var(--dineiq-accent); font-weight: 600; min-height: 42px; }}
        .stButton button:hover, [data-testid="stDownloadButton"] button:hover {{ background: var(--dineiq-primary); border-color: var(--dineiq-primary); color: white; }}
        [data-testid="stDataFrame"] {{ background: {panel}; border: 1px solid {border}; border-radius: 13px; }}
        [data-testid="stExpander"] {{ background: {panel}; border: 1px solid {border}; border-radius: 13px; }}
        input, textarea, [data-baseweb="select"] > div {{ border-radius: 10px !important; }}
        hr {{ border-color: {border}; }}
        .dineiq-brand {{ border-left: 6px solid var(--dineiq-accent); padding: 0.65rem 1rem; margin: 0.15rem 0 1.1rem; background: linear-gradient(90deg, rgba(37,99,235,.13), transparent); border-radius: 0 14px 14px 0; }}
        .dineiq-brand h1 {{ margin-bottom: .15rem; }}
        .dineiq-caption {{ color: {muted}; font-size: 0.88rem; }}
        .dineiq-page-kicker {{ color: var(--dineiq-accent); font-weight: 700; letter-spacing: .08em; text-transform: uppercase; font-size: .72rem; }}
        .dineiq-card {{ background: {panel}; border: 1px solid {border}; border-radius: 16px; padding: 1rem; box-shadow: 0 7px 24px rgba(2,24,52,.10); }}
        .dineiq-chart-card {{ background: {panel}; border: 1px solid {border}; border-radius: 16px; padding: 18px 20px 22px; margin: 12px 0 18px; overflow: visible; }}
        .dineiq-chart-title {{ color: {text}; font-size: 1rem; font-weight: 700; margin: 0 0 8px; }}
        .dineiq-login-card {{ width: min(100%, 560px); margin: 0 auto; padding: 2rem; background: {panel}; border: 1px solid {border}; border-radius: 22px; box-shadow: 0 18px 60px rgba(2,24,52,.18); box-sizing: border-box; }}
        .dineiq-login-heading {{ width: min(100%, 560px); margin: 0 auto 1rem; text-align: center; }}
        .dineiq-login-heading h1 {{ color: {PRIMARY} !important; margin: .35rem 0 .25rem; }}
        .dineiq-login-heading p {{ color: {HEADER_TEXT} !important; margin: 0; }}
        .dineiq-login-mark {{ font-size: 2.4rem; }}
        @media (max-width: 900px) {{
          [data-testid="stSidebar"] {{ min-width: 260px; }}
          [data-testid="stHorizontalBlock"] {{ flex-wrap: wrap; gap: .8rem; }}
          [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {{ min-width: min(100%, 240px); flex: 1 1 240px; }}
          [data-testid="stRadio"] > div {{ flex-wrap: wrap; row-gap: .35rem; }}
        }}
        @media (max-width: 640px) {{
          [data-testid="stAppViewContainer"] .main .block-container {{ padding: .8rem .65rem 2rem; }}
          h1 {{ font-size: 1.65rem !important; }} h2 {{ font-size: 1.25rem !important; }}
          [data-testid="stMetric"] {{ min-height: 84px; padding: 12px; }}
          [data-testid="stMetricValue"] {{ font-size: clamp(1rem, 4vw, 1.35rem); }}
          .dineiq-chart-card {{ padding: 12px 10px 16px; margin: 8px 0 14px; }}
          .dineiq-brand {{ margin-top: .2rem; }}
          .dineiq-login-card {{ padding: 1.25rem; }}
          .dineiq-login-heading h1 {{ font-size: 1.65rem !important; }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_brand_header(title: str = "DineIQ Analytics", subtitle: str | None = None) -> None:
    """Render a consistent application heading without embedding page logic."""
    st.markdown(
        f'<div class="dineiq-brand"><h1>{title}</h1>'
        + (f'<div class="dineiq-caption">{subtitle}</div>' if subtitle else "")
        + "</div>",
        unsafe_allow_html=True,
    )
