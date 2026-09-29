"""Streamlit login gate for named DineIQ roles."""
from __future__ import annotations

import streamlit as st

from dineiq.db.auth import authenticate, user_count
from dineiq.db.permissions import Principal


def login_gate(database_path: str) -> Principal | None:
    if "dineiq_principal" in st.session_state:
        return st.session_state["dineiq_principal"]
    # Keep authentication focused: filters and workspace navigation appear only
    # after a successful database-backed sign-in.
    st.markdown(
        "<style>[data-testid='stSidebar']{display:none!important;}"
        "[data-testid='collapsedControl']{display:none!important;}</style>",
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="dineiq-login-card"><div class="dineiq-login-mark">🍽️</div>'
        '<h1 style="margin:.2rem 0">DineIQ Analytics</h1>'
        '<p class="dineiq-caption">Restaurant intelligence, delivered with clarity.</p></div>',
        unsafe_allow_html=True,
    )
    if user_count(database_path) == 0:
        st.warning("No administrator account exists yet. Create the first administrator from the project terminal.")
        st.code("PYTHONPATH=src ./dineiq_analytics_env/bin/python -m dineiq.cli create-admin")
        st.stop()
    with st.form("dineiq-login", clear_on_submit=True, border=False):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in securely →", type="primary", width="stretch")
    if submitted:
        principal = authenticate(database_path, username, password)
        if principal is None:
            st.error("Sign in failed. Check the username and password or contact an administrator.")
        else:
            st.session_state["dineiq_principal"] = principal
            st.rerun()
    st.stop()
