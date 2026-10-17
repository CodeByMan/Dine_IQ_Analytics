from __future__ import annotations

import hashlib
import hmac
import secrets
import time

import streamlit as st

from dineiq.db.auth import authenticate, user_count
from dineiq.db.permissions import Principal
from dineiq.db.schema import connect_database


_SESSION_PARAMETER = "dineiq_session"
_SESSION_TTL_SECONDS = 12 * 60 * 60
_REFRESH_SESSIONS: dict[str, tuple[int, bytes, float]] = {}


def _query_session_token() -> str:
    """Return the opaque browser refresh token when query parameters are supported."""
    query_params = getattr(st, "query_params", None)
    if query_params is None:
        return ""
    value = query_params.get(_SESSION_PARAMETER, "")
    if isinstance(value, list):
        value = value[-1] if value else ""
    return str(value).strip()


def _remove_query_session_token() -> None:
    query_params = getattr(st, "query_params", None)
    if query_params is not None and _SESSION_PARAMETER in query_params:
        del query_params[_SESSION_PARAMETER]


def _password_fingerprint(password_hash: str) -> bytes:
    return hashlib.sha256(password_hash.encode("utf-8")).digest()


def _account_row(database_path: str, user_id: int):
    with connect_database(database_path) as connection:
        return connection.execute(
            "SELECT user_id,username,password_hash,role,assigned_location_id,is_active "
            "FROM users WHERE user_id=?",
            (user_id,),
        ).fetchone()


def _remember_refresh_session(database_path: str, principal: Principal) -> None:
    """Create a server-held session and expose only its opaque identifier to the browser."""
    query_params = getattr(st, "query_params", None)
    if query_params is None:
        return
    row = _account_row(database_path, principal.user_id)
    if not row or not row["is_active"]:
        return
    token = secrets.token_urlsafe(32)
    _REFRESH_SESSIONS[token] = (
        principal.user_id,
        _password_fingerprint(row["password_hash"]),
        time.time() + _SESSION_TTL_SECONDS,
    )
    query_params[_SESSION_PARAMETER] = token


def _restore_refresh_session(database_path: str) -> Principal | None:
    """Restore a refreshed browser only after revalidating its current database account."""
    now = time.time()
    for expired_token, (_, _, expires_at) in list(_REFRESH_SESSIONS.items()):
        if expires_at <= now:
            _REFRESH_SESSIONS.pop(expired_token, None)

    token = _query_session_token()
    stored = _REFRESH_SESSIONS.get(token) if token else None
    if stored is None:
        if token:
            _remove_query_session_token()
        return None

    user_id, expected_fingerprint, expires_at = stored
    row = _account_row(database_path, user_id)
    valid = bool(
        expires_at > now
        and row
        and row["is_active"]
        and hmac.compare_digest(
            expected_fingerprint,
            _password_fingerprint(row["password_hash"]),
        )
    )
    if not valid:
        _REFRESH_SESSIONS.pop(token, None)
        _remove_query_session_token()
        return None

    _REFRESH_SESSIONS[token] = (user_id, expected_fingerprint, now + _SESSION_TTL_SECONDS)
    return Principal(int(row["user_id"]), row["username"], row["role"], row["assigned_location_id"])


def clear_login_session() -> None:
    """Revoke the current refresh session and clear the authenticated UI state."""
    token = _query_session_token()
    if token:
        _REFRESH_SESSIONS.pop(token, None)
    _remove_query_session_token()
    st.session_state.pop("dineiq_principal", None)


def login_gate(database_path: str) -> Principal | None:
    if "dineiq_principal" in st.session_state:
        principal = st.session_state["dineiq_principal"]
        if not _query_session_token():
            _remember_refresh_session(database_path, principal)
        return principal
    restored = _restore_refresh_session(database_path)
    if restored is not None:
        st.session_state["dineiq_principal"] = restored
        return restored
    # Keep authentication focused: filters and workspace navigation appear only
    # after a successful database-backed sign-in.
    st.markdown(
        "<style>[data-testid='stSidebar']{display:none!important;}"
        "[data-testid='collapsedControl'],[data-testid='stSidebarCollapsedControl']{display:none!important;}"
        "[data-testid='stAppViewContainer']{height:100svh;min-height:100svh;overflow:hidden!important;}"
        "[data-testid='stAppViewContainer']>section.main{height:100svh;overflow:hidden!important;}"
        "[data-testid='stMainBlockContainer']{width:100%!important;max-width:100%!important;min-height:100svh!important;"
        "padding:clamp(1rem,3vh,2rem) clamp(.75rem,4vw,2rem)!important;display:flex!important;"
        "flex-direction:column!important;justify-content:center!important;overflow:hidden!important;}"
        "[data-testid='stMainBlockContainer']>[data-testid='stVerticalBlock'],"
        "[data-testid='stMainBlockContainer']>div:first-child{width:100%!important;min-height:calc(100svh - clamp(2rem,6vh,4rem))!important;"
        "display:flex!important;flex-direction:column!important;justify-content:center!important;}"
        ".st-key-dineiq_login{width:min(100%,560px);max-width:560px;margin:auto!important;min-height:0!important;"
        "padding:0!important;display:flex!important;flex-direction:column!important;justify-content:center!important;}"
        ".st-key-dineiq_login [data-baseweb='input'],"
        ".st-key-dineiq_login [data-testid='stTextInputRootElement']{width:100%!important;max-width:100%!important;"
        "border:1px solid #E2E8F0!important;border-radius:10px!important;box-shadow:none!important;}"
        ".st-key-dineiq_login [data-baseweb='input']:hover,"
        ".st-key-dineiq_login [data-testid='stTextInputRootElement']:hover{border-color:#2563EB!important;"
        "box-shadow:0 0 0 1px #2563EB!important;}"
        ".st-key-dineiq_login [data-baseweb='input']:focus-within,"
        ".st-key-dineiq_login [data-testid='stTextInputRootElement']:focus-within{border-color:#2563EB!important;"
        "outline:0!important;box-shadow:0 0 0 2px #2563EB!important;}"
        ".st-key-dineiq_login input{width:100%!important;min-width:0!important;outline:0!important;"
        "border-color:transparent!important;box-shadow:none!important;}"
        ".st-key-dineiq_login input:hover,.st-key-dineiq_login input:focus{outline:0!important;"
        "border-color:transparent!important;box-shadow:none!important;}"
        "[data-testid='stFormSubmitButton'] button,"
        "[data-testid='stFormSubmitButton'] button:hover,"
        "[data-testid='stFormSubmitButton'] button:focus,"
        "[data-testid='stFormSubmitButton'] button:active"
        "{background:#2563EB!important;border-color:#2563EB!important;color:#FFFFFF!important;transition:none!important;filter:none!important;opacity:1!important;}"
        "[data-testid='stFormSubmitButton'] button *,"
        "[data-testid='stFormSubmitButton'] button:hover *"
        "{color:#FFFFFF!important;}"
        "@media(max-width:640px){"
        "[data-testid='stMainBlockContainer']{padding:1rem .75rem!important;}"
        ".st-key-dineiq_login{width:100%!important;max-width:100%!important;}"
        "}"
        "@media(max-height:520px){"
        "[data-testid='stMainBlockContainer']{justify-content:flex-start!important;padding:.65rem .75rem!important;}"
        "[data-testid='stMainBlockContainer']>[data-testid='stVerticalBlock'],"
        "[data-testid='stMainBlockContainer']>div:first-child{min-height:calc(100svh - 1.3rem)!important;}"
        ".st-key-dineiq_login{width:min(100%,560px);margin:auto!important;}" 
        "}</style>",
        unsafe_allow_html=True,
    )
    if user_count(database_path) == 0:
        st.warning("No administrator account exists yet. Create the first administrator from the project terminal.")
        st.code("PYTHONPATH=src ./dineiq_analytics_env/bin/python -m dineiq.cli create-admin")
        st.stop()
    # Keep the login page limited to the authentication form. The workspace
    # brand header is rendered only after authentication in app.main().
    with st.container(key="dineiq_login"):
        st.markdown(
            '<div style="text-align:center; margin:0 0 1.25rem;">'
            '<h1 style="margin:0; color:#0B1F3A; font-size:clamp(1.6rem,3vw,2.2rem);">'
            'Welcome to DineIQ'
            '</h1></div>',
            unsafe_allow_html=True,
        )
        with st.form("dineiq-login", clear_on_submit=True, border=False):
            username = st.text_input("Username", autocomplete="username")
            password = st.text_input("Password", type="password", autocomplete="current-password")
            submitted = st.form_submit_button("Sign In", type="primary", width="stretch")
    if submitted:
        principal = authenticate(database_path, username, password)
        if principal is None:
            st.error("Sign in failed. Check the username and password or contact an administrator.")
        else:
            st.session_state["dineiq_principal"] = principal
            _remember_refresh_session(database_path, principal)
            st.rerun()
    st.stop()
