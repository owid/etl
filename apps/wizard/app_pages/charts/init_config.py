"""Initiate app."""
import streamlit as st

from apps.wizard import utils

st.session_state["step_name"] = "charts"
APP_STATE = utils.AppState()


def init_app() -> None:
    # CONFIGURATION SIDEBAR
    with st.sidebar:
        if APP_STATE.args.run_checks:
            with st.expander("**Environment checks**", expanded=True):
                env_ok = utils._check_env()
                if env_ok:
                    db_ok = utils._check_db()
                    if db_ok:
                        utils._show_environment()


def set_session_states() -> None:
    """Initiate session states."""
    # Session states
    st.session_state.submitted_datasets = st.session_state.get("submitted_datasets", False)
    st.session_state.submitted_variables = st.session_state.get("submitted_variables", False)
    st.session_state.submitted_revisions = st.session_state.get("submitted_revisions", False)
    st.session_state.variable_mapping = st.session_state.get("variable_mapping", {})
