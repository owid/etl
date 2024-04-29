"""Initiate app."""
import streamlit as st
from st_pages import add_indentation

from apps.wizard import utils

st.session_state["step_name"] = "charts"
APP_STATE = utils.AppState()


def init_app() -> None:
    st.set_page_config(
        page_title="Wizard: Chart Upgrader",
        layout="wide",
        page_icon="🪄",
        initial_sidebar_state="collapsed",
        menu_items={
            "Report a bug": "https://github.com/owid/etl/issues/new?assignees=marigold%2Clucasrodes&labels=wizard&projects=&template=wizard-issue---.md&title=wizard%3A+meaningful+title+for+the+issue",
            "About": """
    After a new dataset has been added to our database, we need to update the affected charts. These are the steps:

    - Select the _old dataset_ and the _new dataset_.
    - Map old indicators in the _old dataset_ to their corresponding new versions in the _new dataset_. This mapping tells Grapher how to "replace" old indicators with new ones.
    - Review the mapping.
    - Update all chart references
    """,
        },
    )
    st.title("Charts 🌟 **:gray[Upgrader]**")
    st.markdown("Update indicators to their new versions.")
    add_indentation()

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
    utils.set_states(
        {
            "submitted_datasets": False,
            "submitted_indicators": False,
            "submitted_revisions": False,
            "variable_mapping": {},
        }
    )
