"""Initiate app."""
import streamlit as st
from st_pages import add_indentation

from apps.wizard import utils

st.session_state["step_name"] = "charts"
APP_STATE = utils.AppState()


def init_app():
    st.set_page_config(
        page_title="Chart revisions baker",
        layout="wide",
        page_icon="🧑‍🍳",
        initial_sidebar_state="collapsed",
        menu_items={
            "Report a bug": "https://github.com/owid/etl/issues/new?assignees=marigold%2Clucasrodes&labels=wizard&projects=&template=wizard-issue---.md&title=wizard%3A+meaningful+title+for+the+issue",
            "About": """
    After the new dataset has been correctly upserted into the database, we need to update the affected charts. This step helps with that. These are the steps (this is all automated):

    - The user is asked to choose the _old dataset_ and the _new dataset_.
    - The user has to establish a mapping between variables in the _old dataset_ and in the _new dataset_. This mapping tells Grapher how to "replace" old variables with new ones.
    - The tool creates chart revisions for all the public charts using variables in the _old dataset_ that have been mapped to variables in the _new dataset_.
    - Once the chart revisions are created, you can review these and submit them to the database so that they become available on the _Approval tool_.

    Note that this step is equivalent to running `etl-match-variables` and `etl-chart-suggester` commands in terminal. Call them in terminal with option `--help` for more details.
    """,
        },
    )
    st.title("🧑‍🍳 Chart revisions baker")
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


def set_session_states():
    # Session states
    if "submitted_datasets" not in st.session_state:
        st.session_state.submitted_datasets = False
    if "submitted_variables" not in st.session_state:
        st.session_state.submitted_variables = False
    if "submitted_revisions" not in st.session_state:
        st.session_state.submitted_revisions = False
    if "show_submission_details" not in st.session_state:
        st.session_state.show_submission_details = False
    if "variable_mapping" not in st.session_state:
        st.session_state.variable_mapping = {}
