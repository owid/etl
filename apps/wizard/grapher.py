import datetime as dt
from pathlib import Path
from typing import Any, Dict

import streamlit as st
from pydantic import BaseModel

from apps.wizard import utils

#########################################################
# CONSTANTS #############################################
#########################################################
# Page config
st.set_page_config(page_title="Wizard (grapher)", page_icon="🪄")
# Get current directory
CURRENT_DIR = Path(__file__).parent
# FIELDS FROM OTHER STEPS
SESSION_STATE = utils.SessionState("grapher")

#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(CURRENT_DIR / "grapher.md", "r") as f:
        return f.read()


class GrapherForm(BaseModel):
    short_name: str
    namespace: str
    version: str
    garden_version: str
    add_to_dag: bool
    dag_file: str
    is_private: bool

    def __init__(self, **data: Any) -> None:
        data = self.filter_relevant_fields(data)
        data["add_to_dag"] = data["dag_file"] != utils.ADD_DAG_OPTIONS[0]
        super().__init__(**data)

    def filter_relevant_fields(self, data: Any) -> Dict[str, Any]:
        return {k.replace("grapher.", ""): v for k, v in data.items() if k.startswith("grapher.")}


#########################################################
# MAIN ##################################################
#########################################################
# TITLE
st.title("Wizard  **:gray[Grapher]**")

# SIDEBAR
with st.sidebar:
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()
        st.markdown(text)

# FORM
with st.form("grapher"):
    namespace = st.text_input(
        "Namespace",
        help="Institution or topic name",
        placeholder="Example: 'emdat', 'health'",
        value=SESSION_STATE.default_value("grapher.namespace"),
        key="grapher.namespace",
    )
    version_grapher = st.text_input(
        "Grapher dataset version",
        help="Version of the grapher dataset (by default, the current date, or exceptionally the publication date).",
        key="grapher.version",
        value=SESSION_STATE.default_value("grapher.version", default_last=utils.DATE_TODAY),
    )
    short_name_grapher = st.text_input(
        "Garden dataset short name",
        help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
        placeholder="Example: 'cherry_blossom'",
        key="grapher.short_name",
        value=SESSION_STATE.default_value("grapher.short_name"),
    )

    st.markdown("#### Dependencies")
    if (default_version := SESSION_STATE.default_value("grapher.garden_version")) == "":
        default_version = SESSION_STATE.default_value("grapher.version", default_last=utils.DATE_TODAY)
    version_snap = st.text_input(
        label="Garden dataset version",
        help="Version of the garden dataset (by default, the current date, or exceptionally the publication date).",
        # placeholder=f"Example: {DATE_TODAY}",
        key="grapher.garden_version",
        value=default_version,
    )

    st.markdown("#### Others")
    dag_selected = SESSION_STATE.default_value("grapher.dag_file")
    dag_index = utils.ADD_DAG_OPTIONS.index(dag_selected) if dag_selected in utils.ADD_DAG_OPTIONS else 0
    dag_file = st.selectbox(
        label="Add to DAG",
        options=utils.ADD_DAG_OPTIONS,
        index=dag_index,
        key="grapher.dag_file",
        help="Add ETL step to a DAG file. This will allow it to be tracked and executed by the `etl` command.",
    )
    private = st.toggle(
        label="Make dataset private",
        key="grapher.is_private",
        value=SESSION_STATE.default_value("grapher.is_private", default_last=False),
    )

    # Submit
    submitted = st.form_submit_button(
        "Submit",
        type="primary",
        use_container_width=True,
        on_click=SESSION_STATE.update,
    )

# st.session_state["DEBUG_M"] = st.session_state.get("snapshot.namespace", "")
# st.write(st.session_state)
# st.write(SESSION_STATE.states)
# print("Meadow: fin")

if submitted:
    st.divider()
    st.write(st.session_state)
