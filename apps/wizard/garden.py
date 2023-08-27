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
st.set_page_config(page_title="Wizard (garden)", page_icon="🪄")
# Get current directory
CURRENT_DIR = Path(__file__).parent
# FIELDS FROM OTHER STEPS
SESSION_STATE = utils.SessionState("garden")

#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(CURRENT_DIR / "garden.md", "r") as f:
        return f.read()


class GardenForm(BaseModel):
    short_name: str
    namespace: str
    version: str
    meadow_version: str
    add_to_dag: bool
    dag_file: str
    include_metadata_yaml: bool
    generate_notebook: bool
    is_private: bool

    def __init__(self, **data: Any) -> None:
        data = self.filter_relevant_fields(data)
        data["add_to_dag"] = data["dag_file"] != utils.ADD_DAG_OPTIONS[0]
        super().__init__(**data)

    def filter_relevant_fields(self, data: Any) -> Dict[str, Any]:
        return {k.replace("garden.", ""): v for k, v in data.items() if k.startswith("garden.")}


#########################################################
# MAIN ##################################################
#########################################################
# TITLE
st.title("Wizard  **:gray[Garden]**")

# SIDEBAR
with st.sidebar:
    with st.expander("**Instructions**", expanded=True):
        text = load_instructions()
        st.markdown(text)

# FORM
with st.form("garden"):
    namespace = st.text_input(
        "Namespace",
        help="Institution or topic name",
        placeholder="Example: 'emdat', 'health'",
        value=SESSION_STATE.default_value("garden.namespace"),
        key="garden.namespace",
    )
    version_garden = st.text_input(
        "Garden dataset version",
        help="Version of the garden dataset (by default, the current date, or exceptionally the publication date).",
        key="garden.version",
        value=SESSION_STATE.default_value("garden.version", default_last=utils.DATE_TODAY),
    )
    short_name_garden = st.text_input(
        "Garden dataset short name",
        help="Dataset short name using [snake case](https://en.wikipedia.org/wiki/Snake_case). Example: natural_disasters",
        placeholder="Example: 'cherry_blossom'",
        key="garden.short_name",
        value=SESSION_STATE.default_value("garden.short_name"),
    )

    st.markdown("#### Dataset")
    import numpy as np
    version_snap = st.number_input(
        label="Dataset update frequency (days)",
        help="Expected number of days between consecutive updates of this dataset by OWID, typically `30`, `90` or `365`.",
        # placeholder=f"Example: {DATE_TODAY}",
        key="garden.update_period_days",
        value=SESSION_STATE.default_value("garden.update_period_days", default_last=np.nan),
        step=1.,
        min_value=1.,
    )

    st.markdown("#### Dependencies")
    if (default_version := SESSION_STATE.default_value("garden.meadow_version")) == "":
        default_version = SESSION_STATE.default_value("garden.version", default_last=utils.DATE_TODAY)
    version_snap = st.text_input(
        label="Meadow dataset version",
        help="Version of the meadow dataset (by default, the current date, or exceptionally the publication date).",
        # placeholder=f"Example: {DATE_TODAY}",
        key="garden.meadow_version",
        value=default_version,
    )

    st.markdown("#### Others")
    dag_selected = SESSION_STATE.default_value("garden.dag_file")
    dag_index = utils.ADD_DAG_OPTIONS.index(dag_selected) if dag_selected in utils.ADD_DAG_OPTIONS else 0
    dag_file = st.selectbox(
        label="Add to DAG",
        options=utils.ADD_DAG_OPTIONS,
        index=dag_index,
        key="garden.dag_file",
        help="Add ETL step to a DAG file. This will allow it to be tracked and executed by the `etl` command.",
    )
    include_metadata_yaml = st.toggle(
        label="Include *.meta.yaml file with metadata",
        key="garden.include_metadata_yaml",
        value=SESSION_STATE.default_value("meadow.include_metadata_yaml", default_last=True),
    )
    playground = st.toggle(
        label="Generate playground notebook",
        key="garden.generate_notebook",
        value=SESSION_STATE.default_value("meadow.generate_notebook", default_last=True),
    )
    private = st.toggle(
        label="Make dataset private",
        key="garden.is_private",
        value=SESSION_STATE.default_value("garden.is_private", default_last=False),
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
