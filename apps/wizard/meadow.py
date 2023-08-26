import datetime as dt
from pathlib import Path

import streamlit as st

from apps.wizard import utils

#########################################################
# CONSTANTS #############################################
#########################################################
# Page config
st.set_page_config(page_title="Wizard (meadow)", page_icon="🪄")
# Get current directory
CURRENT_DIR = Path(__file__).parent
# Default values
DATE_TODAY = dt.date.today().strftime("%Y-%m-%d")


#########################################################
# FUNCTIONS & CLASSES ###################################
#########################################################
@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(CURRENT_DIR / "meadow.md", "r") as f:
        return f.read()


#########################################################
# MAIN ##################################################
#########################################################
# TITLE
st.title("Wizard  **:gray[Meadow]**")

# INSTRUCTIONS
with st.expander("**Instructions**"):
    text = load_instructions()
    st.markdown(text)

# Form
with st.form("meadow"):
    st.markdown("#### Meadow")
    namespace = st.text_input("Namespace", help="Institution or topic name", placeholder="Example: emdat, health")
    version_meadow = st.text_input(
        "Meadow dataset version", help="Institution or topic name", placeholder=f"Example: {DATE_TODAY}"
    )
    short_name_meadow = st.text_input(
        "Meadow dataset short name", help="Institution or topic name", placeholder="Example: cherry_blossom"
    )

    st.markdown("#### Snapshot")
    version_snap = st.text_input(
        "Snapshot version",
        help="Version of the snapshot dataset (by default, the current date, or exceptionally the publication date).",
        placeholder=f"Example: {DATE_TODAY}",
    )
    file_extension = st.text_input(
        "Snapshot version",
        help="File extension (without the '.') of the file to be downloaded.",
        placeholder="Example: csv, xls, zip",
    )

    st.markdown("#### Others")
    add_to_dag = st.selectbox("Add to DAG", utils.ADD_DAG_OPTIONS, index=0)
    playground = st.checkbox("Generate playground notebook", value=True)
    private = st.checkbox("Make dataset private", value=False)

    # Submit
    submitted = st.form_submit_button("Submit", type="primary", use_container_width=True)
