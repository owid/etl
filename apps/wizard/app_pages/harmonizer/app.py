import streamlit as st

from .utils import render

# Get session state
path = None
if ("steps" in st.session_state) and ("garden" in st.session_state.steps):
    garden_vars = st.session_state["steps"]["garden"]
    if ("namespace" in garden_vars) and ("meadow_version" in garden_vars) and ("short_name" in garden_vars):
        path = f"data://meadow/{garden_vars['namespace']}/{garden_vars['meadow_version']}/{garden_vars['short_name']}"


# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Entity Harmonizer",
    page_icon="🪄",
)

render(path)
