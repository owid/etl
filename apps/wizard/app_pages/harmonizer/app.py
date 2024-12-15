import streamlit as st

from .utils import render

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Entity Harmonizer",
    page_icon="🪄",
)

render()
