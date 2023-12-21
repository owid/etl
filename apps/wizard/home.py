"""Home page of wizard."""
import streamlit as st
from st_pages import add_indentation
from streamlit_extras.switch_page_button import switch_page

add_indentation()

st.markdown(
    """
Wizard is a fundamental tool in the workflow of data scientists at OWID. It is used to easily create ETL steps, which are then run by the ETL pipeline to generate datasets in Grapher.

Additionally, it also builds on top of ETL tools to make them more accessible.
"""
)

# ETL Steps
st.markdown("## ETL steps")
st.markdown(
    """
Create an ETL step.
"""
)
pages = [
    "Snapshot",
    "Meadow",
    "Garden",
    "Grapher",
]
for page in pages:
    go_to_page = st.button(f"➡️  {page}")
    if go_to_page:
        switch_page(page)

# Other tools
st.markdown("## Other tools")
st.markdown(
    """
Other helpfull tools in the ETL ecosystem.
"""
)
pages = [
    "Charts",
    "MetaGPT",
    "Dataset Explorer",
]
for page in pages:
    go_to_page = st.button(f"➡️  {page}")
    if go_to_page:
        switch_page(page)
