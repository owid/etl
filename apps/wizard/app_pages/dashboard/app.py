"""Create a dashboard with the main information about ETL steps, and the possibility to update them."""

import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.dashboard.aggrid import make_agrid
from apps.wizard.app_pages.dashboard.operations import render_operations
from apps.wizard.app_pages.dashboard.preview import render_preview_list
from apps.wizard.app_pages.dashboard.selection import render_selection_list
from apps.wizard.app_pages.dashboard.utils import (
    _create_html_button,
    _get_steps_info,
    check_db,
    load_steps_df,
    load_steps_df_to_display,
)
from apps.wizard.utils.components import st_horizontal

st.set_page_config(
    page_title="Wizard: ETL Dashboard",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
)
########################################
# GLOBAL VARIABLES and SESSION STATE
########################################
# TODO:
#  * Consider creating a script to regularly check for snapshot updates, fetch them and add them to the temporary DAG (this is the way that the "update state" will know if there are snapshot updates available).
#  * Define a metric of update prioritization, based on number of charts (or views) and days to update. Sort steps table by this metric.


# Initialise session state
## Selected steps
st.session_state.setdefault("selected_steps", [])
## Selected steps in table
st.session_state.setdefault("preview_steps", [])
# Initialize the cache key in the session state.
# This key will be used to reload the steps table after making changes to the steps.
st.session_state.setdefault("reload_key", 0)

# Logging
log = get_logger()


########################################
# HEADER: title, description
########################################
st.title(":material/tv_gen: ETL Dashboard")

tutorial_html = f"""
**Example:** Updating a specific Grapher dataset.
<ol>
    <li>Select the step from the table.</li>
    <li>The dataset will be added to a preliminary list, where you can preview it.</li>
    <li>Click on{_create_html_button("Add steps", "#002147", "#002147", "#FFFFFF")}.</li>
    <li>Click on{_create_html_button("Add all dependencies", "#333333", "transparent", "#333333")} (and optionally click on {_create_html_button("Remove non-updateable", "#333333", "transparent", "#333333")}).</li>
    <li>In the <b>Update step</b> section, click on{_create_html_button("Run", "#002147", "#002147", "white")} to bulk-update them all in one go.</li>
    <li>Click on{_create_html_button("Replace steps with their latest version", "#333333", "transparent", "#333333")} to populate the <b>Selection list</b> with the newly created steps.</li>
    <li>In the <b>Execute steps</b> section, click on{_create_html_button("Run", "#002147", "#002147", "white")} to run the ETL on the new steps.</li>
    <li>If a step fails, you can manually edit its code and try running ETL again.</li>
</ol>
"""

with st_horizontal(justify_content="space-between"):
    st.markdown("Select an ETL step from the table below and perform actions on it.")

    with st.popover("More details", icon=":material/help:"):
        st.markdown(
            """The following table lists all the active ETL steps.

If you are running Wizard on your local machine, you can select steps from it to perform actions (e.g. archive a dataset)."""
        )
        st.markdown(tutorial_html, unsafe_allow_html=True)


########################################
# LOAD STEPS TABLE
########################################
# Check if the database is accessible.
_ = check_db()

# Load the steps dataframe.
with st.spinner("Loading steps details from ETL and DB..."):
    steps_df = load_steps_df(reload_key=st.session_state["reload_key"])

# Simplify the steps dataframe to show only the relevant columns.
steps_info = _get_steps_info(steps_df)


########################################
# Display STEPS TABLE
########################################
# Streamlit UI to let users toggle the filter
show_all_channels = not st.toggle("Select only grapher and explorer steps", True)

# Get only columns to be shown
steps_df_display = load_steps_df_to_display(show_all_channels, reload_key=st.session_state["reload_key"])

# Build and display the grid table with pagination.
grid_response = make_agrid(steps_df_display)

########################################
# PREVIEW LIST
#
# Preview of the steps based on user selections.
# Once happy, the user should click on "Add steps" button and proceed to the "Selection list".
########################################

# Obtain list of steps in preview
df_selected = grid_response["selected_rows"]
st.session_state.preview_steps = df_selected["step"].tolist() if df_selected is not None else []

# Preview list
render_preview_list(steps_info)

########################################
# SELECTION LIST MANAGEMENT
#
# Add steps based on user selections.
# User can add from checking in the steps table, but also there are some options to add dependencies, usages, etc.
########################################
# Header
# st.write(selected_steps)
render_selection_list(steps_df)


########################################
# OPERATE ON STEPS (ACTIONS)
########################################

if st.session_state.selected_steps:
    render_operations()
