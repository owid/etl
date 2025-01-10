"""Create a dashboard with the main information about ETL steps, and the possibility to update them."""

import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.dashboard.actions import render_action_archive, render_action_execute, render_action_update
from apps.wizard.app_pages.dashboard.agrid import make_agrid
from apps.wizard.app_pages.dashboard.operations import render_operations_list
from apps.wizard.app_pages.dashboard.preview import render_preview_list
from apps.wizard.app_pages.dashboard.utils import (
    _create_html_button,
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
st.session_state.setdefault("selected_steps_table", [])
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
    <li>Select that step from the <i>Steps table</i>.</li>
    <li>Click on{_create_html_button("Add selected steps to the <i>Operations list</i>", "#002147", "#002147", "#FFFFFF")}.</li>
    <li>Click on{_create_html_button("Add all dependencies", "#333333", "transparent", "#333333")} (and optionally click on {_create_html_button("Remove non-updateable", "#333333", "transparent", "#333333")}).</li>
    <li>Click on{_create_html_button("Update X steps", "#002147", "#002147", "white")} to bulk-update them all in one go.</li>
    <li>Click on{_create_html_button("Replace steps with their latest version", "#333333", "transparent", "#333333")} to populate the <i>Operations list</i> with the newly created steps.</li>
    <li>Click on{_create_html_button("Run all ETL steps", "#002147", "#002147", "white")} to run the ETL on the new steps.</li>
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
check_db()

# Streamlit UI to let users toggle the filter
show_all_channels = not st.toggle("Select only grapher and explorer steps", True)

# Load the steps dataframe.
with st.spinner("Loading steps details from ETL and DB..."):
    steps_df = load_steps_df(reload_key=st.session_state["reload_key"])


########################################
# Display STEPS TABLE
########################################
# Get only columns to be shown
steps_df_display = load_steps_df_to_display(show_all_channels, reload_key=st.session_state["reload_key"])

# Build and display the grid table with pagination.
# st.write(steps_df_display.dtypes)
grid_response = make_agrid(steps_df_display)

########################################
# DETAILS LIST
#
# Preview of the steps based on user selections.
# Once happy, the user should click on "Add steps" button and proceed to the "Operations list".
########################################


df_selected = grid_response["selected_rows"]
render_preview_list(df_selected, steps_df)


########################################
# OPERATIONS LIST MANAGEMENT
#
# Add steps based on user selections.
# User can add from checking in the steps table, but also there are some options to add dependencies, usages, etc.
########################################
# Header
render_operations_list(steps_df)


########################################
# OPERATE ON STEPS (ACTIONS)
########################################

if st.session_state.selected_steps:
    cols = st.columns(3, border=True)
    ####################################################################################################################
    # UPDATE STEPS
    ####################################################################################################################
    # Add an expander menu with additional parameters for the update command.
    # with st.container(border=True):
    with cols[0]:
        render_action_update()

    ####################################################################################################################
    # EXECUTE SNAPSHOTS AND ETL STEPS
    ####################################################################################################################
    # Add an expander menu with additional parameters for the ETL command.
    # with st.container(border=True):
    with cols[1]:
        render_action_execute(steps_df)

    ####################################################################################################################
    # ARCHIVE STEPS
    ####################################################################################################################
    # Add an expander menu with additional parameters for the ETL command.
    # with st.container(border=True):
    with cols[2]:
        render_action_archive()
