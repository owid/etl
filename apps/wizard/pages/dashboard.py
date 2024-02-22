"""Create a dashboard with the main information about ETL steps, and the possibility to update them.

"""
import subprocess

import streamlit as st
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_pages import add_indentation

from apps.step_update.cli import StepUpdater

# CONFIG
st.set_page_config(
    page_title="Wizard: ETL Dashboard",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
)

st.title("📋 ETL Dashboard")
add_indentation()


@st.cache_data
def load_steps_df():
    # Load steps dataframe.
    steps_df = StepUpdater().steps_df

    return steps_df


# Load the steps dataframe.
steps_df = load_steps_df()

# Select columns to be displayed in the dashboard.
df = steps_df[["step", "namespace", "channel", "n_versions", "n_charts"]]

# Define the options of the main grid table with pagination.
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_default_column(editable=True, groupable=True, sortable=True, filterable=True, resizable=True)
gb.configure_grid_options(domLayout="autoHeight")
gb.configure_selection(
    selection_mode="multiple",
    use_checkbox=True,
    rowMultiSelectWithClick=True,
    suppressRowDeselection=False,
    groupSelectsChildren=True,
    groupSelectsFiltered=True,
)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)
grid_options = gb.build()

# Display the grid table with pagination.
# TODO: By default it would be good to show only grapher steps, and sort them in some meaningful way.
# TODO: Let VersionTracker.steps_df fetch updatePeriodDays from the datasets table, and add that info to the main table.
# TODO: Consider coloring each row depending on the status of the step (e.g. green for up-to-date, red for outdated).
grid_response = AgGrid(
    df,
    gridOptions=grid_options,
    height=300,
    width="100%",
    update_mode=GridUpdateMode.MODEL_CHANGED,
    fit_columns_on_grid_load=True,
)


def update_operations_list(new_items):
    """Append new items to the operation list, preserving existing order and avoiding duplicates."""
    existing_items = set(st.session_state["selected_steps"])
    for item in new_items:
        if item not in existing_items:
            st.session_state["selected_steps"].append(item)


if "selected_steps" not in st.session_state:
    st.session_state["selected_steps"] = []

# Button to add selected steps to the operation list.
if st.button("Add selected steps to the operation list"):
    new_selected_steps = [row["step"] for row in grid_response["selected_rows"]]
    update_operations_list(new_selected_steps)


def include_dependencies(step):
    step_dependencies = steps_df[steps_df["step"] == step]["all_active_dependencies"].item()
    update_operations_list(step_dependencies)


def include_usages(step):
    step_usages = steps_df[steps_df["step"] == step]["all_active_usages"].item()
    update_operations_list(step_usages)


def remove_selected_step(step):
    st.session_state["selected_steps"] = [s for s in st.session_state["selected_steps"] if s != step]


# Create an operations list, that contains the steps (selected from the main steps table) we will operate upon.
if st.session_state.get("selected_steps"):
    for step in st.session_state["selected_steps"]:
        # Define the layout of the list.
        cols = st.columns([1, 3, 1, 1])

        # Define the columns in order (from left to right) as a list of tuples (message, key suffix, function).
        actions = [
            ("🗑️", "remove", remove_selected_step),
            ("write", None, lambda step: step),
            ("Add dependencies", "deps", include_dependencies),
            ("Add usages", "usages", include_usages),
        ]

        # TODO: Consider adding step buttons to:
        #  * Execute ETL step for only the current step.
        #  * Edit metadata for the current step.
        # TODO: Consider adding bulk buttons to:
        #  * Clear operations list.
        #  * Sort them in ETL execution order.
        #  * Select the steps currently in the operation list in the main table (to see their attributes).
        #  * Execute ETL for all steps in the operation list.

        # Display the operations list.
        for (action, key_suffix, callback), col in zip(actions, cols):
            if action == "write":
                col.write(callback(step))
            else:
                col.button(action, key=f"{key_suffix}_{step}", on_click=callback, args=(step,))
else:
    st.write("No rows selected for operation.")


def execute_command(cmd):
    # Function to execute a command and get its output.
    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return e.stderr


# Button to execute the update command and show its output.
if st.button("Update steps"):
    # TODO: It would be better to directly use StepUpdater instead of a subprocess.
    command = "etl update " + " ".join(st.session_state.get("selected_steps", [])) + " --dry-run --non-interactive"
    cmd_output = execute_command(command)
    # Show the output of the command in an expander.
    with st.expander("Command Output:", expanded=True):
        st.text_area("Output", value=cmd_output, height=300, key="cmd_output_area")
    # Add a button to close the output expander.
    st.button("Close", key="acknowledge_cmd_output")
