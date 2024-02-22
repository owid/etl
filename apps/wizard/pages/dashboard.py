"""Create a dashboard with the main information about ETL steps, and the possibility to update them.

"""
import subprocess

import streamlit as st
from st_aggrid import AgGrid, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder

from apps.step_update.cli import StepUpdater
from etl.config import WIZARD_IS_REMOTE

# CONFIG
st.set_page_config(
    page_title="Wizard: ETL Dashboard",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
)

st.title("📋 ETL Dashboard")
st.markdown("Control panel for ETL updates.")
# add_indentation()


@st.cache_data
def load_steps_df():
    # Load steps dataframe.
    steps_df = StepUpdater().steps_df

    # Fix some columns.
    steps_df["full_path_to_script"] = steps_df["full_path_to_script"].fillna("").astype(str)
    steps_df["dag_file_path"] = steps_df["dag_file_path"].fillna("").astype(str)

    return steps_df


# Load the steps dataframe.
steps_df = load_steps_df()

# Prepare dataframe to be displayed in the dashboard.
df = steps_df[
    [
        "step",
        "db_dataset_name",
        "n_charts",
        "kind",
        "namespace",
        "version",
        "channel",
        "name",
        # "dag_file_name",
        # "n_versions",
        "db_dataset_id",
        "state",
        "full_path_to_script",
        "dag_file_path",
        # "versions",
        # "role",
        # "all_usages",
        # "direct_usages",
        # "all_chart_ids",
        # "all_active_dependencies",
        # "all_active_usages",
        # "direct_dependencies",
        # "chart_ids",
        # "same_steps_forward",
        # "all_dependencies",
        # "same_steps_all",
        # "same_steps_latest",
        # "latest_version",
        # "identifier",
        # "same_steps_backward",
        # "n_newer_versions",
        # "db_archived",
        # "db_private",
    ]
]

# Streamlit UI to let users toggle the filter
show_all_channels = st.checkbox("Show All Channels", False)

if show_all_channels:
    # If the toggle is checked, show all data
    df = df
else:
    # Otherwise, pre-filter the DataFrame to show only rows where "channel" equals "grapher"
    df = df[df["channel"] == "grapher"]

# Sort displayed data conveniently.
df = df.sort_values(by=["kind", "version"], ascending=[False, False])

# Define the width of some columns in the main grid table (to avoid them from taking too much space).
COLUMN_WIDTHS = {
    "kind": 100,
    "channel": 120,
    "namespace": 150,
    "version": 120,
    "n_versions": 100,
    "n_charts": 120,
}

# Define the options of the main grid table with pagination.
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_default_column(editable=False, groupable=True, sortable=True, filterable=True, resizable=True)
for column, width in COLUMN_WIDTHS.items():
    gb.configure_column(column, width=width)
gb.configure_grid_options(domLayout="autoHeight")
gb.configure_selection(
    selection_mode="multiple",
    use_checkbox=True,
    rowMultiSelectWithClick=True,
    suppressRowDeselection=False,
    groupSelectsChildren=True,
    groupSelectsFiltered=True,
)
# TODO: Consider coloring each row depending on the status of the step (e.g. green for up-to-date, red for outdated).
cellstyle_jscode = JsCode(
    """
    function(params){
        if (params.value == 'grapher') {
            return {
                'color': 'black',
                'backgroundColor' : 'orange'
        }
        }
        else{
            return{
                'color': 'black',
                'backgroundColor': 'lightpink'
            }
        }
};
"""
)
gb.configure_columns("channel", cellStyle=cellstyle_jscode)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
grid_options = gb.build()

# Display the grid table with pagination.
# TODO: Let VersionTracker.steps_df fetch updatePeriodDays from the datasets table, and add that info to the main table.
grid_response = AgGrid(
    df,
    gridOptions=grid_options,
    height=300,
    width="100%",
    update_mode=GridUpdateMode.MODEL_CHANGED,
    fit_columns_on_grid_load=False,
    allow_unsafe_jscode=True,
    theme="material",
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
dry_run = st.checkbox("Dry run", True)
if st.button("Update steps"):
    if WIZARD_IS_REMOTE:
        st.error("The update command is not available in the remote version of the wizard.")
        st.stop()
    else:
        # TODO: It would be better to directly use StepUpdater instead of a subprocess.
        command = "etl update " + " ".join(st.session_state.get("selected_steps", [])) + " --non-interactive"
        if dry_run:
            command += " --dry-run"
        cmd_output = execute_command(command)
        # Show the output of the command in an expander.
        with st.expander("Command Output:", expanded=True):
            st.text_area("Output", value=cmd_output, height=300, key="cmd_output_area")
        if "error" not in cmd_output.lower():
            # Celebrate that the update was successful, why not.
            st.balloons()
        # Add a button to close the output expander.
        st.button("Close", key="acknowledge_cmd_output")
