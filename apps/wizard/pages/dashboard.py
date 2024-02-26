"""Create a dashboard with the main information about ETL steps, and the possibility to update them.

"""
import subprocess
from datetime import datetime

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder

from apps.step_update.cli import StepUpdater
from etl.config import WIZARD_IS_REMOTE

# TODO:
# Add columns:
#  * "updatability":
#    * "up-to-date": The step is up to date if update_period_days is 0 or all step's dependencies are on their latest version.
#    * "minor": The step can have a minor update if update_period_days is not 0 and any dependency is not latest while all snapshots are latest.
#    * "major": The step needs a major update if update_period_days is not 0 and any snapshot is not latest.
#    * "archivable": The step is archivable if the step has no chart, and the same step exists in a newer version.
#  * Consider creating a script to regularly check for snapshot updates, fetch them and add them to the temporary dag (this is the way that the "updatability" will know if there are snapshot updates available).
#  * Define a metric of update prioritisation, based on number of charts (or views) and days to update. Sort steps table by this metric.

# Current date.
# This is used as the default version of new steps to be created.
# It is also used to calculate the number of days until the next expected update.
TODAY = datetime.now().strftime("%Y-%m-%d")

# CONFIG
st.set_page_config(
    page_title="Wizard: ETL Dashboard",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
)

st.title("📋 ETL Dashboard")
st.markdown(
    """\
## Control panel for ETL updates.
This dashboard lets you explore all active ETL steps, and, if you are working on your local machine, update them.

🔨 To update some steps, select them from the _Steps table_ and add them to the _Operations list_ below.

💡 If you want to update, say, a specific grapher dataset, you can select just that step to the _Operations list_, then click on "Add dependencies", and bulk-update them all in one go.

### Steps table
"""
)
# add_indentation()


def add_days_to_update_columns(steps_df):
    df = steps_df.copy()
    # There is no clear way to show the expected date of update of a dataset.
    # One could simply add the version to update_days_period.
    # But a dataset may have had a minor update since the main release.
    # So, it would be better to count from the date_published of its snapshots.
    # However, if there are multiple snapshots, it is not clear which one to use as a reference.
    # For example, if a dataset uses the income groups dataset, that would have a date_published that is irrelevant.
    # So, we could create a table of estimated dates of update. But then we would need some manual way to change them.
    # For now, calculate the number of days to the next expected update simply based on the step version.

    # Extract version from steps data frame, and make it a string.
    version = df["version"].copy().astype(str)
    # Assume first of January to those versions that are only given as years.
    filter_years = (version.str.len() == 4) & (version.str.isdigit())
    version[filter_years] = version[filter_years] + "-01-01"
    # Convert version to datetime where possible, setting "latest" to NaT.
    version_date = pd.to_datetime(version, errors="coerce", format="%Y-%m-%d")

    # Extract update_period_days from steps dataframe, ensuring it is numeric, or NaT where it was None.
    update_period_days = pd.to_numeric(df["update_period_days"], errors="coerce")

    # Create a column with the date of next update where possible.
    df["date_of_next_update"] = None
    filter_dates = (version_date.notnull()) & (update_period_days > 0)
    df.loc[filter_dates, "date_of_next_update"] = (
        version_date[filter_dates] + pd.to_timedelta(update_period_days[filter_dates], unit="D")
    ).dt.strftime("%Y-%m-%d")

    # Create a column with the number of days until the next update.
    df["days_to_update"] = None
    df.loc[filter_dates, "days_to_update"] = (
        pd.to_datetime(df.loc[filter_dates, "date_of_next_update"]) - pd.to_datetime(TODAY)
    ).dt.days

    return df


@st.cache_data
def load_steps_df():
    # Load steps dataframe.
    steps_df = StepUpdater().steps_df

    # Fix some columns.
    steps_df["full_path_to_script"] = steps_df["full_path_to_script"].fillna("").astype(str)
    steps_df["dag_file_path"] = steps_df["dag_file_path"].fillna("").astype(str)
    # Add column with the number of days until the next expected update.
    steps_df = add_days_to_update_columns(steps_df=steps_df)

    return steps_df


# Load the steps dataframe.
steps_df = load_steps_df()

# Prepare dataframe to be displayed in the dashboard.
df = steps_df[
    [
        "step",
        "db_dataset_name",
        "n_charts",
        "days_to_update",
        "date_of_next_update",
        "kind",
        "namespace",
        "version",
        "channel",
        "name",
        # "dag_file_name",
        # "n_versions",
        "update_period_days",
        "db_dataset_id",
        # "state",
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
show_all_channels = not st.checkbox("Select only grapher steps with charts, and explorer steps", True)

if show_all_channels:
    # If the toggle is checked, show all data
    df = df
else:
    # Otherwise, pre-filter the DataFrame to show only rows where "channel" equals "grapher"
    df = df[(df["channel"].isin(["grapher", "explorers"])) & (df["n_charts"] > 0)]

# Sort displayed data conveniently.
df = df.sort_values(by=["n_charts", "kind", "version"], ascending=[False, False, True])

# Define the width of some columns in the main grid table (to avoid them from taking too much space).
COLUMN_WIDTHS = {
    "step": 400,
    "kind": 100,
    "channel": 120,
    "namespace": 150,
    "version": 120,
    "n_versions": 100,
    "n_charts": 120,
    "days_to_update": 180,
    "date_of_next_update": 120,
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
days_to_update_jscode = JsCode(
    """
    function(params){
        if (params.value === undefined || params.value === null) {
            return {
                'color': 'black',
                'backgroundColor': 'yellow'
            }
        } else if (params.value <= 0) {
            return {
                'color': 'black',
                'backgroundColor': 'red'
            }
        } else if (params.value > 0 && params.value < 31) {
            return {
                'color': 'black',
                'backgroundColor': 'orange'
            }
        } else {
            return {
                'color': 'black',
                'backgroundColor': 'green'
            }
        }
    }
    """
)
gb.configure_columns("days_to_update", cellStyle=days_to_update_jscode)
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


def include_all_dependencies(step):
    step_dependencies = steps_df[steps_df["step"] == step]["all_active_dependencies"].item()
    update_operations_list(step_dependencies)


def include_direct_dependencies(step):
    step_dependencies = steps_df[steps_df["step"] == step]["direct_dependencies"].item()
    update_operations_list(step_dependencies)


def include_direct_usages(step):
    step_usages = steps_df[steps_df["step"] == step]["direct_usages"].item()
    update_operations_list(step_usages)


def include_all_usages(step):
    step_usages = steps_df[steps_df["step"] == step]["all_active_usages"].item()
    update_operations_list(step_usages)


def remove_selected_step(step):
    st.session_state["selected_steps"] = [s for s in st.session_state["selected_steps"] if s != step]


st.markdown(
    """\
### Operations list

Add here steps from the _Steps table_ and operate on them.
"""
)
with st.container(border=True):
    # Create an operations list, that contains the steps (selected from the main steps table) we will operate upon.
    if st.session_state.get("selected_steps"):
        for step in st.session_state["selected_steps"]:
            # Define the layout of the list.
            cols = st.columns([1, 3, 1, 1, 1, 1])

            # Define the columns in order (from left to right) as a list of tuples (message, key suffix, function).
            actions = [
                ("🗑️", "remove", remove_selected_step, "Remove this step from the _Operations list_."),
                ("write", None, lambda step: step, ""),
                (
                    "Add direct dependencies",
                    "dependencies_direct",
                    include_direct_dependencies,
                    "Add direct dependencies of this step to the _Operations list_.",
                ),
                (
                    "Add all dependencies",
                    "dependencies_all",
                    include_all_dependencies,
                    "Add all dependencies of this step to the _Operations list_.",
                ),
                (
                    "Add direct usages",
                    "usages_direct",
                    include_all_usages,
                    "Add direct usages of this step to the _Operations list_.",
                ),
                (
                    "Add all usages",
                    "usages_all",
                    include_all_usages,
                    "Add all usages of this step to the _Operations list_.",
                ),
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
            for (action, key_suffix, callback, help), col in zip(actions, cols):
                if action == "write":
                    col.write(callback(step))
                else:
                    col.button(action, key=f"{key_suffix}_{step}", on_click=callback, args=(step,), help=help)
    else:
        st.write("No rows selected for operation.")


def execute_command(cmd):
    # Function to execute a command and get its output.
    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return e.stderr


# Add an expander menu with additional parameters for the update command.
with st.expander("Update parameters", expanded=False):
    dry_run = st.checkbox(
        "Dry run", True, help="If checked, the update command will not write anything to the dag or create any files."
    )
    version_new = st.text_input("New version", value=TODAY, help="Version of the new steps to be created.")

# Button to execute the update command and show its output.
if st.button(
    f"Update {len(st.session_state.get('selected_steps', []))} steps",
    help="Update steps in the _Operations list_.",
    type="primary",
):
    with st.spinner("Executing step updater..."):
        if WIZARD_IS_REMOTE:
            st.error("The update command is not available in the remote version of the wizard.")
            st.stop()
        else:
            # TODO: It would be better to directly use StepUpdater instead of a subprocess.
            command = (
                "etl update "
                + " ".join(st.session_state.get("selected_steps", []))
                + " --non-interactive"
                + f" --step-version-new {version_new}"
            )
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
