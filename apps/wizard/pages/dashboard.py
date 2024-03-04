"""Create a dashboard with the main information about ETL steps, and the possibility to update them.

"""
import subprocess
from datetime import datetime
from enum import Enum

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_pages import add_indentation

from apps.step_update.cli import StepUpdater
from etl.config import ADMIN_HOST, ENV_IS_REMOTE
from etl.db import can_connect

########################################
# GLOBAL VARIABLES and SESSION STATE
########################################
# TODO:
#  * Consider creating a script to regularly check for snapshot updates, fetch them and add them to the temporary DAG (this is the way that the "update state" will know if there are snapshot updates available).
#  * Define a metric of update prioritization, based on number of charts (or views) and days to update. Sort steps table by this metric.

# Current date.
# This is used as the default version of new steps to be created.
TODAY = datetime.now().strftime("%Y-%m-%d")

# Define the base URL for the grapher datasets (which will be different depending on the environment).
GRAPHER_DATASET_BASE_URL = f"{ADMIN_HOST}/admin/datasets/"
if not GRAPHER_DATASET_BASE_URL.startswith(("http://", "https://")):
    # Links in the steps table seem to only work if they start with "http://" or "https://".
    # But ADMIN_HOST for staging servers starts with "staging-site-" and is not a valid URL.
    # Therefore, prepend "http://" (not "https://" because the site is not secure, and the browser will block the link).
    GRAPHER_DATASET_BASE_URL = f"http://{GRAPHER_DATASET_BASE_URL}"

# List of dependencies to ignore when calculating the update state.
# This is done to avoid a certain common dependency (e.g. population) to make all steps appear as needing major update.
DEPENDENCIES_TO_IGNORE = [
    # "data://garden/demography/2023-03-31/population",
    "snapshot://hyde/2017/general_files.zip",
]

# Initialise session state
## Selected steps
st.session_state.selected_steps = st.session_state.get("selected_steps", set())


# Define labels for update states.
class UpdateState(Enum):
    UNKNOWN = "Unknown"
    UP_TO_DATE = "No updates known"
    OUTDATED = "Outdated"
    MINOR_UPDATE = "Minor update possible"
    MAJOR_UPDATE = "Major update possible"
    ARCHIVABLE = "Archivable"


########################################
# PAGE CONFIG
########################################
st.set_page_config(
    page_title="Wizard: ETL Dashboard",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
)
add_indentation()

########################################
# TITLE and DESCRIPTION
########################################
st.title("ETL Dashboard 📋 :grey[Control panel for ETL updates]")
st.markdown(
    """\
Explore all active ETL steps, and, if you are working on your local machine, update them.

🔨 To update some steps, select them from the _Steps table_ and add them to the _Operations list_ below.

💡 If you want to update, say, a specific grapher dataset, you can select just that step to the _Operations list_, then click on "Add dependencies", and bulk-update them all in one go.

### Steps table
"""
)
if not can_connect():
    st.error("Unable to connect to grapher DB.")


########################################
# LOAD STEPS TABLE
########################################
@st.cache_data
def load_steps_df_all() -> pd.DataFrame:
    """Generate and load the steps dataframe.

    This is just done once, at the beginning.
    """
    # Load steps dataframe.
    steps_df = StepUpdater().steps_df

    # Fix some columns.
    steps_df["full_path_to_script"] = steps_df["full_path_to_script"].fillna("").astype(str)
    steps_df["dag_file_path"] = steps_df["dag_file_path"].fillna("").astype(str)

    # For convenience, convert days to update to a string, and fill nans with "Unknown".
    # Otherwise when sorting, nans are placed before negative numbers, and hence it's not easy to first see steps that
    # need to be updated more urgently.
    steps_df["days_to_update"] = steps_df["days_to_update"].fillna(9999)

    # For convenience, combine dataset name and url in a single column.
    # This will be useful when creating cells with the name of the dataset as a clickable link.
    # In principle, one can access different columns of the dataframe with UrlCellRenderer
    # (and then hide db_dataset_id column), however, then using "group by" fails.
    # So this is a workaround to allows to have both clickable cells with names, and "group by".
    steps_df["db_dataset_name_and_url"] = [
        f"[{row['db_dataset_name']}]({GRAPHER_DATASET_BASE_URL}{int(row['db_dataset_id'])})"
        if row["db_dataset_name"]
        else None
        for row in steps_df.to_dict(orient="records")
    ]

    steps_df = steps_df.drop(columns=["db_dataset_name", "db_dataset_id"], errors="raise")

    # Add a column with the total number of dependencies that are not their latest version.
    steps_df["n_updateable_dependencies"] = [
        sum(
            [
                not steps_df[steps_df["step"] == dependency]["is_latest"].item()
                for dependency in dependencies
                if dependency not in DEPENDENCIES_TO_IGNORE
            ]
        )
        for dependencies in steps_df["all_active_dependencies"]
    ]
    # Number of snapshot dependencies that are not their latest version.
    steps_df["n_updateable_snapshot_dependencies"] = [
        sum(
            [
                not steps_df[steps_df["step"] == dependency]["is_latest"].item()
                if steps_df[steps_df["step"] == dependency]["channel"].item() == "snapshot"
                else False
                for dependency in dependencies
                if dependency not in DEPENDENCIES_TO_IGNORE
            ]
        )
        for dependencies in steps_df["all_active_dependencies"]
    ]
    # Add a column with the update state.
    # By default, the state is unknown.
    steps_df["update_state"] = UpdateState.UNKNOWN.value
    # If there is a newer version of the step, it is outdated.
    steps_df.loc[~steps_df["is_latest"], "update_state"] = UpdateState.OUTDATED.value
    # If there are any dependencies that are not their latest version, it needs a minor update.
    # NOTE: If any of those dependencies is a snapshot, it needs a major update (defined in the following line).
    steps_df.loc[
        (steps_df["is_latest"]) & (steps_df["n_updateable_dependencies"] > 0), "update_state"
    ] = UpdateState.MINOR_UPDATE.value
    # If there are any snapshot dependencies that are not their latest version, it needs a major update.
    steps_df.loc[
        (steps_df["is_latest"]) & (steps_df["n_updateable_snapshot_dependencies"] > 0), "update_state"
    ] = UpdateState.MAJOR_UPDATE.value
    # If the step does not need to be updated (i.e. update_period_days = 0) or if all dependencies are up to date,
    # then the step is up to date (in other words, we are not aware of any possible update).
    steps_df.loc[
        (steps_df["update_period_days"] == 0)
        | (
            (steps_df["is_latest"])
            & (steps_df["n_updateable_snapshot_dependencies"] == 0)
            & (steps_df["n_updateable_dependencies"] == 0)
        ),
        "update_state",
    ] = UpdateState.UP_TO_DATE.value
    # If a step has no charts and is not the latest version, it is archivable.
    steps_df.loc[(steps_df["n_charts"] == 0) & (~steps_df["is_latest"]), "update_state"] = UpdateState.ARCHIVABLE.value

    # Prepare dataframe to be displayed in the dashboard.
    steps_df = steps_df[
        [
            "step",
            "db_dataset_name_and_url",
            "days_to_update",
            "update_state",
            "n_charts",
            "n_charts_views_7d",
            "n_charts_views_365d",
            "date_of_next_update",
            "namespace",
            "version",
            "channel",
            "name",
            "kind",
            "dag_file_name",
            "n_versions",
            "update_period_days",
            # "state",
            "full_path_to_script",
            "dag_file_path",
            # "versions",
            # "role",
            # "all_usages",
            # "direct_usages",
            # "all_chart_ids",
            # "all_chart_slugs",
            # "all_chart_views_7d",
            # "all_chart_views_365d",
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
    return steps_df


@st.cache_data
def load_steps_df(show_all_channels: bool) -> pd.DataFrame:
    """Load the steps dataframe, and filter it according to the user's choice."""
    # Load all data
    df = load_steps_df_all()

    # If toggle is not shown, pre-filter the DataFrame to show only rows where "channel" equals "grapher"
    if not show_all_channels:
        df = df[((df["channel"] == "grapher") & (df["n_charts"] > 0)) | (df["channel"] == "explorers")]

    # Sort displayed data conveniently.
    df = df.sort_values(
        by=["days_to_update", "n_charts_views_7d", "n_charts", "kind", "version"],
        na_position="last",
        ascending=[True, False, False, False, True],
    )

    return df


# Streamlit UI to let users toggle the filter
show_all_channels = not st.toggle("Select only grapher steps with charts, and explorer steps", True)

# Load the steps dataframe.
steps_df = load_steps_df(show_all_channels)


########################################
# Display STEPS TABLE
########################################
# Define the options of the main grid table with pagination.
gb = GridOptionsBuilder.from_dataframe(steps_df)
gb.configure_grid_options(domLayout="autoHeight", enableCellTextSelection=True)
gb.configure_selection(
    selection_mode="multiple",
    use_checkbox=True,
    rowMultiSelectWithClick=True,
    suppressRowDeselection=False,
    groupSelectsChildren=True,
    groupSelectsFiltered=True,
)
gb.configure_default_column(editable=False, groupable=True, sortable=True, filterable=True, resizable=True)
gb.configure_column("step", headerName="Step", width=500, headerTooltip="Step URI, as it appears in the ETL DAG.")
gb.configure_column(
    "channel", headerName="Channel", width=120, headerTooltip="Channel of the step (e.g. garden or grapher)."
)
gb.configure_column("namespace", headerName="Namespace", width=150, headerTooltip="Namespace of the step.")
gb.configure_column("version", headerName="Version", width=120, headerTooltip="Version of the step.")
gb.configure_column("name", headerName="Step name", width=140, headerTooltip="Short name of the step.")
gb.configure_column("kind", headerName="Kind", width=100, headerTooltip="Kind of step (i.e. public or private).")
gb.configure_column(
    "n_charts", headerName="N. charts", width=120, headerTooltip="Number of charts that use data from the step."
)
gb.configure_column(
    "n_charts_views_7d",
    headerName="7-day views",
    width=140,
    headerTooltip="Number of views of charts that use data from the step in the last 7 days.",
)
gb.configure_column(
    "n_charts_views_365d",
    headerName="365-day views",
    width=140,
    headerTooltip="Number of views of charts that use data from the step in the last 365 days.",
)
gb.configure_column(
    "date_of_next_update",
    headerName="Next update",
    width=140,
    headerTooltip="Date of the next expected OWID update of the step.",
)
gb.configure_column(
    "update_period_days",
    headerName="Update period",
    width=150,
    headerTooltip="Number of days between consecutive OWID updates of the step.",
)
gb.configure_column(
    "dag_file_name",
    headerName="Name of DAG file",
    width=160,
    headerTooltip="Name of the DAG file that defines the step.",
)
gb.configure_column(
    "full_path_to_script",
    headerName="Path to script",
    width=150,
    headerTooltip="Path to the script that creates the ETL snapshot or dataset of this step.",
)
gb.configure_column(
    "dag_file_path",
    headerName="Path to DAG file",
    width=160,
    headerTooltip="Path to the DAG file that defines the step.",
)
gb.configure_column(
    "n_versions",
    headerName="N. versions",
    width=140,
    headerTooltip="Number of (active or archive) versions of the step.",
)
# Create a column with the number of days until the next expected update, colored according to its value.
days_to_update_jscode = JsCode(
    """
    function(params){
        if (params.value <= 0) {
            return {
                'color': 'black',
                'backgroundColor': 'red'
            }
        } else if (params.value > 0 && params.value <= 31) {
            return {
                'color': 'black',
                'backgroundColor': 'orange'
            }
        } else if (params.value > 31) {
            return {
                'color': 'black',
                'backgroundColor': 'green'
            }
        } else {
            return {
                'color': 'black',
                'backgroundColor': 'yellow'
            }
        }
    }
    """
)
gb.configure_columns(
    "days_to_update",
    cellStyle=days_to_update_jscode,
    headerName="Days to update",
    width=120,
    headerTooltip="Number of days until the next expected OWID update of the step (if negative, an update is due).",
)
# Create a column colored depending on the update state.
update_state_jscode = JsCode(
    f"""
function(params){{
    if (params.value === "{UpdateState.UP_TO_DATE.value}") {{
        return {{'color': 'black', 'backgroundColor': 'green'}}
    }} else if (params.value === "{UpdateState.OUTDATED.value}") {{
        return {{'color': 'black', 'backgroundColor': 'gray'}}
    }} else if (params.value === "{UpdateState.MAJOR_UPDATE.value}") {{
        return {{'color': 'black', 'backgroundColor': 'red'}}
    }} else if (params.value === "{UpdateState.MINOR_UPDATE.value}") {{
        return {{'color': 'black', 'backgroundColor': 'orange'}}
    }} else if (params.value === "{UpdateState.ARCHIVABLE.value}") {{
        return {{'color': 'black', 'backgroundColor': 'blue'}}
    }} else {{
        return {{'color': 'black', 'backgroundColor': 'yellow'}}
    }}
}}
"""
)
gb.configure_columns(
    "update_state",
    headerName="Update state",
    cellStyle=update_state_jscode,
    width=150,
    headerTooltip=f'Update state of the step: "{UpdateState.UP_TO_DATE.value}" (up to date), "{UpdateState.MINOR_UPDATE.value}" (a dependency is outdated, but all origins are up to date), "{UpdateState.MAJOR_UPDATE.value}" (an origin is outdated), "{UpdateState.OUTDATED.value}" (there is a newer version of the step), "{UpdateState.ARCHIVABLE.value}" (the step is outdated and not used in charts, therefore can safely be archived).',
)
# Create a column with grapher dataset names that are clickable and open in a new tab.
grapher_dataset_jscode = JsCode(
    r"""
    class UrlCellRenderer {
    init(params) {
        this.eGui = document.createElement('a');
        if(params.value) {
            const match = params.value.match(/\[(.*?)\]\((.*?)\)/);
            if(match && match.length >= 3) {
                const datasetName = match[1];
                const datasetUrl = match[2];
                this.eGui.innerText = datasetName;
                this.eGui.setAttribute('href', datasetUrl);
            } else {
                this.eGui.innerText = '';
            }
        } else {
            this.eGui.innerText = '';
        }
        this.eGui.setAttribute('style', "text-decoration:none");
        this.eGui.setAttribute('target', "_blank");
    }
    getGui() {
        return this.eGui;
    }
    }
    """
)
gb.configure_column(
    "db_dataset_name_and_url",  # This will be the displayed column
    headerName="Grapher dataset",
    cellRenderer=grapher_dataset_jscode,
    headerTooltip="Name of the grapher dataset (if any), linked to its corresponding dataset admin page.",
)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
grid_options = gb.build()

# Display the grid table with pagination.
grid_response = AgGrid(
    data=steps_df,
    gridOptions=grid_options,
    height=1000,
    width="100%",
    update_mode=GridUpdateMode.MODEL_CHANGED,
    fit_columns_on_grid_load=False,
    allow_unsafe_jscode=True,
    theme="streamlit",
    # The following ensures that the pagination controls are not cropped.
    custom_css={
        "#gridToolBar": {
            "padding-bottom": "0px !important",
        }
    },
)

########################################
# OPERATIONS LIST MANAGEMENT
#
# Add steps based on user selections.
# User can add from checking in the steps table, but also there are some options to add dependencies, usages, etc.
########################################


# Execute command to update selected steps.
@st.cache_data(show_spinner=False)
def execute_command(cmd):
    """Execute a command and get its output.

    TODO: This is not ideal. Shouldn't be executing commands in terminal.
    """
    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return e.stderr


# Button to add selected steps to the Operations list.
if st.button("Add selected steps to the _Operations list_", type="primary"):
    new_selected_steps = set(row["step"] for row in grid_response["selected_rows"])
    st.session_state.selected_steps |= new_selected_steps


def include_related_steps(step: str, column_related: str):
    """User can add additional steps to the operations list based on the selected step.

    E.g. adding direct dependencies, all usages, etc.
    """
    steps_related = set(steps_df[steps_df["step"] == step][column_related].item())
    st.session_state.selected_steps |= steps_related


# Header
st.markdown(
    """### Operations list

Add here steps from the _Steps table_ and operate on them.
"""
)

with st.container(border=True):
    # Create an operations list, that contains the steps (selected from the main steps table) we will operate upon.
    # Note: Selected steps might contain steps other those selected in the main steps table, based on user selections (e.g. dependencies).
    if st.session_state.selected_steps:
        for step in st.session_state.selected_steps:
            # Define the layout of the list.
            cols = st.columns([0.5, 3, 1, 1, 1, 1])

            # Define the columns in order (from left to right) as a list of tuples (message, key suffix, function).
            actions = [
                ("🗑️", "remove", "Remove this step from the _Operations list_."),
                (None, "write", ""),
                (
                    "Add direct dependencies",
                    "direct_dependencies",
                    "Add direct dependencies of this step to the _Operations list_.",
                ),
                (
                    "Add all dependencies",
                    "all_active_dependencies",
                    "Add all dependencies of this step to the _Operations list_.",
                ),
                (
                    "Add direct usages",
                    "direct_usages",
                    "Add direct usages of this step to the _Operations list_.",
                ),
                (
                    "Add all usages",
                    "all_active_usages",
                    "Add all usages of this step to the _Operations list_.",
                ),
            ]

            # TODO: Consider adding step buttons to:
            #  * Execute ETL step for only the current step.
            #  * Edit metadata for the current step.
            # TODO: Consider adding bulk buttons to:
            #  * Sort them in ETL execution order.
            #  * Select the steps currently in the operation list in the main table (to see their attributes).
            #  * Execute ETL for all steps in the operation list.

            # Display the operations list.
            for (action_name, key_suffix, help_text), col in zip(actions, cols):
                # Write step URI
                if key_suffix == "write":
                    col.text(step)
                # Remove step
                elif key_suffix == "remove":
                    col.button(
                        label=action_name,
                        key=f"{key_suffix}_{step}",
                        on_click=lambda step=step: st.session_state.selected_steps.discard(step),
                        help=help_text,
                    )
                # Add relared steps
                else:
                    col.button(
                        label=action_name,
                        key=f"{key_suffix}_{step}",
                        on_click=lambda step=step, key_suffix=key_suffix: include_related_steps(step, key_suffix),
                        help=help_text,
                    )
        # Add button to clear the operations list.
        st.button(
            "Clear Operations list",
            help="Remove all steps currently in the _Operations list_.",
            type="secondary",
            on_click=lambda: st.session_state.selected_steps.clear(),
        )

    else:
        st.markdown("_No rows selected for operation..._")


########################################
# SUBMISSION
########################################

if st.session_state.selected_steps:
    # Add an expander menu with additional parameters for the update command.
    with st.container(border=True):
        with st.expander("Additional parameters", expanded=False):
            dry_run = st.toggle(
                "Dry run",
                True,
                help="If checked, the update command will not write anything to the DAG or create any files.",
            )
            version_new = st.text_input("New version", value=TODAY, help="Version of the new steps to be created.")

        btn_submit = st.button(
            f"Update {len(st.session_state.selected_steps)} steps",
            help="Update steps in the _Operations list_.",
            type="primary",
            use_container_width=True,
        )

        # Button to execute the update command and show its output.
        if btn_submit:
            if ENV_IS_REMOTE:
                st.error(
                    "The update command is not available in the remote version of the wizard. Update steps locally."
                )
                st.stop()
            else:
                with st.spinner("Executing step updater..."):
                    # TODO: It would be better to directly use StepUpdater instead of a subprocess.
                    command = (
                        "etl update "
                        + " ".join(st.session_state.selected_steps)
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
