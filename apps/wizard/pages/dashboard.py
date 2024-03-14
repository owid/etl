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
from structlog import get_logger

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

# List of identifiers of steps that should be considered as non-updateable.
# NOTE: The identifier is the step name without the version (and without the "data://").
NON_UPDATEABLE_IDENTIFIERS = [
    # All population-related datasets.
    "garden/demography/population",
    "garden/gapminder/population",
    "garden/hyde/baseline",
    "garden/un/un_wpp",
    "meadow/gapminder/population",
    "meadow/hyde/baseline",
    "meadow/hyde/general_files",
    "meadow/un/un_wpp",
    "open_numbers/open_numbers/gapminder__systema_globalis",
    "open-numbers/ddf--gapminder--systema_globalis",
    "snapshot/hyde/general_files.zip",
    "snapshot/hyde/baseline.zip",
    "snapshot/gapminder/population.xlsx",
    "snapshot/un/un_wpp.zip",
    # Regions dataset.
    "garden/regions/regions",
    # Old WB income groups.
    "garden/wb/wb_income",
    "meadow/wb/wb_income",
    "walden/wb/wb_income",
    # New WB income groups.
    "garden/wb/income_groups",
    "meadow/wb/income_groups",
    "snapshot/wb/income_groups.xlsx",
    # World Bank country shapes.
    "snapshot/countries/world_bank.zip",
    # Other steps we don't want to update (because the underlying data does not get updated).
    # TODO: We need a better way to achieve this, for example adding update_period_days to all steps and snapshots.
    #  A simpler alternative would be to move these steps to a separate file in a meaningful place.
    #  Another option is to have "playlists", e.g. "climate_change_explorer" with the identifiers of steps to update.
    "meadow/epa/ocean_heat_content",
    "snapshot/epa/ocean_heat_content_annual_world_700m.csv",
    "snapshot/epa/ocean_heat_content_annual_world_2000m.csv",
    "garden/epa/ocean_heat_content",
    "meadow/epa/ocean_heat_content",
    "meadow/epa/ice_sheet_mass_balance",
    "snapshot/epa/ice_sheet_mass_balance.csv",
    "garden/epa/ice_sheet_mass_balance",
    "meadow/epa/ice_sheet_mass_balance",
    "meadow/epa/ghg_concentration",
    "snapshot/epa/co2_concentration.csv",
    "snapshot/epa/ch4_concentration.csv",
    "snapshot/epa/n2o_concentration.csv",
    "garden/epa/ghg_concentration",
    "meadow/epa/ghg_concentration",
    "meadow/epa/mass_balance_us_glaciers",
    "snapshot/epa/mass_balance_us_glaciers.csv",
    "garden/epa/mass_balance_us_glaciers",
    "meadow/epa/mass_balance_us_glaciers",
    "meadow/climate/antarctic_ice_core_co2_concentration",
    "snapshot/climate/antarctic_ice_core_co2_concentration.xls",
    "garden/climate/antarctic_ice_core_co2_concentration",
    "meadow/climate/antarctic_ice_core_co2_concentration",
    "meadow/climate/global_sea_level",
    "snapshot/climate/global_sea_level.csv",
    "garden/climate/global_sea_level",
    "meadow/climate/global_sea_level",
]

# List of dependencies to ignore when calculating the update state.
# This is done to avoid a certain common dependency (e.g. hyde) to make all steps appear as needing a major update.
DEPENDENCIES_TO_IGNORE = [
    "snapshot://hyde/2017/general_files.zip",
]

# Initialise session state
## Selected steps
st.session_state.selected_steps = st.session_state.get("selected_steps", [])
## Selected steps in table
st.session_state.selected_steps_table = st.session_state.get("selected_steps_table", [])
# Initialize the cache key in the session state.
# This key will be used to reload the steps table after making changes to the steps.
if "reload_key" not in st.session_state:
    st.session_state["reload_key"] = 0

# Logging
log = get_logger()


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
st.title("ETL Dashboard 📋 :grey[Control panel for ETL steps]")
st.markdown(
    """\
Explore all active ETL steps, and, if you are working on your local machine, perform some actions.

🔨 To perform actions on some steps, select them from the _Steps table_ and add them to the _Operations list_ below.
"""
)


def _create_html_button(text, border_color, background_color):
    html = f"""\
        <div style="border: 1px solid {border_color}; padding: 4px; display: inline-block; border-radius: 10px; background-color: {background_color}; cursor: pointer;">
            {text}
        </div>
"""
    return html


tutorial_html = f"""
<details>
<summary>💡 Common example: Say you want to update a specific grapher dataset. Then:</summary>
<ol>
    <li>Select that step from the <i>Steps table</i>.</li>
    <li>Click on{_create_html_button("Add selected steps to the <i>Operations list</i>", "#FE4446", "#FE4446")}.</li>
    <li>Click on{_create_html_button("Add all dependencies", "#d3d3d3", "#000000")} (and optionally click on {_create_html_button("Remove non-updateable", "#d3d3d3", "#000000")}).</li>
    <li>Click on{_create_html_button("Update X steps", "#FE4446", "#FE4446")} to bulk-update them all in one go.</li>
    <li>Click on{_create_html_button("Replace steps with their latest version", "#d3d3d3", "#000000")} to populate the <i>Operations list</i> with the newly created steps.</li>
    <li>Click on{_create_html_button("Run all ETL steps (including snapshots)", "#FE4446", "#FE4446")} to run the ETL on the new steps.</li>
    <li>If a step fails, you can manually edit its code and try running ETL again.</li>
</ol>
</details>
"""
st.markdown(tutorial_html, unsafe_allow_html=True)

st.markdown("### Steps table")

########################################
# LOAD STEPS TABLE
########################################

if not can_connect():
    st.error("Unable to connect to grapher DB.")


@st.cache_data
def load_steps_df(reload_key: int) -> pd.DataFrame:
    """Generate and load the steps dataframe.

    This is just done once, at the beginning.
    """
    # Ensure that the function is re-run when the reload_key changes.
    _ = reload_key

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

    return steps_df


@st.cache_data
def load_steps_df_to_display(show_all_channels: bool, reload_key: int) -> pd.DataFrame:
    """Load the steps dataframe, and filter it according to the user's choice."""
    # Load all data
    df = load_steps_df(reload_key=reload_key)

    # If toggle is not shown, pre-filter the DataFrame to show only rows where "channel" equals "grapher"
    if not show_all_channels:
        df = df[df["channel"].isin(["grapher", "explorers"])]

    # Sort displayed data conveniently.
    df = df.sort_values(
        by=["days_to_update", "n_charts_views_7d", "n_charts", "kind", "version"],
        na_position="last",
        ascending=[True, False, False, False, True],
    )

    # Prepare dataframe to be displayed in the dashboard.
    df = df[
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
    return df


# Streamlit UI to let users toggle the filter
show_all_channels = not st.toggle("Select only grapher and explorer steps", True)

# Load the steps dataframe.
steps_df = load_steps_df(reload_key=st.session_state["reload_key"])


########################################
# Display STEPS TABLE
########################################
# Get only columns to be shown
steps_df_display = load_steps_df_to_display(show_all_channels, reload_key=st.session_state["reload_key"])

# Define the options of the main grid table with pagination.
gb = GridOptionsBuilder.from_dataframe(steps_df_display)
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
    data=steps_df_display,
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
    """Execute a command and get its output."""
    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return e.stderr


def _add_steps_to_operations(steps_related):
    # Remove those already in operations list
    new_selected_steps = [step for step in steps_related if step not in st.session_state.selected_steps]
    # Add new steps to the operations list.
    st.session_state.selected_steps += new_selected_steps


# Button to add selected steps to the Operations list.
if st.button("Add selected steps to the _Operations list_", type="primary"):
    new_selected_steps = [row["step"] for row in grid_response["selected_rows"]]
    st.session_state.selected_steps_table += new_selected_steps
    _add_steps_to_operations(new_selected_steps)


def include_related_steps(step: str, column_related: str):
    """User can add additional steps to the operations list based on the selected step.

    E.g. adding direct dependencies, all usages, etc.
    """
    steps_related = steps_df[steps_df["step"] == step]
    if len(steps_related) == 0:
        log.error(f"Step {step} not found in the steps table.")
    elif len(steps_related) == 1:
        steps_related = steps_df[steps_df["step"] == step][column_related].item()
        # Add steps to operations list
        _add_steps_to_operations(steps_related)
    else:
        st.error(f"More than one step found with the same URI {step}!")
        st.stop()


def remove_step(step: str):
    """Remove a step from the operations list."""
    st.session_state.selected_steps.remove(step)
    if step in st.session_state.selected_steps_table:
        st.session_state.selected_steps_table.remove(step)


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
                    "Add direct dependencies of this step to the _Operations list_. Direct dependencies are steps that are loaded directly by the current step.",
                ),
                (
                    "Add all dependencies",
                    "all_active_dependencies",
                    "Add all dependencies (including indirect dependencies) of this step to the _Operations list_. Indirect dependencies are steps that are needed, but not directly loaded, by the current step. In other words: dependencies of dependencies.",
                ),
                (
                    "Add direct usages",
                    "direct_usages",
                    "Add direct usages of this step to the _Operations list_. Direct usages are those steps that load the current step directly.",
                ),
                (
                    "Add all usages",
                    "all_active_usages",
                    "Add all usages (including indirect usages) of this step to the _Operations list_. Indirect usages are those steps that need, but do not directly load, the current step. In other words: usages of usages.",
                ),
            ]

            # TODO: Consider adding step buttons to:
            #  * Execute ETL step for only the current step.
            #  * Edit metadata for the current step.

            # Display the operations list.
            for (action_name, key_suffix, help_text), col in zip(actions, cols):
                # Write step URI
                if key_suffix == "write":
                    if step in st.session_state.selected_steps_table:
                        col.markdown(f"**{step}**")
                    else:
                        col.markdown(step)
                # Remove step
                elif key_suffix == "remove":
                    col.button(
                        label=action_name,
                        key=f"{key_suffix}_{step}",
                        on_click=lambda step=step: remove_step(step),
                        help=help_text,
                    )
                # Add related steps
                else:
                    col.button(
                        label=action_name,
                        key=f"{key_suffix}_{step}",
                        on_click=lambda step=step, key_suffix=key_suffix: include_related_steps(step, key_suffix),
                        help=help_text,
                    )

        # Add button to clear the operations list.
        st.button(
            "Clear _Operations list_",
            help="Remove all steps currently in the _Operations list_.",
            type="secondary",
            on_click=lambda: st.session_state.selected_steps.clear(),
        )

        def remove_non_updateable_steps():
            # Remove steps that cannot be updated (because update_period_days is set to 0).
            # For convenience, also remove steps that a user most likely doesn't want to update.
            non_updateable_steps = steps_df[
                (steps_df["update_period_days"] == 0) | (steps_df["identifier"].isin(NON_UPDATEABLE_IDENTIFIERS))
            ]["step"].tolist()
            st.session_state.selected_steps = [
                step for step in st.session_state.selected_steps if step not in non_updateable_steps
            ]

        st.button(
            "Remove non-updateable (e.g. population)",
            help="Remove steps that cannot be updated (i.e. with `update_period_days=0`), and other auxiliary datasets, namely: "
            + "\n- ".join(sorted(NON_UPDATEABLE_IDENTIFIERS)),
            type="secondary",
            on_click=remove_non_updateable_steps(),
        )

        def upgrade_steps_in_operations_list():
            new_list = []
            for step in st.session_state.selected_steps:
                step_info = steps_df[steps_df["step"] == step].iloc[0].to_dict()
                step_identifier = step_info["identifier"]
                latest_version = step_info["latest_version"]
                step_latest = steps_df[
                    (steps_df["identifier"] == step_identifier) & (steps_df["version"] == latest_version)
                ]["step"]
                if not step_latest.empty:
                    new_list.append(step_latest.item())
                else:
                    new_list.append(step)

            st.session_state.selected_steps = new_list

        st.button(
            "Replace steps with their latest versions",
            help="Replace steps in the _Operations list_ by their latest version available. You may want to use this button after updating steps, to be able to operate on the newly created steps.",
            type="secondary",
            on_click=upgrade_steps_in_operations_list(),
        )

    else:
        st.markdown(":grey[_No rows selected for operation..._]")


########################################
# SUBMISSION
########################################

if st.session_state.selected_steps:
    cols_primary_buttons = st.columns([2, 2])

    with cols_primary_buttons[0]:
        # Add an expander menu with additional parameters for the update command.
        with st.container(border=True):
            with st.expander("Additional parameters to update steps", expanded=False):
                dry_run_update = st.toggle(
                    "Dry run",
                    True,
                    help="If checked, the update command will not write anything to the DAG or create any files.",
                )
                version_new = st.text_input("New version", value=TODAY, help="Version of the new steps to be created.")

            btn_submit = st.button(
                f"Update {len(st.session_state.selected_steps)} steps",
                help="Update all steps in the _Operations list_.",
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
                        if dry_run_update:
                            command += " --dry-run"
                        cmd_output = execute_command(command)
                        # Show the output of the command in an expander.
                        with st.expander("Command Output:", expanded=True):
                            st.text_area("Output", value=cmd_output, height=300, key="cmd_output_area")
                        if "error" not in cmd_output.lower():
                            # Celebrate that the update was successful, why not.
                            st.balloons()
                            if not dry_run_update:
                                # Reload steps_df to include the new steps.
                                st.session_state["reload_key"] += 1
                        # Add a button to close the output expander.
                        st.button("Close and reload _Steps table_", key="acknowledge_cmd_output")

    with cols_primary_buttons[1]:
        # Add an expander menu with additional parameters for the ETL command.
        with st.container(border=True):
            with st.expander("Additional parameters to run snapshots and ETL steps", expanded=False):
                dry_run_etl = st.toggle(
                    "Dry run",
                    True,
                    help="If checked, no snapshots will be executed, and ETL will be executed in dry-run mode.",
                )
                force_only = st.toggle(
                    "Force run",
                    False,
                    help="If checked, the ETL steps will be forced to be executed (even if they are already executed).",
                )

            def define_command_to_execute_snapshots_and_etl_steps(dry_run: bool = True, force_only: bool = False):
                # Execute ETL for all steps in the operations list.
                snapshot_steps = [step for step in st.session_state.selected_steps if step.startswith("snapshot://")]
                etl_steps = [step for step in st.session_state.selected_steps if not step.startswith("snapshot://")]

                command = ""
                # First attempt to run all snapshots sequentially.
                for snapshot_step in snapshot_steps:
                    # Identify script for current snapshot.
                    script = steps_df[steps_df["step"] == snapshot_step]["full_path_to_script"].item()
                    # Define command to be executed.
                    command += f"python {script} && "

                if dry_run:
                    # If dry_run, we do not want to execute the command, but simply print it.
                    command = f"echo '{command}' && "

                if etl_steps:
                    # Then let ETL run all remaining steps (ETL will decide the order).
                    # Define command to be executed.
                    command += f"etl run {' '.join(etl_steps)} "

                    if dry_run:
                        command += " --dry-run"

                    if force_only:
                        command += " --force --only"

                return command

            btn_etl_run = st.button(
                "Run all ETL steps (including snapshots)",
                help="Execute all snapshots currently in the _Operations list_, and run ETL on all data steps.",
                type="primary",
                use_container_width=True,
            )

            # Button to execute the update command and show its output.
            if btn_etl_run:
                if ENV_IS_REMOTE:
                    st.error("Running the ETL is not available in the remote version of the wizard. Run them locally.")
                    st.stop()
                else:
                    with st.spinner("Executing ETL..."):
                        command = define_command_to_execute_snapshots_and_etl_steps(
                            dry_run=dry_run_etl, force_only=force_only
                        )
                        cmd_output = execute_command(cmd=command)
                        # Show the output of the command in an expander.
                        with st.expander("Command Output:", expanded=True):
                            st.text_area("Output", value=cmd_output, height=300, key="cmd_output_area")
                        if "error" not in cmd_output.lower():
                            # Celebrate that the update was successful, why not.
                            st.balloons()
                        # Add a button to close the output expander.
                        st.button("Close", key="acknowledge_cmd_output_etl_run")
