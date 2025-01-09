"""Create a dashboard with the main information about ETL steps, and the possibility to update them."""

import subprocess
from datetime import datetime

import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.dashboard.agrid import make_agrid
from apps.wizard.app_pages.dashboard.utils import (
    NON_UPDATEABLE_IDENTIFIERS,
    _create_html_button,
    check_db,
    load_steps_df,
    load_steps_df_to_display,
)
from apps.wizard.utils.components import st_horizontal
from etl.config import OWID_ENV

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

# Current date.
# This is used as the default version of new steps to be created.
TODAY = datetime.now().strftime("%Y-%m-%d")

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


########################################
# HEADER: title, description
########################################
st.title(":material/tv_gen: ETL Dashboard")

tutorial_html = f"""
💡 Common example: Say you want to update a specific grapher dataset. Then:
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

with st_horizontal():
    st.markdown("Select an ETL step from the table below and perform actions on it.")

    with st.popover("More details"):
        st.markdown(
            "The _Steps table_ lists all the active ETL steps. If you are running Wizard on your local machine, you can select steps from it to perform actions (e.g. archive a dataset)."
        )
        st.markdown(tutorial_html, unsafe_allow_html=True)

########################################
# LOAD STEPS TABLE
########################################
st.markdown("### Steps table")

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
grid_response = make_agrid(steps_df_display)

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


st.markdown("### Details list")
if grid_response["selected_rows"]:
    selected_steps = [row["step"] for row in grid_response["selected_rows"]]
    selected_steps_info = (
        steps_df[steps_df["step"].isin(selected_steps)][
            [
                "step",
                "all_active_dependencies",
                "all_active_usages",
                "updateable_dependencies",
            ]
        ]
        .set_index("step")
        .to_dict(orient="index")
    )
    for selected_step, selected_steps_info in selected_steps_info.items():
        # Display each selected row's data.
        with st.expander(f"Details for step {selected_step}"):
            for item, value in selected_steps_info.items():
                item_name = item.replace("_", " ").capitalize()
                if isinstance(value, list):
                    list_html = (
                        f"<details><summary> {item_name} ({len(value)}) </summary><ol>"
                        + "".join([f"<li>{sub_value}</li>" for sub_value in value])
                        + "</ol></details>"
                    )
                    st.markdown(list_html, unsafe_allow_html=True)
                else:
                    st.text(f"{item_name}: {value}")
else:
    st.markdown(":grey[No rows selected for more details.]")

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
        for index, step in enumerate(st.session_state.selected_steps):
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
                # Create a unique key for the button (if any button is to be created).
                unique_key = f"{key_suffix}_{step}_{index}"
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
                        key=unique_key,
                        on_click=lambda step=step: remove_step(step),
                        help=help_text,
                    )
                # Add related steps
                else:
                    col.button(
                        label=action_name,
                        key=unique_key,
                        on_click=lambda step=step, key_suffix=key_suffix: include_related_steps(step, key_suffix),
                        help=help_text,
                    )

        # Add button to clear the operations list.
        st.button(
            "Clear _Operations list_",
            help="Remove all steps currently in the _Operations list_.",
            type="secondary",
            key="clear_operations_list",
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
            key="remove_non_updateable",
            on_click=remove_non_updateable_steps,
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
            key="replace_with_latest",
            on_click=upgrade_steps_in_operations_list,
        )

    else:
        st.markdown(":grey[_No rows selected for operation..._]")


########################################
# SUBMISSION
########################################

if st.session_state.selected_steps:
    ####################################################################################################################
    # UPDATE STEPS
    ####################################################################################################################
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
            if OWID_ENV.env_local == "production":
                st.error("The update command is not available in production. Update steps locally or in staging.")
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
                    with st.expander("Command:", expanded=True):
                        st.text(command)
                        st.text_area("Output", value=cmd_output, height=300, key="cmd_output_area")
                    if "error" not in cmd_output.lower():
                        # Celebrate that the update was successful, why not.
                        st.balloons()
                        if not dry_run_update:
                            # Reload steps_df to include the new steps.
                            st.session_state["reload_key"] += 1
                    # Add a button to close the output expander.
                    st.button("Close and reload _Steps table_", key="acknowledge_cmd_output")

    ####################################################################################################################
    # EXECUTE SNAPSHOTS AND ETL STEPS
    ####################################################################################################################
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
            run_snapshots = st.toggle(
                "Run snapshot scripts",
                False,
                help="If checked, run snapshot scripts (if any in the _Operations list_).",
            )
            run_grapher = st.toggle(
                "Run grapher steps",
                False,
                help="If checked, run grapher steps with --grapher (if any in the _Operations list_).",
            )

        def define_command_to_execute_snapshots_and_etl_steps(
            dry_run: bool = True,
            force_only: bool = False,
            run_snapshots: bool = False,
            run_grapher: bool = False,
        ):
            # Execute ETL for all steps in the operations list.
            snapshot_steps = [step for step in st.session_state.selected_steps if step.startswith("snapshot://")]
            etl_steps = [step for step in st.session_state.selected_steps if not step.startswith("snapshot://")]

            command = ""
            if run_snapshots:
                # First write a command that will attempt to run all snapshots sequentially.
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

                if run_grapher:
                    # To run grapher steps (i.e. grapher://grapher/... steps) we need to remove the "data://" at the
                    # beginning of the step name, otherwise, grapher://grapher/... steps will be ignored.
                    command = command.replace("data://grapher/", "grapher/")
                    command += " --grapher"

            if command.endswith("&& "):
                command = command[:-3]

            return command

        btn_etl_run = st.button(
            "Run all ETL steps",
            help="Run ETL on all data steps in the _Operations list_ (and optionally also execute snapshots).",
            type="primary",
            use_container_width=True,
        )

        # Button to execute the update command and show its output.
        if btn_etl_run:
            if OWID_ENV.env_local == "production":
                st.error("Running the ETL is not available in production. Run them locally or in staging.")
                st.stop()
            else:
                with st.spinner("Executing ETL..."):
                    command = define_command_to_execute_snapshots_and_etl_steps(
                        dry_run=dry_run_etl,
                        force_only=force_only,
                        run_snapshots=run_snapshots,
                        run_grapher=run_grapher,
                    )
                    cmd_output = execute_command(cmd=command)
                    # Show the output of the command in an expander.
                    with st.expander("Command:", expanded=True):
                        st.text(command)
                        st.text_area("Output", value=cmd_output, height=300, key="cmd_output_area")
                    if "error" not in cmd_output.lower():
                        # Celebrate that the update was successful, why not.
                        st.balloons()
                    # Add a button to close the output expander.
                    st.button("Close", key="acknowledge_cmd_output_etl_run")

    ####################################################################################################################
    # ARCHIVE STEPS
    ####################################################################################################################
    # Add an expander menu with additional parameters for the ETL command.
    with st.container(border=True):
        with st.expander("Additional parameters to archive steps", expanded=False):
            dry_run_archive = st.toggle(
                "Dry run",
                True,
                help="If checked, nothing will be written to the dag.",
            )
            include_usages_archive = st.toggle(
                "Include usages",
                True,
                help="If checked, archive also other archivable steps using the steps selected.",
            )

        btn_archive = st.button(
            "Archive steps (when possible)",
            help="Move archivable steps in the _Operations list_ to their corresponding archive dag.",
            type="primary",
            use_container_width=True,
        )

        # Button to execute the update command and show its output.
        if btn_archive:
            if OWID_ENV.env_local == "production":
                st.error("Archiving is not available in production. Run them locally or in staging.")
                st.stop()
            else:
                with st.spinner("Archiving steps..."):
                    command = "etl archive " + " ".join(st.session_state.selected_steps) + " --non-interactive"
                    if dry_run_archive:
                        command += " --dry-run"
                    if include_usages_archive:
                        command += " --include-usages"
                    cmd_output = execute_command(command)
                    # Show the output of the command in an expander.
                    with st.expander("Command:", expanded=True):
                        st.text(command)
                        st.text_area("Output", value=cmd_output, height=300, key="cmd_output_area")
                    if "error" not in cmd_output.lower():
                        # Celebrate that the operation was successful.
                        st.balloons()
                        if not dry_run_update:
                            # Reload steps_df to include the new steps.
                            st.session_state["reload_key"] += 1
                    # Add a button to close the output expander.
                    st.button("Close and reload _Steps table_", key="acknowledge_cmd_output")
