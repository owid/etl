"""Optional actions to be performed on the selected steps."""

from datetime import datetime

import pandas as pd
import streamlit as st

from apps.wizard.utils.cached import execute_bash_command
from apps.wizard.utils.components import st_horizontal
from etl.config import OWID_ENV

# Current date.
# This is used as the default version of new steps to be created.
TODAY = datetime.now().strftime("%Y-%m-%d")


@st.fragment
def render_operations():
    st.markdown("### Operations")
    cols = st.columns(2, border=True)
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
    # with cols[1]:
    #     render_action_execute(steps_df)

    ####################################################################################################################
    # ARCHIVE STEPS
    ####################################################################################################################
    # Add an expander menu with additional parameters for the ETL command.
    # with st.container(border=True):
    with cols[1]:
        render_action_archive()


def render_action_update():
    """Render container with "Update" action."""
    st.markdown("##### Update steps")
    with st_horizontal(vertical_alignment="center"):
        with st.popover("More settings", icon=":material/settings:"):
            dry_run_update = st.toggle(
                "Dry run",
                True,
                help="If checked, the update command will not write anything to the DAG or create any files.",
            )
            version_new = st.text_input("New version", value=TODAY, help="Version of the new steps to be created.")
        btn_submit = st.button(
            "Run",
            help="Update all steps in the **Operations list**.",
            type="primary",
            icon=":material/play_circle:",
            # use_container_width=True,
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
                cmd_output = execute_bash_command(command)
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


def render_action_execute(steps_df: pd.DataFrame):
    st.markdown("##### Execute steps")
    with st_horizontal(vertical_alignment="center"):
        with st.popover("More settings", icon=":material/settings:"):
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
                help="If checked, run snapshot scripts (if any in the **Operations list**).",
            )
            run_grapher = st.toggle(
                "Run grapher steps",
                False,
                help="If checked, run grapher steps with --grapher (if any in the **Operations list**).",
            )

        btn_etl_run = st.button(
            label="Run",
            help="Run ETL on all data steps in the **Operations list** (and optionally also execute snapshots).",
            type="primary",
            icon=":material/play_circle:",
            # use_container_width=True,
        )

        # Button to execute the update command and show its output.
        if btn_etl_run:
            if OWID_ENV.env_local == "production":
                st.error("Running the ETL is not available in production. Run them locally or in staging.")
                st.stop()
            else:
                with st.spinner("Executing ETL..."):
                    command = _define_command_to_execute_snapshots_and_etl_steps(
                        steps_df=steps_df,
                        dry_run=dry_run_etl,
                        force_only=force_only,
                        run_snapshots=run_snapshots,
                        run_grapher=run_grapher,
                    )
                    cmd_output = execute_bash_command(cmd=command)
                    # Show the output of the command in an expander.
                    with st.expander("Command:", expanded=True):
                        st.text(command)
                        st.text_area("Output", value=cmd_output, height=300, key="cmd_output_area")
                    if "error" not in cmd_output.lower():
                        # Celebrate that the update was successful, why not.
                        st.balloons()
                    # Add a button to close the output expander.
                    st.button("Close", key="acknowledge_cmd_output_etl_run")


def render_action_archive():
    st.markdown("##### Archive steps")
    with st_horizontal(vertical_alignment="center"):
        with st.popover("More settings", icon=":material/settings:"):
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
            "Run",
            help="Move archivable steps in the **Operations list** to their corresponding archive dag.",
            type="primary",
            icon=":material/play_circle:",
            # use_container_width=True,
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
                cmd_output = execute_bash_command(command)
                # Show the output of the command in an expander.
                with st.expander("Command:", expanded=True):
                    st.text(command)
                    st.text_area("Output", value=cmd_output, height=300, key="cmd_output_area")
                if "error" not in cmd_output.lower():
                    # Celebrate that the operation was successful.
                    st.balloons()
                    if not dry_run_archive:  # NOTE: this was previously `dry_run_update`. Replace this back to it if unexpected errors appear.
                        # Reload steps_df to include the new steps.
                        st.session_state["reload_key"] += 1
                # Add a button to close the output expander.
                st.button("Close and reload _Steps table_", key="acknowledge_cmd_output")


def _define_command_to_execute_snapshots_and_etl_steps(
    steps_df: pd.DataFrame,
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
