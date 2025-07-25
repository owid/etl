import difflib
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import click
import structlog
from rapidfuzz import fuzz
from rich_click.rich_command import RichCommand

from etl.dag_helpers import (
    create_dag_archive_file,
    get_comments_above_step_in_dag,
    remove_steps_from_dag_file,
    write_to_dag_file,
)
from etl.paths import BASE_DIR, DAG_ARCHIVE_FILE, DAG_DIR, DAG_TEMP_FILE, SNAPSHOTS_DIR, STEP_DIR
from etl.snapshot import SnapshotMeta
from etl.steps import filter_to_subgraph, to_dependency_order
from etl.version_tracker import DAG_TEMP_STEP, UpdateState, VersionTracker

log = structlog.get_logger()

# If a new version is not specified, assume current date.
STEP_VERSION_NEW = datetime.now().strftime("%Y-%m-%d")


class StepUpdater:
    def __init__(self, dry_run: bool = False, interactive: bool = True):
        # Initialize version tracker and load dataframe of all active steps.
        self._load_version_tracker()
        # If dry_run is True, then nothing will be written to the dag, and no files will be created.
        self.dry_run = dry_run
        # If interactive is True, then the user will be asked for confirmation before updating each step, and on
        # situations where there is some ambiguity.
        self.interactive = interactive

    def _load_version_tracker(self) -> None:
        # This function will start a new instance of VersionTracker, and load the dataframe of all active steps.
        # It can be used when initializing StepUpdater, but also to reload steps_df after making changes to the dag.

        # Initialize version tracker.
        self.tracker = VersionTracker(ignore_archive=True)

        # Update the temporary dag.
        _update_temporary_dag(dag_active=self.tracker.dag_active, dag_all_reverse=self.tracker.dag_all_reverse)

        # Load steps dataframe.
        self.steps_df = self.tracker.steps_df.copy()
        # Select only active steps.
        self.steps_df = self.steps_df[self.steps_df["state"] == "active"].reset_index(drop=True)

    def check_that_step_exists(self, step: str) -> None:
        """Check that step to be updated exists in the active dag."""
        # Get the list of active steps.
        active_steps = sorted(set(self.steps_df["step"]))
        if step not in active_steps:
            error_message = f"Step {step} not found among active steps. Closest matches:\n"
            # NOTE: We could use a better edit distance to find the closest matches.
            for match in difflib.get_close_matches(step, active_steps, n=5, cutoff=0.0):
                error_message += f"{match}\n"
            log.error(error_message)
            sys.exit(1)

    def get_step_info(self, step: str) -> Dict[str, Any]:
        # Get info for step to be updated.
        step_info = self.steps_df[self.steps_df["step"] == step].iloc[0].to_dict()

        return step_info

    def _update_snapshot_step(
        self,
        step: str,
        step_version_new: Optional[str] = STEP_VERSION_NEW,
        step_header: Optional[str] = None,
    ) -> int:
        # Get info for step to be updated.
        step_info = self.get_step_info(step=step)

        # Check that a .dvc file exists for this snapshot step.
        step_dvc_file = SNAPSHOTS_DIR / step_info["namespace"] / step_info["version"] / (step_info["name"] + ".dvc")
        if not step_dvc_file.exists():
            log.error(f"No .dvc file found for step {step}.")
            return 1

        # Define folder for new version.
        folder_new = SNAPSHOTS_DIR / step_info["namespace"] / step_version_new

        # Check that the new dvc file does not exist.
        step_dvc_file_new = folder_new / (step_info["name"] + ".dvc")
        if step_dvc_file_new.exists():
            log.error(f"New .dvc file already exists: {step_dvc_file_new}")
            return 1

        # Find script file for old step.
        _step_py_files = list(step_dvc_file.parent.glob("*.py"))
        if len(_step_py_files) == 1:
            # Usually there is a single .py file with the same name. But it is possible that:
            #  * The single .py file has a different name.
            #    Example: gcp/2023-12-12/global_carbon_budget.py
            #  * There are multiple .py files, corresponding to different snapshots altogether.
            #    Example: animal_welfare/2023-08-01/global_hen_inventory.py and uk_egg_statistics.py
            #  Therefore: If there is a single .py file in the same folder, that's the script.
            step_py_file = _step_py_files[0]
        else:
            #  If there are multiple .py files, choose the one whose name has a shorter edit distance to the step name,
            #  and optionally ask for user confirmation (and allow user to choose the correct file).
            steps_sorted = sorted(
                _step_py_files,
                key=lambda file_name: fuzz.ratio(step_dvc_file_new.stem.split(".")[0], file_name.stem),
                reverse=True,
            )
            steps_names_sorted = [file_name.stem for file_name in steps_sorted]
            log.warning(f"Multiple .py files found for step {step}.")
            if self.interactive:
                choice = _confirm_choice(multiple_files=steps_names_sorted)
            else:
                choice = 0
            step_py_file = steps_sorted[choice]

        # Define the new step.
        step_new = step.replace(step_info["version"], step_version_new)  # type: ignore

        # Define a header for the new step in the dag file.
        if step_header is None:
            step_header = (
                get_comments_above_step_in_dag(step=step, dag_file=step_info["dag_file_path"])
                if step_info["dag_file_path"]
                else ""
            )

        if not self.dry_run:
            # If new folder does not exist, create it.
            folder_new.mkdir(parents=True, exist_ok=True)

            # Load metadata from last step.
            metadata = SnapshotMeta.load_from_yaml(step_dvc_file)
            # Update version and date accessed.
            metadata.version = step_version_new  # type: ignore
            if metadata.origin:
                metadata.origin.date_accessed = step_version_new  # type: ignore
            # Write metadata to new file.
            step_dvc_file_new.write_text(metadata.to_yaml())

            # Check if a new py file already exists.
            step_py_file_new = folder_new / step_py_file.name
            if not step_py_file_new.exists():
                # Create a new py file.
                # NOTE: If there is already a .py file in the new folder, it may be because another dvc file (used by
                # that script) has already been updated. So, simply skip it.
                step_py_file_new.write_bytes(step_py_file.read_bytes())

            # Add the new snapshot as a dependency of the temporary dag.
            step_temp = (
                self.steps_df[self.steps_df["step"] == DAG_TEMP_STEP][["step", "direct_dependencies"]]
                .set_index("step")
                .to_dict()["direct_dependencies"]
            )
            step_temp_name = list(step_temp)[0]
            # Add the new snapshot as a dependency of the temporary step (and make it a set instead of a list).
            step_temp[step_temp_name] = set(step_temp[step_temp_name] + [step_new])
            # Save temporary dag.
            write_to_dag_file(dag_file=DAG_TEMP_FILE, dag_part=step_temp, comments={step_new: step_header})

            # Reload steps dataframe.
            self._load_version_tracker()

        return 0

    def _update_data_step(
        self,
        step: str,
        step_version_new: Optional[str] = STEP_VERSION_NEW,
        step_header: Optional[str] = None,
    ) -> int:
        # Get info for step to be updated.
        step_info = self.get_step_info(step=step)

        # Define the folder of the old step files.
        folder = (
            STEP_DIR / step_info["step_type"] / step_info["channel"] / step_info["namespace"] / step_info["version"]
        )

        if ((folder / step_info["name"]).with_suffix(".py")).is_file():
            # Gather all relevant files from this folder.
            step_files = [
                file_name
                for file_name in list(folder.glob("*"))
                if str(file_name.stem).split(".")[0] in [step_info["name"], "shared"]
            ]
        elif (folder / step_info["name"]).is_dir():
            # Gather all relevant files from this folder.
            step_files = [file_name for file_name in list(folder.glob(f"{step_info['name']}/*")) if file_name.is_file()]
        else:
            log.error(f"No step files found for step {step}.")
            return 1

        # Define the new step.
        if step_info["version"] == "latest":
            # If the step has version "latest", the new step will also have version "latest".
            step_new = step
        else:
            step_new = step.replace(step_info["version"], step_version_new)  # type: ignore

        # Find the latest version of each of the step's dependencies.
        step_dependencies = set(self.steps_df[self.steps_df["step"] == step].iloc[0]["direct_dependencies"])
        step_dependencies_new = set(
            [
                self.steps_df[self.steps_df["step"] == dependency]["same_steps_latest"].item()
                for dependency in step_dependencies
            ]
        )
        # Create a new partial dag with the new step and its dependencies.
        dag_part = {step_new: step_dependencies_new}

        # Identify the dag file for this step.
        dag_file = self.steps_df[self.steps_df["step"] == step].iloc[0]["dag_file_path"]

        # Define a header for the new step in the dag file.
        if step_header is None:
            # Get header from the comment lines right above the current step in the dag.
            step_header = get_comments_above_step_in_dag(step=step, dag_file=dag_file)

        if not self.dry_run:
            # If the step has version "latest", the files do not need to be updated, but the dag may need to be updated.
            if step_info["version"] != "latest":
                # Copy files to new folder.
                for step_file in step_files:
                    # Define the path to the new version of this file.
                    step_file_new = Path(str(step_file).replace(step_info["version"], step_version_new))  # type: ignore

                    # Check that the new file does not exist.
                    if step_file_new.exists():
                        log.error(f"New file already exists: {step_file_new}")
                        return 1

                    # If new folder does not exist, create it.
                    step_file_new.parent.mkdir(parents=True, exist_ok=True)

                    # Create new file.
                    step_file_new.write_bytes(step_file.read_bytes())

            # Add new step and its dependencies to the dag.
            write_to_dag_file(dag_file=dag_file, dag_part=dag_part, comments={step_new: step_header})

            # Reload steps dataframe.
            self._load_version_tracker()

        return 0

    def _update_step(
        self, step: str, step_version_new: Optional[str] = STEP_VERSION_NEW, step_header: Optional[str] = None
    ) -> int:
        """Update step to new version."""

        # Check that step to be updated exists in the active dag.
        self.check_that_step_exists(step=step)

        # Extract channel from step.
        step_channel = self.steps_df[self.steps_df["step"] == step].iloc[0]["channel"]
        step_version = self.steps_df[self.steps_df["step"] == step].iloc[0]["version"]

        if step_version == "latest":
            log.info(f"Updating {step} (by simply updating the DAG).")
        else:
            log.info(f"Updating {step} to version {step_version_new}.")
        if step_channel == "snapshot":
            return self._update_snapshot_step(step=step, step_version_new=step_version_new, step_header=step_header)
        elif step_channel in ["meadow", "garden", "grapher", "explorers", "external"]:
            return self._update_data_step(step=step, step_version_new=step_version_new, step_header=step_header)
        else:
            log.error(f"Channel {step_channel} not yet supported.")
            return 1

    def update_steps(
        self,
        steps: List[str],
        step_version_new: Optional[str] = STEP_VERSION_NEW,
        include_dependencies: bool = False,
        include_usages: bool = False,
        step_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """Update one or more steps to their new version, if possible."""
        # Gather all steps to be updated.
        for step in steps:
            # Check that step to be updated exists in the active dag.
            self.check_that_step_exists(step=step)

            if include_dependencies:
                # Add direct dependencies of current step to the list of steps to update (if not already in the list).
                dependencies = self.steps_df[self.steps_df["step"] == step]["direct_dependencies"].item()
                steps += [dependency for dependency in dependencies if dependency not in steps]

            if include_usages:
                # Add direct usages of current step to the list of steps to update (if not already in the list).
                usages = self.steps_df[self.steps_df["step"] == step]["direct_usages"].item()
                steps += [usage for usage in usages if usage not in steps]

        # Remove steps that cannot be updated because their version is already equal to the new version.
        # NOTE: One could think that steps with version "latest" should also be skipped. But their dependencies may need
        # to be updated. So they may need to be updated in the dag, even if their code should not be edited.
        steps = [
            step
            for step in steps
            if (self.steps_df[self.steps_df["step"] == step]["version"].item() != step_version_new)
            and (self.steps_df[self.steps_df["step"] == step]["update_period_days"].item() != 0)
        ]

        # If step_headers is not explicitly defined, get headers for each step from their corresponding dag file.
        if step_headers is None:
            step_headers = {}
            for step in steps:
                dag_file = self.steps_df[self.steps_df["step"] == step]["dag_file_path"].item()
                step_headers.update(
                    {step: get_comments_above_step_in_dag(step=step, dag_file=dag_file) if dag_file else ""}
                )

        if len(steps) == 0:
            log.info(
                "No steps to be updated. "
                "You may have attempted to update steps that are already updated or that have update periods of 0 days."
            )
        else:
            # Steps need to be updated hierarchically: First snapshots, then meadow, then garden, then grapher.
            # Also, if a data step depends on other steps, the dependencies need to be updated first.
            # For example, imagine the following partial dag:
            # {meadow_a: [snapshot:a], meadow_b: [snapshot_b], meadow_c: [meadow_a, meadow_b]}
            # We would need to update first snapshot_a and snapshot_b, then meadow_a and meadow_b, and then meadow_c.
            # Otherwise, if we updated meadow_c before meadow_a and meadow_b, the new meadow_c would depend on the old
            # meadow_a and meadow_b, and the new meadow_a and meadow_b would not be used.
            # This is the same topological sorting problem we have when deciding the order of steps to execute by etl.
            # Therefore, we can use the same functions filter_to_subgraph and to_dependency_order to solve it.
            filtered_dag = filter_to_subgraph(
                graph=self.tracker.dag_active,
                includes=steps,
                excludes=[],
                only=True,
                exact_match=True,
            )
            steps = to_dependency_order(filtered_dag)

            message = "The following steps will be updated:"
            for step in steps:
                message += f"\n  {step}"
            log.info(message)
            if self.interactive:
                input("Press enter to continue.")

            # Update each step.
            for step in steps:
                success = self._update_step(
                    step=step, step_version_new=step_version_new, step_header=step_headers[step]
                )
                if success == 1:
                    log.error(f"Stopped because of a failure on step {step}.")
                    break

    def _archive_step(self, step: str) -> None:
        # Move a certain step from its active dag to its corresponding archive dag.

        # Get info for step to be updated.
        step_info = self.get_step_info(step=step)

        # Skip non-archivable steps.
        if step_info["update_state"] != UpdateState.ARCHIVABLE.value:
            log.info(f"Skipping non-archivable step: {step}")
            return

        # Skip snapshots (since they do not appear as steps in the dag).
        if step_info["channel"] in ["snapshot"]:
            log.info(f"Skipping snapshot: {step}")
            return

        # Get active dag file for this step.
        dag_file_active = step_info["dag_file_path"]

        # Get archive dag file for this step.
        dag_file_archive = Path(
            dag_file_active.as_posix().replace(
                DAG_DIR.relative_to(BASE_DIR).as_posix(), DAG_ARCHIVE_FILE.parent.relative_to(BASE_DIR).as_posix()
            )
        )

        # If the archive dag file does not exist, create it.
        if not dag_file_archive.exists():
            create_dag_archive_file(dag_file_archive=dag_file_archive)

        # Get header from the comment lines right above the current step in the dag.
        step_header = get_comments_above_step_in_dag(step=step, dag_file=dag_file_active)

        # Create the dag_part that needs to be written to the archive file.
        dag_part = {step: set(step_info["direct_dependencies"])}

        log.info(f"Archiving step: {step}")
        if not self.dry_run:
            # Add the new step and its dependencies to the archive dag.
            write_to_dag_file(dag_file=dag_file_archive, dag_part=dag_part, comments={step: step_header})

            # Delete the step from the active dag.
            remove_steps_from_dag_file(dag_file=dag_file_active, steps_to_remove=[step])

            # Reload steps dataframe.
            self._load_version_tracker()

    def archive_steps(self, steps: Union[str, List[str]], include_usages: bool = False) -> None:
        """Move one or more steps from their active to their archive dag."""

        # If a single step is given, convert it to a list.
        if isinstance(steps, str):
            steps = [steps]
        elif isinstance(steps, tuple):
            steps = list(steps)

        # Archivable steps should be archived in groups.
        # For example, the meadow, garden and grapher steps of a step may be archivable, but we shouldn't archive only
        # the meadow step without archiving the garden and grapher steps as well (otherwise there would be a broken
        # dependency in the dag).
        for step in steps:
            if self.steps_df[self.steps_df["step"] == step].empty:
                log.error(f"Step {step} not found in active dag.")
                continue

            if include_usages:
                # Add all active usages of current step to the list of steps to update (if not already in the list).
                usages = self.steps_df[self.steps_df["step"] == step]["all_active_usages"].item()

                steps += [usage for usage in usages if usage not in steps]

        # Sort steps in dependency order (i.e. snapshots first). This avoids an error in the following situation:
        # You attempt to archive [meadow_1, snapshot_1] (where snapshot_1 is a dependency of meadow_1).
        # In this case, if you archive meadow_1 first, the snapshot_1 is also removed from the active dag, and
        # when attempting to archive snapshot_1 afterwards, an error is raised. To avoid this, first archive snapshot_1.
        filtered_dag = filter_to_subgraph(
            graph=self.tracker.dag_active,
            includes=steps,
            excludes=[],
            only=True,
        )
        steps = to_dependency_order(filtered_dag)

        if self.interactive:
            message = "The following steps will be archived:"
            for step in steps:
                message += f"\n  {step}"
            log.info(message)
            if self.interactive:
                input("Press enter to continue.")

        for step in steps:
            self._archive_step(step=step)


def _update_temporary_dag(dag_active, dag_all_reverse) -> None:
    # The temporary step in the temporary dag depends on the latest version of each newly created snapshot, before
    # they are used by any other active steps. We need to check if those snapshots are already used by active steps,
    # and hence can be removed from the temporary dag.
    temp_dependencies_new = set()
    for dependency in dag_active[DAG_TEMP_STEP]:
        # If that dependency is used only by the temporary step, that means no active step is using it yet, and
        # hence it must stay in the temporary dag.
        if set(dag_all_reverse[dependency]) == {DAG_TEMP_STEP}:
            temp_dependencies_new.add(dependency)
    # Update the content of the temporary dag.
    write_to_dag_file(
        dag_file=DAG_TEMP_FILE,
        dag_part={DAG_TEMP_STEP: temp_dependencies_new},
    )


def _confirm_choice(multiple_files: List[Any]) -> int:
    choice_default = 0
    for i, file_name in enumerate(multiple_files):
        print(f"    {i} - {file_name}")
    while True:
        choice = input(f"Press enter to use option {choice_default}, or choose a different number and press enter.")
        if choice == "":
            choice = choice_default
            break
        elif choice.isdigit():
            choice = int(choice)
            break
        else:
            print("Invalid input. Please choose a number.")
    return choice


@click.command(name="update", cls=RichCommand, help=__doc__)
@click.argument("steps", type=str or List[str], nargs=-1)
@click.option(
    "--step-version-new", type=str, default=STEP_VERSION_NEW, help=f"New version for step. Default: {STEP_VERSION_NEW}."
)
@click.option(
    "--include-dependencies",
    is_flag=True,
    default=False,
    type=bool,
    help="Update also steps that are direct dependencies of the given steps. Default: False.",
)
@click.option(
    "--include-usages",
    is_flag=True,
    default=False,
    type=bool,
    help="Update also steps that are directly using the given steps. Default: False.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    type=bool,
    help="Do not write to dag or create step files. Default: False.",
)
@click.option(
    "--interactive/--non-interactive",
    is_flag=True,
    default=False,
    type=bool,
    help="Skip user interactions (for confirmation and when there is ambiguity). Default: False.",
)
def cli(
    steps: Union[str, List[str]],
    step_version_new: Optional[str] = STEP_VERSION_NEW,
    include_dependencies: bool = False,
    include_usages: bool = False,
    dry_run: bool = False,
    interactive: bool = True,
) -> None:
    """Update one or more steps to their new version, if possible.

    This tool lets you update one or more snapshots or data steps to a new version. It will:

    * Create new folders and files for each of the steps.
    * Add the new steps to the dag, with the same header comments as their current version.

    **Notes:**

    Keep in mind that:

    * If there is ambiguity, the user will be asked for confirmation before updating each step, and on situations where there is some ambiguity.
    * If new snapshots are created that are not used by any steps, they are added to a temporary dag (temp.yml). These steps are then removed from the temporary dag as soon as they are used by an active step.
    * All dependencies of new steps will be assumed to use their latest version possible.
    * Steps whose version is already equal to the new version will be skipped.

    **Examples:**

    **Note:** Remove the --dry-run if you want to actually execute the updates in the examples below (but then remember to revert changes).

    * To update a single snapshot to the new version:
        ```
        $ etl update snapshot://animal_welfare/2023-10-24/fur_laws.xlsx --dry-run
        ```

        Note that, since no steps are using this snapshot, the new snapshot will be added to the temporary dag.

    * To update not only that snapshot, but also the steps that use it:
        ```
        $ etl update snapshot://animal_welfare/2023-10-24/fur_laws.xlsx --include-usages --dry-run
        ```

    * To update all dependencies of the climate change impacts explorer:
        ```
        $ etl update data://explorers/climate/latest/climate_change_impacts --include-dependencies --dry-run
        ```

        Note that the code of the explorers step itself will not be updated (since it has version "latest"), but its dependencies will be updated in the dag.
    """
    # If a single step is given, convert it to a list.
    if isinstance(steps, str):
        steps = [steps]
    elif isinstance(steps, tuple):
        steps = list(steps)

    # Initialize step updater and run update.
    StepUpdater(dry_run=dry_run, interactive=interactive).update_steps(
        steps=steps,
        step_version_new=step_version_new,
        include_dependencies=include_dependencies,
        include_usages=include_usages,
    )


@click.command(name="archive", cls=RichCommand, help=__doc__)
@click.argument("steps", type=str or List[str], nargs=-1)
@click.option(
    "--include-usages",
    is_flag=True,
    default=False,
    type=bool,
    help="Archive also steps that are directly using the given steps. Default: False.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    type=bool,
    help="Do not write to dag. Default: False.",
)
@click.option(
    "--interactive/--non-interactive",
    is_flag=True,
    default=False,
    type=bool,
    help="Skip user interactions (for confirmation and when there is ambiguity). Default: False.",
)
def archive_cli(
    steps: Union[str, List[str]],
    include_usages: bool = False,
    dry_run: bool = False,
    interactive: bool = True,
) -> None:
    """Archive one or more steps.

    This tool lets you move one or more data steps from their active to their archive dag.

    **Examples:**

    **Note:** Remove the --dry-run if you want to actually write to the dag.

    * To archive a single step:
        ```
        $ etl archive data://meadow/aviation_safety_network/2022-10-12/aviation_statistics --dry-run
        ```

        Note that, since no steps are using this snapshot, the new snapshot will be added to the temporary dag.

    * To archive not only that step, but also the steps that use it:
        ```
        $ etl archive data://meadow/aviation_safety_network/2022-10-12/aviation_statistics --include-usages --dry-run
        ```
    """
    # Initialize step updater and run update.
    StepUpdater(dry_run=dry_run, interactive=interactive).archive_steps(
        steps=steps,
        include_usages=include_usages,
    )
