import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import click
import structlog
from rich_click.rich_command import RichCommand

from etl.paths import DAG_TEMP_FILE, SNAPSHOTS_DIR
from etl.snapshot import SnapshotMeta
from etl.version_tracker import VersionTracker

log = structlog.get_logger()

# If a new version is not specified, assume current date.
STEP_VERSION_NEW = datetime.now().strftime("%Y-%m-%d")
# TODO: Every time this script runs, it will check if any of those snapshots are already in the dag.
#  If so, it will remove them from the dependencies of the temporary step.


def write_to_dag_file(
    dag_file: Path,
    dag_part: Dict[str, Any],
    comments: Optional[Dict[str, str]] = None,
    indent_step=2,
    indent_dependency=4,
):
    # TODO: Add docstring and unit tests. Consider moving this function to another module.

    # If comments is not defined, assume an empty dictionary.
    if comments is None:
        comments = {}

    # Read the lines in the original dag file.
    with open(dag_file, "r") as file:
        lines = file.readlines()

    # Separate that content into the "steps" section (always given) and the "include" section (sometimes given).
    section_steps = []
    section_include = []
    inside_section_steps = True
    for line in lines:
        if line.strip().startswith("include"):
            inside_section_steps = False
        if inside_section_steps:
            section_steps.append(line)
        else:
            section_include.append(line)

    # Now the "steps" section will be updated, and at the end the "include" section will be appended.

    # Initialize a list with the new lines that will be written to the dag file.
    updated_lines = []
    # Initialize a flag to skip lines until the next step.
    skip_until_next_step = False
    # Initialize a set to keep track of the steps that were found in the original dag file.
    steps_found = set()
    for line in section_steps:
        # Remove leading and trailing whitespace from the line.
        stripped_line = line.strip()

        # Identify the start of a step, e.g. "  data://meadow/temp/latest/step:".
        if stripped_line.endswith(":") and not stripped_line.startswith("-"):
            # Extract the name of the step (without the ":" at the end).
            current_step = ":".join(stripped_line.split(":")[:-1])
            if current_step in dag_part:
                # This step was in dag_part, which means it needs to be updated.
                # First add the step itself.
                updated_lines.append(line)
                # Now add each of its dependencies.
                for dep in dag_part[current_step]:
                    updated_lines.append(" " * indent_dependency + f"- {dep}\n")
                # Skip the following lines until the next step is found.
                skip_until_next_step = True
                # Add the current step to the set of steps found in the dag file.
                steps_found.add(current_step)
                continue
            else:
                # This step was not in dag_part, so it will be copied as is.
                skip_until_next_step = False

        # Skip dependency lines of the step being updated.
        if skip_until_next_step and stripped_line.startswith("-"):
            continue

        # Add lines that should not be skipped.
        updated_lines.append(line)

    # Append new steps that weren't found in the original content.
    for step, dependencies in dag_part.items():
        if step not in steps_found:
            # Add the comment for this step, if any was given.
            if step in comments:
                updated_lines.append(" " * indent_step + comments[step] + "\n")
            # Add the step itself.
            updated_lines.append(" " * indent_step + f"{step}:\n")
            # Add each of its dependencies.
            for dep in dependencies:
                updated_lines.append(" " * indent_dependency + f"- {dep}\n")

    if len(section_include) > 0:
        # Append the include section, ensuring there is only one line break in between.
        for i in range(len(updated_lines) - 1, -1, -1):
            if updated_lines[i] != "\n":
                # Slice the list to remove trailing line breaks
                updated_lines = updated_lines[: i + 1]
                break
        # Add a single line break before the include section, and then add the include section.
        updated_lines.extend(["\n"] + section_include)

    # Write the updated content back to the dag file.
    with open(dag_file, "w") as file:
        file.writelines(updated_lines)


class StepUpdater:
    def __init__(self, dry_run: bool = False):
        # Initialize version tracker and load dataframe of all steps.
        self.steps_df = VersionTracker().steps_df.copy()
        # If dry_run is True, then nothing will be written to the dag, and no files will be created.
        self.dry_run = dry_run

    def check_that_step_exists(self, step: str) -> None:
        """Check that step to be updated exists in the active dag."""
        if step not in set(self.steps_df["step"]):
            log.error(f"Step {step} not found among active steps.")
            sys.exit(1)

    def update_snapshot_step(
        self,
        step: str,
        step_version_new: Optional[str] = STEP_VERSION_NEW,
    ) -> None:
        # Select only active steps.
        steps_df = self.steps_df[self.steps_df["state"] == "active"].reset_index(drop=True)

        # Check that step to be updated exists in the active dag.
        self.check_that_step_exists(step=step)

        # Get info for step to be updated.
        step_info = steps_df[steps_df["step"] == step].iloc[0].to_dict()

        # Check that a .dvc file exists for this snapshot step.
        step_dvc_file = SNAPSHOTS_DIR / step_info["namespace"] / step_info["version"] / (step_info["name"] + ".dvc")
        if not step_dvc_file.exists():
            log.error(f"No .dvc file found for step {step}.")
            sys.exit(1)

        # Define folder for new version.
        folder_new = SNAPSHOTS_DIR / step_info["namespace"] / step_version_new

        # Check that the new dvc file does not exist.
        step_dvc_file_new = folder_new / (step_info["name"] + ".dvc")
        if step_dvc_file_new.exists():
            log.error(f"New .dvc file already exists: {step_dvc_file_new}")
            sys.exit(1)

        # Load metadata from last step.
        metadata = SnapshotMeta.load_from_yaml(step_dvc_file)
        # Update metadata.
        # TODO: Generalize this to be able to update metadata using etl-wizard snapshot.
        metadata.version = step_version_new  # type: ignore

        # Find script file for old step.
        _step_py_files = list(step_dvc_file.parent.glob("*.py"))
        if len(_step_py_files) == 1:
            step_py_file = _step_py_files[0]
        else:
            # TODO: Usually there is a single .py file with the same name. But it is possible that:
            #  * The single .py file has a different name (e.g. gcp/2023-12-12/global_carbon_budget.py).
            #  * There are multiple .py files, corresponding to different snapshots altogether.
            #  Therefore: Check if there is a single .py file in the same folder. If so, that's the script.
            #  If there are multiple, choose the one whose name has a shorter edit distance to the step name,
            #  and prompt user confirmation (and allow inputting the right file).
            log.error(f"No single .py file found for step {step}.")
            sys.exit(1)

        # Define the new step.
        step_new = step.replace(step_info["version"], step_version_new)  # type: ignore

        if not self.dry_run:
            # If new folder does not exist, create it.
            folder_new.mkdir(parents=True, exist_ok=True)

            # Write metadata to new dvc file.
            step_dvc_file_new.write_text(metadata.to_yaml())

            # Check if a new py file already exists.
            step_py_file_new = folder_new / step_py_file.name
            if step_py_file_new.exists():
                # If there is already a .py file in the new folder, it may be because another dvc file (used by that script) has
                # already been updated. So, simply skip it (alternatively, consider raising a warning).
                pass
            else:
                # Create a new py file.
                step_py_file_new.write_bytes(step_py_file.read_bytes())

            # Add the new snapshot as a dependency of the temporary dag.
            step_temp = (
                self.steps_df[self.steps_df["namespace"] == "temp"][["step", "direct_dependencies"]]
                .set_index("step")
                .to_dict()["direct_dependencies"]
            )
            step_temp_name = list(step_temp)[0]
            # Add the new snapshot as a dependency of the temporary step (and make it a set instead of a list).
            step_temp[step_temp_name] = set(step_temp[step_temp_name] + [step_new])
            # Save temporary dag.
            write_to_dag_file(dag_file=DAG_TEMP_FILE, dag_part=step_temp)

    def update_step(self, step: str, step_version_new: Optional[str] = STEP_VERSION_NEW) -> None:
        """Update step to new version."""

        # Extract channel from step.
        step_channel = self.steps_df[self.steps_df["step"] == step].iloc[0]["channel"]

        if step_channel == "snapshot":
            self.update_snapshot_step(step=step, step_version_new=step_version_new)
        else:
            log.error(f"Channel {step_channel} not yet supported.")
            sys.exit(1)


@click.command(cls=RichCommand, help=__doc__)
@click.argument("step", type=str)
@click.option(
    "--step-version-new", type=str, default=STEP_VERSION_NEW, help=f"New version for step. Default: {STEP_VERSION_NEW}."
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    type=bool,
    help="Do not write to dag or create step files. Default: False.",
)
def cli(
    step: str,
    step_version_new: Optional[str] = STEP_VERSION_NEW,
    dry_run: bool = False,
) -> None:
    """TODO"""
    # Initialize step updater and run update.
    StepUpdater(dry_run=dry_run).update_step(step=step, step_version_new=step_version_new)
