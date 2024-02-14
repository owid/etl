import sys
from datetime import datetime
from typing import Optional

import click
import pandas as pd
import structlog
from rich_click.rich_command import RichCommand

from etl.paths import SNAPSHOTS_DIR
from etl.snapshot import SnapshotMeta
from etl.version_tracker import VersionTracker

log = structlog.get_logger()

# If a new version is not specified, assume current date.
STEP_VERSION_NEW = datetime.now().strftime("%Y-%m-%d")


def update_snapshot_step(
    step: str,
    steps_df: Optional[pd.DataFrame] = None,
    step_version_new: Optional[str] = STEP_VERSION_NEW,
    create_files: bool = True,
) -> None:
    if steps_df is None:
        # Initialize version tracker and load dataframe of all steps.
        steps_df = VersionTracker().steps_df.copy()
    else:
        steps_df = steps_df.copy()

    # Select only active steps.
    steps_df = steps_df[steps_df["state"] == "active"].reset_index(drop=True)

    # Check that step to be updated exists in the active dag.
    if step not in set(steps_df["step"]):
        log.error(f"Step {step} not found among active steps.")
        sys.exit(1)

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
    metadata.version = step_version_new

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

    if create_files:
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

    # TODO: For snapshot files, there is nothing to write to the dag. But then, how will the next meadow step know that this
    #  snapshot exists? Some possible solutions:
    #  * Create a temporary dag file with a temporary step that depends on the snapshot.


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
    # Initialize version tracker and load dataframe of all steps.
    steps_df = VersionTracker().steps_df.copy()

    if step not in set(steps_df["step"]):
        log.error(f"Step {step} not found among active steps.")
        sys.exit(1)

    # Extract channel from step.
    step_channel = steps_df[steps_df["step"] == step].iloc[0]["channel"]

    if step_channel == "snapshot":
        update_snapshot_step(step=step, step_version_new=step_version_new, steps_df=steps_df, create_files=not dry_run)
    else:
        log.error(f"Channel {step_channel} not yet supported.")
        sys.exit(1)
