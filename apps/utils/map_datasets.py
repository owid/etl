"""Temporary script that prints the grapher dataset pairs that need to be (manually) given to the chart upgrader, based
on the committed changes in your current git branch.

NOTE:
 * This script should eventually be part of the new indicator upgrader, but for now it can be helpful as a CLI.
 * The logic may be more complicated than needed. It may suffice to find the newly created grapher datasets that do not
   yet have charts, and then attempt to find their corresponding previous version. But some of the code can be useful
   for other reasons (for example in the new chart diff tool).

"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import click
import numpy as np
import pandas as pd
from git import Repo
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl.paths import BASE_DIR, SNAPSHOTS_DIR, STEP_DIR
from etl.version_tracker import VersionTracker

# Initialize logger.
log = get_logger()


def get_changed_files(
    current_branch: Optional[str] = None,
    base_branch: Optional[str] = None,
    repo_path: Union[Path, str] = BASE_DIR,
    only_committed: bool = False,
) -> Dict[str, Dict[str, str]]:
    """Return files that are different between the current branch and the specified base branch."""
    repo = Repo(repo_path)

    if current_branch is None:
        # If not specified, use the current branch.
        current_branch = repo.active_branch.name
    else:
        # Otherwise, switch to the given branch to compare from.
        repo.git.checkout(current_branch)

    if base_branch is None:
        # If not specified, use "master" branch.
        # However, if there is a "master-1" branch, that means we are on a staging server; if so, use "master-1".
        base_branch = "master-1" if "master-1" in [branch.name for branch in repo.branches] else "master"

    # Fetch the latest changes from the remote repository
    repo.remotes.origin.fetch()

    # Find the common ancestor of the remote base branch and the current local branch.
    # In other words, "merge_base" is the last common commit between those two branches.
    merge_base = repo.git.merge_base(f"origin/{base_branch}", f"{current_branch}").strip()

    # Create a dictionary {file_path: {"status": status, "diff": diff_content}}, where
    # * status is the change status, namely: 'M' if the file was modified, 'A' if appended, 'D' if deleted.
    # * diff_content shows the difference between files.
    changes = {}

    # Get the diff between the current branch and the base branch.
    diff_index = repo.git.diff(f"{merge_base}..{current_branch}", name_status=True, no_renames=True)
    if diff_index:
        for line in diff_index.splitlines():
            parts = line.split("\t")
            if len(parts) == 2:
                status, file_path = parts
                # Fetch diff content.
                diff_content = repo.git.diff(f"{merge_base}...{current_branch}", "--", file_path, p=True)
                changes[file_path] = {"status": status, "diff": diff_content}
            else:
                # Not sure if this could happen.
                log.error(f"Could not parse diff line: {line}")

    if not only_committed:
        # Include uncommitted changes
        uncommitted_diff = repo.git.diff(name_status=True, no_renames=True)
        if uncommitted_diff:
            for line in uncommitted_diff.splitlines():
                parts = line.split("\t")
                if len(parts) == 2:
                    status, file_path = parts
                    diff_content = repo.git.diff("--", file_path, p=True)
                    changes[file_path] = {"status": status, "diff": diff_content}

        # Add untracked files.
        changes.update({file_path: {"status": "A", "diff": ""} for file_path in repo.untracked_files})

    return changes


def get_grapher_changes(files_changed: Dict[str, Dict[str, str]], steps_df: pd.DataFrame) -> List[Dict[str, Any]]:
    steps_affected = []
    files_unidentified = []
    grapher_changes = []
    for file_path in files_changed:
        """Get list of new grapher steps (submitted to the database) with their corresponding old steps."""
        # Get status: D (deleted), A (added), M (modified)
        file_status = files_changed[file_path]["status"]

        # If deleted, skip loop iteration
        if file_status == "D":
            # Skip deleted files.
            continue

        # Get identify file (if applicable). Obtain its parts (version, identifier, etc.)
        if file_path.startswith(SNAPSHOTS_DIR.relative_to(BASE_DIR).as_posix()) and file_path.endswith(".dvc"):
            parts = Path(file_path).with_suffix("").as_posix().split("/")[1:]
            version = parts.pop(-2)
            identifier = "snapshot/" + "/".join(parts)
        elif file_path.startswith(STEP_DIR.relative_to(BASE_DIR).as_posix()) and file_path.endswith(".py"):
            parts = Path(file_path).with_suffix("").as_posix().split("/")[-4:]
            version = parts.pop(-2)
            identifier = "/".join(parts)
        else:
            files_unidentified.append(file_path)
            continue
        # Obtain row in steps_df that corresponds to the file.
        candidate = steps_df[(steps_df["identifier"] == identifier) & (steps_df["version"] == version)]

        # Obtain old version
        ## This could happen with non-etl step files, like "shared.py". Ignore them
        if len(candidate) == 0:
            log.error("No candidate in steps_df was found! Working with old datasets (non-ETL)?")
        ## Unknown error.
        elif len(candidate) > 1:
            log.error(f"Could not identify a step matching file {file_path}")
        ## Get candidate's info (old version)
        else:
            steps_affected.append(candidate["step"].item())
            steps_affected.extend(candidate["all_usages"].item())

            if (candidate["channel"].item() == "grapher") & (file_status == "A"):
                current_grapher_step = candidate["step"].item()

                ## Get grapher dataset id and name of the new dataset.
                ## If no ID is detected, we can assume that this is not a migration we want to do!
                new = _get_dataset_name(steps_df, current_grapher_step)
                if new:
                    grapher_changes_ = {
                        "new": new,
                    }

                    # If there is any, get the info of the previous grapher step.
                    previous_grapher_steps = candidate["same_steps_backward"].item()
                    if len(previous_grapher_steps) > 0:
                        previous_grapher_step = previous_grapher_steps[-1]
                        # Get grapher dataset id for the old dataset.
                        old = _get_dataset_name(steps_df, previous_grapher_step)
                        if old:
                            grapher_changes_["old"] = old

                    grapher_changes.append(grapher_changes_)

    return grapher_changes


def _get_dataset_name(steps_df: pd.DataFrame, step: str) -> Dict[str, Any] | None:
    identifier = steps_df.loc[steps_df["step"] == step, "db_dataset_id"].item()
    if np.isnan(identifier):
        log.error(f"Grapher dataset ({step}) in ETL detected that was not submitted to the database! Ignoring it.")
        return None
    else:
        identifier = int(identifier)
        name = steps_df.loc[steps_df["step"] == step, "db_dataset_name"].item()
    return {
        "id": identifier,
        "name": name,
        "step": step,
    }


def get_datasets_mapped(files_changed: Dict[str, Dict[str, str]]) -> List[Dict[str, Dict[str, Any]]]:
    """Get grapher dataset pairs that need to be (manually) given to the chart upgrader, based on the committed changes
    in the current git branch.

    Parameters
    ----------
    files_changed : Dict[str, Dict[str, str]]
        Dictionary of files that have been modified in the current branch compared to the base (master) branch.

    Returns
    -------
    grapher_changes : List[Dict[str, Dict[str, Any]]]
        List of (old and new) grapher datasets that need to be mapped.

    """
    # List all files that have been modified in the current branch compared to the base branch.
    # files_changed = get_changed_files(base_branch="update-electricity-mix-data-reference")
    files_changed = get_changed_files()

    # Load dataframe of steps.
    steps_df = VersionTracker().steps_df

    # In the end we want all useful info for:
    #  * New grapher datasets created by changed steps.
    #  * Existing grapher datasets that need to be replaced by new ones.
    #  * Existing grapher datasets that do not have replacement, but that may be affected by changed steps.
    grapher_changes = get_grapher_changes(files_changed, steps_df)

    # To get all info of the affected steps:
    # steps_df_affected = steps_df[steps_df["step"].isin(steps_affected) & (steps_df["db_dataset_name"].notnull())].reset_index(drop=True)

    # Consider warning about unidentified files.

    return grapher_changes


@click.command(name="map-datasets", cls=RichCommand, help=__doc__)
def cli() -> None:
    """Print grapher dataset pairs that need to be (manually) given to the chart upgrader, based on the committed
    changes in the current git branch.

    """
    files_changed = get_changed_files()
    changes = get_datasets_mapped(files_changed=files_changed)

    print(
        f"Based on the changes committed to the current git branch, "
        f"{len(changes)} grapher dataset pairs need to be mapped.\n"
    )
    for change in changes:
        print(f'[{change["old"]["id"]}] {change["old"]["name"]}')
        print(f'    Step: {change["old"]["step"]}')
        print(f'[{change["new"]["id"]}] {change["new"]["name"]}')
        print(f'    Step: {change["new"]["step"]}')
        print()
