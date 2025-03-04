"""This module contains functions that interact with ETL, DB, and possibly our API.

Together with utils.db and utils.cached, it might need some rethinking on where it goes.
"""

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pymysql import OperationalError
from sqlalchemy.orm import Session
from structlog import get_logger

import etl.grapher.model as gm
from apps.wizard.utils.cached import get_datasets_from_version_tracker
from etl.dag_helpers import load_dag
from etl.git_helpers import get_changed_files
from etl.grapher.io import get_all_datasets
from etl.paths import BASE_DIR, SNAPSHOTS_DIR, STEP_DIR
from etl.steps import filter_to_subgraph

# Initialize logger.
log = get_logger()


########################################################################################################################
# Consider deprecating this function, which is very slow, and possibly an overkill.
# NOTE: I think it can easily be replaced in the context of Anomalist, but unclear yet if it can be replace in the context of indicator upgrader.
def get_steps_df(archived: bool = True):
    """Get steps_df, and grapher_changes from version tracker."""
    # NOTE: The following ignores DB datasets that are archived (which is a bit unexpected).
    # I had to manually un-archive the testing datasets from the database manually to make things work.
    # This could be fixed, but maybe it's not necessary (since we won't archive an old version of a dataset until the
    # new has been analyzed).
    steps_df_grapher, grapher_changes = get_datasets_from_version_tracker()

    # Combine with datasets from database that are not present in ETL
    # Get datasets from Database
    try:
        datasets_db = get_all_datasets(archived=archived)
    except OperationalError as e:
        raise OperationalError(
            f"Could not retrieve datasets. Try reloading the page. If the error persists, please report an issue. Error: {e}"
        )

    # Get table with all datasets (ETL + DB)
    steps_df_grapher = (
        steps_df_grapher.merge(datasets_db, on="id", how="outer", suffixes=("_etl", "_db"))
        .sort_values(by="id", ascending=False)
        .drop(columns="updatedAt")
        .astype({"id": int})
    )
    columns = ["namespace", "name"]
    for col in columns:
        steps_df_grapher[col] = steps_df_grapher[f"{col}_etl"].fillna(steps_df_grapher[f"{col}_db"])
        steps_df_grapher = steps_df_grapher.drop(columns=[f"{col}_etl", f"{col}_db"])

    assert steps_df_grapher["name"].notna().all(), "NaNs found in `name`"
    assert steps_df_grapher["namespace"].notna().all(), "NaNs found in `namespace`"

    return steps_df_grapher, grapher_changes


########################################################################################################################


def get_changed_steps(files_changed: Dict[str, Dict[str, str]]) -> List[str]:
    changed_steps = []
    for file_path, file_status in files_changed.items():
        # File status can be: D (deleted), A (added), M (modified).
        # NOTE: In principle, we could select only "A" files. But it is possible that the user adds a new grapher step, and then commits changes to it, in which case (I think) the status would be "M".

        # If deleted, skip loop iteration
        if file_status == "D":
            # Skip deleted files.
            continue

        # Identify potential recipes for data steps
        if file_path.startswith(
            (STEP_DIR.relative_to(BASE_DIR).as_posix(), SNAPSHOTS_DIR.relative_to(BASE_DIR).as_posix())
        ):
            changed_steps.append(file_path)
        else:
            continue

    return changed_steps


def get_changed_grapher_steps(files_changed: Dict[str, Dict[str, str]]) -> List[str]:
    """Get list of new grapher steps with their corresponding old steps."""
    steps = []
    for step_path in get_changed_steps(files_changed):
        if step_path.endswith(".py"):
            parts = Path(step_path).with_suffix("").as_posix().split("/")
            if len(parts) >= 4 and parts[-4] == "grapher":
                steps.append(step_path)
    return steps


def get_new_grapher_datasets_and_their_previous_versions(session: Session) -> Dict[int, Optional[int]]:
    """Detect which local grapher step files have changed, identify their corresponding grapher dataset ids, and the grapher dataset id of the previous version (if any).

    The result is a dictionary {dataset_id (of the new dataset): previous_dataset_id or None (if there is no previous version)}.
    """
    # Get list of all files changed locally.
    files_changed = get_changed_files()
    # Select new (or modified) grapher steps.
    grapher_steps = get_changed_grapher_steps(files_changed)
    # Get properties of the modified grapher steps.
    namespaces = sorted(set([step.split("/")[-3] for step in grapher_steps]))
    short_names = sorted(set([step.split("/")[-1].replace(".py", "") for step in grapher_steps]))

    # Load all relevant grapher datasets from DB.
    datasets = (
        session.query(gm.Dataset)
        .filter(
            gm.Dataset.namespace.in_(namespaces),
            gm.Dataset.shortName.in_(short_names),
        )
        .all()
    )
    df_datasets = pd.DataFrame(datasets)
    # For each modified grapher step, check if the corresponding dataset is the latest version.
    # If there is no dataset, raise a warning (either it has not been run yet, or it was deleted).
    new_datasets = dict()
    for grapher_step in grapher_steps:
        namespace, version, short_name = grapher_step.replace(".py", "").split("/")[-3:]
        selected_datasets = df_datasets[
            (df_datasets["namespace"] == namespace) & (df_datasets["shortName"] == short_name)
        ].sort_values("version", ascending=False)
        if (len(selected_datasets) == 0) or (version not in selected_datasets["version"].tolist()):
            log.warning(
                f"Warning: No grapher dataset found for {grapher_step}. It might not have been run yet, or it was deleted from DB."
            )
            continue

        # Check if the dataset is the latest version.
        if selected_datasets["version"].iloc[0] == version:
            # Find the dataset id of the current grapher dataset.
            ds_id = selected_datasets["id"].iloc[0]
            # This is new grapher dataset and will be added to the dictionary.
            # But let's also find out if there is a previous version.
            if len(selected_datasets) > 1:
                # Get the dataset id of the previous version.
                previous_dataset = selected_datasets["id"].iloc[1]
            else:
                # There was no previous version.
                previous_dataset = None
            # Add the dataset to the dictionary.
            new_datasets[ds_id] = previous_dataset

    return new_datasets


def get_all_changed_catalog_paths(files_changed: Dict[str, Dict[str, str]]) -> List[str]:
    """Get all changed steps and their downstream dependencies."""
    dataset_catalog_paths = []

    # Get catalog paths of all datasets with changed files.
    for step_path in get_changed_steps(files_changed):
        abs_step_path = BASE_DIR / Path(step_path)
        try:
            # TODO: use StepPath from https://github.com/owid/etl/pull/3165 to refactor this
            if step_path.startswith("snapshots/"):
                ds_path = abs_step_path.relative_to(SNAPSHOTS_DIR).with_suffix("").with_suffix("").as_posix()
            else:
                ds_path = abs_step_path.relative_to(STEP_DIR / "data").with_suffix("").with_suffix("").as_posix()
            dataset_catalog_paths.append(ds_path)
        except ValueError:
            continue

    # NOTE:
    # This is OK, as it filters down the DAG a little bit. But using VersionTracker.steps_df would be much more precise. You could do:
    # steps_df[(steps_df["step"].isin([...])]["all_active_usages"]
    # And that would give you only the steps that are affected by the changed files. That would be ultimately what we need. But I
    # understand that loading steps_df is very slow.

    # Add all downstream dependencies of those datasets.
    DAG = load_dag()
    dag_steps = list(filter_to_subgraph(DAG, dataset_catalog_paths, downstream=True).keys())

    # From data://... extract catalogPath
    # TODO: use StepPath from https://github.com/owid/etl/pull/3165 to refactor this
    catalog_paths = [step.split("://")[1] for step in dag_steps if step.startswith("data://")]

    return catalog_paths
