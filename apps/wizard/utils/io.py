"""This module contains functions that interact with ETL, DB, and possibly our API.

Together with utils.db and utils.cached, it might need some rethinking on where it goes.
"""

from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session
from structlog import get_logger

import etl.grapher.model as gm
from etl.git_helpers import get_changed_files
from etl.io import get_changed_steps

# Initialize logger.
log = get_logger()


def get_changed_grapher_steps(files_changed: dict[str, dict[str, str]]) -> list[str]:
    """Get list of new grapher steps with their corresponding old steps."""
    steps = []
    for step_path in get_changed_steps(files_changed):
        if step_path.endswith(".py"):
            parts = Path(step_path).with_suffix("").as_posix().split("/")
            if len(parts) >= 4 and parts[-4] == "grapher":
                steps.append(step_path)
    return steps


def get_new_grapher_datasets_and_their_previous_versions(session: Session) -> dict[int, int | None]:
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
