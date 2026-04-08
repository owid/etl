from pathlib import Path
from typing import Dict, List

from structlog import get_logger

from etl.dag_helpers import load_dag
from etl.paths import BASE_DIR, SNAPSHOTS_DIR, STEP_DIR
from etl.steps import filter_to_subgraph

# Initialize logger.
log = get_logger()


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

    if not dataset_catalog_paths:
        return []

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
