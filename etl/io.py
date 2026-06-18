from pathlib import Path

from structlog import get_logger

from etl.dag_helpers import load_dag
from etl.paths import BASE_DIR, SNAPSHOTS_DIR, STEP_DIR
from etl.steps import filter_to_subgraph

# Initialize logger.
log = get_logger()


def get_changed_steps(files_changed: dict[str, dict[str, str]]) -> list[str]:
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


def get_all_changed_catalog_paths(files_changed: dict[str, dict[str, str]], include_export: bool = False) -> list[str]:
    """Get all changed steps and their downstream dependencies.

    :param include_export: If True, also return downstream export steps (e.g. multidim/explorer
        exports) with their full ``export://`` URI. These have no data:// catalogPath, so they're
        excluded by default (chart-diff/datadiff only care about data steps).
    """
    dataset_catalog_paths = []
    # Directly-changed export steps (e.g. a modified multidim/explorer recipe). These live under
    # etl/steps/export/, not etl/steps/data/, so they aren't data catalog paths; we keep their full
    # export:// URI and add them to the result when include_export is set.
    changed_export_uris = []

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
            # Not a data/snapshot step. It might be an export step (etl/steps/export/...); if so,
            # record its export:// URI so a branch that only edits an export recipe still selects it.
            try:
                export_path = abs_step_path.relative_to(STEP_DIR / "export").with_suffix("").with_suffix("").as_posix()
            except ValueError:
                continue
            changed_export_uris.append(f"export://{export_path}")

    if not dataset_catalog_paths:
        # No data steps changed. We can still have directly-changed export steps; return those when
        # requested (their downstream subgraph is computed from data steps, of which there are none).
        return changed_export_uris if include_export else []

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

    # Optionally also return export steps, keeping their full URI so callers can match them with
    # `export://...` include patterns (export steps have no data:// catalogPath). This covers both
    # downstream exports (reached via changed data steps) and directly-changed export recipes.
    if include_export:
        downstream_export_uris = [step for step in dag_steps if step.startswith("export://")]
        # Dedupe while preserving order (a directly-changed export can also be a downstream one).
        seen = set(catalog_paths)
        for uri in downstream_export_uris + changed_export_uris:
            if uri not in seen:
                seen.add(uri)
                catalog_paths.append(uri)

    return catalog_paths
