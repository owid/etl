import re
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
                rel_export = abs_step_path.relative_to(STEP_DIR / "export")
            except ValueError:
                continue
            # A collection recipe can be split across companion config files named
            # `<short>.<key>.config.yml` (e.g. democracy.eiu.config.yml) that all feed the single
            # `<short>.py` step. Blindly stripping suffixes would invent a nonexistent
            # `<short>.<key>` step, so resolve to the sibling `<short>.py` recipe when it exists.
            short = abs_step_path.name.split(".", 1)[0]
            if (abs_step_path.parent / f"{short}.py").exists():
                export_path = (rel_export.parent / short).as_posix()
            else:
                export_path = rel_export.with_suffix("").with_suffix("").as_posix()
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

    DAG = load_dag()

    # A version bump only touches files under the new version's folder, so `dataset_catalog_paths`
    # so far contains only the new version's path. If we stopped here, callers that turn this list
    # into an --include filter (e.g. datadiff's `--changed`) would filter the previous version's
    # catalog path out of the comparison entirely, and `etl diff` would report the bump as a
    # brand-new dataset instead of diffing it against the old version. Pull in just the closest
    # *preceding* version of the same channel/namespace/short_name so it stays in scope for
    # comparison.
    #
    # Only the immediate predecessor is added — not every other active version. Some datasets
    # (e.g. WDI) keep several vintages active in parallel for different downstream consumers
    # rather than superseding one in place; matching all of them would sweep unrelated, untouched
    # datasets into the comparison. Since those aren't part of `dataset_catalog_paths` and usually
    # aren't built locally either, `etl diff` would report each one as falsely "removed".
    all_data_steps = {s.split("://", 1)[1] for s in DAG if s.startswith("data://")}
    sibling_versions = []
    for ds_path in dataset_catalog_paths:
        parts = ds_path.split("/")
        if len(parts) != 4:
            # Not a channel/namespace/version/short_name data step (e.g. a snapshot path).
            continue
        channel, namespace, version, short_name = parts
        pattern = re.compile(rf"^{re.escape(channel)}/{re.escape(namespace)}/([^/]+)/{re.escape(short_name)}$")
        # Versions are (almost always) ISO dates, so the lexicographically-largest one that's
        # still less than the new version is its closest predecessor.
        preceding = [
            (match.group(1), candidate)
            for candidate in all_data_steps
            if candidate != ds_path and (match := pattern.match(candidate)) and match.group(1) < version
        ]
        if preceding:
            sibling_versions.append(max(preceding, key=lambda p: p[0])[1])
    # Add all downstream dependencies of the originally-changed datasets only. Siblings are added
    # afterward as comparison targets, not as subgraph-expansion inputs — otherwise every real
    # downstream consumer of an old sibling version (and their own upstream dependencies) would be
    # swept into the result too, even though only the new version is actually changing.
    dag_steps = list(filter_to_subgraph(DAG, dataset_catalog_paths, downstream=True).keys())

    # From data://... extract catalogPath
    # TODO: use StepPath from https://github.com/owid/etl/pull/3165 to refactor this
    catalog_paths = [step.split("://")[1] for step in dag_steps if step.startswith("data://")]
    for sibling in sibling_versions:
        if sibling not in catalog_paths:
            catalog_paths.append(sibling)

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
