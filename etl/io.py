import re
from pathlib import Path

from structlog import get_logger

from etl.dag_helpers import load_dag, load_single_dag_file, parse_dag_yaml_text
from etl.git_helpers import get_file_at_merge_base
from etl.paths import BASE_DIR, SNAPSHOTS_DIR, STEP_DIR
from etl.steps import filter_to_subgraph

# Initialize logger.
log = get_logger()


def get_changed_steps(files_changed: dict[str, dict[str, str]]) -> list[str]:
    changed_steps = []
    for file_path, file_status in files_changed.items():
        # File status can be: D (deleted), A (added), M (modified).
        # NOTE: In principle, we could select only "A" files. But it is possible that the user adds a new grapher step, and then commits changes to it, in which case (I think) the status would be "M".
        # files_changed values are {"status": ..., "diff": ...} dicts from get_changed_files, but plain
        # status strings are also accepted.
        status = file_status.get("status") if isinstance(file_status, dict) else file_status

        # Skip deleted files: a removed (or moved-away) recipe is not a step to select. Before this
        # unwrapped the dict, every deleted step file produced a phantom step name, which `--modified`
        # then used to decide which flags to enable.
        if status == "D":
            continue

        # Identify potential recipes for data steps
        if file_path.startswith(
            (STEP_DIR.relative_to(BASE_DIR).as_posix(), SNAPSHOTS_DIR.relative_to(BASE_DIR).as_posix())
        ):
            changed_steps.append(file_path)
        else:
            continue

    return changed_steps


def get_dag_dependency_changed_steps(files_changed: dict[str, dict[str, str]]) -> set[str]:
    """Steps whose dependency lists changed in edited ``dag/*.yml`` files.

    A dependency-only edit — e.g. repointing a consumer step from one input version to another —
    touches no file under ``etl/steps/`` or ``snapshots/``, so :func:`get_changed_steps` cannot
    see it; yet the step rebuilds to different content. Without this, ``--modified`` builds (and
    the chart-diff / datadiff selections built on the same machinery) silently skip repointed
    steps, and their value changes ship with the next full build unreviewed (WDI 2026-07: six
    repointed consumers changed data that neither diff surfaced).

    Diff each changed DAG file against its merge-base version and select every step whose
    dependency set differs, including steps newly declared in the file. ``dag/archive/*`` is a
    generated record of retired steps and is ignored.
    """
    changed_steps: set[str] = set()
    for file_path, file_status in files_changed.items():
        # files_changed values are {"status": ..., "diff": ...} dicts from get_changed_files,
        # but plain status strings are also accepted (mirrors get_changed_steps' inputs in tests).
        status = file_status.get("status") if isinstance(file_status, dict) else file_status
        if status == "D":
            continue
        if not file_path.startswith("dag/") or file_path.startswith("dag/archive/") or not file_path.endswith(".yml"):
            continue
        current = load_single_dag_file(BASE_DIR / file_path)
        base_text = get_file_at_merge_base(file_path)
        base = parse_dag_yaml_text(base_text) if base_text else {}
        for step, deps in current.items():
            if set(deps or set()) != set(base.get(step) or set()):
                changed_steps.add(step)
    return changed_steps


# Step types whose recipes live under `etl/steps/<type>/` but produce no data:// catalog path.
NON_DATA_STEP_TYPES = ("viz", "export")
NON_DATA_URI_PREFIXES = tuple(f"{step_type}://" for step_type in NON_DATA_STEP_TYPES)


def get_all_changed_catalog_paths(files_changed: dict[str, dict[str, str]], include_export: bool = False) -> list[str]:
    """Get all changed steps and their downstream dependencies.

    :param include_export: If True, also return downstream viz and export steps (e.g. chart,
        explorer, bespoke recipes) with their full ``viz://`` / ``export://`` URI. These have no
        data:// catalogPath, so they're excluded by default (chart-diff/datadiff only care about
        data steps).
    """
    dataset_catalog_paths = []
    # Directly-changed viz/export steps (e.g. a modified chart or explorer recipe). These live under
    # etl/steps/viz/ or etl/steps/export/, not etl/steps/data/, so they aren't data catalog paths; we
    # keep their full URI and add them to the result when include_export is set.
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
            # Not a data/snapshot step. It might be a viz or export step (etl/steps/viz/...,
            # etl/steps/export/...); if so, record its URI so a branch that only edits such a
            # recipe still selects it.
            try:
                rel_step = abs_step_path.relative_to(STEP_DIR)
            except ValueError:
                continue
            step_type = rel_step.parts[0]
            if step_type not in NON_DATA_STEP_TYPES:
                continue
            rel_export = rel_step.relative_to(step_type)
            # A collection recipe can be split across companion config files named
            # `<short>.<key>.config.yml` (e.g. democracy.eiu.config.yml) that all feed the single
            # `<short>.py` step. Blindly stripping suffixes would invent a nonexistent
            # `<short>.<key>` step, so resolve to the sibling `<short>.py` recipe when it exists.
            short = abs_step_path.name.split(".", 1)[0]
            if (abs_step_path.parent / f"{short}.py").exists():
                export_path = (rel_export.parent / short).as_posix()
            else:
                export_path = rel_export.with_suffix("").with_suffix("").as_posix()
            changed_export_uris.append(f"{step_type}://{export_path}")

    # Dependency-only DAG edits (repoints) change a step's content without touching its files —
    # select those steps too, so they enter the subgraph expansion below like any changed step.
    for step_uri in sorted(get_dag_dependency_changed_steps(files_changed)):
        if step_uri.startswith(("data://", "data-private://")):
            ds_path = step_uri.split("://", 1)[1]
            if ds_path not in dataset_catalog_paths:
                dataset_catalog_paths.append(ds_path)
        elif step_uri.startswith(NON_DATA_URI_PREFIXES) and step_uri not in changed_export_uris:
            changed_export_uris.append(step_uri)

    if not dataset_catalog_paths:
        # No data steps changed. We can still have directly-changed viz/export steps; return those when
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
    all_data_steps = {s.split("://", 1)[1] for s in DAG if s.startswith(("data://", "data-private://"))}
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

    # From data://... extract catalogPath. Keep data-private:// steps too: dropping them here would
    # return an empty list for a private-only change, so `etl run --modified --private` (and
    # chart-diff/datadiff) would silently skip the affected private steps.
    # TODO: use StepPath from https://github.com/owid/etl/pull/3165 to refactor this
    catalog_paths = [step.split("://", 1)[1] for step in dag_steps if step.startswith(("data://", "data-private://"))]
    for sibling in sibling_versions:
        if sibling not in catalog_paths:
            catalog_paths.append(sibling)

    # Optionally also return viz/export steps, keeping their full URI so callers can match them with
    # `viz://...` include patterns (these steps have no data:// catalogPath). This covers both
    # downstream steps (reached via changed data steps) and directly-changed recipes.
    if include_export:
        downstream_export_uris = [step for step in dag_steps if step.startswith(NON_DATA_URI_PREFIXES)]
        # Dedupe while preserving order (a directly-changed export can also be a downstream one).
        seen = set(catalog_paths)
        for uri in downstream_export_uris + changed_export_uris:
            if uri not in seen:
                seen.add(uri)
                catalog_paths.append(uri)

    return catalog_paths
