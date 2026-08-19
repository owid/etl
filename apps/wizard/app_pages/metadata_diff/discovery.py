"""What did this branch change? Discovery for the Metadata Diff app.

The app used to ask the user to *find* the changes: pick an MDim from a list, or type a chart slug.
This module answers the question instead — "which indicators, MDims and explorers does this staging
server render differently from the baseline?" — so the page can open on the changes.

Deliberately free of Streamlit calls: the wizard sections and the owidbot PR comment both use it.

Two-step narrowing for the indicator layer, mirroring chart-diff's `_modified_data_metadata_on_staging`:
first restrict to variables whose dataset was touched on this staging server *and* whose catalog path
appears in this branch's changed files, then compare the actual texts. Without the narrowing, a branch
lagging master reports everything master has moved since as "changed here".

Indicators are matched across environments by **catalogPath, not id**: a version-bumped grapher step
mints fresh ids on staging, so id-matching would report every indicator of that dataset as changed.
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any

import git
import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.core import (
    ChangeGroup,
    ViewDiff,
    build_view_bundle,
    diff_views,
    field_label,
    group_changes,
    renders_change,
)
from apps.wizard.app_pages.metadata_diff.data import (
    _load_configs,
    build_env_bundles,
    fetch_variable_rows,
    fetch_variable_rows_by_path,
    load_mdim_config,
)
from apps.wizard.app_pages.metadata_diff.usage import _indicator_ids_in_mdim_config, charts_using_indicators
from etl.db import read_sql
from etl.git_helpers import get_changed_files
from etl.io import get_all_changed_catalog_paths
from etl.paths import STEP_DIR

log = get_logger()

# Above this many flagged MDims we stop resolving their texts view by view and report the cheap
# config/indicator flag instead. Real PRs touch a handful; a regions or FAOSTAT update can flag many.
MAX_MDIMS_RESOLVED = 25

# The `export://<kind>/...` segments that publish the two products this tool reviews. A recipe name is
# only unique within its kind, so every scope test names the kind it is asking about.
MDIM_EXPORT_KIND = "multidim"
EXPLORER_EXPORT_KIND = "explorers"


@dataclass
class IndicatorChanges:
    """Indicators whose user-visible metadata text differs from the baseline, keyed by catalogPath."""

    diffs: dict[str, ViewDiff] = field(default_factory=dict)
    ids: dict[str, int] = field(default_factory=dict)  # catalogPath -> staging variable id
    new_paths: set[str] = field(default_factory=set)  # on staging, absent from the baseline
    narrowed: bool = True  # False when git narrowing was unavailable (may include master's lag)

    @property
    def paths(self) -> list[str]:
        return sorted(self.diffs)

    @property
    def ids_list(self) -> list[int]:
        return sorted({self.ids[p] for p in self.diffs if p in self.ids})

    def view_diffs(self) -> list[ViewDiff]:
        return [self.diffs[p] for p in self.paths]


def _staging_creation_time(source_engine: Engine):
    """When this staging server was created — datasets edited before that carry master's state, not ours."""
    # Imported here: apps.wizard.utils pulls in Streamlit, and owidbot should not depend on it at import time.
    from apps.wizard.utils import get_staging_creation_time

    with Session(source_engine) as session:
        return get_staging_creation_time(session)


def _dataset_of(catalog_path: str) -> str:
    """Dataset identity of any catalog path, channel-insensitive.

    `grapher/wid/2026-06-18/wid/table#short` and `garden/wid/2026-06-18/wid` both give
    `wid/2026-06-18/wid` — which is also how `datasets.catalogPath` stores it.
    """
    parts = catalog_path.split("#")[0].strip("/").split("/")
    if parts and parts[0] in ("grapher", "garden", "meadow", "snapshot", "export"):
        parts = parts[1:]
    return "/".join(parts[:3])


@dataclass
class BranchScope:
    """What this branch actually builds — the yardstick for "did *we* change this?".

    A staging server drifts from production for two unrelated reasons: the branch's own edits, and
    master having moved on since the server was created. Both show up as differences, and only the first
    is the reviewer's business. Everything outside the scope is still reported, but separately: silently
    dropping a difference would be worse than showing it in the wrong bucket.
    """

    dataset_paths: set[str] = field(default_factory=set)  # dataset-level catalog paths (channel/ns/ver/ds)
    # `(export kind, name)` of what each changed export recipe publishes — the kind included because a
    # name is only unique within one: `migration/latest/migration_flows.py` exists under both `multidim`
    # and `explorers`, so a name-only set would let an edit to either recipe vouch for both products.
    export_products: set[tuple[str, str]] = field(default_factory=set)
    available: bool = True  # False when git could not tell us (then nothing is narrowed)

    @property
    def dataset_keys(self) -> set[str]:
        """The datasets in scope, keyed channel-insensitively (`ns/version/short`)."""
        return {_dataset_of(p) for p in self.dataset_paths}

    def covers_indicator(self, catalog_path: str) -> bool:
        return _dataset_of(catalog_path) in self.dataset_keys

    def covers_export(self, kind: str, short_name: str) -> bool:
        """Did this branch change the recipe that publishes this `kind` of product under this name?"""
        return (kind, short_name) in self.export_products


def branch_scope() -> BranchScope:
    """Read this branch's changed steps from git (data steps and export recipes alike)."""
    try:
        paths = get_all_changed_catalog_paths(get_changed_files(), include_export=True)
    except (git.exc.GitCommandError, git.exc.InvalidGitRepositoryError) as e:
        log.warning("metadata_diff.git_narrowing_unavailable", error=str(e))
        return BranchScope(available=False)

    datasets = {p for p in paths if not p.startswith("export://")}
    exports = {(_export_kind(p), name) for p in paths if p.startswith("export://") for name in _export_scope_names(p)}
    return BranchScope(dataset_paths=datasets, export_products=exports, available=True)


def _export_kind(export_uri: str) -> str:
    """The product an export recipe publishes, from its URI: `export://multidim/...` -> `multidim`."""
    return export_uri[len("export://") :].lstrip("/").split("/")[0]


# Where a collection recipe names what it publishes: `paths.create_collection(short_name=...)`, or the
# `collection_name=` a later call passes.
_COLLECTION_NAME_RE = re.compile(r"""(?:short_name|collection_name)\s*=\s*["']([^"']+)["']""")


def _emitted_collection_names(source: str) -> set[str]:
    """The collection names an export recipe's source publishes."""
    return set(_COLLECTION_NAME_RE.findall(source))


def _export_scope_names(export_uri: str) -> set[str]:
    """What an `export://` URI can be matched against — its step file name *and* what it publishes.

    An export URI is `export://explorers/<ns>/<version>/<short>`, whose tail is the recipe's **file
    name**. That is usually also the explorer slug or the MDim catalogPath tail, but not always:
    `explorers/emissions/latest/ipcc_scenarios.py` publishes `ipcc-scenarios`, and
    `multidim/un/latest/un_wpp.py` publishes `population-and-demography`. Matching on the file name
    alone files a recipe edit of those as baseline lag and drops it from the review, so read the names
    the recipe emits and accept either.
    """
    rel = export_uri[len("export://") :].rstrip("/")
    names = {rel.split("/")[-1]}
    step_file = STEP_DIR / "export" / f"{rel}.py"
    try:
        names |= _emitted_collection_names(step_file.read_text())
    except OSError:
        # A deleted or renamed recipe: the file name is all we have, and is still worth matching on.
        return names
    # A recipe publishing *several* collections names them from its own config files instead of from
    # literals — `multidim/covid/latest/covid.py` turns `covid.cases.yml` into `covid_cases` — so the
    # literals above find none of them. The `<recipe>.<key>[.config].yml` companion convention
    # reconstructs those names, and a name no collection answers to simply never matches anything.
    for config_file in step_file.parent.glob(f"{step_file.stem}.*.y*ml"):
        names.add(_config_file_collection_name(config_file.name))
    return names


def _config_file_collection_name(file_name: str) -> str:
    """The collection a `<recipe>.<key>[.config].yml` companion file stands for.

    `covid.cases.yml` -> `covid_cases`, `democracy.eiu.config.yml` -> `democracy_eiu`.
    """
    return "_".join(p for p in file_name.split(".")[:-1] if p != "config")


def _branch_catalog_paths() -> list[str] | None:
    """Dataset-level catalog paths this branch's changed files build (None if git can't tell us)."""
    scope = branch_scope()
    return sorted(scope.dataset_paths) if scope.available else None


def narrow_to_branch(paths: list[str], branch_paths: list[str] | None) -> tuple[list[str], bool]:
    """Keep only indicator paths whose dataset this branch builds. Returns (paths, narrowed)."""
    if branch_paths is None:
        return paths, False
    allowed = {_dataset_of(p) for p in branch_paths}
    return [p for p in paths if _dataset_of(p) in allowed], True


def datasets_built_here(source_engine: Engine) -> set[str]:
    """Datasets whose metadata this staging server rebuilt after it was created.

    The decisive "we did this" signal, and much tighter than the git scope: changed *files* expand into
    their whole downstream subgraph, so a one-line edit to one dataset's metadata put 118 datasets in
    scope on this branch while 9 had actually been rebuilt here. Attributing on scope alone therefore
    credited 526 differences in an unrelated UN WPP data page to this branch.
    """
    created = _staging_creation_time(source_engine)
    df = read_sql(
        "select catalogPath, metadataEditedAt from datasets where catalogPath is not null",
        engine=source_engine,
    )
    return {
        str(r["catalogPath"])
        for r in df.to_dict("records")
        if r["metadataEditedAt"] is not None and r["metadataEditedAt"] >= created
    }


def select_candidates(paths: list[str], scope: BranchScope, built: set[str]) -> tuple[list[str], bool]:
    """The pure selection: in the git scope AND rebuilt on this server. Returns (paths, narrowed).

    Both conditions are needed and neither suffices. The git scope alone is far too broad — changed files
    expand into their whole downstream subgraph — while "rebuilt here" alone would also match a dataset
    an automatic job refreshed on this server without the branch asking for it.
    """
    narrowed_paths, narrowed = narrow_to_branch(paths, sorted(scope.dataset_paths) if scope.available else None)
    return [p for p in narrowed_paths if _dataset_of(p) in built], narrowed


def candidate_paths(
    source_engine: Engine, paths: list[str], scope: BranchScope | None = None
) -> tuple[list[str], bool]:
    """`select_candidates`, reading the scope from git and the rebuilt-here set from the server."""
    scope = scope if scope is not None else branch_scope()
    return select_candidates(paths, scope, datasets_built_here(source_engine))


def charted_indicator_paths(source_engine: Engine) -> list[str]:
    """Catalog paths of indicators used by a published chart, whose dataset was edited on this server.

    Restricting to *charted* indicators is what keeps this cheap on a big data update: a WDI refresh
    touches tens of thousands of variables, but only the charted ones can change what a reader sees.
    """
    df = read_sql(
        """
        select distinct v.catalogPath as catalogPath
        from variables v
        join datasets d on v.datasetId = d.id
        join chart_dimensions cd on cd.variableId = v.id
        join charts c on c.id = cd.chartId
        where v.catalogPath is not null
          and c.publishedAt is not null
          and (d.dataEditedAt >= %(ts)s or d.metadataEditedAt >= %(ts)s)
        """,
        engine=source_engine,
        params={"ts": _staging_creation_time(source_engine)},
    )
    return [str(p) for p in df["catalogPath"].tolist()]


def compare_indicator_texts(
    source_rows: dict[str, dict[str, Any]],
    target_rows: dict[str, dict[str, Any]],
) -> IndicatorChanges:
    """Pure comparison: which indicators' texts differ, as one ViewDiff each.

    Producing ViewDiffs (an indicator is a "view" with no dimensions) means `group_changes`, the diff
    renderers and the PR brief all work on chart-side changes without a second implementation.
    """
    out = IndicatorChanges()
    for path, src_row in source_rows.items():
        if src_row.get("id") is not None:
            out.ids[path] = int(src_row["id"])
        tgt_row = target_rows.get(path)
        if tgt_row is None:
            out.new_paths.add(path)
            continue
        src = build_view_bundle(view={"dimensions": {}}, config_metadata=None, variable_row=src_row, chart_config=None)
        tgt = build_view_bundle(view={"dimensions": {}}, config_metadata=None, variable_row=tgt_row, chart_config=None)
        diff = diff_views([src], [tgt])[0]
        if diff.changed:
            diff.catalog_path = path
            out.diffs[path] = diff
    return out


def changed_indicators(
    source_engine: Engine,
    target_engine: Engine,
    catalog_paths: list[str] | None = None,
) -> IndicatorChanges:
    """Indicators whose text this branch changed. `catalog_paths` defaults to charted indicators."""
    paths = charted_indicator_paths(source_engine) if catalog_paths is None else list(catalog_paths)
    paths, narrowed = narrow_to_branch(paths, _branch_catalog_paths())
    if not paths:
        return IndicatorChanges(narrowed=narrowed)

    source_rows = fetch_variable_rows_by_path(source_engine, paths)
    target_rows = fetch_variable_rows_by_path(target_engine, paths)
    out = compare_indicator_texts(source_rows, target_rows)
    out.narrowed = narrowed
    return out


def dataset_edit_times(engine: Engine) -> dict[str, Any]:
    """Dataset catalogPath (`ns/version/short`) -> when its metadata was last edited in this environment."""
    df = read_sql(
        "select catalogPath, metadataEditedAt from datasets where catalogPath is not null",
        engine=engine,
    )
    return {str(r["catalogPath"]): r["metadataEditedAt"] for r in df.to_dict("records")}


# Where a difference came from. Only OURS is this branch's own text; the others each need saying out loud.
OURS = "ours"  # differs from the baseline AND from master's own environment — nobody else has this text
MASTER = "master"  # identical to master's environment — master's edit the baseline has not rebuilt yet
STALE = "stale"  # this server's build of the dataset predates the baseline's, so the diff reads backwards
UNKNOWN = "unknown"  # master's environment was unavailable, so the two could not be told apart


def stale_datasets(source_engine: Engine, target_engine: Engine) -> dict[str, tuple[Any, Any]]:
    """Datasets this server built and has since fallen behind the baseline on: dataset -> (here, baseline).

    The failure this exists for, seen on a real branch: a staging build only rebuilds steps that differ
    from master, so the moment a branch's edit to a dataset is reverted, that dataset stops being selected
    and the server keeps serving its old build indefinitely — including the reverted edit. The tool then
    shows text nobody on the branch wrote, as though the branch had written it. Nothing else notices.

    "Built here" is what separates that from the baseline merely moving on. A dataset this server never
    rebuilt is older here for the ordinary reason — the baseline rebuilt it after the fork — and no
    rebuild of ours would change that, so counting it would put a 🚧 and a pointless rebuild command on
    every server that has been up for a while. It cannot be narrowed by the git scope instead: a reverted
    edit leaves the branch's diff, so the very datasets this exists for are out of scope by then.
    """
    created = _staging_creation_time(source_engine)
    source_times = dataset_edit_times(source_engine)
    target_times = dataset_edit_times(target_engine)
    out: dict[str, tuple[Any, Any]] = {}
    for dataset, here in source_times.items():
        there = target_times.get(dataset)
        if here is None or there is None:
            continue
        # `here >= created` is "this server rebuilt it after it was forked" — as in datasets_built_here.
        if here >= created and here < there:
            out[dataset] = (here, there)
    return out


def classify_origins(
    catalog_paths: list[str],
    identical_to_master: set[str],
    stale: dict[str, tuple[Any, Any]],
    master_checked: bool,
) -> dict[str, str]:
    """Pure classification of each changed indicator's difference.

    Precedence is deliberate. STALE first, because a stale build makes the whole comparison read backwards
    and no other label is worth acting on until it is fixed. Then MASTER, which is decisive when it holds:
    text identical to master's own environment cannot be this branch's invention. What remains is OURS —
    and unlike the dataset-level guess this replaces, that verdict is per text, so a change the branch
    really did make no longer gets hedged just because master also touched the same dataset.
    """
    out: dict[str, str] = {}
    for path in catalog_paths:
        if _dataset_of(path) in stale:
            out[path] = STALE
        elif not master_checked:
            out[path] = UNKNOWN
        elif path in identical_to_master:
            out[path] = MASTER
        else:
            out[path] = OURS
    return out


def attribute_indicator_changes(
    source_engine: Engine,
    target_engine: Engine,
    catalog_paths: list[str],
    master_engine: Engine | None = None,
) -> dict[str, str]:
    """Say, per changed indicator, where its difference came from.

    `master_engine` is master's own staging server, and it is what makes the answer decisive rather than
    circumstantial: if our text matches master's, master wrote it and the baseline simply has not rebuilt
    yet. Pass None (or the baseline itself, which would answer trivially) and every change falls back to
    UNKNOWN rather than being guessed at.
    """
    if not catalog_paths:
        return {}

    stale = stale_datasets(source_engine, target_engine)

    identical_to_master: set[str] = set()
    master_checked = False
    if master_engine is not None and master_engine is not target_engine:
        try:
            result = compare_indicator_texts(
                fetch_variable_rows_by_path(source_engine, catalog_paths),
                fetch_variable_rows_by_path(master_engine, catalog_paths),
            )
            identical_to_master = set(catalog_paths) - set(result.diffs) - result.new_paths
            master_checked = True
        except Exception as e:  # noqa: BLE001 — no master server is a reason to say "unknown", not to fail
            log.warning("metadata_diff.master_comparison_failed", error=str(e))

    return classify_origins(catalog_paths, identical_to_master, stale, master_checked)


def charts_affected(source_engine: Engine, changed: IndicatorChanges) -> dict[int, list[dict[str, Any]]]:
    """Staging indicator id -> published charts rendering it (empty when nothing changed)."""
    if not changed.ids_list:
        return {}
    return charts_using_indicators(source_engine, changed.ids_list)


# --- MDims --------------------------------------------------------------------------------------


def mdim_list(engine: Engine) -> pd.DataFrame:
    """Every MDim with its config hash, publication state and slug (most recently updated first).

    `config` is deliberately excluded: it is a large JSON blob, and selecting it alongside
    `order by updatedAt` makes MySQL sort rows carrying those blobs, which overruns the sort buffer.
    """
    return read_sql(
        """
        select catalogPath, configMd5, published, slug
        from multi_dim_data_pages
        where catalogPath is not null
        order by updatedAt desc
        """,
        engine=engine,
    )


def mdim_indicator_paths(source_engine: Engine, configs: dict[str, dict[str, Any]]) -> dict[str, set[str]]:
    """catalogPath of each MDim -> catalog paths of the indicators its views reference."""
    ids_by_mdim = {cp: _indicator_ids_in_mdim_config(cfg) for cp, cfg in configs.items()}
    all_ids = sorted({i for ids in ids_by_mdim.values() for i in ids})
    if not all_ids:
        return {cp: set() for cp in configs}

    rows = fetch_variable_rows(source_engine, all_ids)
    path_by_id = {i: str(r["catalogPath"]) for i, r in rows.items() if r.get("catalogPath")}
    return {cp: {path_by_id[i] for i in ids if i in path_by_id} for cp, ids in ids_by_mdim.items()}


def mdim_changes_df(source_engine: Engine, target_engine: Engine) -> pd.DataFrame:
    """Every MDim on this server, flagged against the baseline. Indexed by catalogPath.

    Two independent signals, because either changes what a reader sees:
    - `config_changed`: the MDim's own config (view definitions, view-level metadata overrides).
    - `indicator_changed`: the text of an indicator it uses. Most text edits are authored in the garden
      step and land here, leaving the config byte-identical — so a config-only signal misses exactly
      the case this tool exists for.

    `has_changes` is their union. `in_branch` narrows that to the MDims this branch is responsible for:
    `config_changed` on its own is usually baseline lag (master rebuilt the MDim after this server was
    created), which would otherwise bury the branch's own edits. Nothing is dropped — the section shows
    the rest under "other differences". `indicator_check_failed` says the second signal could not be
    computed, so the caller can warn instead of silently showing fewer changes.
    """
    df_source = mdim_list(source_engine)
    df_target = mdim_list(target_engine)

    df = pd.merge(df_source, df_target, on="catalogPath", suffixes=("_source", "_target"), how="left")
    df["is_new"] = df["configMd5_target"].isnull()
    df["config_changed"] = df["configMd5_source"] != df["configMd5_target"]

    changed_mdims: set[str] = set()
    try:
        paths_by_mdim = mdim_indicator_paths(source_engine, _load_configs(source_engine))
        all_paths = sorted({p for paths in paths_by_mdim.values() for p in paths})
        narrowed_paths, _ = candidate_paths(source_engine, all_paths)
        changed_paths: set[str] = set()
        if narrowed_paths:
            result = compare_indicator_texts(
                fetch_variable_rows_by_path(source_engine, narrowed_paths),
                fetch_variable_rows_by_path(target_engine, narrowed_paths),
            )
            changed_paths = set(result.diffs) | result.new_paths
        changed_mdims = {cp for cp, paths in paths_by_mdim.items() if paths & changed_paths}
    except Exception as e:  # noqa: BLE001 — never let the flag break the page; degrade and say so
        log.warning("metadata_diff.indicator_change_check_failed", error=str(e))
        df["indicator_check_failed"] = True
    else:
        df["indicator_check_failed"] = False

    df["indicator_changed"] = df["catalogPath"].isin(changed_mdims)
    df["has_changes"] = df["is_new"] | df["config_changed"] | df["indicator_changed"]

    scope = branch_scope()
    if scope.available:
        # An MDim's own recipe changing (`export://multidim/.../<short>`) is the other way this branch can
        # move its texts, so a config change counts when the recipe is ours.
        own_recipe = df["catalogPath"].map(lambda cp: scope.covers_export(MDIM_EXPORT_KIND, mdim_short_name(str(cp))))
        df["in_branch"] = mdim_in_branch(df, own_recipe)
    else:
        df["in_branch"] = df["has_changes"]
    df["scope_available"] = scope.available
    return df.set_index("catalogPath")


def mdim_in_branch(df: pd.DataFrame, own_recipe: pd.Series) -> pd.Series:
    """Which flagged MDims are this branch's work rather than the baseline having moved on.

    An indicator-layer change is already narrowed to indicators this server rebuilt from this branch's
    files. Everything else about an MDim lives in its config, so it counts only when this branch changed
    the recipe that publishes it — and that includes *being new*: a staging server materializes master's
    rebuilds too, so an MDim master added after the baseline was published arrives here with nobody in
    this PR having touched it. Nothing is dropped either way; `has_changes` still carries it into the
    "not this branch" bucket the page and the PR comment report separately.
    """
    return df["indicator_changed"] | ((df["is_new"] | df["config_changed"]) & own_recipe)


def flagged_mdims(source_engine: Engine, target_engine: Engine, in_branch_only: bool = True) -> list[str]:
    """catalogPaths of the MDims worth diffing (this branch's by default)."""
    df = mdim_changes_df(source_engine, target_engine)
    column = "in_branch" if in_branch_only else "has_changes"
    return [str(cp) for cp in df.index[df[column]]]


def mdim_short_name(catalog_path: str) -> str:
    """The step short name an MDim's catalogPath ends in (`grapher/wid/latest/incomes_wid#…` -> `incomes_wid`)."""
    return catalog_path.split("#")[0].rstrip("/").split("/")[-1]


def split_mdim_groups(
    catalog_path: str, view_diffs: list[ViewDiff], scope: BranchScope | None = None
) -> tuple[list[ChangeGroup], list[ChangeGroup]]:
    """Split one MDim's changes into (this branch's, other differences).

    Being flagged as this branch's MDim does not make *every* difference in it ours. Its views' chart
    configs are rebuilt whenever master rebuilds the MDim, so on an older staging server they differ
    wholesale — enough to report a hundred "chart title" changes nobody in this PR wrote. So unless the
    branch changed the MDim's own recipe (where config-level edits are exactly what it does), only
    indicator-layer changes count as ours; the rest are returned separately, not dropped.
    """
    scope = scope if scope is not None else branch_scope()
    groups = group_changes([v for v in view_diffs if v.changed])
    if not scope.available or scope.covers_export(MDIM_EXPORT_KIND, mdim_short_name(catalog_path)):
        return groups, []
    return [g for g in groups if g.affects_indicator], [g for g in groups if not g.affects_indicator]


def mdim_text_changes(source_engine: Engine, target_engine: Engine, catalog_path: str) -> list[ViewDiff]:
    """Per-view text diff of one MDim against the baseline (cached for the app in `cached.py`)."""
    source_config = load_mdim_config(source_engine, catalog_path)
    if source_config is None:
        return []
    target_config = load_mdim_config(target_engine, catalog_path)
    source_bundles = build_env_bundles(source_engine, source_config)
    target_bundles = build_env_bundles(target_engine, target_config) if target_config else []
    return diff_views(source_bundles, target_bundles)


# --- Explorers ----------------------------------------------------------------------------------


def explorer_view_rows(engine: Engine) -> dict[tuple[str, str], dict[str, Any]]:
    """Resolved text of every view of every published explorer, keyed by (explorer slug, viewId).

    `chart_configs.full` is the *resolved* config, so indicator-driven inheritance (`title_public` ->
    title, `description_short` -> subtitle) is already baked in — no metadata resolution needed here.
    Caveats the UI states rather than hides: a legacy CSV-backed explorer has no `explorer_views` rows,
    and `full` only refreshes when the explorer's export step re-runs.
    """
    df = read_sql(
        """
        select ev.explorerSlug as explorerSlug,
               ev.viewId as viewId,
               ev.dimensions as dimensions,
               cc.full ->> '$.dimensions' as indicators,
               cc.full ->> '$.title' as title,
               cc.full ->> '$.subtitle' as subtitle,
               cc.full ->> '$.note' as note
        from explorer_views ev
        join explorers e on e.slug = ev.explorerSlug
        join chart_configs cc on cc.id = ev.chartConfigId
        where e.isPublished = 1
        """,
        engine=engine,
    )
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for record in df.to_dict("records"):
        raw_dims = record.get("dimensions")
        try:
            dims = json.loads(raw_dims) if isinstance(raw_dims, str) else (raw_dims or {})
        except (TypeError, ValueError):
            dims = {}
        out[(str(record["explorerSlug"]), str(record["viewId"]))] = {
            "dimensions": {str(k): str(v) for k, v in dims.items()} if isinstance(dims, dict) else {},
            "indicator_ids": _variable_ids(record.get("indicators")),
            "title": record.get("title"),
            "subtitle": record.get("subtitle"),
            "note": record.get("note"),
        }
    return out


def _variable_ids(raw: Any) -> set[int]:
    """The variable ids a chart config's `dimensions` array references."""
    try:
        dims = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return set()
    if not isinstance(dims, list):
        return set()
    return {int(d["variableId"]) for d in dims if isinstance(d, dict) and d.get("variableId") is not None}


def compare_explorer_views_detailed(
    source_rows: dict[tuple[str, str], dict[str, Any]],
    target_rows: dict[tuple[str, str], dict[str, Any]],
) -> dict[tuple[str, str], ViewDiff]:
    """Pure comparison, keyed by (explorer slug, viewId) — the key that is stable across environments."""
    out: dict[tuple[str, str], ViewDiff] = {}
    for (slug, view_id), src_row in sorted(source_rows.items()):
        tgt_row = target_rows.get((slug, view_id))
        dims = src_row.get("dimensions") or {}
        src = build_view_bundle(
            view={"dimensions": dims}, config_metadata=None, variable_row=None, chart_config=src_row
        )
        targets = []
        if tgt_row is not None:
            targets.append(
                build_view_bundle(
                    view={"dimensions": dims}, config_metadata=None, variable_row=None, chart_config=tgt_row
                )
            )
        diff = diff_views([src], targets)[0]
        if diff.changed:
            out[(slug, view_id)] = diff
    return out


def compare_explorer_views(
    source_rows: dict[tuple[str, str], dict[str, Any]],
    target_rows: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, list[ViewDiff]]:
    """The same comparison, grouped by explorer slug."""
    out: dict[str, list[ViewDiff]] = {}
    for (slug, _), diff in compare_explorer_views_detailed(source_rows, target_rows).items():
        out.setdefault(slug, []).append(diff)
    return out


@dataclass
class ExplorerChanges:
    """Explorer views that differ from the baseline, split by whether this branch caused it.

    An explorer view's text is stored resolved, so it also moves when master rebuilds the explorer. On a
    two-week-old staging server that lag can run to thousands of views — which would bury the handful the
    branch actually changed. Both are kept; only the grouping differs.
    """

    views: dict[str, list[ViewDiff]] = field(default_factory=dict)  # slug -> changed views
    in_branch: set[str] = field(default_factory=set)  # slugs with at least one view this branch changed
    narrowed: bool = True
    # Attribution is per *view*, not per slug: a lagging explorer differs in every one of its views, and
    # one of them carrying this branch's edit must not drag the other hundreds along. Set together by
    # `changed_explorer_views`; left None by hand-built instances, which fall back to whole slugs.
    branch_view_diffs: dict[str, list[ViewDiff]] | None = None
    other_view_diffs: dict[str, list[ViewDiff]] | None = None

    def branch_views(self) -> dict[str, list[ViewDiff]]:
        if not self.narrowed:
            return self.views
        if self.branch_view_diffs is not None:
            return self.branch_view_diffs
        return {slug: diffs for slug, diffs in self.views.items() if slug in self.in_branch}

    def other_views(self) -> dict[str, list[ViewDiff]]:
        if not self.narrowed:
            return {}
        if self.other_view_diffs is not None:
            return self.other_view_diffs
        return {slug: diffs for slug, diffs in self.views.items() if slug not in self.in_branch}


def changed_explorer_views(source_engine: Engine, target_engine: Engine) -> ExplorerChanges:
    """Published explorers whose view text differs from the baseline, attributed to branch or lag."""
    source_rows = explorer_view_rows(source_engine)
    detailed = compare_explorer_views_detailed(source_rows, explorer_view_rows(target_engine))

    views: dict[str, list[ViewDiff]] = {}
    for (slug, _), diff in detailed.items():
        views.setdefault(slug, []).append(diff)

    scope = branch_scope()
    if not scope.available or not views:
        return ExplorerChanges(views=views, in_branch=set(views), narrowed=scope.available)

    # A *view* is this branch's if the explorer's own export recipe changed (then every view of it is),
    # or if the text of an indicator that view renders actually changed. "Uses a dataset this branch
    # builds" is too loose: an explorer master rebuilt last week differs in every view, and one of its
    # datasets being in scope would then credit all of it to this PR. Per view rather than per slug for
    # the same reason: one qualifying view must not vouch for the other hundreds in the same explorer.
    own_recipe = {slug for slug in views if scope.covers_export(EXPLORER_EXPORT_KIND, slug)}
    ids_by_view: dict[tuple[str, str], set[int]] = {}
    for key in detailed:
        if key[0] in own_recipe:
            continue
        ids_by_view[key] = set(source_rows.get(key, {}).get("indicator_ids") or set())

    all_ids = sorted({i for ids in ids_by_view.values() for i in ids})
    rows = fetch_variable_rows(source_engine, all_ids) if all_ids else {}
    paths_by_id = {i: str(r["catalogPath"]) for i, r in rows.items() if r.get("catalogPath")}
    # Only indicators this branch actually rebuilt here can carry one of its edits — compare just those.
    candidates, _ = candidate_paths(source_engine, sorted(set(paths_by_id.values())), scope)
    candidates = sorted(set(candidates))
    changed_paths: set[str] = set()
    if candidates:
        result = compare_indicator_texts(
            fetch_variable_rows_by_path(source_engine, candidates),
            fetch_variable_rows_by_path(target_engine, candidates),
        )
        changed_paths = set(result.diffs) | result.new_paths

    ours_keys = {key for key in detailed if key[0] in own_recipe}
    ours_keys |= {key for key, ids in ids_by_view.items() if any(paths_by_id.get(i) in changed_paths for i in ids)}

    branch: dict[str, list[ViewDiff]] = {}
    other: dict[str, list[ViewDiff]] = {}
    for key, diff in detailed.items():
        bucket = branch if key in ours_keys else other
        bucket.setdefault(key[0], []).append(diff)

    return ExplorerChanges(
        views=views,
        in_branch=set(branch),
        narrowed=True,
        branch_view_diffs=branch,
        other_view_diffs=other,
    )


# --- One summary for the section badges and the owidbot comment ----------------------------------


@dataclass
class Summary:
    """Counts behind the section badges and the owidbot report."""

    n_charts: int = 0  # published charts rendering a changed indicator text
    n_indicators: int = 0  # indicators whose text changed
    n_chart_changes: int = 0  # distinct (field, old -> new) changes on the indicator layer
    n_new_indicators: int = 0  # on staging, absent from the baseline (no old text to diff)
    n_mdims: int = 0  # MDims whose view texts changed
    n_mdim_changes: int = 0
    n_mdims_flagged: int = 0  # config or indicator metadata differs (a superset of n_mdims)
    n_explorers: int = 0
    n_explorer_views: int = 0
    n_other_mdims: int = 0  # differ from the baseline, but not attributable to this branch
    n_other_explorers: int = 0
    fields: dict[str, int] = field(default_factory=dict)  # field label -> distinct changes
    # Indicator changes by origin: ours / master / stale / unknown (see classify_origins).
    attribution: dict[str, int] = field(default_factory=dict)
    # Datasets this server holds an older build of than the baseline: dataset -> (here, baseline).
    # Their differences read backwards, so this is a defect in the server, not in the branch.
    stale: dict[str, tuple[Any, Any]] = field(default_factory=dict)
    n_distinct_changes: int = 0  # distinct texts changed, counted once across all surfaces
    mdims_resolved: bool = True  # False when there were too many flagged MDims to diff view by view
    narrowed: bool = True
    warnings: list[str] = field(default_factory=list)

    @property
    def n_other(self) -> int:
        """Differences the branch is not responsible for (baseline lag), shown separately."""
        return self.n_other_mdims + self.n_other_explorers

    @property
    def total_changes(self) -> int:
        return self.n_chart_changes + self.n_mdim_changes + self.n_explorer_views

    @property
    def has_changes(self) -> bool:
        """Whether there is reader-facing text here that nobody has reviewed yet.

        New indicators count. A version bump replaces every catalog path, so nothing has a baseline
        counterpart to diff against — and reporting that as "no metadata text changes" would wave
        through a whole dataset's worth of new text.
        """
        return bool(self.n_charts or self.n_mdims or self.n_explorers or self.n_new_indicators)


def charts_reached(groups: list[ChangeGroup], usage: dict[int, list[dict[str, Any]]]) -> set[int]:
    """Chart ids where at least one of these changes is actually visible to a reader.

    Every chart using the indicator is *affected*; not every one *shows* the field that changed. A WYSK
    edit reaches only the charts with a data page, so counting the rest would have the PR comment claim
    an audience that cannot see the edit.
    """
    reached: set[int] = set()
    for g in groups:
        ids = g.indicator_ids or ({g.indicator_id} if g.indicator_id is not None else set())
        for iid in ids:
            reached.update(int(c["chartId"]) for c in usage.get(iid, []) if renders_change(g, c))
    return reached


def change_identity(g: ChangeGroup) -> tuple[str, str]:
    """A distinct text change, independent of which surface renders it."""
    return (g.field, json.dumps([g.old, g.new], sort_keys=True, default=str))


def _collect_changes(seen: set[tuple[str, str]], groups: list[ChangeGroup]) -> None:
    """Accumulate distinct changes across surfaces.

    One reworded shared definition surfaces on its indicator, on every MDim view rendering it, and on
    every chart inheriting it. Adding those up would report the same edit several times over, so the
    field breakdown counts identities, not sightings.
    """
    seen.update(change_identity(g) for g in groups)


def _count_fields(counts: dict[str, int], diffs: list[ViewDiff]) -> None:
    """Field breakdown of a single surface's diffs (used where no cross-surface dedup is needed)."""
    for g in group_changes(diffs):
        label = field_label(g.field)
        counts[label] = counts.get(label, 0) + 1


def summarize(source_engine: Engine, target_engine: Engine, master_engine: Engine | None = None) -> Summary:
    """Everything the section badges and the owidbot comment need, in one pass.

    `master_engine` (master's own staging server) is optional but worth passing: without it, a change
    cannot be told from an edit master made that the baseline has not rebuilt yet, and everything lands
    under UNKNOWN.

    Each surface is wrapped: one failing (a table missing on an old staging server, say) must not blank
    the whole page or the PR comment, so it degrades to a warning the UI shows.
    """
    summary = Summary()
    seen: set[tuple[str, str]] = set()

    # --- Charts (indicator layer) ---
    try:
        changed = changed_indicators(source_engine, target_engine)
        summary.narrowed = changed.narrowed
        summary.n_indicators = len(changed.diffs)
        summary.n_new_indicators = len(changed.new_paths)
        diffs = changed.view_diffs()
        chart_groups = group_changes(diffs)
        summary.n_chart_changes = len(chart_groups)
        _collect_changes(seen, chart_groups)
        usage = charts_affected(source_engine, changed)
        summary.n_charts = len(charts_reached(chart_groups, usage))
        for origin in attribute_indicator_changes(source_engine, target_engine, changed.paths, master_engine).values():
            summary.attribution[origin] = summary.attribution.get(origin, 0) + 1
        # Report every stale dataset, not only ones behind a reported change: a stale build can also
        # *hide* a change, and then there is nothing in the lists to hang the warning off.
        summary.stale = stale_datasets(source_engine, target_engine)
    except Exception as e:  # noqa: BLE001 — a broken surface must not blank the whole report
        log.warning("metadata_diff.chart_discovery_failed", error=str(e))
        summary.warnings.append(f"Chart discovery failed: {e}")

    # --- MDims ---
    try:
        df_mdims = mdim_changes_df(source_engine, target_engine)
        if bool(df_mdims["indicator_check_failed"].any()):
            summary.warnings.append(
                "Could not compare indicator metadata for MDims — the count reflects config changes only."
            )
        flagged = [str(cp) for cp in df_mdims.index[df_mdims["in_branch"]]]
        summary.n_mdims_flagged = len(flagged)
        summary.n_other_mdims = int(df_mdims["has_changes"].sum()) - len(flagged)
        if len(flagged) > MAX_MDIMS_RESOLVED:
            # Too many to diff view by view here; report the flag count and say the number is a ceiling.
            summary.mdims_resolved = False
            summary.n_mdims = len(flagged)
        else:
            scope = branch_scope()
            for cp in flagged:
                view_diffs = [v for v in mdim_text_changes(source_engine, target_engine, cp) if v.changed]
                ours, _other = split_mdim_groups(cp, view_diffs, scope)
                if ours:
                    summary.n_mdims += 1
                    summary.n_mdim_changes += len(ours)
                    _collect_changes(seen, ours)
    except Exception as e:  # noqa: BLE001
        log.warning("metadata_diff.mdim_discovery_failed", error=str(e))
        summary.warnings.append(f"MDim discovery failed: {e}")

    # --- Explorers ---
    try:
        explorers = changed_explorer_views(source_engine, target_engine)
        branch_views = explorers.branch_views()
        summary.n_explorers = len(branch_views)
        summary.n_explorer_views = sum(len(v) for v in branch_views.values())
        summary.n_other_explorers = len(explorers.other_views())
        for diffs in branch_views.values():
            _collect_changes(seen, group_changes(diffs))
    except Exception as e:  # noqa: BLE001
        log.warning("metadata_diff.explorer_discovery_failed", error=str(e))
        summary.warnings.append(f"Explorer discovery failed: {e}")

    for field_name, _ in seen:
        label = field_label(field_name)
        summary.fields[label] = summary.fields.get(label, 0) + 1
    summary.n_distinct_changes = len(seen)

    return summary
