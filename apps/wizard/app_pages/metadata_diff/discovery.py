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
from pathlib import Path
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
    surface_key,
)
from apps.wizard.app_pages.metadata_diff.data import (
    _load_configs,
    build_env_bundles,
    count_reviewed,
    fetch_variable_rows,
    fetch_variable_rows_by_path,
    load_mdim_config,
)
from apps.wizard.app_pages.metadata_diff.usage import _indicator_ids_in_mdim_config, charts_using_indicators
from etl.db import read_sql
from etl.git_helpers import get_changed_files
from etl.io import get_all_changed_catalog_paths, get_directly_changed_export_uris
from etl.paths import BASE_DIR, STEP_DIR

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
    return "/".join(_catalog_path_parts(catalog_path)[:3])


_CHANNEL_PREFIXES = ("grapher", "garden", "meadow", "snapshot", "export")


def _catalog_path_parts(catalog_path: str) -> list[str]:
    """`namespace/version/short/...` of a catalog path, with any channel prefix dropped."""
    parts = catalog_path.split("#")[0].strip("/").split("/")
    if parts and parts[0] in _CHANNEL_PREFIXES:
        parts = parts[1:]
    return parts


@dataclass
class BranchScope:
    """What this branch actually builds — the yardstick for "did *we* change this?".

    A staging server drifts from production for two unrelated reasons: the branch's own edits, and
    master having moved on since the server was created. Both show up as differences, and only the first
    is the reviewer's business. Everything outside the scope is still reported, but separately: silently
    dropping a difference would be worse than showing it in the wrong bucket.
    """

    dataset_paths: set[str] = field(default_factory=set)  # dataset-level catalog paths (channel/ns/ver/ds)
    # `(export kind, name)` of what each export recipe this branch *edited* publishes — the kind
    # included because a name is only unique within one: `migration/latest/migration_flows.py` exists
    # under both `multidim` and `explorers`, so a name-only set would let an edit to either recipe vouch
    # for both products. Only recipes edited here belong in it: a data-step edit expands into every
    # export downstream of it, and claiming those would hand the branch a hundred config-level texts
    # nobody in the PR wrote — exactly what `split_mdim_groups` and `mdim_in_branch` exist to prevent.
    export_products: set[tuple[str, str]] = field(default_factory=set)
    # `(kind, name)` -> the namespaces of the edited recipes publishing under that name. A name is not
    # unique within a kind either: `multidim/emissions/latest/air_pollution.py` and
    # `multidim/ihme_gbd/latest/air_pollution.py` both publish an `air_pollution` MDim, so kind+name alone
    # let an edit to one vouch for the other's lagging views. Empty for a hand-built scope, which then
    # keeps matching on kind+name alone.
    export_namespaces: dict[tuple[str, str], set[str]] = field(default_factory=dict)
    available: bool = True  # False when git could not tell us (then nothing is narrowed)

    @property
    def dataset_keys(self) -> set[str]:
        """The datasets in scope, keyed channel-insensitively (`ns/version/short`)."""
        return {_dataset_of(p) for p in self.dataset_paths}

    def covers_indicator(self, catalog_path: str) -> bool:
        return _dataset_of(catalog_path) in self.dataset_keys

    def covers_export(self, kind: str, short_name: str, namespace: str | None = None) -> bool:
        """Did this branch change the recipe that publishes this `kind` of product under this name?

        `namespace` disambiguates the two recipes of one kind that share a file name. Pass it whenever the
        product's own identity carries a namespace — an MDim catalogPath does; an explorer slug does not.
        """
        if (kind, short_name) not in self.export_products:
            return False
        if namespace is None:
            return True
        owners = self.export_namespaces.get((kind, short_name))
        # No recorded namespace means a hand-built scope, so kind+name is all there is to go on.
        return namespace in owners if owners else True

    def covers_mdim(self, catalog_path: str) -> bool:
        """Did this branch change the recipe that publishes this MDim?

        `paths.create_collection` builds the catalogPath from the recipe's own namespace, version and
        short name, so both halves of the comparison come from the same place.
        """
        return self.covers_export(MDIM_EXPORT_KIND, mdim_short_name(catalog_path), mdim_namespace(catalog_path))


def branch_scope() -> BranchScope:
    """Read this branch's changed steps from git (data steps and export recipes alike)."""
    try:
        files_changed = get_changed_files()
        paths = get_all_changed_catalog_paths(files_changed, include_export=True)
        # Only the recipes this branch *edited*. `paths` also carries every export downstream of a
        # changed data step, and taking those as ours is what `export_products` must not do.
        changed_export_uris = get_directly_changed_export_uris(files_changed)
    except (git.exc.GitCommandError, git.exc.InvalidGitRepositoryError) as e:
        log.warning("metadata_diff.git_narrowing_unavailable", error=str(e))
        return BranchScope(available=False)

    datasets = {p for p in paths if not p.startswith("export://")} | _shared_step_file_datasets(files_changed)
    # Only recipes the DAG still builds. A retired recipe left in the tree derives a URI that publishes
    # nothing, yet still *reads* like the product it used to build, and `_export_scope_names` takes the
    # names out of its source: `explorers/wash/2024-02-15/water_and_sanitation.py` is in no DAG but still
    # emits `water-and-sanitation`, so editing it claimed the live `wash/latest` explorer and filed that
    # explorer's baseline lag as this branch's work. Erring narrow is the right direction on the export
    # side — `covers_export` has no second gate and hands over every config difference in the product it
    # matches — and a dropped URI is still reported, under "other differences".
    active_exports = _active_export_uris()
    export_uris = {u for u in changed_export_uris if u in active_exports} | _shared_export_recipe_uris(files_changed)
    exports = {(_export_kind(p), name) for p in export_uris for name in _export_scope_names(p)}
    namespaces: dict[tuple[str, str], set[str]] = {}
    for uri in export_uris:
        namespace = _export_namespace(uri)
        for name in _export_scope_names(uri):
            namespaces.setdefault((_export_kind(uri), name), set()).add(namespace)
    return BranchScope(dataset_paths=datasets, export_products=exports, export_namespaces=namespaces, available=True)


def _shared_step_file_datasets(files_changed: dict[str, Any]) -> set[str]:
    """Datasets reached by a changed *shared* file in a step folder, which names no step of its own.

    `shared.meta.yml` is merged into every sibling `<step>.meta.yml` by the catalog loader, and a
    `shared.py` helper is imported by its siblings — neither is a step. `get_all_changed_catalog_paths`
    resolves them to a `.../shared` path that appears in no DAG, so the subgraph expansion returns
    nothing and the scope comes back **empty**. An empty scope narrows every rebuilt indicator away, and
    the tool then reports "no metadata text changes" for an edit that rewrote reader-facing text across
    every dataset in the folder (`ihme_gbd/2026-02-07` has eleven).

    So when a changed step file is not itself a step, credit every DAG step in its folder. Erring broad
    is the safe direction here: this is a narrowing filter, and `datasets_built_here` still has to agree
    before anything counts as ours.

    A step implemented as a *package* fails the same way for the opposite reason: its files sit one level
    **below** the sibling layout, inside the step folder. `garden/democracy/2026-03-17/vdem` keeps 200 kB
    of reader-facing text in `.../vdem/vdem.meta.yml`, which strips to `.../vdem/vdem` — again a path in
    no DAG, again an empty scope, again a false all-clear on an edit that rewrote a whole dataset's text.
    Six active steps are packages. So resolve a nested file to the nearest *ancestor* folder that names a
    step, and only fall back to the folder-wide credit when no ancestor does.

    Note this only repairs *this* tool's scope. Both blind spots sit in
    `get_all_changed_catalog_paths` itself, so chart-diff, datadiff and `etl run --modified` still miss a
    shared-metadata edit and a package step's own files; fixing it there is a separate change with a much
    wider blast radius.

    Still out of scope: a helper at the *namespace* level (`garden/democracy/shared.py`, the only one
    today) has no version folder to fall back to, and crediting every version of the namespace is a
    broader call than either case above needs. It carries no metadata text — there is no `.meta.yml`
    above a version folder anywhere in the tree.
    """
    from etl.dag_helpers import load_dag

    dag_steps = {s.split("://", 1)[1] for s in load_dag() if s.startswith(("data://", "data-private://"))}
    out: set[str] = set()
    for file_path in files_changed:
        path = Path(file_path)
        if path.suffix not in (".py", ".yml", ".yaml"):
            continue
        try:
            rel = (BASE_DIR / path).relative_to(STEP_DIR / "data")
        except ValueError:
            continue
        # `shared.meta.yml` -> `shared`; the same stripping `get_all_changed_catalog_paths` does.
        own = (rel.parent / rel.name.split(".")[0]).as_posix()
        if own in dag_steps:
            continue
        folders = [rel.parent, *rel.parent.parents]
        # A step implemented as a *package* keeps its files inside the step folder, so its step is an
        # ancestor of the file rather than a sibling: `.../vdem/vdem.meta.yml` is the step `.../vdem`.
        # The nearest such ancestor wins, so a package file credits its own step and not the whole
        # version folder (`who/latest/monkeypox` shares its folder with three unrelated steps).
        step_ancestor = next((f.as_posix() for f in folders if f.as_posix() in dag_steps), None)
        if step_ancestor is not None:
            out.add(step_ancestor)
            continue
        # Otherwise credit every step in the `channel/namespace/version` folder the file sits under. A
        # helper nested in its own sub-package (`demography/2024-01-25/utils/`) serves the same siblings
        # a flat `shared.py` would.
        version_folder = next((f.as_posix() for f in folders if f.as_posix().count("/") == 2), None)
        if version_folder is None:
            continue
        out |= {s for s in dag_steps if s.startswith(f"{version_folder}/")}
    return out


def _active_export_uris() -> set[str]:
    """The `export://` steps the DAG actually builds."""
    from etl.dag_helpers import load_dag

    return {s for s in load_dag() if s.startswith("export://")}


def _shared_export_recipe_uris(files_changed: dict[str, Any]) -> set[str]:
    """`export://` URIs of the recipes a changed *shared* file in an export folder feeds.

    The export mirror of `_shared_step_file_datasets`, and the same blind spot: a recipe's helpers are not
    recipes. `explorers/un/latest/un_wpp.py` imports its siblings `utils.py` and `view_edits.py`, and
    reads `map_brackets.yml`; none of the three is a step, so `get_directly_changed_export_uris` derives
    `export://explorers/un/latest/utils` — a recipe that exists in no DAG and publishes nothing. The
    explorer's own text then belongs to nobody in this branch, and the review files every difference in it
    as baseline lag: the one bucket where a reviewer will not look for their own edit.

    Config companions are already handled upstream (`un_wpp.sex_ratio.config.yml` resolves to the sibling
    `un_wpp.py`), so those name a real step here and expand to nothing extra.

    Only the siblings that actually *use* the helper get credited, unlike the data-step mirror. There,
    erring broad is free because `select_candidates` still has to see the dataset rebuilt on this server,
    and the master cross-check labels whatever the branch did not write. An export recipe has no such
    second gate: `covers_mdim` alone decides, and it makes `split_mdim_groups` hand over *every*
    config-level difference in that MDim. So crediting a folder wholesale would report an untouched
    sibling's baseline lag — a whole MDim's worth of it — as this branch's work.
    """
    dag_uris = _active_export_uris()
    out: set[str] = set()
    for file_path in files_changed:
        path = Path(file_path)
        if path.suffix not in (".py", ".yml", ".yaml"):
            continue
        try:
            rel = (BASE_DIR / path).relative_to(STEP_DIR / "export")
        except ValueError:
            continue
        own = f"export://{(rel.parent / rel.name.split('.')[0]).as_posix()}"
        folder = rel.parent.as_posix()
        # Only a `kind/namespace/version` folder has sibling recipes to credit.
        if own in dag_uris or folder.count("/") != 2:
            continue
        siblings = {u for u in dag_uris if u.startswith(f"export://{folder}/")}
        out |= _recipes_using(rel.name, siblings) or siblings
    return out


def _recipes_using(file_name: str, recipe_uris: set[str]) -> set[str]:
    """Which of `recipe_uris` reach the helper `file_name` — empty when none of them names it.

    A helper is reached by name whether it is imported or read: `un_wpp.py` does `from utils import
    MDIMCreator` and `paths.side_file("map_brackets.yml")`, so match an import of the module *and* a
    literal mention of the file. Its siblings `child_labor.py` and `hazardous_work.py` name neither, and
    stay out of scope.

    Empty means "cannot tell", not "nobody": a helper reached only through another helper names no recipe
    directly, and the caller then falls back to the whole folder rather than to nothing — the direction
    that keeps a reviewer's own edit visible.
    """
    stem = file_name.split(".")[0]
    imported = re.compile(rf"^\s*(?:from|import)\s+\.*{re.escape(stem)}\b", re.MULTILINE)
    mentioned = re.compile(rf"""["']{re.escape(file_name)}["']""")
    out: set[str] = set()
    for uri in recipe_uris:
        try:
            source = (STEP_DIR / "export" / f"{uri[len('export://') :]}.py").read_text()
        except OSError:
            # A recipe whose file we cannot read tells us nothing either way; leave it to the fallback.
            continue
        if imported.search(source) or mentioned.search(source):
            out.add(uri)
    return out


def _export_namespace(export_uri: str) -> str:
    """The namespace an export recipe lives in: `export://multidim/ihme_gbd/latest/x` -> `ihme_gbd`."""
    parts = export_uri[len("export://") :].lstrip("/").split("/")
    return parts[1] if len(parts) > 1 else ""


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
    # Only MDims this server actually serves. Grapher renders an MDim only when `published = 1`, and 38
    # of the 78 here are drafts — counting one as a reader-facing change inflates the section header and
    # the PR comment with work no reader can see, which the brief's own wording rules out. `charts_using_
    # indicators` and `explorer_view_rows` already filter their surfaces this way; MDims were the outlier.
    #
    # The *baseline* list stays unfiltered on purpose: an MDim this branch publishes is a draft there, and
    # filtering it out of the baseline would leave no row to join against, reporting a brand-new MDim
    # where the branch only changed its publication state.
    # Drafts are kept, and marked. They show nothing to readers, so they do not belong in the
    # reader-facing count — but they are also the text that goes live the moment `published` flips, and
    # dropping them from the query would hide them from the list and the "other differences" section too.
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
        own_recipe = df["catalogPath"].map(lambda cp: scope.covers_mdim(str(cp)))
        df["in_branch"] = mdim_in_branch(df, own_recipe)
    else:
        df["in_branch"] = df["has_changes"]
    df["scope_available"] = scope.available
    df["is_draft"] = df["published_source"] != 1
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


def mdim_namespace(catalog_path: str) -> str | None:
    """The namespace an MDim's catalogPath sits in (`grapher/wid/latest/incomes_wid#…` -> `wid`).

    None when the path is too short to carry one, so a bare short name still matches on name alone.
    """
    parts = _catalog_path_parts(catalog_path)
    return parts[0] if len(parts) >= 3 else None


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
    if not scope.available or scope.covers_mdim(catalog_path):
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
    n_draft_mdims: int = 0  # changed by this branch, but unpublished — no reader sees them yet
    n_explorers: int = 0
    n_explorer_views: int = 0
    n_other_mdims: int = 0  # differ from the baseline, but not attributable to this branch
    n_other_explorers: int = 0
    fields: dict[str, int] = field(default_factory=dict)  # field label -> distinct changes
    # Indicator changes by origin: ours / master / stale / unknown (see classify_origins).
    attribution: dict[str, int] = field(default_factory=dict)
    # Section -> (changes ticked off, changes in total). What the badges count down.
    review_progress: dict[str, tuple[int, int]] = field(default_factory=dict)
    # Datasets this server holds an older build of than the baseline: dataset -> (here, baseline).
    # Their differences read backwards, so this is a defect in the server, not in the branch.
    stale: dict[str, tuple[Any, Any]] = field(default_factory=dict)
    n_distinct_changes: int = 0  # distinct texts changed, counted once across all surfaces
    mdims_resolved: bool = True  # False when there were too many flagged MDims to diff view by view
    draft_mdims_resolved: bool = True  # False when there were too many draft MDims to diff view by view
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
        """Whether this branch changed any metadata text that nobody has reviewed yet.

        Not the same question as "does a reader see it". An unpublished MDim shows nothing to readers, so
        it stays out of the reader-facing counts — but a branch whose only change is a draft MDim has
        still changed text, and answering "No metadata text changes" there would hide the very edit the
        PR exists to make. Drafts are counted here and labelled where they are reported.

        Counted on the *changes*, never on their audience. `n_charts` is filtered to charts a reader can
        actually see the change on, which is the right number to report as reach but the wrong one to
        exist by: a WYSK edit whose indicator only feeds multi-indicator charts reaches nobody, so
        `n_charts` is 0 while the Charts section still renders a real, reviewable change. Keying off it
        put a green all-clear and a "No metadata text changes" comment over exactly that.

        New indicators count too. A version bump replaces every catalog path, so nothing has a baseline
        counterpart to diff against — and reporting that as "no metadata text changes" would wave
        through a whole dataset's worth of new text.
        """
        return bool(
            self.n_chart_changes
            or self.n_indicators
            or self.n_charts
            or self.n_mdims
            or self.n_explorers
            or self.n_new_indicators
            or self.n_draft_mdims
        )


def charts_reached(groups: list[ChangeGroup], usage: dict[int, list[dict[str, Any]]]) -> set[int]:
    """Chart ids these changes reach.

    Every published chart using the indicator: its readers can see the new text either on the chart's data
    page or through "Learn more about this data". Which of the two is a matter of prominence, reported in
    the lists rather than deducted from the count.
    """
    reached: set[int] = set()
    for g in groups:
        for iid in g.indicator_ids or ({g.indicator_id} if g.indicator_id is not None else set()):
            reached.update(int(c["chartId"]) for c in usage.get(iid, []))
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
        summary.review_progress["charts"] = (
            count_reviewed(source_engine, surface_key("charts", "indicators"), chart_groups),
            len(chart_groups),
        )
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
        reader_facing = df_mdims["in_branch"] & ~df_mdims["is_draft"]
        flagged = [str(cp) for cp in df_mdims.index[reader_facing]]
        drafts = [str(cp) for cp in df_mdims.index[df_mdims["in_branch"] & df_mdims["is_draft"]]]
        summary.n_mdims_flagged = len(flagged)
        # Baseline lag only. Subtracting `flagged` alone left this branch's own drafts in here, so the same
        # MDim was reported twice — once as "Unpublished MDims changed", once as a difference the branch is
        # not responsible for. This is the set the MDims list already shows under "other differences".
        summary.n_other_mdims = int((df_mdims["has_changes"] & ~df_mdims["in_branch"]).sum())
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
                    done, total = summary.review_progress.get("mdims", (0, 0))
                    summary.review_progress["mdims"] = (
                        done + count_reviewed(source_engine, surface_key("mdim", cp), ours),
                        total + len(ours),
                    )
        # Drafts are counted only where they actually have changed text, same test as the rest.
        if len(drafts) > MAX_MDIMS_RESOLVED:
            # Too many to diff view by view. Report the flag count as a ceiling and say so, rather than
            # resolve the first 25 and present the truncated number as exact — that is the silent
            # under-report this tool exists to catch. The flag is the drafts' own: an overflow here says
            # nothing about whether the reader-facing count above could be resolved.
            summary.draft_mdims_resolved = False
            summary.n_draft_mdims = len(drafts)
        else:
            scope_for_drafts = branch_scope()
            for cp in drafts:
                view_diffs = [v for v in mdim_text_changes(source_engine, target_engine, cp) if v.changed]
                if split_mdim_groups(cp, view_diffs, scope_for_drafts)[0]:
                    summary.n_draft_mdims += 1
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
        for slug, diffs in branch_views.items():
            explorer_groups = group_changes(diffs)
            _collect_changes(seen, explorer_groups)
            done, total = summary.review_progress.get("explorers", (0, 0))
            summary.review_progress["explorers"] = (
                done + count_reviewed(source_engine, surface_key("explorer", slug), explorer_groups),
                total + len(explorer_groups),
            )
    except Exception as e:  # noqa: BLE001
        log.warning("metadata_diff.explorer_discovery_failed", error=str(e))
        summary.warnings.append(f"Explorer discovery failed: {e}")

    for field_name, _ in seen:
        label = field_label(field_name)
        summary.fields[label] = summary.fields.get(label, 0) + 1
    summary.n_distinct_changes = len(seen)

    return summary
