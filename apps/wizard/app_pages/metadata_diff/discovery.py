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

import hashlib
import json
import re
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import git
import pandas as pd
import yaml
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELDS,
    COUNTED_SECTIONS,
    ChangeGroup,
    ViewDiff,
    build_view_bundle,
    dataset_shape,
    diff_views,
    edit_fingerprint,
    field_label,
    group_changes,
    indicator_identity,
    mark_identity,
    surface_key,
)
from apps.wizard.app_pages.metadata_diff.data import (
    _chunked,
    _load_configs,
    build_env_bundles,
    fetch_indicator_config_texts,
    fetch_latest_dataset_versions,
    fetch_variable_paths,
    fetch_variable_rows_by_path,
    load_mdim_config,
)
from apps.wizard.app_pages.metadata_diff.usage import _indicator_ids_in_mdim_config, charts_using_indicators
from etl.db import read_sql
from etl.git_helpers import get_changed_files
from etl.io import get_all_changed_catalog_paths, get_directly_changed_export_uris
from etl.paths import BASE_DIR, STEP_DIR

T = TypeVar("T")

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
    new_paths: set[str] = field(default_factory=set)
    # source path -> the baseline path it was compared with, when the two differ only by version. A
    # dataset update lands here, and it is worth saying: the diff below crossed a re-version, so a
    # difference may be an edit or may be how the new release words things.
    across_versions: dict[str, str] = field(default_factory=dict)  # on staging, absent from the baseline
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


def _both(fetch: Callable[..., T], first: Engine, second: Engine, *args: Any) -> tuple[T, T]:
    """Run one fetch against two environments concurrently, returning (first, second).

    Every comparison in this module reads the same thing from the staging server and from the baseline.
    The two are independent round trips, so running them in sequence is latency and nothing else; the
    engines' pool is 30 connections wide, and results come back in argument order, never completion order.
    """
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending_first = pool.submit(fetch, first, *args)
        pending_second = pool.submit(fetch, second, *args)
        return pending_first.result(), pending_second.result()


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
    if step_file.exists():
        names |= _emitted_collection_names(step_file.read_text())
    elif not step_file.with_suffix(".config.yml").exists():
        # A deleted or renamed recipe: the file name is all we have, and is still worth matching on.
        return names
    # A recipe publishing *several* collections names them from its own config files instead of from
    # literals — `multidim/covid/latest/covid.py` turns `covid.cases.yml` into `covid_cases` — so the
    # literals above find none of them. The `<recipe>.<key>[.config].yml` companion convention
    # reconstructs those names, and a name no collection answers to simply never matches anything.
    # A YAML-only step — no `.py`, just `<recipe>.config.yml`, the way ETL-authored single charts are
    # written — has no literals to read either, and names itself here.
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
    source_engine: Engine,
    paths: list[str],
    scope: BranchScope | None = None,
    built: set[str] | None = None,
) -> tuple[list[str], bool]:
    """`select_candidates`, reading the scope from git and the rebuilt-here set from the server.

    Both are per-run facts, not per-call ones: `scope` shells out to git and `built` reads every dataset
    row. Callers that make several selections in one pass should read them once and pass them in.
    """
    scope = scope if scope is not None else branch_scope()
    built = built if built is not None else datasets_built_here(source_engine)
    return select_candidates(paths, scope, built)


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
    other_versions: dict[str, dict[str, Any]] | None = None,
) -> IndicatorChanges:
    """Pure comparison: which indicators' texts differ, as one ViewDiff each.

    Producing ViewDiffs (an indicator is a "view" with no dimensions) means `group_changes`, the diff
    renderers and the PR brief all work on chart-side changes without a second implementation.

    `other_versions` carries the baseline's rows for *any* version of the same datasets, and is what makes
    a dataset update reviewable. A bump moves `grapher/wb/2026-06-26/…#mean` to
    `grapher/wb/2026-09-02/…#mean`, so the exact-path lookup finds nothing and every indicator of the
    dataset reads as new — on the one workflow this tool exists for. Falling back to the same indicator in
    whatever version the baseline serves compares the texts that actually face readers before and after.

    The version is dropped from the key and nothing else is (`indicator_identity`): short names repeat
    across datasets, so matching on `#short_name` alone would compare one source's `gini` with another's.
    Where the baseline holds several versions the newest is taken — the one it is most likely serving —
    and the pairing is recorded in `across_versions` so the UI can say the diff crossed a bump rather than
    implying the text moved on its own.
    """
    out = IndicatorChanges()
    by_identity: dict[tuple[str, ...], list[str]] = {}
    for path in other_versions or {}:
        by_identity.setdefault(indicator_identity(path), []).append(path)

    for path, src_row in source_rows.items():
        if src_row.get("id") is not None:
            out.ids[path] = int(src_row["id"])
        tgt_row = target_rows.get(path)
        tgt_path = path
        if tgt_row is None:
            candidates = sorted(by_identity.get(indicator_identity(path), []))
            if not candidates:
                out.new_paths.add(path)
                continue
            # Newest last: versions are dates, so lexicographic order is chronological.
            tgt_path = candidates[-1]
            tgt_row = (other_versions or {})[tgt_path]
            out.across_versions[path] = tgt_path
        src = build_view_bundle(view={"dimensions": {}}, config_metadata=None, variable_row=src_row, chart_config=None)
        tgt = build_view_bundle(view={"dimensions": {}}, config_metadata=None, variable_row=tgt_row, chart_config=None)
        diff = diff_views([src], [tgt])[0]
        if diff.changed:
            diff.catalog_path = path
            out.diffs[path] = diff
        elif path in out.across_versions:
            # Same text either side of the bump: nothing to review, and not a change. The pairing is
            # dropped so the count of cross-version diffs matches what is listed.
            out.across_versions.pop(path, None)
    return out


def fetch_baseline_counterparts(target_engine: Engine, unmatched: list[str]) -> dict[str, dict[str, Any]]:
    """The baseline's rows for these indicators under whatever version it holds them.

    Two cheap steps rather than one broad one: ask which version the baseline has of each dataset, then
    name the exact paths wanted by swapping that version into each unmatched path. Fetching every version
    of a dataset to find the pairing meant 73,000 rows for two datasets, and a bump of something
    WDI-sized would be far worse.

    A table renamed between versions is not matched by this and stays reported as new — the constructed
    path names the source's table. Worth knowing rather than worth guessing at: a wrong pairing would
    diff two different indicators and call the result an edit.
    """
    shapes = [shape for shape in (dataset_shape(path) for path in unmatched) if shape]
    versions = fetch_latest_dataset_versions(target_engine, shapes)
    if not versions:
        return {}

    # source path -> the path the baseline would use for the same indicator
    expected: dict[str, str] = {}
    for path in unmatched:
        shape = dataset_shape(path)
        version = versions.get(shape) if shape else None
        if not version:
            continue
        left, sep, short = path.partition("#")
        parts = left.strip("/").split("/")
        if len(parts) < 4:
            continue
        parts[2] = version
        candidate = "/".join(parts) + sep + short
        if candidate != path:
            expected[path] = candidate

    rows = fetch_variable_rows_by_path(target_engine, sorted(set(expected.values())))
    # Keyed by the baseline's own path, which is what `compare_indicator_texts` matches on by identity.
    return {path: row for path, row in rows.items()}


def changed_indicators(
    source_engine: Engine,
    target_engine: Engine,
    catalog_paths: list[str] | None = None,
    scope: BranchScope | None = None,
) -> IndicatorChanges:
    """Indicators whose text this branch changed. `catalog_paths` defaults to charted indicators."""
    paths = charted_indicator_paths(source_engine) if catalog_paths is None else list(catalog_paths)
    scope = scope if scope is not None else branch_scope()
    paths, narrowed = narrow_to_branch(paths, sorted(scope.dataset_paths) if scope.available else None)
    if not paths:
        return IndicatorChanges(narrowed=narrowed)

    source_rows, target_rows = _both(fetch_variable_rows_by_path, source_engine, target_engine, paths)
    # Only when the exact-path pass leaves something unmatched, which is the version-bump case. On a
    # branch that edits metadata in place this costs nothing.
    unmatched = [p for p in source_rows if p not in target_rows]
    other_versions: dict[str, dict[str, Any]] = {}
    if unmatched:
        try:
            other_versions = fetch_baseline_counterparts(target_engine, unmatched)
        except Exception as e:  # noqa: BLE001 — without it those indicators read as new, as before
            log.warning("metadata_diff.other_versions_unavailable", error=str(e))
    out = compare_indicator_texts(source_rows, target_rows, other_versions)
    out.narrowed = narrowed
    return out


@dataclass
class ChartTextChanges:
    """Published charts whose own config text differs from the baseline."""

    # slug -> the diff of its chart-level fields (title / subtitle / note).
    diffs: dict[str, ViewDiff] = field(default_factory=dict)
    # slug -> {chartId, slug}, for the reach lists and the links.
    charts: dict[str, dict[str, Any]] = field(default_factory=dict)
    # False when the branch's scope could not be read, so the caller can say the list is unfiltered.
    narrowed: bool = True

    def view_diffs(self) -> list[ViewDiff]:
        return [self.diffs[slug] for slug in sorted(self.diffs)]


def catalog_path_like_patterns(dataset_path: str) -> list[str]:
    """SQL LIKE patterns selecting every indicator *of* this dataset, and nothing past its name.

    `<path>%` also matches a sibling dataset whose name merely starts with this one — and those exist in
    live paths: `climate/2026-08-21/surface_temperature` selected `surface_temperature_anomalies` too, so
    a difference of the sibling's (which may simply lag the baseline) was attributed to this branch. An
    indicator's catalogPath continues with `/<table>#<short_name>`, so the separator is the boundary.
    """
    return [f"{dataset_path}/%", f"{dataset_path}#%"]


def chart_text_rows(engine: Engine, dataset_paths: list[str]) -> dict[str, dict[str, Any]]:
    """Resolved title/subtitle/note of every published chart rendering one of these datasets, by slug.

    The *resolved* config, so a value the chart inherits from its indicator's `grapher_config` is already
    baked in — which is the whole point: that is where a garden-authored subtitle ends up, and it is not
    in the `variables` row at all.
    """
    if not dataset_paths:
        return {}
    cfg = "config"
    clauses, params = [], {}
    for i, path in enumerate(sorted(set(dataset_paths))):
        alternatives = []
        for j, pattern in enumerate(catalog_path_like_patterns(path)):
            params[f"p{i}_{j}"] = pattern
            alternatives.append(f"v.catalogPath like %(p{i}_{j})s")
        clauses.append("(" + " or ".join(alternatives) + ")")
    # One row per (chart, matching indicator), collapsed in Python. It used to group in SQL and take
    # `min(v.catalogPath)` as the indicator that authored the text, which is alphabetical order dressed up
    # as attribution: a chart rendering both a `demography/…/population` series and a `wb/…` one had its
    # subtitle credited to population, and the note then told a reviewer they had edited a dataset they
    # had not touched. Which indicator actually carries the text is decided by
    # `changed_indicator_configs`, on these candidates.
    df = read_sql(
        f"""
        select c.id as chartId,
               cc.slug as slug,
               cc.{cfg} ->> '$.title' as title,
               cc.{cfg} ->> '$.subtitle' as subtitle,
               cc.{cfg} ->> '$.note' as note,
               v.catalogPath as catalogPath
        from charts c
        join chart_configs cc on cc.id = c.configId
        join chart_dimensions cd on cd.chartId = c.id
        join variables v on v.id = cd.variableId
        where c.publishedAt is not null
          and cc.slug is not null
          and ({" or ".join(clauses)})
        """,
        engine=engine,
        params=params,
    )
    rows: dict[str, dict[str, Any]] = {}
    for record in df.to_dict("records"):
        slug = str(record["slug"])
        entry = rows.setdefault(
            slug,
            {
                "chartId": record["chartId"],
                "slug": slug,
                "title": record["title"],
                "subtitle": record["subtitle"],
                "note": record["note"],
                "paths": [],
            },
        )
        path = record.get("catalogPath")
        if path and str(path) not in entry["paths"]:
            entry["paths"].append(str(path))
    return rows


def chart_text_rows_by_slug(engine: Engine, slugs: list[str]) -> dict[str, dict[str, Any]]:
    """The same three fields for named slugs, for the other side of the comparison.

    By slug, not by dataset: the baseline may not carry the branch's datasets at all (a new step), and the
    question here is only what the chart said before.
    """
    if not slugs:
        return {}
    cfg = "config"
    rows: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(sorted(set(slugs))):
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"""
            select cc.slug as slug,
                   cc.{cfg} ->> '$.title' as title,
                   cc.{cfg} ->> '$.subtitle' as subtitle,
                   cc.{cfg} ->> '$.note' as note
            from charts c
            join chart_configs cc on cc.id = c.configId
            where c.publishedAt is not null and cc.slug in ({placeholders})
            """,
            engine=engine,
            params=tuple(chunk),
        )
        for record in df.to_dict("records"):
            rows[str(record["slug"])] = {str(k): v for k, v in record.items()}
    return rows


def compare_chart_texts(
    source_rows: dict[str, dict[str, Any]],
    target_rows: dict[str, dict[str, Any]],
) -> ChartTextChanges:
    """Pure comparison of chart-level text, one ViewDiff per chart.

    Each chart is treated as a view whose only dimension is its slug, so `group_changes` collapses charts
    that say the same thing into one change — which is what a shared `definitions.*` edit produces — and
    the existing renderers, brief and reach model need no chart-specific branch.
    """
    out = ChartTextChanges()
    for slug, src_row in source_rows.items():
        target_row = target_rows.get(slug)
        if target_row is None:
            # A chart the baseline does not publish is not a text change; it is a new chart, and
            # chart-diff is where a new chart belongs.
            continue
        src = build_view_bundle(
            view={"dimensions": {"chart": slug}},
            config_metadata=None,
            # Only for its catalogPath: the text being compared is the chart's own config, not the
            # indicator's metadata, but the indicator is what says which garden dataset authored it.
            variable_row={"catalogPath": src_row.get("catalogPath")} if src_row.get("catalogPath") else None,
            chart_config={key: src_row.get(key) for key in CHART_FIELDS},
        )
        tgt = build_view_bundle(
            view={"dimensions": {"chart": slug}},
            config_metadata=None,
            variable_row=None,
            chart_config={key: target_row.get(key) for key in CHART_FIELDS},
        )
        diff = diff_views([src], [tgt])[0]
        if diff.changed:
            out.diffs[slug] = diff
            out.charts[slug] = {
                "chartId": int(src_row["chartId"]) if src_row.get("chartId") is not None else None,
                "slug": slug,
                "has_data_page": True,  # a chart's own text is on its canvas, never behind the drawer
                "is_published": True,
            }
    return out


def changed_chart_texts(
    source_engine: Engine,
    target_engine: Engine,
    scope: BranchScope | None = None,
    built: set[str] | None = None,
) -> ChartTextChanges:
    """Charts whose own config text this branch changed.

    Restricted the same way indicator changes are: the chart must render a dataset in the branch's git
    scope that this server rebuilt. Without both, a chart master rebuilt would read as this branch's work.
    """
    scope = scope if scope is not None else branch_scope()
    built = built if built is not None else datasets_built_here(source_engine)
    if not scope.available:
        return ChartTextChanges(narrowed=False)

    # In the channel the *variables* use. The scope holds whichever channel the changed file sits in, and
    # a garden pattern matches no variable catalogPath at all: an edit to a `shared.meta.yml` resolves to
    # its folder's garden steps only (they are the steps that own the file), so the charts a shared
    # `grapher_config` edit reaches were invisible here — the one comparison that can see that edit.
    in_play = sorted({f"grapher/{_dataset_of(p)}" for p in scope.dataset_paths if _dataset_of(p) in built})
    if not in_play:
        return ChartTextChanges()

    source_rows = chart_text_rows(source_engine, in_play)
    target_rows = chart_text_rows_by_slug(target_engine, list(source_rows))
    out = compare_chart_texts(source_rows, target_rows)

    # Which indicator authored the text, asked rather than assumed: the one whose own
    # `presentation.grapher_config` differs from the baseline. A chart with none — its text inherited from
    # elsewhere, or typed in the admin — is attributed to nothing, which reads as "we do not know" rather
    # than as a dataset somebody did not edit.
    candidates = sorted({path for slug in out.diffs for path in (source_rows.get(slug, {}).get("paths") or [])})
    authored = changed_indicator_configs(source_engine, target_engine, candidates) if candidates else set()
    attribute_chart_texts(out, {slug: row.get("paths") or [] for slug, row in source_rows.items()}, authored)
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
                *_both(fetch_variable_rows_by_path, source_engine, master_engine, catalog_paths)
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
    """Every MDim with its config hash, publication state, slug and title (most recently updated first).

    Sorted in pandas, not in SQL. Touching `config` at all — even to extract the title scalar with `->>` —
    puts the row's JSON in MySQL's sort set, and `order by updatedAt desc` over that overruns the sort
    buffer outright ("Out of sort memory"). Measured, not assumed: the earlier version of this query with
    both the extraction and the ORDER BY failed against a real server.
    """
    df = read_sql(
        """
        select catalogPath, configMd5, published, slug, updatedAt,
               coalesce(config ->> '$.title.title', config ->> '$.title') as title
        from multi_dim_data_pages
        where catalogPath is not null
        """,
        engine=engine,
    )
    return df.sort_values("updatedAt", ascending=False).drop(columns=["updatedAt"])


def mdim_indicator_paths(source_engine: Engine, configs: dict[str, dict[str, Any]]) -> dict[str, set[str]]:
    """catalogPath of each MDim -> catalog paths of the indicators its views reference."""
    ids_by_mdim = {cp: _indicator_ids_in_mdim_config(cfg) for cp, cfg in configs.items()}
    all_ids = sorted({i for ids in ids_by_mdim.values() for i in ids})
    if not all_ids:
        return {cp: set() for cp in configs}

    path_by_id = fetch_variable_paths(source_engine, all_ids)
    return {cp: {path_by_id[i] for i in ids if i in path_by_id} for cp, ids in ids_by_mdim.items()}


def mdim_changes_df(
    source_engine: Engine,
    target_engine: Engine,
    scope: BranchScope | None = None,
    built: set[str] | None = None,
) -> pd.DataFrame:
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
    df_source, df_target = _both(mdim_list, source_engine, target_engine)

    df = pd.merge(df_source, df_target, on="catalogPath", suffixes=("_source", "_target"), how="left")
    df["is_new"] = df["configMd5_target"].isnull()
    df["config_changed"] = df["configMd5_source"] != df["configMd5_target"]

    changed_mdims: set[str] = set()
    try:
        paths_by_mdim = mdim_indicator_paths(source_engine, _load_configs(source_engine))
        all_paths = sorted({p for paths in paths_by_mdim.values() for p in paths})
        narrowed_paths, _ = candidate_paths(source_engine, all_paths, scope, built)
        changed_paths: set[str] = set()
        if narrowed_paths:
            result = compare_indicator_texts(
                *_both(fetch_variable_rows_by_path, source_engine, target_engine, narrowed_paths)
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

    scope = scope if scope is not None else branch_scope()
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
    source_config, target_config = _both(load_mdim_config, source_engine, target_engine, catalog_path)
    if source_config is None:
        return []
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending_source = pool.submit(build_env_bundles, source_engine, source_config)
        pending_target = pool.submit(build_env_bundles, target_engine, target_config) if target_config else None
        source_bundles = pending_source.result()
        target_bundles = pending_target.result() if pending_target is not None else []
    return diff_views(source_bundles, target_bundles)


# --- Explorers ----------------------------------------------------------------------------------


def explorer_view_hashes(engine: Engine) -> dict[tuple[str, str], str]:
    """Every published explorer view's resolved-config hash, keyed by (explorer slug, viewId).

    The cheap half of the comparison: no JSON extraction, one small column per row. Two views whose
    hashes match cannot differ in their text, so this is what decides whose text is worth reading.
    """
    df = read_sql(
        """
        select ev.explorerSlug as explorerSlug, ev.viewId as viewId, cc.configMd5 as configMd5
        from explorer_views ev
        join explorers e on e.slug = ev.explorerSlug
        join chart_configs cc on cc.id = ev.chartConfigId
        where e.isPublished = 1
        """,
        engine=engine,
    )
    return {(str(r["explorerSlug"]), str(r["viewId"])): str(r["configMd5"]) for r in df.to_dict("records")}


def explorer_view_rows(
    engine: Engine, keys: list[tuple[str, str]] | None = None
) -> dict[tuple[str, str], dict[str, Any]]:
    """Resolved text of every view of every published explorer, keyed by (explorer slug, viewId).

    `chart_configs.full` is the *resolved* config, so indicator-driven inheritance (`title_public` ->
    title, `description_short` -> subtitle) is already baked in — no metadata resolution needed here.
    `keys` restricts the read to particular views — which is how this is called: reading the text of all
    nine thousand published views to find the handful that changed took 3.75s of the page's cold load,
    where a hash join over `configMd5` names the candidates for a fraction of that. Passing None reads
    everything, which is what a caller comparing from scratch wants.

    Caveats the UI states rather than hides: a legacy CSV-backed explorer has no `explorer_views` rows,
    and the resolved config only refreshes when the explorer's export step re-runs.
    """
    if keys is not None and not keys:
        return {}
    select = """
        select ev.explorerSlug as explorerSlug,
               ev.viewId as viewId,
               ev.dimensions as dimensions,
               cc.config ->> '$.dimensions' as indicators,
               cc.config ->> '$.title' as title,
               cc.config ->> '$.subtitle' as subtitle,
               cc.config ->> '$.note' as note
        from explorer_views ev
        join explorers e on e.slug = ev.explorerSlug
        join chart_configs cc on cc.id = ev.chartConfigId
        where e.isPublished = 1
        """
    if keys is None:
        df = read_sql(select, engine=engine)
    else:
        # Chunked: the candidate set is normally tiny, but a branch that rebuilds an explorer makes every
        # one of its views a candidate, and a single IN list of thousands of pairs is its own problem.
        frames = []
        for start in range(0, len(keys), 500):
            chunk = keys[start : start + 500]
            placeholders = ", ".join(["(%s, %s)"] * len(chunk))
            frames.append(
                read_sql(
                    f"{select} and (ev.explorerSlug, ev.viewId) in ({placeholders})",
                    engine=engine,
                    params=tuple(value for key in chunk for value in key),
                )
            )
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        return {}
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


def attribute_chart_texts(changes: "ChartTextChanges", paths_by_slug: dict[str, list[str]], authored: set[str]) -> None:
    """Credit each changed chart's text to an indicator that actually carries the change.

    Pure, and separate from the queries, because getting it wrong is invisible: the old code took the
    alphabetically first of a chart's indicators, which named the wrong garden dataset without ever
    looking wrong. `authored` is the set whose own ETL config text differs from the baseline; a chart with
    none of those keeps no attribution at all.
    """
    for slug, diff in changes.diffs.items():
        mine = [path for path in paths_by_slug.get(slug, []) if path in authored]
        diff.catalog_path = mine[0] if mine else None


def changed_indicator_configs(source_engine: Engine, target_engine: Engine, paths: list[str]) -> set[str]:
    """Indicators whose garden-authored chart text (`presentation.grapher_config`) differs from the baseline.

    An indicator absent from the baseline is not reported here: it has no old text to differ from, and the
    new-indicator case is already carried by `compare_indicator_texts`.
    """
    src, tgt = _both(fetch_indicator_config_texts, source_engine, target_engine, paths)
    changed = set()
    for path, fields in src.items():
        before = tgt.get(path)
        if before is None:
            continue
        if any((fields.get(key) or "") != (before.get(key) or "") for key in ("title", "subtitle", "note")):
            changed.add(path)
    return changed


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


def changed_explorer_views(
    source_engine: Engine,
    target_engine: Engine,
    scope: BranchScope | None = None,
    built: set[str] | None = None,
) -> ExplorerChanges:
    """Published explorers whose view text differs from the baseline, attributed to branch or lag."""
    # Hashes first, text second. Views whose resolved config hash matches cannot differ in their text,
    # so only the rest are worth reading — the difference between four JSON extractions on nine thousand
    # rows and on a handful.
    source_hashes, target_hashes = _both(explorer_view_hashes, source_engine, target_engine)
    candidates = sorted(
        key for key in set(source_hashes) | set(target_hashes) if source_hashes.get(key) != target_hashes.get(key)
    )
    source_rows, target_rows = _both(explorer_view_rows, source_engine, target_engine, candidates)
    detailed = compare_explorer_views_detailed(source_rows, target_rows)

    views: dict[str, list[ViewDiff]] = {}
    for (slug, _), diff in detailed.items():
        views.setdefault(slug, []).append(diff)

    scope = scope if scope is not None else branch_scope()
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
    paths_by_id = fetch_variable_paths(source_engine, all_ids)
    # Only indicators this branch actually rebuilt here can carry one of its edits — compare just those.
    candidates, _ = candidate_paths(source_engine, sorted(set(paths_by_id.values())), scope, built)
    candidates = sorted(set(candidates))
    changed_paths: set[str] = set()
    if candidates:
        result = compare_indicator_texts(*_both(fetch_variable_rows_by_path, source_engine, target_engine, candidates))
        changed_paths = set(result.diffs) | result.new_paths
        # An explorer view's text is a chart config, and a garden step can author that text directly
        # through `presentation.grapher_config`. That edit changes no column of the `variables` row, so
        # asking only `compare_indicator_texts` credited the branch with none of it: a reworded shared
        # subtitle moved 402 LIS explorer views and every one was filed as master's lag.
        changed_paths |= changed_indicator_configs(source_engine, target_engine, candidates)

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
    # Charts whose own config text changed — a garden `presentation.grapher_config` edit lands there and
    # never touches the `variables` row, so it needs its own comparison and its own count.
    n_chart_text_changes: int = 0
    n_charts_own_text: int = 0
    # Per changed text, every surface it lands on. Powers the Blast radius section; `mdims_resolved`
    # False means its MDim rows are incomplete, for the same reason the MDim count is a ceiling.
    reach: list["ChangeReach"] = field(default_factory=list)
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
    # Section -> the (surface, change key, content hash) of every change in it. The badges count these,
    # and read how many are ticked separately: identifying them is slow and cacheable, whereas whether one
    # is ticked changes the moment a reviewer presses a toggle.
    review_keys: dict[str, list[tuple[str, str, str]]] = field(default_factory=dict)
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
    def mdim_counts_are_ceilings(self) -> bool:
        """Either MDim count went unresolved, so the section holds cards it has no review keys for.

        Two independent overflows — too many published MDims to diff view by view, or too many
        unpublished ones — and either leaves the MDims section with a zero review total while its cards
        are still rendered. Greying the section on that zero shuts the only door to them.
        """
        return not self.mdims_resolved or not self.draft_mdims_resolved

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
            or self.n_chart_text_changes
            or self.n_charts
            or self.n_mdims
            or self.n_explorers
            or self.n_new_indicators
            or self.n_draft_mdims
        )


def charts_reached(groups: list[ChangeGroup], usage: dict[int, list[dict[str, Any]]]) -> set[int]:
    """Chart ids these changes reach — published only.

    Every published chart using the indicator: its readers can see the new text either on the chart's data
    page or through "Learn more about this data". Which of the two is a matter of prominence, reported in
    the lists rather than deducted from the count.

    Drafts arrive in the same list from `charts_using_indicators` (they are listed, per surface, so their
    author can see the edit landed there) and are dropped here: nobody can open one, and this number is
    the reach reported in the PR comment.
    """
    reached: set[int] = set()
    for g in groups:
        for iid in g.indicator_ids or ({g.indicator_id} if g.indicator_id is not None else set()):
            reached.update(int(c["chartId"]) for c in usage.get(iid, []) if c.get("is_published", True))
    return reached


@dataclass
class ChangeReach:
    """One changed text and every surface it lands on — the blast radius of a single edit.

    The unit is the *text*, not the sighting: a reworded shared definition renders on its indicator's
    charts, on every MDim view using it and on every explorer view, and those are one edit to judge with
    several audiences, not several edits. `change_identity` is what merges them, so the same text edited
    in two garden datasets still collapses here.
    """

    field: str
    old: Any = None
    new: Any = None
    charts: list[dict[str, Any]] = field(default_factory=list)  # published, with `has_data_page`
    draft_charts: list[dict[str, Any]] = field(default_factory=list)
    mdims: list[dict[str, Any]] = field(default_factory=list)  # {catalogPath, n_views, is_draft}
    explorers: list[dict[str, Any]] = field(default_factory=list)  # {slug, n_views}
    # The indicators carrying this text, so anything reporting the edit can name the garden dataset it was
    # authored in. A shared definition edited in two datasets collapses into one row here, and both paths
    # are kept: naming one of them would send somebody to fix half the change.
    catalog_paths: set[str] = field(default_factory=set)

    @property
    def n_reader_facing(self) -> int:
        """Places a reader can actually reach this text: published charts and published views."""
        return (
            len(self.charts)
            + sum(int(m["n_views"]) for m in self.mdims if not m["is_draft"])
            + sum(int(e["n_views"]) for e in self.explorers)
        )

    @property
    def n_hidden(self) -> int:
        """Places carrying it that no reader can open — draft charts and unpublished MDim views."""
        return len(self.draft_charts) + sum(int(m["n_views"]) for m in self.mdims if m["is_draft"])


@dataclass
class EditGroup:
    """One authored edit, and the rendered texts it produced.

    The distinction the counts turned on: `changes` is what the site renders (one per distinct text),
    `self` is what somebody wrote. A shared definition makes those numbers differ by an order of
    magnitude, and reporting the larger one as "changes to review" overstates the work by that much.
    """

    field: str
    inserted: str
    deleted: str
    changes: list[ChangeReach] = field(default_factory=list)

    @property
    def n_texts(self) -> int:
        return len(self.changes)

    def surfaces(self) -> dict[str, set[str]]:
        """Distinct places this edit lands, by kind — deduped, since texts share pages.

        MDim and explorer views are counted as their page: view counts belong to a text, not to the
        edit, because two texts of one edit can land on overlapping views of the same MDim.
        """
        out: dict[str, set[str]] = {"charts": set(), "draft_charts": set(), "mdims": set(), "explorers": set()}
        for c in self.changes:
            out["charts"].update(str(x["chartId"]) for x in c.charts)
            out["draft_charts"].update(str(x["chartId"]) for x in c.draft_charts)
            out["mdims"].update(str(m["catalogPath"]) for m in c.mdims if not m["is_draft"])
            out["explorers"].update(str(e["slug"]) for e in c.explorers)
        return out

    @property
    def n_reader_facing(self) -> int:
        """Pages a reader can reach — each counted once, however many of these texts it renders."""
        s = self.surfaces()
        return len(s["charts"]) + len(s["mdims"]) + len(s["explorers"])


def group_by_edit(reach: list[ChangeReach]) -> list[EditGroup]:
    """Collapse rendered texts into the edits that produced them, widest reach first."""
    groups: dict[tuple[str, str, str], EditGroup] = {}
    for r in reach:
        inserted, deleted = edit_fingerprint(r.old, r.new)
        key = (r.field, inserted, deleted)
        group = groups.get(key)
        if group is None:
            group = EditGroup(field=r.field, inserted=inserted, deleted=deleted)
            groups[key] = group
        group.changes.append(r)
    return sorted(groups.values(), key=lambda g: (-g.n_reader_facing, -g.n_texts, g.field))


def edits_for(summary: "Summary", section: str) -> list[EditGroup]:
    """The authored edits that land on one section's surface, widest first.

    Scoped before grouping, not after: an edit reaching both a chart and an MDim is one card in each
    section, and each card describes only the pages on its own surface — so a section never counts the
    other's pages, and a reviewer working through one surface never meets the other's.
    """

    def lands(r: ChangeReach) -> bool:
        if section == "mdims":
            return bool(r.mdims)
        if section == "explorers":
            return bool(r.explorers)
        return bool(r.charts or r.draft_charts)

    return group_by_edit([r for r in summary.reach if lands(r)])


def edit_key(edit: EditGroup) -> str:
    """The slot a tick on an edit is stored in: the field, the words taken out, and where it was authored.

    Anchored on what the *baseline* carried, so it survives the author rewording their own insertion: the
    slot stays, the content hash below moves, and the tick reopens as stale — the same behaviour a view's
    tick has when its text is edited again. It also survives a new text picking the edit up, which is why
    it is not the count of texts.
    """
    paths = sorted({p for change in edit.changes for p in (change.catalog_paths or set())})
    return json.dumps([edit.field, edit.deleted, paths], ensure_ascii=False)


def edit_slot(edit: EditGroup) -> str:
    """A short, URL-safe handle for one edit — the same on every rerun, so a link to it can be pasted.

    Hashed from the words alone, not from `edit_key`: a section's card holds the edit scoped to its own
    surface, while Blast radius holds it whole, and the two carry different texts and datasets. The words
    inserted and deleted are what `group_by_edit` keys on, so they are the one thing both agree about.
    """
    return hashlib.sha1(json.dumps([edit.field, edit.inserted, edit.deleted]).encode()).hexdigest()[:12]


def edit_fields(edit: EditGroup) -> dict[str, dict[str, Any]]:
    """What a tick on an edit binds to — the words themselves. Reword the insertion and the tick goes stale."""
    return {edit.field: {"old": edit.deleted, "new": edit.inserted}}


def reach_by_surface(reach: list[ChangeReach]) -> list[dict[str, Any]]:
    """Invert the blast radius: one row per affected surface, naming the changes that land on it.

    The by-change view answers "how far does this edit go", which is the review question. This answers
    "what happens to this page", which is the question you have when a specific chart matters to you —
    and it is the only view where a chart carrying two separate edits shows up as one thing.
    """
    rows: dict[tuple[str, str], dict[str, Any]] = {}

    def slot(kind: str, name: str, detail: str, published: bool) -> dict[str, Any]:
        row = rows.get((kind, name))
        if row is None:
            row = {"kind": kind, "name": name, "detail": detail, "published": published, "fields": []}
            rows[(kind, name)] = row
        return row

    for r in reach:
        label = field_label(r.field)
        for c in r.charts:
            detail = "data page" if c.get("has_data_page", True) else "via Learn more about this data"
            slot("chart", str(c.get("slug") or f"chart {c.get('chartId')}"), detail, True)["fields"].append(label)
        for c in r.draft_charts:
            name = str(c.get("slug") or f"chart {c.get('chartId')}")
            slot("draft_chart", name, "unpublished chart", False)["fields"].append(label)
        for m in r.mdims:
            n_views = int(m["n_views"])
            detail = f"{n_views} view{'s' if n_views != 1 else ''}"
            slot("mdim", str(m["catalogPath"]), detail, not m["is_draft"])["fields"].append(label)
        for e in r.explorers:
            n_views = int(e["n_views"])
            detail = f"{n_views} view{'s' if n_views != 1 else ''}"
            slot("explorer", str(e["slug"]), detail, True)["fields"].append(label)

    # Reader-facing surfaces first, data pages ahead of the sources drawer, then the pages carrying the
    # most edits — the order a reviewer reads them in.
    order = {"chart": 0, "mdim": 1, "explorer": 2, "draft_chart": 3}
    for row in rows.values():
        counts: dict[str, int] = {}
        for label in row["fields"]:
            counts[label] = counts.get(label, 0) + 1
        # A page can carry the same field twice — two distinct WYSK edits, say, from two indicators it
        # renders. Deduping for display without saying so made those rows unreadable.
        row["field_counts"] = counts
        row["n_changes"] = len(row["fields"])
        row["fields"] = sorted(counts)
    return sorted(
        rows.values(),
        key=lambda r: (
            order.get(r["kind"], 9),
            0 if r["detail"] == "data page" else 1,
            -r["n_changes"],
            r["name"],
        ),
    )


def change_identity(g: ChangeGroup) -> tuple[str, str]:
    """A distinct text change, independent of which surface renders it."""
    return (g.field, json.dumps([g.old, g.new], sort_keys=True, default=str))


def _mdim_reach(catalog_path: str, g: ChangeGroup, df_mdims: "pd.DataFrame", is_draft: bool) -> dict[str, Any]:
    """One MDim's entry in a change's reach: what to call it, where its views live, and which they are.

    `title` and `slug` come from the MDim list, so the detail views can use the same naming as the
    dimension grid and link each view to the page a reader would open. `views` carries the dimension dicts
    rather than a count, because a link needs them and a count can be derived.
    """
    row = df_mdims.loc[catalog_path] if catalog_path in df_mdims.index else None

    def field(name: str) -> str:
        if row is None:
            return ""
        value = row.get(f"{name}_source", row.get(name))
        return "" if value is None or value != value else str(value)  # NaN-safe

    return {
        "catalogPath": catalog_path,
        "title": field("title") or catalog_path,
        "slug": field("slug"),
        "n_views": len(g.view_dims),
        "views": [dict(d) for d in g.view_dims],
        "is_draft": is_draft,
    }


def _reach_slot(reach: dict[tuple[str, str], ChangeReach], g: ChangeGroup) -> ChangeReach:
    """The blast-radius row for this change, creating it on first sight of the text."""
    key = change_identity(g)
    slot = reach.get(key)
    if slot is None:
        slot = ChangeReach(field=g.field, old=g.old, new=g.new)
        reach[key] = slot
    slot.catalog_paths |= set(g.authored_in or g.catalog_paths or ({g.catalog_path} if g.catalog_path else set()))
    return slot


def keep_sections(summary: "Summary") -> set[str]:
    """Sections whose zero badge stays clickable instead of greying out.

    A zero is only worth greying when it is a finding rather than a silence, and there are three ways it
    is a silence: a surface whose lookup warned (its zero means "we could not look"), an MDim count that
    overflowed the view-by-view budget in either direction, and new indicators, which the Charts section
    reports even though they are not reviewable changes.
    """
    if summary.warnings:
        return set(COUNTED_SECTIONS)
    keep: set[str] = set()
    if summary.mdim_counts_are_ceilings:
        keep.add("mdims")
    if summary.n_new_indicators:
        keep.add("charts")
    return keep


def _collect_changes(seen: set[tuple[str, str]], groups: list[ChangeGroup]) -> None:
    """Accumulate distinct changes across surfaces.

    One reworded shared definition surfaces on its indicator, on every MDim view rendering it, and on
    every chart inheriting it. Adding those up would report the same edit several times over, so the
    field breakdown counts identities, not sightings.
    """
    seen.update(change_identity(g) for g in groups)


def _record_mdim_groups(
    summary: "Summary",
    reach: dict[tuple[str, str], "ChangeReach"],
    seen: set[tuple[str, str]],
    catalog_path: str,
    groups: list[ChangeGroup],
    df_mdims: "pd.DataFrame",
    *,
    is_draft: bool,
) -> None:
    """One MDim's changes into the summary: what it is counted as, its review marks, and its reach rows.

    Drafts go through the same recording, deliberately. Their cards carry Reviewed toggles like every
    other card, and the MDims badge counts `review_keys` — so recording a draft's reach without its marks
    left a branch whose only change is an unpublished MDim looking at a greyed-out MDims section it could
    never open, while the page said, correctly, that text had changed.

    What being a draft changes is only what it is *counted as*: `n_draft_mdims`, kept out of the
    reader-facing `n_mdims`, because no reader sees it yet.
    """
    if is_draft:
        summary.n_draft_mdims += 1
    else:
        summary.n_mdims += 1
        summary.n_mdim_changes += len(groups)
    _collect_changes(seen, groups)
    surface = surface_key("mdim", catalog_path)
    summary.review_keys.setdefault("mdims", []).extend((surface, *mark_identity(surface, g)) for g in groups)
    for g in groups:
        _reach_slot(reach, g).mdims.append(_mdim_reach(catalog_path, g, df_mdims, is_draft=is_draft))


def _count_fields(counts: dict[str, int], diffs: list[ViewDiff]) -> None:
    """Field breakdown of a single surface's diffs (used where no cross-surface dedup is needed)."""
    for g in group_changes(diffs):
        label = field_label(g.field)
        counts[label] = counts.get(label, 0) + 1


def _resolved(given: T | None, pending: "Future[T] | None") -> T:
    """Whichever the caller supplied, or the reading started on its behalf — exactly one exists.

    `summarize` prefetches only what it was not given, so a surface has either a value or a future. Any
    exception surfaces here, inside the try/except that owns that surface.
    """
    if given is not None:
        return given
    assert pending is not None, "a surface with no value must have a pending read"
    return pending.result()


def shared_facts(source_engine: Engine) -> tuple[BranchScope, set[str]]:
    """(git scope, datasets rebuilt here) — the two facts every surface needs, read once.

    One shells out to git and the other reads every dataset row, so they wait on different things and
    wait together. Callers that make several selections in one pass should read this once and pass both
    down; six `branch_scope()` calls and two `datasets_built_here()` were most of a cold load.
    """
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending_scope = pool.submit(branch_scope)
        pending_built = pool.submit(datasets_built_here, source_engine)
        return pending_scope.result(), pending_built.result()


def _mdim_groups_for(
    source_engine: Engine,
    target_engine: Engine,
    catalog_paths: list[str],
    scope: BranchScope,
) -> list[list[ChangeGroup]]:
    """This branch's change groups for each MDim, diffed concurrently, returned in the order given.

    One MDim's view diff is two independent queries, so a list of them is latency rather than work. The
    order is the caller's, not completion order: counts and review keys must not depend on timing.
    """
    if not catalog_paths:
        return []

    def groups_for(catalog_path: str) -> list[ChangeGroup]:
        view_diffs = [v for v in mdim_text_changes(source_engine, target_engine, catalog_path) if v.changed]
        return split_mdim_groups(catalog_path, view_diffs, scope)[0]

    with ThreadPoolExecutor(max_workers=min(8, len(catalog_paths))) as pool:
        return list(pool.map(groups_for, catalog_paths))


def summarize(
    source_engine: Engine,
    target_engine: Engine,
    master_engine: Engine | None = None,
    *,
    changed: "IndicatorChanges | None" = None,
    df_mdims: "pd.DataFrame | None" = None,
    explorers: "ExplorerChanges | None" = None,
    facts: tuple[BranchScope, set[str]] | None = None,
    attribution: dict[str, str] | None = None,
) -> Summary:
    """Everything the section badges and the owidbot comment need, in one pass.

    `master_engine` (master's own staging server) is optional but worth passing: without it, a change
    cannot be told from an edit master made that the baseline has not rebuilt yet, and everything lands
    under UNKNOWN.

    Each surface is wrapped: one failing (a table missing on an old staging server, say) must not blank
    the whole page or the PR comment, so it degrades to a warning the UI shows.
    """
    summary = Summary()
    seen: set[tuple[str, str]] = set()
    reach: dict[tuple[str, str], ChangeReach] = {}

    scope, built = facts if facts is not None else shared_facts(source_engine)

    # The three surfaces share nothing but the engines, and their cost is time spent waiting on MySQL, so
    # whatever the caller has not already read is fetched here, together. Each block below still consumes
    # its own result inside its own try/except, which keeps one broken surface from blanking the page.
    with ThreadPoolExecutor(max_workers=3) as pool:
        pending_charts = (
            pool.submit(changed_indicators, source_engine, target_engine, None, scope) if changed is None else None
        )
        pending_mdims = (
            pool.submit(mdim_changes_df, source_engine, target_engine, scope, built) if df_mdims is None else None
        )
        pending_explorers = (
            pool.submit(changed_explorer_views, source_engine, target_engine, scope, built)
            if explorers is None
            else None
        )
        pending_chart_text = pool.submit(changed_chart_texts, source_engine, target_engine, scope, built)

    # --- Charts (indicator layer) ---
    try:
        changed = _resolved(changed, pending_charts)
        summary.narrowed = changed.narrowed
        summary.n_indicators = len(changed.diffs)
        summary.n_new_indicators = len(changed.new_paths)
        diffs = changed.view_diffs()
        chart_groups = group_changes(diffs)
        summary.n_chart_changes = len(chart_groups)
        _collect_changes(seen, chart_groups)
        usage = charts_affected(source_engine, changed)
        summary.n_charts = len(charts_reached(chart_groups, usage))
        for g in chart_groups:
            by_id: dict[int, dict[str, Any]] = {}
            for iid in g.indicator_ids or ({g.indicator_id} if g.indicator_id is not None else set()):
                for c in usage.get(iid, []):
                    by_id.setdefault(int(c["chartId"]), c)
            slot = _reach_slot(reach, g)
            slot.charts = [c for c in by_id.values() if c.get("is_published", True)]
            slot.draft_charts = [c for c in by_id.values() if not c.get("is_published", True)]
        charts_surface = surface_key("charts", "indicators")
        summary.review_keys["charts"] = [(charts_surface, *mark_identity(charts_surface, g)) for g in chart_groups]
        origins = (
            attribution
            if attribution is not None
            else attribute_indicator_changes(source_engine, target_engine, changed.paths, master_engine)
        )
        for origin in origins.values():
            summary.attribution[origin] = summary.attribution.get(origin, 0) + 1
        # Report every stale dataset, not only ones behind a reported change: a stale build can also
        # *hide* a change, and then there is nothing in the lists to hang the warning off.
        summary.stale = stale_datasets(source_engine, target_engine)
    except Exception as e:  # noqa: BLE001 — a broken surface must not blank the whole report
        log.warning("metadata_diff.chart_discovery_failed", error=str(e))
        summary.warnings.append(f"Chart discovery failed: {e}")

    # --- Charts (their own config text) ---
    try:
        chart_text = pending_chart_text.result()
        chart_text_groups = group_changes(chart_text.view_diffs())
        summary.n_chart_text_changes = len(chart_text_groups)
        summary.n_charts_own_text = len(chart_text.diffs)
        _collect_changes(seen, chart_text_groups)
        charts_surface = surface_key("charts", "indicators")
        summary.review_keys.setdefault("charts", []).extend(
            (charts_surface, *mark_identity(charts_surface, g)) for g in chart_text_groups
        )
        for g in chart_text_groups:
            # The affected charts are the group's own membership: each chart is a view keyed by its slug.
            slot = _reach_slot(reach, g)
            slot.charts.extend(
                chart_text.charts[dims["chart"]] for dims in g.view_dims if dims.get("chart") in chart_text.charts
            )
    except Exception as e:  # noqa: BLE001
        log.warning("metadata_diff.chart_text_discovery_failed", error=str(e))
        summary.warnings.append(f"Chart text discovery failed: {e}")

    # --- MDims ---
    try:
        df_mdims = _resolved(df_mdims, pending_mdims)
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
            # Each MDim's view diff is two queries; up to MAX_MDIMS_RESOLVED of them run concurrently.
            # Results are consumed in `flagged` order, so what the page reports never depends on timing.
            for cp, ours in zip(flagged, _mdim_groups_for(source_engine, target_engine, flagged, scope)):
                if ours:
                    _record_mdim_groups(summary, reach, seen, cp, ours, df_mdims, is_draft=False)
        # Drafts are counted only where they actually have changed text, same test as the rest.
        if len(drafts) > MAX_MDIMS_RESOLVED:
            # Too many to diff view by view. Report the flag count as a ceiling and say so, rather than
            # resolve the first 25 and present the truncated number as exact — that is the silent
            # under-report this tool exists to catch. The flag is the drafts' own: an overflow here says
            # nothing about whether the reader-facing count above could be resolved.
            summary.draft_mdims_resolved = False
            summary.n_draft_mdims = len(drafts)
        else:
            for cp, draft_groups in zip(drafts, _mdim_groups_for(source_engine, target_engine, drafts, scope)):
                if draft_groups:
                    _record_mdim_groups(summary, reach, seen, cp, draft_groups, df_mdims, is_draft=True)
    except Exception as e:  # noqa: BLE001
        log.warning("metadata_diff.mdim_discovery_failed", error=str(e))
        summary.warnings.append(f"MDim discovery failed: {e}")

    # --- Explorers ---
    try:
        explorers = _resolved(explorers, pending_explorers)
        branch_views = explorers.branch_views()
        summary.n_explorers = len(branch_views)
        summary.n_explorer_views = sum(len(v) for v in branch_views.values())
        summary.n_other_explorers = len(explorers.other_views())
        for slug, diffs in branch_views.items():
            explorer_groups = group_changes(diffs)
            _collect_changes(seen, explorer_groups)
            explorer_surface = surface_key("explorer", slug)
            summary.review_keys.setdefault("explorers", []).extend(
                (explorer_surface, *mark_identity(explorer_surface, g)) for g in explorer_groups
            )
            for g in explorer_groups:
                # The dimensions, not just the count: one edit renders into a text per view here, so
                # counts alone cannot be combined across texts without double counting or under-counting.
                _reach_slot(reach, g).explorers.append(
                    {"slug": slug, "n_views": len(g.view_dims), "views": [dict(d) for d in g.view_dims]}
                )
    except Exception as e:  # noqa: BLE001
        log.warning("metadata_diff.explorer_discovery_failed", error=str(e))
        summary.warnings.append(f"Explorer discovery failed: {e}")

    for field_name, _ in seen:
        label = field_label(field_name)
        summary.fields[label] = summary.fields.get(label, 0) + 1
    summary.n_distinct_changes = len(seen)
    # Widest first: the blast radius is read to find what an edit costs, and the expensive ones lead.
    summary.reach = sorted(reach.values(), key=lambda r: (-r.n_reader_facing, -r.n_hidden, r.field))

    return summary


def dataset_owners(garden_dirs: list[str]) -> dict[str, list[str]]:
    """garden dataset dir -> the people who own it, from the step's own `.meta.yml`.

    `dataset.owners` is the authoritative record and the first entry is the accountable owner (see the
    dataset schema). Read from the file rather than from a roster in this app, so it cannot drift from
    what the dataset says — and only names, since nothing machine-readable maps a name to a GitHub handle
    and guessing one pings the wrong person.

    A dataset whose file cannot be read simply has no owner here: the instructions then say to hand the
    work to whoever is editing, which is true either way.
    """
    out: dict[str, list[str]] = {}
    for directory in garden_dirs:
        for suffix in (".meta.yml", ".meta.override.yml"):
            path = BASE_DIR / f"{directory}{suffix}"
            if not path.exists():
                continue
            try:
                parsed = yaml.safe_load(path.read_text()) or {}
            except Exception as e:  # noqa: BLE001 — a malformed file means "unknown owner", not a crash
                log.warning("metadata_diff.owners_unreadable", path=str(path), error=str(e))
                continue
            owners = ((parsed.get("dataset") or {}) if isinstance(parsed, dict) else {}).get("owners") or []
            names = [str(name).strip() for name in owners if str(name).strip()]
            if names:
                out[directory] = names
                break
    return out
