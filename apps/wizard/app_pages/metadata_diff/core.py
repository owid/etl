"""Pure diff logic for the Metadata Diff app.

This module has no Streamlit or database dependencies so it can be unit-tested in
isolation. It takes MDIM configs (as stored in `multi_dim_data_pages.config`) and
per-environment variable/chart-config lookups, and produces a per-view diff of the
user-visible metadata texts.

Environments are called "source" (the staging server, i.e. the state of this branch)
and "target" (production, i.e. the baseline), matching chart-diff's naming.
"""

import difflib
import hashlib
import html
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from etl.files import ruamel_dump

# Metadata fields we diff, in display order. Keys are the camelCase names used both as
# columns of the `variables` table and in the (camelized) view-level `metadata`
# overrides stored in the MDIM config.
METADATA_FIELDS = {
    "titlePublic": "Title",
    "descriptionShort": "Description",
    "descriptionKey": "WYSK",
    "descriptionProcessing": "Processing notes",
    "descriptionFromProducer": "Description from producer",
}

# Chart text fields, taken from the per-view chart config (chart_configs.full).
CHART_FIELDS = {
    "title": "Chart title",
    "subtitle": "Chart subtitle",
    "note": "Chart footnote",
}

# Prefix used to disambiguate chart fields from metadata fields in a flat field dict.
CHART_FIELD_PREFIX = "chart."


def owid_merge(dst: Any, src: Any) -> Any:
    """Merge `src` into `dst` the way the grapher site does (non-mutating).

    This mirrors the site's custom `merge` (`@ourworldindata/utils` `Util.ts`), which is
    used to merge view-level metadata overrides over the indicator metadata
    (`MultiDimDataPageConfig.mergeViewMetadata`): dicts merge recursively, but — unlike
    plain lodash merge — **arrays are overwritten completely instead of merged**. So a
    one-element `description_key` override replaces the whole bullet list.
    """
    if isinstance(dst, dict) and isinstance(src, dict):
        out = dict(dst)
        for k, v in src.items():
            if k in out:
                out[k] = owid_merge(out[k], v)
            else:
                out[k] = v
        return out
    # Arrays and scalars: the source value wins wholesale.
    return src


@dataclass
class ViewBundle:
    """Everything needed to compute the user-visible texts of one view in one environment."""

    dimensions: dict[str, str]
    metadata: dict[str, Any]  # merged metadata fields (flat, METADATA_FIELDS keys)
    chart: dict[str, Any]  # chart config text fields (CHART_FIELDS keys)
    # The indicator layer *before* MDIM overrides: the raw `variables` metadata. Diffing this
    # (rather than the merged text) tells us whether a change is shared with charts/other MDIMs
    # (the indicator itself changed) or contained to this MDIM (only an override changed).
    base: dict[str, Any] = field(default_factory=dict)
    indicator_id: int | None = None  # id of the view's first y-indicator in this environment
    catalog_path: str | None = None  # e.g. grapher/ns/ver/dataset/table#short_name — for the PR brief


@dataclass
class ViewDiff:
    dimensions: dict[str, str]
    is_new: bool = False  # view does not exist in target (production)
    # field name -> {"old": ..., "new": ...}; descriptionKey values are lists of strings.
    fields: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Staging id of the view's first y-indicator — the key for the blast-radius lookup.
    indicator_id: int | None = None
    catalog_path: str | None = None  # indicator catalogPath — resolves to the garden file in the PR brief
    # Subset of METADATA_FIELDS whose *indicator-layer* value changed (i.e. the change is not
    # just an MDIM override). These are the changes that also propagate to charts / other MDIMs.
    indicator_changed_fields: set[str] = field(default_factory=set)
    # The view's first y-indicator is a different indicator here than in the baseline — a *replacement*,
    # not an edit. Its texts then differ for two reasons at once, so no rewording can be attributed to
    # this branch; worth saying, rather than filing the difference under someone else's rebuild.
    indicator_replaced: bool = False

    @property
    def changed(self) -> bool:
        return self.is_new or bool(self.fields)

    @property
    def affects_indicator(self) -> bool:
        """Whether any user-visible change here comes from the shared indicator metadata."""
        return bool(self.indicator_changed_fields)


def _is_nan(value: Any) -> bool:
    """SQL NULLs surface as float NaN in pandas rows; NaN != NaN breaks comparisons."""
    return isinstance(value, float) and value != value


def _parse_json_maybe(value: Any) -> Any:
    """`variables.descriptionKey` comes back from MySQL as a JSON string.

    Only for that column. Every other field is plain text, and decoding those would rewrite a text
    that happens to be valid JSON into something the reader never sees — a title of `false` becoming
    the boolean, a description of `null` becoming empty.
    """
    if _is_nan(value):
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


# The only JSON-backed column among METADATA_FIELDS.
JSON_METADATA_FIELDS = {"descriptionKey"}


def _flatten_metadata_override(override: dict[str, Any]) -> dict[str, Any]:
    """Flatten a (camelized) view `metadata` override onto our flat field names.

    The stored override follows the indicator-metadata shape, where the public title
    lives under `presentation.titlePublic`. We only keep the fields we diff.
    """
    flat: dict[str, Any] = {}
    for key in METADATA_FIELDS:
        if key in override:
            flat[key] = override[key]
    presentation = override.get("presentation") or {}
    if "titlePublic" in presentation:
        flat["titlePublic"] = presentation["titlePublic"]
    return flat


def build_view_bundle(
    view: dict[str, Any],
    config_metadata: dict[str, Any] | None,
    variable_row: dict[str, Any] | None,
    chart_config: dict[str, Any] | None,
) -> ViewBundle:
    """Compute the final user-visible texts of a view, the way the site does.

    - `variable_row`: row of the `variables` table for the view's first y-indicator.
    - `config_metadata`: MDIM-level `metadata` override (applies to all views).
    - `view["metadata"]`: view-level override; merged last, element-wise for lists.
    - `chart_config`: the view's chart config (`chart_configs.full`).
    """
    base: dict[str, Any] = {}
    indicator_id: int | None = None
    if variable_row:
        for key in METADATA_FIELDS:
            value = variable_row.get(key)
            base[key] = _parse_json_maybe(value) if key in JSON_METADATA_FIELDS else (None if _is_nan(value) else value)
        # The site falls back to the internal name when no public title is set.
        if not base.get("titlePublic"):
            base["titlePublic"] = variable_row.get("name")
        if variable_row.get("id") is not None:
            indicator_id = int(variable_row["id"])
    catalog_path = variable_row.get("catalogPath") if variable_row else None
    if _is_nan(catalog_path):
        catalog_path = None

    merged = base
    for override in (config_metadata, view.get("metadata")):
        if override:
            merged = owid_merge(merged, _flatten_metadata_override(override))

    chart = {}
    if chart_config:
        chart = {key: chart_config.get(key) for key in CHART_FIELDS}

    return ViewBundle(
        dimensions=view["dimensions"],
        metadata=merged,
        chart=chart,
        base=base,
        indicator_id=indicator_id,
        catalog_path=str(catalog_path) if catalog_path else None,
    )


def as_bullets(value: Any) -> Any:
    """Normalize a value to a bullet list when it is one, so it can be rendered structurally.

    `variables.descriptionKey` reaches us either as a JSON list OR as a single markdown string
    with bullets joined by "\\n- " (how the grapher channel serializes multi-bullet keys). Return a
    list of bullet strings when the value is a bullet list in either form; leave genuine prose
    (and non-strings) untouched, so the renderer shows bullets as bullets and prose as prose.
    """
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        lines = [line.strip() for line in value.splitlines() if line.strip()]
        if lines and all(line.startswith("- ") for line in lines):
            return [line[2:].strip() for line in lines]
    return value


def _dims_key(dimensions: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(dimensions.items()))


def _normalize(value: Any) -> Any:
    """Normalize values for comparison (treat None, NaN and "" the same, strip whitespace)."""
    if value is None or _is_nan(value):
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    return value


def diff_views(
    source_bundles: list[ViewBundle],
    target_bundles: list[ViewBundle],
) -> list[ViewDiff]:
    """Compare each source (staging) view against the matching target (production) view.

    Views are matched by their dimension choices. Source views with no counterpart in
    the target are flagged `is_new`. Target views that disappeared from the source are
    ignored (removed views have no staging text to inspect).
    """
    target_by_key = {_dims_key(b.dimensions): b for b in target_bundles}

    diffs = []
    for src in source_bundles:
        target = target_by_key.get(_dims_key(src.dimensions))
        view_diff = ViewDiff(
            dimensions=src.dimensions,
            is_new=target is None,
            indicator_id=src.indicator_id,
            catalog_path=src.catalog_path,
        )

        for fields, attr, prefix in (
            (METADATA_FIELDS, "metadata", ""),
            (CHART_FIELDS, "chart", CHART_FIELD_PREFIX),
        ):
            src_values = getattr(src, attr)
            target_values = getattr(target, attr) if target else {}
            for key in fields:
                old = _normalize(target_values.get(key))
                new = _normalize(src_values.get(key))
                if old != new:
                    view_diff.fields[prefix + key] = {"old": old, "new": new}

        # Indicator-layer diff: compare the raw indicator metadata (before overrides) between the
        # two environments. A field that changed *here* propagates to every chart / other MDIM that
        # uses this indicator. A field that changed only in the merged text (an MDIM override) does
        # not. We skip new views: a brand-new MDIM view does not, by itself, change any indicator.
        # And we skip a view whose indicator was *replaced* — see `_same_indicator`.
        if target is not None and _same_indicator(src.catalog_path, target.catalog_path):
            for key in METADATA_FIELDS:
                if _normalize(src.base.get(key)) != _normalize(target.base.get(key)):
                    view_diff.indicator_changed_fields.add(key)
        elif target is not None and view_diff.fields:
            # Same view, different indicator. Recorded so the caller can say so instead of implying the
            # baseline moved on.
            view_diff.indicator_replaced = True

        diffs.append(view_diff)
    return diffs


def _same_indicator(src_path: str | None, target_path: str | None) -> bool:
    """Do two catalog paths name the same indicator, ignoring which version it came from?

    A view repointed to a *different* indicator is a replacement, not an edit. Its `base` metadata then
    describes two unrelated indicators, and every field they disagree on would be reported as a shared
    indicator edit — sending the reviewer to change garden metadata on an indicator nobody touched, and
    counting every other chart that uses it as blast radius.

    Identity is everything about the path except the **version**, because the version is the only segment
    a bump changes: it moves `grapher/un/2026-05-01/wpp/tbl#population` to
    `grapher/un/2026-08-19/wpp/tbl#population` without changing which indicator that is, and demanding
    equal paths would report no indicator-layer change at all for the update the tool exists to review.

    Comparing the `#short_name` tail alone would be simpler and is not enough: short names are only unique
    within a dataset, and the common ones are common precisely where a repoint is plausible — an MDim view
    moved from one source's `gini` or `population` to another's would compare as the same indicator, which
    is the case this function exists to reject.

    Nothing is lost by skipping a replacement: a view's indicator can only change because the MDim's own
    config did, so `covers_mdim` is true for whoever changed it and `split_mdim_groups` reports the view's
    text difference either way. An unknown path on either side stays comparable — "cannot tell" errs
    toward reporting, since a spurious shared-edit flag is visible and arguable while a dropped one is not.
    """
    if not src_path or not target_path:
        return True
    return _indicator_identity(src_path) == _indicator_identity(target_path)


def _indicator_identity(catalog_path: str) -> tuple[str, ...]:
    """Everything that names an indicator except which version of the dataset it came from."""
    left, _, short_name = catalog_path.partition("#")
    parts = left.strip("/").split("/")
    if parts and parts[0] in ("grapher", "garden", "meadow", "snapshot"):
        parts = parts[1:]
    # parts is `namespace / version / dataset [/ table]` — drop the version, keep the rest.
    if len(parts) >= 2:
        parts = parts[:1] + parts[2:]
    return (*parts, short_name)


def surface_key(kind: str, ident: str) -> str:
    """Namespaced key for the reviewed-state rows of one surface (`list:chart:<slug>`, ...).

    The `list:` prefix is historical: it kept these apart from an Approve/Flag sign-off that keyed on the
    bare catalogPath, which is gone — that second review record was read by nothing and could contradict
    these ticks indefinitely. Kept as-is so existing ticks still resolve.
    """
    return f"list:{kind}:{ident}"


def mark_identity(surface: str, group: "ChangeGroup") -> tuple[str, str]:
    """(slot key, content hash) for one change on one surface.

    The slot has to name *where* the change is, not just which field it is: chart-side changes carry no
    view dimensions at all (an indicator is a view with none), so keying on field + dimensions alone —
    the way the MDim review page can — would give every `description_short` change on the page the same
    key, and they would share a single row. Including the indicators the change lands on separates them.

    The hash covers only the text, so the slot survives an edit while the mark goes stale.
    """
    where = sorted(group.catalog_paths) or ([group.catalog_path] if group.catalog_path else [])
    slot = json.dumps(
        [surface, group.field, where, sorted(json.dumps(d, sort_keys=True) for d in group.view_dims)],
        sort_keys=True,
    )
    change_key = hashlib.sha256(slot.encode()).hexdigest()
    content_hash = hashlib.sha256(json.dumps([group.old, group.new], sort_keys=True, default=str).encode()).hexdigest()
    return change_key, content_hash


def field_label(field_name: str) -> str:
    if field_name.startswith(CHART_FIELD_PREFIX):
        return CHART_FIELDS[field_name[len(CHART_FIELD_PREFIX) :]]
    return METADATA_FIELDS[field_name]


# Where a per-view override for a given diff field lives in the MDim's Python step:
# (view attribute, nested dict key or None, snake_case metadata/config key).
OVERRIDE_TARGET: dict[str, tuple[str, str | None, str]] = {
    "titlePublic": ("metadata", "presentation", "title_public"),
    "descriptionShort": ("metadata", None, "description_short"),
    "descriptionKey": ("metadata", None, "description_key"),
    "descriptionProcessing": ("metadata", None, "description_processing"),
    "descriptionFromProducer": ("metadata", None, "description_from_producer"),
    "chart.title": ("config", None, "title"),
    "chart.subtitle": ("config", None, "subtitle"),
    "chart.note": ("config", None, "note"),
}


def _py_value(val: Any, indent: str = "        ") -> str:
    """Render a value as a Python literal; lists get one item per line for readable bullet overrides."""
    if isinstance(val, list):
        if not val:
            return "[]"
        inner = "\n".join(f"{indent}    {json.dumps(v, ensure_ascii=False)}," for v in val)
        return "[\n" + inner + f"\n{indent}]"
    return json.dumps(val, ensure_ascii=False)


def override_snippet(view: ViewDiff, field_name: str, value: Any) -> str:
    """A copy-pasteable MDim `.py` block that sets THIS view's field to `value` as a view override.

    Mirrors the real override idiom (`view.metadata = view.metadata or {}; view.metadata[...] = ...`)
    used in e.g. wb/latest/incomes_pip.py.
    """
    container, nested, key = OVERRIDE_TARGET[field_name]
    dims = ", ".join(f"{slug}={json.dumps(choice, ensure_ascii=False)}" for slug, choice in view.dimensions.items())
    value = _py_value(value)
    lines = [
        "for view in c.views:",
        f"    if view.matches({dims}):",
        f"        view.{container} = view.{container} or {{}}",
    ]
    if nested:
        lines.append(f'        view.{container}.setdefault("{nested}", {{}})["{key}"] = {value}')
    else:
        lines.append(f'        view.{container}["{key}"] = {value}')
    return "\n".join(lines)


def parse_catalog_path(catalog_path: str | None) -> tuple[str, str, str] | None:
    """Resolve an indicator's grapher catalogPath to (garden .meta file dir, table, short_name).

    `grapher/worldbank_wdi/2026-07-27/wdi/wdi#fp_cpi_totl_zg`
        -> ('etl/steps/data/garden/worldbank_wdi/2026-07-27/wdi', 'wdi', 'fp_cpi_totl_zg')

    The metadata texts are authored in the *garden* step, so we point there (the grapher step just
    re-exports). The exact filename (`<dataset>.meta.yml` vs `.meta.override.yml`) and whether the
    garden version matches the grapher one are confirmed when the PR is actually built."""
    if not catalog_path or "#" not in catalog_path:
        return None
    left, short_name = catalog_path.split("#", 1)
    parts = left.strip("/").split("/")
    if parts and parts[0] in ("grapher", "garden", "meadow", "snapshot"):
        parts = parts[1:]
    if len(parts) < 3 or not short_name:
        return None
    namespace, version, dataset = parts[0], parts[1], parts[2]
    table = parts[3] if len(parts) >= 4 else dataset
    # The grapher channel flattens a dimensional indicator into one column per dimension combination,
    # suffixing the base name with `__<dim>_<value>...`. The garden `.meta.yml` is authored under the
    # BASE name (the Jinja template renders per dimension), so strip the flattening suffix.
    base_short_name = short_name.split("__", 1)[0]
    return f"etl/steps/data/garden/{namespace}/{version}/{dataset}", table, base_short_name


def yaml_field_snippet(field_name: str, value: Any) -> str:
    """A pastable `<snake_key>: <value>` YAML snippet for one indicator metadata field, so the value
    (including multi-bullet `description_key` lists) can be dropped straight under its variable."""
    key = OVERRIDE_TARGET.get(field_name, (None, None, field_name))[2]
    # The grapher channel serializes `description_key` as one markdown string ("- b1\n- b2"); normalize
    # it back to a list so it dumps as YAML bullets rather than one giant quoted scalar.
    value = as_bullets(value)
    try:
        return ruamel_dump({key: value}).rstrip("\n")
    except Exception:
        return f"{key}: {value!r}"


@dataclass
class ChangeGroup:
    """One distinct text change, shared by every view that renders it — the review unit.

    "Review by distinct text": a shared indicator change renders identically across every view using
    that indicator, so it collapses to one group the reviewer judges once (rather than view-by-view).
    """

    field: str
    old: Any
    new: Any
    view_dims: list[dict[str, str]] = field(default_factory=list)
    affects_indicator: bool = False
    # Any view in this group renders a different indicator here than in the baseline.
    indicator_replaced: bool = False
    indicator_id: int | None = None
    catalog_path: str | None = None  # indicator catalogPath (shared changes) — for the PR brief
    # Every distinct indicator *id* whose indicator layer carries this same text change. A shared
    # definition renders into many indicators, so the reach of "apply to all" is the union of all their
    # charts/MDims — not just the first indicator's. The brief aggregates usage over this whole set.
    indicator_ids: set[int] = field(default_factory=set)
    # Every distinct indicator catalogPath whose *indicator layer* carries this same text change. When
    # the identical change lands on more than one indicator, that's the fingerprint of a shared
    # `definitions.*`/anchor edit (one template renders into many variables) — the PR brief points there
    # instead of guessing a single variable, and warns the observed reach is a floor.
    catalog_paths: set[str] = field(default_factory=set)


def dims_str(dims: dict[str, str]) -> str:
    return ", ".join(f"{k}={v}" for k, v in dims.items()) or "(default view)"


def as_plaintext(val: Any) -> str:
    """A field value flattened to one line, for the Markdown outputs."""
    if isinstance(val, list):
        return " · ".join(str(x) for x in val)
    if val in (None, ""):
        return "—"
    return str(val)


def group_usage(g: "ChangeGroup", usage: dict[int, dict[str, list[Any]]]) -> dict[str, list[Any]]:
    """Union the blast radius over *every* indicator the change touches, deduped.

    A shared definition renders into many indicators, so "apply to all" reaches the union of all their
    charts and MDims. Reading only the group's first indicator (`usage[g.indicator_id]`) undercounts
    that reach — for a shared-definition edit, badly. We aggregate over `g.indicator_ids` (falling back
    to the single `indicator_id` for older groups), deduping charts by chartId and MDims by catalogPath.
    """
    ids = g.indicator_ids or ({g.indicator_id} if g.indicator_id is not None else set())
    charts: dict[int, dict[str, Any]] = {}
    drafts: dict[int, dict[str, Any]] = {}
    mdims: dict[str, dict[str, Any]] = {}
    for iid in ids:
        imp = usage.get(iid, {})
        for c in imp.get("charts", []):
            charts.setdefault(c["chartId"], c)
        for c in imp.get("draft_charts", []):
            drafts.setdefault(c["chartId"], c)
        for m in imp.get("mdims", []):
            mdims.setdefault(m["catalogPath"], m)
    return {
        "charts": list(charts.values()),
        "draft_charts": list(drafts.values()),
        "mdims": list(mdims.values()),
    }


# Fields laid out in full on an indicator's own data page. Grapher gives a data page to a
# single-indicator chart; a chart combining several has none, and its readers reach the same text through
# "Learn more about this data", per indicator. Both audiences can see the change — one has to open a
# drawer first — so this distinction is about prominence, never about whether a chart counts.
DATA_PAGE_ONLY_FIELDS = {"descriptionKey", "descriptionProcessing", "descriptionFromProducer"}


def behind_sources_drawer(fields: Iterable[str], chart: dict[str, Any]) -> bool:
    """Whether this chart shows these fields only behind "Learn more about this data".

    True for a multi-indicator chart when every changed field is one the data page owns. For labelling
    only: an earlier version read the same condition as "the change reaches nobody here" and dropped such
    charts from every count, which understated the reach of a WYSK edit — the commonest change reviewed
    here — on exactly the charts whose readers have to go looking for it.
    """
    if set(fields) - DATA_PAGE_ONLY_FIELDS:
        return False
    return not chart.get("has_data_page", True)


def charts_behind_drawer(fields: Iterable[str], charts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The charts among these that show the fields only behind the sources drawer."""
    return [c for c in charts if behind_sources_drawer(fields, c)]


def split_by_prominence(
    charts: list[dict[str, Any]], fields: Iterable[str] | None = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(charts whose page shows the change, charts that keep it behind the sources drawer), each by slug.

    Two groups rather than one flagged list: on thirty charts a per-row badge is something you scan for,
    while a heading is something you see. `fields` decides what counts as prominent — a title or short
    description is on every chart's canvas, so for those the second group is empty and the list stays one
    section.
    """
    behind = (
        charts_behind_drawer(fields, charts)
        if fields is not None
        else [c for c in charts if not c.get("has_data_page", True)]
    )
    behind_ids = {id(c) for c in behind}
    on_page = [c for c in charts if id(c) not in behind_ids]
    return charts_in_reading_order(on_page), charts_in_reading_order(behind)


def charts_in_reading_order(charts: list[dict[str, Any]], fields: Iterable[str] | None = None) -> list[dict[str, Any]]:
    """Charts ordered the way a reviewer wants to read them: most prominent first, then by slug.

    A chart with a data page lays the changed text out on the page; one without shows it behind "Learn
    more about this data". Both are affected, but the first group is where the change actually meets a
    reader, so it goes first — and a long list stays scannable because the split is visible rather than
    interleaved. `fields` decides what counts as prominent; without it, fall back to whether the chart has
    a data page at all.
    """

    def key(chart: dict[str, Any]) -> tuple[bool, str]:
        if fields is not None:
            behind = behind_sources_drawer(fields, chart)
        else:
            behind = not chart.get("has_data_page", True)
        return (behind, str(chart.get("slug") or ""))

    return sorted(charts, key=key)


def affected_charts(g: "ChangeGroup", usage: dict[int, dict[str, list[Any]]]) -> list[dict[str, Any]]:
    """Every published chart this change reaches — the one reach number, wherever it is reported."""
    return list(group_usage(g, usage).get("charts", []))


def affected_drafts(g: "ChangeGroup", usage: dict[int, dict[str, list[Any]]]) -> list[dict[str, Any]]:
    """Unpublished charts this change lands on — shown, never counted as reach.

    A draft renders the new text for whoever opens it in the admin and for nobody else, so it belongs in
    the review (its author may well be the person reading this) and outside every reader-facing number.
    """
    return list(group_usage(g, usage).get("draft_charts", []))


def group_changes(view_diffs: list[ViewDiff]) -> list[ChangeGroup]:
    """Collapse per-view field changes into distinct (field, old→new) groups, ranked by reach (views)."""
    groups: dict[tuple[str, str, str], ChangeGroup] = {}
    order: list[tuple[str, str, str]] = []
    for v in view_diffs:
        if not v.changed:
            continue
        for fld, change in v.fields.items():
            key = (
                fld,
                json.dumps(change.get("old"), sort_keys=True, default=str),
                json.dumps(change.get("new"), sort_keys=True, default=str),
            )
            g = groups.get(key)
            if g is None:
                g = ChangeGroup(field=fld, old=change.get("old"), new=change.get("new"))
                groups[key] = g
                order.append(key)
            g.view_dims.append(v.dimensions)
            if v.indicator_replaced:
                g.indicator_replaced = True
            if fld in v.indicator_changed_fields:
                g.affects_indicator = True
                if g.indicator_id is None:
                    g.indicator_id = v.indicator_id
                if g.catalog_path is None:
                    g.catalog_path = v.catalog_path
                if v.indicator_id is not None:
                    g.indicator_ids.add(v.indicator_id)
                if v.catalog_path:
                    g.catalog_paths.add(v.catalog_path)
    return sorted((groups[k] for k in order), key=lambda g: (-len(g.view_dims), g.field))


def distinct_indicator_short_names(catalog_paths: Iterable[str]) -> list[str]:
    """Distinct base (pre-flatten) indicator short_names among a set of indicator catalogPaths.

    When one distinct text change renders identically across several of these, that's the fingerprint of
    a shared `definitions.*`/anchor edit: the same text can only reach multiple indicators via a shared
    template. The PR brief uses this to point at the shared definition instead of a single variable, and
    to warn that the reach observed in the diff is a floor.
    """
    names: list[str] = []
    for cp in catalog_paths:
        parsed = parse_catalog_path(cp)
        if parsed and parsed[2] not in names:
            names.append(parsed[2])
    return sorted(names)


def distinct_garden_datasets(catalog_paths: Iterable[str]) -> list[str]:
    """Distinct garden step dirs (`etl/steps/data/garden/<ns>/<version>/<dataset>`) among indicator paths.

    Groups are keyed on the text, so an identical edit made in two different garden datasets lands in one
    group. That is the right review unit — the reviewer judges the text once — but the *edit* is then two
    edits in two files, and both rebuilds are needed. Anything that names a file or a build command reads
    this rather than the group's first path, so neither dataset is silently dropped.
    """
    dirs: list[str] = []
    for cp in catalog_paths:
        parsed = parse_catalog_path(cp)
        if parsed and parsed[0] not in dirs:
            dirs.append(parsed[0])
    return sorted(dirs)


def text_change_key(catalog_path: str, field: str, old: Any, new: Any) -> str:
    """View-agnostic, content-bound key for one distinct text change.

    Unlike `change_group_identity` (per group of views), this keys only on (MDim, field, old→new), so the
    same distinct change maps to one key however it is grouped. Content-bound: editing the text mints a
    new key, so anything stored against it resets with the text.
    """
    content = json.dumps([old, new], sort_keys=True, default=str)
    return hashlib.sha256(f"{catalog_path}\x1f{field}\x1f{content}".encode()).hexdigest()


def change_group_identity(catalog_path: str, group: ChangeGroup) -> tuple[str, str]:
    """Stable ``(change_key, content_hash)`` for a change group — the review lock-in identity.

    ``change_key`` identifies the *slot* (this MDim, this field, this set of views) so a stored review
    can be found again; ``content_hash`` binds it to the exact old→new text, so ANY later edit makes the
    stored review stale — it no longer matches — and forces a re-review. This mirrors how Chart Diff
    binds an approval to a chart's ``sourceUpdatedAt``/``targetUpdatedAt``.
    """
    dims_sig = json.dumps(sorted(json.dumps(d, sort_keys=True) for d in group.view_dims))
    change_key = hashlib.sha256(f"{catalog_path}\x1f{group.field}\x1f{dims_sig}".encode()).hexdigest()
    content_hash = hashlib.sha256(json.dumps([group.old, group.new], sort_keys=True, default=str).encode()).hexdigest()
    return change_key, content_hash


# ---------------------------------------------------------------------------
# Inline word-level diff rendering (shared by the tree tooltips and the view page)
# ---------------------------------------------------------------------------


def _tokenize(text: str) -> list[str]:
    """Split text into words and whitespace, so diffs align on word boundaries."""
    tokens = []
    current = ""
    for ch in text:
        if ch.isspace() != (current[-1].isspace() if current else None):
            if current:
                tokens.append(current)
            current = ch
        else:
            current += ch
    if current:
        tokens.append(current)
    return tokens


def inline_diff_html(old: str, new: str, side: str = "both") -> str:
    """Word-level diff of two strings as HTML.

    `side` controls what is shown: "old" renders the old text with deletions
    highlighted, "new" renders the new text with insertions highlighted, "both"
    interleaves deletions and insertions (for compact previews).
    """
    old_tokens = _tokenize(old or "")
    new_tokens = _tokenize(new or "")
    matcher = difflib.SequenceMatcher(a=old_tokens, b=new_tokens, autojunk=False)

    parts = []
    for op, a1, a2, b1, b2 in matcher.get_opcodes():
        old_chunk = html.escape("".join(old_tokens[a1:a2]))
        new_chunk = html.escape("".join(new_tokens[b1:b2]))
        if op == "equal":
            parts.append(old_chunk)
        else:
            if old_chunk and side in ("old", "both"):
                parts.append(f'<del class="mdd-del">{old_chunk}</del>')
            if new_chunk and side in ("new", "both"):
                parts.append(f'<ins class="mdd-ins">{new_chunk}</ins>')
    return "".join(parts)


def diff_preview_html(view_diff: ViewDiff, max_fields: int = 3, max_chars: int = 320) -> str:
    """Compact HTML preview of a view's changes, used in the tree hover tooltips."""
    if view_diff.is_new:
        return '<p class="mdd-new">New view — it does not exist in production.</p>'
    if not view_diff.fields:
        return "<p>No changes.</p>"

    blocks = []
    for field_name, change in list(view_diff.fields.items())[:max_fields]:
        old, new = as_bullets(change["old"]), as_bullets(change["new"])
        if isinstance(old, list) or isinstance(new, list):
            old_list = old if isinstance(old, list) else [old]
            new_list = new if isinstance(new, list) else [new]
            bullet_bits = []
            for i in range(max(len(old_list), len(new_list))):
                o = old_list[i] if i < len(old_list) else ""
                n = new_list[i] if i < len(new_list) else ""
                if _normalize(o) != _normalize(n):
                    bullet_bits.append(f"<li>{truncate_html(inline_diff_html(str(o), str(n)), max_chars)}</li>")
            body = f'<ul class="mdd-bullets">{"".join(bullet_bits)}</ul>'
        else:
            body = truncate_html(inline_diff_html(str(old), str(new)), max_chars)
        blocks.append(f'<div class="mdd-field"><b>{html.escape(field_label(field_name))}</b>{body}</div>')

    hidden = len(view_diff.fields) - max_fields
    if hidden > 0:
        blocks.append(f"<p>… and {hidden} more field{'s' if hidden > 1 else ''}.</p>")
    return "".join(blocks)


def edit_fingerprint(old: Any, new: Any) -> tuple[str, str]:
    """(inserted text, deleted text) for one change — what was *authored*, wherever it landed.

    Two changes with the same fingerprint are one edit applied in several places: a sentence added to a
    shared `definitions.*` entry renders into every description referencing it, each with different
    surrounding text, so comparing whole texts sees many changes where a reviewer sees one.

    Two edits that happen to insert and delete exactly the same words also group. That is a fair reading
    of "one edit applied twice", and grouping them is the lesser error: the alternative counts a single
    authored sentence as eleven separate changes to review.
    """
    old_tokens, new_tokens = _tokenize(as_plaintext(old)), _tokenize(as_plaintext(new))
    matcher = difflib.SequenceMatcher(a=old_tokens, b=new_tokens, autojunk=False)
    inserted, deleted = [], []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag in ("insert", "replace"):
            inserted.append("".join(new_tokens[j1:j2]))
        if tag in ("delete", "replace"):
            deleted.append("".join(old_tokens[i1:i2]))

    def joined(parts: list[str]) -> str:
        return " … ".join(" ".join(p.split()) for p in parts if p.strip())

    return joined(inserted), joined(deleted)


def diff_window_html(old: Any, new: Any, max_chars: int = 240, lead: int = 70) -> str:
    """Word-level diff of one change, windowed on the first edit rather than cut from the start.

    Everything `inline_diff_html` emits before its first `<del>`/`<ins>` is plain text — unchanged tokens
    carry no markup — so the window can start at any character before that point without splitting a tag.
    """
    rendered = inline_diff_html(as_plaintext(old), as_plaintext(new))
    marks = [i for i in (rendered.find("<del"), rendered.find("<ins")) if i != -1]
    if not marks:
        # No highlight to centre on: the change is a reorder, or the values differ only in whitespace.
        return truncate_html(rendered, max_chars)

    start = max(0, min(marks) - lead)
    if start:
        # Snap forward to a word boundary so the window does not open mid-word.
        space = rendered.find(" ", start)
        start = space + 1 if 0 <= space < min(marks) else start
    return ("… " if start else "") + truncate_html(rendered[start:], max_chars)


def truncate_html(rendered: str, max_chars: int) -> str:
    """Truncate rendered diff HTML to `max_chars` of *visible* text (tags don't count).

    Counting tag characters against the budget made the preview cut off right after the change;
    measuring only visible text keeps a few more words of context after the edit.
    """
    visible = 0
    i = 0
    n = len(rendered)
    while i < n and visible < max_chars:
        if rendered[i] == "<":
            close = rendered.find(">", i)
            if close == -1:
                break
            i = close + 1
        else:
            visible += 1
            i += 1
    if i >= n:
        return rendered
    safe = rendered[:i]
    # Close any dangling del/ins so the tooltip doesn't bleed styling.
    for tag in ("del", "ins"):
        if safe.count(f"<{tag}") > safe.count(f"</{tag}>"):
            safe += f"</{tag}>"
    return safe + "…"


# Sections of the page. Blast radius leads — it is the overview of everywhere the branch's edits land,
# and the three review surfaces follow in Chart Diff's order, with the icons Chart Diff gives them.
SECTIONS = {
    "blast": (":material/explosion:", "Blast radius"),
    "charts": (":material/show_chart:", "Charts"),
    "mdims": (":material/dashboard:", "MDims"),
    "explorers": (":material/explore:", "Explorers"),
}
# Sections whose badge counts reviewed changes. Blast radius holds no sign-off of its own — it reports
# reach — so a counter there would read as "nothing to review" when it means "nothing to count".
COUNTED_SECTIONS = frozenset({"charts", "mdims", "explorers"})
# The page opens on Blast radius: the first question about a branch is how far its edits go, and the three
# review sections are where you go once you know. It is also the section the URL omits when it is current.
DEFAULT_SECTION = "blast"


def section_label(section: str, progress: dict[str, tuple[int, int]]) -> str:
    """Section label carrying its review progress, so "anything left?" is answerable without clicking.

    `reviewed/total` while anything is outstanding, and a tick once the section is done — a bare total
    cannot distinguish "nothing to review" from "nothing reviewed yet", which is the whole question.
    """
    icon, name = SECTIONS[section]
    if section not in COUNTED_SECTIONS:
        return f"{icon} {name}"
    done, total = progress.get(section, (0, 0))
    if not total:
        return f"{icon} {name} (0)"
    if done == total:
        return f"{icon} {name} ({total} ✓)"
    return f"{icon} {name} ({done}/{total})"


def empty_sections(progress: dict[str, tuple[int, int]], keep: Iterable[str] = ()) -> list[str]:
    """Counted sections with nothing in them — to be shown greyed out, not removed.

    Removing them would read as tidier and say less: "Explorers (0)" is the tool reporting that it looked,
    and a bar that silently shrinks cannot be told apart from a tool that never checked. Greyed keeps the
    zero legible and still stops anyone opening a page with nothing on it.

    A zero is only worth greying when it is a finding rather than a silence, so callers pass `keep` for
    the sections whose count they cannot vouch for: a surface whose lookup warned, or one whose page
    carries something that is not a reviewable change (new indicators have no old text to diff, so they
    sit behind a zero badge and still need reading).

    Blast radius is never counted and never greyed: it is the default, and the page has to land somewhere.
    """
    forced = set(keep)
    return [s for s in SECTIONS if s in COUNTED_SECTIONS and s not in forced and progress.get(s, (0, 0))[1] == 0]


def coerce_section(value: object, fallback: str = DEFAULT_SECTION) -> str:
    """Whatever the switcher hands back (or the URL carries), as a section key.

    `st.segmented_control` uses the formatted label as its wire format and returns that label unchanged
    when it matches no current option. Our labels count reviewed changes, so every tick invalidates the
    label the browser is holding — without this, the label leaks into the selection and into the URL,
    where the next page load rejects it. The count is stripped by matching the label's stable prefix.
    """
    if not isinstance(value, str):
        return fallback
    if value in SECTIONS:
        return value
    for section, (icon, name) in SECTIONS.items():
        if value.startswith(f"{icon} {name}"):
            return section
    return fallback
