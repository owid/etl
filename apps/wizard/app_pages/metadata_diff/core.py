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
    """`variables.descriptionKey` comes back from MySQL as a JSON string."""
    if _is_nan(value):
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


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
            base[key] = _parse_json_maybe(variable_row.get(key))
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
        if target is not None:
            for key in METADATA_FIELDS:
                if _normalize(src.base.get(key)) != _normalize(target.base.get(key)):
                    view_diff.indicator_changed_fields.add(key)

        diffs.append(view_diff)
    return diffs


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


def text_change_key(catalog_path: str, field: str, old: Any, new: Any) -> str:
    """View-agnostic, content-bound key for one distinct text change (the AUTHOR's scope decision).

    Unlike `change_group_identity` (per group of views), this keys only on (MDim, field, old→new), so
    the same distinct change maps to ONE decision whether the author sets it from a single view (View
    diff) or the reviewer sees it grouped (Review). Content-bound: editing the text mints a new key,
    so the scope decision resets with the text.
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
                    bullet_bits.append(f"<li>{_truncate_html(inline_diff_html(str(o), str(n)), max_chars)}</li>")
            body = f'<ul class="mdd-bullets">{"".join(bullet_bits)}</ul>'
        else:
            body = _truncate_html(inline_diff_html(str(old), str(new)), max_chars)
        blocks.append(f'<div class="mdd-field"><b>{html.escape(field_label(field_name))}</b>{body}</div>')

    hidden = len(view_diff.fields) - max_fields
    if hidden > 0:
        blocks.append(f"<p>… and {hidden} more field{'s' if hidden > 1 else ''}.</p>")
    return "".join(blocks)


def _truncate_html(rendered: str, max_chars: int) -> str:
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
