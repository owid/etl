"""Pure diff logic for the Metadata Diff app.

This module has no Streamlit or database dependencies so it can be unit-tested in
isolation. It takes MDIM configs (as stored in `multi_dim_data_pages.config`) and
per-environment variable/chart-config lookups, and produces a per-view diff of the
user-visible metadata texts.

Environments are called "source" (the staging server, i.e. the state of this branch)
and "target" (production, i.e. the baseline), matching chart-diff's naming.
"""

import difflib
import html
import json
from dataclasses import dataclass, field
from typing import Any

# Metadata fields we diff, in display order. Keys are the camelCase names used both as
# columns of the `variables` table and in the (camelized) view-level `metadata`
# overrides stored in the MDIM config.
METADATA_FIELDS = {
    "titlePublic": "Title",
    "descriptionShort": "Description",
    "descriptionKey": "What you should know about this data",
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


@dataclass
class ViewDiff:
    dimensions: dict[str, str]
    is_new: bool = False  # view does not exist in target (production)
    # field name -> {"old": ..., "new": ...}; descriptionKey values are lists of strings.
    fields: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def changed(self) -> bool:
        return self.is_new or bool(self.fields)


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
    if variable_row:
        for key in METADATA_FIELDS:
            base[key] = _parse_json_maybe(variable_row.get(key))
        # The site falls back to the internal name when no public title is set.
        if not base.get("titlePublic"):
            base["titlePublic"] = variable_row.get("name")

    merged = base
    for override in (config_metadata, view.get("metadata")):
        if override:
            merged = owid_merge(merged, _flatten_metadata_override(override))

    chart = {}
    if chart_config:
        chart = {key: chart_config.get(key) for key in CHART_FIELDS}

    return ViewBundle(dimensions=view["dimensions"], metadata=merged, chart=chart)


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
        view_diff = ViewDiff(dimensions=src.dimensions, is_new=target is None)

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

        diffs.append(view_diff)
    return diffs


def field_label(field_name: str) -> str:
    if field_name.startswith(CHART_FIELD_PREFIX):
        return CHART_FIELDS[field_name[len(CHART_FIELD_PREFIX) :]]
    return METADATA_FIELDS[field_name]


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


def diff_preview_html(view_diff: ViewDiff, max_fields: int = 3, max_chars: int = 280) -> str:
    """Compact HTML preview of a view's changes, used in the tree hover tooltips."""
    if view_diff.is_new:
        return '<p class="mdd-new">New view — it does not exist in production.</p>'
    if not view_diff.fields:
        return "<p>No changes.</p>"

    blocks = []
    for field_name, change in list(view_diff.fields.items())[:max_fields]:
        old, new = change["old"], change["new"]
        if isinstance(old, list) or isinstance(new, list):
            old_list = old if isinstance(old, list) else [old]
            new_list = new if isinstance(new, list) else [new]
            bullet_bits = []
            for i in range(max(len(old_list), len(new_list))):
                o = old_list[i] if i < len(old_list) else ""
                n = new_list[i] if i < len(new_list) else ""
                if _normalize(o) != _normalize(n):
                    bullet_bits.append(
                        f"<li>{_truncate_html(inline_diff_html(str(o), str(n)), max_chars)}</li>"
                    )
            body = f'<ul class="mdd-bullets">{"".join(bullet_bits)}</ul>'
        else:
            body = _truncate_html(inline_diff_html(str(old), str(new)), max_chars)
        blocks.append(f'<div class="mdd-field"><b>{html.escape(field_label(field_name))}</b>{body}</div>')

    hidden = len(view_diff.fields) - max_fields
    if hidden > 0:
        blocks.append(f"<p>… and {hidden} more field{'s' if hidden > 1 else ''}.</p>")
    return "".join(blocks)


def _truncate_html(rendered: str, max_chars: int) -> str:
    """Truncate rendered diff HTML without breaking tags (crude but safe: cut on text length)."""
    if len(rendered) <= max_chars:
        return rendered
    # Cut at a tag boundary to avoid splitting an HTML tag in half.
    cut = rendered.rfind("<", 0, max_chars)
    safe = rendered[: cut if cut > 0 else max_chars]
    # Close any dangling del/ins so the tooltip doesn't bleed styling.
    for tag in ("del", "ins"):
        if safe.count(f"<{tag}") > safe.count(f"</{tag}>"):
            safe += f"</{tag}>"
    return safe + "…"
