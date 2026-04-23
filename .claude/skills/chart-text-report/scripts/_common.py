"""Shared helpers for the chart-text-report skill.

Used by both `generate_mdim_text_report.py` (MDim view mode) and
`grapher_dataset_mode.py` (dataset / indicator-list mode).

Critical inheritance rule captured here:
  - Chart Title / Subtitle / Footnote resolve ONLY from presentation.grapher_config.{title,subtitle,note}
  - description_short / description_key resolve from the top-level VariableMeta fields
  - Never fall back to title / title_public / display.name / description_short for chart-level FAUST —
    those are the data-page fields and produce text that does not match what Grapher renders.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote, urlencode

from owid.catalog import Dataset, Table
from owid.catalog.meta import VariableMeta

from etl.paths import DATA_DIR

ADMIN_BASE = "https://admin.owid.io/admin/grapher"

# Cache of loaded grapher-channel tables keyed by (namespace, version, dataset_name, table_name).
_TABLE_CACHE: dict[tuple[str, str, str, str], Table] = {}


# ---------------------------------------------------------------------------
# Catalog-path parsing and metadata loading
# ---------------------------------------------------------------------------


def parse_catalog_path(catalog_path: str) -> tuple[str, str, str, str, str, str]:
    """Parse 'grapher/<ns>/<ver>/<ds>/<table>#<col>' into (channel, ns, ver, ds, table, col)."""
    path_part, col = catalog_path.split("#", 1)
    parts = path_part.split("/")
    if len(parts) != 5:
        raise ValueError(f"Unexpected catalog path shape: {catalog_path}")
    channel, namespace, version, dataset_name, table_name = parts
    return channel, namespace, version, dataset_name, table_name, col


def load_grapher_table(namespace: str, version: str, dataset_name: str, table_name: str) -> Table:
    """Load a grapher-channel table (cached)."""
    key = (namespace, version, dataset_name, table_name)
    if key not in _TABLE_CACHE:
        ds_path = DATA_DIR / "grapher" / namespace / version / dataset_name
        ds = Dataset(ds_path)
        _TABLE_CACHE[key] = ds.read(table_name, safe_types=False)
    return _TABLE_CACHE[key]


def get_indicator_meta(catalog_path: str) -> VariableMeta | None:
    """Return the VariableMeta for the variable referenced by a grapher catalogPath.

    Loads from the GRAPHER channel because grapher flattens multi-dimensional
    indicators into one column per dimension combination and renders Jinja templates
    against the specific dimension values — matching what Grapher actually shows.

    Returns `None` only when the column legitimately isn't in the table (i.e. the
    specific indicator is absent). Other failures — malformed path, wrong channel,
    missing dataset, unreadable feather — propagate as exceptions so the caller
    doesn't silently audit the wrong state.
    """
    channel, ns, ver, ds_name, table_name, col = parse_catalog_path(catalog_path)
    if channel != "grapher":
        raise ValueError(
            f"Only 'grapher/...' catalog paths are supported; got channel '{channel}' "
            f"in {catalog_path}. Metadata must come from the grapher channel so Jinja "
            f"templates are rendered against the correct dimension values."
        )
    tb = load_grapher_table(ns, ver, ds_name, table_name)
    if col not in tb.columns:
        # Legit "this indicator is absent" case — the report will tag it [missing].
        print(f"  ! Column '{col}' not found in grapher table {ns}/{ver}/{ds_name}/{table_name}")
        return None
    return tb[col].metadata


# ---------------------------------------------------------------------------
# Inheritance resolvers
# ---------------------------------------------------------------------------


def _grapher_config(meta: VariableMeta | None) -> dict[str, Any]:
    if meta is None:
        return {}
    pres = getattr(meta, "presentation", None)
    if pres is None:
        return {}
    gc = getattr(pres, "grapher_config", None) or {}
    return gc if isinstance(gc, dict) else {}


def inherited_title(meta: VariableMeta | None) -> str | None:
    """Chart title: presentation.grapher_config.title only."""
    return _grapher_config(meta).get("title")


def inherited_subtitle(meta: VariableMeta | None) -> str | None:
    """Chart subtitle: presentation.grapher_config.subtitle only."""
    return _grapher_config(meta).get("subtitle")


def inherited_note(meta: VariableMeta | None) -> str | None:
    """Chart footnote: presentation.grapher_config.note only."""
    return _grapher_config(meta).get("note")


def resolve_field(view_value: Any, inherited_value: Any) -> tuple[str, Any]:
    """Pick override if set, else inherited. Return (source, value).

    An explicit empty string (e.g. `note: ""` in an MDim view to suppress an
    inherited footnote) counts as an override — Grapher renders that as empty,
    and the audit should show the same. Only `None` means "not set at this layer".
    """
    if view_value is not None:
        return "override", view_value
    if inherited_value is not None:
        return "inherited", inherited_value
    return "missing", None


# ---------------------------------------------------------------------------
# Markdown rendering helpers
# ---------------------------------------------------------------------------


def md_escape(s: str) -> str:
    return s.replace("\n", " ").strip()


def render_value(source: str, value: Any) -> str:
    tag = f"[{source}]"
    if value is None:
        return f"{tag} _—_"
    if isinstance(value, list):
        if not value:
            return f"{tag} _(empty list)_"
        items = [f"  - {md_escape(str(item))}" for item in value]
        return f"{tag}\n" + "\n".join(items)
    if isinstance(value, str) and value == "":
        # Explicit empty-string override — Grapher renders this as blank, so surface
        # it in the report rather than leaving an ambiguous empty line.
        return f"{tag} _(empty)_"
    return f"{tag} {md_escape(str(value))}"


def preview_url(catalog_path: str, dimensions: dict[str, str] | None = None) -> str:
    """Admin preview URL. `catalog_path` like 'wb/latest/incomes_pip#incomes_pip'.
    Dimensions are appended as query string if provided."""
    encoded_path = quote(catalog_path, safe="")
    base = f"{ADMIN_BASE}/{encoded_path}"
    if not dimensions:
        return base
    return f"{base}?{urlencode(dimensions)}"


# ---------------------------------------------------------------------------
# Description-key bullet library (dedup + auto-slug)
# ---------------------------------------------------------------------------


# Common stopwords plus a few domain-specific ones. Used to skip through filler
# words when auto-generating short slugs for description_key bullets.
_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "being", "but", "by",
    "can", "could", "do", "does", "doing", "done", "each", "either", "for",
    "from", "has", "have", "having", "how", "if", "in", "into", "is", "it",
    "its", "may", "might", "more", "most", "much", "neither", "no", "nor",
    "not", "of", "on", "or", "our", "out", "over", "per", "so", "some", "such",
    "than", "that", "the", "their", "them", "there", "these", "they", "this",
    "those", "through", "to", "up", "was", "we", "were", "what", "when",
    "where", "which", "while", "who", "whose", "why", "will", "with", "within",
    "would", "year", "years",
    # domain-specific filler that tends to start bullets
    "data", "chart", "depending", "country", "countries", "many", "people",
    "today", "past", "also", "used", "use",
}  # fmt: skip

# Optional manual overrides keyed by the first ~80 chars of the bullet text (md-escaped).
# Each entry maps a prefix → desired short slug. If no prefix matches, we auto-generate.
BULLET_SLUG_OVERRIDES: dict[str, str] = {}


def _auto_slug(text: str, max_words: int = 3) -> str:
    no_links = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    cleaned = re.sub(r"[^\w\s-]", " ", no_links)
    # Treat underscores as whitespace so Markdown italic runs like `_pre-tax_` split
    # cleanly into `pre-tax` instead of producing slugs like `_pre-tax_`.
    cleaned = cleaned.replace("_", " ")
    tokens = [t.lower() for t in cleaned.split() if t]

    picks: list[str] = []
    for tok in tokens:
        if len(picks) >= max_words:
            break
        if tok in _STOPWORDS:
            continue
        if len(tok) <= 2:
            continue
        picks.append(tok)
    return "-".join(picks) if picks else "bullet"


def _disambiguate(slug: str, existing: set[str]) -> str:
    if slug not in existing:
        return slug
    i = 2
    while f"{slug}-{i}" in existing:
        i += 1
    return f"{slug}-{i}"


def _bullet_slug(text: str, existing: set[str]) -> str:
    for prefix, slug in BULLET_SLUG_OVERRIDES.items():
        if text.startswith(prefix):
            return _disambiguate(slug, existing)
    return _disambiguate(_auto_slug(text), existing)


class BulletLibrary:
    """Collects unique description_key bullets and assigns short slugs."""

    def __init__(self) -> None:
        self._bullets: list[str] = []
        self._slugs: list[str] = []
        self._index: dict[str, int] = {}
        self._slug_set: set[str] = set()

    def register(self, bullets: list[str]) -> list[str]:
        ids = []
        for b in bullets:
            text = md_escape(str(b))
            if text not in self._index:
                slug = _bullet_slug(text, self._slug_set)
                self._index[text] = len(self._bullets)
                self._bullets.append(text)
                self._slugs.append(slug)
                self._slug_set.add(slug)
            ids.append(self._slugs[self._index[text]])
        return ids

    def legend_lines(self) -> list[str]:
        if not self._bullets:
            return []
        lines = ["## Description-key bullet legend", ""]
        for slug, text in zip(self._slugs, self._bullets):
            lines.append(f"- **{slug}** — {text}")
        lines.append("")
        lines.append("---")
        lines.append("")
        return lines


# ---------------------------------------------------------------------------
# Standard markdown header blocks reused by all modes
# ---------------------------------------------------------------------------


def how_to_read_block(has_overrides: bool) -> list[str]:
    """Rendered `## How to read this file` explanation.

    `has_overrides=True` for the MDim mode (tags include [override]); False for
    dataset mode where the only sources are [inherited] and [missing]."""
    lines = ["## How to read this file", ""]
    lines.append("Every field is tagged by where its text came from:")
    lines.append("")
    if has_overrides:
        lines.append(
            "- **[override]** — the text is set explicitly on this view in the MDim config "
            "(`.config.yml` or programmatically in the step's `.py`). Takes precedence."
        )
    lines.append(
        "- **[inherited]** — the value comes from the indicator's ETL metadata. "
        "For Title/Subtitle/Footnote the source is `presentation.grapher_config.{title,subtitle,note}`; "
        "for description_short/description_key the source is the namesake field on the indicator."
    )
    lines.append(
        "- **[missing]** — neither the view nor the indicator defines the field. The chart will "
        "render without it (or with an admin-DB value that this report cannot see)."
    )
    lines.append("")
    lines.append(
        "Description-key bullets are deduplicated across the file; each unique bullet is "
        "assigned a short slug. See the legend directly below for the full text."
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines
