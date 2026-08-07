"""Typed content bundles: the unit of exchange between the gatherer, the lint pass, and the
inspector skill.

A bundle describes one public-facing content object (a chart, an MDim, an explorer, or a post)
with every user-visible text field, each tagged with its origin (which config or indicator it
comes from) and a fix location (where an editor would go to correct it). Texts shared across
views are factored out (indicator metadata into the ``indicators`` legend, view fields identical
across all views into ``shared_view_fields``) so each distinct text appears once.
"""

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# Content kinds.
KIND_CHART = "chart"
KIND_MULTIDIM = "multidim"
KIND_EXPLORER = "explorer"
KIND_POST = "post"
KINDS = [KIND_CHART, KIND_MULTIDIM, KIND_EXPLORER, KIND_POST]

# Finding categories (used by both the lint pass and the agent).
CATEGORIES = [
    "typo",
    "grammar",
    "semantic-mismatch",
    "unit-mismatch",
    "nonsense-combination",
    "stale-text",
    "formatting-artifact",
    "style",
]

SEVERITIES = ["high", "medium", "low"]


@dataclass
class TextField:
    """One user-visible piece of text, with provenance."""

    # Field name, e.g. "title", "description_key[2]", "dimensions[0].display.name".
    name: str
    text: str
    # Where the text comes from: "chart_config" | "indicator" | "collection_config" | "markdown".
    origin: str
    # Stable id of the source, e.g. "chart:1234", "variable:5678", "collection:energy", "post:<gdocId>".
    origin_id: str
    # Where an editor would fix it: admin URL, ETL catalogPath, or gdoc edit link.
    fix_location: str


@dataclass
class Indicator:
    """Legend entry: one indicator's user-visible metadata, listed once per bundle regardless of
    how many views use it."""

    variable_id: int
    catalog_path: str | None
    fields: list[TextField]


@dataclass
class View:
    """One rendered view (a chart has exactly one; MDims/explorers have many)."""

    # Chart slug, MDim viewId (e.g. "gas__total"), or explorer_views row id.
    view_id: str
    # Dimension choices that produce this view (empty for charts).
    dimensions: dict[str, str]
    url: str
    # View-specific rendered fields (fields identical across all views are hoisted to
    # ``ContentBundle.shared_view_fields``).
    fields: list[TextField]
    # References into ``ContentBundle.indicators`` by variable_id; the primary (first y)
    # indicator comes first.
    indicator_ids: list[int]
    # What Grapher displays as title/subtitle: the view's own config fields when set, otherwise
    # resolved from the primary indicator (titlePublic/display.name/name, descriptionShort).
    # Plain strings (not TextFields) so shared indicator text isn't double-counted; findings on
    # them attach to the view's title field or to the primary indicator's metadata.
    rendered_title: str = ""
    rendered_subtitle: str = ""


@dataclass
class EmbeddedChart:
    """A chart referenced inside a post, with just enough text to judge the surrounding prose."""

    slug: str
    url: str
    title: str
    subtitle: str


@dataclass
class ContentBundle:
    kind: str
    slug: str
    url: str
    fix_location: str
    # Collection-level texts (MDim/explorer config: names of dimensions and choices, etc.).
    config_fields: list[TextField] = field(default_factory=list)
    # Fields identical across every view, listed once.
    shared_view_fields: list[TextField] = field(default_factory=list)
    views: list[View] = field(default_factory=list)
    indicators: list[Indicator] = field(default_factory=list)
    # Posts only.
    markdown: str | None = None
    embedded_charts: list[EmbeddedChart] = field(default_factory=list)
    # Hash over every text in the bundle; used to skip unchanged content and to expire dismissals.
    content_hash: str = ""

    def all_fields(self) -> list[TextField]:
        """Every text field in the bundle, in a stable order."""
        fields = list(self.config_fields) + list(self.shared_view_fields)
        for view in self.views:
            fields.extend(view.fields)
        for indicator in self.indicators:
            fields.extend(indicator.fields)
        if self.markdown:
            fields.append(
                TextField(
                    name="markdown",
                    text=self.markdown,
                    origin="markdown",
                    origin_id=f"post:{self.slug}",
                    fix_location=self.fix_location,
                )
            )
        return fields

    def compute_content_hash(self) -> str:
        payload = json.dumps(
            [(f.origin_id, f.name, f.text) for f in self.all_fields()],
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ContentBundle":
        d = dict(d)
        d["config_fields"] = [TextField(**f) for f in d.get("config_fields", [])]
        d["shared_view_fields"] = [TextField(**f) for f in d.get("shared_view_fields", [])]
        d["views"] = [
            View(**{**v, "fields": [TextField(**f) for f in v.get("fields", [])]}) for v in d.get("views", [])
        ]
        d["indicators"] = [
            Indicator(**{**i, "fields": [TextField(**f) for f in i.get("fields", [])]}) for i in d.get("indicators", [])
        ]
        d["embedded_charts"] = [EmbeddedChart(**e) for e in d.get("embedded_charts", [])]
        return cls(**d)

    def save(self, path: Path) -> None:
        if not self.content_hash:
            self.content_hash = self.compute_content_hash()
        path.write_text(json.dumps(self.to_dict(), indent=1, ensure_ascii=False))

    @classmethod
    def load(cls, path: Path) -> "ContentBundle":
        return cls.from_dict(json.loads(path.read_text()))
