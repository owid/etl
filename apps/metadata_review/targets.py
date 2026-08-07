"""Dataclasses describing what can be reviewed in the Metadata Review tool.

A `ReviewableField` is one commentable unit of user-facing text (a chart title,
a dropdown label, a description_short, ...) with its provenance resolved, so a
reviewer sees where the text comes from and the implementer knows where to edit.
"""

from dataclasses import dataclass, field
from typing import Literal

Provenance = Literal["override", "inherited", "missing"]
TargetType = Literal["mdim", "indicator"]

# Canonical fieldPath -> human label shown in the UI.
FIELD_LABELS = {
    # MDim page level.
    "title.title": "Title",
    "title.title_variant": "Title variant",
    # MDim view level.
    "config.title": "Title",
    "config.subtitle": "Subtitle",
    "config.note": "Footnote",
    "metadata.description_short": "Description (short)",
    "metadata.description_key": "Key information",
    # Indicator level (data pages).
    "grapher_config.title": "Title",
    "grapher_config.subtitle": "Subtitle",
    "grapher_config.note": "Footnote",
    "description_short": "Description (short)",
    "description_key": "Key information",
}

# Bullet-list fields: tracked/diffed bullet-by-bullet, shared by bullet transfer
# (never by dimension-word pattern — word substitution is meaningless for lists).
DESCRIPTION_KEY_FIELDS = {"metadata.description_key", "description_key"}

# View-level fieldPath -> the indicator-level fieldPath it inherits from.
VIEW_TO_INDICATOR_FIELD = {
    "config.title": "grapher_config.title",
    "config.subtitle": "grapher_config.subtitle",
    "config.note": "grapher_config.note",
    "metadata.description_short": "description_short",
    "metadata.description_key": "description_key",
}


@dataclass
class ReviewableField:
    """One commentable field with resolved provenance."""

    target_type: TargetType
    # MDim catalogPath (as stored in multi_dim_data_pages) or variable catalogPath.
    target_path: str
    # Normalized dimensionsToViewId string; None for page-level and indicator fields.
    view_id: str | None
    field_path: str
    label: str
    provenance: Provenance
    # The value Grapher renders (override if set, else inherited; None when missing).
    current_value: str | None
    # What inheritance would give — shown alongside overridden values.
    inherited_value: str | None = None
    # Indicator catalogPath the field inherits from (primary y indicator for views).
    inherited_from: str | None = None
    # MDim configMd5 / variables.metadataChecksum at resolution time.
    page_checksum: str | None = None
    # Coarse "where to edit" text for the UI (the export CLI traces the exact location).
    edit_hint: str = ""
    preview_url: str | None = None

    def source_key(self) -> tuple[str, str, str | None, str]:
        """The persistence key a suggestion on this field uses.

        Suggestions attach to the *underlying parameter*, not the page where they
        were filed: view-level fields that are not overridden (inherited or missing
        with a known source indicator) key to the source indicator, so the same
        thread surfaces on every view and data page rendering that text.
        """
        if (
            self.target_type == "mdim"
            and self.view_id is not None
            and self.provenance != "override"
            and self.inherited_from is not None
            and self.field_path in VIEW_TO_INDICATOR_FIELD
        ):
            return ("indicator", self.inherited_from, None, VIEW_TO_INDICATOR_FIELD[self.field_path])
        return (self.target_type, self.target_path, self.view_id, self.field_path)


@dataclass
class DimensionChoice:
    slug: str
    name: str
    description: str | None = None
    group: str | None = None


@dataclass
class DimensionInfo:
    slug: str
    name: str
    choices: list[DimensionChoice] = field(default_factory=list)

    def choice_name(self, choice_slug: str) -> str:
        for choice in self.choices:
            if choice.slug == choice_slug:
                return choice.name
        return choice_slug


@dataclass
class ViewReview:
    """One MDim view with its resolved FAUST fields."""

    view_id: str
    # Dimension selection, e.g. {"indicator": "deaths", "estimate": "best"}.
    dimensions: dict[str, str]
    # Primary y indicator catalogPath (inheritance source), if resolvable.
    indicator_path: str | None
    fields: list[ReviewableField] = field(default_factory=list)


@dataclass
class MdimReview:
    """A fully resolved MDim page: page-level fields, dropdown labels, and views."""

    target_path: str
    slug: str | None
    title: str | None
    title_variant: str | None
    page_checksum: str | None
    dimensions: list[DimensionInfo] = field(default_factory=list)
    # title/title_variant + dimension/choice label fields.
    page_fields: list[ReviewableField] = field(default_factory=list)
    views: list[ViewReview] = field(default_factory=list)

    @property
    def all_fields(self) -> list[ReviewableField]:
        return self.page_fields + [f for view in self.views for f in view.fields]

    @property
    def indicator_paths(self) -> list[str]:
        """All source indicator catalogPaths used by views (deduplicated, ordered)."""
        seen: dict[str, None] = {}
        for view in self.views:
            if view.indicator_path:
                seen.setdefault(view.indicator_path, None)
        return list(seen)

    def human_dimensions(self, dims: dict[str, str]) -> dict[str, str]:
        """Map a view's {dim_slug: choice_slug} to {dim name: choice name}."""
        by_slug = {d.slug: d for d in self.dimensions}
        out = {}
        for dim_slug, choice_slug in dims.items():
            dim = by_slug.get(dim_slug)
            if dim is None:
                out[dim_slug] = choice_slug
            else:
                out[dim.name] = dim.choice_name(choice_slug)
        return out


@dataclass
class IndicatorReview:
    """One indicator (data page) with its resolved fields — inherited/missing only."""

    catalog_path: str
    variable_id: int | None
    name: str | None
    metadata_checksum: str | None
    fields: list[ReviewableField] = field(default_factory=list)


@dataclass
class DatasetReview:
    """A grapher dataset: one IndicatorReview per variable."""

    # datasets.catalogPath form: "namespace/version/dataset" (no channel prefix).
    dataset_catalog_path: str
    name: str | None
    indicators: list[IndicatorReview] = field(default_factory=list)

    @property
    def indicator_paths(self) -> list[str]:
        return [ind.catalog_path for ind in self.indicators]
