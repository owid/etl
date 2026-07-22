"""DB-based resolution of reviewable fields with provenance.

Reads the enriched MDim config from `multi_dim_data_pages` and indicator metadata
from `variables` + `chart_configs.patch` (via `grapherConfigIdETL`), so it works
against any grapher database (staging or production) without a local ETL catalog.

Inheritance rules ported from the faust-metadata-audit skill
(.claude/skills/faust-metadata-audit/scripts/_common.py):

- Chart Title / Subtitle / Footnote inherit ONLY from
  `presentation.grapher_config.{title,subtitle,note}` — in DB terms, the
  `chart_configs.patch` of the variable's `grapherConfigIdETL`. Never fall back
  to `name` / `titlePublic` / `display.name`; those are data-page fields that do
  not match what Grapher renders.
- description_short / description_key inherit from the namesake `variables` columns.
- An explicit empty-string override counts as an override (Grapher renders blank).
"""

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode

import structlog
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from apps.metadata_review.diffs import apply_bullet_edits
from apps.metadata_review.targets import (
    DESCRIPTION_KEY_FIELDS,
    FIELD_LABELS,
    VIEW_TO_INDICATOR_FIELD,
    DatasetReview,
    DimensionChoice,
    DimensionInfo,
    IndicatorReview,
    MdimReview,
    Provenance,
    ReviewableField,
    ViewReview,
)
from etl.config import OWID_ENV, OWIDEnv

log = structlog.get_logger()

# (view config key, view metadata key) -> canonical view fieldPath, in display order.
VIEW_FAUST_FIELDS = [
    ("config.title", ("config", "title")),
    ("config.subtitle", ("config", "subtitle")),
    ("config.note", ("config", "note")),
    ("metadata.description_short", ("metadata", "descriptionShort")),
    ("metadata.description_key", ("metadata", "descriptionKey")),
]

# Indicator fieldPath -> where the inherited value lives.
INDICATOR_FIELDS = [
    "grapher_config.title",
    "grapher_config.subtitle",
    "grapher_config.note",
    "description_short",
    "description_key",
]


# ---------------------------------------------------------------------------
# View ids (port of owid-grapher's dimensionsToViewId + slugify, Util.ts)
# ---------------------------------------------------------------------------

_SUB_SUPERSCRIPTS = str.maketrans("₀₁₂₃₄₅₆₇₈₉⁰¹²³⁴⁵⁶⁷⁸⁹", "01234567890123456789")


def slugify(s: str) -> str:
    """Port of owid-grapher's `slugify` (Util.ts). Identity for snake_case slugs."""
    s = s.translate(_SUB_SUPERSCRIPTS).lower().strip()
    s = re.sub(r"\s*\*.+\*", "", s)
    s = re.sub(r"[^\w\- /]+", "", s)
    s = re.sub(r" +", "-", s)
    return s.replace("/", "")


def dimensions_to_view_id(dimensions: dict[str, str]) -> str:
    """Port of owid-grapher's `dimensionsToViewId` (Util.ts): the stable,
    environment-agnostic key of an MDim view."""
    return "__".join(f"{slugify(k)}={slugify(str(v))}" for k, v in sorted(dimensions.items())).lower()


# ---------------------------------------------------------------------------
# Field resolution (ported from faust-metadata-audit _common.py)
# ---------------------------------------------------------------------------


def resolve_field(view_value: Any, inherited_value: Any) -> tuple[Provenance, Any]:
    """Pick override if set, else inherited. Return (provenance, value).

    An explicit empty string (e.g. `note: ""` in an MDim view to suppress an
    inherited footnote) counts as an override — Grapher renders that as empty.
    Only `None` means "not set at this layer".
    """
    if view_value is not None:
        return "override", view_value
    if inherited_value is not None:
        return "inherited", inherited_value
    return "missing", None


@dataclass
class IndicatorMeta:
    """The inheritance-relevant metadata of one variable, loaded from the DB."""

    catalog_path: str
    variable_id: int
    name: str | None
    description_short: str | None
    description_key: str | None
    metadata_checksum: str | None
    # The ETL-authored grapher config (chart_configs.patch), {} when absent.
    grapher_config: dict[str, Any]

    def inherited(self, indicator_field_path: str) -> str | None:
        if indicator_field_path == "description_short":
            return self.description_short
        if indicator_field_path == "description_key":
            return self.description_key
        assert indicator_field_path.startswith("grapher_config.")
        return self.grapher_config.get(indicator_field_path.removeprefix("grapher_config."))


def load_indicator_metas(
    session: Session,
    catalog_paths: list[str] | None = None,
    variable_ids: list[int] | None = None,
) -> dict[str, IndicatorMeta]:
    """Bulk-load IndicatorMeta for catalog paths and/or variable ids, keyed by catalog path."""
    v, cc = gm.Variable, gm.ChartConfig
    conditions = []
    if catalog_paths:
        conditions.append(v.catalogPath.in_(catalog_paths))
    if variable_ids:
        conditions.append(v.id.in_(variable_ids))
    if not conditions:
        return {}

    rows = session.execute(
        select(
            v.id,
            v.catalogPath,
            v.name,
            v.descriptionShort,
            v.descriptionKey,
            v.metadataChecksum,
            cc.patch,
        )
        .outerjoin(cc, cc.id == v.grapherConfigIdETL)
        .where(or_(*conditions))
    ).all()

    metas = {}
    for var_id, catalog_path, name, description_short, description_key, metadata_checksum, patch in rows:
        if catalog_path is None:
            continue
        metas[catalog_path] = IndicatorMeta(
            catalog_path=catalog_path,
            variable_id=var_id,
            name=name,
            description_short=description_short,
            description_key=description_key,
            metadata_checksum=metadata_checksum,
            grapher_config=patch if isinstance(patch, dict) else {},
        )
    return metas


# ---------------------------------------------------------------------------
# Edit hints (coarse; the export CLI traces the exact file/key)
# ---------------------------------------------------------------------------


def _edit_hint(provenance: Provenance, inherited_from: str | None) -> str:
    if provenance == "override":
        return "Set in the MDim config (override)."
    if provenance == "inherited":
        return f"Inherited from `{inherited_from}` — edited in the garden step metadata."
    if inherited_from:
        return "Not set — usually added in the garden step metadata (preferred) or as an MDim override."
    return "Not set."


MDIM_CONFIG_HINT = "Set in the MDim config."


# ---------------------------------------------------------------------------
# MDim resolution
# ---------------------------------------------------------------------------


def _primary_indicator(view: dict[str, Any]) -> tuple[str | None, int | None]:
    """Return (catalogPath, variable_id) of the first y indicator of a view.

    All inheritance resolves from y[0] — same convention as the faust-metadata-audit
    skill. Entries may be strings (catalog paths) or dicts with catalogPath and/or id.
    """
    y = (view.get("indicators") or {}).get("y") or []
    if not y:
        return None, None
    first = y[0]
    if isinstance(first, str):
        return first, None
    return first.get("catalogPath"), first.get("id")


def _mdim_preview_url(owid_env: OWIDEnv, catalog_path: str, dimensions: dict[str, str] | None = None) -> str:
    url = f"{owid_env.admin_site}/grapher/{quote(catalog_path, safe='')}"
    if dimensions:
        url = f"{url}?{urlencode(dimensions)}"
    return url


def resolve_mdim(session: Session, catalog_path: str, owid_env: OWIDEnv | None = None) -> MdimReview:
    """Resolve every reviewable field of an MDim from its enriched DB config."""
    owid_env = owid_env or OWID_ENV
    mdim = gm.MultiDimDataPage.load_mdim(session, catalogPath=catalog_path)
    if mdim is None:
        raise ValueError(f"MDim not found in multi_dim_data_pages: {catalog_path}")
    config = mdim.config
    page_checksum = mdim.configMd5

    review = MdimReview(
        target_path=catalog_path,
        slug=mdim.slug,
        title=(config.get("title") or {}).get("title"),
        title_variant=(config.get("title") or {}).get("titleVariant"),
        page_checksum=page_checksum,
    )

    def page_field(field_path: str, value: str | None, provenance: Provenance = "override") -> ReviewableField:
        return ReviewableField(
            target_type="mdim",
            target_path=catalog_path,
            view_id=None,
            field_path=field_path,
            label=FIELD_LABELS.get(field_path, field_path),
            provenance=provenance,
            current_value=value,
            edit_hint=MDIM_CONFIG_HINT,
            page_checksum=page_checksum,
            preview_url=_mdim_preview_url(owid_env, catalog_path),
        )

    # Page-level title fields.
    review.page_fields.append(page_field("title.title", review.title))
    review.page_fields.append(
        page_field("title.title_variant", review.title_variant, "override" if review.title_variant else "missing")
    )

    # Dropdown labels: dimension names and choice names/descriptions.
    for dim in config.get("dimensions") or []:
        dim_info = DimensionInfo(slug=dim["slug"], name=dim.get("name") or dim["slug"])
        review.page_fields.append(page_field(f"dimensions.{dim_info.slug}.name", dim.get("name")))
        for choice in dim.get("choices") or []:
            dim_info.choices.append(
                DimensionChoice(
                    slug=choice["slug"],
                    name=choice.get("name") or choice["slug"],
                    description=choice.get("description"),
                    group=choice.get("group"),
                )
            )
            base = f"dimensions.{dim_info.slug}.choices.{choice['slug']}"
            review.page_fields.append(page_field(f"{base}.name", choice.get("name")))
            review.page_fields.append(
                page_field(
                    f"{base}.description",
                    choice.get("description"),
                    "override" if choice.get("description") is not None else "missing",
                )
            )
        review.dimensions.append(dim_info)

    for field in review.page_fields:
        if field.field_path.startswith("dimensions."):
            field.label = _dropdown_label(review, field.field_path)

    # Views. First pass: collect inheritance sources so we can bulk-load them.
    views = config.get("views") or []
    paths_needed = []
    ids_needed = []
    for view in views:
        path, var_id = _primary_indicator(view)
        if path:
            paths_needed.append(path)
        elif var_id:
            ids_needed.append(var_id)
    metas = load_indicator_metas(session, catalog_paths=paths_needed, variable_ids=ids_needed)
    metas_by_id = {meta.variable_id: meta for meta in metas.values()}

    for view in views:
        dims = view.get("dimensions") or {}
        view_id = dimensions_to_view_id(dims)
        path, var_id = _primary_indicator(view)
        meta = metas.get(path) if path else None
        if meta is None and var_id is not None:
            meta = metas_by_id.get(var_id)
        if meta is None:
            log.warning("metadata_review.unresolved_indicator", mdim=catalog_path, view=view_id, path=path, id=var_id)

        view_review = ViewReview(
            view_id=view_id,
            dimensions=dims,
            indicator_path=meta.catalog_path if meta else path,
        )
        overrides = {"config": view.get("config") or {}, "metadata": view.get("metadata") or {}}
        for field_path, (section, key) in VIEW_FAUST_FIELDS:
            override_value = overrides[section].get(key)
            inherited_value = meta.inherited(VIEW_TO_INDICATOR_FIELD[field_path]) if meta else None
            provenance, value = resolve_field(override_value, inherited_value)
            view_review.fields.append(
                ReviewableField(
                    target_type="mdim",
                    target_path=catalog_path,
                    view_id=view_id,
                    field_path=field_path,
                    label=FIELD_LABELS[field_path],
                    provenance=provenance,
                    current_value=value,
                    inherited_value=inherited_value,
                    inherited_from=view_review.indicator_path,
                    page_checksum=page_checksum,
                    edit_hint=_edit_hint(provenance, view_review.indicator_path),
                    preview_url=_mdim_preview_url(owid_env, catalog_path, dims),
                )
            )
        review.views.append(view_review)

    return review


def _dropdown_label(review: MdimReview, field_path: str) -> str:
    """Human label for a dropdown-label field, e.g. 'Metric ▸ Total number — name'."""
    parts = field_path.split(".")
    dim = next((d for d in review.dimensions if d.slug == parts[1]), None)
    dim_name = dim.name if dim else parts[1]
    if len(parts) == 3:  # dimensions.<dim>.name
        return f"Dropdown “{dim_name}” — name"
    choice_slug = parts[3]
    choice_name = dim.choice_name(choice_slug) if dim else choice_slug
    what = "name" if parts[4] == "name" else "description"
    return f"Dropdown “{dim_name}” ▸ “{choice_name}” — {what}"


# ---------------------------------------------------------------------------
# Indicator / dataset resolution (data pages)
# ---------------------------------------------------------------------------


def resolve_indicator_fields(meta: IndicatorMeta, owid_env: OWIDEnv | None = None) -> list[ReviewableField]:
    """Resolve the five indicator-level fields (inherited/missing only — no views here)."""
    owid_env = owid_env or OWID_ENV
    fields = []
    for field_path in INDICATOR_FIELDS:
        inherited_value = meta.inherited(field_path)
        provenance, value = resolve_field(None, inherited_value)
        fields.append(
            ReviewableField(
                target_type="indicator",
                target_path=meta.catalog_path,
                view_id=None,
                field_path=field_path,
                label=FIELD_LABELS[field_path],
                provenance=provenance,
                current_value=value,
                inherited_value=inherited_value,
                inherited_from=meta.catalog_path,
                page_checksum=meta.metadata_checksum,
                edit_hint=_edit_hint(provenance, meta.catalog_path),
                preview_url=owid_env.indicator_admin_site(meta.variable_id),
            )
        )
    return fields


def resolve_dataset(session: Session, dataset_catalog_path: str, owid_env: OWIDEnv | None = None) -> DatasetReview:
    """Resolve all indicators of a grapher dataset (datasets.catalogPath = 'ns/version/dataset')."""
    owid_env = owid_env or OWID_ENV
    dataset = session.scalars(select(gm.Dataset).where(gm.Dataset.catalogPath == dataset_catalog_path)).one_or_none()
    if dataset is None:
        raise ValueError(f"Dataset not found in grapher DB: {dataset_catalog_path}")

    variable_ids = [
        row for row in session.scalars(select(gm.Variable.id).where(gm.Variable.datasetId == dataset.id)).all()
    ]
    metas = load_indicator_metas(session, variable_ids=variable_ids)

    review = DatasetReview(dataset_catalog_path=dataset_catalog_path, name=dataset.name)
    for meta in sorted(metas.values(), key=lambda m: m.catalog_path):
        review.indicators.append(
            IndicatorReview(
                catalog_path=meta.catalog_path,
                variable_id=meta.variable_id,
                name=meta.name,
                metadata_checksum=meta.metadata_checksum,
                fields=resolve_indicator_fields(meta, owid_env=owid_env),
            )
        )
    return review


# ---------------------------------------------------------------------------
# Listings
# ---------------------------------------------------------------------------


def list_mdims(session: Session) -> list[dict[str, Any]]:
    """All MDims with a catalogPath: [{catalog_path, slug, title, title_variant, published, updated_at}].

    Title fields are extracted server-side (JSON_EXTRACT) so the big config
    columns never leave the database.
    """
    rows = session.execute(
        select(
            gm.MultiDimDataPage.catalogPath,
            gm.MultiDimDataPage.slug,
            func.json_unquote(func.json_extract(gm.MultiDimDataPage.config, "$.title.title")),
            func.json_unquote(func.json_extract(gm.MultiDimDataPage.config, "$.title.titleVariant")),
            gm.MultiDimDataPage.published,
            gm.MultiDimDataPage.updatedAt,
        ).where(gm.MultiDimDataPage.catalogPath.is_not(None))
    ).all()
    return [
        {
            "catalog_path": catalog_path,
            "slug": slug,
            "title": title,
            "title_variant": title_variant,
            "published": bool(published),
            "updated_at": updated_at,
        }
        for catalog_path, slug, title, title_variant, published, updated_at in sorted(
            rows, key=lambda r: r[5], reverse=True
        )
    ]


def list_datasets(session: Session) -> list[dict[str, Any]]:
    """All non-archived grapher datasets with a catalogPath: [{catalog_path, name, updated_at}]."""
    rows = session.execute(
        select(gm.Dataset.catalogPath, gm.Dataset.name, gm.Dataset.updatedAt)
        .where(gm.Dataset.catalogPath.is_not(None))
        .where(gm.Dataset.isArchived == 0)
        .order_by(gm.Dataset.updatedAt.desc())
    ).all()
    return [
        {"catalog_path": catalog_path, "name": name, "updated_at": updated_at}
        for catalog_path, name, updated_at in rows
    ]


# ---------------------------------------------------------------------------
# Suggestions: source-keyed lookup, shared views, staleness
# ---------------------------------------------------------------------------


def suggestions_by_source_key(
    suggestions: list[gm.MetadataReviewSuggestion],
) -> dict[tuple[str, str, str | None, str], list[gm.MetadataReviewSuggestion]]:
    """Group persisted suggestions by their source key (targetType, targetPath, viewId, fieldPath)."""
    grouped: dict[tuple[str, str, str | None, str], list[gm.MetadataReviewSuggestion]] = {}
    for suggestion in suggestions:
        key = (suggestion.targetType, suggestion.targetPath, suggestion.viewId, suggestion.fieldPath)
        grouped.setdefault(key, []).append(suggestion)
    return grouped


def parametrize_value(
    review: MdimReview, view_dims: dict[str, str], value: str | None
) -> tuple[str, dict[str, str]] | None:
    """Replace the view's own dimension words in `value` with `{dim}` placeholders.

    Views of one MDim often render a single garden template whose output differs
    only by dimension-derived words ("Income share of the richest 1%" vs "...the
    richest 0.1%"). Substituting each dimension's surface form (the choice's human
    name or slug variants, longest candidate first, case-insensitive) yields the
    shared pattern; two views connect when their patterns match.

    Returns (pattern, {dim_slug: matched substring}) when at least one dimension
    word was found, else None.
    """
    if not value:
        return None
    dims_by_slug = {d.slug: d for d in review.dimensions}
    pattern = str(value)
    matches: dict[str, str] = {}
    for dim_slug, choice_slug in view_dims.items():
        dim = dims_by_slug.get(dim_slug)
        candidates = {str(choice_slug), str(choice_slug).replace("_", " "), str(choice_slug).replace("_", "-")}
        if dim is not None:
            candidates.add(dim.choice_name(str(choice_slug)))
        candidates |= {c.lstrip("_ -") for c in set(candidates)}
        for candidate in sorted({c.strip() for c in candidates if len(c.strip()) >= 2}, key=len, reverse=True):
            idx = pattern.lower().find(candidate.lower())
            if idx >= 0:
                matches[dim_slug] = pattern[idx : idx + len(candidate)]
                pattern = pattern[:idx] + "{" + dim_slug + "}" + pattern[idx + len(candidate) :]
                break
    if not matches:
        return None
    return pattern, matches


def _field_pattern(review: MdimReview, view: "ViewReview", field: ReviewableField) -> str | None:
    parametrized = parametrize_value(review, view.dimensions, _norm(field.current_value))
    return parametrized[0] if parametrized else None


def transfer_proposal(
    review: MdimReview,
    proposal: gm.MetadataReviewSuggestion,
    to_field: ReviewableField,
) -> str | None:
    """Re-render a pattern-shared proposal for another view.

    Substitutes the filing view's dimension words in the proposed text with the
    target view's — so a wording change filed on "richest 1%" displays with
    "richest 0.1%" on that view. Returns None when the proposal edited the
    dimension words themselves (no faithful transfer possible).
    """
    if proposal.suggestedValue is None or to_field.view_id is None:
        return None
    views_by_id = {v.view_id: v for v in review.views}
    filing_view = views_by_id.get(proposal.filedFromViewId or "")
    target_view = views_by_id.get(to_field.view_id)
    if filing_view is None or target_view is None:
        return None
    source = parametrize_value(review, filing_view.dimensions, _norm(proposal.currentValue))
    target = parametrize_value(review, target_view.dimensions, _norm(to_field.current_value))
    if source is None or target is None or source[0] != target[0]:
        return None
    transferred = str(proposal.suggestedValue)
    for dim_slug, source_word in source[1].items():
        target_word = target[1].get(dim_slug)
        if target_word is None:
            return None
        idx = transferred.lower().find(source_word.lower())
        if idx < 0:
            # The proposal changed the dimension word itself — don't transfer.
            return None
        transferred = transferred[:idx] + target_word + transferred[idx + len(source_word) :]
    return transferred


def shared_view_ids(review: MdimReview, field: ReviewableField) -> list[str]:
    """View ids (other than the field's own) rendering the same text for this field.

    Sharing is by identical rendered value, regardless of provenance or source
    indicator: MDims commonly fan one garden Jinja template out into a different
    expanded indicator per view, all rendering the same text — a suggestion on
    that text belongs to every one of those views. (Shared `common_views` /
    generated overrides likewise arrive in the DB config as per-view copies, so
    value equality is the detectable signal there too.)
    """
    if field.view_id is None or field.current_value is None:
        return []
    value = _norm(field.current_value)
    own_view = next((v for v in review.views if v.view_id == field.view_id), None)
    use_pattern = own_view is not None and field.field_path not in DESCRIPTION_KEY_FIELDS
    pattern = _field_pattern(review, own_view, field) if use_pattern else None
    shared = []
    for view in review.views:
        if view.view_id == field.view_id:
            continue
        for other in view.fields:
            if other.field_path != field.field_path:
                continue
            if _norm(other.current_value) == value or (
                pattern is not None and _field_pattern(review, view, other) == pattern
            ):
                shared.append(view.view_id)
    return shared


def threads_for_field(
    review: MdimReview | None,
    field: ReviewableField,
    suggestions_by_key: dict[tuple[str, str, str | None, str], list[gm.MetadataReviewSuggestion]],
) -> list[gm.MetadataReviewSuggestion]:
    """All suggestion threads that belong on this field's panel.

    Includes the field's own source-keyed threads plus threads filed on any other
    view's field (same field, identical rendered text or matching dimension
    pattern) — even when those views inherit from *different* expanded indicators
    of the same garden template — plus text-matched threads keyed to indicators of
    sibling pages (e.g. another MDim built from the same garden metadata).
    Deduplicated by suggestion id, oldest first. `review` may be None (dataset mode).
    """
    keys = {field.source_key()}
    if review is not None and field.view_id is not None and field.current_value is not None:
        value = _norm(field.current_value)
        own_view = next((v for v in review.views if v.view_id == field.view_id), None)
        use_pattern = own_view is not None and field.field_path not in DESCRIPTION_KEY_FIELDS
        pattern = _field_pattern(review, own_view, field) if use_pattern else None
        for view in review.views:
            for other in view.fields:
                if other.field_path != field.field_path:
                    continue
                if _norm(other.current_value) == value or (
                    pattern is not None and _field_pattern(review, view, other) == pattern
                ):
                    keys.add(other.source_key())
    seen: dict[int, gm.MetadataReviewSuggestion] = {}
    for key in keys:
        for suggestion in suggestions_by_key.get(key, []):
            seen[suggestion.id] = suggestion

    # Cross-page sharing: threads keyed to indicators NOT used by this page (e.g.
    # filed on another MDim built from the same garden metadata) still belong here
    # when they target the equivalent field and either their text snapshot matches
    # ours, or — for bullet lists — the bullets they edit exist in our list too
    # (lists share individual bullets via YAML anchors while differing as a whole).
    # NOTE: indicators backing OTHER VIEWS of this same page are excluded — sibling
    # views (e.g. before vs after tax) are governed by the exact-text/pattern rules
    # above; bullet-matching across them would mix dimension-specific content.
    if field.current_value is not None:
        value = _norm(field.current_value)
        own_page_indicators = set(review.indicator_paths) if review is not None else set()
        indicator_field = VIEW_TO_INDICATOR_FIELD.get(field.field_path, field.field_path)
        is_bullets = indicator_field == "description_key"
        for key, threads in suggestions_by_key.items():
            if key in keys or key[0] != "indicator" or key[3] != indicator_field:
                continue
            if key[1] in own_page_indicators:
                continue
            for suggestion in threads:
                if _norm(suggestion.currentValue) == value or (
                    is_bullets
                    and suggestion.suggestedValue is not None
                    and apply_bullet_edits(suggestion.currentValue, suggestion.suggestedValue, field.current_value)
                    is not None
                ):
                    seen.setdefault(suggestion.id, suggestion)
    return sorted(seen.values(), key=lambda s: s.createdAt)


@dataclass
class Staleness:
    """Result of comparing a persisted suggestion against the currently resolved field."""

    target_gone: bool = False
    field_changed: bool = False
    page_changed: bool = False
    current_value: str | None = None
    current_provenance: str | None = None

    @property
    def is_stale(self) -> bool:
        return self.target_gone or self.field_changed


def check_staleness(
    suggestion: gm.MetadataReviewSuggestion,
    current_fields_by_key: dict[tuple[str, str, str | None, str], ReviewableField],
    display_field: ReviewableField | None = None,
) -> Staleness:
    """Field-level staleness: the suggestion's snapshot vs the live resolved field.

    `current_fields_by_key` maps `ReviewableField.source_key()` of every field on the
    page being rendered. A suggestion whose key is absent points at a view/indicator
    that isn't on this page — that's `target_gone`, unless `display_field` is given
    (a cross-page borrowed thread shown on a text-matched field), in which case
    staleness is judged against the field actually displaying it.
    """
    key = (suggestion.targetType, suggestion.targetPath, suggestion.viewId, suggestion.fieldPath)
    field = current_fields_by_key.get(key)
    if field is None:
        field = display_field
    if field is None:
        return Staleness(target_gone=True)
    return Staleness(
        field_changed=(field.provenance != suggestion.provenance)
        or (_norm(field.current_value) != _norm(suggestion.currentValue)),
        page_changed=suggestion.pageChecksum is not None and suggestion.pageChecksum != field.page_checksum,
        current_value=field.current_value,
        current_provenance=field.provenance,
    )


def _norm(value: Any) -> str | None:
    """Normalize a field value for staleness comparison (values are stored as text)."""
    if value is None:
        return None
    return str(value).strip()
