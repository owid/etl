"""Gather public-facing content from the grapher DB into typed content bundles.

One bundle per content object (chart, MDim, explorer, post), with every user-visible text field
tagged with its origin and fix location. See ``apps.inspector.schema`` for the bundle format.
"""

import json
import re
from typing import Any
from urllib.parse import quote, urlencode

import pandas as pd
from structlog import get_logger

from apps.inspector.schema import (
    KIND_CHART,
    KIND_EXPLORER,
    KIND_MULTIDIM,
    KIND_POST,
    ContentBundle,
    EmbeddedChart,
    Indicator,
    TextField,
    View,
)
from etl import config
from etl.db import read_sql

log = get_logger()

# Chart-config fields that users see on the chart itself.
CHART_CONFIG_FIELDS = ["title", "subtitle", "note", "sourceDesc"]

# Variable (indicator) columns whose text is user-visible (chart footer, data page, tooltips).
VARIABLE_TEXT_COLUMNS = [
    "name",
    "titlePublic",
    "titleVariant",
    "unit",
    "shortUnit",
    "descriptionShort",
    "descriptionFromProducer",
    "descriptionProcessing",
    "attribution",
    "attributionShort",
]

# Keys extracted from collection (MDim/explorer) configs by the generic walker.
COLLECTION_TEXT_KEYS = [
    "title",
    "titleVariant",
    "title_variant",
    "subtitle",
    "name",
    "label",
    "description",
    "group",
    "tooltip",
    "explorerTitle",
    "explorerSubtitle",
    "relatedQuestionText",
]

# Keys whose subtrees duplicate per-view chart configs (those are gathered from chart_configs).
COLLECTION_SKIP_KEYS = ["views", "graphers", "config", "blocks", "block"]

# Matches grapher chart references in post markdown (absolute or site-relative).
EMBEDDED_CHART_RE = re.compile(r"(?:https?://(?:www\.)?ourworldindata\.org)?/grapher/([a-zA-Z0-9_-]+)")


def _site() -> str:
    return config.OWID_ENV.site or "https://ourworldindata.org"


def _admin_site() -> str:
    return config.OWID_ENV.admin_site or "https://admin.owid.io/admin"


# ---------------------------------------------------------------------------
# Indicators
# ---------------------------------------------------------------------------


def _fetch_indicators(variable_ids: set[int]) -> dict[int, Indicator]:
    """Load user-visible metadata for the given variables, one Indicator per variable."""
    if not variable_ids:
        return {}
    columns = ", ".join(["id", "catalogPath", "display", "descriptionKey"] + VARIABLE_TEXT_COLUMNS)
    df = read_sql(
        f"SELECT {columns} FROM variables WHERE id IN %(ids)s",  # noqa: S608
        params={"ids": tuple(variable_ids)},
    )
    indicators = {}
    for row in df.to_dict("records"):
        variable_id = int(row["id"])
        catalog_path = row.get("catalogPath") or None
        # The catalogPath names the grapher step whose metadata produced this text; that (or the
        # garden step behind it) is where an editor fixes indicator text.
        fix_location = catalog_path or f"{_admin_site()}/variables/{variable_id}"
        origin_id = f"variable:{variable_id}"
        fields = []
        for column in VARIABLE_TEXT_COLUMNS:
            value = row.get(column)
            if value and str(value).strip():
                fields.append(
                    TextField(
                        name=column,
                        text=str(value),
                        origin="indicator",
                        origin_id=origin_id,
                        fix_location=fix_location,
                    )
                )
        # descriptionKey is a JSON array of bullets; each bullet is its own field.
        description_key = row.get("descriptionKey")
        if description_key:
            try:
                bullets = json.loads(description_key) if isinstance(description_key, str) else description_key
            except json.JSONDecodeError:
                bullets = []
            for i, bullet in enumerate(bullets or []):
                if bullet and str(bullet).strip():
                    fields.append(
                        TextField(
                            name=f"descriptionKey[{i}]",
                            text=str(bullet),
                            origin="indicator",
                            origin_id=origin_id,
                            fix_location=fix_location,
                        )
                    )
        # display.name overrides the legend label.
        display = row.get("display")
        if display:
            try:
                display_obj = json.loads(display) if isinstance(display, str) else display
            except json.JSONDecodeError:
                display_obj = {}
            display_name = (display_obj or {}).get("name")
            if display_name and str(display_name).strip():
                fields.append(
                    TextField(
                        name="display.name",
                        text=str(display_name),
                        origin="indicator",
                        origin_id=origin_id,
                        fix_location=fix_location,
                    )
                )
        indicators[variable_id] = Indicator(variable_id=variable_id, catalog_path=catalog_path, fields=fields)
    return indicators


def _variable_ids_from_config(chart_config: dict[str, Any]) -> list[int]:
    """Variable ids in a chart config, y-dimension variables first (the first y is the one
    Grapher's default title/subtitle resolve from)."""
    ys, others = [], []
    for dimension in chart_config.get("dimensions") or []:
        variable_id = dimension.get("variableId")
        if not variable_id:
            continue
        (ys if dimension.get("property") == "y" else others).append(int(variable_id))
    ordered = []
    for variable_id in ys + others:
        if variable_id not in ordered:
            ordered.append(variable_id)
    return ordered


def _resolve_rendered_fields(views: list[View], indicators: dict[int, Indicator]) -> None:
    """Set each view's rendered title/subtitle: config fields when present, otherwise Grapher's
    fallback to the primary indicator's metadata."""
    for view in views:
        by_name = {f.name: f.text for f in view.fields}
        view.rendered_title = by_name.get("title", "")
        view.rendered_subtitle = by_name.get("subtitle", "")
        if view.rendered_title and view.rendered_subtitle:
            continue
        primary = next((indicators[i] for i in view.indicator_ids if i in indicators), None)
        if primary is None:
            continue
        primary_by_name = {f.name: f.text for f in primary.fields}
        if not view.rendered_title:
            view.rendered_title = (
                primary_by_name.get("titlePublic")
                or primary_by_name.get("display.name")
                or primary_by_name.get("name")
                or ""
            )
        if not view.rendered_subtitle:
            view.rendered_subtitle = primary_by_name.get("descriptionShort", "")


def _parse_config(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if raw:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _chart_config_fields(chart_config: dict[str, Any], origin_id: str, fix_location: str) -> list[TextField]:
    """Extract user-visible fields from a (full, merged) chart config."""
    fields = []
    for name in CHART_CONFIG_FIELDS:
        value = chart_config.get(name)
        if value and str(value).strip():
            fields.append(
                TextField(
                    name=name, text=str(value), origin="chart_config", origin_id=origin_id, fix_location=fix_location
                )
            )
    for i, dimension in enumerate(chart_config.get("dimensions") or []):
        display = dimension.get("display") or {}
        for key, value in display.items():
            if isinstance(value, str) and value.strip():
                fields.append(
                    TextField(
                        name=f"dimensions[{i}].display.{key}",
                        text=value,
                        origin="chart_config",
                        origin_id=origin_id,
                        fix_location=fix_location,
                    )
                )
    return fields


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def gather_charts(slugs: list[str] | None = None, limit: int | None = None) -> list[ContentBundle]:
    query = """
        SELECT c.id AS chart_id, cc.slug AS slug, cc.full AS config
        FROM charts c
        JOIN chart_configs cc ON c.configId = cc.id
        WHERE cc.slug IS NOT NULL
    """
    params: dict[str, Any] = {}
    if slugs:
        # Targeted runs may inspect drafts on purpose.
        query += " AND cc.slug IN %(slugs)s"
        params["slugs"] = tuple(slugs)
    else:
        query += " AND cc.full ->> '$.isPublished' = 'true'"
    query += " ORDER BY cc.slug"
    if limit:
        query += " LIMIT %(limit)s"
        params["limit"] = limit
    df = read_sql(query, params=params or None)

    # Parse configs first so indicators can be fetched in a single query for all charts.
    rows = []
    all_variable_ids: set[int] = set()
    for row in df.to_dict("records"):
        chart_config = _parse_config(row["config"])
        variable_ids = _variable_ids_from_config(chart_config)
        all_variable_ids |= set(variable_ids)
        rows.append((row, chart_config, variable_ids))
    indicators = _fetch_indicators(all_variable_ids)

    bundles = []
    for row, chart_config, variable_ids in rows:
        slug = row["slug"]
        chart_id = int(row["chart_id"])
        url = f"{_site()}/grapher/{slug}"
        fix_location = f"{_admin_site()}/charts/{chart_id}/edit"
        origin_id = f"chart:{chart_id}"
        views = [
            View(
                view_id=slug,
                dimensions={},
                url=url,
                fields=_chart_config_fields(chart_config, origin_id, fix_location),
                indicator_ids=variable_ids,
            )
        ]
        _resolve_rendered_fields(views, indicators)
        bundle = ContentBundle(
            kind=KIND_CHART,
            slug=slug,
            url=url,
            fix_location=fix_location,
            views=views,
            indicators=[indicators[i] for i in variable_ids if i in indicators],
        )
        bundle.content_hash = bundle.compute_content_hash()
        bundles.append(bundle)
    return bundles


# ---------------------------------------------------------------------------
# MDims
# ---------------------------------------------------------------------------


def _multidim_filter_clause(slug_filters: list[str] | None, table_alias: str = "md") -> tuple[str, dict[str, Any]]:
    """Match an MDim by published slug, by short_name (part after ``#`` in catalogPath), or by
    catalogPath segment, so drafts (slug NULL) and whole namespaces are addressable."""
    if not slug_filters:
        return "", {}
    clauses = [
        f"{table_alias}.slug IN %(mdim_slugs)s",
        f"SUBSTRING_INDEX({table_alias}.catalogPath, '#', -1) IN %(mdim_slugs)s",
    ]
    params: dict[str, Any] = {"mdim_slugs": tuple(slug_filters)}
    for i, slug in enumerate(slug_filters):
        escaped = slug.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        clauses.append(f"{table_alias}.catalogPath LIKE %(mdim_like_{i}_mid)s")
        params[f"mdim_like_{i}_mid"] = f"%/{escaped}/%"
        clauses.append(f"{table_alias}.catalogPath LIKE %(mdim_like_{i}_start)s")
        params[f"mdim_like_{i}_start"] = f"{escaped}/%"
    return f"AND ({' OR '.join(clauses)})", params


def _parse_multidim_view_id(view_id: str, mdim_config: dict[str, Any]) -> dict[str, str]:
    """Map an MDim viewId back to ``{dimension: choice}``.

    Current format is ``dim=choice`` pairs joined by ``__`` (e.g. ``age=_15_49__cause=malaria``);
    the legacy format is bare choice values joined by ``__``, ordered by dimension slug
    alphabetically.
    """
    if "=" in view_id:
        result = {}
        # Split on "__" only when followed by another "key=" pair, since choice slugs may
        # themselves contain underscores.
        for pair in re.split(r"__(?=[^=_][^=]*=)", view_id):
            if "=" in pair:
                key, value = pair.split("=", 1)
                result[key] = value
        return result
    dimensions = mdim_config.get("dimensions") or []
    parts = view_id.split("__")
    if not dimensions or len(parts) != len(dimensions):
        return {}
    dim_slugs_sorted = sorted(d.get("slug", "") for d in dimensions)
    return {dim: choice for dim, choice in zip(dim_slugs_sorted, parts) if dim}


def _multidim_view_url(slug: str, published: bool, catalog_path: str | None, dimensions: dict[str, str]) -> str:
    query = f"?{urlencode(dimensions)}" if dimensions else ""
    if published:
        return f"{_site()}/grapher/{slug}{query}"
    catalog_quoted = quote(str(catalog_path or ""), safe="")
    return f"{_site()}/admin/grapher/{catalog_quoted}{query}#{slug}"


def _collection_config_fields(config_obj: dict[str, Any], origin_id: str, fix_location: str) -> list[TextField]:
    """Walk a collection config and extract user-visible texts (dimension and choice names,
    titles, related questions), skipping subtrees that duplicate per-view chart configs."""
    fields = []

    def walk(obj: Any, path: str, depth: int = 0) -> None:
        if depth > 12:
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in COLLECTION_SKIP_KEYS:
                    continue
                child_path = f"{path}.{key}" if path else key
                if key in COLLECTION_TEXT_KEYS and isinstance(value, str):
                    # Strip templating placeholders; what remains is still user-visible text.
                    cleaned = re.sub(r"\{[^}]*\}", "", value)
                    if cleaned.strip():
                        fields.append(
                            TextField(
                                name=child_path,
                                text=cleaned,
                                origin="collection_config",
                                origin_id=origin_id,
                                fix_location=fix_location,
                            )
                        )
                else:
                    walk(value, child_path, depth + 1)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                walk(item, f"{path}[{i}]", depth + 1)

    walk(config_obj, "")
    return fields


def _hoist_shared_view_fields(views: list[View]) -> list[TextField]:
    """Move (name, text) pairs present in every view to the bundle level, so repeated texts are
    listed once."""
    if len(views) < 2:
        return []
    common = {(f.name, f.text) for f in views[0].fields}
    for view in views[1:]:
        common &= {(f.name, f.text) for f in view.fields}
    if not common:
        return []
    shared = [f for f in views[0].fields if (f.name, f.text) in common]
    for view in views:
        view.fields = [f for f in view.fields if (f.name, f.text) not in common]
    return shared


def gather_multidims(slugs: list[str] | None = None, limit: int | None = None) -> list[ContentBundle]:
    filter_clause, params = _multidim_filter_clause(slugs, table_alias="md")
    query = f"""
        SELECT md.id AS mdim_id,
               COALESCE(md.slug, SUBSTRING_INDEX(md.catalogPath, '#', -1)) AS slug,
               md.catalogPath AS catalog_path,
               md.published AS published,
               md.config AS mdim_config,
               mx.viewId AS view_id,
               mx.variableId AS variable_id,
               cc.full AS view_config
        FROM multi_dim_data_pages md
        JOIN multi_dim_x_chart_configs mx ON md.id = mx.multiDimId
        LEFT JOIN chart_configs cc ON mx.chartConfigId = cc.id
        WHERE 1 = 1 {filter_clause}
        ORDER BY slug, mx.viewId
    """  # noqa: S608
    df = read_sql(query, params=params or None)

    bundles = []
    for slug, group in df.groupby("slug", sort=True):
        if limit and len(bundles) >= limit:
            break
        first = group.iloc[0]
        mdim_config = _parse_config(first["mdim_config"])
        published = bool(first["published"])
        catalog_path = first["catalog_path"]
        origin_id = f"multidim:{slug}"
        # MDim text is authored in the ETL export step; the catalogPath names it.
        fix_location = str(catalog_path) if catalog_path else f"{_site()}/grapher/{slug}"
        bundle_url = _multidim_view_url(str(slug), published, catalog_path, {})

        views = []
        all_variable_ids: set[int] = set()
        for row in group.to_dict("records"):
            view_config = _parse_config(row["view_config"])
            dimensions = _parse_multidim_view_id(str(row["view_id"]), mdim_config)
            # mx.variableId is the view's primary y indicator; keep it first.
            variable_ids = []
            if row.get("variable_id") is not None and not pd.isna(row["variable_id"]):
                variable_ids.append(int(row["variable_id"]))
            variable_ids += [v for v in _variable_ids_from_config(view_config) if v not in variable_ids]
            all_variable_ids |= set(variable_ids)
            views.append(
                View(
                    view_id=str(row["view_id"]),
                    dimensions=dimensions,
                    url=_multidim_view_url(str(slug), published, catalog_path, dimensions),
                    fields=_chart_config_fields(view_config, f"{origin_id}/view:{row['view_id']}", fix_location),
                    indicator_ids=variable_ids,
                )
            )

        indicators = _fetch_indicators(all_variable_ids)
        _resolve_rendered_fields(views, indicators)
        bundle = ContentBundle(
            kind=KIND_MULTIDIM,
            slug=str(slug),
            url=bundle_url,
            fix_location=fix_location,
            config_fields=_collection_config_fields(mdim_config, origin_id, fix_location),
            views=views,
            indicators=[indicators[i] for i in sorted(indicators)],
        )
        bundle.shared_view_fields = _hoist_shared_view_fields(bundle.views)
        bundle.content_hash = bundle.compute_content_hash()
        bundles.append(bundle)
    return bundles


# ---------------------------------------------------------------------------
# Explorers
# ---------------------------------------------------------------------------


def gather_explorers(slugs: list[str] | None = None, limit: int | None = None) -> list[ContentBundle]:
    params: dict[str, Any] = {}
    slug_clause = ""
    if slugs:
        slug_clause = "AND ev.explorerSlug IN %(slugs)s"
        params["slugs"] = tuple(slugs)
    query = f"""
        SELECT ev.id AS view_id, ev.explorerSlug AS slug, ev.dimensions AS dimensions,
               cc.full AS view_config
        FROM explorer_views ev
        LEFT JOIN chart_configs cc ON ev.chartConfigId = cc.id
        WHERE ev.error IS NULL {slug_clause}
        ORDER BY ev.explorerSlug, ev.id
    """  # noqa: S608
    df = read_sql(query, params=params or None)

    config_query = "SELECT slug, config FROM explorers"
    config_params = None
    if slugs:
        config_query += " WHERE slug IN %(slugs)s"
        config_params = {"slugs": tuple(slugs)}
    df_configs = read_sql(config_query, params=config_params)
    configs_by_slug = {r["slug"]: _parse_config(r["config"]) for r in df_configs.to_dict("records")}

    bundles = []
    for slug, group in df.groupby("slug", sort=True):
        if limit and len(bundles) >= limit:
            break
        origin_id = f"explorer:{slug}"
        url = f"{_site()}/explorers/{slug}"
        # Explorer text is authored in the ETL export step (or the legacy TSV) behind this slug.
        fix_location = url
        views = []
        all_variable_ids: set[int] = set()
        for row in group.to_dict("records"):
            view_config = _parse_config(row["view_config"])
            dimensions = _parse_config(row["dimensions"])
            dimensions = {str(k): str(v) for k, v in dimensions.items()}
            variable_ids = _variable_ids_from_config(view_config)
            all_variable_ids |= set(variable_ids)
            view_url = f"{url}?{urlencode(dimensions)}" if dimensions else url
            views.append(
                View(
                    view_id=str(row["view_id"]),
                    dimensions=dimensions,
                    url=view_url,
                    fields=_chart_config_fields(view_config, f"{origin_id}/view:{row['view_id']}", fix_location),
                    indicator_ids=variable_ids,
                )
            )

        indicators = _fetch_indicators(all_variable_ids)
        _resolve_rendered_fields(views, indicators)
        explorer_config = configs_by_slug.get(slug, {})
        bundle = ContentBundle(
            kind=KIND_EXPLORER,
            slug=str(slug),
            url=url,
            fix_location=fix_location,
            config_fields=_collection_config_fields(explorer_config, origin_id, fix_location),
            views=views,
            indicators=[indicators[i] for i in sorted(indicators)],
        )
        bundle.shared_view_fields = _hoist_shared_view_fields(bundle.views)
        bundle.content_hash = bundle.compute_content_hash()
        bundles.append(bundle)
    return bundles


# ---------------------------------------------------------------------------
# Posts
# ---------------------------------------------------------------------------


def _fetch_embedded_charts(markdown: str) -> list[EmbeddedChart]:
    slugs = sorted(set(EMBEDDED_CHART_RE.findall(markdown)))
    if not slugs:
        return []
    df = read_sql(
        """
        SELECT cc.slug AS slug, cc.full ->> '$.title' AS title, cc.full ->> '$.subtitle' AS subtitle
        FROM chart_configs cc
        WHERE cc.slug IN %(slugs)s
        """,
        params={"slugs": tuple(slugs)},
    )
    return [
        EmbeddedChart(
            slug=r["slug"],
            url=f"{_site()}/grapher/{r['slug']}",
            title=r.get("title") or "",
            subtitle=r.get("subtitle") or "",
        )
        for r in df.to_dict("records")
    ]


def gather_posts(slugs: list[str] | None = None, limit: int | None = None) -> list[ContentBundle]:
    params: dict[str, Any] = {}
    if slugs:
        where = "WHERE slug IN %(slugs)s"
        params["slugs"] = tuple(slugs)
    else:
        # When sweeping, skip unpublished drafts.
        where = "WHERE published = 1"
    query = f"SELECT id, slug, type, markdown FROM posts_gdocs {where} ORDER BY slug"  # noqa: S608
    if limit:
        query += " LIMIT %(limit)s"
        params["limit"] = limit
    df = read_sql(query, params=params or None)

    bundles = []
    for row in df.to_dict("records"):
        markdown = row.get("markdown") or ""
        if not markdown.strip():
            continue
        slug = row["slug"]
        post_type = row.get("type") or ""
        if post_type == "data-insight":
            url = f"{_site()}/data-insights/{slug}"
        else:
            url = f"{_site()}/{slug}"
        bundle = ContentBundle(
            kind=KIND_POST,
            slug=slug,
            url=url,
            # posts_gdocs.id is the Google Doc id; link straight to the editable doc.
            fix_location=f"https://docs.google.com/document/d/{row['id']}/edit",
            markdown=markdown,
            embedded_charts=_fetch_embedded_charts(markdown),
        )
        bundle.content_hash = bundle.compute_content_hash()
        bundles.append(bundle)
    return bundles


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

GATHERERS = {
    KIND_CHART: gather_charts,
    KIND_MULTIDIM: gather_multidims,
    KIND_EXPLORER: gather_explorers,
    KIND_POST: gather_posts,
}


def gather(
    slugs: list[str] | None = None,
    kinds: list[str] | None = None,
    limit: int | None = None,
) -> list[ContentBundle]:
    """Gather content bundles, optionally filtered by slug and kind."""
    bundles = []
    for kind, gatherer in GATHERERS.items():
        if kinds and kind not in kinds:
            continue
        found = gatherer(slugs=slugs, limit=limit)
        log.info("inspector.gather", kind=kind, bundles=len(found))
        bundles.extend(found)
    return bundles
