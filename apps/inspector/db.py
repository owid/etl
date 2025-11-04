# Database queries and data loading
import json
import re
from typing import Any

import pandas as pd
from rich import print as rprint
from structlog import get_logger

from etl import config
from etl.db import read_sql

log = get_logger()


def fetch_explorer_data(explorer_slugs: list[str] | None = None) -> pd.DataFrame:
    """Fetch all explorer views with their metadata from the database.

    Args:
        explorer_slugs: Optional list of explorer slugs to filter by

    Returns:
        DataFrame with columns: id, explorerSlug, dimensions, chartConfigId,
                                chart_config, and multiple variable_* columns
    """
    where_clause = "WHERE ev.error IS NULL"
    if explorer_slugs:
        slugs_str = "', '".join(explorer_slugs)
        where_clause += f" AND ev.explorerSlug IN ('{slugs_str}')"

    # Fetch views without configs first
    query = f"""
        SELECT
            ev.id,
            'explorer' as view_type,
            ev.explorerSlug,
            ev.dimensions,
            ev.chartConfigId,
            cc.full as chart_config,
            v.id as variable_id,
            v.name as variable_name,
            v.unit as variable_unit,
            v.description as variable_description,
            v.shortUnit as variable_short_unit,
            v.shortName as variable_short_name,
            v.display as variable_display,
            v.titlePublic as variable_title_public,
            v.titleVariant as variable_title_variant,
            v.descriptionShort as variable_description_short,
            v.descriptionFromProducer as variable_description_from_producer,
            v.descriptionKey as variable_description_key,
            v.descriptionProcessing as variable_description_processing,
            v.attribution as variable_attribution,
            v.attributionShort as variable_attribution_short
        FROM explorer_views ev
        LEFT JOIN chart_configs cc ON ev.chartConfigId = cc.id
        LEFT JOIN JSON_TABLE(
            cc.full,
            '$.dimensions[*]' COLUMNS(
                variableId INT PATH '$.variableId'
            )
        ) jt ON TRUE
        LEFT JOIN variables v ON jt.variableId = v.id
        {where_clause}
        ORDER BY ev.explorerSlug, ev.id
    """

    log.info("Fetching explorer data from database...")
    df = read_sql(query)
    log.info(f"Fetched {len(df)} explorer view records (views x variables, which will be aggregated)")

    # Fetch explorer configs separately (much more efficient)
    config_where = ""
    if explorer_slugs:
        slugs_str = "', '".join(explorer_slugs)
        config_where = f"WHERE slug IN ('{slugs_str}')"

    config_query = f"""
        SELECT slug, config
        FROM explorers
        {config_where}
    """
    log.info("Fetching explorer configs...")
    configs_df = read_sql(config_query)
    log.info(f"Fetched {len(configs_df)} explorer config(s)")

    # Deduplicate configs by slug (keep first occurrence)
    # This prevents cartesian product issues with NULL slugs during merge
    configs_df = configs_df.drop_duplicates(subset=["slug"], keep="first")

    # Merge configs with views
    df = df.merge(
        configs_df.rename(columns={"slug": "explorerSlug", "config": "explorer_config"}), on="explorerSlug", how="left"
    )

    return df


def aggregate_explorer_views(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate explorer views by grouping variables for each view.

    Args:
        df: Raw dataframe from fetch_explorer_data

    Returns:
        Aggregated dataframe with one row per explorer view
    """
    # Group by explorer view and aggregate variable metadata
    # Use dropna=False to preserve rows with NULL explorerSlug (if any)
    agg_df = (
        df.groupby(
            ["id", "view_type", "explorerSlug", "dimensions", "chartConfigId", "chart_config", "explorer_config"],
            dropna=False,
        )
        .agg(
            {
                "variable_id": lambda x: list(x.dropna()),
                "variable_name": lambda x: list(x.dropna()),
                "variable_unit": lambda x: list(x.dropna()),
                "variable_description": lambda x: list(x.dropna()),
                "variable_short_unit": lambda x: list(x.dropna()),
                "variable_short_name": lambda x: list(x.dropna()),
                "variable_display": lambda x: list(x.dropna()),
                "variable_title_public": lambda x: list(x.dropna()),
                "variable_title_variant": lambda x: list(x.dropna()),
                "variable_description_short": lambda x: list(x.dropna()),
                "variable_description_from_producer": lambda x: list(x.dropna()),
                "variable_description_key": lambda x: list(x.dropna()),
                "variable_description_processing": lambda x: list(x.dropna()),
                "variable_attribution": lambda x: list(x.dropna()),
                "variable_attribution_short": lambda x: list(x.dropna()),
            }
        )
        .reset_index()
    )

    return agg_df


def fetch_multidim_data(slug_filters: list[str] | None = None) -> pd.DataFrame:
    """Fetch multidimensional indicator data from the database.

    Args:
        slug_filters: Optional list of multidim slugs to filter by

    Returns:
        DataFrame with columns matching explorer structure: id, explorerSlug, dimensions,
                                chartConfigId, chart_config, and variable_* columns
    """
    where_clause = ""
    if slug_filters:
        slugs_str = "', '".join(slug_filters)
        where_clause = f"WHERE md.slug IN ('{slugs_str}')"

    # Fetch views without configs first
    query = f"""
        SELECT
            mx.id as id,
            'multidim' as view_type,
            md.slug as explorerSlug,
            mx.viewId as dimensions,
            md.published as mdim_published,
            md.catalogPath as mdim_catalog_path,
            mx.chartConfigId,
            cc.full as chart_config,
            v.id as variable_id,
            v.name as variable_name,
            v.unit as variable_unit,
            v.description as variable_description,
            v.shortUnit as variable_short_unit,
            v.shortName as variable_short_name,
            v.display as variable_display,
            v.titlePublic as variable_title_public,
            v.titleVariant as variable_title_variant,
            v.descriptionShort as variable_description_short,
            v.descriptionFromProducer as variable_description_from_producer,
            v.descriptionKey as variable_description_key,
            v.descriptionProcessing as variable_description_processing,
            v.attribution as variable_attribution,
            v.attributionShort as variable_attribution_short
        FROM multi_dim_data_pages md
        JOIN multi_dim_x_chart_configs mx ON md.id = mx.multiDimId
        LEFT JOIN chart_configs cc ON mx.chartConfigId = cc.id
        LEFT JOIN variables v ON mx.variableId = v.id
        {where_clause}
        ORDER BY md.slug, mx.viewId
    """

    log.info("Fetching multidimensional indicator data from database...")
    df = read_sql(query)
    log.info(f"Fetched {len(df)} multidim view records")

    # Fetch multidim configs separately (much more efficient)
    config_where = ""
    if slug_filters:
        slugs_str = "', '".join(slug_filters)
        config_where = f"WHERE slug IN ('{slugs_str}')"

    config_query = f"""
        SELECT slug, config
        FROM multi_dim_data_pages
        {config_where}
    """
    log.info("Fetching multidim configs...")
    configs_df = read_sql(config_query)
    log.info(f"Fetched {len(configs_df)} multidim config(s)")

    # Deduplicate configs by slug (keep first occurrence)
    # This prevents cartesian product issues with NULL slugs during merge
    configs_df = configs_df.drop_duplicates(subset=["slug"], keep="first")

    # Merge configs with views
    df = df.merge(
        configs_df.rename(columns={"slug": "explorerSlug", "config": "explorer_config"}), on="explorerSlug", how="left"
    )

    return df


def aggregate_multidim_views(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate multidim views by grouping variables for each view.

    Args:
        df: Raw dataframe from fetch_multidim_data

    Returns:
        Aggregated dataframe with one row per multidim view
    """
    # Group by multidim view and aggregate variable metadata (same as explorers)
    # Use dropna=False to preserve rows with NULL explorerSlug
    agg_df = (
        df.groupby(
            [
                "id",
                "view_type",
                "explorerSlug",
                "dimensions",
                "mdim_published",
                "mdim_catalog_path",
                "chartConfigId",
                "chart_config",
                "explorer_config",
            ],
            dropna=False,
        )
        .agg(
            {
                "variable_id": lambda x: list(x.dropna()),
                "variable_name": lambda x: list(x.dropna()),
                "variable_unit": lambda x: list(x.dropna()),
                "variable_description": lambda x: list(x.dropna()),
                "variable_short_unit": lambda x: list(x.dropna()),
                "variable_short_name": lambda x: list(x.dropna()),
                "variable_display": lambda x: list(x.dropna()),
                "variable_title_public": lambda x: list(x.dropna()),
                "variable_title_variant": lambda x: list(x.dropna()),
                "variable_description_short": lambda x: list(x.dropna()),
                "variable_description_from_producer": lambda x: list(x.dropna()),
                "variable_description_key": lambda x: list(x.dropna()),
                "variable_description_processing": lambda x: list(x.dropna()),
                "variable_attribution": lambda x: list(x.dropna()),
                "variable_attribution_short": lambda x: list(x.dropna()),
            }
        )
        .reset_index()
    )

    return agg_df


def fetch_posts(post_slugs: list[str] | None = None) -> list[dict[str, Any]]:
    """Fetch post markdown from posts_gdocs table.

    Posts include articles, data insights, and topic pages.

    Args:
        post_slugs: Optional list of post slugs to filter by

    Returns:
        List of post dictionaries with markdown content
    """
    log.info("Fetching posts from database...")

    # Build WHERE clause for SQL query
    where_clause = ""
    if post_slugs:
        slugs_str = "', '".join(post_slugs)
        where_clause = f"WHERE slug IN ('{slugs_str}')"

    query = f"""
        SELECT
            id,
            slug,
            type,
            'post' as view_type,
            markdown
        FROM posts_gdocs
        {where_clause}
        ORDER BY slug
    """

    df = read_sql(query)
    log.info(f"Fetched {len(df)} post(s)")

    if df.empty:
        return []

    result: list[dict[str, Any]] = df.to_dict("records")  # type: ignore
    return result


def fetch_chart_configs(chart_slugs: list[str] | None = None) -> list[dict[str, Any]]:
    """Fetch chart configs from database by slug, including variable metadata.

    Args:
        chart_slugs: Optional list of chart slugs to filter by

    Returns:
        List of chart config dictionaries with variable metadata
    """
    log.info("Fetching chart configs from database...")

    # Build WHERE clause for SQL query
    where_clause = ""
    if chart_slugs:
        slugs_str = "', '".join(chart_slugs)
        where_clause = f"WHERE cc.slug IN ('{slugs_str}')"
    else:
        # By default, only get published charts with slugs
        # Note: Explorer views use unpublished charts (without slugs), so there's no duplication
        where_clause = "WHERE cc.slug IS NOT NULL"

    # Fetch charts with their variable metadata (similar to explorer views)
    query = f"""
        SELECT
            c.id,
            cc.id as config_id,
            cc.slug,
            'chart' as view_type,
            cc.full as chart_config,
            v.id as variable_id,
            v.name as variable_name,
            v.unit as variable_unit,
            v.description as variable_description,
            v.shortUnit as variable_short_unit,
            v.shortName as variable_short_name,
            v.display as variable_display,
            v.titlePublic as variable_title_public,
            v.titleVariant as variable_title_variant,
            v.descriptionShort as variable_description_short,
            v.descriptionFromProducer as variable_description_from_producer,
            v.descriptionKey as variable_description_key,
            v.descriptionProcessing as variable_description_processing,
            v.attribution as variable_attribution,
            v.attributionShort as variable_attribution_short
        FROM charts c
        JOIN chart_configs cc ON c.configId = cc.id
        LEFT JOIN JSON_TABLE(
            cc.full,
            '$.dimensions[*]' COLUMNS(
                variableId INT PATH '$.variableId'
            )
        ) jt ON TRUE
        LEFT JOIN variables v ON jt.variableId = v.id
        {where_clause}
        ORDER BY cc.slug, c.id
    """

    df = read_sql(query)
    log.info(f"Fetched {len(df)} chart records (charts x variables, which will be aggregated)")

    if df.empty:
        return []

    # Aggregate by chart to group all variables together
    agg_df = (
        df.groupby(["id", "config_id", "slug", "view_type", "chart_config"], dropna=False)
        .agg(
            {
                "variable_id": lambda x: list(x.dropna()),
                "variable_name": lambda x: list(x.dropna()),
                "variable_unit": lambda x: list(x.dropna()),
                "variable_description": lambda x: list(x.dropna()),
                "variable_short_unit": lambda x: list(x.dropna()),
                "variable_short_name": lambda x: list(x.dropna()),
                "variable_display": lambda x: list(x.dropna()),
                "variable_title_public": lambda x: list(x.dropna()),
                "variable_title_variant": lambda x: list(x.dropna()),
                "variable_description_short": lambda x: list(x.dropna()),
                "variable_description_from_producer": lambda x: list(x.dropna()),
                "variable_description_key": lambda x: list(x.dropna()),
                "variable_description_processing": lambda x: list(x.dropna()),
                "variable_attribution": lambda x: list(x.dropna()),
                "variable_attribution_short": lambda x: list(x.dropna()),
            }
        )
        .reset_index()
    )

    result: list[dict[str, Any]] = agg_df.to_dict("records")  # type: ignore
    log.info(f"Aggregated to {len(result)} chart configs")
    return result


def extract_chart_fields(chart_config: dict[str, Any]) -> list[tuple[str, Any]]:
    """Extract human-readable fields from chart config JSON.

    Args:
        chart_config: Chart configuration dictionary from chart_configs.full

    Returns:
        List of (field_name, field_value) tuples for all text fields
    """
    fields = []

    # Top-level text fields (excluding internalNotes as requested)
    for field_name in ["title", "subtitle", "note", "sourceDesc"]:
        if field_name in chart_config and chart_config[field_name]:
            fields.append((field_name, chart_config[field_name]))

    # Extract display names and other text from dimensions
    if "dimensions" in chart_config and chart_config["dimensions"]:
        for dim_idx, dimension in enumerate(chart_config["dimensions"]):
            if "display" in dimension and dimension["display"]:
                for display_key, display_value in dimension["display"].items():
                    # Only include string fields (names, labels, tooltips, etc.)
                    if isinstance(display_value, str) and display_value.strip():
                        field_key = f"dimension_{dim_idx}_display_{display_key}"
                        fields.append((field_key, display_value))

    return fields


def extract_human_readable_text(config_json: str) -> str:
    """Extract only human-readable fields from config JSON and strip template variables.

    Args:
        config_json: JSON string of explorer/multidim config

    Returns:
        Cleaned text with only human-readable content and templates removed
    """
    try:
        config = json.loads(config_json)
    except json.JSONDecodeError:
        # Fallback to full text if parse fails
        return config_json

    texts = []

    def extract(obj: Any, depth: int = 0) -> None:
        if depth > 10:  # Prevent infinite recursion
            return

        if isinstance(obj, dict):
            for key, value in obj.items():
                # Extract common human-readable field names
                if key in [
                    "title",
                    "title_variant",  # Multidim title variant
                    "subtitle",
                    "note",
                    "description",
                    "label",
                    "text",
                    "tooltip",
                    "name",  # Dimension/choice names
                    "group",  # Choice groups
                    "explorerTitle",
                    "explorerSubtitle",
                    "relatedQuestionText",  # Explorer schema
                    "sourceName",  # Display metadata
                    "sourceDesc",  # Source description (grapher config)
                    "additionalInfo",  # Display metadata
                    "dataPublishedBy",  # Display metadata
                ]:
                    if isinstance(value, str):
                        # Strip {template} patterns like {welfare['welfare_type'][wel]}
                        cleaned = re.sub(r"\{[^}]+\}", "", value)
                        if cleaned.strip():
                            texts.append(cleaned)
                extract(value, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, depth + 1)

    extract(config)
    return "\n".join(texts)


def load_views(slug_list: list[str] | None, limit: int | None, content_type: str | None = None) -> list[dict[str, Any]]:
    """Load views from database and aggregate by view ID.

    Args:
        slug_list: List of slugs to filter by (works for explorers, multidims, charts, and posts)
        limit: Maximum number of views to return
        content_type: Optional filter by content type ('explorer', 'multidim', 'chart', or 'post')

    Returns:
        List of view dictionaries (explorers, multidims, charts, and posts)
    """
    # Fetch data from all sources (explorers, multidimensional indicators, charts, and posts)
    # When slug_list is provided, all queries filter by the same slugs
    df_explorers = fetch_explorer_data(explorer_slugs=slug_list)
    df_mdims = fetch_multidim_data(slug_filters=slug_list)
    chart_configs = fetch_chart_configs(chart_slugs=slug_list)
    posts = fetch_posts(post_slugs=slug_list)

    # Check if we got any results
    if df_explorers.empty and df_mdims.empty and not chart_configs and not posts:
        if slug_list:
            slugs_str = ", ".join(slug_list)
            rprint(f"[red]Error: No views found for slug(s) '{slugs_str}'[/red]")
        else:
            rprint("[red]Error: No explorer, multidim, chart, or post views found in database[/red]")
        return []

    # Aggregate views
    agg_df_explorers = aggregate_explorer_views(df_explorers) if not df_explorers.empty else pd.DataFrame()
    agg_df_mdims = aggregate_multidim_views(df_mdims) if not df_mdims.empty else pd.DataFrame()
    agg_df = pd.concat([agg_df_explorers, agg_df_mdims], ignore_index=True)

    # Sort by explorerSlug to group views from same collection together
    # This ensures batches don't mix different collections, which would confuse Claude
    if not agg_df.empty:
        agg_df = agg_df.sort_values("explorerSlug").reset_index(drop=True)

    views: list[dict[str, Any]] = agg_df.to_dict("records")  # type: ignore

    # Add chart configs as views (charts are treated as single "views" without collections)
    views.extend(chart_configs)

    # Add posts as views (posts are individual items without collections)
    views.extend(posts)

    # Apply content type filter if specified
    if content_type:
        original_count = len(views)
        views = [v for v in views if v.get("view_type") == content_type]
        if original_count > len(views):
            rprint(f"[yellow]Filtered to {len(views)} {content_type} view(s) from {original_count} total[/yellow]")

    # Apply limit if specified
    if limit is not None and limit > 0:
        views = views[:limit]
        rprint(f"[yellow]Limiting to first {limit} views (for testing)[/yellow]")

    # Add collection config pseudo-views for AI semantic checking
    # These will be checked alongside regular views
    configs_by_slug = {}
    for view in views:
        slug = view.get("explorerSlug")
        config = view.get("explorer_config")
        view_type = view.get("view_type", "explorer")
        if slug and config and slug not in configs_by_slug:
            # Create a pseudo-view for this collection's config
            config_view = {
                "id": f"config_{slug}",
                "view_type": view_type,
                "explorerSlug": slug,
                "config_text": extract_human_readable_text(config),
                "is_config_view": True,
                # Copy multidim-specific fields for URL building
                "mdim_published": view.get("mdim_published", True),
                "mdim_catalog_path": view.get("mdim_catalog_path"),
                # Add empty lists for expected fields
                "variable_name": [],
                "variable_description": [],
                "variable_title_public": [],
                "variable_description_short": [],
                "variable_description_from_producer": [],
                "variable_description_key": [],
                "variable_description_processing": [],
            }
            configs_by_slug[slug] = config_view
            views.append(config_view)

    if slug_list:
        slugs_str = ", ".join(slug_list)
        rprint(f"[cyan]Filtering to slug(s): {slugs_str}[/cyan]")

    config_count = len(configs_by_slug)
    view_count = len(views) - config_count

    # Count charts and posts separately (they have view_type="chart" and "post")
    chart_count = len([v for v in views if v.get("view_type") == "chart"])
    post_count = len([v for v in views if v.get("view_type") == "post"])

    breakdown = f"{len(agg_df_explorers)} explorers + {len(agg_df_mdims)} multidims"
    if chart_count > 0:
        breakdown += f" + {chart_count} charts"
    if post_count > 0:
        breakdown += f" + {post_count} posts"

    rprint(
        f"[cyan]Aggregated to {view_count} unique views + {config_count} collection configs "
        f"({breakdown})...[/cyan]\n"
    )

    return views


def parse_multidim_view_id(view_id: str, mdim_config: str | None) -> dict[str, str]:
    """Parse multidim viewId into dimension values.

    ViewId format: choice values are ordered alphabetically by dimension slug.
    E.g., for dimensions [metric, antigen], viewId "comparison__vaccinated" maps to
    {antigen: comparison, metric: vaccinated} (alphabetical order: antigen, metric).

    Args:
        view_id: ViewId string like "level_side_by_side__number__both"
        mdim_config: JSON config string with dimension definitions

    Returns:
        Dict mapping dimension slugs to choice slugs
    """
    if not view_id or not mdim_config:
        return {}

    try:
        config = json.loads(mdim_config)
        dimensions = config.get("dimensions", [])

        if not dimensions:
            return {}

        # Split viewId by double underscore to get choice values
        parts = view_id.split("__")

        if len(parts) != len(dimensions):
            return {}

        # Sort dimension slugs alphabetically (this is the viewId ordering convention)
        dim_slugs_sorted = sorted([dim.get("slug", "") for dim in dimensions])

        # Map sorted dimension slugs to viewId parts
        result = {}
        for dim_slug, choice_slug in zip(dim_slugs_sorted, parts):
            if dim_slug:
                result[dim_slug] = choice_slug

        return result
    except (json.JSONDecodeError, AttributeError, KeyError):
        return {}


def parse_chart_config(chart_config_raw: Any) -> dict[str, Any]:
    """Parse chart_config field which can be a dict (chart views) or JSON string (explorer views).

    Args:
        chart_config_raw: Raw chart_config value - can be dict or JSON string

    Returns:
        Parsed chart config dict, or empty dict if not present
    """
    if isinstance(chart_config_raw, dict):
        return chart_config_raw
    elif chart_config_raw:
        try:
            return json.loads(chart_config_raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def parse_dimensions(dimensions_raw: Any, mdim_config: str | None = None) -> dict[str, Any]:
    """Parse dimensions field which can be JSON (explorers) or just a viewId (mdims).

    Args:
        dimensions_raw: Raw dimensions value from database
        mdim_config: For multidims, the JSON config string

    Returns:
        Parsed dimensions dict, or empty dict if not applicable
    """
    if not dimensions_raw:
        return {}
    if isinstance(dimensions_raw, dict):
        # Already parsed
        return dimensions_raw
    if isinstance(dimensions_raw, str) and dimensions_raw.startswith("{"):
        # Explorer: dimensions is a JSON string
        return json.loads(dimensions_raw)
    # Mdim: dimensions is a viewId - parse it with the config
    return parse_multidim_view_id(str(dimensions_raw), mdim_config)


def build_explorer_url(
    explorer_slug: str,
    dimensions: dict[str, Any],
    view_type: str = "explorer",
    mdim_published: bool = True,
    mdim_catalog_path: str | None = None,
) -> str:
    """Build URL for explorer or multidim view with dimensions.

    Args:
        explorer_slug: Explorer slug (e.g., 'air-pollution')
        dimensions: Dictionary of dimension key-value pairs
        view_type: Type of view ('explorer' or 'multidim')
        mdim_published: Whether the multidim is published
        mdim_catalog_path: Catalog path for unpublished multidims

    Returns:
        Full URL to the view with properly encoded query parameters
    """
    from urllib.parse import quote, urlencode

    base_url = config.OWID_ENV.site

    is_mdim = view_type == "multidim"

    if is_mdim:
        if mdim_published:
            # Published multidim: /grapher/{slug}?dimensions
            url = f"{base_url}/grapher/{explorer_slug}"
            if dimensions:
                params = {k: v for k, v in dimensions.items() if v}
                if params:
                    url += "?" + urlencode(params)
        else:
            # Unpublished multidim: /admin/grapher/{catalogPath}?dimensions#{slug}
            # Ensure catalog_path is a string before quoting
            catalog_path_str = str(mdim_catalog_path) if mdim_catalog_path is not None else ""
            catalog_path = quote(catalog_path_str, safe="")
            url = f"{base_url}/admin/grapher/{catalog_path}"
            if dimensions:
                params = {k: v for k, v in dimensions.items() if v}
                if params:
                    url += "?" + urlencode(params)
            url += f"#{explorer_slug}"
    else:
        # Regular explorer: /explorers/{slug}?dimensions
        url = f"{base_url}/explorers/{explorer_slug}"
        if dimensions:
            params = {k: v for k, v in dimensions.items() if v}
            if params:
                url += "?" + urlencode(params)

    return url
