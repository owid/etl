"""Upsert a Collection with zero dimensions as a regular Grapher chart.

When a multidim `Collection` has no dimensions and exactly one view, it is the degenerate
"single chart" case. Instead of pushing it as a multi-dim data page via the `/multi-dims/`
admin endpoint, we translate the view into a standard Grapher chart config and push it
via `AdminAPI.create_chart` / `AdminAPI.update_chart`.

This keeps one authoring format (mdim config) across the chart↔multidim spectrum: adding a
dimension to a single-chart collection promotes it to a proper multidim without a config
migration.
"""

from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from apps.chart_sync.admin_api import AdminAPI
from etl.collection.utils import map_indicator_path_to_id
from etl.config import DEFAULT_GRAPHER_SCHEMA, GRAPHER_USER_ID, OWIDEnv
from etl.grapher.model import Chart

if TYPE_CHECKING:
    from etl.collection.model.core import Collection
    from etl.collection.model.view import View

log = structlog.get_logger()


_AXIS_ORDER = ("y", "x", "size", "color")


def upsert_collection_as_chart(collection: "Collection", owid_env: OWIDEnv) -> int:
    """Push a zero-dimension collection to Grapher as a regular chart.

    Expects `len(collection.dimensions) == 0` and `len(collection.views) == 1`.
    """
    if len(collection.dimensions) != 0:
        raise ValueError("upsert_collection_as_chart called on a collection with dimensions.")
    if len(collection.views) != 1:
        raise ValueError(f"Chart mode (no dimensions) requires exactly one view; got {len(collection.views)}.")

    view = collection.views[0]
    slug = _resolve_chart_slug(collection, view)
    config = _build_chart_config(view, slug)

    admin_api = AdminAPI(owid_env)
    user_id = int(GRAPHER_USER_ID) if GRAPHER_USER_ID else None

    with Session(owid_env.engine) as session:
        try:
            existing = Chart.load_chart(session, slug=slug)
        except NoResultFound:
            existing = None

    if existing is not None:
        # Preserve publication state unless the user explicitly overrode it.
        config.setdefault("isPublished", existing.config.get("isPublished", False))
        log.info("collection.chart.update", slug=slug, chart_id=existing.id)
        admin_api.update_chart(chart_id=existing.id, chart_config=config, user_id=user_id)
        chart_id = existing.id
    else:
        # New charts default to unpublished so humans can review before go-live.
        config.setdefault("isPublished", False)
        log.info("collection.chart.create", slug=slug)
        result = admin_api.create_chart(chart_config=config, user_id=user_id)
        chart_id = result["chartId"]

    log.info(
        "collection.chart.upsert_success",
        slug=slug,
        chart_id=chart_id,
        admin_url=f"{owid_env.admin_site}/admin/charts/{chart_id}/edit",
    )
    return chart_id


def _resolve_chart_slug(collection: "Collection", view: "View") -> str:
    """Derive the chart slug from the collection's short_name.

    Grapher chart slugs are conventionally dash-separated; the mdim short_name is snake_case.
    """
    del view  # unused for now; kept in the signature for future explicit-slug overrides
    return collection.short_name.replace("_", "-")


def _build_chart_config(view: "View", slug: str) -> dict[str, Any]:
    """Translate `view.config` + `view.indicators` into a grapher chart config dict."""
    config: dict[str, Any] = dict(view.config or {})
    config["slug"] = slug
    config.setdefault("$schema", DEFAULT_GRAPHER_SCHEMA)

    # Resolve indicator catalog paths (y/x/size/color) to variable IDs and emit as
    # the grapher `dimensions` block, which charts identify by numeric variableId.
    dimensions: list[dict[str, Any]] = []
    for axis in _AXIS_ORDER:
        entries = _axis_entries(view, axis)
        for indicator in entries:
            dim: dict[str, Any] = {"property": axis, "variableId": int(map_indicator_path_to_id(indicator.catalogPath))}
            if indicator.display:
                dim["display"] = indicator.display
            dimensions.append(dim)
    if not dimensions:
        raise ValueError(f"Chart view for slug '{slug}' has no indicators.")
    config["dimensions"] = dimensions

    # Rewrite catalog-path references in `sortColumnSlug` and `map.columnSlug` to IDs.
    if "sortColumnSlug" in config:
        config["sortColumnSlug"] = str(map_indicator_path_to_id(config["sortColumnSlug"]))
    if isinstance(config.get("map"), dict) and "columnSlug" in config["map"]:
        config["map"]["columnSlug"] = str(map_indicator_path_to_id(config["map"]["columnSlug"]))

    return config


def _axis_entries(view: "View", axis: str) -> list:
    value = getattr(view.indicators, axis, None)
    if value is None:
        return []
    return value if isinstance(value, list) else [value]
