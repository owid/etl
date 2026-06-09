"""Upsert a Collection with zero dimensions as a regular Grapher chart.

The collection's YAML is treated as the chart's ETL-authored grapher config and
written to `chart_configs.etlConfig` via `PUT /admin/api/charts/:id/etlConfig`.
Admin-authored edits live in `chart_configs.patch` and are preserved across
ETL re-pushes by construction (ETL and admin write to different columns).
"""

from typing import TYPE_CHECKING, Any

import structlog
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from apps.chart_sync.admin_api import AdminAPI
from etl.collection.utils import map_indicator_path_to_id
from etl.config import DEFAULT_GRAPHER_SCHEMA, OWIDEnv
from etl.files import read_json_schema
from etl.grapher.model import Chart
from etl.paths import SCHEMAS_DIR

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
    # Grapher slugs are dash-separated; mdim short_names are snake_case.
    slug = collection.short_name.replace("_", "-")
    chart_config = _build_chart_config(view, slug)
    _validate_chart_config(chart_config, slug)

    admin_api = AdminAPI(owid_env)

    # Look up the chart by its ETL catalog path (the stable ETL identity, like
    # multi_dim_data_pages.catalogPath) — not by slug, which is a mutable public
    # URL. If it exists, we keep it; otherwise we create a new one with a minimal
    # bootstrap config and then write the full config into chart_configs.etlConfig.
    with Session(owid_env.engine) as session:
        try:
            existing = Chart.load_chart(session, catalog_path=collection.catalog_path)
        except NoResultFound:
            existing = None

    if existing is None:
        # Minimal bootstrap so the chart row exists. We deliberately keep the
        # patch tiny: just what the grapher save path needs (schema, slug,
        # dimensions, isPublished). Everything else lands in etlConfig below.
        bootstrap = {
            "$schema": chart_config.get("$schema", DEFAULT_GRAPHER_SCHEMA),
            "slug": slug,
            "dimensions": chart_config["dimensions"],
            "isPublished": False,
            # Enable indicator-to-chart config inheritance for ETL-authored charts,
            # so grapher_config set on an indicator flows into every chart built on
            # it. Set only at creation, so existing charts are never touched.
            "isInheritanceEnabled": True,
        }
        log.info("collection.chart.create", slug=slug)
        result = admin_api.create_chart(chart_config=bootstrap)
        chart_id = result["chartId"]
        is_new = True
    else:
        chart_id = existing.id
        log.info("collection.chart.update", slug=slug, chart_id=chart_id)
        is_new = False

    # Write the chart's ETL-authored config. This recomputes `full` server-side
    # as merge(variableETL, etlConfig, existing patch); any admin patches
    # already in chart_configs.patch are preserved.
    admin_api.put_chart_etl_config(chart_id=chart_id, grapher_config=chart_config, catalog_path=collection.catalog_path)

    # Set topic tags on freshly created charts only — once a chart exists,
    # tags are admin-managed and ETL must not stomp on them.
    if is_new and collection.topic_tags:
        tags = _resolve_topic_tags(owid_env, collection.topic_tags)
        if tags:
            admin_api.set_tags(chart_id=chart_id, tags=tags)

    log.info(
        "collection.chart.upsert_success",
        slug=slug,
        chart_id=chart_id,
        admin_url=f"{owid_env.admin_site}/charts/{chart_id}/edit",
    )
    return chart_id


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


def _validate_chart_config(config: dict[str, Any], slug: str) -> None:
    """Validate the built config against the local grapher schema before pushing.

    The admin `etlConfig` endpoint only checks the schema *version*, not the
    config's structure, so a typo'd field or wrong type would be stored and just
    render wrong. We catch it here, with an error pointing at the offending field.
    Skips if the config's schema version isn't vendored locally.
    """
    schema_file = SCHEMAS_DIR / str(config.get("$schema", "")).rsplit("/", 1)[-1]
    if not schema_file.exists():
        return
    try:
        validate(config, read_json_schema(schema_file))
    except ValidationError as e:
        location = "/".join(str(p) for p in e.absolute_path) or "(root)"
        raise ValueError(f"Invalid chart config for slug '{slug}' at `{location}`: {e.message}") from e


def _axis_entries(view: "View", axis: str) -> list:
    value = getattr(view.indicators, axis, None)
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _resolve_topic_tags(owid_env: OWIDEnv, tag_names: list[str]) -> list[dict[str, Any]]:
    """Resolve tag names to the dict shape `AdminAPI.set_tags` expects."""
    stmt = text("SELECT id, name FROM tags WHERE name IN :names").bindparams(bindparam("names", expanding=True))
    with Session(owid_env.engine) as session:
        rows = session.execute(stmt, {"names": tag_names}).mappings().all()
    by_name = {row["name"]: row["id"] for row in rows}
    missing = [n for n in tag_names if n not in by_name]
    if missing:
        log.warning("collection.chart.unknown_topic_tags", tags=missing)
    return [
        {"id": by_name[name], "name": name, "isApproved": True, "keyChartLevel": 0}
        for name in tag_names
        if name in by_name
    ]
