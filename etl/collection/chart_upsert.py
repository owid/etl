"""Upsert a Collection with zero dimensions as a regular Grapher chart.

The collection's YAML is treated as the chart's ETL-authored grapher config and
written to the chart's ETL config row via
`PUT /admin/api/charts/by-config/:chartConfigId/etlConfig`, addressed by the
chart's config UUID (`charts.configId`) — the chart's stable identity, declared
as `chart_config_id` in the config YAML. The endpoint has upsert semantics: if no
chart with that UUID exists yet, the admin creates a minimal draft chart carrying
it. Nothing is ever looked up per environment, so the same YAML addresses the
same chart everywhere. Admin-authored edits live in
`chart_configs.patch` and are preserved across ETL re-pushes by construction
(ETL and admin write to different rows).
"""

import secrets
import time
import uuid
from typing import TYPE_CHECKING, Any

import structlog
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from apps.chart_sync.admin_api import AdminAPI
from etl.collection.utils import map_indicator_path_to_id
from etl.config import DEFAULT_GRAPHER_SCHEMA, OWIDEnv
from etl.files import read_json_schema
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

    # The chart's identity is the config UUID declared in the config YAML — no
    # environment lookup involved, so the same YAML addresses the same chart in
    # every environment (local, staging, prod). It is the config author's job to
    # put the right UUID there: the existing chart's `charts.configId` when
    # bringing a chart into ETL, or a freshly minted UUIDv7 (see
    # `new_chart_config_id`) for a brand-new chart.
    # `Collection.save()` already ran this, but re-run it here so the guarantee holds for any
    # entry point into the upsert: a malformed UUID must never reach the admin, where it would
    # miss the intended chart and silently create a new one.
    collection.validate_chart_config_id()
    chart_config_id = collection.chart_config_id

    # Write the chart's ETL-authored config. The endpoint has upsert semantics: it
    # creates the chart carrying this UUID if it doesn't exist yet, otherwise it
    # updates that chart's ETL config row. Because creation and config-write happen
    # in the same request, a new chart's admin patch starts out (almost) empty, so
    # the ETL layer owns all fields — notably `dimensions` — from birth. New charts
    # are unpublished drafts with indicator inheritance enabled (the admin's
    # default for new charts). Server-side, `full` is recomputed as
    # merge(variableETL, etlConfig, existing patch), so any admin patches already
    # in chart_configs.patch are preserved.
    log.info("collection.chart.upsert", slug=slug, chart_config_id=chart_config_id)
    result = admin_api.upsert_chart_etl_config(
        chart_config_id=chart_config_id,
        grapher_config=chart_config,
        catalog_path=collection.catalog_path,
    )
    chart_id = result["chartId"]
    is_new = result["created"]

    # Set topic tags on freshly created charts only — once a chart exists,
    # tags are admin-managed and ETL must not stomp on them.
    if collection.topic_tags:
        if is_new:
            tags = _resolve_topic_tags(owid_env, collection.topic_tags)
            if tags:
                admin_api.set_tags(chart_id=chart_id, tags=tags)
        else:
            # Editing `topic_tags` on a chart that already exists does nothing, and without
            # this the config author has no way of telling that from a successful push.
            log.warning(
                "collection.chart.topic_tags_ignored",
                chart_id=chart_id,
                topic_tags=collection.topic_tags,
                reason="tags are admin-managed once a chart exists; ETL only sets them at creation",
            )

    # The slug is derived from the file's short name, but grapher excludes `slug` from what an
    # ETL layer may contribute, so an existing chart keeps the slug it already had. Renaming the
    # config file therefore looks like it renames the chart and doesn't.
    if not is_new:
        slug_in_grapher = admin_api.get_chart_config(chart_id).get("slug")
        if slug_in_grapher and slug_in_grapher != slug:
            log.warning(
                "collection.chart.slug_not_applied",
                chart_id=chart_id,
                slug_in_grapher=slug_in_grapher,
                slug_from_file_name=slug,
                reason="an existing chart's slug cannot be changed from ETL; rename it in the admin instead",
            )

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


def new_chart_config_id() -> str:
    """Mint a chart config UUID for a brand-new ETL-authored chart.

    Put the returned value in the chart's `.config.yml` as `chart_config_id`; it
    becomes the chart's identity in every environment it is pushed to.

    It's a UUIDv7 (time-ordered), matching the UUIDs the grapher admin generates
    for `chart_configs.id`. Python's `uuid` module only ships this from 3.14, so
    build it by hand: 48-bit unix-ms timestamp, 4-bit version, 74 random bits
    (12-bit rand_a + 2-bit variant + 62-bit rand_b).
    """
    timestamp_ms = time.time_ns() // 1_000_000
    rand = secrets.randbits(74)
    value = (timestamp_ms << 80) | (0x7 << 76) | ((rand >> 62) << 64) | (0b10 << 62) | (rand & ((1 << 62) - 1))
    return str(uuid.UUID(int=value))


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
