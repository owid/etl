"""Upsert a Collection with zero dimensions as a regular Grapher chart.

How to avoid syncing issues between ETL and grapher?
Users can edit ETL-managed charts directly in the grapher admin. To avoid silently
overwriting their work on the next ETL run, we will marks the chart as edited by the ETL_GRAPHER_USER_ID.
Any human admin edit will be stored as edits with their own id.
Before pushing, we check the marker: if it's not the ETL, the chart was edited in admin
since the last ETL push, and we refuse to overwrite unless `ETL_FORCE_CHART=1`.

"""

import os
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
import structlog
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from apps.chart_sync.admin_api import AdminAPI
from etl.collection.utils import map_indicator_path_to_id
from etl.config import DEFAULT_GRAPHER_SCHEMA, ETL_GRAPHER_USER_ID, OWIDEnv
from etl.grapher.model import Chart, User

if TYPE_CHECKING:
    from etl.collection.model.core import Collection
    from etl.collection.model.view import View


class ChartAdminDriftError(Exception):
    """Raised when an ETL-managed chart has been edited in admin since the last ETL push."""


log = structlog.get_logger()


_AXIS_ORDER = ("y", "x", "size", "color")
_FORCE_ENV_VAR = "ETL_FORCE_CHART"


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
    config = _build_chart_config(view, slug)

    admin_api = AdminAPI(owid_env)
    etl_user_id = ETL_GRAPHER_USER_ID

    with Session(owid_env.engine) as session:
        try:
            existing = Chart.load_chart(session, slug=slug)
        except NoResultFound:
            existing = None

    if existing is not None:
        _check_admin_drift(owid_env, existing, slug, etl_user_id)
        # Preserve publication state unless the user explicitly overrode it.
        config.setdefault("isPublished", existing.config.get("isPublished", False))
        log.info("collection.chart.update", slug=slug, chart_id=existing.id)
        admin_api.update_chart(chart_id=existing.id, chart_config=config, user_id=etl_user_id)
        chart_id = existing.id
    else:
        # New charts default to unpublished so humans can review before go-live.
        config.setdefault("isPublished", False)
        log.info("collection.chart.create", slug=slug)
        result = admin_api.create_chart(chart_config=config, user_id=etl_user_id)
        chart_id = result["chartId"]

    # Stamp the chart as ETL-managed so the next ETL run's drift check recognises it.
    _mark_chart_as_etl_managed(owid_env, chart_id, etl_user_id)

    log.info(
        "collection.chart.upsert_success",
        slug=slug,
        chart_id=chart_id,
        admin_url=f"{owid_env.admin_site}/admin/charts/{chart_id}/edit",
    )
    return chart_id


def _check_admin_drift(owid_env: OWIDEnv, existing: Chart, slug: str, etl_user_id: int) -> None:
    """Refuse to overwrite a chart whose marker shows a human edited it since the last ETL push.

    The marker is `charts.lastEditedByUserId`. ETL stamps it to `etl_user_id` after every
    push; any admin edit naturally overwrites it with the human's id. If we see a non-ETL
    id, the chart has been edited in admin and overwriting would lose that work.

    Override with `ETL_FORCE_CHART=1` (after pulling the admin changes into the ETL
    config, or deciding to discard them).
    """
    last_editor_id = int(existing.lastEditedByUserId)
    if last_editor_id == etl_user_id:
        return  # ETL was the last editor — safe to push

    if os.environ.get(_FORCE_ENV_VAR) == "1":
        log.warning(
            "collection.chart.force_override_admin_drift",
            slug=slug,
            chart_id=existing.id,
            last_editor_id=last_editor_id,
            last_edited_at=str(existing.lastEditedAt),
        )
        return

    with Session(owid_env.engine) as session:
        editor = session.get(User, last_editor_id)
        editor_name = editor.fullName if editor else f"user #{last_editor_id}"

    admin_url = f"{owid_env.admin_site}/admin/charts/{existing.id}/edit"
    raise ChartAdminDriftError(
        f"Chart '{slug}' (id={existing.id}) was last edited in admin by {editor_name} "
        f"on {existing.lastEditedAt}. Refusing to overwrite. Options:\n"
        f"  • Inspect the admin changes at {admin_url}\n"
        f"  • Pull the admin changes back into the ETL config, then re-run.\n"
        f"  • Or set {_FORCE_ENV_VAR}=1 to overwrite the admin changes."
    )


def _mark_chart_as_etl_managed(owid_env: OWIDEnv, chart_id: int, etl_user_id: int) -> None:
    """Stamp `charts.lastEditedByUserId = etl_user_id` so the next drift check passes.

    We do this directly via SQL because the admin API records the actual auth user, which
    is the ETL bot only when `ADMIN_API_KEY` is set. Without the key (dev/staging), the
    admin records the human running the command, which would make every ETL run look like
    admin drift to the next one. This explicit SQL update closes the loop.
    """
    with Session(owid_env.engine) as session:
        session.execute(
            sa.text("UPDATE charts SET lastEditedByUserId = :uid WHERE id = :cid"),
            {"uid": etl_user_id, "cid": chart_id},
        )
        session.commit()


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
