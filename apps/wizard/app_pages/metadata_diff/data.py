"""Database access for the Metadata Diff app.

Fetches MDIM configs, variable metadata, and per-view chart configs from the two
environments being compared (staging = "source", production = "target"). Read-only.
"""

import json
from typing import Any

import pandas as pd
from sqlalchemy.engine.base import Engine
from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.core import METADATA_FIELDS, ViewBundle, build_view_bundle
from etl.db import read_sql

log = get_logger()


def get_mdim_changes(source_engine: Engine, target_engine: Engine) -> pd.DataFrame:
    """All MDIMs on the staging server, flagging those whose config differs from production.

    NOTE: an unchanged MDIM config does not imply unchanged texts — the texts mostly live
    in indicator metadata, which can change without touching the config. This flag is only
    used to sort the selection list, not to skip diffing.
    """
    q = """
    select catalogPath, configMd5, published, slug
    from multi_dim_data_pages
    where catalogPath is not null
    order by updatedAt desc
    """
    df_source = read_sql(q, engine=source_engine)
    df_target = read_sql(q, engine=target_engine)

    df = pd.merge(df_source, df_target, on="catalogPath", suffixes=("_source", "_target"), how="left")
    df["is_new"] = df["configMd5_target"].isnull()
    df["config_changed"] = df["configMd5_source"] != df["configMd5_target"]
    return df.set_index("catalogPath")


def load_mdim_config(engine: Engine, catalog_path: str) -> dict[str, Any] | None:
    df = read_sql(
        "select config from multi_dim_data_pages where catalogPath = %(catalog_path)s",
        engine=engine,
        params={"catalog_path": catalog_path},
    )
    if df.empty:
        return None
    return json.loads(df.iloc[0]["config"])


def _first_y_indicator_id(view: dict[str, Any]) -> int | None:
    """The view's first y-indicator: the site uses its metadata for the data-page texts."""
    y = (view.get("indicators") or {}).get("y") or []
    if not y:
        return None
    first = y[0]
    if isinstance(first, dict):
        return first.get("id")
    if isinstance(first, int):
        return first
    return None


def _chunked(values: list, size: int = 500):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def fetch_variable_rows(engine: Engine, variable_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Metadata columns of the `variables` table for the given IDs, keyed by ID."""
    if not variable_ids:
        return {}
    columns = ["id", "name", "catalogPath"] + list(METADATA_FIELDS)
    rows: dict[int, dict[str, Any]] = {}
    for chunk in _chunked(sorted(set(variable_ids))):
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"select {', '.join(columns)} from variables where id in ({placeholders})",
            engine=engine,
            params=tuple(chunk),
        )
        for record in df.to_dict("records"):
            rows[int(record["id"])] = record
    return rows


def fetch_chart_text(engine: Engine, config_uuids: list[str]) -> dict[str, dict[str, Any]]:
    """Chart text fields (title, subtitle, note) of chart configs, keyed by UUID."""
    if not config_uuids:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(sorted(set(config_uuids))):
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"""
            select cc.id,
                   cc.full ->> '$.title' as title,
                   cc.full ->> '$.subtitle' as subtitle,
                   cc.full ->> '$.note' as note
            from chart_configs cc
            where cc.id in ({placeholders})
            """,
            engine=engine,
            params=tuple(chunk),
        )
        for record in df.to_dict("records"):
            out[str(record["id"])] = {k: record[k] for k in ("title", "subtitle", "note")}
    return out


def build_env_bundles(engine: Engine, config: dict[str, Any]) -> list[ViewBundle]:
    """Build a ViewBundle for every view of an MDIM config, resolving lookups in bulk."""
    views = config.get("views") or []

    variable_ids = [vid for view in views if (vid := _first_y_indicator_id(view)) is not None]
    variable_rows = fetch_variable_rows(engine, variable_ids)

    config_uuids = [str(uuid) for view in views if (uuid := view.get("fullConfigId"))]
    chart_text = fetch_chart_text(engine, config_uuids)

    config_metadata = config.get("metadata")

    bundles = []
    for view in views:
        vid = _first_y_indicator_id(view)
        uuid = view.get("fullConfigId")
        bundles.append(
            build_view_bundle(
                view=view,
                config_metadata=config_metadata,
                variable_row=variable_rows.get(vid) if vid is not None else None,
                chart_config=chart_text.get(str(uuid)) if uuid else None,
            )
        )
    return bundles
