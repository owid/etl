"""Database access for the Metadata Diff app.

Fetches MDIM configs, variable metadata, and per-view chart configs from the two
environments being compared (staging = "source", production = "target"). Read-only.
"""

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.core import METADATA_FIELDS, ViewBundle, build_view_bundle
from etl.db import read_sql

log = get_logger()


# --- Review persistence -------------------------------------------------------------------------
# A self-contained app-state table on the staging (source) DB, so review decisions are durable and
# shared across sessions/reviewers — analogous to `chart_diff_approvals`. Lives on the branch's
# staging DB, so it is inherently branch-scoped (no branch column needed).
_REVIEW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS metadata_review (
    id INT AUTO_INCREMENT PRIMARY KEY,
    catalogPath VARCHAR(500) NOT NULL,
    changeKey VARCHAR(64) NOT NULL,
    contentHash VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    comment TEXT,
    reviewer VARCHAR(255),
    updatedAt DATETIME NOT NULL,
    UNIQUE KEY uniq_change_key (changeKey),
    KEY idx_catalog (catalogPath)
)
"""


def _ensure_review_table(engine: Engine) -> None:
    with engine.begin() as con:
        con.execute(text(_REVIEW_TABLE_DDL))


def load_reviews(engine: Engine, catalog_path: str) -> dict[str, dict[str, Any]]:
    """Persisted review rows for one MDim, keyed by changeKey (contentHash binds each to its text)."""
    _ensure_review_table(engine)
    df = read_sql(
        "select changeKey, contentHash, status, comment, reviewer, updatedAt "
        "from metadata_review where catalogPath = %(cp)s",
        engine=engine,
        params={"cp": catalog_path},
    )
    return {str(r["changeKey"]): {str(k): v for k, v in r.to_dict().items()} for _, r in df.iterrows()}


# Stored status for a change the reviewer has ticked off in a list. Distinct from the Review page's
# "approved"/"flagged", so a list tick is never mistaken for a sign-off.
REVIEWED = "reviewed"


def count_ticked(engine: Engine, entries: list[tuple[str, str, str]]) -> int:
    """How many of these (surface, change key, content hash) changes are currently ticked reviewed.

    Deliberately uncached and cheap — one query per surface — because it answers a question that changes
    the instant someone presses a toggle. Identifying *which* changes exist is the expensive half and is
    cached elsewhere; if this were cached with it, the counter would sit still for minutes after a tick.

    A tick only counts against the text it was made on: an edit since then leaves the row in place but no
    longer reviewed, the same rule the toggles apply.
    """
    if not entries:
        return 0
    by_surface: dict[str, dict[str, str]] = {}
    for surface, change_key, content_hash in entries:
        by_surface.setdefault(surface, {})[change_key] = content_hash

    done = 0
    for surface, wanted in by_surface.items():
        stored = load_reviews(engine, surface)
        for change_key, content_hash in wanted.items():
            row = stored.get(change_key)
            if row and row.get("status") == REVIEWED and row.get("contentHash") == content_hash:
                done += 1
    return done


def upsert_review(
    engine: Engine,
    catalog_path: str,
    change_key: str,
    content_hash: str,
    status: str,
    comment: str | None,
    reviewer: str | None,
) -> None:
    """Record a decision, bound to the current contentHash (a later text edit makes it stale)."""
    _ensure_review_table(engine)
    with engine.begin() as con:
        con.execute(
            text(
                "insert into metadata_review "
                "(catalogPath, changeKey, contentHash, status, comment, reviewer, updatedAt) "
                "values (:cp, :ck, :ch, :st, :cm, :rv, :ts) "
                "on duplicate key update contentHash=values(contentHash), status=values(status), "
                "comment=values(comment), reviewer=values(reviewer), updatedAt=values(updatedAt)"
            ),
            {
                "cp": catalog_path,
                "ck": change_key,
                "ch": content_hash,
                "st": status,
                "cm": comment,
                "rv": reviewer,
                "ts": datetime.now(timezone.utc),
            },
        )


def delete_review(engine: Engine, change_key: str) -> None:
    """Reset a change back to Pending (remove its row)."""
    _ensure_review_table(engine)
    with engine.begin() as con:
        con.execute(text("delete from metadata_review where changeKey = :ck"), {"ck": change_key})


def _load_configs(engine: Engine) -> dict[str, dict[str, Any]]:
    """Every MDIM's config, keyed by catalogPath.

    Deliberately unordered: `config` is a large JSON blob, and combining it with an `order by` makes
    MySQL sort rows carrying those blobs, which overruns the server's sort buffer.
    """
    df = read_sql(
        "select catalogPath, config from multi_dim_data_pages where catalogPath is not null",
        engine=engine,
    )
    configs: dict[str, dict[str, Any]] = {}
    for record in df.to_dict("records"):
        raw = record.get("config")
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError):
            continue
        if isinstance(parsed, dict):
            configs[str(record["catalogPath"])] = parsed
    return configs


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


_CONFIG_COLUMN_CACHE: dict[str, str] = {}


def config_column(engine: Engine) -> str:
    """Name of `chart_configs`' resolved-config column in this environment.

    Grapher renamed it from `full` to `config`, and the two environments this tool compares migrate at
    different times — production first, a branch's staging server whenever it rebuilds. Asking the schema
    is the only thing that works for both, and the answer cannot change under a running app, so it is
    read once per environment.
    """
    key = str(engine.url)
    cached_name = _CONFIG_COLUMN_CACHE.get(key)
    if cached_name is None:
        columns = set(read_sql("show columns from chart_configs", engine=engine)["Field"].tolist())
        cached_name = "config" if "config" in columns else "full"
        _CONFIG_COLUMN_CACHE[key] = cached_name
    return cached_name


def _chunked(values: list, size: int = 500):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def _fetch_chunks(fetch: Callable[[list], list[Any]], values: list) -> list[Any]:
    """Run one chunked query over `values` concurrently, returning every row.

    A 10,000-id lookup is 21 chunks of 500, and issuing them in sequence pays the round trip to the
    staging server 21 times — 2.2s of a 7s page load, all of it waiting. The chunks are independent and
    the engines pool 30 connections, so they go out together; rows are merged by key afterwards, so
    completion order never matters.
    """
    chunks = list(_chunked(values))
    if len(chunks) <= 1:
        return [row for chunk in chunks for row in fetch(chunk)]
    with ThreadPoolExecutor(max_workers=min(8, len(chunks))) as pool:
        return [row for rows in pool.map(fetch, chunks) for row in rows]


def fetch_variable_rows(engine: Engine, variable_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Metadata columns of the `variables` table for the given IDs, keyed by ID."""
    if not variable_ids:
        return {}
    columns = ["id", "name", "catalogPath"] + list(METADATA_FIELDS)

    def fetch(chunk: list) -> list[Any]:
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"select {', '.join(columns)} from variables where id in ({placeholders})",
            engine=engine,
            params=tuple(chunk),
        )
        return df.to_dict("records")

    # to_dict returns Hashable keys; our columns are all strings.
    return {
        int(record["id"]): {str(k): v for k, v in record.items()}
        for record in _fetch_chunks(fetch, sorted(set(variable_ids)))
    }


def fetch_variable_paths(engine: Engine, variable_ids: list[int]) -> dict[int, str]:
    """id -> catalogPath, for callers that only need the mapping.

    `fetch_variable_rows` returns every metadata column, which is a lot of text to move when the answer
    wanted is one identifier: on this server it cost 2.4s for the 10,387 variables an MDim sweep touches.
    """
    if not variable_ids:
        return {}

    def fetch(chunk: list) -> list[Any]:
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"select id, catalogPath from variables where id in ({placeholders}) and catalogPath is not null",
            engine=engine,
            params=tuple(chunk),
        )
        return df.to_dict("records")

    return {int(record["id"]): str(record["catalogPath"]) for record in _fetch_chunks(fetch, sorted(set(variable_ids)))}


def fetch_variable_rows_by_path(engine: Engine, catalog_paths: list[str]) -> dict[str, dict[str, Any]]:
    """Same columns as `fetch_variable_rows`, but keyed by catalogPath.

    catalogPath is the only identifier that is stable across environments: a version-bumped grapher step
    mints fresh variable ids on staging, so an id-keyed comparison against production reports every
    indicator of that dataset as changed.
    """
    if not catalog_paths:
        return {}
    columns = ["id", "name", "catalogPath"] + list(METADATA_FIELDS)
    rows: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(sorted(set(catalog_paths))):
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"select {', '.join(columns)} from variables where catalogPath in ({placeholders})",
            engine=engine,
            params=tuple(chunk),
        )
        for record in df.to_dict("records"):
            rows[str(record["catalogPath"])] = {str(k): v for k, v in record.items()}
    return rows


def fetch_chart_text(engine: Engine, config_uuids: list[str]) -> dict[str, dict[str, Any]]:
    """Chart text fields (title, subtitle, note) of chart configs, keyed by UUID."""
    if not config_uuids:
        return {}
    cfg = config_column(engine)
    out: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(sorted(set(config_uuids))):
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"""
            select cc.id,
                   cc.{cfg} ->> '$.title' as title,
                   cc.{cfg} ->> '$.subtitle' as subtitle,
                   cc.{cfg} ->> '$.note' as note
            from chart_configs cc
            where cc.id in ({placeholders})
            """,
            engine=engine,
            params=tuple(chunk),
        )
        for record in df.to_dict("records"):
            out[str(record["id"])] = {k: record[k] for k in ("title", "subtitle", "note")}
    return out


def resolve_chart(engine: Engine, ref: str) -> dict[str, Any] | None:
    """Resolve a published chart by numeric id, slug, or a grapher URL. Returns its id/slug/FAUST."""
    ref = (ref or "").strip()
    if not ref:
        return None
    if ref.isdigit():
        where, params = "c.id = %(v)s", {"v": int(ref)}
    else:
        slug = ref.rstrip("/").split("/")[-1].split("?")[0]  # tolerate a full /grapher/<slug>?… URL
        where, params = "cc.slug = %(v)s", {"v": slug}
    cfg = config_column(engine)
    df = read_sql(
        f"""
        select c.id as chartId,
               cc.slug as slug,
               cc.{cfg} ->> '$.title' as title,
               cc.{cfg} ->> '$.subtitle' as subtitle,
               cc.{cfg} ->> '$.note' as note
        from charts c
        join chart_configs cc on cc.id = c.configId
        where {where} and c.publishedAt is not null
        limit 1
        """,
        engine=engine,
        params=params,
    )
    if df.empty:
        return None
    return {str(k): v for k, v in df.iloc[0].to_dict().items()}


def build_chart_bundle(engine: Engine, ref: str) -> tuple[ViewBundle, dict[str, Any]] | None:
    """Build a single-"view" bundle for a standalone chart: its y-indicator metadata + its FAUST.

    Returns (bundle, chart) or None if the chart can't be resolved. The chart dict also carries
    `n_indicators` and `has_data_page` — grapher renders a data page (and thus this WYSK text) only
    for single-indicator charts; a scatter/multi-series chart has none. The bundle has empty
    dimensions, so it matches its baseline counterpart in `diff_views`.
    """
    chart = resolve_chart(engine, ref)
    if chart is None:
        return None
    dims = read_sql(
        "select variableId, property from chart_dimensions where chartId = %(id)s order by `order`",
        engine=engine,
        params={"id": int(chart["chartId"])},
    )
    chart["n_indicators"] = int(dims["variableId"].nunique()) if not dims.empty else 0
    chart["has_data_page"] = chart["n_indicators"] == 1
    # Primary y-indicator (fall back to the first dimension) — whose metadata the data page renders.
    y = dims[dims["property"] == "y"] if not dims.empty else dims
    if not y.empty:
        vid: int | None = int(y.iloc[0]["variableId"])
    elif not dims.empty:
        vid = int(dims.iloc[0]["variableId"])
    else:
        vid = None
    variable_row = fetch_variable_rows(engine, [vid]).get(vid) if vid is not None else None
    chart_config = {"title": chart.get("title"), "subtitle": chart.get("subtitle"), "note": chart.get("note")}
    bundle = build_view_bundle(
        view={"dimensions": {}}, config_metadata=None, variable_row=variable_row, chart_config=chart_config
    )
    return bundle, chart


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
