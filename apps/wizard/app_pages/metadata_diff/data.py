"""Database access for the Metadata Diff app.

Fetches MDIM configs, variable metadata, and per-view chart configs from the two
environments being compared (staging = "source", production = "target"). Read-only.
"""

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.core import METADATA_FIELDS, ViewBundle, build_view_bundle, mark_identity
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


def count_reviewed(engine: Engine, surface: str, groups: list) -> int:
    """How many of these changes are ticked off, counting only ticks the text has not outgrown.

    A stored mark carries the content hash it was made against, so an edit since then leaves the row in
    place but no longer reviewed — the same rule the toggles apply, applied here so a progress count and
    the toggles beside it can never disagree.
    """
    if not groups:
        return 0
    stored = load_reviews(engine, surface)
    done = 0
    for group in groups:
        change_key, content_hash = mark_identity(surface, group)
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


# --- Author scope decisions ---------------------------------------------------------------------
# The AUTHOR's per-change decision — "apply to all charts/views" vs "scope to only these views" —
# stored separately from the reviewer's sign-off. The reviewer is shown this decision and approves
# or rejects it (they don't set the scope themselves).
_SCOPE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS metadata_scope (
    id INT AUTO_INCREMENT PRIMARY KEY,
    catalogPath VARCHAR(500) NOT NULL,
    changeKey VARCHAR(64) NOT NULL,
    scope VARCHAR(16) NOT NULL,
    author VARCHAR(255),
    updatedAt DATETIME NOT NULL,
    UNIQUE KEY uniq_scope_key (changeKey),
    KEY idx_scope_catalog (catalogPath)
)
"""


def _ensure_scope_table(engine: Engine) -> None:
    with engine.begin() as con:
        con.execute(text(_SCOPE_TABLE_DDL))


def load_scopes(engine: Engine, catalog_path: str) -> dict[str, str]:
    """The author's scope decisions for one MDim, keyed by changeKey → 'all' | 'scoped'."""
    _ensure_scope_table(engine)
    df = read_sql(
        "select changeKey, scope from metadata_scope where catalogPath = %(cp)s",
        engine=engine,
        params={"cp": catalog_path},
    )
    return {str(r["changeKey"]): str(r["scope"]) for _, r in df.iterrows()}


def set_scope(engine: Engine, catalog_path: str, change_key: str, scope: str, author: str | None) -> None:
    """Record the author's scope decision for a change."""
    _ensure_scope_table(engine)
    with engine.begin() as con:
        con.execute(
            text(
                "insert into metadata_scope (catalogPath, changeKey, scope, author, updatedAt) "
                "values (:cp, :ck, :sc, :au, :ts) "
                "on duplicate key update scope=values(scope), author=values(author), updatedAt=values(updatedAt)"
            ),
            {"cp": catalog_path, "ck": change_key, "sc": scope, "au": author, "ts": datetime.now(timezone.utc)},
        )


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
            # to_dict returns Hashable keys; our columns are all strings.
            rows[int(record["id"])] = {str(k): v for k, v in record.items()}
    return rows


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
    df = read_sql(
        f"""
        select c.id as chartId,
               cc.slug as slug,
               cc.full ->> '$.title' as title,
               cc.full ->> '$.subtitle' as subtitle,
               cc.full ->> '$.note' as note
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
