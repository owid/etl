"""Database access for the Metadata Diff app.

Fetches MDIM configs, variable metadata, and per-view chart configs from the two
environments being compared (staging = "source", production = "target"). Read-only.
"""

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.core import (
    METADATA_FIELDS,
    ViewBundle,
    ViewDiff,
    build_view_bundle,
    diff_views,
    indicator_identity,
    parse_chart_ref,
)
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


# Stored status for a change the reviewer has ticked off in a list. Distinct from the Summary tab's
# "approved"/"flagged", so a list tick is never mistaken for a sign-off.
REVIEWED = "reviewed"
# A row that exists only to hold a note. Needed because "reviewed" used to mean "a row exists": writing a
# note without ticking would otherwise have marked the item reviewed.
NOTED = "noted"
# The reviewer read this and does not want it to ship. A verdict, not an action: nothing in ETL or in
# grapher reads this table, so rejecting changes no text — it records what has to be undone, and the
# Summary tab turns those records into instructions to hand back to whoever is editing.
REJECTED = "rejected"
# The two states that mean a decision has been made, either way. Progress counts these; a note alone is
# not a decision.
DECIDED = (REVIEWED, REJECTED)


def load_item_notes(engine: Engine, prefix: str = "list:item:") -> list[dict[str, Any]]:
    """Every note and tick on the item surfaces, newest first — what the Summary tab consolidates.

    Keyed by surface rather than fetched per item: the tab's whole job is to gather what is scattered
    across the sections, and one query does it.

    The prefix is `list:item:`, not `item:`: `surface_key` namespaces every surface with `list:`, so a
    query for `item:%` matched nothing and the tab showed "nothing recorded" over a table with rows in it.
    """
    _ensure_review_table(engine)
    df = read_sql(
        "select catalogPath, changeKey, contentHash, status, comment, reviewer, updatedAt "
        "from metadata_review where catalogPath like %(pat)s order by updatedAt desc",
        engine=engine,
        params={"pat": f"{prefix}%"},
    )
    return [{str(k): v for k, v in row.to_dict().items()} for _, row in df.iterrows()]


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


def bulk_upsert_reviews(engine: Engine, rows: list[dict[str, Any]]) -> int:
    """Record many decisions at once — one statement, so a bulk verdict is not N round trips.

    Each row is {catalogPath, changeKey, contentHash, status, comment, reviewer}. An existing row for the
    same slot is overwritten, which is what makes "reject everything" idempotent: pressing it twice leaves
    the same rows rather than doubling anything, and it re-binds each to the text as it stands now.
    """
    if not rows:
        return 0
    _ensure_review_table(engine)
    now = datetime.now(timezone.utc)
    with engine.begin() as con:
        con.execute(
            text(
                "insert into metadata_review "
                "(catalogPath, changeKey, contentHash, status, comment, reviewer, updatedAt) "
                "values (:cp, :ck, :ch, :st, :cm, :rv, :ts) "
                "on duplicate key update contentHash=values(contentHash), status=values(status), "
                "comment=values(comment), reviewer=values(reviewer), updatedAt=values(updatedAt)"
            ),
            [
                {
                    "cp": row["catalogPath"],
                    "ck": row["changeKey"],
                    "ch": row["contentHash"],
                    "st": row["status"],
                    "cm": row.get("comment"),
                    "rv": row.get("reviewer"),
                    "ts": now,
                }
                for row in rows
            ],
        )
    return len(rows)


def clear_status(engine: Engine, surfaces: list[str], status: str) -> int:
    """Undo every decision of one kind on these surfaces, keeping what the reviewer wrote.

    A row carrying a note is demoted to `noted` rather than deleted — the same rule unticking follows,
    because a bulk undo must not throw away sentences somebody typed. Returns how many rows changed.
    """
    if not surfaces:
        return 0
    _ensure_review_table(engine)
    placeholders = ", ".join(f":s{i}" for i in range(len(surfaces)))
    params: dict[str, Any] = {f"s{i}": surface for i, surface in enumerate(surfaces)}
    params["st"] = status
    with engine.begin() as con:
        kept = con.execute(
            text(
                f"update metadata_review set status = '{NOTED}', updatedAt = now() "
                f"where status = :st and catalogPath in ({placeholders}) "
                "and comment is not null and comment != ''"
            ),
            params,
        ).rowcount
        dropped = con.execute(
            text(f"delete from metadata_review where status = :st and catalogPath in ({placeholders})"),
            params,
        ).rowcount
    return int(kept) + int(dropped)


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


def fetch_explorer_titles(engine: Engine) -> dict[str, str]:
    """Each published explorer's reader-facing name, by slug.

    Deliberately unordered. `config` is a large JSON blob, and an `order by` over rows carrying it
    overruns MySQL's sort buffer even when only a scalar is selected out of it.
    """
    df = read_sql(
        """
        select slug, config ->> '$.explorerTitle' as title
        from explorers
        where isPublished = 1
        """,
        engine=engine,
    )
    return {str(r["slug"]): str(r["title"]) for r in df.to_dict("records") if r.get("title")}


def fetch_indicator_config_texts(engine: Engine, catalog_paths: list[str]) -> dict[str, dict[str, Any]]:
    """The chart text a garden step authors for an indicator, by catalogPath.

    `presentation.grapher_config`'s title / subtitle / note do not live in the `variables` row — they are a
    config of their own, reached through `variables.patchConfigIdETL`. So none of the columns
    `fetch_variable_rows_by_path` reads move when a garden step rewords a subtitle, while every chart, MDim
    view and explorer view rendering that indicator does.
    """
    if not catalog_paths:
        return {}
    rows: dict[str, dict[str, Any]] = {}

    def fetch(chunk: list) -> list[Any]:
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"""
            select v.catalogPath as catalogPath,
                   cc.config ->> '$.title' as title,
                   cc.config ->> '$.subtitle' as subtitle,
                   cc.config ->> '$.note' as note
            from variables v
            join chart_configs cc on cc.id = v.patchConfigIdETL
            where v.catalogPath in ({placeholders})
            """,
            engine=engine,
            params=tuple(chunk),
        )
        return df.to_dict("records")

    for record in _fetch_chunks(fetch, sorted(set(catalog_paths))):
        rows[str(record["catalogPath"])] = {key: record.get(key) for key in ("title", "subtitle", "note")}
    return rows


def fetch_latest_dataset_versions(engine: Engine, shapes: list[tuple[str, str]]) -> dict[tuple[str, str], str]:
    """(namespace, dataset) -> the newest version of it this environment holds.

    What a version bump needs from the baseline: it serves `grapher/wb/2026-06-26/…` while the branch
    built `grapher/wb/2026-09-02/…`, so a query for the branch's paths returns nothing and every
    indicator of the dataset reads as new.

    Only the version is asked for, not the rows. Pulling every version's variables to find the pairing
    cost 73,000 rows and four seconds for two datasets — `world_bank_pip` alone has eight versions in
    production — and once the version is known the caller can name the exact paths it wants. Older
    versions linger here because charts still point at them, so the newest is the one production serves.
    """
    if not shapes:
        return {}
    out: dict[tuple[str, str], str] = {}
    for namespace, dataset in sorted(set(shapes)):
        df = read_sql(
            # The version is the third segment of `grapher/<ns>/<version>/<dataset>/<table>#<short>`.
            # Two patterns, because an indicator's path does not always carry a table:
            # `grapher/<ns>/<ver>/<dataset>#<short>` is the other live form, and matching only the first
            # left every indicator of such a dataset unpaired across a version bump — read as new, with
            # no text diff and nothing to attribute, on the workflow the pairing exists for.
            "select max(substring_index(substring_index(catalogPath, '/', 3), '/', -1)) as version "
            "from variables where catalogPath like %(with_table)s or catalogPath like %(no_table)s",
            engine=engine,
            params={
                "with_table": f"grapher/{namespace}/%/{dataset}/%",
                "no_table": f"grapher/{namespace}/%/{dataset}#%",
            },
        )
        version = df["version"].iloc[0] if len(df) else None
        if version:
            out[(namespace, dataset)] = str(version)
    return out


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
                   cc.config ->> '$.title' as title,
                   cc.config ->> '$.subtitle' as subtitle,
                   cc.config ->> '$.note' as note
            from chart_configs cc
            where cc.id in ({placeholders})
            """,
            engine=engine,
            params=tuple(chunk),
        )
        for record in df.to_dict("records"):
            out[str(record["id"])] = {k: record[k] for k in ("title", "subtitle", "note")}
    return out


_CHART_COLUMNS = """c.id as chartId,
               cc.slug as slug,
               c.publishedAt is not null as is_published,
               cc.config ->> '$.title' as title,
               cc.config ->> '$.subtitle' as subtitle,
               cc.config ->> '$.note' as note"""


def resolve_charts(engine: Engine, refs: list[str], include_drafts: bool = False) -> dict[str, dict[str, Any]]:
    """`resolve_chart` for a whole list at once: ref -> chart row, with unresolvable refs simply absent.

    Two queries for any number of charts — one keyed by id, one by slug — because a ref names a chart
    either way and the two cannot be looked up in the same `in (...)`. Chart by chart this was a query
    each, which is what made hashing every changed chart's verdict look unaffordable.
    """
    parsed = {ref: parse_chart_ref(ref) for ref in dict.fromkeys(refs)}
    ids = [cid for cid, _ in parsed.values() if cid is not None]
    slugs = [slug for cid, slug in parsed.values() if cid is None and slug]

    published = "" if include_drafts else " and c.publishedAt is not null"
    by_id: dict[int, dict[str, Any]] = {}
    by_slug: dict[str, dict[str, Any]] = {}
    for values, column in ((ids, "c.id"), (slugs, "cc.slug")):
        if not values:
            continue
        for chunk in _chunked(sorted(set(values))):
            placeholders = ", ".join(["%s"] * len(chunk))
            df = read_sql(
                f"""
                select {_CHART_COLUMNS}
                from charts c
                join chart_configs cc on cc.id = c.configId
                where {column} in ({placeholders}){published}
                """,
                engine=engine,
                params=tuple(chunk),
            )
            for record in df.to_dict("records"):
                row = {str(k): v for k, v in record.items()}
                by_id[int(row["chartId"])] = row
                if row.get("slug"):
                    by_slug[str(row["slug"])] = row

    out: dict[str, dict[str, Any]] = {}
    for ref, (chart_id, slug) in parsed.items():
        row = by_id.get(chart_id) if chart_id is not None else (by_slug.get(slug) if slug else None)
        if row is not None:
            out[ref] = dict(row)  # a copy: callers annotate it with `n_indicators` / `has_data_page`
    return out


def resolve_chart(engine: Engine, ref: str, include_drafts: bool = False) -> dict[str, Any] | None:
    """Resolve a chart from anything that names one — id, slug, grapher URL or admin URL.

    `include_drafts` for the lookup box, where a reviewer may well paste an unpublished chart and "not
    found" would be the wrong answer: the row carries `is_published` so the caller can say which it is.
    Left off by default, because every other caller counts reach and a draft is not reach.
    """
    return resolve_charts(engine, [ref], include_drafts=include_drafts).get(ref)


def fetch_chart_dimensions(engine: Engine, chart_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """chartId -> its `chart_dimensions` rows, in `order`, for many charts at once."""
    if not chart_ids:
        return {}
    out: dict[int, list[dict[str, Any]]] = {}
    for chunk in _chunked(sorted(set(int(cid) for cid in chart_ids))):
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"""
            select chartId, variableId, property
            from chart_dimensions
            where chartId in ({placeholders})
            order by chartId, `order`
            """,
            engine=engine,
            params=tuple(chunk),
        )
        for record in df.to_dict("records"):
            out.setdefault(int(record["chartId"]), []).append({str(k): v for k, v in record.items()})
    return out


def fetch_chart_indicator_paths_bulk(engine: Engine, chart_ids: list[int]) -> dict[int, list[str]]:
    """chartId -> the catalogPaths of every indicator it renders, for many charts at once."""
    if not chart_ids:
        return {}
    out: dict[int, list[str]] = {}
    for chunk in _chunked(sorted(set(int(cid) for cid in chart_ids))):
        placeholders = ", ".join(["%s"] * len(chunk))
        df = read_sql(
            f"""
            select distinct cd.chartId as chartId, v.catalogPath as catalogPath
            from chart_dimensions cd
            join variables v on v.id = cd.variableId
            where cd.chartId in ({placeholders}) and v.catalogPath is not null
            """,
            engine=engine,
            params=tuple(chunk),
        )
        for record in df.to_dict("records"):
            if record["catalogPath"]:
                out.setdefault(int(record["chartId"]), []).append(str(record["catalogPath"]))
    return out


def fetch_chart_indicator_paths(engine: Engine, chart_id: int) -> list[str]:
    """The catalogPaths of every indicator a chart renders — what decides whether it was compared at all.

    A chart outside the branch's scope was never compared, and saying "no metadata change" about it would
    be a claim the tool has not tested.
    """
    return fetch_chart_indicator_paths_bulk(engine, [int(chart_id)]).get(int(chart_id), [])


def build_chart_bundles(
    engine: Engine,
    refs: list[str],
    pinned: dict[str, str] | None = None,
    include_drafts: bool = False,
) -> dict[str, tuple[ViewBundle, dict[str, Any]]]:
    """Build a single-"view" bundle for each of many standalone charts: y-indicator metadata + FAUST.

    ref -> (bundle, chart), with charts that cannot be resolved simply absent. Each chart dict carries
    `n_indicators` and `has_data_page` — grapher renders a data page (and thus this WYSK text) only for
    single-indicator charts; a scatter/multi-series chart has none. The bundles have empty dimensions, so
    each matches its baseline counterpart in `diff_views`.

    `pinned` maps a ref to the catalogPath the metadata should be read from. Without one the primary y is
    used, which is the one a data page renders. That is the right default and the wrong answer for a
    multi-series chart in the changed list: it is there because *some* indicator of it moved, and the
    caller may know which. Read by catalogPath rather than by id, the only identifier that means the same
    thing in both environments — and where this environment does not hold that path, the primary y stands
    in. That substitution is a different indicator, so a caller comparing two environments has to check
    the bundle's `catalog_path` before trusting the result; `compare_charts` does.

    Four queries for any number of charts, whatever the mix of pins: charts, their dimensions, the pinned
    variables by path, the rest by id.
    """
    pinned = pinned or {}
    charts = resolve_charts(engine, refs, include_drafts=include_drafts)
    dims_by_chart = fetch_chart_dimensions(engine, [int(c["chartId"]) for c in charts.values()])

    # Plan every lookup before issuing any, so the two variable fetches are one query each. The primary y
    # is resolved even for a pinned chart: it is what the pin falls back to, and an id already in a
    # batched `in (...)` costs nothing to carry.
    plan: dict[str, tuple[str | None, int | None]] = {}
    for ref, chart in charts.items():
        rows = dims_by_chart.get(int(chart["chartId"]), [])
        chart["n_indicators"] = len({r["variableId"] for r in rows})
        chart["has_data_page"] = chart["n_indicators"] == 1
        y_rows = [r for r in rows if r.get("property") == "y"] or rows
        plan[ref] = (pinned.get(ref), int(y_rows[0]["variableId"]) if y_rows else None)

    rows_by_path = fetch_variable_rows_by_path(engine, [p for p, _ in plan.values() if p])
    rows_by_id = fetch_variable_rows(engine, [vid for _, vid in plan.values() if vid is not None])

    out: dict[str, tuple[ViewBundle, dict[str, Any]]] = {}
    for ref, chart in charts.items():
        path, vid = plan[ref]
        variable_row = rows_by_path.get(path) if path else None
        if variable_row is None and vid is not None:
            variable_row = rows_by_id.get(vid)
        chart_config = {"title": chart.get("title"), "subtitle": chart.get("subtitle"), "note": chart.get("note")}
        out[ref] = (
            build_view_bundle(
                view={"dimensions": {}}, config_metadata=None, variable_row=variable_row, chart_config=chart_config
            ),
            chart,
        )
    return out


def build_chart_bundle(
    engine: Engine, ref: str, catalog_path: str | None = None, include_drafts: bool = False
) -> tuple[ViewBundle, dict[str, Any]] | None:
    """One chart's bundle: `build_chart_bundles` for a single ref, so the two can never disagree."""
    pinned = {ref: catalog_path} if catalog_path else None
    return build_chart_bundles(engine, [ref], pinned, include_drafts=include_drafts).get(ref)


@dataclass
class ChartComparison:
    """One chart's two bundles and the diff between them — everything a verdict on it is built from."""

    chart: dict[str, Any]
    source: ViewBundle
    target: ViewBundle | None
    diff: ViewDiff
    # The indicator the comparison ended up reading, when it is not the primary y. Only a multi-series
    # chart has one, and only when the primary y turned out not to be the indicator that moved.
    pinned_path: str | None = None


def compare_charts(
    source_engine: Engine,
    target_engine: Engine,
    refs: list[str],
    changed_paths: Any = frozenset(),
    baseline_paths: dict[str, str] | None = None,
    include_drafts: bool = False,
) -> dict[str, ChartComparison]:
    """Compare each chart's current text against the baseline's — in bulk, in a fixed number of queries.

    The one place a chart's changed fields are decided. Both callers go through it: the chart page, which
    renders the comparison, and `item_index`, which hashes it so a stored verdict reopens when the text
    moves. They used to be two code paths over the same question, and a verdict hashed by one of them
    could not be checked by the other — which is why chart verdicts alone never reopened.

    `changed_paths` is the set of catalogPaths this branch actually changed (`indicator_changes().diffs`).
    It is only consulted for the second pass: a multi-series chart whose primary y shows nothing is in the
    changed list because some *other* indicator of it moved, so the comparison is rebuilt on the first of
    those. Passed in rather than fetched here, to keep this free of the app's caching layer.

    `baseline_paths` is `indicator_changes().across_versions` — source path -> the path the baseline holds
    the same indicator at. A pin names an indicator on *this branch*, and a version bump moves it, so the
    baseline resolves nothing under that path and would read the chart's primary y instead. Reusing the
    pairing the indicator comparison already made keeps the two surfaces saying the same thing about which
    baseline indicator a branch's one corresponds to.
    """
    src = build_chart_bundles(source_engine, refs, include_drafts=include_drafts)
    tgt = build_chart_bundles(target_engine, list(src), include_drafts=include_drafts)

    out: dict[str, ChartComparison] = {}
    retry: dict[str, int] = {}
    for ref, (source, chart) in src.items():
        target = tgt[ref][0] if ref in tgt else None
        diff = diff_views([source], [target] if target is not None else [])[0]
        out[ref] = ChartComparison(chart=chart, source=source, target=target, diff=diff)
        # Retried when the series read here is not one this branch changed — not merely when it shows
        # nothing. A primary y whose own text lags the baseline has a difference of its own, and reporting
        # that as the chart's change describes a series nobody in this PR edited while the edited one goes
        # unread, with the verdict hashed on the wrong wording. Only a multi-indicator chart has another
        # series to move to, and a single-indicator chart's primary *is* what its data page renders.
        if int(chart.get("n_indicators") or 0) > 1 and (source.catalog_path or "") not in changed_paths:
            retry[ref] = int(chart["chartId"])

    if not retry or not changed_paths:
        return out

    # Second pass: one changed indicator, not all of them — a chart carrying edits to two of its series
    # still shows the first. Sorted, so the chart reviews the same series on every run and the verdict
    # recorded against it keeps meaning the same thing.
    paths_by_chart = fetch_chart_indicator_paths_bulk(source_engine, list(retry.values()))
    pinned: dict[str, str] = {}
    for ref, chart_id in retry.items():
        for path in sorted(paths_by_chart.get(chart_id, [])):
            if path in changed_paths:
                pinned[ref] = path
                break
    if not pinned:
        return out

    # The baseline is pinned to its own path for the same indicator, not to the branch's: a version bump
    # moves `grapher/wb/2026-09-02/…#mean` to what the baseline still serves as `…/2026-06-26/…#mean`.
    target_pins = {ref: (baseline_paths or {}).get(path, path) for ref, path in pinned.items()}
    src2 = build_chart_bundles(source_engine, list(pinned), pinned, include_drafts=include_drafts)
    tgt2 = build_chart_bundles(target_engine, list(src2), target_pins, include_drafts=include_drafts)
    for ref, (source, chart) in src2.items():
        target = tgt2[ref][0] if ref in tgt2 else None
        # Only where the baseline resolved the pin to the same indicator, whatever version it holds it at.
        # Where it did not — a renamed table, an indicator this branch introduced — `build_chart_bundles`
        # stands the chart's primary y in, and this pass would compare the changed series against a
        # different indicator: fields nobody edited, and a hash binding the chart's verdict to a pairing
        # nobody made. Those keep the first pass's like-for-like comparison instead.
        if target is not None and (
            not target.catalog_path or indicator_identity(target.catalog_path) != indicator_identity(pinned[ref])
        ):
            continue
        out[ref] = ChartComparison(
            chart=chart,
            source=source,
            target=target,
            diff=diff_views([source], [target] if target is not None else [])[0],
            pinned_path=pinned[ref],
        )
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


def fetch_mdim_redirected_charts(engine: Engine) -> dict[str, str]:
    """Chart slug -> the MDim its URL redirects to, for charts superseded by an MDim.

    The chart→MDim migration leaves the chart row published and redirects its URL, so `charts` alone still
    calls it a live chart while `/grapher/<slug>` serves an MDim view. Measured on production: 267 such
    redirects, every one of them unconditional.

    Only unconditional redirects count. A row carrying `sourceQueryParams` redirects one parameterised URL
    and leaves the plain one serving the chart — that is how the explorer view redirects are written (981
    of them), and treating one as "this chart is gone" would hide a page readers can still open.
    """
    df = read_sql(
        """
        select r.source as source, coalesce(m.slug, m.catalogPath) as mdim
        from multi_dim_redirects r
        left join multi_dim_data_pages m on m.id = r.multiDimId
        where r.sourceQueryParams is null and r.source like '/grapher/%%'
        """,
        engine=engine,
    )
    return {
        str(row["source"]).removeprefix("/grapher/"): str(row["mdim"] or "an MDim") for row in df.to_dict("records")
    }
