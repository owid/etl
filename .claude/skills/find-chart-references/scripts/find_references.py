"""Find every OWID surface that references a chart, indicator, MDIM, or explorer.

READ-ONLY, pure SQL — works with read-only credentials (no ADMIN_API_KEY needed).

One question, one answer, for any grapher object: *what would break, go stale, or
need editing if this changed or went away?* Each finding carries a `kind` that says
how the surface holds the object, which is what decides whether a fix is needed:

  render  the surface resolves the object and draws it (a chart on an indicator, an
          MDIM view, an explorer view). Changing the object changes what readers see.
  embed   the surface embeds it by id/slug and renders its config directly (article
          chart blocks, data insights, static viz, explorers). A URL redirect does
          NOT fix these. A narrative chart counts only when its parent is an MDIM
          view — one parented to a chart carries a copy of the config and so is a
          `link`.
  link    a hyperlink. A redirect covers it; the href is still worth updating.

Subjects (at least one; they can be combined):
    --chart-ids 123,456        --chart-slugs a,b,c
    --variable-ids 1,2         --dataset-id 6789
    --mdim <slug|catalogPath>  --explorer <slug>

Chart subjects are expanded to every old slug that still reaches them
(`chart_slug_redirects`), because references written before a rename point at the
old slug and are otherwise missed.

`--transitive` adds a second hop for INDICATOR subjects only: after finding the charts
that render an indicator, also find the articles that reference those charts. Off by
default — it multiplies the sweep on widely-charted datasets. It does nothing for a
`--mdim` or `--explorer` subject: an MDIM's own references are all direct, and the
indirect hop from its views' indicators is `--variable-ids`/`--dataset-id` work.

Usage:
    ENV_FILE=<creds> DATA_API_ENV=production .venv/bin/python \
        .claude/skills/find-chart-references/scripts/find_references.py \
        --chart-slugs life-expectancy --json out.json
"""

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

from etl.config import OWID_ENV

RENDER, EMBED, LINK = "render", "embed", "link"

# Frozen snapshots of the site. A link into one keeps rendering whatever it captured, so it
# is never part of a blast radius — every raw-URL sweep has to drop it, or an archived copy
# of a page is reported as a live reference somebody has to migrate.
ARCHIVE_HOST = "archive.ourworldindata.org"

# Surfaces this run could NOT sweep, and subjects that did not resolve. An empty result for
# any of these means UNKNOWN, not "nothing references it", so they must survive past stdout:
# `--gaps-json` hands them to whatever wraps this script (audit_references.py puts them in
# its own Unverified bucket) instead of leaving a truncated sweep looking complete.
COVERAGE_GAPS: list[str] = []


def gap(message: str) -> None:
    """Record a coverage gap and say so on stdout."""
    COVERAGE_GAPS.append(message)
    print(f"  COVERAGE GAP: {message}")


COLUMNS = [
    "subject_type", "subject", "subject_id", "surface", "kind",
    "where", "where_path", "context", "query_string", "text", "published",
]  # fmt: skip


def rec(subject_type, subject, subject_id, surface, kind, where, where_path="",
        context="", query_string="", text="", published=True) -> dict:  # fmt: skip
    return {
        "subject_type": subject_type,
        "subject": subject,
        "subject_id": subject_id,
        "surface": surface,
        "kind": kind,
        "where": where,
        "where_path": where_path,
        "context": context,
        "query_string": query_string or "",
        "text": text or "",
        "published": bool(published),
    }


# ----- Subject resolution ---------------------------------------------------------


def resolve_chart_subjects(chart_ids: list[int], chart_slugs: list[str]) -> dict[str, dict]:
    """Return {slug (current or old) -> {"id", "slug"}} for the requested charts."""
    if not chart_ids and not chart_slugs:
        return {}
    where, params = [], {}
    if chart_ids:
        where.append("c.id IN %(ids)s")
        params["ids"] = tuple(chart_ids)
    if chart_slugs:
        where.append("cc.slug IN %(slugs)s")
        # A requested slug can itself be an old one that still reaches the chart — that is
        # exactly the URL someone pastes when auditing. Resolving aliases here rather than
        # only expanding them afterwards is what makes a sole old-slug subject work: the
        # expansion below runs off the charts this query found, so without it nothing
        # resolves and the live URL is reported as unresolved and never swept.
        where.append("c.id IN (SELECT chart_id FROM chart_slug_redirects WHERE slug IN %(slugs)s)")
        params["slugs"] = tuple(chart_slugs)
    df = OWID_ENV.read_sql(
        f"SELECT c.id, cc.slug FROM charts c JOIN chart_configs cc ON cc.id = c.configId WHERE {' OR '.join(where)}",
        params=params,
    )
    charts = {int(r["id"]): r["slug"] for r in df.to_dict("records") if r["slug"]}
    by_slug = {slug: {"id": cid, "slug": slug} for cid, slug in charts.items()}
    if charts:
        # Old slugs still reach the chart, so references may point at them.
        old = OWID_ENV.read_sql(
            "SELECT csr.slug AS old_slug, csr.chart_id FROM chart_slug_redirects csr WHERE csr.chart_id IN %(ids)s",
            params={"ids": tuple(charts)},
        )
        for r in old.to_dict("records"):
            cid = int(r["chart_id"])
            by_slug[r["old_slug"]] = {"id": cid, "slug": charts[cid]}

    # A subject that resolves to nothing is UNKNOWN, not "nothing references it". Report it
    # loudly — otherwise a typo, a deletion, or a mixed request reads as a clean blast-radius
    # result. Checked after the old-slug expansion, so passing an old slug isn't flagged.
    missing = [str(i) for i in chart_ids if i not in charts] + [s for s in chart_slugs if s not in by_slug]
    if missing:
        gap(f"{len(missing)} chart subject(s) did not resolve and were NOT swept: {sorted(missing)}")
        print("           A blank result for these means UNKNOWN, not 'nothing references them'.")
    return by_slug


def resolve_variable_ids(variable_ids: list[int], dataset_id: int | None) -> list[int]:
    """Requested indicator ids, checked against `variables`.

    An id that does not exist matches nothing in every downstream query, so the sweep
    would end with `references: 0` — the same output as a real indicator nothing points
    at. Report the unresolved ids, as `resolve_chart_subjects` does for charts, so a
    typo or a deleted indicator can never read as a clean blast radius.
    """
    ids = set(variable_ids)
    if ids:
        found = OWID_ENV.read_sql("SELECT id FROM variables WHERE id IN %(v)s", params={"v": tuple(sorted(ids))})
        resolved = {int(i) for i in found["id"]}
        missing = sorted(ids - resolved)
        if missing:
            gap(f"{len(missing)} requested indicator id(s) do not exist and were NOT swept: {missing}")
            print("           A blank result for these means UNKNOWN, not 'nothing references them'.")
        if not resolved and dataset_id is None:
            raise SystemExit("None of the requested --variable-ids exist in the grapher DB — nothing to sweep.")
        ids = resolved
    if dataset_id is not None:
        df = OWID_ENV.read_sql("SELECT id FROM variables WHERE datasetId = %(d)s", params={"d": dataset_id})
        if df.empty:
            gap(f"dataset {dataset_id} has no indicators — check the dataset id; nothing was swept for it")
        ids |= {int(i) for i in df["id"]}
    return sorted(ids)


# ----- Chart surfaces -------------------------------------------------------------


def sweep_gdoc_links(by_slug: dict[str, dict]) -> list[dict]:
    """Article links and embeds (posts_gdocs_links).

    `componentType` is the discriminator: a `span-*` value is a hyperlink in prose;
    anything else is a block-level component that renders the chart itself.
    """
    slugs = tuple(by_slug)
    df = OWID_ENV.read_sql(
        "SELECT pg.slug AS post_slug, pg.type AS post_type, pg.published, "
        "       pgl.target, pgl.queryString, pgl.componentType, pgl.text "
        "FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        "WHERE pgl.target IN %(s)s AND pgl.linkType IN ('grapher', 'guided-chart') ORDER BY pg.slug",
        params={"s": slugs},
    )
    out = []
    for r in df.to_dict("records"):
        component = r["componentType"] or ""
        subj = by_slug[r["target"]]
        out.append(
            rec(
                "chart",
                r["target"],
                subj["id"],
                "gdoc",
                LINK if component.startswith("span-") else EMBED,
                r["post_slug"],
                f"/{r['post_slug']}",
                f"{component or 'unknown'} ({r['post_type']})",
                r["queryString"],
                r["text"],
                r["published"],
            )  # fmt: skip
        )
    return out


def url_query(target: str, fallback: str = "") -> str:
    """Query string of a raw pasted URL, falling back to the column when it has none.

    `posts_gdocs_links.queryString` is populated for grapher-item links, but a
    `linkType='url'` row keeps its query inside `target` — so reading the column alone
    drops exactly the parameters that select a view, and the reported reference collapses
    to the base page.
    """
    without_fragment = (target or "").split("#", 1)[0]
    return without_fragment.split("?", 1)[1] if "?" in without_fragment else fallback


def sweep_gdoc_url_links(by_slug: dict[str, dict]) -> list[dict]:
    """Raw URL links (linkType='url') pointing at a live grapher page."""
    slugs = tuple(by_slug)
    clauses = " OR ".join(f"pgl.target LIKE %(t{i})s" for i in range(len(slugs)))
    params = {f"t{i}": f"%/grapher/{s}%" for i, s in enumerate(slugs)}
    df = OWID_ENV.read_sql(
        "SELECT pg.slug AS post_slug, pg.type AS post_type, pg.published, "
        "       pgl.target, pgl.queryString, pgl.componentType, pgl.text "
        f"FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        f"WHERE pgl.linkType = 'url' AND ({clauses})",
        params=params,
    )
    out = []
    for r in df.to_dict("records"):
        target = r["target"] or ""
        if ARCHIVE_HOST in target:
            continue  # archived snapshots are frozen by design
        slug = target.split("/grapher/", 1)[-1].split("?")[0].split("#")[0].rstrip("/")
        if slug not in by_slug:
            continue
        component = r["componentType"] or ""
        out.append(
            rec(
                "chart",
                slug,
                by_slug[slug]["id"],
                "gdoc (url link)",
                LINK if component.startswith("span-") else EMBED,
                r["post_slug"],
                f"/{r['post_slug']}",
                f"{component or 'unknown'} ({r['post_type']})",
                url_query(target, r["queryString"] or ""),
                r["text"],
                r["published"],
            )  # fmt: skip
        )
    return out


def sweep_explorer_charts(by_slug: dict[str, dict]) -> list[dict]:
    """Explorers referencing charts by id (explorer_charts)."""
    ids = tuple({v["id"] for v in by_slug.values()})
    df = OWID_ENV.read_sql(
        "SELECT ec.chartId AS chart_id, ec.explorerSlug, e.isPublished, cc.slug "
        "FROM explorer_charts ec JOIN explorers e ON e.slug = ec.explorerSlug "
        "JOIN charts c ON c.id = ec.chartId JOIN chart_configs cc ON cc.id = c.configId "
        "WHERE ec.chartId IN %(ids)s ORDER BY e.slug",
        params={"ids": ids},
    )
    return [
        rec(
            "chart",
            r["slug"],
            int(r["chart_id"]),
            "explorer",
            EMBED,
            r["explorerSlug"],
            f"/explorers/{r['explorerSlug']}",
            "references the chart by id",
            published=r["isPublished"],
        )  # fmt: skip
        for r in df.to_dict("records")
    ]


def sweep_narrative_charts_of_charts(by_slug: dict[str, dict]) -> list[dict]:
    """Narrative charts whose parent is one of these charts.

    Classified as `link`, not `embed`: a narrative chart owns a materialized full config
    of its own (written at creation) and renders from that, so unpublishing the parent
    does not touch what readers see. The parent is joined in only to build the "Explore
    the data" href from its slug, which a redirect covers.

    The href carries `queryParamsForParentChart`, so it is still worth checking — those
    params ride along to the target and can collide with an MDIM view's dimensions.
    """
    ids = tuple({v["id"] for v in by_slug.values()})
    df = OWID_ENV.read_sql(
        "SELECT nc.id, nc.name, nc.parentChartId AS chart_id, nc.queryParamsForParentChart AS qp, cc.slug "
        "FROM narrative_charts nc JOIN charts c ON c.id = nc.parentChartId "
        "JOIN chart_configs cc ON cc.id = c.configId WHERE nc.parentChartId IN %(ids)s ORDER BY nc.name",
        params={"ids": ids},
    )
    out = []
    for r in df.to_dict("records"):
        params = parse_json_obj(r["qp"])
        out.append(
            rec(
                "chart",
                r["slug"],
                int(r["chart_id"]),
                "narrative chart",
                LINK,
                r["name"],
                f"/admin/narrative-charts/{r['id']}/edit",
                'renders its own config; only its "Explore the data" link uses the parent slug',
                "&".join(f"{k}={v}" for k, v in sorted(params.items())),
            )  # fmt: skip
        )
    return out


def sweep_data_insights(by_slug: dict[str, dict]) -> list[dict]:
    """Data insights store the chart in content->>'$."grapher-url"', not in posts_gdocs_links."""
    slugs = tuple(by_slug)
    df = OWID_ENV.read_sql(
        "SELECT post_slug, published, grapher_url, slug FROM ("
        "  SELECT pg.slug AS post_slug, pg.published, pg.content->>'$.\"grapher-url\"' AS grapher_url,"
        "         SUBSTRING_INDEX(SUBSTRING_INDEX(pg.content->>'$.\"grapher-url\"', '/grapher/', -1), '?', 1) AS slug"
        "  FROM posts_gdocs pg WHERE pg.type = 'data-insight'"
        "    AND pg.content->>'$.\"grapher-url\"' IS NOT NULL"
        ") t WHERE slug IN %(s)s",
        params={"s": slugs},
    )
    out = []
    for r in df.to_dict("records"):
        url = r["grapher_url"] or ""
        out.append(
            rec(
                "chart",
                r["slug"],
                by_slug[r["slug"]]["id"],
                "data insight",
                EMBED,
                r["post_slug"],
                f"/data-insights/{r['post_slug']}",
                "grapher-url in the insight's front matter",
                url.split("?", 1)[1] if "?" in url else "",
                published=r["published"],
            )  # fmt: skip
        )
    return out


def sweep_static_viz(by_slug: dict[str, dict]) -> list[dict]:
    df = OWID_ENV.read_sql(
        "SELECT sv.grapherSlug AS slug, sv.id FROM static_viz sv WHERE sv.grapherSlug IN %(s)s",
        params={"s": tuple(by_slug)},
    )
    return [
        rec(
            "chart",
            r["slug"],
            by_slug[r["slug"]]["id"],
            "static viz",
            EMBED,
            f"static_viz #{r['id']}",
            context="image built from the chart",
        )  # fmt: skip
        for r in df.to_dict("records")
    ]


def sweep_key_charts(by_slug: dict[str, dict]) -> list[dict]:
    """Topic-page key-chart slots. MDIM tags don't participate in that rotation."""
    ids = tuple({v["id"] for v in by_slug.values()})
    df = OWID_ENV.read_sql(
        "SELECT ct.chartId AS chart_id, t.name AS tag, ct.keyChartLevel, cc.slug "
        "FROM chart_tags ct JOIN tags t ON t.id = ct.tagId "
        "JOIN charts c ON c.id = ct.chartId JOIN chart_configs cc ON cc.id = c.configId "
        "WHERE ct.chartId IN %(ids)s AND ct.keyChartLevel > 0 ORDER BY t.name",
        params={"ids": ids},
    )
    return [
        rec(
            "chart",
            r["slug"],
            int(r["chart_id"]),
            "key chart",
            RENDER,
            r["tag"],
            context=f"keyChartLevel={r['keyChartLevel']}",
        )  # fmt: skip
        for r in df.to_dict("records")
    ]


def sweep_wordpress(by_slug: dict[str, dict]) -> list[dict]:
    """Legacy WordPress posts. The table may be absent on newer environments — fail open."""
    slugs = tuple(by_slug)
    clauses = " OR ".join(f"pl.target LIKE %(t{i})s" for i in range(len(slugs)))
    params = {f"t{i}": f"%/grapher/{s}%" for i, s in enumerate(slugs)}
    try:
        df = OWID_ENV.read_sql(
            "SELECT p.slug AS post_slug, pl.target FROM posts_links pl JOIN posts p ON p.id = pl.sourceId "
            f"WHERE p.status = 'publish' AND ({clauses})",
            params=params,
        )
    except Exception as e:  # noqa: BLE001 - optional legacy surface; report as a coverage gap
        gap(f"WordPress sweep skipped ({type(e).__name__}) — legacy posts were NOT checked")
        return []
    out = []
    for r in df.to_dict("records"):
        target = r["target"] or ""
        slug = target.split("/grapher/", 1)[-1].split("?")[0].split("#")[0].rstrip("/")
        if slug not in by_slug:
            continue
        out.append(
            rec(
                "chart",
                slug,
                by_slug[slug]["id"],
                "wordpress",
                LINK,
                r["post_slug"],
                f"/{r['post_slug']}",
                "legacy post link",
                target.split("?", 1)[1] if "?" in target else "",
            )  # fmt: skip
        )
    return out


# ----- Indicator surfaces ---------------------------------------------------------


def parse_json_obj(value) -> dict:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def sweep_charts_of_indicators(variable_ids: list[int]) -> list[dict]:
    """Charts rendering these indicators. Drafts can have no slug — they still render.

    `charts.publishedAt` is the first-publish timestamp and survives an unpublish, so it
    would report already-retired charts as live and have their references graded for
    reader impact. The live state is `isPublished` in the config.
    """
    df = OWID_ENV.read_sql(
        "SELECT DISTINCT cd.variableId, c.id AS chart_id, cc.slug, "
        "COALESCE(cc.full->>'$.isPublished', 'false') = 'true' AS published "
        "FROM chart_dimensions cd JOIN charts c ON c.id = cd.chartId "
        "JOIN chart_configs cc ON cc.id = c.configId WHERE cd.variableId IN %(ids)s ORDER BY cc.slug",
        params={"ids": tuple(variable_ids)},
    )
    out = []
    for r in df.to_dict("records"):
        slug = r["slug"] or ""
        out.append(
            rec(
                "indicator",
                str(r["variableId"]),
                int(r["variableId"]),
                "chart",
                RENDER,
                slug or f"chart {r['chart_id']} (no slug)",
                f"/grapher/{slug}" if slug else "",
                f"chart id {r['chart_id']}",
                published=r["published"],
            )  # fmt: skip
        )
    return out


def entry_variable_id(entry, by_path: dict[str, int]) -> int | None:
    """A `views[].indicators.*` entry -> variable id, tolerating every stored shape.

    Entries are stored as an int id, a dict carrying `id`, a dict carrying only
    `catalogPath`, or a bare catalogPath string. `by_path` maps the catalog paths of the
    *requested* variables, so a path we don't care about resolves to None either way.
    """
    if isinstance(entry, dict):
        if entry.get("id") is not None:
            return int(entry["id"])
        path = entry.get("catalogPath")
        return by_path.get(path) if path else None
    if isinstance(entry, bool):  # bool is an int subclass — never a variable id
        return None
    if isinstance(entry, int):
        return entry
    if isinstance(entry, str):
        return by_path.get(entry)
    return None


def catalog_paths_of(variable_ids: list[int]) -> dict[str, int]:
    """{catalogPath -> variable id} for the requested variables, for catalogPath-only configs."""
    df = OWID_ENV.read_sql(
        "SELECT id, catalogPath FROM variables WHERE id IN %(ids)s AND catalogPath IS NOT NULL",
        params={"ids": tuple(variable_ids)},
    )
    return {str(p): int(i) for i, p in zip(df["id"], df["catalogPath"])}


def sweep_mdim_views_of_indicators(variable_ids: list[int]) -> list[dict]:
    """MDIM views rendering these indicators.

    multi_dim_x_chart_configs.variableId records only the FIRST y indicator, so a view
    plotting several indicators is invisible to that join — scan the stored configs as
    well. Findings are keyed by (mdim, view, indicator), not by view: one view can render
    several of the requested indicators, and each of them is a separate reference. Keying
    by view alone would let the join's single row mask every other indicator in it.
    """
    ids = set(variable_ids)
    by_path = catalog_paths_of(variable_ids)
    out: list[dict] = []
    seen: set[tuple] = set()

    def emit(slug, catalog_path, published, view_id, variable_id: int) -> None:
        if (slug, view_id, variable_id) in seen:
            return
        seen.add((slug, view_id, variable_id))
        out.append(
            rec(
                "indicator",
                str(variable_id),
                variable_id,
                "mdim view",
                RENDER,
                f"{slug}:{view_id}",
                f"/grapher/{slug}",
                catalog_path,
                published=published,
            )  # fmt: skip
        )

    df = OWID_ENV.read_sql(
        "SELECT mx.variableId, md.slug, md.catalogPath, md.published, mx.viewId "
        "FROM multi_dim_x_chart_configs mx JOIN multi_dim_data_pages md ON md.id = mx.multiDimId "
        "WHERE mx.variableId IN %(ids)s",
        params={"ids": tuple(variable_ids)},
    )
    for r in df.to_dict("records"):
        emit(r["slug"], r["catalogPath"], r["published"], r["viewId"], int(r["variableId"]))

    configs = OWID_ENV.read_sql("SELECT slug, catalogPath, published, config FROM multi_dim_data_pages")
    for row in configs.to_dict("records"):
        cfg = row["config"]
        cfg = json.loads(cfg) if isinstance(cfg, str) else (cfg or {})
        for view in cfg.get("views", []):
            view_id = "__".join(f"{k}={v}" for k, v in sorted((view.get("dimensions") or {}).items()))
            indicators = view.get("indicators") or {}
            for slot in ("y", "x", "size", "color"):
                entries = indicators.get(slot)
                if entries is None:
                    continue
                for e in entries if isinstance(entries, list) else [entries]:
                    vid = entry_variable_id(e, by_path)
                    if vid in ids:
                        emit(row["slug"], row["catalogPath"], row["published"], view_id, int(vid))
    return out


def sweep_explorer_views_of_indicators(variable_ids: list[int]) -> list[dict]:
    """Explorers built on these indicators, aggregated per explorer (not per view)."""
    try:
        df = OWID_ENV.read_sql(
            "SELECT ev.variableId, ev.explorerSlug, e.isPublished, COUNT(*) AS n "
            "FROM explorer_variables ev JOIN explorers e ON e.slug = ev.explorerSlug "
            "WHERE ev.variableId IN %(ids)s GROUP BY ev.variableId, ev.explorerSlug, e.isPublished",
            params={"ids": tuple(variable_ids)},
        )
    except Exception as e:  # noqa: BLE001 - table shape varies across environments
        gap(f"explorer_variables sweep skipped ({type(e).__name__}) — explorers on these indicators were NOT checked")
        return []
    return [
        rec(
            "indicator",
            str(r["variableId"]),
            int(r["variableId"]),
            "explorer",
            RENDER,
            r["explorerSlug"],
            f"/explorers/{r['explorerSlug']}",
            f"{r['n']} view(s)",
            published=r["isPublished"],
        )  # fmt: skip
        for r in df.to_dict("records")
    ]


# ----- MDIM / explorer subjects ---------------------------------------------------


def sweep_mdim_subject(mdim: str) -> list[dict]:
    df = OWID_ENV.read_sql(
        "SELECT id, slug, catalogPath FROM multi_dim_data_pages WHERE slug = %(m)s OR catalogPath = %(m)s",
        params={"m": mdim},
    )
    if df.empty:
        print(f"  (MDIM not found: {mdim})")
        return []
    row = df.to_dict("records")[0]
    slug, mdim_id = row["slug"], int(row["id"])
    out = []

    links = OWID_ENV.read_sql(
        "SELECT pg.slug AS post_slug, pg.type AS post_type, pg.published, "
        "       pgl.queryString, pgl.componentType, pgl.text "
        "FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        "WHERE pgl.target = %(s)s AND pgl.linkType IN ('grapher', 'guided-chart')",
        params={"s": slug},
    )
    for r in links.to_dict("records"):
        component = r["componentType"] or ""
        out.append(
            rec(
                "mdim",
                slug,
                mdim_id,
                "gdoc",
                LINK if component.startswith("span-") else EMBED,
                r["post_slug"],
                f"/{r['post_slug']}",
                f"{component or 'unknown'} ({r['post_type']})",
                r["queryString"],
                r["text"],
                r["published"],
            )  # fmt: skip
        )

    # An article can paste the MDIM's URL instead of linking it as a grapher item, which
    # lands in the same table as `linkType='url'`. The chart sweep already covers that row
    # shape; without the counterpart here a direct --mdim report can miss reader-facing
    # references entirely. The SQL prefilter is loose, so the path segment is re-checked in
    # Python — otherwise a longer slug that merely starts with this one matches too.
    raw = OWID_ENV.read_sql(
        "SELECT pg.slug AS post_slug, pg.type AS post_type, pg.published, "
        "       pgl.target, pgl.queryString, pgl.componentType, pgl.text "
        "FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        "WHERE pgl.linkType = 'url' AND pgl.target LIKE %(t)s",
        params={"t": f"%/grapher/{slug}%"},
    )
    exact = re.compile(rf"/grapher/{re.escape(slug)}(?:[?#/]|$)")
    for r in raw.to_dict("records"):
        target = r["target"] or ""
        if ARCHIVE_HOST in target or not exact.search(target):
            continue
        component = r["componentType"] or ""
        out.append(
            rec(
                "mdim",
                slug,
                mdim_id,
                "gdoc",
                LINK if component.startswith("span-") else EMBED,
                r["post_slug"],
                f"/{r['post_slug']}",
                f"raw URL, {component or 'unknown'} ({r['post_type']})",
                url_query(target, r["queryString"] or ""),
                r["text"],
                r["published"],
            )  # fmt: skip
        )

    nc = OWID_ENV.read_sql(
        "SELECT nc.id, nc.name, mx.viewId FROM narrative_charts nc "
        "JOIN multi_dim_x_chart_configs mx ON mx.id = nc.parentMultiDimXChartConfigId "
        "WHERE mx.multiDimId = %(id)s",
        params={"id": mdim_id},
    )
    for r in nc.to_dict("records"):
        out.append(
            rec(
                "mdim",
                slug,
                mdim_id,
                "narrative chart",
                EMBED,
                r["name"],
                f"/admin/narrative-charts/{r['id']}/edit",
                f"pinned to view {r['viewId']} — blocks re-publish if that view disappears",
            )  # fmt: skip
        )

    redirects = OWID_ENV.read_sql(
        "SELECT source FROM multi_dim_redirects WHERE multiDimId = %(id)s", params={"id": mdim_id}
    )
    for r in redirects.to_dict("records"):
        out.append(rec("mdim", slug, mdim_id, "redirect", LINK, r["source"], r["source"], "redirects here"))
    return out


def sweep_explorer_subject(explorer: str) -> list[dict]:
    # An unknown slug matches nothing in every query below, so the run would end with
    # `references: 0` — the same output as an explorer nothing points at. Resolve it first,
    # as the chart, indicator and MDIM subjects do.
    if OWID_ENV.read_sql("SELECT slug FROM explorers WHERE slug = %(s)s", params={"s": explorer}).empty:
        print(f"  (explorer not found: {explorer})")
        return []

    df = OWID_ENV.read_sql(
        "SELECT pg.slug AS post_slug, pg.type AS post_type, pg.published, "
        "       pgl.target, pgl.queryString, pgl.componentType, pgl.text "
        "FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        "WHERE pgl.linkType = 'explorer' AND pgl.target = %(s)s",
        params={"s": explorer},
    )
    out = []
    for r in df.to_dict("records"):
        component = r["componentType"] or ""
        out.append(
            rec(
                "explorer",
                explorer,
                None,
                "gdoc",
                LINK if component.startswith("span-") else EMBED,
                r["post_slug"],
                f"/{r['post_slug']}",
                f"{component or 'unknown'} ({r['post_type']})",
                r["queryString"],
                r["text"],
                r["published"],
            )  # fmt: skip
        )
    return out


# ----- Reporting -------------------------------------------------------------------


def summarize(findings: list[dict]) -> None:
    by_kind: dict[str, int] = defaultdict(int)
    by_surface: dict[str, int] = defaultdict(int)
    for f in findings:
        by_kind[f["kind"]] += 1
        by_surface[f["surface"]] += 1
    print(f"\nreferences: {len(findings)}  " + "  ".join(f"{k}: {n}" for k, n in sorted(by_kind.items())))
    for surface in sorted(by_surface):
        print(f"  {surface}: {by_surface[surface]}")
    unpublished = sum(1 for f in findings if not f["published"])
    if unpublished:
        print(f"  ({unpublished} on unpublished/draft sources)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Find every surface that references a chart, indicator, MDIM or explorer.")
    ap.add_argument("--chart-ids", help="comma-separated chart ids")
    ap.add_argument("--chart-slugs", help="comma-separated chart slugs")
    ap.add_argument("--variable-ids", help="comma-separated variable ids")
    ap.add_argument("--dataset-id", type=int, help="all variables of this dataset")
    ap.add_argument("--mdim", action="append", default=[], help="MDIM slug or catalogPath (repeatable)")
    ap.add_argument("--explorer", action="append", default=[], help="explorer slug (repeatable)")
    ap.add_argument("--transitive", action="store_true",
                    help="indicator subjects only: also sweep the articles referencing the charts found")  # fmt: skip
    ap.add_argument("--json", dest="json_out", help="write findings as JSON to this path")
    ap.add_argument("--csv", dest="csv_out", help="write findings as CSV to this path")
    ap.add_argument("--gaps-json", dest="gaps_out",
                    help="write the surfaces this run could not sweep as JSON to this path")  # fmt: skip
    args = ap.parse_args()

    def ints(value: str | None) -> list[int]:
        return [int(x) for x in value.split(",") if x.strip()] if value else []

    def strs(value: str | None) -> list[str]:
        return [x.strip() for x in value.split(",") if x.strip()] if value else []

    chart_ids, chart_slugs = ints(args.chart_ids), strs(args.chart_slugs)
    variable_ids = resolve_variable_ids(ints(args.variable_ids), args.dataset_id)
    if not any([chart_ids, chart_slugs, variable_ids, args.mdim, args.explorer]):
        raise SystemExit("Give at least one subject: --chart-ids / --chart-slugs / --variable-ids / "
                         "--dataset-id / --mdim / --explorer")  # fmt: skip

    print(f"grapher DB: {OWID_ENV.name}")
    findings: list[dict] = []

    by_slug = resolve_chart_subjects(chart_ids, chart_slugs)
    if (chart_ids or chart_slugs) and not by_slug:
        print("chart subjects: NONE of the requested charts resolved — no chart surface was swept.")
    if by_slug:
        print(f"chart subjects: {len({v['id'] for v in by_slug.values()})} chart(s), {len(by_slug)} slug(s) incl. old")
        findings += sweep_gdoc_links(by_slug)
        findings += sweep_gdoc_url_links(by_slug)
        findings += sweep_explorer_charts(by_slug)
        findings += sweep_narrative_charts_of_charts(by_slug)
        findings += sweep_data_insights(by_slug)
        findings += sweep_static_viz(by_slug)
        findings += sweep_key_charts(by_slug)
        findings += sweep_wordpress(by_slug)

    if variable_ids:
        print(f"indicator subjects: {len(variable_ids)} variable(s)")
        chart_hits = sweep_charts_of_indicators(variable_ids)
        findings += chart_hits
        findings += sweep_mdim_views_of_indicators(variable_ids)
        findings += sweep_explorer_views_of_indicators(variable_ids)
        if args.transitive:
            # Slugless drafts have no URL, so nothing can reference them by slug.
            hop = resolve_chart_subjects([], sorted({f["where"] for f in chart_hits if f["where_path"]}))
            if hop:
                print(f"  transitive: sweeping articles for {len(hop)} chart slug(s)")
                findings += sweep_gdoc_links(hop)
                findings += sweep_data_insights(hop)
                findings += sweep_narrative_charts_of_charts(hop)

    # `--transitive` only has a second hop to make from an indicator. Passing it with just an
    # MDIM or explorer subject would otherwise look like it widened the sweep when it did not.
    if args.transitive and not variable_ids and (args.mdim or args.explorer):
        print("  note: --transitive applies to indicator subjects only; it added nothing to this run.")

    for mdim in args.mdim:
        findings += sweep_mdim_subject(mdim)
    for explorer in args.explorer:
        findings += sweep_explorer_subject(explorer)

    order = {EMBED: 0, RENDER: 1, LINK: 2}
    findings.sort(key=lambda f: (order[f["kind"]], f["surface"], str(f["subject"]), f["where"]))
    summarize(findings)

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(findings, indent=2) + "\n")
        print(f"\n-> {args.json_out}")
    if args.csv_out:
        with open(args.csv_out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS)
            w.writeheader()
            w.writerows(findings)
        print(f"-> {args.csv_out}")
    if args.gaps_out:
        Path(args.gaps_out).write_text(json.dumps(COVERAGE_GAPS, indent=2) + "\n")
    if not args.json_out and not args.csv_out:
        print("\n(pass --json / --csv to save the findings)")
    # Last thing on stdout, after the counts: a sweep that skipped a surface must not read
    # as a complete answer just because the summary above it looks tidy.
    if COVERAGE_GAPS:
        print(f"\n{len(COVERAGE_GAPS)} coverage gap(s) — this sweep is NOT complete:")
        for g in COVERAGE_GAPS:
            print(f"  - {g}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
