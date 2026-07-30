"""Find every OWID surface that references a chart, indicator, MDIM, or explorer.

READ-ONLY, pure SQL — works with read-only credentials (no ADMIN_API_KEY needed).

One question, one answer, for any grapher object: *what would break, go stale, or
need editing if this changed or went away?* Each finding carries a `kind` that says
how the surface holds the object, which is what decides whether a fix is needed:

  render  the surface resolves the object and draws it (a chart on an indicator, an
          MDIM view, an explorer view). Changing the object changes what readers see.
  embed   the surface embeds it by id/slug and renders its config directly (article
          chart blocks, narrative charts, data insights, static viz, explorers).
          A URL redirect does NOT fix these.
  link    a hyperlink. A redirect covers it; the href is still worth updating.

Subjects (at least one; they can be combined):
    --chart-ids 123,456        --chart-slugs a,b,c
    --variable-ids 1,2         --dataset-id 6789
    --mdim <slug|catalogPath>  --explorer <slug>

Chart subjects are expanded to every old slug that still reaches them
(`chart_slug_redirects`), because references written before a rename point at the
old slug and are otherwise missed.

`--transitive` adds a second hop for indicator/MDIM subjects: after finding the
charts that render an indicator, also find the articles that reference those charts.
Off by default — it multiplies the sweep on widely-charted datasets.

Usage:
    ENV_FILE=<creds> DATA_API_ENV=production .venv/bin/python \
        .claude/skills/find-chart-references/scripts/find_references.py \
        --chart-slugs life-expectancy --json out.json
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

from etl.config import OWID_ENV

RENDER, EMBED, LINK = "render", "embed", "link"

COLUMNS = [
    "subject_type", "subject", "subject_id", "surface", "kind",
    "where", "where_path", "surface_id", "config_id", "context",
    "query_string", "text", "published",
]  # fmt: skip


def rec(subject_type, subject, subject_id, surface, kind, where, where_path="", *,
        surface_id=None, config_id=None, context="", query_string="", text="",
        published=True) -> dict:  # fmt: skip
    """One reference.

    `surface_id` and `config_id` are the handles a caller needs to go further without
    re-deriving the joins: `surface_id` identifies the surface object (chart id,
    multi_dim_x_chart_configs.id, narrative chart id, explorer slug), and `config_id`
    is its `chart_configs.id` where one exists — enough to fetch the rendered config
    and inspect it (time pins, entity selections, FAUST text).
    """
    return {
        "subject_type": subject_type,
        "subject": subject,
        "subject_id": subject_id,
        "surface": surface,
        "kind": kind,
        "where": where,
        "where_path": where_path,
        "surface_id": surface_id,
        "config_id": config_id,
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
        params["slugs"] = tuple(chart_slugs)
    df = OWID_ENV.read_sql(
        f"SELECT c.id, cc.slug FROM charts c JOIN chart_configs cc ON cc.id = c.configId WHERE {' OR '.join(where)}",
        params=params,
    )
    charts = {int(r["id"]): r["slug"] for r in df.to_dict("records") if r["slug"]}
    if not charts:
        return {}
    by_slug = {slug: {"id": cid, "slug": slug} for cid, slug in charts.items()}
    # Old slugs still reach the chart, so references may point at them.
    old = OWID_ENV.read_sql(
        "SELECT csr.slug AS old_slug, csr.chart_id FROM chart_slug_redirects csr WHERE csr.chart_id IN %(ids)s",
        params={"ids": tuple(charts)},
    )
    for r in old.to_dict("records"):
        cid = int(r["chart_id"])
        by_slug[r["old_slug"]] = {"id": cid, "slug": charts[cid]}
    return by_slug


def resolve_variable_ids(variable_ids: list[int], dataset_id: int | None) -> list[int]:
    ids = list(variable_ids)
    if dataset_id is not None:
        df = OWID_ENV.read_sql("SELECT id FROM variables WHERE datasetId = %(d)s", params={"d": dataset_id})
        ids += [int(i) for i in df["id"]]
    return sorted(set(ids))


# ----- Chart surfaces -------------------------------------------------------------


def sweep_gdoc_links(by_slug: dict[str, dict]) -> list[dict]:
    """Article links and embeds (posts_gdocs_links).

    `componentType` is the discriminator: a `span-*` value is a hyperlink in prose;
    anything else is a block-level component that renders the chart itself.
    """
    slugs = tuple(by_slug)
    df = OWID_ENV.read_sql(
        "SELECT pg.id AS gdoc_id, pg.slug AS post_slug, pg.type AS post_type, pg.published, "
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
                surface_id=r["gdoc_id"],
                context=f"{component or 'unknown'} ({r['post_type']})",
                query_string=r["queryString"],
                text=r["text"],
                published=r["published"],
            )  # fmt: skip
        )
    return out


def sweep_gdoc_url_links(by_slug: dict[str, dict]) -> list[dict]:
    """Raw URL links (linkType='url') pointing at a live grapher page."""
    slugs = tuple(by_slug)
    clauses = " OR ".join(f"pgl.target LIKE %(t{i})s" for i in range(len(slugs)))
    params = {f"t{i}": f"%/grapher/{s}%" for i, s in enumerate(slugs)}
    df = OWID_ENV.read_sql(
        "SELECT pg.id AS gdoc_id, pg.slug AS post_slug, pg.type AS post_type, pg.published, "
        "       pgl.target, pgl.queryString, pgl.componentType, pgl.text "
        f"FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        f"WHERE pgl.linkType = 'url' AND ({clauses})",
        params=params,
    )
    out = []
    for r in df.to_dict("records"):
        target = r["target"] or ""
        if "archive.ourworldindata.org" in target:
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
                surface_id=r["gdoc_id"],
                context=f"{component or 'unknown'} ({r['post_type']})",
                query_string=target.split("?", 1)[1] if "?" in target else "",
                text=r["text"],
                published=r["published"],
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
            surface_id=r["explorerSlug"],
            context="references the chart by id",
            published=r["isPublished"],
        )  # fmt: skip
        for r in df.to_dict("records")
    ]


def sweep_narrative_charts_of_charts(by_slug: dict[str, dict]) -> list[dict]:
    """Narrative charts whose parent is one of these charts.

    The narrative chart keeps rendering (its config is fetched by UUID), but its
    "Explore the data" link is built from the parent's slug.
    """
    ids = tuple({v["id"] for v in by_slug.values()})
    df = OWID_ENV.read_sql(
        "SELECT nc.id, nc.name, nc.chartConfigId, nc.parentChartId AS chart_id, "
        "       nc.queryParamsForParentChart AS qp, cc.slug "
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
                EMBED,
                r["name"],
                f"/admin/narrative-charts/{r['id']}/edit",
                surface_id=int(r["id"]),
                # NOTE: chart_configs.full for a narrative chart is materialized and lags a
                # parent edit. To inspect one, use AdminAPI.get_narrative_chart(id)["configFull"]
                # rather than reading this row directly.
                config_id=r["chartConfigId"],
                context='"Explore the data" link is built from the parent chart slug',
                query_string="&".join(f"{k}={v}" for k, v in sorted(params.items())),
            )  # fmt: skip
        )
    return out


def sweep_data_insights(by_slug: dict[str, dict]) -> list[dict]:
    """Data insights store the chart in content->>'$."grapher-url"', not in posts_gdocs_links."""
    slugs = tuple(by_slug)
    df = OWID_ENV.read_sql(
        "SELECT gdoc_id, post_slug, published, grapher_url, slug FROM ("
        "  SELECT pg.id AS gdoc_id, pg.slug AS post_slug, pg.published,"
        "         pg.content->>'$.\"grapher-url\"' AS grapher_url,"
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
                surface_id=r["gdoc_id"],
                context="grapher-url in the insight's front matter",
                query_string=url.split("?", 1)[1] if "?" in url else "",
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
        "SELECT ct.chartId AS chart_id, ct.tagId, t.name AS tag, ct.keyChartLevel, cc.slug "
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
            surface_id=int(r["tagId"]),
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
        print(f"  (WordPress sweep skipped: {type(e).__name__})")
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
                surface_id=r["gdoc_id"],
                context="legacy post link",
                query_string=target.split("?", 1)[1] if "?" in target else "",
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
    df = OWID_ENV.read_sql(
        "SELECT DISTINCT cd.variableId, c.id AS chart_id, cc.id AS config_id, cc.slug, "
        "       c.publishedAt IS NOT NULL AS published, c.isInheritanceEnabled AS inheritance "
        "FROM chart_dimensions cd JOIN charts c ON c.id = cd.chartId "
        "JOIN chart_configs cc ON cc.id = c.configId WHERE cd.variableId IN %(ids)s ORDER BY cc.slug",
        params={"ids": tuple(variable_ids)},
    )
    return [
        rec(
            "indicator",
            str(r["variableId"]),
            int(r["variableId"]),
            "chart",
            RENDER,
            # A chart can have no slug (drafts never given one). Label it by id rather
            # than emitting a null `where`, which breaks any caller that sorts or groups.
            r["slug"] or f"(chart {r['chart_id']}, no slug)",
            f"/grapher/{r['slug']}" if r["slug"] else "",
            surface_id=int(r["chart_id"]),
            config_id=r["config_id"],
            context=f"inheritance {'on' if r['inheritance'] else 'off'}"
            + ("" if r["slug"] else " — draft with no slug"),
            published=r["published"],
        )  # fmt: skip
        for r in df.to_dict("records")
    ]


def sweep_mdim_views_of_indicators(variable_ids: list[int]) -> list[dict]:
    """MDIM views rendering these indicators.

    multi_dim_x_chart_configs.variableId records only the FIRST y indicator, so a
    view plotting several indicators is invisible to that join — scan the stored
    configs as well.
    """
    ids = set(variable_ids)
    out = []
    df = OWID_ENV.read_sql(
        "SELECT mx.variableId, mx.id AS mx_id, mx.chartConfigId, md.slug, md.catalogPath, md.published, mx.viewId "
        "FROM multi_dim_x_chart_configs mx JOIN multi_dim_data_pages md ON md.id = mx.multiDimId "
        "WHERE mx.variableId IN %(ids)s",
        params={"ids": tuple(variable_ids)},
    )
    seen = set()
    for r in df.to_dict("records"):
        key = (r["slug"], r["viewId"])
        seen.add(key)
        out.append(
            rec(
                "indicator",
                str(r["variableId"]),
                int(r["variableId"]),
                "mdim view",
                RENDER,
                f"{r['slug']}:{r['viewId']}",
                f"/grapher/{r['slug']}",
                surface_id=int(r["mx_id"]),
                config_id=r["chartConfigId"],
                context=r["catalogPath"],
                published=r["published"],
            )  # fmt: skip
        )
    configs = OWID_ENV.read_sql("SELECT slug, catalogPath, published, config FROM multi_dim_data_pages")
    for row in configs.to_dict("records"):
        cfg = row["config"]
        cfg = json.loads(cfg) if isinstance(cfg, str) else (cfg or {})
        for view in cfg.get("views", []):
            view_id = "__".join(f"{k}={v}" for k, v in sorted((view.get("dimensions") or {}).items()))
            if (row["slug"], view_id) in seen:
                continue
            indicators = view.get("indicators") or {}
            hit = None
            for slot in ("y", "x", "size", "color"):
                entries = indicators.get(slot)
                if entries is None:
                    continue
                for e in entries if isinstance(entries, list) else [entries]:
                    vid = e.get("id") if isinstance(e, dict) else (e if isinstance(e, int) else None)
                    if vid in ids:
                        hit = int(vid)
                        break
                if hit:
                    break
            if hit:
                seen.add((row["slug"], view_id))
                out.append(
                    rec(
                        "indicator",
                        str(hit),
                        hit,
                        "mdim view",
                        RENDER,
                        f"{row['slug']}:{view_id}",
                        f"/grapher/{row['slug']}",
                        # fullConfigId is the view's chart_configs row (config_id elsewhere too)
                        config_id=view.get("fullConfigId"),
                        context=f"{row['catalogPath']} (found by config scan, not mx.variableId)",
                        published=row["published"],
                    )  # fmt: skip
                )
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
        print(f"  (explorer_variables sweep skipped: {type(e).__name__})")
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
            surface_id=r["explorerSlug"],
            # Aggregated per explorer: use explorer_views -> chart_configs for the
            # individual view configs when you need to inspect them.
            context=f"{r['n']} view(s)",
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
                surface_id=r["gdoc_id"],
                context=f"{component or 'unknown'} ({r['post_type']})",
                query_string=r["queryString"],
                text=r["text"],
                published=r["published"],
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
                surface_id=int(r["id"]),
                context=f"pinned to view {r['viewId']} — blocks re-publish if that view disappears",
            )  # fmt: skip
        )

    redirects = OWID_ENV.read_sql(
        "SELECT source FROM multi_dim_redirects WHERE multiDimId = %(id)s", params={"id": mdim_id}
    )
    for r in redirects.to_dict("records"):
        out.append(rec("mdim", slug, mdim_id, "redirect", LINK, r["source"], r["source"], context="redirects here"))
    return out


def sweep_explorer_subject(explorer: str) -> list[dict]:
    df = OWID_ENV.read_sql(
        "SELECT pg.id AS gdoc_id, pg.slug AS post_slug, pg.type AS post_type, pg.published, "
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
                surface_id=r["gdoc_id"],
                context=f"{component or 'unknown'} ({r['post_type']})",
                query_string=r["queryString"],
                text=r["text"],
                published=r["published"],
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


GDOC_SURFACES = ("gdoc", "gdoc (url link)", "data insight")


def doc_url(f: dict) -> str:
    """Google Doc edit URL. posts_gdocs.id IS the Google Doc id, so this is a direct link."""
    return f"https://docs.google.com/document/d/{f['surface_id']}/edit" if f["surface_id"] else ""


def deep_link(f: dict, host: str) -> str:
    """Published-article URL with a scroll-to-text fragment on the link's anchor text.

    Opens the article scrolled to (and highlighting) the reference, which is the
    fastest way to find it. Falls back to the plain article URL when the reference
    is a block embed with no anchor text.
    """
    base = f"{host}{f['where_path']}"
    anchor = (f.get("text") or "").strip()
    if not anchor:
        return base
    # Text fragments need parentheses literal and hyphens percent-encoded (see
    # apps/wizard/app_pages/chart_diff/citations.py:create_text_fragment_url).
    encoded = quote(anchor[:200], safe="()").replace("-", "%2D")
    return f"{base}#:~:text={encoded}"


def write_markdown(findings: list[dict], path: str, host: str) -> None:
    """Human-readable report: where each reference is, and how to open it."""
    by_kind: dict[str, list[dict]] = defaultdict(list)
    for f in findings:
        by_kind[f["kind"]].append(f)

    lines = ["# What references these objects", ""]
    for kind, blurb in [
        (EMBED, "Renders the object's own config — a redirect or rename does **not** fix these."),
        (RENDER, "Resolves and draws the object; changing it changes what readers see."),
        (LINK, "Hyperlinks. A redirect covers them, but the href is worth updating."),
    ]:
        group = by_kind.get(kind)
        if not group:
            continue
        lines += [f"## {kind} ({len(group)})", "", blurb, ""]
        by_surface: dict[str, list[dict]] = defaultdict(list)
        for f in group:
            by_surface[f["surface"]].append(f)
        for surface in sorted(by_surface):
            lines += [f"### {surface} ({len(by_surface[surface])})", ""]
            for f in by_surface[surface]:
                draft = "" if f["published"] else " _(unpublished)_"
                lines.append(f"- **{f['subject']}** in `{f['where']}`{draft} — {f['context']}")
                if f["surface"] in GDOC_SURFACES:
                    lines.append(f"    - 📄 Google Doc: {doc_url(f)}")
                    if f["text"]:
                        lines.append(f'    - 🔎 find in the doc: search for "{f["text"]}"')
                    lines.append(f"    - 🔗 open at the reference: {deep_link(f, host)}")
                elif f["where_path"]:
                    lines.append(f"    - 🔗 {host}{f['where_path']}")
                if f["query_string"]:
                    lines.append(f"    - params: `{f['query_string']}`")
                if f["config_id"]:
                    lines.append(f"    - config: `{f['config_id']}`")
            lines.append("")
    Path(path).write_text("\n".join(lines))


def main() -> int:
    ap = argparse.ArgumentParser(description="Find every surface that references a chart, indicator, MDIM or explorer.")
    ap.add_argument("--chart-ids", help="comma-separated chart ids")
    ap.add_argument("--chart-slugs", help="comma-separated chart slugs")
    ap.add_argument("--variable-ids", help="comma-separated variable ids")
    ap.add_argument("--dataset-id", type=int, help="all variables of this dataset")
    ap.add_argument("--mdim", action="append", default=[], help="MDIM slug or catalogPath (repeatable)")
    ap.add_argument("--explorer", action="append", default=[], help="explorer slug (repeatable)")
    ap.add_argument("--transitive", action="store_true",
                    help="for indicator/MDIM subjects, also sweep the articles referencing the charts found")  # fmt: skip
    ap.add_argument("--json", dest="json_out", help="write findings as JSON to this path")
    ap.add_argument("--csv", dest="csv_out", help="write findings as CSV to this path")
    ap.add_argument("--markdown", dest="md_out",
                    help="write a human-readable report (Google Doc links + scroll-to-reference links)")  # fmt: skip
    ap.add_argument("--host", default=None, help="site for links (default: the DB environment's site)")
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
            hop = resolve_chart_subjects([], sorted({f["where"] for f in chart_hits}))
            if hop:
                print(f"  transitive: sweeping articles for {len(hop)} chart slug(s)")
                findings += sweep_gdoc_links(hop)
                findings += sweep_data_insights(hop)
                findings += sweep_narrative_charts_of_charts(hop)

    for mdim in args.mdim:
        findings += sweep_mdim_subject(mdim)
    for explorer in args.explorer:
        findings += sweep_explorer_subject(explorer)

    order = {EMBED: 0, RENDER: 1, LINK: 2}
    # Coerce every sort key to str: any surface field can be NULL in the DB, and a
    # None here would abort the whole run at the very end.
    findings.sort(key=lambda f: (order[f["kind"]], str(f["surface"]), str(f["subject"]), str(f["where"] or "")))
    summarize(findings)

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(findings, indent=2) + "\n")
        print(f"\n-> {args.json_out}")
    if args.csv_out:
        import csv as _csv

        with open(args.csv_out, "w", newline="") as f:
            w = _csv.DictWriter(f, fieldnames=COLUMNS)
            w.writeheader()
            w.writerows(findings)
        print(f"-> {args.csv_out}")
    if args.md_out:
        write_markdown(findings, args.md_out, (args.host or OWID_ENV.site or "https://ourworldindata.org").rstrip("/"))
        print(f"-> {args.md_out}")
    if not any([args.json_out, args.csv_out, args.md_out]):
        print("\n(pass --json / --csv / --markdown to save the findings)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
