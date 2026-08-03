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
from urllib.parse import parse_qs, quote, urlencode, urlsplit

from etl.config import OWID_ENV

RENDER, EMBED, LINK = "render", "embed", "link"
TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")
GOOGLE_REDIRECT_RE = re.compile(r"^https?://(?:www\.)?google\.[a-z.]+/url\?", re.IGNORECASE)
# Every raw-URL sweep adds this to its SQL prefilter. A wrapper can percent-encode the
# nested URL's slashes, in which case a `LIKE '%/grapher/<slug>%'` prefilter drops the row
# before `unwrap_redirect` ever sees it — so wrapper rows are always candidates and the
# path is decided in Python. Cheap: production holds four such rows in total.
WRAPPER_LIKE = "%google.%/url?%"

# Frozen snapshots of the site. A link into one keeps rendering whatever it captured, so it
# is never part of a blast radius — every raw-URL sweep has to drop it, or an archived copy
# of a page is reported as a live reference somebody has to migrate.
ARCHIVE_HOST = "archive.ourworldindata.org"

# Surfaces this run could NOT sweep, and subjects that did not resolve. An empty result for
# any of these means UNKNOWN, not "nothing references it", so they must survive past stdout:
# they go into the `--markdown` report's "Not searched" section, and `--gaps-json` hands them
# to whatever wraps this script (audit_references.py puts them in its own Unverified bucket).
# Otherwise a truncated sweep reads as a complete audit.
COVERAGE_GAPS: list[str] = []


def gap(message: str) -> None:
    """Record a coverage gap and say so on stdout."""
    COVERAGE_GAPS.append(message)
    print(f"  COVERAGE GAP: {message}")


COLUMNS = [
    "subject_type", "subject", "subject_label", "subject_id", "surface", "kind",
    "where", "where_path", "surface_id", "config_id", "context",
    "query_string", "text", "published", "preview_url", "admin_url",
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
        "subject_label": str(subject),
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
        "preview_url": "",
        "admin_url": "",
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


def unwrap_redirect(target: str) -> str:
    """The real URL behind a `google.com/url?…` wrapper, which is what a raw link often is.

    Google Docs rewrites some pasted links into that wrapper, and the reference's own
    parameters — the `country=`/`time=` pins the downstream audits grade — sit
    percent-encoded inside the wrapper, behind its tracking query. Reading the target as-is
    would both match the path in the wrong place and hand those audits `sa=`/`usg=` instead
    of the pins.

    Both parameter spellings are read: docs-style wrappers carry the URL in `q`, search-style
    ones in `url` — and it is the `url` form that arrives fully percent-encoded, slashes
    included, so it is the one a path-based filter would otherwise never see.
    """
    if not target or not GOOGLE_REDIRECT_RE.match(target):
        return target or ""
    params = parse_qs(urlsplit(target).query)
    for key in ("q", "url"):
        inner = params.get(key, [""])[0]
        if inner.startswith("http"):
            return inner
    return target


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
    params: dict[str, str] = {f"t{i}": f"%/grapher/{s}%" for i, s in enumerate(slugs)}
    params["wrap"] = WRAPPER_LIKE
    df = OWID_ENV.read_sql(
        "SELECT pg.id AS gdoc_id, pg.slug AS post_slug, pg.type AS post_type, pg.published, "
        "       pgl.target, pgl.queryString, pgl.componentType, pgl.text "
        f"FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        f"WHERE pgl.linkType = 'url' AND ({clauses} OR pgl.target LIKE %(wrap)s)",
        params=params,
    )
    out = []
    for r in df.to_dict("records"):
        target = unwrap_redirect(r["target"])
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
                surface_id=r["gdoc_id"],
                context=f"{component or 'unknown'} ({r['post_type']})",
                query_string=url_query(target, r["queryString"] or ""),
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

    Classified as `link`, not `embed`: a narrative chart owns a materialized full config
    of its own (written at creation) and renders from that, so unpublishing the parent
    does not touch what readers see. The parent is joined in only to build the "Explore
    the data" href from its slug, which a redirect covers.

    The href carries `queryParamsForParentChart`, so it is still worth checking — those
    params ride along to the target and can collide with an MDIM view's dimensions.
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
                LINK,
                r["name"],
                f"/admin/narrative-charts/{r['id']}/edit",
                surface_id=int(r["id"]),
                # NOTE: chart_configs.full for a narrative chart is materialized and lags a
                # parent edit. To inspect one, use AdminAPI.get_narrative_chart(id)["configFull"]
                # rather than reading this row directly.
                config_id=r["chartConfigId"],
                context='renders its own config; only its "Explore the data" link uses the parent slug',
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


# There is deliberately no WordPress sweep. The `posts` mirror is dead: every published row
# that links a chart 404s on the live site, and none of those slugs exists as a published
# gdoc, so they are not migrated content the gdoc sweeps already cover. The sweep that used
# to live here also never worked — it matched `posts_links.target LIKE '%/grapher/%'`, but
# that column holds the bare slug for `linkType='grapher'` rows, so it returned nothing while
# 676 such links existed. Reinstating it would surface references to pages that all 404.


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


def parse_json_list(value) -> list:
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def sweep_charts_of_indicators(variable_ids: list[int]) -> list[dict]:
    """Charts rendering these indicators. Drafts can have no slug — they still render.

    `charts.publishedAt` is the first-publish timestamp and survives an unpublish, so it
    would report already-retired charts as live and have their references graded for
    reader impact. The live state is `isPublished` in the config.
    """
    df = OWID_ENV.read_sql(
        "SELECT DISTINCT cd.variableId, c.id AS chart_id, cc.id AS config_id, cc.slug, "
        "       COALESCE(cc.full->>'$.isPublished', 'false') = 'true' AS published, "
        "       c.isInheritanceEnabled AS inheritance "
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

    # Index each view's dimensions by its fullConfigId. Dimensions are what a preview URL
    # needs, and taking them from the config avoids splitting viewId on "__" — a choice
    # slug may itself contain underscores.
    configs = OWID_ENV.read_sql("SELECT slug, catalogPath, published, config FROM multi_dim_data_pages")
    parsed = []
    dims_by_config_id: dict[str, dict] = {}
    for row in configs.to_dict("records"):
        cfg = row["config"]
        cfg = json.loads(cfg) if isinstance(cfg, str) else (cfg or {})
        parsed.append((row, cfg))
        for view in cfg.get("views", []):
            if view.get("fullConfigId"):
                dims_by_config_id[view["fullConfigId"]] = view.get("dimensions") or {}

    def emit(slug, catalog_path, published, view_id, variable_id, config_id, surface_id, dims, note=""):
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
                surface_id=surface_id,
                config_id=config_id,
                context=catalog_path + note,
                query_string=urlencode(sorted(dims.items())) if dims else "",
                published=published,
            )  # fmt: skip
        )

    df = OWID_ENV.read_sql(
        "SELECT mx.variableId, mx.id AS mx_id, mx.chartConfigId, md.slug, md.catalogPath, md.published, mx.viewId "
        "FROM multi_dim_x_chart_configs mx JOIN multi_dim_data_pages md ON md.id = mx.multiDimId "
        "WHERE mx.variableId IN %(ids)s",
        params={"ids": tuple(variable_ids)},
    )
    for r in df.to_dict("records"):
        emit(
            r["slug"], r["catalogPath"], r["published"], r["viewId"], int(r["variableId"]),
            r["chartConfigId"], int(r["mx_id"]), dims_by_config_id.get(r["chartConfigId"], {}),
        )  # fmt: skip

    # The join above records ONE variableId per view, so walk the configs for the rest.
    for row, cfg in parsed:
        for view in cfg.get("views", []):
            dims = view.get("dimensions") or {}
            view_id = "__".join(f"{k}={v}" for k, v in sorted(dims.items()))
            indicators = view.get("indicators") or {}
            for slot in ("y", "x", "size", "color"):
                entries = indicators.get(slot)
                if entries is None:
                    continue
                for e in entries if isinstance(entries, list) else [entries]:
                    vid = entry_variable_id(e, by_path)
                    if vid in ids:
                        emit(
                            row["slug"], row["catalogPath"], row["published"], view_id, int(vid),
                            view.get("fullConfigId"), None, dims,
                        )  # fmt: skip
    return out


def sweep_narrative_charts_of_mdim_views(mdim_rows: list[dict]) -> list[dict]:
    """Narrative charts whose parent is one of these MDIM views.

    A narrative chart can hang off an MDIM view as well as off a chart, and it carries
    its own config — so a sweep that only walks charts leaves that config unaudited.
    Both branches of the MDIM sweep record the view's `chart_configs` id, which is
    `multi_dim_x_chart_configs.chartConfigId`, so that is the join back.

    One view can render several of the requested indicators — the MDIM sweep emits a row
    per (mdim, view, indicator) for exactly that reason — so the parents are kept as a
    LIST per config. Keyed by config alone, only the last indicator would survive, and
    every per-indicator consumer would read "no narrative reference" for the others.
    """
    by_config: dict[str, list[dict]] = defaultdict(list)
    for r in mdim_rows:
        if r["config_id"]:
            by_config[r["config_id"]].append(r)
    if not by_config:
        return []
    df = OWID_ENV.read_sql(
        "SELECT nc.id, nc.name, nc.chartConfigId, mx.chartConfigId AS view_config_id, "
        "       mx.viewId, md.slug, md.published "
        "FROM narrative_charts nc JOIN multi_dim_x_chart_configs mx ON mx.id = nc.parentMultiDimXChartConfigId "
        "JOIN multi_dim_data_pages md ON md.id = mx.multiDimId "
        "WHERE mx.chartConfigId IN %(c)s ORDER BY nc.name",
        params={"c": tuple(by_config)},
    )
    out = []
    for r in df.to_dict("records"):
        # The query is filtered on the keys of `by_config`, so a miss is impossible — but
        # fall back to one blank parent rather than dropping the narrative chart entirely,
        # since an unattributed row is still a surface someone has to look at.
        for parent in by_config.get(r["view_config_id"]) or [{}]:
            out.append(
                rec(
                    "indicator",
                    parent.get("subject", ""),
                    parent.get("subject_id"),
                    "narrative chart",
                    EMBED,
                    r["name"],
                    f"/admin/narrative-charts/{r['id']}/edit",
                    surface_id=int(r["id"]),
                    # NOTE: as for chart-parented narrative charts, read the merged config via
                    # AdminAPI.get_narrative_chart(id)["configFull"] — the stored row lags a
                    # parent edit until the child is re-saved.
                    config_id=r["chartConfigId"],
                    context=f"pinned to MDIM view {r['slug']}:{r['viewId']}",
                    published=r["published"],
                )  # fmt: skip
            )
    return out


def sweep_explorer_views_of_indicators(variable_ids: list[int]) -> list[dict]:
    """Explorer views rendering these indicators — one row per view, with its config id.

    `explorer_variables` records which explorers use an indicator, not which of their
    views do, and an explorer-level aggregate gives a caller nothing to inspect: the
    pinned entities and time bounds live in each view's own grapher config. That link
    is `explorer_views.chartConfigId` -> `chart_configs`, so narrow to the explorers
    that use the indicators, then match inside their view configs. Every row then
    carries a `config_id`, like a chart or an MDIM view.
    """
    ids = set(variable_ids)
    try:
        used = OWID_ENV.read_sql(
            "SELECT ev.variableId, ev.explorerSlug, e.isPublished "
            "FROM explorer_variables ev JOIN explorers e ON e.slug = ev.explorerSlug "
            "WHERE ev.variableId IN %(ids)s GROUP BY ev.variableId, ev.explorerSlug, e.isPublished",
            params={"ids": tuple(variable_ids)},
        )
    except Exception as e:  # noqa: BLE001 - table shape varies across environments
        gap(f"explorer_variables sweep skipped ({type(e).__name__}) — explorers on these indicators were NOT checked")
        return []
    if used.empty:
        return []
    published = {r["explorerSlug"]: r["isPublished"] for r in used.to_dict("records")}

    views = OWID_ENV.read_sql(
        "SELECT ev.explorerSlug, ev.viewId, ev.dimensions, ev.chartConfigId, "
        "       cc.full->'$.dimensions' AS config_dimensions "
        "FROM explorer_views ev JOIN chart_configs cc ON cc.id = ev.chartConfigId "
        "WHERE ev.explorerSlug IN %(s)s ORDER BY ev.explorerSlug, ev.viewId",
        params={"s": tuple(published)},
    )
    out, covered = [], set()
    for r in views.to_dict("records"):
        hits = sorted(
            {
                int(d["variableId"])
                for d in parse_json_list(r["config_dimensions"])
                if isinstance(d, dict) and d.get("variableId") in ids
            }
        )
        if not hits:
            continue
        covered.update((vid, r["explorerSlug"]) for vid in hits)
        # `explorer_views.dimensions` holds the choice labels an explorer URL uses
        # verbatim, so they double as the query string that opens this exact view.
        choices = parse_json_obj(r["dimensions"])
        # One row per matching indicator, as `sweep_charts_of_indicators` does: a view
        # plotting two of them is a reference for both, and attributing it to only one
        # would report the other as unreferenced.
        for vid in hits:
            out.append(
                rec(
                    "indicator",
                    str(vid),
                    vid,
                    "explorer view",
                    RENDER,
                    f"{r['explorerSlug']}:{r['viewId']}",
                    f"/explorers/{r['explorerSlug']}",
                    surface_id=r["explorerSlug"],
                    config_id=r["chartConfigId"],
                    context=" · ".join(f"{k}: {v}" for k, v in sorted(choices.items())),
                    query_string=urlencode(sorted(choices.items())) if choices else "",
                    published=published[r["explorerSlug"]],
                )  # fmt: skip
            )
    # An indicator can be registered against an explorer whose view configs never name
    # it (a stale row, or views built outside `explorer_views`). Keep the explorer-level
    # reference for those instead of dropping it — the empty `config_id` is the signal
    # that there is no view config to inspect.
    for r in used.to_dict("records"):
        if (int(r["variableId"]), r["explorerSlug"]) in covered:
            continue
        out.append(
            rec(
                "indicator",
                str(r["variableId"]),
                int(r["variableId"]),
                "explorer",
                RENDER,
                r["explorerSlug"],
                f"/explorers/{r['explorerSlug']}",
                surface_id=r["explorerSlug"],
                context="used by the explorer, but no view config records it — nothing to inspect",
                published=r["isPublished"],
            )  # fmt: skip
        )
    return out


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
        "SELECT pg.id AS gdoc_id, pg.slug AS post_slug, pg.type AS post_type, pg.published, "
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

    # An article can paste the MDIM's URL instead of linking it as a grapher item, which
    # lands in the same table as `linkType='url'`. The chart sweep already covers that row
    # shape; without the counterpart here a direct --mdim report can miss reader-facing
    # references entirely. The SQL prefilter is loose, so the path segment is re-checked in
    # Python — otherwise a longer slug that merely starts with this one matches too.
    raw = OWID_ENV.read_sql(
        "SELECT pg.id AS gdoc_id, pg.slug AS post_slug, pg.type AS post_type, pg.published, "
        "       pgl.target, pgl.componentType, pgl.text "
        "FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        "WHERE pgl.linkType = 'url' AND (pgl.target LIKE %(t)s OR pgl.target LIKE %(wrap)s)",
        params={"t": f"%/grapher/{slug}%", "wrap": WRAPPER_LIKE},
    )
    exact = re.compile(rf"/grapher/{re.escape(slug)}(?:[?#/]|$)")
    for r in raw.to_dict("records"):
        target = unwrap_redirect(r["target"])
        if not exact.search(target):
            continue
        if ARCHIVE_HOST in target:
            continue  # archived snapshots are frozen by design
        component = r["componentType"] or ""
        out.append(
            rec(
                "mdim",
                slug,
                mdim_id,
                "gdoc (url link)",
                LINK if component.startswith("span-") else EMBED,
                r["post_slug"],
                f"/{r['post_slug']}",
                surface_id=r["gdoc_id"],
                context=f"raw URL, {component or 'unknown'} ({r['post_type']})",
                # A raw URL carries its parameters in the target itself, so this SELECT omits
                # `pgl.queryString` and `url_query` is called without a fallback. That column
                # IS populated on some `linkType='url'` rows — but never on one whose target
                # has no `?`, so the fallback it would feed is unreachable (checked against
                # production 2026-07: 652 such rows, all of them with the query in the target).
                query_string=url_query(target),
                text=r["text"],
                published=r["published"],
            )  # fmt: skip
        )

    nc = OWID_ENV.read_sql(
        "SELECT nc.id, nc.name, nc.chartConfigId, mx.viewId FROM narrative_charts nc "
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
                config_id=r["chartConfigId"],
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
    # An unknown slug matches nothing in every query below, so a run would end with
    # `references: 0` — the same output as an explorer nothing points at. But unlike a chart,
    # an explorer that is GONE is the normal end state of a migration, and pages can still
    # link to it: every query here keys on the slug, not on a row id, so they all work for a
    # deleted explorer. So record a gap and keep sweeping instead of returning empty — a
    # retired explorer's leftover links are exactly what someone needs to find, and
    # returning [] hid them. A typo now surfaces as a gap rather than a bare print.
    if OWID_ENV.read_sql("SELECT slug FROM explorers WHERE slug = %(s)s", params={"s": explorer}).empty:
        gap(
            f"explorer '{explorer}' is not in the `explorers` table — swept its references anyway "
            "(already retired?). If the slug is simply wrong, every count for it is a false zero."
        )

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

    # An article can paste the explorer's URL instead of linking it as an explorer item,
    # which lands in the same table as `linkType='url'` and is invisible to the typed query
    # above — exactly the gap the MDIM sweep closes with its own raw-URL pass. Those rows
    # carry the `country=`/`time=` pins the downstream audits grade, so leaving them out
    # lets a pin escape while the report still reads as full coverage. Same shape as the
    # MDIM pass: loose SQL prefilter, path re-checked in Python so a longer slug that merely
    # starts with this one does not match.
    raw = OWID_ENV.read_sql(
        "SELECT pg.id AS gdoc_id, pg.slug AS post_slug, pg.type AS post_type, pg.published, "
        "       pgl.target, pgl.componentType, pgl.text "
        "FROM posts_gdocs_links pgl JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
        "WHERE pgl.linkType = 'url' AND (pgl.target LIKE %(t)s OR pgl.target LIKE %(wrap)s)",
        params={"t": f"%/explorers/{explorer}%", "wrap": WRAPPER_LIKE},
    )
    exact = re.compile(rf"/explorers/{re.escape(explorer)}(?:[?#/]|$)")
    for r in raw.to_dict("records"):
        target = unwrap_redirect(r["target"])
        if not exact.search(target):
            continue
        if ARCHIVE_HOST in target:
            continue  # archived snapshots are frozen by design
        component = r["componentType"] or ""
        out.append(
            rec(
                "explorer",
                explorer,
                None,
                "gdoc (url link)",
                LINK if component.startswith("span-") else EMBED,
                r["post_slug"],
                f"/{r['post_slug']}",
                surface_id=r["gdoc_id"],
                context=f"raw URL, {component or 'unknown'} ({r['post_type']})",
                # As in the MDIM pass: a raw URL carries its parameters in the target, so the
                # SELECT omits `pgl.queryString` and `url_query` needs no fallback.
                query_string=url_query(target),
                text=r["text"],
                published=r["published"],
            )  # fmt: skip
        )

    out += sweep_explorer_inbound_redirects(explorer)
    return out


def sweep_explorer_inbound_redirects(explorer: str) -> list[dict]:
    """Site redirects pointing AT this explorer.

    Symmetric with the `redirect` surface the MDIM subject already reports, and load-bearing
    rather than informational: an explorer path that is the target of a site redirect cannot
    be given an MDIM redirect at all — the admin endpoint rejects it as a chain, and because
    it caches its per-source checks, that one row fails every entry for the explorer. So this
    is a blocker a consumer needs to surface before it tries to apply anything.

    The `LIKE` prefilter is re-checked in Python for the same reason the URL passes are: a
    prefix match would let `/explorers/inequality-wb` answer for `inequality`.
    """
    df = OWID_ENV.read_sql(
        "SELECT id, source, target FROM redirects WHERE target LIKE %(like)s",
        params={"like": f"%/explorers/{explorer}%"},
    )
    pattern = re.compile(rf"/explorers/{re.escape(explorer)}(?:[?#/]|$)")
    out = []
    for r in df.to_dict("records"):
        if not pattern.search(r["target"] or ""):
            continue
        out.append(
            rec(
                "explorer",
                explorer,
                None,
                "site redirect",
                LINK,
                r["source"],
                r["source"],
                surface_id=int(r["id"]),
                context=f"site redirect id={r['id']} points here — blocks creating an MDIM redirect (chain)",
                query_string=url_query(r["target"]),
            )  # fmt: skip
        )
    return out


def sweep_containers_for_articles(mdim_rows: list[dict], explorer_rows: list[dict]) -> list[dict]:
    """Article references to the MDIMs / explorers whose views render the subject indicators.

    The transitive chart hop only finds articles that name a *chart*. An article that
    links the MDIM page or the explorer itself references the indicators just as much,
    and carries the same `country=`/`time=` pins the downstream audits grade — so walk
    the containers too. Rows keep their container as the subject: the reference is to
    the page, not to any one indicator inside it.
    """
    mdims = sorted({f["where_path"].rsplit("/", 1)[-1] for f in mdim_rows if f["where_path"]})
    explorers = sorted({f["surface_id"] for f in explorer_rows if f["surface_id"]})
    if not mdims and not explorers:
        return []
    print(f"  transitive: sweeping articles for {len(mdims)} MDIM(s) and {len(explorers)} explorer(s)")
    out = []
    for slug in mdims:
        out += [f for f in sweep_mdim_subject(slug) if f["surface"] in GDOC_SURFACES]
    for slug in explorers:
        out += [f for f in sweep_explorer_subject(slug) if f["surface"] in GDOC_SURFACES]
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


def gdoc_preview_url(f: dict, admin: str) -> str:
    """Admin preview of the article itself — renders the gdoc, including unpublished drafts."""
    return f"{admin}/gdocs/{f['surface_id']}/preview" if f["surface_id"] else ""


def doc_url(f: dict) -> str:
    """Google Doc edit URL. posts_gdocs.id IS the Google Doc id, so this is a direct link."""
    return f"https://docs.google.com/document/d/{f['surface_id']}/edit" if f["surface_id"] else ""


def admin_url(where_path: str, host: str, admin: str = "") -> str:
    """Absolute URL for a `where_path`, routing admin routes to the admin origin.

    `admin` is an admin ROOT (".../admin"), while an admin `where_path` already starts
    with `/admin/`, so the root's own suffix has to come off before joining or the result
    carries `/admin/admin/`.
    """
    if not where_path:
        return ""
    if where_path.startswith("/admin/"):
        origin = (admin or host).removesuffix("/").removesuffix("/admin")
        return f"{origin}{where_path}"
    return f"{host}{where_path}"


def deep_link(f: dict, host: str, admin: str = "") -> str:
    """Published-article URL with a scroll-to-text fragment on the link's anchor text.

    Opens the article scrolled to (and highlighting) the reference, which is the
    fastest way to find it. Falls back to the plain article URL when the reference
    is a block embed with no anchor text.

    Some `where_path`s are admin routes, not public ones (a narrative chart's editor).
    Those must resolve against the admin origin — the public site does not serve
    `/admin/...`, so prefixing them with `host` yields a link that 404s.
    """
    base = admin_url(f["where_path"], host, admin)
    anchor = (f.get("text") or "").strip()
    if not anchor:
        return base
    # Text fragments need parentheses literal and hyphens percent-encoded (see
    # apps/wizard/app_pages/chart_diff/citations.py:create_text_fragment_url).
    encoded = quote(anchor[:200], safe="()").replace("-", "%2D")
    return f"{base}#:~:text={encoded}"


def admin_base() -> str:
    """Admin root for whichever environment OWID_ENV resolves to.

    Staging hosts carry a tailscale suffix that is noise in a link handed to a human
    (and the short form resolves fine), so strip it.
    """
    admin = (OWID_ENV.admin_site or "https://admin.owid.io/admin").rstrip("/")
    return TAILSCALE_SUFFIX_RE.sub("", admin)


def add_admin_urls(findings: list[dict]) -> None:
    """Every chart gets an edit link, in the environment being audited.

    A reference is only actionable if you can open the thing to change it — for a chart
    that is the admin editor, not the public page.
    """
    admin = admin_base()
    for f in findings:
        if f["surface"] == "narrative chart" and f["surface_id"]:
            f["admin_url"] = f"{admin}/narrative-charts/{f['surface_id']}/edit"
        elif f["surface"] == "chart" and f["surface_id"]:
            f["admin_url"] = f"{admin}/charts/{f['surface_id']}/edit"
        elif f["subject_type"] == "chart" and f["subject_id"]:
            # The row is a reference *to* a chart (article, explorer, static viz…).
            f["admin_url"] = f"{admin}/charts/{f['subject_id']}/edit"


def add_preview_urls(findings: list[dict], host: str) -> None:
    """The referenced view itself, as the reader sees it.

    For an article reference that is the chart plus the reference's own params; for an
    MDIM view it is the MDIM at that view's dimensions. This is what makes a row
    judgeable — the slug alone doesn't tell you which view is in play.
    """
    for f in findings:
        qs = f"?{f['query_string'].lstrip('?')}" if f["query_string"] else ""
        if f["surface"] == "mdim view":
            mdim_slug = f["where"].split(":", 1)[0]
            f["preview_url"] = f"{host}/grapher/{mdim_slug}{qs}"
        elif f["surface"] == "explorer view":
            f["preview_url"] = f"{host}/explorers/{f['where'].split(':', 1)[0]}{qs}"
        elif f["surface"] == "explorer":
            f["preview_url"] = f"{host}/explorers/{f['where']}"
        elif f["subject_type"] == "chart":
            f["preview_url"] = f"{host}/grapher/{f['subject']}{qs}"
        elif f["subject_type"] == "mdim":
            # A direct --mdim sweep: the reference points at the MDIM page itself, and
            # the reference's own params say which of its views the reader lands on.
            f["preview_url"] = f"{host}/grapher/{f['subject']}{qs}"
        elif f["subject_type"] == "explorer":
            f["preview_url"] = f"{host}/explorers/{f['subject']}{qs}"
        elif f["surface"] == "chart" and f["where_path"]:
            f["preview_url"] = f"{host}{f['where_path']}{qs}"


def label_indicator_subjects(findings: list[dict]) -> None:
    """Replace bare variable ids with the indicator's name, so a reader can tell them apart."""
    ids = {f["subject_id"] for f in findings if f["subject_type"] == "indicator" and f["subject_id"]}
    if not ids:
        return
    df = OWID_ENV.read_sql("SELECT id, name FROM variables WHERE id IN %(i)s", params={"i": tuple(ids)})
    names = {int(r["id"]): r["name"] for r in df.to_dict("records") if r["name"]}
    for f in findings:
        if f["subject_type"] == "indicator" and f["subject_id"] in names:
            f["subject_label"] = f"{names[f['subject_id']]} ({f['subject_id']})"


def is_all_charts(f: dict) -> bool:
    """An entry in a topic page's auto-generated 'All charts' index.

    Not authored in the doc: the block lists every chart carrying the page's tag, so it
    updates itself and a retired chart simply drops out. There is nothing to edit and
    nothing that breaks, which makes these rows noise in a 'what must I update' audit.
    """
    return (f.get("context") or "").startswith("all-charts")


def search_hint(f: dict) -> str:
    """What to search for inside the Google Doc to land on this reference.

    A prose hyperlink has visible anchor text, so that phrase is the search term. A
    block embed has none — the doc holds a bare grapher URL — so the slug is. Use the
    slug exactly as recorded: `posts_gdocs_links.target` keeps what the author typed,
    which may be an old slug, and that is what is actually in the document.
    """
    # The cell holds the search string and nothing else, so it can be copied straight
    # into the doc's find box. What each variant means is in the legend under the table.
    anchor = (f.get("text") or "").strip()
    if anchor:
        safe = cell(anchor, 55).replace("`", "'")
        return f"`{safe}`"
    if f["subject_type"] == "chart":
        return f"`{cell(f['subject'], 55)}`"
    return "—"


def cell(value: str, limit: int = 70) -> str:
    """Table-safe cell: escape pipes and newlines, truncate runaway text."""
    text = " ".join(str(value or "").split()).replace("|", "\\|")
    return (text[: limit - 1] + "…") if len(text) > limit else text


# The surfaces this sweep structurally cannot see. They are stated in the report itself
# rather than only in SKILL.md, because the report is what gets handed on: a reader who
# never opens the skill would otherwise read a short table — or an empty one — as a
# complete blast radius. Keep in step with the "Known gaps" section of SKILL.md.
NOT_SEARCHED = [
    "Non-ETL explorers whose config lives in the `explorers` TSV, and legacy CSV-backed "
    "explorers (`data://explorers/…`, e.g. the poverty explorer): their views and selections "
    "live outside grapher's config tables, so no query here can reach them.",
    "`linkType='url'` article rows pointing at `archive.ourworldindata.org`, dropped as frozen by design.",
    "Indicator-level `presentation.grapher_config`, which lives in garden/grapher `.meta.yml` "
    "rather than the DB — invisible here, and it fans out to every thin MDIM/explorer view that "
    "inherits it. Needs a repo grep.",
    "Data insights that record the reference somewhere other than `grapher-url`.",
    "Charts nested inside article layout containers, which may produce no `posts_gdocs_links` row at all.",
]


def write_markdown(findings: list[dict], path: str, host: str, admin: str, caveats: list[str] | None = None) -> None:
    """Human-readable report: one table per surface, with links to open each reference.

    `caveats` are the coverage limits of *this particular run* (a subject that did not
    resolve, a hop that was not requested); `NOT_SEARCHED` holds the ones that always apply.
    """
    by_kind: dict[str, list[dict]] = defaultdict(list)
    for f in findings:
        by_kind[f["kind"]].append(f)

    lines = [
        "# What references these objects",
        "",
        f"{len(findings)} reference(s). Grouped by how the surface holds the object, "
        "because that decides whether a redirect or rename covers it.",
        "",
    ]
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
            rows = by_surface[surface]
            lines += [f"### {surface} ({len(rows)})", ""]
            if surface in GDOC_SURFACES:
                lines += [
                    "| Chart | Article | Open | Find in the doc |",
                    "|---|---|---|---|",
                ]
                for f in rows:
                    draft = "" if f["published"] else " ⚠️draft"
                    # The page type (article / topic-page / data-insight) changes who owns
                    # the fix; the block type does not, and `kind` already implies it.
                    ptype = f["context"].split("(")[-1].rstrip(")") if "(" in f["context"] else ""
                    page_type = f" _{ptype}_" if ptype else ""
                    preview = f" · [👁 preview]({gdoc_preview_url(f, admin)})" if f["surface_id"] else ""
                    links = f"[📄 doc]({doc_url(f)}){preview} · [🔗 page]({deep_link(f, host, admin)})"
                    if f["admin_url"]:
                        links += f" · [✎ chart admin]({f['admin_url']})"
                    subject = (
                        f"[`{cell(f['subject_label'], 44)}`]({f['preview_url']})"
                        if f["preview_url"]
                        else f"`{cell(f['subject_label'], 44)}`"
                    )
                    find = search_hint(f)
                    lines.append(f"| {subject} | {cell(f['where'], 44)}{page_type}{draft} | {links} | {find} |")
            else:
                lines += ["| Subject | Where | Context | Open |", "|---|---|---|---|"]
                for f in rows:
                    draft = "" if f["published"] else " ⚠️draft"
                    link = f"[🔗 open]({admin_url(f['where_path'], host, admin)})" if f["where_path"] else "—"
                    lines.append(
                        f"| {cell(f['subject_label'], 44)} | {cell(f['where'], 72)}{draft} | "
                        f"{cell(f['context'], 44)} | "
                        f"{'[✎ admin](' + f['admin_url'] + ') · ' if f['admin_url'] else ''}"
                        f"{'[👁 view](' + f['preview_url'] + ')' if f['preview_url'] else link} |"
                    )
            lines.append("")
    lines += [
        "---",
        "",
        "The **chart name links to the view as that reference renders it** (its own params applied). "
        "📄 opens the Google Doc to edit · 🔗 opens the published page scrolled to the reference · "
        "👁 opens the article in the admin previewer (works for unpublished drafts too). "
        "**Find in the doc** is a copy-paste search string for the Google Doc: the link text "
        "for a prose hyperlink, or the chart slug for a block embed (the doc holds a bare "
        "grapher URL there — and the slug is stored as the author typed it, so it matches "
        "even when the doc still uses an old one). A `—` means there is nothing to search "
        "for.",
        "",
        "## Not searched",
        "",
        "This section is the report's coverage boundary. The count above is what the sweep "
        "*found*, not everything that exists — nothing below was checked, so a short or empty "
        "table is not by itself a clean result.",
        "",
    ]
    # A surface that failed open — an absent legacy table, a subject that did not resolve —
    # is the sharpest caveat there is, because it is the one the reader has no other way of
    # knowing about. It leads.
    for note in COVERAGE_GAPS:
        lines.append(f"- **NOT SWEPT in this run:** {note}")
    for note in caveats or []:
        lines.append(f"- **This run:** {note}")
    lines += [f"- {note}" for note in NOT_SEARCHED]
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
    ap.add_argument("--include-all-charts", action="store_true",
                    help="keep topic pages' auto-generated 'All charts' entries (excluded by default)")  # fmt: skip
    ap.add_argument("--transitive", action="store_true",
                    help="indicator subjects only: also sweep the articles referencing the charts found")  # fmt: skip
    ap.add_argument("--json", dest="json_out", help="write findings as JSON to this path")
    ap.add_argument("--csv", dest="csv_out", help="write findings as CSV to this path")
    ap.add_argument("--markdown", dest="md_out",
                    help="write a human-readable report (Google Doc links + scroll-to-reference links)")  # fmt: skip
    ap.add_argument("--gaps-json", dest="gaps_out",
                    help="write the surfaces this run could not sweep as JSON to this path")  # fmt: skip
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
    # Coverage limits specific to this invocation, carried into the report so it can never
    # present a partial sweep as a full one.
    caveats: list[str] = []

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

    if variable_ids:
        print(f"indicator subjects: {len(variable_ids)} variable(s)")
        chart_hits = sweep_charts_of_indicators(variable_ids)
        mdim_hits = sweep_mdim_views_of_indicators(variable_ids)
        findings += chart_hits
        findings += mdim_hits
        explorer_hits = sweep_explorer_views_of_indicators(variable_ids)
        findings += explorer_hits
        if args.transitive:
            # Slugless drafts have no URL, so nothing can reference them by slug.
            hop = resolve_chart_subjects([], sorted({f["where"] for f in chart_hits if f["where_path"]}))
            if hop:
                print(f"  transitive: sweeping articles for {len(hop)} chart slug(s)")
                findings += sweep_gdoc_links(hop)
                # Raw-URL links are a separate table scan from the typed ones, and an
                # article pinning `country=`/`time=` in a bare URL is exactly what the
                # downstream audits look for — sweep both hops, not just the typed one.
                findings += sweep_gdoc_url_links(hop)
                findings += sweep_data_insights(hop)
                findings += sweep_narrative_charts_of_charts(hop)
            # A narrative chart can hang off an MDIM view instead of a chart, so the
            # chart hop alone leaves those configs unaudited.
            findings += sweep_narrative_charts_of_mdim_views(mdim_hits)
            # An article can also link straight at the MDIM or explorer page whose views
            # render these indicators, naming no chart at all. The chart hop above cannot
            # see those, so sweep each distinct container for its own article references.
            # Only the article surfaces are kept: the narrative-chart and redirect rows
            # `sweep_mdim_subject` also returns are already emitted above, and re-adding
            # them here would double-count them.
            findings += sweep_containers_for_articles(mdim_hits, explorer_hits)
        else:
            caveats.append(
                "`--transitive` was not passed, so no article, data-insight or narrative-chart "
                "surface was swept for the indicator subjects — only the charts, MDIM views and "
                "explorer views that render them directly."
            )

    # `--transitive` only has a second hop to make from an indicator. Passing it with just an
    # MDIM or explorer subject would otherwise look like it widened the sweep when it did not.
    if args.transitive and not variable_ids and (args.mdim or args.explorer):
        print("  note: --transitive applies to indicator subjects only; it added nothing to this run.")

    for mdim in args.mdim:
        findings += sweep_mdim_subject(mdim)
    for explorer in args.explorer:
        findings += sweep_explorer_subject(explorer)

    all_charts = [f for f in findings if is_all_charts(f)]
    if all_charts and not args.include_all_charts:
        findings = [f for f in findings if not is_all_charts(f)]
        pages = sorted({f["where"] for f in all_charts})
        print(f"excluded {len(all_charts)} 'All charts' index entries on {pages} "
              "(auto-generated from the page's tags — nothing to edit; --include-all-charts to keep)")  # fmt: skip
        caveats.append(
            f"{len(all_charts)} auto-generated 'All charts' index entries on {', '.join(pages)} were "
            "excluded from the tables below (`--include-all-charts` keeps them)."
        )

    label_indicator_subjects(findings)
    add_admin_urls(findings)
    add_preview_urls(findings, (args.host or OWID_ENV.site or "https://ourworldindata.org").rstrip("/"))

    order = {EMBED: 0, RENDER: 1, LINK: 2}
    # Coerce every sort key to str: any surface field can be NULL in the DB, and a
    # None here would abort the whole run at the very end.
    findings.sort(key=lambda f: (order[f["kind"]], str(f["surface"]), str(f["subject"]), str(f["where"] or "")))
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
    if args.md_out:
        write_markdown(
            findings,
            args.md_out,
            (args.host or OWID_ENV.site or "https://ourworldindata.org").rstrip("/"),
            admin_base(),
            caveats,
        )
        print(f"-> {args.md_out}")
    if args.gaps_out:
        Path(args.gaps_out).write_text(json.dumps(COVERAGE_GAPS, indent=2) + "\n")
        print(f"-> {args.gaps_out}")
    if not any([args.json_out, args.csv_out, args.md_out]):
        print("\n(pass --json / --csv / --markdown to save the findings)")
    # Last thing on stdout, after the counts: a sweep that skipped a surface must not read
    # as a complete answer just because the summary above it looks tidy.
    if COVERAGE_GAPS:
        print(f"\n{len(COVERAGE_GAPS)} coverage gap(s) — this sweep is NOT complete:")
        for g in COVERAGE_GAPS:
            print(f"  - {g}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
