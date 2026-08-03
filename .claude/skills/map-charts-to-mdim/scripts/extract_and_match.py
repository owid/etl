"""Extract published charts + MDIM views from the grapher DB, match charts to
MDIM views by indicator IDs, and write a redirect proposal.

Everything here is READ-ONLY: it queries the grapher DB via ``OWID_ENV``
(``STAGING=1`` for the current branch's staging DB, or against production with
``ENV_FILE=<prod creds file> DATA_API_ENV=production``) and writes artifacts
into ``--out``. The resolved environment is printed at startup — check it: a
reachable local dev DB also passes the preflight, and extracting from the wrong
environment produces a mapping that silently points at the wrong charts. No
redirect is created here, or anywhere in this skill — applying is
``yarn createMultiDimRedirectsFromCsv`` in owid-grapher, run by a human, after
``preflight.py`` validates the proposal.

Charts are selected by exactly one of ``--tag`` / ``--slugs`` / ``--dataset-id``
and matched against the views of every published MDIM (or only those passed via
repeatable ``--mdim``). A chart matches a view when their indicator IDs agree on
every slot: same set of y variables, same x/size/color (absent == absent) —
after stripping *decoration* indicators (population as Marimekko width / bubble
size, owid_region as continent coloring) from x/size/color on both sides, since
charts and views carry those inconsistently without changing what data is
plotted. A scatter's x axis is exempt — there population is the plotted
relationship, not decoration. Ties between several matching views are broken by
chart type; anything still ambiguous is reported, not proposed.

Outputs into ``--out``:
- ``charts.csv``                 — the selected source charts and their indicator slots
- ``multidim_views.csv``         — every candidate MDIM view (id A1.., B1.., ...)
- ``mapping_proposal.csv``       — one row per chart with match quality + target view
- ``mapping.json``               — combined machine record (confident, conflict-free matches)
- ``payloads/<slug>.json``       — one JSON per source chart, the copy-paste handoff unit
- ``redirects_for_cli.csv``      — the apply input for `yarn createMultiDimRedirectsFromCsv`
- ``migration_log_template.csv`` — (old_slug, mdim_slug, view_id, cutover_date) for analytics
- ``unmatched.md``               — human-readable report of everything not proposed
- ``_sources.json``              — machine record of the run inputs (don't hand-edit)

Optional ``overrides.csv`` in ``--out`` (never overwritten; re-runs pick it up):
``chart_id,action,note`` where action is ``SKIP``, ``<mdim_catalog_path>|<view_id>``,
or a bare view_config_id UUID.

Usage:
    ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
        .claude/skills/map-charts-to-mdim/scripts/extract_and_match.py \
        --tag "Economic Inequality" \
        --out ai/economic-inequality-charts-mdim-mapping
"""

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlencode

from etl.config import OWID_ENV

PUBLIC_HOST = "https://ourworldindata.org"
EXTRA_SLOTS = ("x", "size", "color")

# Decoration indicators style a chart without changing what data it plots, and charts vs
# MDIM views carry them inconsistently: an MDIM view adds x=population so its Marimekko
# tab has bar widths, a chart editor adds color=owid_region to color countries by
# continent. Requiring these slots to agree exactly silently dropped
# same-y-different-decoration pairs into `none` — with equal y sets they are not even a
# near miss, so nothing reported the gap. They are stripped from x/size/color (never
# from y) on BOTH sides before matching — except a scatter's x slot, which is the plotted
# relationship itself (see `effective()`); the raw slots stay in charts.csv /
# multidim_views.csv, and an exact match made across a decoration difference says so in
# the proposal's `note` column. Only these two families qualify — a color like
# "political regime" or a scatter x like GDP per capita is content and stays in the
# match key. (Matched against catalogPath, so version bumps keep matching; legacy
# variables with a NULL catalogPath are conservatively treated as content.)
# Both alternatives are end-anchored: the demography `population` dataset also carries
# population *density* columns (`population_density#population_density`,
# `historical#population_density_historical`, `projections#population_density_projection`)
# which an unanchored `#population` prefix would swallow — density is content, not
# decoration. The anchored family is exactly the raw head-counts: `#population`,
# `#population_historical`, `#population_projection`.
DECORATION_PATTERN = re.compile(
    r"/population/[^/#]+#population(_historical|_projection)?$"  # demography population head-counts
    r"|/regions/regions#\w+_region$"  # owid_region & friends (continent coloring)
)
QUALITIES = ("exact", "forced", "ambiguous", "near_miss", "none", "skipped")

PROPOSAL_COLUMNS = [
    "chart_id", "chart_slug", "chart_title", "chart_type", "chart_url", "chart_config_md5", "y_variable_ids",
    "match_quality", "tiebreak", "target_mdim_catalog_path", "target_mdim_slug",
    "target_view_id", "target_view_config_id", "target_view_config_md5", "target_url", "target_view_title",
    "target_chart_type", "n_candidates", "candidate_view_ids", "near_miss_detail",
    "shared_target_chart_ids", "conflict", "cli_required", "old_slugs", "cli_notes", "note",
]  # fmt: skip


def check_db_connection():
    """Fail fast with actionable guidance if the grapher DB isn't reachable; say which env resolved."""
    try:
        OWID_ENV.read_sql("SELECT 1")
    except Exception as e:  # noqa: BLE001 - connectivity preflight, surface a friendly hint
        raise SystemExit(
            "Cannot reach the grapher DB via OWID_ENV:\n"
            f"  {type(e).__name__}: {str(e).splitlines()[0]}\n\n"
            "This skill reads charts + MDIMs from the grapher DB. Point OWID_ENV at a DB\n"
            "that has both, by one of:\n"
            "  - `STAGING=1` (the current branch's staging DB; `STAGING=<name>` for another branch —\n"
            "    just being on the branch is NOT enough, the default is your local dev DB), or\n"
            "  - `ENV_FILE=<prod creds file> DATA_API_ENV=production` (e.g. .env.prod / .env.live), or\n"
            "  - `ENV_FILE=<your creds file>`.\n"
            "If you don't have a credentials file, ask which one to use — don't hardcode secrets."
        )
    # A reachable DB is not necessarily the intended one (a local dev DB also passes) —
    # print what resolved so a wrong-environment run is visible, not silent.
    print(f"grapher DB: {OWID_ENV.name}")


def excel_prefix(i: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA — MDIM row-id prefixes that survive >26 MDIMs."""
    s = ""
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(ord("A") + r) + s
    return s


def path_tail(catalog_path: str | None) -> str:
    """'grapher/ns/v/ds/table#col' -> 'table#col' for compact display."""
    return catalog_path.split("/")[-1] if catalog_path else ""


# ----- Charts ---------------------------------------------------------------------

CHART_SELECT = """
SELECT DISTINCT c.id AS chart_id, cc.slug AS chart_slug, cc.chartType AS chart_type,
       cc.full->>'$.title' AS title, c.publishedAt AS published_at, cc.fullMd5 AS config_md5
FROM charts c
JOIN chart_configs cc ON cc.id = c.configId
"""

# `charts.publishedAt` records the *first* publish and stays set after an unpublish — 308
# production charts are unpublished with the timestamp still on them. Selecting on it would
# put charts an editor has already retired into the migration CSV, and applying that CSV
# would make their dead URLs resolve again. The live state is `isPublished` in the config,
# the same test `preflight.py:unpublished_sources` uses.
LIVE_PUBLISHED = "COALESCE(cc.full->>'$.isPublished', 'false') = 'true'"


def resolve_charts(args) -> tuple[list[dict], dict]:
    """Return the selected published charts as dicts, plus the selection that produced them."""
    if args.tag:
        df = OWID_ENV.read_sql(
            CHART_SELECT
            + f"""
            JOIN chart_tags ct ON ct.chartId = c.id
            JOIN tags t ON t.id = ct.tagId
            WHERE {LIVE_PUBLISHED} AND t.name = %(tag)s
            """,
            params={"tag": args.tag},
        )
        nearby = OWID_ENV.read_sql(
            "SELECT name FROM tags WHERE name LIKE %(like)s AND name != %(tag)s",
            params={"like": args.tag + "%", "tag": args.tag},
        )
        if not nearby.empty:
            print(f"note: tag match is exact; nearby tags NOT included: {nearby['name'].tolist()}")
        selection = {"mode": "tag", "value": args.tag}
    elif args.slugs:
        raw = args.slugs
        if raw.startswith("@"):
            slugs = [s.strip() for s in Path(raw[1:]).read_text().splitlines() if s.strip()]
        else:
            slugs = [s.strip() for s in raw.split(",") if s.strip()]
        df = OWID_ENV.read_sql(
            CHART_SELECT + f"WHERE {LIVE_PUBLISHED} AND cc.slug IN %(slugs)s",
            params={"slugs": tuple(slugs)},
        )
        missing = sorted(set(slugs) - set(df["chart_slug"]))
        if missing:
            print(f"warning: {len(missing)} slug(s) not found as published charts: {missing}")
        selection = {"mode": "slugs", "value": slugs}
    else:
        df = OWID_ENV.read_sql(
            CHART_SELECT
            + f"""
            WHERE {LIVE_PUBLISHED} AND c.id IN (
                SELECT DISTINCT cd.chartId FROM chart_dimensions cd
                JOIN variables v ON v.id = cd.variableId
                WHERE v.datasetId = %(ds)s
            )
            """,
            params={"ds": args.dataset_id},
        )
        selection = {"mode": "dataset-id", "value": args.dataset_id}

    if df.empty:
        raise SystemExit(f"No published charts found for {selection['mode']}={selection['value']!r}.")

    charts = []
    for r in df.sort_values("chart_slug").to_dict("records"):
        if not r["chart_slug"]:
            print(f"warning: chart {r['chart_id']} has no slug — cannot be a redirect source, excluded")
            continue
        r["published_at"] = str(r["published_at"])
        charts.append(r)
    return charts, selection


def attach_chart_slots(charts: list[dict]) -> None:
    """Fill each chart dict with y (frozenset of variable ids) and x/size/color (id or None)."""
    df = OWID_ENV.read_sql(
        "SELECT chartId, variableId, property, `order` FROM chart_dimensions "
        "WHERE chartId IN %(ids)s ORDER BY chartId, `order`",
        params={"ids": tuple(c["chart_id"] for c in charts)},
    )
    slots: dict[int, dict] = defaultdict(lambda: {slot: [] for slot in ("y", *EXTRA_SLOTS)})
    for r in df.to_dict("records"):
        s = slots[r["chartId"]]
        if r["property"] in s:
            s[r["property"]].append(int(r["variableId"]))
    for c in charts:
        s = slots[c["chart_id"]]
        if len(s["y"]) != len(set(s["y"])):
            print(f"warning: chart {c['chart_id']} ({c['chart_slug']}) repeats a y variable — deduplicated")
        c["y"] = frozenset(s["y"])
        for slot in EXTRA_SLOTS:
            c[slot] = s[slot][0] if s[slot] else None
        # A chart signature holds one indicator per x/size/color. Several distinct ones in a
        # slot means the signature can't be represented — truncating to the first could
        # spuriously exact-match a view that lacks the rest. Excluded, exactly as the
        # view-side normalization excludes the mirror-image shape.
        multi = [slot for slot in EXTRA_SLOTS if len(set(s[slot])) > 1]
        c["exclude_reason"] = (
            f"multiple indicators in {', '.join(repr(slot) for slot in multi)} — a chart slot holds one"
            if multi
            else ""
        )
        if multi:
            print(
                f"warning: chart {c['chart_id']} ({c['chart_slug']}) has {c['exclude_reason']} — excluded from matching"
            )
        if not c["y"]:
            print(f"warning: chart {c['chart_id']} ({c['chart_slug']}) has no y indicators")


# ----- MDIM views -----------------------------------------------------------------


def normalize_entries(raw) -> list[tuple[int | None, str | None]]:
    """views[].indicators.* entries -> [(variable_id, catalog_path)], tolerating every stored shape."""
    if raw is None:
        return []
    if not isinstance(raw, list):
        raw = [raw]
    out = []
    for e in raw:
        if isinstance(e, dict):
            if e.get("id") is not None:
                out.append((int(e["id"]), e.get("catalogPath")))
            elif e.get("catalogPath"):
                out.append((None, e["catalogPath"]))
        elif isinstance(e, int):
            out.append((e, None))
        elif isinstance(e, str):
            out.append((None, e))
    return out


def get_mdims_and_views(restrict: list[str], host: str) -> tuple[list[dict], list[dict], dict[int, str]]:
    """Return (mdims, views, id->catalogPath map) for every published MDIM (or the --mdim subset)."""
    sql = "SELECT id, catalogPath, slug, config FROM multi_dim_data_pages WHERE published = 1 AND slug IS NOT NULL"
    params = {}
    if restrict:
        sql += " AND catalogPath IN %(cps)s"
        params["cps"] = tuple(restrict)
    # No ORDER BY in SQL: the multi-MB JSON configs travel with each row and blow
    # the server's sort buffer (error 1038) — sort client-side instead.
    df = OWID_ENV.read_sql(sql, params=params or None)
    df = df.sort_values("catalogPath").reset_index(drop=True)
    if restrict:
        missing = sorted(set(restrict) - set(df["catalogPath"]))
        if missing:
            raise SystemExit(
                f"MDIM(s) not found as published-with-slug in this DB: {missing}\n"
                "Check the catalogPath spelling (multi_dim_data_pages.catalogPath) and that they're published."
            )
    if df.empty:
        raise SystemExit("No published MDIMs found in this DB.")

    mdims, views = [], []
    for i, row in enumerate(df.to_dict("records")):
        cfg = row["config"]
        cfg = json.loads(cfg) if isinstance(cfg, str) else cfg
        prefix = excel_prefix(i)
        mdim = {
            "id": int(row["id"]),
            "catalog_path": row["catalogPath"],
            "slug": row["slug"],
            "prefix": prefix,
            "title": (cfg.get("title") or {}).get("title", ""),
            "n_views": len(cfg.get("views", [])),
        }
        mdims.append(mdim)
        for n, v in enumerate(cfg.get("views", []), start=1):
            dims = v.get("dimensions", {})
            query_str = urlencode(sorted(dims.items()))  # grapher builds redirect targets key-sorted too
            ind = v.get("indicators", {}) or {}
            view = {
                "row_id": f"{prefix}{n}",
                "mdim_id": mdim["id"],
                "mdim_catalog_path": mdim["catalog_path"],
                "mdim_slug": mdim["slug"],
                "dims": dims,
                "view_id": "__".join(f"{k}={v}" for k, v in sorted(dims.items())),
                "query_str": query_str,
                "url": f"{host}/grapher/{mdim['slug']}?{query_str}",
                "view_config_id": v.get("fullConfigId"),
                "title": (v.get("config") or {}).get("title") or mdim["title"],
                "_raw_y": normalize_entries(ind.get("y")),
                **{f"_raw_{slot}": normalize_entries(ind.get(slot)) for slot in EXTRA_SLOTS},
            }
            views.append(view)

    resolve_view_indicator_ids(views)

    id_to_path: dict[int, str] = {}
    for v in views:
        for slot in ("y", *EXTRA_SLOTS):
            for vid, cp in v[f"_raw_{slot}"]:
                if vid is not None and cp:
                    id_to_path[vid] = cp

    usable = []
    for v in views:
        # A view with an unresolvable indicator would get a truncated signature and could
        # spuriously exact-match a chart that only carries the remaining indicators.
        unresolved = [cp for slot in ("y", *EXTRA_SLOTS) for vid, cp in v[f"_raw_{slot}"] if vid is None]
        if unresolved:
            print(
                f"warning: view {v['row_id']} ({v['mdim_slug']}?{v['query_str']}) has unresolvable "
                f"indicator(s) {unresolved} — excluded from matching"
            )
            continue
        # A chart holds a single indicator per x/size/color slot, so a view carrying several
        # in one slot has no chart-shaped signature — truncating to the first could spuriously
        # exact-match a chart that lacks the rest. Exclude the view, like unresolved ones.
        multi = [slot for slot in EXTRA_SLOTS if sum(1 for vid, _ in v[f"_raw_{slot}"] if vid is not None) > 1]
        if multi:
            print(
                f"warning: view {v['row_id']} ({v['mdim_slug']}?{v['query_str']}) has multiple indicators "
                f"in {', '.join(repr(s) for s in multi)} — excluded from matching (a chart slot holds one)"
            )
            continue
        v["y"] = frozenset(vid for vid, _ in v["_raw_y"] if vid is not None)
        for slot in EXTRA_SLOTS:
            entries = [vid for vid, _ in v[f"_raw_{slot}"] if vid is not None]
            v[slot] = entries[0] if entries else None
        if not v["view_config_id"]:
            print(
                f"warning: view {v['row_id']} ({v['mdim_slug']}?{v['query_str']}) has no fullConfigId — excluded from matching"
            )
            continue
        usable.append(v)

    attach_view_config_facts(usable, [m["id"] for m in mdims])
    return mdims, usable, id_to_path


def resolve_view_indicator_ids(views: list[dict]) -> None:
    """Batch-resolve catalogPath-only indicator entries to variable ids."""
    unresolved = sorted(
        {cp for v in views for slot in ("y", *EXTRA_SLOTS) for vid, cp in v[f"_raw_{slot}"] if vid is None and cp}
    )
    if not unresolved:
        return
    df = OWID_ENV.read_sql(
        "SELECT id, catalogPath FROM variables WHERE catalogPath IN %(cps)s", params={"cps": tuple(unresolved)}
    )
    by_path = dict(zip(df["catalogPath"], df["id"].astype(int)))
    for v in views:
        for slot in ("y", *EXTRA_SLOTS):
            v[f"_raw_{slot}"] = [(vid if vid is not None else by_path.get(cp), cp) for vid, cp in v[f"_raw_{slot}"]]
            for vid, cp in v[f"_raw_{slot}"]:
                if vid is None:
                    print(
                        f"warning: view {v['row_id']} ({v['mdim_slug']}): unresolvable catalogPath {cp!r} in '{slot}'"
                    )


def attach_view_config_facts(views: list[dict], mdim_ids: list[int]) -> None:
    """Attach each view's chart type and the md5 of its rendered config.

    The md5 is the target-side mirror of a chart's `config_md5`: a reviewer approves a
    specific rendering of the target view, not just its slot in the MDIM. It cannot be
    replaced by the view's config id — grapher keys view configs on the dimension-derived
    view id and *updates the row in place* when an MDIM is re-exported
    (adminSiteServer/multiDim.ts), so the id survives content changes and only ever
    changes together with the view id, which is already tracked.
    """
    df = OWID_ENV.read_sql(
        "SELECT mx.chartConfigId AS cc_id, cc.chartType AS chart_type, cc.fullMd5 AS config_md5 "
        "FROM multi_dim_x_chart_configs mx JOIN chart_configs cc ON cc.id = mx.chartConfigId "
        "WHERE mx.multiDimId IN %(ids)s",
        params={"ids": tuple(mdim_ids)},
    )
    by_cc = {r["cc_id"]: r for r in df.to_dict("records")}
    for v in views:
        cc = by_cc.get(v["view_config_id"]) or {}
        v["chart_type"] = cc.get("chart_type")
        v["view_config_md5"] = cc.get("config_md5") or ""


# ----- Matching -------------------------------------------------------------------


def match_charts(charts: list[dict], views: list[dict], id_to_path: dict[int, str]) -> None:
    """Set match_quality / target / candidates / near-miss info on each chart dict."""

    def effective(rec) -> tuple:
        """x/size/color with decoration indicators treated as absent.

        On a scatter the x axis IS the content — population there is the plotted
        relationship (e.g. "GDP per capita vs. population"), not Marimekko bar-width
        decoration — so ScatterPlot records keep their x slot literal on both sides.
        A scatter's size=population (bubble size) and color=owid_region stay decoration.
        """
        slots = []
        for s in EXTRA_SLOTS:
            vid = rec[s]
            content_slot = s == "x" and rec["chart_type"] == "ScatterPlot"
            strip = vid and not content_slot and DECORATION_PATTERN.search(id_to_path.get(vid) or "")
            slots.append(None if strip else vid)
        return tuple(slots)

    def slot_label(vid) -> str:
        return f"{vid} ({path_tail(id_to_path.get(vid))})" if vid else "absent"

    by_key: dict[tuple, list[dict]] = defaultdict(list)
    for v in views:
        by_key[(v["y"], *effective(v))].append(v)

    for c in charts:
        c.update({"quality": "none", "tiebreak": "", "target": None, "candidates": [], "near_misses": [], "note": ""})
        if c.get("exclude_reason"):
            # An override can still force a target: that is a human choosing deliberately.
            c["note"] = c["exclude_reason"]
            continue
        if not c["y"]:
            c["note"] = "chart has no y indicators"
            continue
        c_eff = effective(c)
        candidates = by_key.get((c["y"], *c_eff), [])
        if len(candidates) == 1:
            c["quality"], c["target"] = "exact", candidates[0]
        elif len(candidates) > 1:
            survivors = [v for v in candidates if v["chart_type"] == c["chart_type"]]
            if len(survivors) == 1:
                c["quality"], c["target"], c["tiebreak"] = "exact", survivors[0], "chart_type"
                c["candidates"] = candidates
            else:
                c["quality"], c["candidates"] = "ambiguous", candidates
        else:
            # Any view sharing an indicator, not just one whose y set strictly contains or is
            # contained by the chart's. A chart plotting {A, B} against a view plotting {A, C}
            # is neither, yet A is shared — classifying that as "none" would have the
            # suggestions report assert that no view carries any of the chart's indicators,
            # which is exactly what it is not. That broader reading is what the CSV legend
            # has always documented ("indicator sets overlap but differ").
            near = [v for v in views if effective(v) == c_eff and (v["y"] & c["y"]) and v["y"] != c["y"]]
            near.sort(key=lambda v: (-len(v["y"] & c["y"]), len(v["y"] ^ c["y"])))
            if near:
                c["quality"], c["near_misses"] = "near_miss", near[:3]

        # A match made across a decoration difference is still a match, but the reviewer
        # should see that the two sides did not agree literally.
        if c["target"] is not None:
            diffs = [
                f"{s}: chart {slot_label(c[s])} vs view {slot_label(c['target'][s])}"
                for s in EXTRA_SLOTS
                if c[s] != c["target"][s]
            ]
            if diffs:
                c["note"] = "decoration difference ignored — " + "; ".join(diffs)


def describe_near_miss(c: dict, id_to_path: dict[int, str]) -> str:
    """Both sides of the gap, per candidate view.

    A near miss is any partial overlap, so the two sets can each hold indicators the other
    lacks. Reporting only one side — or labelling the view's surplus as the chart's — states
    the gap backwards, which is worse than not stating it.
    """

    def ids(values) -> str:
        return ", ".join(f"{i} ({path_tail(id_to_path.get(i))})" for i in sorted(values))

    parts = []
    for v in c["near_misses"]:
        sides = []
        if v["y"] - c["y"]:
            sides.append(f"view extra: {ids(v['y'] - c['y'])}")
        if c["y"] - v["y"]:
            sides.append(f"chart extra: {ids(c['y'] - v['y'])}")
        parts.append(f"{v['mdim_slug']}:{v['view_id']} [{'; '.join(sides)}]")
    return " | ".join(parts)


def apply_overrides(charts: list[dict], views: list[dict], out: Path) -> None:
    path = out / "overrides.csv"
    if not path.exists():
        return
    with open(path) as f:
        rows = list(csv.DictReader(f))
    if rows and not {"chart_id", "action"} <= set(rows[0].keys()):
        raise SystemExit(f"{path} must have columns: chart_id,action[,note]")
    overrides = {int(r["chart_id"]): r for r in rows if (r.get("chart_id") or "").strip()}
    by_id = {c["chart_id"]: c for c in charts}
    for chart_id, r in overrides.items():
        c = by_id.get(chart_id)
        if c is None:
            print(f"warning: overrides.csv chart_id {chart_id} is not in the current selection — ignored")
            continue
        action = r["action"].strip()
        c["note"] = (r.get("note") or "").strip()
        if action.upper() == "SKIP":
            c.update({"quality": "skipped", "target": None, "tiebreak": ""})
            continue
        if "|" in action:
            cp, view_id = action.split("|", 1)
            found = [v for v in views if v["mdim_catalog_path"] == cp.strip() and v["view_id"] == view_id.strip()]
        else:
            found = [v for v in views if v["view_config_id"] == action]
        if len(found) != 1:
            raise SystemExit(
                f"overrides.csv: cannot resolve action {action!r} for chart {chart_id} "
                f"({len(found)} views matched). Use '<mdim_catalog_path>|<view_id>' or a view_config_id "
                "from multidim_views.csv."
            )
        c.update({"quality": "forced", "target": found[0], "tiebreak": ""})
    print(f"overrides: applied {len(overrides)} row(s) from {path.name}")


# ----- Conflict checks --------------------------------------------------------------


def check_conflicts(charts: list[dict], mdims: list[dict]) -> None:
    """Replicate the CLI's own validation, read-only, at proposal time.

    The apply path is `yarn createMultiDimRedirectsFromCsv`, which runs ONE transaction:
    any row it rejects aborts the whole migration. So every check here mirrors a check
    the CLI performs, and a hit means "fix before running", not "skip this row".

    Two situations the CLI *handles* and the admin API refuses — recorded as
    c["cli_required"], NOT as conflicts:
      - incoming chart_slug_redirects (old slugs pointing at this chart): the CLI's
        replaceChartSlugRedirects deletes each row and re-creates it as a
        multi_dim_redirects row aimed at the MDIM view, so old slugs keep working
        in one hop. The admin API 400s on these.
      - site redirects pointing AT this chart: replaceSiteRedirects repoints them.

    Sets, on each matched chart: c["conflict"] (blocker string), c["cli_required"]
    (list of reasons the CLI is mandatory), c["cli_notes"] (lossy-but-fine warnings),
    c["old_slugs"] (aliases that will be migrated), or c["already_done"]=True.
    """
    matched = [c for c in charts if c["target"] is not None]
    for c in charts:
        c["conflict"], c["already_done"] = "", False
        c["cli_required"], c["cli_notes"], c["old_slugs"] = [], [], []
    if not matched:
        return
    sources = tuple(f"/grapher/{c['chart_slug']}" for c in matched)
    slugs = tuple(c["chart_slug"] for c in matched)

    mdr = OWID_ENV.read_sql(
        "SELECT source, multiDimId, viewConfigId FROM multi_dim_redirects WHERE source IN %(s)s",
        params={"s": sources},
    )
    mdr_by_source = {r["source"]: r for r in mdr.to_dict("records")}

    site = OWID_ENV.read_sql(
        "SELECT source, target FROM redirects WHERE source IN %(s)s OR target IN %(s)s", params={"s": sources}
    )
    site_sources = {r["source"]: r["target"] for r in site.to_dict("records")}
    site_targets = defaultdict(list)
    for r in site.to_dict("records"):
        site_targets[r["target"]].append(r["source"])

    own_old = OWID_ENV.read_sql("SELECT slug FROM chart_slug_redirects WHERE slug IN %(s)s", params={"s": slugs})
    own_old_slugs = set(own_old["slug"])

    # Old slugs pointing at each source chart. The CLI migrates these; it also drops
    # their target_query_param (multi_dim_redirects has no column for it).
    incoming = OWID_ENV.read_sql(
        "SELECT csr.slug AS old_slug, csr.target_query_param, cc.slug AS chart_slug FROM chart_slug_redirects csr "
        "JOIN charts c ON c.id = csr.chart_id JOIN chart_configs cc ON cc.id = c.configId "
        "WHERE cc.slug IN %(s)s",
        params={"s": slugs},
    )
    incoming_by_slug: dict[str, list[dict]] = defaultdict(list)
    for r in incoming.to_dict("records"):
        incoming_by_slug[r["chart_slug"]].append(r)

    # Each migrated old slug is re-validated by the CLI as a NEW redirect source (after
    # its chart_slug_redirects row is deleted), so it must not already be a source in
    # `redirects` or `multi_dim_redirects`.
    old_sources = tuple(f"/grapher/{r['old_slug']}" for rows in incoming_by_slug.values() for r in rows)
    taken_old_sources: set[str] = set()
    if old_sources:
        taken_old_sources = set(
            OWID_ENV.read_sql(
                "SELECT source FROM redirects WHERE source IN %(s)s "
                "UNION SELECT source FROM multi_dim_redirects WHERE source IN %(s)s",
                params={"s": old_sources},
            )["source"]
        )

    # Target side, once per targeted MDIM: its own /grapher/<slug> must not be a redirect source.
    targeted_slugs = tuple({c["target"]["mdim_slug"] for c in matched})
    targeted_sources = tuple(f"/grapher/{s}" for s in targeted_slugs)
    bad_targets = set(
        OWID_ENV.read_sql(
            "SELECT source FROM redirects WHERE source IN %(s)s "
            "UNION SELECT source FROM multi_dim_redirects WHERE source IN %(s)s",
            params={"s": targeted_sources},
        )["source"]
    ) | {
        f"/grapher/{s}"
        for s in OWID_ENV.read_sql(
            "SELECT slug FROM chart_slug_redirects WHERE slug IN %(s)s", params={"s": targeted_slugs}
        )["slug"]
    }

    for c in matched:
        source, slug, t = f"/grapher/{c['chart_slug']}", c["chart_slug"], c["target"]
        reasons = []

        # Collected BEFORE the already-redirected exit below. An `already_done` chart is
        # unpublished by hand, and hand-unpublishing a chart that carries old slugs deletes
        # its chart_slug_redirects rows and turns those URLs into hard 404s — so preflight
        # needs oldSlugs on those entries precisely to refuse that. Everything derived from
        # them (cli_required, the clash check) stays below: it only applies to rows the CLI
        # will actually create.
        rows = sorted(incoming_by_slug.get(slug, []), key=lambda r: r["old_slug"])
        c["old_slugs"] = [r["old_slug"] for r in rows]

        prior = mdr_by_source.get(source)
        if prior is not None:
            if int(prior["multiDimId"]) == t["mdim_id"] and prior["viewConfigId"] == t["view_config_id"]:
                c["already_done"] = True
                continue
            reasons.append(f"already redirected to a DIFFERENT mdim view (multiDimId={prior['multiDimId']})")
        if source in site_sources:
            reasons.append(f"already a site redirect source -> {site_sources[source]}")
        if slug in own_old_slugs:
            reasons.append("chart slug is itself an old slug in chart_slug_redirects")
        if slug == t["mdim_slug"]:
            reasons.append("self-redirect: chart slug equals the target MDIM slug")
        if f"/grapher/{t['mdim_slug']}" in bad_targets:
            reasons.append(f"target /grapher/{t['mdim_slug']} is itself a redirect source")

        # Handled by the CLI (and only by the CLI).
        if rows:
            c["cli_required"].append(f"{len(rows)} incoming chart_slug_redirects: {c['old_slugs']}")
            with_params = [r["old_slug"] for r in rows if r["target_query_param"]]
            if with_params:
                c["cli_notes"].append(
                    f"target_query_param dropped when flattening: {with_params} "
                    "(multi_dim_redirects has no column for it)"
                )
            clashing = [s for s in c["old_slugs"] if f"/grapher/{s}" in taken_old_sources]
            if clashing:
                reasons.append(f"old slug(s) already a redirect source elsewhere: {clashing}")
        if source in site_targets:
            c["cli_required"].append(f"site redirect(s) point AT this chart: {site_targets[source]}")

        c["conflict"] = "; ".join(reasons)


# ----- Outputs ----------------------------------------------------------------------


def proposed_charts(charts: list[dict]) -> list[dict]:
    """Charts that go into the CLI CSV: matched, conflict-free, not already redirected."""
    return [c for c in charts if c["target"] is not None and not c["conflict"] and not c["already_done"]]


def fill_shared_targets(charts: list[dict]) -> None:
    by_target = defaultdict(list)
    for c in charts:
        if c["target"] is not None:
            by_target[c["target"]["view_config_id"]].append(c["chart_id"])
    for c in charts:
        ids = by_target.get(c["target"]["view_config_id"], []) if c["target"] is not None else []
        c["shared_with"] = ",".join(str(i) for i in ids) if len(ids) > 1 else ""


def write_charts_csv(out: Path, charts: list[dict], host: str) -> None:
    with open(out / "charts.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["chart_id", "chart_slug", "title", "chart_type", "published_at",
                    "y_variable_ids", "x_variable_id", "size_variable_id", "color_variable_id", "chart_url"])  # fmt: skip
        for c in charts:
            w.writerow([c["chart_id"], c["chart_slug"], c["title"], c["chart_type"], c["published_at"],
                        "|".join(str(i) for i in sorted(c["y"])), c["x"], c["size"], c["color"],
                        f"{host}/grapher/{c['chart_slug']}"])  # fmt: skip


def write_views_csv(out: Path, views: list[dict]) -> None:
    with open(out / "multidim_views.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "mdim_catalog_path", "mdim_slug", "view_id", "view_config_id", "dimensions_query",
                    "y_variable_ids", "x_variable_id", "size_variable_id", "color_variable_id",
                    "chart_type", "view_title", "view_url"])  # fmt: skip
        for v in views:
            w.writerow([v["row_id"], v["mdim_catalog_path"], v["mdim_slug"], v["view_id"], v["view_config_id"],
                        v["query_str"], "|".join(str(i) for i in sorted(v["y"])), v["x"], v["size"], v["color"],
                        v["chart_type"], v["title"], v["url"]])  # fmt: skip


def write_proposal_csv(out: Path, charts: list[dict], id_to_path: dict[int, str], host: str) -> None:
    with open(out / "mapping_proposal.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=PROPOSAL_COLUMNS)
        w.writeheader()
        for c in charts:
            t = c["target"]
            w.writerow({
                "chart_id": c["chart_id"], "chart_slug": c["chart_slug"], "chart_title": c["title"],
                "chart_type": c["chart_type"], "chart_url": f"{host}/grapher/{c['chart_slug']}",
                "chart_config_md5": c["config_md5"],
                "y_variable_ids": "|".join(str(i) for i in sorted(c["y"])),
                "match_quality": c["quality"], "tiebreak": c["tiebreak"],
                "target_mdim_catalog_path": t["mdim_catalog_path"] if t else "",
                "target_mdim_slug": t["mdim_slug"] if t else "",
                "target_view_id": t["view_id"] if t else "",
                "target_view_config_id": t["view_config_id"] if t else "",
                "target_view_config_md5": t["view_config_md5"] if t else "",
                "target_url": t["url"] if t else "",
                "target_view_title": t["title"] if t else "",
                "target_chart_type": t["chart_type"] if t else "",
                "n_candidates": len(c["candidates"]),
                "candidate_view_ids": " | ".join(f"{v['mdim_slug']}:{v['view_id']}" for v in c["candidates"]),
                "near_miss_detail": describe_near_miss(c, id_to_path),
                "shared_target_chart_ids": c["shared_with"],
                "conflict": "already redirected (same target) — nothing to do" if c["already_done"] else c["conflict"],
                "cli_required": "; ".join(c["cli_required"]),
                "old_slugs": "|".join(c["old_slugs"]),
                "cli_notes": "; ".join(c["cli_notes"]),
                "note": c["note"],
            })  # fmt: skip


def chart_json(c: dict) -> dict:
    # configMd5 lets preflight.py detect charts edited after the proposal was written.
    return {"id": c["chart_id"], "slug": c["chart_slug"], "title": c["title"], "configMd5": c["config_md5"]}


def redirect_json(c: dict) -> dict:
    t = c["target"]
    entry = {
        "source": f"/grapher/{c['chart_slug']}",
        "chart": chart_json(c),
        "target": {
            "multiDimId": t["mdim_id"],
            "catalogPath": t["mdim_catalog_path"],
            "mdimSlug": t["mdim_slug"],
            "viewId": t["view_id"],
            "viewConfigId": t["view_config_id"],
            # Lets preflight.py detect a target view edited after the proposal was written.
            "viewConfigMd5": t["view_config_md5"],
            "dimensions": t["dims"],
            "queryStr": t["query_str"],
            "url": t["url"],
        },
        "matchQuality": c["quality"],
    }
    if c["shared_with"]:
        entry["sharedTargetChartIds"] = [int(i) for i in c["shared_with"].split(",")]
    if c["old_slugs"]:
        # Aliases the CLI will migrate from chart_slug_redirects into multi_dim_redirects.
        entry["oldSlugs"] = c["old_slugs"]
    if c["cli_notes"]:
        entry["notes"] = c["cli_notes"]
    return entry


def cli_target_path(c: dict) -> str:
    """`/grapher/<mdim-slug>?<key-sorted dims>` — the CLI re-sorts anyway, but match it."""
    t = c["target"]
    return f"/grapher/{t['mdim_slug']}" + (f"?{t['query_str']}" if t["query_str"] else "")


def write_cli_csv(out: Path, charts: list[dict]) -> tuple[Path, int]:
    """Write the `;`-delimited source;target file consumed by the grapher CLI.

    Format per owid-grapher devTools/createMultiDimRedirectsFromCsv.ts parseCsvEntries():
    two positional columns, `;` delimiter, a header tolerated ONLY on line 1, no comment
    lines anywhere, no duplicate sources, both fields must start with "/". Sources carry
    no query string — serving matches the bare path, so params there would never fire.
    """
    proposed = proposed_charts(charts)
    path = out / "redirects_for_cli.csv"
    lines = ["source;target"]
    for c in proposed:
        lines.append(f"/grapher/{c['chart_slug']};{cli_target_path(c)}")
    path.write_text("\n".join(lines) + "\n")
    validate_cli_csv(path)
    return path, len(proposed)


def validate_cli_csv(path: Path) -> None:
    """Re-parse under the CLI's own rules — one bad row aborts its whole transaction."""
    rows = [ln for ln in path.read_text().split("\n")]
    seen: set[str] = set()
    for i, line in enumerate(rows):
        if not line.strip():
            continue
        fields = line.split(";")
        if len(fields) != 2:
            raise SystemExit(f"{path}:{i + 1}: expected exactly 2 `;`-separated fields, got {len(fields)}")
        source, target = (f.strip() for f in fields)
        if not source.startswith("/") or not target.startswith("/"):
            if i == 0:
                continue  # header, tolerated on line 1 only
            raise SystemExit(f"{path}:{i + 1}: both fields must start with '/' (the CLI only skips a line-1 header)")
        if source in seen:
            raise SystemExit(f"{path}:{i + 1}: duplicate source {source!r} — the CLI rejects duplicates")
        seen.add(source)
        if "?" in source:
            raise SystemExit(f"{path}:{i + 1}: source must not carry a query string ({source!r})")
        if source.endswith("/") or not target.startswith("/grapher/"):
            raise SystemExit(f"{path}:{i + 1}: source cannot end with '/' and target must be a /grapher/ path")


def write_migration_log_template(out: Path, charts: list[dict]) -> Path:
    """Analytics can't reconstruct this mapping after the fact — record it at cutover.

    Post-cutover the source chart's view history goes chart_id = NULL, and
    prod_semantic.redirects carries no multi_dim_redirects rows, so (old_slug -> mdim)
    exists only in grapher MySQL and in this file.
    """
    path = out / "migration_log_template.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["old_slug", "mdim_slug", "view_id", "cutover_date"])
        for c in proposed_charts(charts):
            t = c["target"]
            for slug in [c["chart_slug"], *c["old_slugs"]]:
                w.writerow([slug, t["mdim_slug"], t["view_id"], ""])
    return path


def write_payloads(out: Path, charts: list[dict]) -> int:
    """One JSON per source chart under <out>/payloads/ — the copy-paste handoff unit.

    Mirrors the explorer→MDIM redirect deliverable convention: each payload file
    describes exactly ONE source page and its redirect. The combined mapping.json
    stays the machine record that preflight.py and audit_references.py read.
    """
    payload_dir = out / "payloads"
    payload_dir.mkdir(exist_ok=True)
    for stale in payload_dir.glob("*.json"):
        stale.unlink()
    proposed = proposed_charts(charts)
    for c in proposed:
        (payload_dir / f"{c['chart_slug']}.json").write_text(json.dumps(redirect_json(c), indent=2) + "\n")
    return len(proposed)


def write_mapping_json(out: Path, charts: list[dict], mdims: list[dict], selection: dict) -> dict:
    proposed = proposed_charts(charts)
    conflicted = [c for c in charts if c["target"] is not None and c["conflict"]]
    done = [c for c in charts if c["already_done"]]
    unmatched = [c for c in charts if c["target"] is None and c["quality"] != "skipped"]
    stats = {"charts": len(charts), **{q: sum(c["quality"] == q for c in charts) for q in QUALITIES},
             "conflicts": len(conflicted), "already_done": len(done), "proposed": len(proposed),
             "cli_required": sum(1 for c in proposed if c["cli_required"])}  # fmt: skip
    data = {
        "selection": selection,
        "mdims": [{"id": m["id"], "catalogPath": m["catalog_path"], "slug": m["slug"]} for m in mdims],
        "stats": stats,
        "redirects": [redirect_json(c) for c in proposed],
        "conflicts": [{**redirect_json(c), "conflictReason": c["conflict"]} for c in conflicted],
        "already_done": [redirect_json(c) for c in done],
        "unmatched": [
            {
                "chartId": c["chart_id"],
                "slug": c["chart_slug"],
                "quality": c["quality"],
                "candidates": [
                    {"mdimSlug": v["mdim_slug"], "viewId": v["view_id"], "viewConfigId": v["view_config_id"]}
                    for v in (c["candidates"] or c["near_misses"])
                ],
            }  # fmt: skip
            for c in unmatched
        ],
    }
    (out / "mapping.json").write_text(json.dumps(data, indent=2) + "\n")
    return stats


def indicator_names(ids: set[int]) -> dict[int, str]:
    """{variable id -> name} for readable suggestions."""
    if not ids:
        return {}
    df = OWID_ENV.read_sql("SELECT id, name FROM variables WHERE id IN %(i)s", params={"i": tuple(sorted(ids))})
    return {int(i): n for i, n in zip(df["id"], df["name"]) if n}


def write_mdim_suggestions(out: Path, charts: list[dict], id_to_path: dict[int, str], host: str) -> int:
    """What would have to change in the MDIMs for the unmatched charts to be redirectable.

    A near miss is not just a failure to match — it is a precise description of the gap:
    the view carries an indicator the chart doesn't, or vice versa. Stated that way it
    becomes an MDIM-authoring to-do, which is usually the real fix for a chart that has
    no home yet. Charts with no overlap at all need a new view instead.
    """
    near = [c for c in charts if c["quality"] == "near_miss"]
    # `quality` starts at "none" and `match_charts` bails out before searching for any chart
    # it cannot represent (several indicators in one x/size/color slot, or no y at all),
    # leaving that default in place with the reason in `note`. No overlap search ever ran
    # for those, so listing them as "no view shares this chart's indicators" would assert
    # something nobody checked — and could send someone off to author a view that already
    # exists. They need the shape problem resolved first, so they get their own section.
    none = [c for c in charts if c["quality"] == "none" and not c["note"]]
    unchecked = [c for c in charts if c["quality"] == "none" and c["note"]]
    wanted: set[int] = set()
    for c in near + none:
        wanted |= set(c["y"])
        for v in c["near_misses"]:
            wanted |= set(v["y"])
    names = indicator_names(wanted)

    def label(i: int) -> str:
        return names.get(i) or path_tail(id_to_path.get(i)) or str(i)

    lines = [
        "# Suggested MDIM changes",
        "",
        "These charts have no redirect target today. Each entry says what the MDIM would",
        "need for the chart to become redirectable — decide per case whether the MDIM",
        "*should* carry it, or whether the chart simply retires without a successor.",
        "",
    ]

    if near:
        lines += [
            f"## Close — indicator sets overlap but differ ({len(near)})",
            "",
            "The closest view shares at least one indicator with the chart and differs by the",
            "ones listed — in either direction, or both at once. Adding a view with the chart's",
            "exact indicator set is what makes the redirect possible.",
            "",
        ]
        for c in near:
            lines.append(f"### {c['chart_slug']} — [{c['title']}]({host}/grapher/{c['chart_slug']})")
            lines.append("")
            for v in c["near_misses"]:
                view_extra = sorted(v["y"] - c["y"])
                chart_extra = sorted(c["y"] - v["y"])
                lines.append(f"- **{v['mdim_slug']}** view `{v['view_id']}` ([open]({v['url']}))")
                if view_extra:
                    names_s = ", ".join(f"`{label(i)}`" for i in view_extra)
                    lines.append(f"    - the view also plots {names_s} — the chart does not")
                if chart_extra:
                    names_s = ", ".join(f"`{label(i)}`" for i in chart_extra)
                    lines.append(f"    - the chart also plots {names_s} — the view does not")
                lines.append(f"    - to redirect: add a view plotting exactly {len(c['y'])} indicator(s) — "
                             + ", ".join(f"`{label(i)}`" for i in sorted(c["y"])))  # fmt: skip
            lines.append("")

    if none:
        lines += [
            f"## No overlap — would need a new view ({len(none)})",
            "",
            "No published MDIM view shares this chart's indicators at all. Redirecting these",
            "means authoring a view for them, which is a bigger call than a near miss:",
            "it is usually the question of whether the MDIM should cover this topic.",
            "",
        ]
        for c in none:
            inds = ", ".join(f"`{label(i)}`" for i in sorted(c["y"])[:4])
            more = "" if len(c["y"]) <= 4 else f" (+{len(c['y']) - 4} more)"
            lines.append(f"- **{c['chart_slug']}** — plots {inds}{more}")
        lines.append("")

    if unchecked:
        lines += [
            f"## Not searched — chart shape unsupported ({len(unchecked)})",
            "",
            "These were never compared against any view, so nothing here says whether a",
            "matching view exists. The chart's shape has to be resolved first — then re-run",
            "the match to find out which of the sections above it belongs in.",
            "",
        ]
        for c in unchecked:
            lines.append(f"- **{c['chart_slug']}** — {c['note']}")
        lines.append("")

    (out / "mdim_suggestions.md").write_text("\n".join(lines))
    return len(near) + len(none) + len(unchecked)


def write_handoff(out: Path, charts: list[dict], selection: dict, cli_csv: Path) -> Path:
    """A note the person running the CLI can act on without having been in the session."""
    proposed = proposed_charts(charts)
    cli_only = [c for c in proposed if c["cli_required"]]
    # Their redirect row already exists, so the CLI leaves them out of the CSV entirely —
    # but the source chart can still be published, and a redirect over a published chart
    # never fires. Running the CLI does not resolve them; someone has to. Omitting them
    # would let the operator finish the run believing the migration was complete.
    done = [c for c in charts if c["already_done"]]
    # Matched, but blocked by a redirect that already points somewhere else. `proposed_charts`
    # drops them and they are not `already_done`, so without this they appear nowhere in the
    # handoff — and an operator who runs the CSV to completion would believe the migration was
    # done while these charts stay published with no redirect to the MDIM. Same reason the
    # already-redirected rows are listed: the CLI not touching a chart is not the chart
    # being finished.
    blocked = [c for c in charts if c["target"] is not None and c["conflict"] and not c["already_done"]]
    lines = [
        "# Chart → MDIM redirects: handoff",
        "",
        f"**{len(proposed)} redirects** proposed from `{selection['mode']}={selection['value']}`.",
        f"Input file: `{cli_csv.name}` (in this folder).",
        "",
        *(
            [
                f"{len(done)} further chart(s) are already redirected and are **not** in the CSV — "
                "they still need a manual check, see the end of this note.",
                "",
            ]  # fmt: skip
            if done
            else []
        ),
        *(
            [
                f"{len(blocked)} chart(s) matched a view but are **blocked** and are not in the CSV "
                "either — running the CLI leaves them published and unredirected. See the end of "
                "this note.",
                "",
            ]  # fmt: skip
            if blocked
            else []
        ),
        "## Run it",
        "",
        "From the **owid-grapher** repo, against **production**:",
        "",
        "```bash",
        f"yarn createMultiDimRedirectsFromCsv {cli_csv.resolve()} --dry-run   # rehearse",
        f"yarn createMultiDimRedirectsFromCsv {cli_csv.resolve()}             # for real",
        "```",
        "",
        "## What it does, and what to know before running",
        "",
        "- It creates each `multi_dim_redirects` row, migrates any old `chart_slug_redirects`",
        "  aliases onto the MDIM, **and unpublishes each source chart** — all in one",
        "  transaction. `--dry-run` rolls the whole thing back and skips the unpublishing.",
        "- **Unpublishing is required, not tidy-up**: a grapher redirect is only consulted",
        "  when the URL 404s, so a redirect over a still-published chart never fires.",
        "- **Don't hand-unpublish anything first.** Unpublishing deletes that chart's",
        "  `chart_slug_redirects` rows, so its old slugs would become hard 404s. The CLI's",
        "  ordering is what preserves them.",
        "- **One transaction, all-or-nothing**: a single rejected row aborts the migration.",
        "  `preflight.py` in this skill re-checks every row against the live DB first.",
        "- **Production only** — redirect tables do not sync staging → production, and the",
        "  CLI takes `GRAPHER_DB_*` from owid-grapher's `.env` with no environment guard.",
        "",
    ]
    if cli_only:
        n_alias = sum(len(c["old_slugs"]) for c in cli_only)
        lines += [
            f"## Why the CLI and not the admin ({len(cli_only)} of {len(proposed)})",
            "",
            f"These charts carry {n_alias} old slug(s) redirecting into them. The admin API",
            "rejects those as redirect chains; the CLI migrates them onto the MDIM instead.",
            "",
        ]
        for c in cli_only:
            lines.append(f"- `{c['chart_slug']}` — old slugs: {', '.join(c['old_slugs'])}")
        lines.append("")
    if done:
        with_aliases = [c for c in done if c["old_slugs"]]
        lines += [
            f"## Not in the CSV — handle these by hand ({len(done)})",
            "",
            "These charts already have the redirect row this run would have created, so the",
            "CLI skips them. That is **not** the same as being finished: the redirect only",
            "fires once the source chart stops being published. Check each one and unpublish",
            "it if it is still live.",
            "",
        ]
        for c in done:
            t = c["target"]
            alias = f" — **carries old slug(s)**: {', '.join(c['old_slugs'])}" if c["old_slugs"] else ""
            lines.append(
                f"- `{c['chart_slug']}` (chart {c['chart_id']}) → `{t['mdim_slug']}` view `{t['view_id']}`{alias}"
            )
        lines.append("")
        if with_aliases:
            lines += [
                f"**Do not simply unpublish the {len(with_aliases)} marked above.** Unpublishing a",
                "chart deletes its `chart_slug_redirects` rows, so those old slugs would become",
                "hard 404s — the same trap the CLI's ordering avoids for the proposed rows. Those",
                "aliases have to be migrated onto the MDIM first. `preflight.py` refuses these",
                "entries for exactly this reason.",
                "",
            ]
    if blocked:
        lines += [
            f"## Blocked — matched but not in the CSV ({len(blocked)})",
            "",
            "Each of these found a target view, but something already in the DB stops the row",
            "from being created. The CLI will not migrate them and running it does not resolve",
            "them: the chart stays published, with no redirect to the MDIM. Someone has to",
            "decide per row — resolve the blocker, or accept that the chart is not migrating.",
            "",
        ]
        for c in blocked:
            t = c["target"]
            lines.append(f"- `{c['chart_slug']}` (chart {c['chart_id']}) → `{t['mdim_slug']}` view `{t['view_id']}`")
            lines.append(f"    - blocked by: {c['conflict']}")
        lines += [
            "",
            "`unmatched.md` in this folder has the same rows alongside every other chart",
            "that did not produce a redirect.",
            "",
        ]
    lines += [
        "## After the run",
        "",
        "Stamp `cutover_date` in `migration_log_template.csv` and keep it. Analytics cannot",
        "reconstruct the mapping later: once a chart stops being published its whole view",
        "history resolves to `chart_id = NULL`, and `prod_semantic.redirects` carries no",
        "`multi_dim_redirects` rows.",
        "",
        "`references.md` lists what the redirect does *not* fix — embeds that render the",
        "chart's own config and need editing by hand.",
        "",
    ]
    path = out / "HANDOFF.md"
    path.write_text("\n".join(lines))
    return path


def write_unmatched_md(out: Path, charts: list[dict], id_to_path: dict[int, str], host: str) -> None:
    lines = ["# Charts without a proposed redirect", ""]
    sections = [
        ("ambiguous", "Ambiguous — several MDIM views match; pick one via overrides.csv or skip"),
        ("near_miss", "Near miss — indicator sets overlap but differ; never auto-proposed"),
        ("none", "No match — no MDIM view shares this chart's indicators"),
        ("skipped", "Skipped via overrides.csv"),
    ]
    for quality, heading in sections:
        rows = [c for c in charts if c["quality"] == quality]
        if not rows:
            continue
        lines += [f"## {heading} ({len(rows)})", ""]
        for c in rows:
            lines.append(
                f"- **{c['chart_slug']}** (id {c['chart_id']}) — [{c['title']}]({host}/grapher/{c['chart_slug']})"
            )
            for v in c["candidates"]:
                lines.append(
                    f"    - candidate: [{v['mdim_slug']}:{v['view_id']}]({v['url']}) (view_config_id `{v['view_config_id']}`)"
                )
            if c["near_misses"]:
                lines.append(f"    - {describe_near_miss(c, id_to_path)}")
            if c["note"]:
                lines.append(f"    - note: {c['note']}")
        lines.append("")
    conflicted = [c for c in charts if c["target"] is not None and c["conflict"]]
    if conflicted:
        lines += [f"## Conflicts — matched but blocked by existing redirects ({len(conflicted)})", ""]
        for c in conflicted:
            lines.append(
                f"- **{c['chart_slug']}** (id {c['chart_id']}) -> {c['target']['mdim_slug']}:{c['target']['view_id']}"
            )
            lines.append(f"    - {c['conflict']}")
        lines.append("")

    # This whole report is unfinished work, so it says which kind each row is rather than
    # leaving a reader to infer it: a pick someone can make now, versus a chart nobody has
    # judged yet. Mirrors the closing block of audit_references.py's references.md.
    by_quality = Counter(c["quality"] for c in charts)
    decidable = by_quality["ambiguous"] + len(conflicted)
    unjudged = by_quality["near_miss"] + by_quality["none"]
    lines += [
        "## What's still open",
        "",
        "**Handed off** — nothing. This report assigns no owners; every row above is still "
        "waiting on whoever runs the migration.",
        "",
        (
            f"**Proposed** — {decidable} chart(s) can be decided right now: {by_quality['ambiguous']} ambiguous "
            f"row(s) need one candidate picked in overrides.csv, and {len(conflicted)} matched row(s) are blocked "
            "by an existing redirect that has to be resolved or the chart skipped."
            if decidable
            else "**Proposed** — nothing. No chart is waiting on a pick or on an existing redirect being resolved."
        ),
        "",
        (
            f"**Unverified** — {unjudged} chart(s) nobody has judged: {by_quality['near_miss']} near miss(es) and "
            f"{by_quality['none']} with no match at all. Matching is by indicator set, so 'no match' means no "
            "published MDIM view carries the same indicators — not that no suitable replacement exists. "
            f"{by_quality['skipped']} chart(s) were skipped deliberately via overrides.csv and are not in scope."
            if unjudged
            else "**Unverified** — nothing. Every chart either matched or was skipped deliberately."
        ),
        "",
    ]
    (out / "unmatched.md").write_text("\n".join(lines))


def report(charts: list[dict], mdims: list[dict], views: list[dict], stats: dict, selection: dict) -> None:
    print()
    print(f"charts: {stats['charts']} ({selection['mode']}={selection['value']!r})")
    print(f"mdim target pool: {len(mdims)} published MDIMs, {len(views)} views")
    print(
        "matches: "
        + " | ".join(f"{q}: {stats[q]}" for q in QUALITIES)
        + f"  ->  proposed redirects: {stats['proposed']} (conflicts: {stats['conflicts']}, already done: {stats['already_done']})"
    )
    proposed = proposed_charts(charts)
    per_mdim = defaultdict(set)
    for c in proposed:
        per_mdim[c["target"]["mdim_slug"]].add(c["target"]["view_id"])
    for slug in sorted(per_mdim):
        n_charts = sum(1 for c in proposed if c["target"]["mdim_slug"] == slug)
        print(f"  {slug}: {n_charts} chart(s) -> {len(per_mdim[slug])} distinct view(s)")
    cli_required = [c for c in proposed if c["cli_required"]]
    if cli_required:
        n_aliases = sum(len(c["old_slugs"]) for c in cli_required)
        print(
            f"\nCLI-only rows: {len(cli_required)} chart(s) carry {n_aliases} old slug(s)/inbound redirect(s).\n"
            "  The CLI migrates these; the admin API rejects them as redirect chains. Never hand-unpublish\n"
            "  these charts first — unpublishing deletes their chart_slug_redirects rows and the old slugs 404."
        )
        for c in cli_required:
            for reason in c["cli_required"]:
                print(f"  {c['chart_slug']}: {reason}")
            for note in c["cli_notes"]:
                print(f"  {c['chart_slug']}: NOTE {note}")
    shared = [c for c in charts if c["shared_with"]]
    if shared:
        print(f"rows pointing at a shared MDIM view: {len(shared)} (see shared_target_chart_ids)")
    flags = [c for c in charts if c["quality"] == "ambiguous" or (c["target"] is not None and c["conflict"])]
    if flags:
        print("\nFLAGS (need a human decision — see unmatched.md):")
        for c in flags:
            what = c["conflict"] or f"ambiguous between {len(c['candidates'])} views"
            print(f"  chart {c['chart_id']} ({c['chart_slug']}): {what}")
    else:
        print("\nNo flags: every matched chart resolved cleanly.")


def main():
    ap = argparse.ArgumentParser(description="Match published charts to MDIM views and propose redirects.")
    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument("--tag", help="Exact tags.name, e.g. 'Economic Inequality'")
    sel.add_argument("--slugs", help="Comma-separated chart slugs, or @file with one slug per line")
    sel.add_argument("--dataset-id", type=int, help="Published charts using any variable of this dataset id")
    ap.add_argument("--mdim", action="append", default=[],
                    help="Restrict targets to these MDIM catalogPaths (repeatable; default: all published MDIMs)")  # fmt: skip
    ap.add_argument("--out", required=True, help="Output folder, e.g. ai/<name>-charts-mdim-mapping")
    ap.add_argument("--host", default=None,
                    help="Base URL for chart/view links (default: the site of the DB environment "
                         "OWID_ENV resolves to, e.g. staging DB -> staging links)")  # fmt: skip
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    check_db_connection()

    # Links must point at the environment the data came from — a mapping extracted from
    # staging with production URLs would be reviewed against the wrong charts.
    host = (args.host or OWID_ENV.site or PUBLIC_HOST).rstrip("/")
    print(f"link host: {host}" + ("" if args.host else "  (from the DB environment; override with --host)"))

    charts, selection = resolve_charts(args)
    attach_chart_slots(charts)
    print(f"charts: {len(charts)} published, with slugs")

    mdims, views, id_to_path = get_mdims_and_views(args.mdim, host)
    print(f"mdims: {len(mdims)} published ({len(views)} views)")

    # Chart-side variable ids also need catalogPaths, for readable near-miss diffs.
    chart_var_ids = sorted(
        {vid for c in charts for vid in (*c["y"], c["x"], c["size"], c["color"]) if vid} - set(id_to_path)
    )
    if chart_var_ids:
        df = OWID_ENV.read_sql(
            "SELECT id, catalogPath FROM variables WHERE id IN %(ids)s", params={"ids": tuple(chart_var_ids)}
        )
        id_to_path.update({int(i): cp for i, cp in zip(df["id"], df["catalogPath"]) if cp})

    match_charts(charts, views, id_to_path)
    apply_overrides(charts, views, out)
    check_conflicts(charts, mdims)
    fill_shared_targets(charts)

    write_charts_csv(out, charts, host)
    write_views_csv(out, views)
    write_proposal_csv(out, charts, id_to_path, host)
    stats = write_mapping_json(out, charts, mdims, selection)
    n_payloads = write_payloads(out, charts)
    cli_csv, n_cli = write_cli_csv(out, charts)
    log_template = write_migration_log_template(out, charts)
    write_unmatched_md(out, charts, id_to_path, host)
    n_suggestions = write_mdim_suggestions(out, charts, id_to_path, host)
    handoff = write_handoff(out, charts, selection, cli_csv)
    (out / "_sources.json").write_text(json.dumps({
        "selection": selection,
        "host": host,
        "mdim_restrict": args.mdim,
        "mdims": [{"id": m["id"], "catalogPath": m["catalog_path"], "slug": m["slug"], "prefix": m["prefix"]} for m in mdims],
    }, indent=2) + "\n")  # fmt: skip

    report(charts, mdims, views, stats, selection)
    print(f"\n-> {out}/mapping_proposal.csv  (review this; then build_review.py for the side-by-side HTML)")
    print(f"-> {out}/mapping.json  (combined machine record for preflight.py)")
    print(f"-> {out}/payloads/*.json  ({n_payloads} files, ONE source chart per JSON — the copy-paste handoff unit)")
    print(f"-> {cli_csv}  ({n_cli} rows, the apply input for `yarn createMultiDimRedirectsFromCsv`)")
    print(f"-> {log_template}  (stamp cutover_date when the CLI runs — analytics can't recover it later)")
    print(f"-> {out}/mdim_suggestions.md  ({n_suggestions} unmatched chart(s): what the MDIMs would need)")
    print(f"-> {handoff}  (give this to whoever runs the CLI)")
    print("\nNext: preflight.py to validate before the CLI runs (one bad row aborts its whole transaction).")


if __name__ == "__main__":
    main()
