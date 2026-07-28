"""Extract published charts + MDIM views from the grapher DB, match charts to
MDIM views by indicator IDs, and write a redirect proposal.

Everything here is READ-ONLY: it queries the grapher DB via ``OWID_ENV``
(``STAGING=1`` for the current branch's staging DB, or against production with
``ENV_FILE=<prod creds file> DATA_API_ENV=production``) and writes artifacts
into ``--out``. The resolved environment is printed at startup — check it: a
reachable local dev DB also passes the preflight, and extracting from the wrong
environment produces a mapping that silently points at the wrong charts. No
redirect is created — that's ``apply_redirects.py``, gated on explicit user
confirmation.

Charts are selected by exactly one of ``--tag`` / ``--slugs`` / ``--dataset-id``
and matched against the views of every published MDIM (or only those passed via
repeatable ``--mdim``). A chart matches a view when their indicator IDs agree on
every slot: same set of y variables, same x/size/color (absent == absent). Ties
between several matching views are broken by chart type; anything still
ambiguous is reported, not proposed.

Outputs into ``--out``:
- ``charts.csv``            — the selected source charts and their indicator slots
- ``multidim_views.csv``    — every candidate MDIM view (id A1.., B1.., ...)
- ``mapping_proposal.csv``  — one row per chart with match quality + target view
- ``mapping.json``          — redirect payload (confident, conflict-free matches only)
- ``unmatched.md``          — human-readable report of everything not proposed
- ``_sources.json``         — machine record of the run inputs (don't hand-edit)

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
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlencode

from etl.config import OWID_ENV

PUBLIC_HOST = "https://ourworldindata.org"
EXTRA_SLOTS = ("x", "size", "color")
QUALITIES = ("exact", "forced", "ambiguous", "near_miss", "none", "skipped")

PROPOSAL_COLUMNS = [
    "chart_id", "chart_slug", "chart_title", "chart_type", "chart_url", "y_variable_ids",
    "match_quality", "tiebreak", "target_mdim_catalog_path", "target_mdim_slug",
    "target_view_id", "target_view_config_id", "target_url", "target_view_title",
    "target_chart_type", "n_candidates", "candidate_view_ids", "near_miss_detail",
    "shared_target_chart_ids", "conflict", "note",
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


def resolve_charts(args) -> list[dict]:
    """Return the selected published charts as dicts, per the chosen selection mode."""
    if args.tag:
        df = OWID_ENV.read_sql(
            CHART_SELECT
            + """
            JOIN chart_tags ct ON ct.chartId = c.id
            JOIN tags t ON t.id = ct.tagId
            WHERE c.publishedAt IS NOT NULL AND t.name = %(tag)s
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
            CHART_SELECT + "WHERE c.publishedAt IS NOT NULL AND cc.slug IN %(slugs)s",
            params={"slugs": tuple(slugs)},
        )
        missing = sorted(set(slugs) - set(df["chart_slug"]))
        if missing:
            print(f"warning: {len(missing)} slug(s) not found as published charts: {missing}")
        selection = {"mode": "slugs", "value": slugs}
    else:
        df = OWID_ENV.read_sql(
            CHART_SELECT
            + """
            WHERE c.publishedAt IS NOT NULL AND c.id IN (
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
    slots: dict[int, dict] = defaultdict(lambda: {"y": [], "x": None, "size": None, "color": None})
    for r in df.to_dict("records"):
        s = slots[r["chartId"]]
        if r["property"] == "y":
            s["y"].append(int(r["variableId"]))
        elif r["property"] in EXTRA_SLOTS:
            if s[r["property"]] is not None:
                print(f"warning: chart {r['chartId']} has multiple '{r['property']}' dimensions — keeping the first")
            else:
                s[r["property"]] = int(r["variableId"])
    for c in charts:
        s = slots[c["chart_id"]]
        if len(s["y"]) != len(set(s["y"])):
            print(f"warning: chart {c['chart_id']} ({c['chart_slug']}) repeats a y variable — deduplicated")
        c["y"] = frozenset(s["y"])
        for slot in EXTRA_SLOTS:
            c[slot] = s[slot]
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
        v["y"] = frozenset(vid for vid, _ in v["_raw_y"] if vid is not None)
        for slot in EXTRA_SLOTS:
            entries = [vid for vid, _ in v[f"_raw_{slot}"] if vid is not None]
            if len(entries) > 1:
                print(
                    f"warning: view {v['row_id']} ({v['mdim_slug']}) has multiple '{slot}' indicators — keeping the first"
                )
            v[slot] = entries[0] if entries else None
        if not v["view_config_id"]:
            print(
                f"warning: view {v['row_id']} ({v['mdim_slug']}?{v['query_str']}) has no fullConfigId — excluded from matching"
            )
            continue
        usable.append(v)

    attach_view_chart_types(usable, [m["id"] for m in mdims])
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


def attach_view_chart_types(views: list[dict], mdim_ids: list[int]) -> None:
    df = OWID_ENV.read_sql(
        "SELECT mx.chartConfigId AS cc_id, cc.chartType AS chart_type "
        "FROM multi_dim_x_chart_configs mx JOIN chart_configs cc ON cc.id = mx.chartConfigId "
        "WHERE mx.multiDimId IN %(ids)s",
        params={"ids": tuple(mdim_ids)},
    )
    by_cc = dict(zip(df["cc_id"], df["chart_type"]))
    for v in views:
        v["chart_type"] = by_cc.get(v["view_config_id"])


# ----- Matching -------------------------------------------------------------------


def match_charts(charts: list[dict], views: list[dict], id_to_path: dict[int, str]) -> None:
    """Set match_quality / target / candidates / near-miss info on each chart dict."""
    by_key: dict[tuple, list[dict]] = defaultdict(list)
    for v in views:
        by_key[(v["y"], v["x"], v["size"], v["color"])].append(v)

    for c in charts:
        c.update({"quality": "none", "tiebreak": "", "target": None, "candidates": [], "near_misses": [], "note": ""})
        if not c["y"]:
            c["note"] = "chart has no y indicators"
            continue
        candidates = by_key.get((c["y"], c["x"], c["size"], c["color"]), [])
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
            near = [v for v in views if all(v[s] == c[s] for s in EXTRA_SLOTS) and (c["y"] < v["y"] or v["y"] < c["y"])]
            near.sort(key=lambda v: (-len(v["y"] & c["y"]), len(v["y"] ^ c["y"])))
            if near:
                c["quality"], c["near_misses"] = "near_miss", near[:3]


def describe_near_miss(c: dict, id_to_path: dict[int, str]) -> str:
    parts = []
    for v in c["near_misses"]:
        extra = sorted(v["y"] - c["y"]) or sorted(c["y"] - v["y"])
        side = "view extra" if v["y"] > c["y"] else "chart extra"
        ids = ", ".join(f"{i} ({path_tail(id_to_path.get(i))})" for i in extra)
        parts.append(f"{v['mdim_slug']}:{v['view_id']} [{side}: {ids}]")
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
    """Replicate grapher's validateMultiDimRedirect chain checks, read-only, at proposal time.

    Sets c["conflict"] (reason string) or c["already_done"]=True on matched charts.
    """
    matched = [c for c in charts if c["target"] is not None]
    for c in charts:
        c["conflict"], c["already_done"] = "", False
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

    incoming = OWID_ENV.read_sql(
        "SELECT csr.slug AS old_slug, cc.slug AS chart_slug FROM chart_slug_redirects csr "
        "JOIN charts c ON c.id = csr.chart_id JOIN chart_configs cc ON cc.id = c.configId "
        "WHERE cc.slug IN %(s)s",
        params={"s": slugs},
    )
    incoming_by_slug = defaultdict(list)
    for r in incoming.to_dict("records"):
        incoming_by_slug[r["chart_slug"]].append(r["old_slug"])

    redirected_mdim_slugs = set(
        OWID_ENV.read_sql(
            "SELECT DISTINCT mdp.slug FROM multi_dim_redirects mdr "
            "JOIN multi_dim_data_pages mdp ON mdp.id = mdr.multiDimId WHERE mdp.slug IS NOT NULL"
        )["slug"]
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
        prior = mdr_by_source.get(source)
        if prior is not None:
            if int(prior["multiDimId"]) == t["mdim_id"] and prior["viewConfigId"] == t["view_config_id"]:
                c["already_done"] = True
                continue
            reasons.append(f"already redirected to a DIFFERENT mdim view (multiDimId={prior['multiDimId']})")
        if source in site_sources:
            reasons.append(f"already a site redirect source -> {site_sources[source]}")
        if source in site_targets:
            reasons.append(f"chain: site redirect(s) point AT this chart: {site_targets[source]}")
        if slug in incoming_by_slug:
            reasons.append(f"chain: incoming chart_slug_redirects: {sorted(incoming_by_slug[slug])}")
        if slug in own_old_slugs:
            reasons.append("chart slug is itself an old slug in chart_slug_redirects")
        if slug in redirected_mdim_slugs:
            reasons.append("chart slug equals an MDIM slug that is already a redirect target")
        if slug == t["mdim_slug"]:
            reasons.append("self-redirect: chart slug equals the target MDIM slug")
        if f"/grapher/{t['mdim_slug']}" in bad_targets:
            reasons.append(f"target /grapher/{t['mdim_slug']} is itself a redirect source")
        c["conflict"] = "; ".join(reasons)


# ----- Outputs ----------------------------------------------------------------------


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
                "y_variable_ids": "|".join(str(i) for i in sorted(c["y"])),
                "match_quality": c["quality"], "tiebreak": c["tiebreak"],
                "target_mdim_catalog_path": t["mdim_catalog_path"] if t else "",
                "target_mdim_slug": t["mdim_slug"] if t else "",
                "target_view_id": t["view_id"] if t else "",
                "target_view_config_id": t["view_config_id"] if t else "",
                "target_url": t["url"] if t else "",
                "target_view_title": t["title"] if t else "",
                "target_chart_type": t["chart_type"] if t else "",
                "n_candidates": len(c["candidates"]),
                "candidate_view_ids": " | ".join(f"{v['mdim_slug']}:{v['view_id']}" for v in c["candidates"]),
                "near_miss_detail": describe_near_miss(c, id_to_path),
                "shared_target_chart_ids": c["shared_with"],
                "conflict": "already redirected (same target) — nothing to do" if c["already_done"] else c["conflict"],
                "note": c["note"],
            })  # fmt: skip


def chart_json(c: dict) -> dict:
    # configMd5 lets apply_redirects.py detect charts edited after the proposal was written.
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
            "dimensions": t["dims"],
            "queryStr": t["query_str"],
            "url": t["url"],
        },
        "matchQuality": c["quality"],
    }
    if c["shared_with"]:
        entry["sharedTargetChartIds"] = [int(i) for i in c["shared_with"].split(",")]
    return entry


def write_payloads(out: Path, charts: list[dict]) -> int:
    """One JSON per source chart under <out>/payloads/ — the copy-paste handoff unit.

    Mirrors the explorer→MDIM redirect deliverable convention: each payload file
    describes exactly ONE source page and its redirect. The combined mapping.json
    stays the machine record for apply_redirects.py.
    """
    payload_dir = out / "payloads"
    payload_dir.mkdir(exist_ok=True)
    for stale in payload_dir.glob("*.json"):
        stale.unlink()
    proposed = [c for c in charts if c["target"] is not None and not c["conflict"] and not c["already_done"]]
    for c in proposed:
        (payload_dir / f"{c['chart_slug']}.json").write_text(json.dumps(redirect_json(c), indent=2) + "\n")
    return len(proposed)


def write_mapping_json(out: Path, charts: list[dict], mdims: list[dict], selection: dict) -> dict:
    proposed = [c for c in charts if c["target"] is not None and not c["conflict"] and not c["already_done"]]
    conflicted = [c for c in charts if c["target"] is not None and c["conflict"]]
    done = [c for c in charts if c["already_done"]]
    unmatched = [c for c in charts if c["target"] is None and c["quality"] != "skipped"]
    stats = {"charts": len(charts), **{q: sum(c["quality"] == q for c in charts) for q in QUALITIES},
             "conflicts": len(conflicted), "already_done": len(done), "proposed": len(proposed)}  # fmt: skip
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
    per_mdim = defaultdict(set)
    for c in charts:
        if c["target"] is not None and not c["conflict"] and not c["already_done"]:
            per_mdim[c["target"]["mdim_slug"]].add(c["target"]["view_id"])
    for slug in sorted(per_mdim):
        n_charts = sum(
            1 for c in charts
            if c["target"] is not None and not c["conflict"] and not c["already_done"] and c["target"]["mdim_slug"] == slug
        )  # fmt: skip
        print(f"  {slug}: {n_charts} chart(s) -> {len(per_mdim[slug])} distinct view(s)")
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
    write_unmatched_md(out, charts, id_to_path, host)
    (out / "_sources.json").write_text(json.dumps({
        "selection": selection,
        "host": host,
        "mdim_restrict": args.mdim,
        "mdims": [{"id": m["id"], "catalogPath": m["catalog_path"], "slug": m["slug"], "prefix": m["prefix"]} for m in mdims],
    }, indent=2) + "\n")  # fmt: skip

    report(charts, mdims, views, stats, selection)
    print(f"\n-> {out}/mapping_proposal.csv  (review this; then build_review.py for the side-by-side HTML)")
    print(f"-> {out}/mapping.json  (combined machine record for apply_redirects.py — gated, ask the user first)")
    print(f"-> {out}/payloads/*.json  ({n_payloads} files, ONE source chart per JSON — the copy-paste handoff unit)")


if __name__ == "__main__":
    main()
