"""Validate a chart → MDIM redirect proposal before the grapher CLI runs it.

READ-ONLY. This script never creates a redirect and never unpublishes a chart.
Applying is done in owid-grapher:

    yarn createMultiDimRedirectsFromCsv /abs/path/redirects_for_cli.csv --dry-run

Why a preflight exists: that CLI runs **one transaction**, and any row it rejects
aborts the entire migration. So every check here mirrors a check the CLI performs,
against the live DB, so the bad rows surface before the run instead of during it.

It also catches things the CLI cannot know about:
- charts edited (or deleted) since the proposal was written — `configMd5` drift,
  which usually means the target view no longer matches what a human reviewed;
- MDIMs deleted, renamed, or rebuilt since the proposal, so the reviewed view no
  longer exists or no longer sits at the recorded URL;
- charts the reviewer flagged in the review HTML (`--decisions`);
- charts still embedded elsewhere. Those never abort the CLI — they break
  *silently* when it unpublishes the source chart — so they gate readiness here.

Usage:
    ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
        .claude/skills/map-charts-to-mdim/scripts/preflight.py \
        --mapping ai/<name>-charts-mdim-mapping \
        [--decisions ai/<name>_chart_mdim_review.json]
"""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from etl.config import OWID_ENV


def load_mapping(path_arg: str) -> tuple[dict, Path]:
    path = Path(path_arg)
    mapping_dir = path if path.is_dir() else path.parent
    if path.is_dir():
        path = path / "mapping.json"
    if not path.exists():
        raise SystemExit(f"Not found: {path}. Run extract_and_match.py first.")
    return json.loads(path.read_text()), mapping_dir


def load_decisions(path_arg: str) -> dict[int, dict]:
    """Load the review export from build_review.py (the ⬇ JSON / ⬇ CSV buttons), keyed by chart id."""
    path = Path(path_arg)
    if not path.exists():
        raise SystemExit(f"Decisions file not found: {path}")
    if path.suffix.lower() == ".csv":
        with open(path, newline="") as f:
            rows = list(csv.DictReader(f))
    else:
        rows = json.loads(path.read_text())
    return {
        int(r["id"]): {
            "status": (r.get("status") or "").strip(),
            "note": (r.get("note") or "").strip(),
            # the target the decision was made on, used to detect stale decisions
            "target_mdim": (r.get("target_mdim") or "").strip(),
            "view_id": (r.get("view_id") or "").strip(),
        }
        for r in rows
    }


def apply_decisions(
    entries: list[dict], decisions: dict[int, dict]
) -> tuple[list[dict], list[tuple[dict, str]], list[dict], int]:
    """Drop entries whose chart the reviewer flagged; count kept entries that carry no decision.

    A decision is bound to the proposal it was made on: if the export carries the reviewed
    target (target_mdim/view_id) and it differs from the entry's current target, the decision
    is stale and treated as no decision at all.
    """
    kept, flagged, stale, undecided = [], [], [], 0
    for e in entries:
        d = decisions.get(e["chart"]["id"], {})
        status = d.get("status", "")
        reviewed_target = (d.get("target_mdim", ""), d.get("view_id", ""))
        if status and any(reviewed_target) and reviewed_target != (e["target"]["mdimSlug"], e["target"]["viewId"]):
            stale.append(e)
            status = ""
        if status == "flagged":
            flagged.append((e, d.get("note", "")))
        else:
            kept.append(e)
            if status != "approved":
                undecided += 1
    return kept, flagged, stale, undecided


def stale_charts(entries: list[dict]) -> dict[str, str]:
    """Source charts whose slug or config changed since the proposal was written."""
    ids = tuple(e["chart"]["id"] for e in entries)
    if not ids:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT c.id, cc.slug, cc.fullMd5 AS config_md5 FROM charts c "
        "JOIN chart_configs cc ON cc.id = c.configId WHERE c.id IN %(ids)s",
        params={"ids": ids},
    )
    current = {int(r["id"]): r for r in df.to_dict("records")}
    stale = {}
    for e in entries:
        ch = e["chart"]
        cur = current.get(ch["id"])
        if cur is None:
            stale[e["source"]] = "chart no longer exists — re-run extract_and_match.py"
        elif cur["slug"] != ch["slug"]:
            stale[e["source"]] = f"chart slug changed since the proposal (now '{cur['slug']}') — re-run extract_and_match.py"  # fmt: skip
        elif ch.get("configMd5") and cur["config_md5"] != ch["configMd5"]:
            stale[e["source"]] = "chart config changed since the proposal — re-run extract_and_match.py"
    return stale


def stale_targets(entries: list[dict]) -> dict[str, str]:
    """Targets whose MDIM or reviewed view changed since the proposal was written.

    The mirror of `stale_charts` on the target side. A deleted or renamed MDIM, or a
    view whose config was regenerated, leaves the CSV pointing at a URL nobody
    reviewed — and `cli_blockers` would not notice, because it only looks up MDIMs
    that still exist under the recorded slug.
    """
    ids = tuple({e["target"]["multiDimId"] for e in entries})
    if not ids:
        return {}
    # No ORDER BY: these configs are multi-MB JSON and sorting them server-side blows
    # the MySQL sort buffer (error 1038).
    df = OWID_ENV.read_sql("SELECT id, slug, config FROM multi_dim_data_pages WHERE id IN %(ids)s", params={"ids": ids})
    current: dict[int, dict] = {}
    for row in df.to_dict("records"):
        cfg = row["config"]
        cfg = json.loads(cfg) if isinstance(cfg, str) else (cfg or {})
        views = {
            v.get("fullConfigId"): "__".join(f"{k}={val}" for k, val in sorted((v.get("dimensions") or {}).items()))
            for v in cfg.get("views", [])
        }
        current[int(row["id"])] = {"slug": row["slug"], "views": views}

    rerun = " — re-run extract_and_match.py and re-review"
    stale: dict[str, str] = {}
    for e in entries:
        t = e["target"]
        cur = current.get(t["multiDimId"])
        if cur is None:
            stale[e["source"]] = f"target MDIM (multiDimId={t['multiDimId']}) no longer exists{rerun}"
        elif cur["slug"] != t["mdimSlug"]:
            stale[e["source"]] = f"target MDIM slug changed since the proposal (now '{cur['slug']}'){rerun}"
        elif t["viewConfigId"] not in cur["views"]:
            stale[e["source"]] = f"the reviewed target view is gone from the MDIM{rerun}"
        elif cur["views"][t["viewConfigId"]] != t["viewId"]:
            stale[e["source"]] = (
                f"the reviewed view config now belongs to a different view "
                f"(now '{cur['views'][t['viewConfigId']]}'){rerun}"
            )
    return stale


def cli_blockers(redirects: list[dict]) -> dict[str, list[str]]:
    """Re-run the CLI's validatePathIsNotRedirectSource checks against the live DB.

    Mirrors owid-grapher devTools/createMultiDimRedirectsFromCsv.ts:168-201, which runs on
    the source, on the target base path, and again on every old slug the CLI re-creates as
    a multi_dim_redirects source. Anything here aborts the CLI's whole transaction.
    """
    reasons: dict[str, list[str]] = defaultdict(list)
    if not redirects:
        return reasons
    sources = tuple(r["source"] for r in redirects)
    slugs = tuple(r["chart"]["slug"] for r in redirects)

    site_sources = dict(
        OWID_ENV.read_sql(
            "SELECT source, target FROM redirects WHERE source IN %(s)s", params={"s": sources}
        ).itertuples(index=False, name=None)
    )
    own_old_slugs = set(
        OWID_ENV.read_sql("SELECT slug FROM chart_slug_redirects WHERE slug IN %(s)s", params={"s": slugs})["slug"]
    )

    # Old slugs the CLI will re-create as multi_dim_redirects sources must be free too.
    old_sources = tuple(f"/grapher/{s}" for r in redirects for s in r.get("oldSlugs", []))
    taken_old: set[str] = set()
    if old_sources:
        taken_old = set(
            OWID_ENV.read_sql(
                "SELECT source FROM redirects WHERE source IN %(s)s "
                "UNION SELECT source FROM multi_dim_redirects WHERE source IN %(s)s",
                params={"s": old_sources},
            )["source"]
        )

    # Target side, once per targeted MDIM.
    targeted_slugs = tuple({r["target"]["mdimSlug"] for r in redirects})
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

    # The CLI refuses to point at an unpublished MDIM (loadMultiDims throws for the whole run).
    unpublished = set(
        OWID_ENV.read_sql(
            "SELECT slug FROM multi_dim_data_pages WHERE slug IN %(s)s AND published = 0",
            params={"s": targeted_slugs},
        )["slug"]
    )

    for r in redirects:
        source, slug, t = r["source"], r["chart"]["slug"], r["target"]
        if source in site_sources:
            reasons[source].append(f"already a site redirect source -> {site_sources[source]}")
        if slug in own_old_slugs:
            reasons[source].append("chart slug is itself an old slug in chart_slug_redirects")
        if slug == t["mdimSlug"]:
            reasons[source].append("self-redirect: chart slug equals the target MDIM slug")
        if f"/grapher/{t['mdimSlug']}" in bad_targets:
            reasons[source].append(f"target /grapher/{t['mdimSlug']} is itself a redirect source")
        if t["mdimSlug"] in unpublished:
            reasons[source].append(f"target MDIM /grapher/{t['mdimSlug']} is not published")
        clashing = [s for s in r.get("oldSlugs", []) if f"/grapher/{s}" in taken_old]
        if clashing:
            reasons[source].append(f"old slug(s) already a redirect source elsewhere: {clashing}")
    return reasons


def existing_mdim_redirects(sources: tuple[str, ...]) -> dict[str, dict]:
    if not sources:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT source, multiDimId, viewConfigId FROM multi_dim_redirects WHERE source IN %(s)s",
        params={"s": sources},
    )
    return {r["source"]: r for r in df.to_dict("records")}


def validate_cli_csv(path: Path, redirects: list[dict]) -> list[str]:
    """Re-parse the CSV under the CLI's own rules and check it matches the vetted set."""
    problems = []
    if not path.exists():
        return [f"{path} is missing — re-run extract_and_match.py"]
    seen: list[str] = []
    for i, line in enumerate(path.read_text().split("\n")):
        if not line.strip():
            continue
        fields = line.split(";")
        if len(fields) != 2:
            problems.append(f"line {i + 1}: expected 2 `;`-separated fields, got {len(fields)}")
            continue
        source, target = (f.strip() for f in fields)
        if not source.startswith("/") or not target.startswith("/"):
            if i != 0:  # a header is tolerated on line 1 only
                problems.append(f"line {i + 1}: both fields must start with '/'")
            continue
        if source in seen:
            problems.append(f"line {i + 1}: duplicate source {source!r}")
        if "?" in source:
            problems.append(f"line {i + 1}: source must not carry a query string")
        if not target.startswith("/grapher/"):
            problems.append(f"line {i + 1}: target must be a /grapher/ path")
        seen.append(source)
    dropped = {r["source"] for r in redirects} - set(seen)
    extra = set(seen) - {r["source"] for r in redirects}
    if dropped:
        problems.append(f"in mapping.json but not in the CSV: {sorted(dropped)}")
    if extra:
        problems.append(f"in the CSV but not vetted here: {sorted(extra)} — re-run extract_and_match.py")
    return problems


def embed_references(redirects: list[dict]) -> dict[int, str]:
    """One line per chart: the surfaces that embed it and so break when it is unpublished.

    A redirect only rescues hyperlinks. Everything counted here holds the chart by id or
    slug and renders its own config, so unpublishing the source breaks it with no error
    anywhere — which is why these gate readiness instead of merely being reported. That
    holds whether the CLI unpublishes the chart or a human does (the `already_done` rows),
    so pass both sets. Old slugs are included: references written before a rename point
    at those.

    Pure SQL, so this works with read-only credentials (the admin references API needs
    ADMIN_API_KEY). Counts only — audit_references.py does the full sweep with
    replacement URLs.
    """
    ids = tuple(r["chart"]["id"] for r in redirects)
    if not ids:
        return {}
    by_slug = {r["chart"]["slug"]: r["chart"]["id"] for r in redirects}
    for r in redirects:
        for old in r.get("oldSlugs", []):
            by_slug[old] = r["chart"]["id"]
    slugs = tuple(by_slug)
    counts: dict[int, dict[str, int]] = defaultdict(dict)

    def add_by_id(surface: str, df) -> None:
        for row in df.to_dict("records"):
            counts[int(row["chart_id"])][surface] = counts[int(row["chart_id"])].get(surface, 0) + int(row["n"])

    def add_by_slug(surface: str, df) -> None:
        for row in df.to_dict("records"):
            cid = by_slug[row["slug"]]
            counts[cid][surface] = counts[cid].get(surface, 0) + int(row["n"])

    add_by_id(
        "explorers",
        OWID_ENV.read_sql(
            "SELECT ec.chartId AS chart_id, COUNT(DISTINCT ec.explorerSlug) AS n FROM explorer_charts ec "
            "JOIN explorers e ON e.slug = ec.explorerSlug "
            "WHERE ec.chartId IN %(ids)s AND e.isPublished = 1 GROUP BY ec.chartId",
            params={"ids": ids},
        ),
    )
    add_by_id(
        "narrativeCharts",
        OWID_ENV.read_sql(
            "SELECT parentChartId AS chart_id, COUNT(*) AS n FROM narrative_charts "
            "WHERE parentChartId IN %(ids)s GROUP BY parentChartId",
            params={"ids": ids},
        ),
    )
    add_by_slug(
        "staticViz",
        OWID_ENV.read_sql(
            "SELECT grapherSlug AS slug, COUNT(*) AS n FROM static_viz "
            "WHERE grapherSlug IN %(slugs)s GROUP BY grapherSlug",
            params={"slugs": slugs},
        ),
    )
    # Block-level components render the chart; `span-*` is a hyperlink in prose, which the
    # 301 covers. LEFT() rather than LIKE: a literal '%' in the SQL string would collide
    # with pymysql's own parameter formatting.
    add_by_slug(
        "articleEmbeds",
        OWID_ENV.read_sql(
            "SELECT pgl.target AS slug, COUNT(*) AS n FROM posts_gdocs_links pgl "
            "JOIN posts_gdocs pg ON pg.id = pgl.sourceId "
            "WHERE pgl.target IN %(slugs)s AND pgl.linkType IN ('grapher', 'guided-chart') "
            "AND pg.published = 1 AND (pgl.componentType IS NULL OR LEFT(pgl.componentType, 5) <> 'span-') "
            "GROUP BY pgl.target",
            params={"slugs": slugs},
        ),
    )
    # Data insights carry the chart in content->>'$."grapher-url"', not in posts_gdocs_links.
    # The derived slug can't be grouped in the same SELECT under only_full_group_by.
    add_by_slug(
        "dataInsights",
        OWID_ENV.read_sql(
            "SELECT slug, COUNT(*) AS n FROM ("
            "  SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(pg.content->>'$.\"grapher-url\"', '/grapher/', -1), '?', 1) AS slug"
            "  FROM posts_gdocs pg WHERE pg.type = 'data-insight' AND pg.published = 1"
            "    AND pg.content->>'$.\"grapher-url\"' IS NOT NULL"
            ") t WHERE slug IN %(slugs)s GROUP BY slug",
            params={"slugs": slugs},
        ),
    )

    return {cid: ", ".join(f"{k} ({n})" for k, n in sorted(v.items())) for cid, v in counts.items() if v}


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate a chart → MDIM redirect proposal before the grapher CLI runs.")
    ap.add_argument("--mapping", required=True, help="mapping.json path, or the folder containing it")
    ap.add_argument("--decisions",
                    help="review export from build_review.py (⬇ JSON / ⬇ CSV) — charts the reviewer flagged are excluded")  # fmt: skip
    ap.add_argument("--no-references", action="store_true",
                    help="skip the embedded-reference gate (only once those references have been migrated)")  # fmt: skip
    args = ap.parse_args()

    mapping, mapping_dir = load_mapping(args.mapping)
    redirects = mapping.get("redirects", [])
    # Redirects that already existed at proposal time: no redirect to create, but their source
    # charts are still published (the extractor only selects published charts), so they still
    # shadow their redirect and still need to go through the CLI to be unpublished.
    already_done = mapping.get("already_done", [])

    if args.decisions:
        decisions = load_decisions(args.decisions)
        redirects, flagged_r, stale_r, undecided = apply_decisions(redirects, decisions)
        already_done, flagged_d, stale_d, _ = apply_decisions(already_done, decisions)
        flagged = flagged_r + flagged_d
        print(f"Review decisions ({args.decisions}): {len(flagged)} flagged (excluded), "
              f"{undecided} proposed redirect(s) without a decision (kept).")  # fmt: skip
        for e, note in flagged:
            print(f"  FLAGGED  {e['source']}{'  — ' + note if note else ''}")
        for e in stale_r + stale_d:
            print(f"  STALE DECISION (made on a different target — treated as undecided)  {e['source']}")
        if flagged:
            print("  NOTE: flagged rows are still in redirects_for_cli.csv — remove them there, or mark them\n"
                  "        SKIP in overrides.csv and re-run extract_and_match.py, before running the CLI.")  # fmt: skip
    else:
        print("note: no --decisions file — flags made in the review HTML are NOT consumed. Export them "
              "(⬇ JSON) and pass --decisions, or fold them into overrides.csv and re-run extract_and_match.py.")  # fmt: skip

    if not redirects and not already_done:
        print("mapping.json has no proposed redirects — nothing to do.")
        return 0

    print(f"\nEnvironment: {OWID_ENV.site or '(unknown site)'}   READ-ONLY preflight — nothing is created here.")
    print(f"Proposed redirects: {len(redirects)}   already redirected at proposal time: {len(already_done)}")

    sources = tuple(r["source"] for r in redirects + already_done)
    existing = existing_mdim_redirects(sources)
    blockers = cli_blockers(redirects)
    stale = stale_charts(redirects + already_done)
    for source, reason in stale_targets(redirects + already_done).items():
        stale.setdefault(source, reason)

    rows = []
    for r in redirects:
        source, t = r["source"], r["target"]
        if source in stale:
            rows.append((source, t["url"], "STALE", stale[source]))
            continue
        prior = existing.get(source)
        if prior is not None:
            if int(prior["multiDimId"]) == t["multiDimId"] and prior["viewConfigId"] == t["viewConfigId"]:
                # The CLI would still abort: source is already a multi_dim_redirects source.
                rows.append((source, t["url"], "EXISTS", "already redirected to this target — drop this row from the CSV"))  # fmt: skip
            else:
                rows.append((source, t["url"], "DIFFERS", f"already redirected to multiDimId={prior['multiDimId']} "
                                                          f"viewConfigId={prior['viewConfigId']}"))  # fmt: skip
        elif source in blockers:
            rows.append((source, t["url"], "BLOCKER", "; ".join(blockers[source])))
        else:
            rows.append((source, t["url"], "OK", "CLI required" if r.get("oldSlugs") else ""))

    # Re-verify the proposal-time redirects still exist and still point at the proposed target.
    for r in already_done:
        source, t = r["source"], r["target"]
        if source in stale:
            rows.append((source, t["url"], "STALE", stale[source]))
            continue
        prior = existing.get(source)
        if prior is not None and int(prior["multiDimId"]) == t["multiDimId"] and prior["viewConfigId"] == t["viewConfigId"]:  # fmt: skip
            # The CLI cannot finish these: their source is already a multi_dim_redirects
            # source, which its own validation rejects, so the row would abort the run.
            if r.get("oldSlugs"):
                rows.append((source, t["url"], "MANUAL", f"redirect in place, chart still published — but it carries "
                                                         f"old slug(s) {r['oldSlugs']} that a hand-unpublish would turn "
                                                         f"into hard 404s; ask the Grapher team to migrate them"))  # fmt: skip
            else:
                rows.append((source, t["url"], "MANUAL", "redirect in place but the chart is still published, so it "
                                                         "never fires — unpublish the chart in the grapher admin "
                                                         "(the CLI rejects this row: its source is already a redirect source)"))  # fmt: skip
        elif prior is not None:
            rows.append((source, t["url"], "DIFFERS", f"already redirected to multiDimId={prior['multiDimId']} "
                                                      f"viewConfigId={prior['viewConfigId']}"))  # fmt: skip
        else:
            rows.append((source, t["url"], "GONE", "redirect existed at proposal time but is now missing — re-run extract_and_match.py"))  # fmt: skip

    width = max(len(r[0]) for r in rows) + 2
    print(f"\n{'source':<{width}} {'status':<8} note / target")
    print("-" * (width + 90))
    for source, url, status, note in sorted(rows, key=lambda x: (x[2], x[0])):
        print(f"{source:<{width}} {status:<8} {note or '-> ' + url}")

    csv_path = mapping_dir / "redirects_for_cli.csv"
    csv_problems = validate_cli_csv(csv_path, redirects)
    if csv_problems:
        print(f"\nCSV problems in {csv_path}:")
        for p in csv_problems:
            print(f"  {p}")

    # `already_done` rows are unpublished by hand, which breaks their embeds just as the
    # CLI's unpublish step would — they belong behind the same gate.
    embeds = {} if args.no_references else embed_references(redirects + already_done)
    if embeds:
        print("\nSurfaces a redirect will NOT fix (these embed the chart and break when it is unpublished):")
        for r in redirects + already_done:
            note = embeds.get(r["chart"]["id"])
            if note:
                print(f"  {r['chart']['slug']}: {note}")
        print("  Run audit_references.py for the full list with replacement URLs.")

    bad = [r for r in rows if r[2] not in ("OK", "MANUAL")]
    manual = [r for r in rows if r[2] == "MANUAL"]
    if bad or csv_problems:
        print(f"\nNOT ready: {len(bad)} row(s) and {len(csv_problems)} CSV problem(s) would abort the CLI.")
        print("It runs a single transaction — any one of these aborts the entire migration.")
    if embeds:
        print(f"\nNOT ready: {len(embeds)} source chart(s) are still embedded elsewhere. These do NOT abort the CLI —"
              "\nthey break silently the moment the chart is unpublished, by the CLI or by hand (MANUAL rows)."
              "\nMigrate them first (audit_references.py lists each reference with its replacement URL);"
              "\n--no-references skips this gate once they are handled.")  # fmt: skip
    if manual:
        print(f"\n{len(manual)} row(s) need a step outside the CLI — see MANUAL above.")
    if bad or csv_problems or embeds or manual:
        return 1

    print(f"\nReady: {len(rows)} row(s) validate against the live DB. Rehearse, then apply, from owid-grapher:")
    print(f"  yarn createMultiDimRedirectsFromCsv {csv_path.resolve()} --dry-run")
    print("  (--dry-run rolls the transaction back and skips unpublishing entirely)")
    print("Then stamp cutover_date in migration_log_template.csv — analytics cannot recover it later.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
