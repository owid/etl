"""Validate explorer→MDIM redirect payloads against the live DB before anything is posted.

READ-ONLY (nothing is written unless --record). Exits non-zero while any BLOCKER remains.

Every check here mirrors a validation the admin bulk endpoint performs, or a fact about how
the redirect is served. That matters more than on the chart side for two reasons:

- **There is no bulk delete.** Redirects are removed one row at a time, so a 460-row batch
  posted wrongly costs 460 clicks. This script is the whole safety net.
- **The endpoint memoizes its source-side checks and re-throws the cached rejection**, so a
  single source-level problem fails EVERY entry for that explorer, not one row. Checks are
  therefore grouped per explorer, and their messages say so.

Usage:
    ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
        .claude/skills/map-explorer-to-mdim/scripts/preflight.py \
        --mapping ai/<a>-mdim-mapping --mapping ai/<b>-mdim-mapping \
        --out ai/<combined>-redirects [--no-references] [--record <path>]
"""

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

from etl.config import OWID_ENV

sys.path.insert(0, str(Path(__file__).resolve().parent))
from redirect_rules import (  # noqa: E402
    build_source_rules,
    duplicate_conditions,
    parse_explorer_views,
    views_fingerprint,
)

TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")
BLOCKER, WARN, OK = "BLOCKER", "WARN", "ok"


def load_runs(paths: list[str]) -> dict[str, dict]:
    """{explorer slug -> {payload, mapping, sources, dir, rules}} from each --mapping dir.

    The PAYLOAD is what gets posted, so it is what gets validated — `mapping.json` keeps the
    empty source dimensions that the payload strips, and validating the record instead of the
    artifact would check something nobody applies.
    """
    runs: dict[str, dict] = {}
    for raw in paths:
        d = Path(raw)
        if not d.is_dir():
            d = d.parent
        payload_path = d / "admin_bulk_payload.json"
        if not payload_path.exists():
            raise SystemExit(
                f"Not found: {payload_path}. Run build_mapping.py — it writes the apply-ready payload "
                "next to mapping.json."
            )
        payload = json.loads(payload_path.read_text())
        slug = payload["explorer"]["slug"]
        sources_path = d / "_sources.json"
        runs[slug] = {
            "dir": d,
            "payload": payload,
            "rules": build_source_rules(payload),
            "sources": json.loads(sources_path.read_text()) if sources_path.exists() else {},
        }
    return runs


def live_explorer_state(slugs: list[str]) -> dict[str, dict]:
    df = OWID_ENV.read_sql(
        "SELECT slug, tsv, isPublished, configMd5 FROM explorers WHERE slug IN %(s)s",
        params={"s": tuple(slugs)},
    )
    return {r["slug"]: r for r in df.to_dict("records")}


def existing_redirects(slugs: list[str]) -> dict[str, list[dict]]:
    df = OWID_ENV.read_sql(
        "SELECT source, sourceQueryParams, multiDimId, viewConfigId FROM multi_dim_redirects WHERE source IN %(s)s",
        params={"s": tuple(f"/explorers/{s}" for s in slugs)},
    )
    out: dict[str, list[dict]] = defaultdict(list)
    for r in df.to_dict("records"):
        out[r["source"].removeprefix("/explorers/")].append(r)
    return out


def site_redirect_rows(slugs: list[str]) -> tuple[dict[str, list], dict[str, list]]:
    """({slug -> rows where the explorer path is the SOURCE}, {slug -> rows where it is the TARGET}).

    Both are blockers, for different reasons: a source collision is
    `checkSourceNotSiteRedirectSource`, a target one is `checkSourceNotRedirectTarget` — the
    chain check. The `LIKE` is re-checked in Python so `/explorers/inequality-wb` cannot
    answer for `inequality`.
    """
    paths = [f"/explorers/{s}" for s in slugs]
    df = OWID_ENV.read_sql(
        "SELECT id, source, target FROM redirects WHERE source IN %(p)s OR target LIKE %(like)s",
        params={"p": tuple(paths), "like": "%/explorers/%"},
    )
    as_source: dict[str, list] = defaultdict(list)
    as_target: dict[str, list] = defaultdict(list)
    for r in df.to_dict("records"):
        for slug in slugs:
            if r["source"] == f"/explorers/{slug}":
                as_source[slug].append(r)
            if re.search(rf"/explorers/{re.escape(slug)}(?:[?#/]|$)", r["target"] or ""):
                as_target[slug].append(r)
    return as_source, as_target


def chart_slug_collisions(slugs: list[str]) -> set[str]:
    """Explorer slugs that are also a chart's OLD slug (`checkSourceNotChartSlugRedirectSource`)."""
    df = OWID_ENV.read_sql("SELECT slug FROM chart_slug_redirects WHERE slug IN %(s)s", params={"s": tuple(slugs)})
    return set(df["slug"])


def target_state(catalog_paths: set[str]) -> dict[str, dict]:
    """{catalogPath -> {id, slug, published, views}} where `views` maps a view's dimension
    signature to its `fullConfigId`.

    The view map is what makes target-side staleness detectable. Checking only that the MDIM
    row exists and is published passes an MDIM that was rebuilt, re-sliced or re-slugged after
    extraction — so preflight would report Ready for a payload whose reviewed target views no
    longer exist, and applying it would reject some entries AFTER creating others (there is no
    transaction, and no bulk undo). It also yields the expected `viewConfigId` per rule, which
    is what distinguishes "already applied" from "applied differently".
    """
    if not catalog_paths:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT catalogPath, id, slug, published, config FROM multi_dim_data_pages WHERE catalogPath IN %(c)s",
        params={"c": tuple(sorted(catalog_paths))},
    )
    out: dict[str, dict] = {}
    for r in df.to_dict("records"):
        cfg = json.loads(r["config"]) if isinstance(r["config"], str) else (r["config"] or {})
        views = {
            tuple(sorted((v.get("dimensions") or {}).items())): v.get("fullConfigId") for v in cfg.get("views") or []
        }
        out[r["catalogPath"]] = {
            "id": int(r["id"]),
            "slug": r["slug"] or "",
            "published": bool(r["published"]),
            "views": views,
        }
    return out


def expected_row(rule, targets: dict[str, dict]) -> tuple[int | None, str | None]:
    """The (multiDimId, viewConfigId) the endpoint would store for this rule.

    A catch-all constrains no dimensions, so it is stored with `viewConfigId = NULL` (verified
    against production) — which is why an empty target signature maps to None rather than to
    whichever view happens to have no dimensions.
    """
    t = targets.get(rule.catalog_path)
    if t is None:
        return None, None
    if not rule.target_dims:
        return t["id"], None
    return t["id"], t["views"].get(tuple(sorted(rule.target_dims.items())))


def target_is_redirect_source(mdim_slugs: set[str]) -> set[str]:
    """MDIM slugs whose `/grapher/<slug>` is already a redirect source (`checkTargetNotRedirectSource`)."""
    if not mdim_slugs:
        return set()
    paths = tuple(f"/grapher/{s}" for s in sorted(mdim_slugs))
    taken: set[str] = set()
    for sql, params in (
        ("SELECT source FROM redirects WHERE source IN %(p)s", {"p": paths}),
        ("SELECT source FROM multi_dim_redirects WHERE source IN %(p)s", {"p": paths}),
    ):
        taken |= {r.removeprefix("/grapher/") for r in OWID_ENV.read_sql(sql, params=params)["source"]}
    df = OWID_ENV.read_sql(
        "SELECT slug FROM chart_slug_redirects WHERE slug IN %(s)s", params={"s": tuple(sorted(mdim_slugs))}
    )
    return taken | set(df["slug"])


def reference_gate(out: Path, slugs: list[str]) -> tuple[str, str]:
    """(status, message) from the audit's own report — the surfaces the redirect breaks.

    A missing `references.csv` is a BLOCKER, not a pass: 'we never looked' and 'we looked and
    found nothing' must not produce the same verdict.
    """
    path = out / "references.csv"
    if not path.exists():
        return BLOCKER, f"no references.csv in {out} — run audit_references.py first (or pass --no-references)"
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    # Coverage comes from the audit's manifest, never from the rows: an explorer nothing
    # references contributes no rows, so deriving it from the CSV alone would report a
    # perfectly clean audit as "not audited" and gate on it forever, unclearable by re-running.
    manifest_path = out / "references_manifest.json"
    if manifest_path.exists():
        audited = set(json.loads(manifest_path.read_text()).get("explorers") or [])
    else:
        audited = {r["explorer"] for r in rows}  # pre-manifest run: best effort
    unaudited = sorted(set(slugs) - audited)
    breaks = [r for r in rows if r["severity"] == "RED" and r["surface"] != "site redirect"]
    if unaudited:
        return BLOCKER, f"the audit did not cover {unaudited} — re-run audit_references.py for every explorer"
    if breaks:
        per = defaultdict(int)
        for r in breaks:
            per[r["explorer"]] += 1
        detail = ", ".join(f"{k}: {v}" for k, v in sorted(per.items()))
        return BLOCKER, f"{len(breaks)} embedded reference(s) still to migrate ({detail}) — see references.md"
    return OK, f"{len(rows)} reference(s) audited, none embedded"


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate explorer redirect payloads before applying.")
    ap.add_argument("--mapping", action="append", required=True, help="A mapping dir (repeatable)")
    ap.add_argument("--out", default=None, help="Folder holding references.csv (default: the sole mapping dir)")
    ap.add_argument("--no-references", action="store_true", help="Skip the embedded-reference gate")
    ap.add_argument("--record", default=None, help="Also write a combined machine record to this path")
    args = ap.parse_args()

    runs = load_runs(args.mapping)
    slugs = sorted(runs)
    if args.out:
        out = Path(args.out)
    elif len(args.mapping) == 1:
        out = next(iter(runs.values()))["dir"]
    else:
        raise SystemExit("--out is required when more than one --mapping is given")

    admin = TAILSCALE_SUFFIX_RE.sub("", (OWID_ENV.admin_site or "https://admin.owid.io/admin").rstrip("/"))
    print(f"Environment: {OWID_ENV.site}   READ-ONLY preflight — nothing is created here.")
    print(f"explorers: {', '.join(slugs)}\n")

    live = live_explorer_state(slugs)
    existing = existing_redirects(slugs)
    as_source, as_target = site_redirect_rows(slugs)
    colliding = chart_slug_collisions(slugs)
    all_paths = {r.catalog_path for run in runs.values() for r in run["rules"] if r.catalog_path}
    targets = target_state(all_paths)
    taken_targets = target_is_redirect_source({t["slug"] for t in targets.values() if t["slug"]})

    findings: list[tuple[str, str, str]] = []  # (status, explorer, message)
    done: set[str] = set()  # explorers already fully applied — nothing to paste
    for slug in slugs:
        run = runs[slug]
        rules = run["rules"]
        row = live.get(slug)
        applied = existing.get(slug, [])

        if row is None:
            if applied:
                done.add(slug)
                findings.append(
                    (
                        OK,
                        slug,
                        f"DONE — explorer retired and {len(applied)} redirect(s) already live. Nothing to apply.",
                    )
                )
                continue
            findings.append(
                (BLOCKER, slug, "explorer is not in the `explorers` table and has no redirects — wrong slug?")
            )
            continue

        if row["isPublished"]:
            findings.append(
                (
                    WARN,
                    slug,
                    "explorer is still PUBLISHED — creating the redirect darkens it immediately (the redirect "
                    "is checked on every /explorers/* request, ahead of the page itself)",
                )
            )

        recorded = (run["sources"].get("explorer") or {}).get("viewsFingerprint")
        if recorded:
            dim_names, live_rows = parse_explorer_views(row["tsv"])
            actual = views_fingerprint(dim_names, live_rows)
            if actual != recorded:
                findings.append(
                    (
                        BLOCKER,
                        slug,
                        f"STALE — the explorer's views changed since extraction ({recorded} -> {actual}, "
                        f"{(run['sources']['explorer'].get('viewCount'))} -> {len(live_rows)} views). Every "
                        "positional view id in the payload is now untrustworthy: re-run extract_views.py, "
                        "re-review, rebuild.",
                    )
                )
            elif (run["sources"].get("explorer") or {}).get("configMd5") not in (None, "", row["configMd5"]):
                findings.append(
                    (
                        WARN,
                        slug,
                        "EDITED — the explorer's config changed but its view grid did not; mapping still valid",
                    )
                )
        else:
            findings.append(
                (WARN, slug, "no viewsFingerprint recorded (extracted before they existed) — staleness unchecked")
            )

        for r in as_source.get(slug, []):
            findings.append(
                (
                    BLOCKER,
                    slug,
                    f"/explorers/{slug} is already a site redirect source (id={r['id']} -> {r['target']}). The "
                    "endpoint refuses it, and the site redirect would win anyway. Delete or repoint that row.",
                )
            )
        for r in as_target.get(slug, []):
            findings.append(
                (
                    BLOCKER,
                    slug,
                    f"CHAIN — site redirect id={r['id']} ({r['source']} -> {r['target']}) points AT this explorer. "
                    f"The endpoint rejects it as a chain, and because it caches per-source checks this fails ALL "
                    f"{len(rules)} entries for this explorer, not one row.\n"
                    f"      Fix: repoint, do not just delete — a row whose source is a real URL becomes a 404. "
                    f"Delete at {admin}/site-redirects, then POST {admin}/api/site-redirects/new with the MDIM "
                    f"target (references.md computes it). A vanity launch path nothing links to can simply go.",
                )
            )
        if slug in colliding:
            findings.append(
                (
                    BLOCKER,
                    slug,
                    f"'{slug}' is also a chart's old slug in chart_slug_redirects — the endpoint refuses it",
                )
            )

        for path in sorted({r.catalog_path for r in rules if r.catalog_path}):
            t = targets.get(path)
            if t is None:
                findings.append((BLOCKER, slug, f"target MDIM not in multi_dim_data_pages: {path}"))
            elif not t["published"]:
                findings.append(
                    (
                        BLOCKER,
                        slug,
                        f"target MDIM is NOT published: {path}. The endpoint refuses it at create time and the "
                        "baker filters on published again, so the redirect would silently serve nothing.",
                    )
                )
            elif t["slug"] in taken_targets:
                findings.append((BLOCKER, slug, f"target /grapher/{t['slug']} is itself a redirect source"))

        # Per-RULE target revalidation. An MDIM can be rebuilt, re-sliced or re-slugged while
        # its catalogPath stays present and published, in which case the reviewed view no
        # longer exists — and applying would reject those entries after creating others.
        missing_views = []
        reslugged = []
        for rule in rules:
            t = targets.get(rule.catalog_path)
            if t is None or not t["published"]:
                continue  # already reported above
            if rule.mdim_slug and rule.mdim_slug != t["slug"]:
                reslugged.append((rule.mdim_slug, t["slug"]))
            if rule.target_dims and tuple(sorted(rule.target_dims.items())) not in t["views"]:
                missing_views.append((rule.source_view_id, rule.view_id, rule.target_dims))
        for was, now in sorted(set(reslugged)):
            findings.append(
                (
                    BLOCKER,
                    slug,
                    f"target MDIM was re-slugged since extraction ({was} -> {now}) — every replacement URL in "
                    "the payload points at the old slug. Re-run extract_views.py and rebuild.",
                )
            )
        if missing_views:
            sample = "; ".join(f"view {sid} ({vid}) -> {dims}" for sid, vid, dims in missing_views[:3])
            findings.append(
                (
                    BLOCKER,
                    slug,
                    f"{len(missing_views)} target view(s) no longer exist in the live MDIM config — the MDIM was "
                    f"rebuilt or re-sliced since extraction. e.g. {sample}. Re-extract, re-review, rebuild.",
                )
            )

        for a, b in duplicate_conditions(rules):
            findings.append(
                (
                    BLOCKER,
                    slug,
                    f"views {a.source_view_id or 'catchAll'} and {b.source_view_id or 'catchAll'} share the "
                    f"condition {a.condition or '{}'} — the endpoint rejects the second, so it could never serve",
                )
            )

        if applied:
            have = {
                tuple(sorted((json.loads(r["sourceQueryParams"]) or {}).items())): (r["multiDimId"], r["viewConfigId"])
                for r in applied
            }
            want = {tuple(sorted(r.condition.items())): expected_row(r, targets) for r in rules}
            if set(have) != set(want):
                findings.append(
                    (
                        BLOCKER,
                        slug,
                        f"{len(applied)} redirect(s) already exist for this explorer and differ from the payload "
                        f"({len(set(want) - set(have))} to add, {len(set(have) - set(want))} unexpected). There is "
                        f"NO bulk delete: remove them one at a time at {admin}/multi-dim-redirects before "
                        "re-applying.",
                    )
                )
            else:
                # Every condition is present. Whether that is DONE or a conflict depends on the
                # TARGETS: re-posting an identical payload is rejected as duplicate sources, and
                # a set pointing somewhere else must never read as ready.
                retargeted = {c for c in want if have[c] != want[c]}
                if retargeted:
                    findings.append(
                        (
                            BLOCKER,
                            slug,
                            f"all {len(want)} source condition(s) already exist but {len(retargeted)} point at a "
                            f"DIFFERENT target than the payload. Remove those rows at {admin}/multi-dim-redirects "
                            "before re-applying — the endpoint would reject them as duplicate sources.",
                        )
                    )
                else:
                    done.add(slug)
                    findings.append(
                        (
                            OK,
                            slug,
                            f"DONE — all {len(want)} redirect(s) already applied and pointing at the payload's "
                            "targets. Do not paste it again; the endpoint rejects duplicate source conditions.",
                        )
                    )

        n_skipped = sum(1 for e in run["payload"].get("redirects") or [] if not e.get("target"))
        if n_skipped:
            findings.append(
                (
                    WARN,
                    slug,
                    f"{n_skipped} view(s) unresolved — the endpoint reports them `skipped`; those URLs keep "
                    "serving the explorer until the catch-all takes them",
                )
            )
        if not (run["payload"].get("catchAll") or {}).get("target"):
            findings.append((WARN, slug, "no catch-all — the bare explorer URL keeps serving the explorer"))

    if not args.no_references:
        status, message = reference_gate(out, slugs)
        findings.append((status, "(all)", message))

    print(f"{'status':8} {'explorer':38} note")
    print("-" * 150)
    for status, slug, message in sorted(findings, key=lambda f: ({BLOCKER: 0, WARN: 1, OK: 2}[f[0]], f[1])):
        print(f"{status:8} {slug:38} {message}")

    blockers = [f for f in findings if f[0] == BLOCKER]
    print()
    print(
        "> Creating an explorer redirect takes effect on the NEXT REQUEST, while the explorer is still "
        "published, and everything embedding it breaks at that moment. There is no bulk undo."
    )
    if args.record:
        record = {"explorers": [{"slug": s, "payload": runs[s]["payload"]} for s in slugs]}
        Path(args.record).write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n")
        print(
            f"\n-> {args.record}  (a RECORD, not a payload: the endpoint's schema has a single catchAll, so a "
            "combined file would drop all but one. Post the per-explorer admin_bulk_payload.json files.)"
        )
    if blockers:
        print(f"\nNOT READY: {len(blockers)} blocker(s). Nothing should be posted until each is cleared.")
        return 1
    to_apply = [s for s in slugs if s not in done]
    if not to_apply:
        print(f"\nNothing to apply: all {len(slugs)} explorer(s) are already fully redirected.")
        return 0
    print(
        f"\nReady: {sum(len(runs[s]['rules']) for s in to_apply)} redirect rule(s) across {len(to_apply)} explorer(s)."
    )
    for slug in to_apply:
        print(f"  paste {runs[slug]['dir']}/admin_bulk_payload.json at {admin}/multi-dim-redirects")
    if done:
        print(f"  (skipping {sorted(done)} — already applied)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
