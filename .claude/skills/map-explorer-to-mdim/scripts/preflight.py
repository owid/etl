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
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "find-chart-references" / "scripts"))
from redirect_rules import (  # noqa: E402
    build_source_rules,
    drop_duplicate_entries,
    duplicate_conditions,
    parse_explorer_views,
    strip_empty,
    strip_payload,
    views_fingerprint,
)
from reference_report import reference_digest, run_sweep  # noqa: E402

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


def extraction_conditions(d: Path) -> dict[int, dict[str, str]] | None:
    """{sourceViewId -> source condition} implied by the extraction sitting in this run's folder.

    None when the pair of files needed to derive it is not there, which is the only honest
    answer for a run predating them — the caller reports that as unchecked rather than clean.
    """
    sources_path, views_path = d / "_sources.json", d / "explorer_views.csv"
    if not (sources_path.exists() and views_path.exists()):
        return None
    names = list((json.loads(sources_path.read_text()).get("explorer") or {}).get("dimensions") or [])
    if not names:
        return None
    grid: dict[int, dict[str, str]] = {}
    with open(views_path, newline="") as f:
        reader = csv.DictReader(f)
        # `_sources.json` names dimension_1..N and the CSV carries those columns; extraction
        # writes both from one list, so they can only disagree if the two files come from
        # different runs. That is precisely what this check exists to catch, so refuse to
        # compare rather than silently read a short row as a set of empty values.
        if [f"dimension_{i}" for i in range(1, len(names) + 1)] != [c for c in (reader.fieldnames or []) if c != "id"]:
            return None
        for row in reader:
            values = [(row.get(f"dimension_{i}") or "").strip() for i in range(1, len(names) + 1)]
            grid[int(row["id"])] = strip_empty(dict(zip(names, values)))
    return grid


def payload_binding(run: dict) -> tuple[str, str]:
    """(status, message): was `admin_bulk_payload.json` built from the extraction beside it?

    The payload and `_sources.json` are separate files loaded independently, so nothing ties one
    to the other. Re-running extract_views.py refreshes `_sources.json`; if the rebuild that
    should follow never happens — or aborts partway, which it does on duplicate conditions,
    AFTER mapping.json is written but BEFORE the payload is — the OLD payload is left beside the
    NEW extraction. The live-vs-recorded fingerprint check then passes (it validates the fresh
    extraction) while every rule about to be posted still comes from the stale payload, and
    preflight reports Ready for source conditions the explorer no longer has. So the payload's
    own conditions are compared against the view grid on disk, not just its fingerprint.
    """
    grid = extraction_conditions(run["dir"])
    if grid is None:
        # A blocker, not a warning: warnings do not reach the exit code, so `Ready` would print
        # over a report that says the payload's provenance is unknown. Same rule the reference
        # gate already applies to a missing references.csv — "we could not check" must not land
        # in the same bucket as "we checked and it is fine".
        return (
            BLOCKER,
            "UNVERIFIABLE PAYLOAD — no usable explorer_views.csv/_sources.json pair beside it (missing, or the "
            "two disagree on the dimension columns), so nothing confirms this payload was built from this "
            "extraction. Re-run extract_views.py and build_mapping.py, and re-review, rather than posting "
            "source conditions no artifact backs.",
        )
    entries = run["payload"].get("redirects") or []
    mismatched, unknown = [], []
    for entry in entries:
        vid = entry.get("sourceViewId")
        if vid not in grid:
            unknown.append(vid)
        elif grid[vid] != strip_empty((entry.get("source") or {}).get("dimensions") or {}):
            mismatched.append(vid)
    # A view missing from the payload is expected in exactly one case: its condition collapsed
    # onto an earlier rule's and --allow-duplicate-conditions dropped it, in which case that
    # condition is still represented by the rule that kept it. Anything else is a view the
    # payload never mapped — an extraction that gained rows since it was built.
    represented = {tuple(sorted(strip_empty((e.get("source") or {}).get("dimensions") or {}).items())) for e in entries}
    represented.add(())  # the catch-all constrains nothing, so it stands in for an all-empty row
    covered = {e.get("sourceViewId") for e in entries}
    absent = [i for i in sorted(grid) if i not in covered and tuple(sorted(grid[i].items())) not in represented]
    if not (mismatched or unknown or absent):
        return OK, f"payload matches the {len(grid)}-view extraction beside it"
    parts = []
    if mismatched:
        parts.append(f"{len(mismatched)} entry(ies) carry dimensions the extraction does not (e.g. {mismatched[:5]})")
    if unknown:
        parts.append(f"{len(unknown)} entry(ies) name view ids the extraction no longer has (e.g. {unknown[:5]})")
    if absent:
        parts.append(f"{len(absent)} extracted view(s) have no entry in the payload (e.g. {absent[:5]})")
    return BLOCKER, (
        "STALE PAYLOAD — admin_bulk_payload.json was not built from the extraction beside it: "
        + "; ".join(parts)
        + ". Re-run build_mapping.py and re-review; an aborted build leaves the previous payload in place, "
        "and the fingerprint check alone cannot see that."
    )


def payload_matches_build(run: dict) -> tuple[str, str]:
    """(status, message): is the payload the one this run's `mapping.json` produces?

    `payload_binding` only ties the payload's SOURCE side to the extraction, which leaves the
    target side loose — and the targets are what a reviewer signs off on. `mapping.json` is
    written from the current build immediately BEFORE the payload, and the payload is a pure
    function of it, so re-deriving that transform proves the two came from the same build. This
    is what catches a build that aborted between them (duplicate conditions do exactly that,
    after mapping.json is written): `mapping_proposal.csv` and `mapping.json` then describe the
    targets a human reviewed, while the payload still carries the previous run's, with no
    source-side difference to give it away.
    """
    mapping_path = run["dir"] / "mapping.json"
    if not mapping_path.exists():
        return (
            BLOCKER,
            "UNVERIFIABLE PAYLOAD — no mapping.json beside it, so nothing confirms the payload and the reviewed "
            "proposal came from the same build. Re-run build_mapping.py; a warning here would let `Ready` print "
            "over a payload whose targets no artifact backs.",
        )
    mapping = json.loads(mapping_path.read_text())
    strict, _, clashes = strip_payload(mapping)
    # Either duplicate-handling outcome is legitimate: the builder aborts on clashes unless
    # --allow-duplicate-conditions was passed, in which case it drops the later ones.
    candidates = [strict]
    if clashes:
        candidates.append(drop_duplicate_entries(json.loads(json.dumps(strict)), clashes))
    if any(run["payload"] == candidate for candidate in candidates):
        return OK, "payload is exactly what mapping.json builds — same build"
    return BLOCKER, (
        "STALE PAYLOAD — admin_bulk_payload.json is not what mapping.json next to it builds, so the two are "
        "from different runs. mapping.json and mapping_proposal.csv are written BEFORE the payload, so a build "
        "that aborted in between leaves a reviewed proposal beside an unreviewed payload — including different "
        "targets. Re-run build_mapping.py and re-review before applying."
    )


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
    """{catalogPath -> {id, slug, published, views, md5}} where `views` maps a view's dimension
    signature to its `fullConfigId`, and `md5` that config id to the config's current `fullMd5`.

    The view map is what makes target-side staleness detectable. Checking only that the MDIM
    row exists and is published passes an MDIM that was rebuilt, re-sliced or re-slugged after
    extraction — so preflight would report Ready for a payload whose reviewed target views no
    longer exist, and applying it would reject some entries AFTER creating others (there is no
    transaction, and no bulk undo). It also yields the expected `viewConfigId` per rule, which
    is what distinguishes "already applied" from "applied differently".

    The md5 map covers the case none of that catches: grapher edits a view's chart config IN
    PLACE, so swapping its indicators or changing its rendering leaves both the dimension
    signature and the `fullConfigId` untouched. Without the md5 a mapping reviewed against the
    old rendering reaches Ready and sends readers somewhere materially different.
    """
    if not catalog_paths:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT catalogPath, id, slug, published, config FROM multi_dim_data_pages WHERE catalogPath IN %(c)s",
        params={"c": tuple(sorted(catalog_paths))},
    )
    out: dict[str, dict] = {}
    config_ids: set[str] = set()
    for r in df.to_dict("records"):
        cfg = json.loads(r["config"]) if isinstance(r["config"], str) else (r["config"] or {})
        views = {
            tuple(sorted((v.get("dimensions") or {}).items())): v.get("fullConfigId") for v in cfg.get("views") or []
        }
        config_ids |= {c for c in views.values() if c}
        out[r["catalogPath"]] = {
            "id": int(r["id"]),
            "slug": r["slug"] or "",
            "published": bool(r["published"]),
            "views": views,
        }
    live_md5 = view_config_md5s(config_ids)
    for t in out.values():
        t["md5"] = {cid: live_md5.get(cid, "") for cid in t["views"].values() if cid}
    return out


def view_config_md5s(config_ids: set[str]) -> dict[str, str]:
    """{chart_configs.id -> fullMd5} for a set of MDIM view configs."""
    if not config_ids:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT id, fullMd5 FROM chart_configs WHERE id IN %(ids)s", params={"ids": tuple(sorted(config_ids))}
    )
    return dict(zip(df["id"], df["fullMd5"]))


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


def revalidate_targets(rules, targets: dict[str, dict]) -> tuple[list, list, list]:
    """Per-rule target staleness: (missing_views, reslugged, retargeted_views).

    An MDIM can be rebuilt, re-sliced or re-slugged while its catalogPath stays present and
    published, in which case the reviewed view no longer exists — and applying would reject
    those entries after creating others. It can also keep every view in place and only change
    what one of them renders, which nothing above notices.
    """
    missing_views, reslugged, retargeted_views = [], [], []
    for rule in rules:
        t = targets.get(rule.catalog_path)
        if t is None or not t["published"]:
            continue  # reported separately, per catalogPath
        if rule.mdim_slug and rule.mdim_slug != t["slug"]:
            reslugged.append((rule.mdim_slug, t["slug"]))
        # Resolve through expected_row rather than testing signature membership: a view can be
        # present but carry no `fullConfigId`, in which case it cannot be stored as a
        # viewConfigId at all — the chart-side extractor drops such views from its target pool
        # for the same reason. Membership alone would let that reach `Ready`.
        _, config_id = expected_row(rule, targets)
        if rule.target_dims and config_id is None:
            unstorable = tuple(sorted(rule.target_dims.items())) in t["views"]
            missing_views.append((rule.source_view_id, rule.view_id, rule.target_dims, unstorable))
            continue
        # Same view id, same dimensions, different rendering: grapher edits a view's chart
        # config in place. Skipped when no md5 was recorded (a run from before this was
        # tracked, or a catch-all, which points at whatever the default view happens to be) —
        # absence of a recorded rendering is not evidence of drift.
        if rule.view_config_md5:
            live = t["md5"].get(config_id or "", "")
            if live and live != rule.view_config_md5:
                retargeted_views.append((rule.source_view_id, rule.view_id))
    return missing_views, reslugged, retargeted_views


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


def compare_applied(rules, applied: list[dict], targets: dict[str, dict], admin: str) -> tuple[str, str]:
    """(status, message) comparing the redirects already in the DB with the payload.

    Called for BOTH a live and an already-retired explorer. Presence of *some* rows is not
    completion: a partial or wrong bulk run leaves a subset behind — often just the catch-all —
    and if the explorer has since been deleted there is nothing else left to notice it. So the
    conditions AND their targets are compared before anything is called done.
    """

    # A catch-all row stores sourceQueryParams as SQL NULL, so this cannot assume a JSON
    # string — json.loads(None) raises, and every explorer that already has a catch-all
    # applied would crash here rather than being compared.
    def condition_of(row: dict) -> tuple:
        raw = row["sourceQueryParams"]
        params = json.loads(raw) if isinstance(raw, str) else (raw or {})
        return tuple(sorted((params or {}).items()))

    have = {condition_of(r): (r["multiDimId"], r["viewConfigId"]) for r in applied}
    want = {tuple(sorted(r.condition.items())): expected_row(r, targets) for r in rules}
    if set(have) != set(want):
        missing, extra = len(set(want) - set(have)), len(set(have) - set(want))
        return (
            BLOCKER,
            f"{len(applied)} redirect(s) exist but the set differs from the payload ({missing} missing, "
            f"{extra} unexpected) — a partial or superseded run. There is NO bulk delete: remove the "
            f"unexpected rows one at a time at {admin}/multi-dim-redirects, then apply the payload.",
        )
    retargeted = {c for c in want if have[c] != want[c]}
    if retargeted:
        return (
            BLOCKER,
            f"all {len(want)} source condition(s) exist but {len(retargeted)} point at a DIFFERENT target than "
            f"the payload. Remove those rows at {admin}/multi-dim-redirects before re-applying — the endpoint "
            "would reject them as duplicate sources.",
        )
    return (
        OK,
        f"DONE — all {len(want)} redirect(s) already applied and pointing at the payload's targets. Do not "
        "paste it again; the endpoint rejects duplicate source conditions.",
    )


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


def audit_freshness(out: Path, slugs: list[str]) -> tuple[str, str]:
    """(status, message): does the audit on disk still describe the live site?

    `reference_gate` reads a CSV an earlier run produced, so by itself it cannot see anything
    that changed since — a page that added an explorer embed after the audit, or an audit folder
    carried over from another migration. In both cases it reports a clean audit while the
    redirect is about to break something live, which is the exact failure the gate exists to
    prevent. So the sweep is re-run here and compared against the digest the audit recorded.
    """
    manifest_path = out / "references_manifest.json"
    recorded = {}
    if manifest_path.exists():
        recorded = json.loads(manifest_path.read_text()).get("referenceDigests") or {}
    stale_record = sorted(set(slugs) - set(recorded))
    if stale_record:
        # A blocker, for the same reason a missing references.csv is one: this branch does not
        # run the sweep, so nothing at all has looked at the live site. Warning would let
        # `Ready` print while an embed added since that audit stays invisible — and creating the
        # redirect breaks it on the next request.
        return (
            BLOCKER,
            f"UNVERIFIABLE AUDIT — no reference digest recorded for {stale_record}, so references.csv cannot be "
            "checked against the live site and anything added since it ran is invisible. Re-run "
            "audit_references.py (it records the digest) before applying.",
        )
    raw, gaps = run_sweep([arg for slug in slugs for arg in ("--explorer", slug)])
    drifted = sorted(slug for slug in slugs if reference_digest(raw, slug) != recorded[slug])
    if drifted:
        return (
            BLOCKER,
            f"the live references for {drifted} no longer match the audit — something links or embeds those "
            "explorers that references.csv does not list (or this folder's audit is from another run). Re-run "
            "audit_references.py and re-read references.md before applying.",
        )
    if gaps:
        return WARN, f"live references still match the audit, but the sweep reported {len(gaps)} gap(s): {gaps[:3]}"
    return OK, f"live references still match the audit for {len(slugs)} explorer(s)"


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
        retired = row is None

        if retired and not applied:
            findings.append(
                (BLOCKER, slug, "explorer is not in the `explorers` table and has no redirects — wrong slug?")
            )
            continue

        for check in (payload_binding, payload_matches_build):
            status, message = check(run)
            findings.append((status, slug, message))

        if retired:
            # No explorer row means no TSV to re-fingerprint, so the source side cannot be
            # rechecked against anything live. Everything on the TARGET side still decides
            # whether the stored redirects serve what was reviewed, so the checks below run for a
            # retired explorer too — an MDIM that has since been unpublished, rebuilt, re-slugged
            # or edited in place breaks redirects that are already in the DB, and matching stored
            # ids alone would report that as done.
            findings.append(
                (
                    WARN,
                    slug,
                    "explorer row is gone, so its view grid cannot be re-fingerprinted against the live TSV — "
                    "only the extraction on disk backs the payload's source conditions",
                )
            )
        else:
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

        missing_views, reslugged, retargeted_views = revalidate_targets(rules, targets)
        if retargeted_views:
            sample = ", ".join(f"view {sid} -> {vid}" for sid, vid in retargeted_views[:4])
            findings.append(
                (
                    BLOCKER,
                    slug,
                    f"{len(retargeted_views)} target view(s) have been EDITED since extraction — same view id and "
                    f"same dimensions, but a different rendered config, so the redirect would send readers to "
                    f"materially different content than was reviewed. e.g. {sample}. Re-extract and re-review.",
                )
            )

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
            sample = "; ".join(f"view {sid} ({vid}) -> {dims}" for sid, vid, dims, _ in missing_views[:3])
            no_config = sum(1 for *_, unstorable in missing_views if unstorable)
            detail = (
                f" {no_config} of them still exist but carry no fullConfigId, so they cannot be stored as a "
                "redirect target at all."
                if no_config
                else ""
            )
            findings.append(
                (
                    BLOCKER,
                    slug,
                    f"{len(missing_views)} target view(s) are not usable in the live MDIM config — rebuilt or "
                    f"re-sliced since extraction.{detail} e.g. {sample}. Re-extract, re-review, rebuild.",
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
            status, message = compare_applied(rules, applied, targets, admin)
            # `compare_applied` only compares stored ids against the payload, so on its own it
            # would call a migration done while the checks above say the target side no longer
            # renders what was reviewed. A slug carrying a blocker is never done.
            if status == OK and any(f[0] == BLOCKER and f[1] == slug for f in findings):
                status, message = (
                    WARN,
                    f"the {len(rules)} stored redirect(s) do match the payload's targets, but the blocker(s) above "
                    "mean the target side no longer serves what was reviewed — NOT done",
                )
            elif status == OK:
                done.add(slug)
                if retired:
                    message = f"DONE — explorer retired; {message.removeprefix('DONE — ')}"
            elif retired:
                message = (
                    f"explorer is already retired, but its redirects are INCOMPLETE: {message} "
                    "(the explorer row is gone, so nothing else would surface this)"
                )
            findings.append((status, slug, message))

        n_skipped = sum(1 for e in run["payload"].get("redirects") or [] if not e.get("target"))
        if n_skipped:
            if (run["payload"].get("catchAll") or {}).get("target"):
                note = (
                    f"{n_skipped} view(s) unresolved — the endpoint reports them `skipped`, so they get no rule of "
                    "their own and the catch-all (which constrains no params) matches them instead: those URLs do "
                    "NOT keep serving the explorer, they land on the target MDIM's DEFAULT view with the "
                    "explorer's own params still on the URL. Fine only if those views have no MDIM equivalent — "
                    "otherwise resolve them and rebuild before applying."
                )
            else:
                note = (
                    f"{n_skipped} view(s) unresolved — the endpoint reports them `skipped` and there is no "
                    "catch-all, so those URLs keep serving the explorer"
                )
            findings.append((WARN, slug, note))
        if not (run["payload"].get("catchAll") or {}).get("target"):
            findings.append((WARN, slug, "no catch-all — the bare explorer URL keeps serving the explorer"))

    if not args.no_references:
        for gate in (reference_gate, audit_freshness):
            status, message = gate(out, slugs)
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
