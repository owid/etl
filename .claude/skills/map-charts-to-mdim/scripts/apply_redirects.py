"""Apply the chart → MDIM view redirects proposed by extract_and_match.py.

GATED: dry-run by default — it audits and prints what would happen, never
mutating anything. `--execute` POSTs each redirect to the grapher admin API
(`POST /multi-dims/:id/redirects`), which writes the `multi_dim_redirects` table
and **triggers a static build per redirect**. `--unpublish` (only together with
`--execute`, plus a typed confirmation) additionally unpublishes the source
charts — the destructive half; a redirect alone is cheap to reverse (there's a
DELETE endpoint), an unpublish removes the chart's page and embeds.

Claude: never pass `--execute` or `--unpublish` unless the user explicitly asked
for it in this conversation.

Targets whichever env OWID_ENV resolves to. Redirect tables are per-environment
(staging does NOT sync to production), so production redirects require running
this against production admin creds.

`--decisions` takes the review export from build_review.py (⬇ JSON / ⬇ CSV) and
excludes every chart the reviewer flagged — pass it whenever a review happened.

Usage:
    .venv/bin/python .claude/skills/map-charts-to-mdim/scripts/apply_redirects.py \
        --mapping ai/<name>-charts-mdim-mapping \
        [--decisions ai/<name>-charts-mdim-mapping/<name>_chart_mdim_review.json] \
        [--execute] [--unpublish]
"""

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV
from etl.http import session as http_session

TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")

# References a redirect does NOT fix: these surfaces embed the chart directly (by id/slug)
# and break if it is unpublished. Same set as add-gdp-scatter's redirect_to_scatter.py.
MANUAL_REF_KEYS = ("explorers", "narrativeCharts", "dataInsights", "staticViz")


def short_admin_host() -> str:
    return TAILSCALE_SUFFIX_RE.sub("", OWID_ENV.admin_api).rstrip("/").removesuffix("/api")


def load_mapping(path_arg: str) -> dict:
    path = Path(path_arg)
    if path.is_dir():
        path = path / "mapping.json"
    if not path.exists():
        raise SystemExit(f"Not found: {path}. Run extract_and_match.py first.")
    return json.loads(path.read_text())


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


def existing_mdim_redirects(sources: tuple[str, ...]) -> dict[str, dict]:
    if not sources:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT source, multiDimId, viewConfigId FROM multi_dim_redirects WHERE source IN %(s)s",
        params={"s": sources},
    )
    return {r["source"]: r for r in df.to_dict("records")}


def chain_conflicts(redirects: list[dict]) -> dict[str, list[str]]:
    """Fresh chain checks, mirroring extract_and_match.check_conflicts (the DB may have changed since the proposal)."""
    reasons: dict[str, list[str]] = defaultdict(list)
    if not redirects:
        return reasons
    sources = tuple(r["source"] for r in redirects)
    slugs = tuple(r["chart"]["slug"] for r in redirects)

    site = OWID_ENV.read_sql(
        "SELECT source, target FROM redirects WHERE source IN %(s)s OR target IN %(s)s", params={"s": sources}
    )
    for r in site.to_dict("records"):
        if r["source"] in sources:
            reasons[r["source"]].append(f"already a site redirect source -> {r['target']}")
        if r["target"] in sources:
            reasons[r["target"]].append(f"chain: site redirect {r['source']} points at this chart")

    incoming = OWID_ENV.read_sql(
        "SELECT csr.slug AS old_slug, cc.slug AS chart_slug FROM chart_slug_redirects csr "
        "JOIN charts c ON c.id = csr.chart_id JOIN chart_configs cc ON cc.id = c.configId "
        "WHERE cc.slug IN %(s)s",
        params={"s": slugs},
    )
    for r in incoming.to_dict("records"):
        reasons[f"/grapher/{r['chart_slug']}"].append(f"chain: incoming chart_slug_redirect from '{r['old_slug']}'")

    own_old = OWID_ENV.read_sql("SELECT slug FROM chart_slug_redirects WHERE slug IN %(s)s", params={"s": slugs})
    for slug in own_old["slug"]:
        reasons[f"/grapher/{slug}"].append("chart slug is itself an old slug in chart_slug_redirects")

    redirected_mdim_slugs = set(
        OWID_ENV.read_sql(
            "SELECT DISTINCT mdp.slug FROM multi_dim_redirects mdr "
            "JOIN multi_dim_data_pages mdp ON mdp.id = mdr.multiDimId WHERE mdp.slug IS NOT NULL"
        )["slug"]
    )

    # Target side, once per targeted MDIM: its own /grapher/<slug> must not be a redirect source.
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

    for r in redirects:
        source, slug, t = r["source"], r["chart"]["slug"], r["target"]
        if slug in redirected_mdim_slugs:
            reasons[source].append("chart slug equals an MDIM slug that is already a redirect target")
        if slug == t["mdimSlug"]:
            reasons[source].append("self-redirect: chart slug equals the target MDIM slug")
        if f"/grapher/{t['mdimSlug']}" in bad_targets:
            reasons[source].append(f"target /grapher/{t['mdimSlug']} is itself a redirect source")
    return reasons


def classify_post_error(exc: Exception) -> tuple[str, str]:
    msg = str(getattr(getattr(exc, "response", None), "text", "") or exc)
    low = msg.lower()
    if "chain" in low:
        return "CHAIN", msg[:120]
    if "not found for this multi-dim" in low:
        return "BAD_VIEW", "view config not in current MDIM config — re-run extract_and_match.py"
    if "already exists" in low:
        return "EXISTS", msg[:120]
    return "ERROR", msg[:120]


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply proposed chart → MDIM redirects (dry-run by default).")
    ap.add_argument("--mapping", required=True, help="mapping.json path, or the folder containing it")
    ap.add_argument("--execute", action="store_true", help="actually create the redirects (otherwise audit only)")
    ap.add_argument("--unpublish", action="store_true",
                    help="after creating redirects, unpublish the source charts (requires --execute + typed confirmation)")  # fmt: skip
    ap.add_argument("--decisions",
                    help="review export from build_review.py (⬇ JSON / ⬇ CSV) — charts the reviewer flagged are excluded")  # fmt: skip
    args = ap.parse_args()

    if args.unpublish and not args.execute:
        raise SystemExit("--unpublish only makes sense together with --execute.")

    mapping = load_mapping(args.mapping)
    redirects = mapping.get("redirects", [])
    # Redirects that already existed at proposal time: no redirect to create, but the source
    # charts are still published (the extractor only selects published charts), so they still
    # shadow their redirect and must be included in the unpublish step.
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
    elif args.execute:
        print("note: no --decisions file — flags made in the review HTML are NOT consumed. Export them "
              "(⬇ JSON) and pass --decisions, or fold them into overrides.csv and re-run extract_and_match.py.")  # fmt: skip

    if not redirects and not already_done:
        print("mapping.json has no proposed redirects — nothing to do.")
        return 0

    print(f"Target admin: {short_admin_host()}   mode: {'EXECUTE' if args.execute else 'AUDIT (dry-run)'}")
    print(f"Proposed redirects: {len(redirects)}   already redirected at proposal time: {len(already_done)}")
    if args.execute:
        print(
            f"note: each created redirect triggers a static build ({len(redirects)} builds; the deploy queue coalesces)."
        )

    sources = tuple(r["source"] for r in redirects + already_done)
    existing = existing_mdim_redirects(sources)
    chains = chain_conflicts(redirects)
    stale = stale_charts(redirects + already_done)

    rows = []
    to_create = []
    for r in redirects:
        source, t = r["source"], r["target"]
        if source in stale:
            rows.append((source, t["url"], "STALE", stale[source]))
            continue
        prior = existing.get(source)
        if prior is not None:
            if int(prior["multiDimId"]) == t["multiDimId"] and prior["viewConfigId"] == t["viewConfigId"]:
                rows.append((source, t["url"], "EXISTS", "same target — nothing to do"))
            else:
                rows.append((source, t["url"], "DIFFERS", f"already redirected to multiDimId={prior['multiDimId']} "
                                                          f"viewConfigId={prior['viewConfigId']}"))  # fmt: skip
        elif source in chains:
            rows.append((source, t["url"], "CONFLICT", "; ".join(chains[source])))
        else:
            rows.append((source, t["url"], "CREATE", ""))
            to_create.append(r)

    # Re-verify the proposal-time redirects still exist and still point at the proposed target.
    still_done = []
    for r in already_done:
        source, t = r["source"], r["target"]
        if source in stale:
            rows.append((source, t["url"], "STALE", stale[source]))
            continue
        prior = existing.get(source)
        if prior is not None and int(prior["multiDimId"]) == t["multiDimId"] and prior["viewConfigId"] == t["viewConfigId"]:  # fmt: skip
            rows.append((source, t["url"], "EXISTS", "already redirected at proposal time — chart still needs unpublishing"))  # fmt: skip
            still_done.append(r)
        elif prior is not None:
            rows.append((source, t["url"], "DIFFERS", f"already redirected to multiDimId={prior['multiDimId']} "
                                                      f"viewConfigId={prior['viewConfigId']}"))  # fmt: skip
        else:
            rows.append((source, t["url"], "GONE", "redirect existed at proposal time but is now missing — re-run extract_and_match.py"))  # fmt: skip

    if args.execute and to_create:
        api = AdminAPI(OWID_ENV)
        headers = api._headers()
        executed = []
        for r in to_create:
            source, t = r["source"], r["target"]
            try:
                resp = http_session.post(
                    f"{OWID_ENV.admin_api}/multi-dims/{t['multiDimId']}/redirects",
                    headers=headers,
                    json={"source": source, "viewConfigId": t["viewConfigId"]},
                )
                resp.raise_for_status()
                executed.append((source, t["url"], "CREATED", f"id={resp.json().get('redirect', {}).get('id')}"))
                r["_created"] = True
            except Exception as e:  # noqa: BLE001 - keep going, report per-row
                status, note = classify_post_error(e)
                executed.append((source, t["url"], status, note))
        rows = [row for row in rows if row[2] != "CREATE"] + executed

    width = max(len(r[0]) for r in rows) + 2
    print(f"\n{'source':<{width}} {'status':<9} note / target")
    print("-" * (width + 90))
    for source, url, status, note in sorted(rows, key=lambda x: (x[2], x[0])):
        print(f"{source:<{width}} {status:<9} {note or '-> ' + url}")

    bad = [r for r in rows if r[2] in ("DIFFERS", "CONFLICT", "ERROR", "CHAIN", "BAD_VIEW", "GONE", "STALE")]
    unpublish_failures = 0

    if args.execute:
        created_or_existing = [r for r in redirects if r.get("_created")] + [
            r for r in redirects if existing.get(r["source"]) is not None
            and int(existing[r["source"]]["multiDimId"]) == r["target"]["multiDimId"]
            and existing[r["source"]]["viewConfigId"] == r["target"]["viewConfigId"]
        ] + still_done  # fmt: skip
        if created_or_existing:
            print(
                "\nSource charts now redirected — they should be UNPUBLISHED so the live page doesn't shadow the redirect:"
            )
            for r in created_or_existing:
                print(
                    # short_admin_host() already ends in /admin (it only strips the /api suffix).
                    f"  {r['chart']['id']:>6}  {r['chart']['slug']}  ({short_admin_host()}/charts/{r['chart']['id']}/edit)"
                )
            if args.unpublish:
                api = AdminAPI(OWID_ENV)
                # A redirect only covers the /grapher/<slug> URL. Explorers, narrative charts,
                # data insights, and static viz reference the chart directly and break if it is
                # unpublished — audit and block those; they need manual migration first.
                to_unpublish, blocked = [], []
                for r in created_or_existing:
                    try:
                        refs = api.get_chart_references(r["chart"]["id"]).get("references", {})
                        manual = {k: len(refs[k]) for k in MANUAL_REF_KEYS if refs.get(k)}
                    except Exception as e:  # noqa: BLE001 - fail closed: can't verify -> don't unpublish
                        blocked.append((r, f"could not fetch chart references: {str(e)[:80]}"))
                        continue
                    if manual:
                        blocked.append((r, "directly referenced by " + ", ".join(f"{k} ({n})" for k, n in manual.items())))  # fmt: skip
                    else:
                        to_unpublish.append(r)
                if blocked:
                    unpublish_failures += len(blocked)
                    print("\nBLOCKED from unpublishing (a redirect does not update these references — migrate them manually first):")  # fmt: skip
                    for r, why in blocked:
                        print(f"  {r['chart']['id']:>6}  {r['chart']['slug']}  {why}")
                if to_unpublish:
                    expected = f"unpublish {len(to_unpublish)} charts"
                    answer = input(f"\nType '{expected}' to unpublish them now: ").strip()
                    if answer != expected:
                        print("Confirmation did not match — NOT unpublishing.")
                        return 1
                    for r in to_unpublish:
                        chart_id = r["chart"]["id"]
                        try:
                            cfg = api.get_chart_config(chart_id)
                            if cfg.get("isPublished"):
                                cfg["isPublished"] = False
                                api.update_chart(chart_id, cfg)
                                print(f"  unpublished {chart_id} ({r['chart']['slug']})")
                            else:
                                print(f"  {chart_id} ({r['chart']['slug']}) already unpublished")
                        except Exception as e:  # noqa: BLE001 - keep going, report per-row
                            unpublish_failures += 1
                            print(f"  ERROR unpublishing {chart_id} ({r['chart']['slug']}): {str(e)[:100]}")
                if unpublish_failures:
                    print(f"\n{unpublish_failures} chart(s) could NOT be unpublished — their live pages still shadow the redirects.")  # fmt: skip
            else:
                print("(re-run with --unpublish to unpublish them, or do it in the admin)")
    elif not bad:
        print(f"\nDry-run clean: {sum(1 for r in rows if r[2] == 'CREATE')} redirect(s) would be created. "
              "Re-run with --execute once the user has approved.")  # fmt: skip

    return 1 if bad or unpublish_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
