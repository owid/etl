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

Usage:
    .venv/bin/python .claude/skills/map-charts-to-mdim/scripts/apply_redirects.py \
        --mapping ai/<name>-charts-mdim-mapping [--execute] [--unpublish]
"""

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV
from etl.http import session as http_session

TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")


def short_admin_host() -> str:
    return TAILSCALE_SUFFIX_RE.sub("", OWID_ENV.admin_api).rstrip("/").removesuffix("/api")


def load_mapping(path_arg: str) -> dict:
    path = Path(path_arg)
    if path.is_dir():
        path = path / "mapping.json"
    if not path.exists():
        raise SystemExit(f"Not found: {path}. Run extract_and_match.py first.")
    return json.loads(path.read_text())


def existing_mdim_redirects(sources: tuple[str, ...]) -> dict[str, dict]:
    if not sources:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT source, multiDimId, viewConfigId FROM multi_dim_redirects WHERE source IN %(s)s",
        params={"s": sources},
    )
    return {r["source"]: r for r in df.to_dict("records")}


def chain_conflicts(sources: tuple[str, ...], slugs: tuple[str, ...]) -> dict[str, list[str]]:
    """Fresh chain checks (the DB may have changed since the proposal was written)."""
    reasons: dict[str, list[str]] = defaultdict(list)
    if not sources:
        return reasons
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
    args = ap.parse_args()

    if args.unpublish and not args.execute:
        raise SystemExit("--unpublish only makes sense together with --execute.")

    mapping = load_mapping(args.mapping)
    redirects = mapping.get("redirects", [])
    if not redirects:
        print("mapping.json has no proposed redirects — nothing to do.")
        return 0

    print(f"Target admin: {short_admin_host()}   mode: {'EXECUTE' if args.execute else 'AUDIT (dry-run)'}")
    print(f"Proposed redirects: {len(redirects)}")
    if args.execute:
        print(
            f"note: each created redirect triggers a static build ({len(redirects)} builds; the deploy queue coalesces)."
        )

    sources = tuple(r["source"] for r in redirects)
    slugs = tuple(r["chart"]["slug"] for r in redirects)
    existing = existing_mdim_redirects(sources)
    chains = chain_conflicts(sources, slugs)

    rows = []
    to_create = []
    for r in redirects:
        source, t = r["source"], r["target"]
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

    bad = [r for r in rows if r[2] in ("DIFFERS", "CONFLICT", "ERROR", "CHAIN", "BAD_VIEW")]

    if args.execute:
        created_or_existing = [r for r in redirects if r.get("_created")] + [
            r for r in redirects if existing.get(r["source"]) is not None
            and int(existing[r["source"]]["multiDimId"]) == r["target"]["multiDimId"]
            and existing[r["source"]]["viewConfigId"] == r["target"]["viewConfigId"]
        ]  # fmt: skip
        if created_or_existing:
            print(
                "\nSource charts now redirected — they should be UNPUBLISHED so the live page doesn't shadow the redirect:"
            )
            for r in created_or_existing:
                print(
                    f"  {r['chart']['id']:>6}  {r['chart']['slug']}  ({short_admin_host()}/admin/charts/{r['chart']['id']}/edit)"
                )
            if args.unpublish:
                expected = f"unpublish {len(created_or_existing)} charts"
                answer = input(f"\nType '{expected}' to unpublish them now: ").strip()
                if answer != expected:
                    print("Confirmation did not match — NOT unpublishing.")
                    return 1
                api = AdminAPI(OWID_ENV)
                for r in created_or_existing:
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
                        print(f"  ERROR unpublishing {chart_id} ({r['chart']['slug']}): {str(e)[:100]}")
            else:
                print("(re-run with --unpublish to unpublish them, or do it in the admin)")
    elif not bad:
        print(f"\nDry-run clean: {sum(1 for r in rows if r[2] == 'CREATE')} redirect(s) would be created. "
              "Re-run with --execute once the user has approved.")  # fmt: skip

    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
