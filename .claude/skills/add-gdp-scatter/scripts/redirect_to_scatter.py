"""Redirect old "X vs. GDP per capita" scatter charts to the scatter tab of the
new target charts (part 2 of the add-gdp-scatter workflow).

Reads a JSON list of `{grapher_url, target_chart_url}` from stdin (public
ourworldindata.org/grapher/<slug> URLs). For each pair it audits what references
the OLD chart (admin "Refs" tab) and — with `--apply` — creates a site redirect
`/grapher/<src> -> /grapher/<tgt>?tab=scatter` and unpublishes the old chart.

Report-first: without `--apply` it only audits and never mutates.

Mechanism notes:
- Uses the site `redirects` table (supports a query string in the target), NOT
  chart_slug_redirects (slug->id only, no `?tab=`).
- Targets whichever env OWID_ENV resolves to (staging on a feature branch). The
  site `redirects` table is per-staging and does NOT sync to production, so prod
  redirects are a separate manual step after the scatter views ship to prod.
"""

import argparse
import json
import re
import sys

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV
from etl.http import session as http_session

SLUG_RE = re.compile(r"/grapher/([^/?#]+)")
TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")

# Query string appended to the redirect target: open the scatter tab on the latest year.
TARGET_QUERY = "tab=scatter&time=latest"

# Reference categories a redirect alone does NOT fix — flag for manual follow-up.
MANUAL_REF_KEYS = ["explorers", "narrativeCharts", "dataInsights", "staticViz"]
ALL_REF_KEYS = ["postsWordpress", "postsGdocs", *MANUAL_REF_KEYS]
REF_LABEL = {
    "postsWordpress": "wp",
    "postsGdocs": "gdoc",
    "explorers": "expl",
    "narrativeCharts": "narr",
    "dataInsights": "ins",
    "staticViz": "sviz",
}


def short_admin_host() -> str:
    return TAILSCALE_SUFFIX_RE.sub("", OWID_ENV.admin_api).rstrip("/").removesuffix("/api")


def slug_from_url(url: str) -> str:
    m = SLUG_RE.search(url)
    if not m:
        raise ValueError(f"Could not extract /grapher/<slug> from {url!r}")
    return m.group(1)


def chart_id_for_slug(slug: str) -> int | None:
    df = OWID_ENV.read_sql(
        "SELECT c.id FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE cc.slug = %(s)s",
        params={"s": slug},
    )
    return int(df.iloc[0]["id"]) if not df.empty else None


def existing_redirects() -> dict[str, dict]:
    """Map of existing site-redirect source -> {id, target}."""
    resp = http_session.get(f"{OWID_ENV.admin_api}/site-redirects.json", headers=AdminAPI(OWID_ENV)._headers())
    resp.raise_for_status()
    return {r["source"]: {"id": r["id"], "target": r["target"]} for r in resp.json().get("redirects", [])}


def ref_counts(references: dict) -> dict[str, int]:
    return {k: len(references.get(k) or []) for k in ALL_REF_KEYS}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="create redirects + unpublish sources (otherwise audit only)")
    args = parser.parse_args()

    payload = json.load(sys.stdin)
    if not isinstance(payload, list):
        print("ERROR: stdin must be a JSON list", file=sys.stderr)
        return 2

    api = AdminAPI(OWID_ENV)
    host = short_admin_host()
    print(f"Target admin: {host}   mode: {'APPLY' if args.apply else 'AUDIT (dry-run)'}")

    existing = existing_redirects() if args.apply else {}

    audit_rows = []
    action_rows = []

    for row in payload:
        try:
            src_slug = slug_from_url(row["grapher_url"])
            tgt_slug = slug_from_url(row["target_chart_url"])
        except (KeyError, ValueError) as e:
            audit_rows.append(("-", "-", "-", "-", f"ERROR: {e}"))
            continue

        src_id = chart_id_for_slug(src_slug)
        tgt_id = chart_id_for_slug(tgt_slug)
        if src_id is None or tgt_id is None:
            audit_rows.append((src_slug, src_id, tgt_id, "-", "ERROR: slug not resolved"))
            continue

        # References audit on the SOURCE chart.
        refs = api.get_chart_references(src_id).get("references", {})
        counts = ref_counts(refs)
        manual = sum(counts[k] for k in MANUAL_REF_KEYS) > 0
        counts_str = " ".join(f"{REF_LABEL[k]}={counts[k]}" for k in ALL_REF_KEYS)
        audit_rows.append((src_slug, src_id, tgt_id, "MANUAL" if manual else "", counts_str))

        if not args.apply:
            continue

        # Pre-flight guards on the TARGET.
        tgt_cfg = api.get_chart_config(tgt_id)
        if "ScatterPlot" not in (tgt_cfg.get("chartTypes") or []):
            action_rows.append((f"{src_id}->{tgt_id}", src_slug, "SKIPPED", "target has no ScatterPlot tab (wrong env?)"))
            continue
        if not tgt_cfg.get("isPublished"):
            action_rows.append((f"{src_id}->{tgt_id}", src_slug, "SKIPPED", "target not published"))
            continue

        source = f"/grapher/{src_slug}"
        target = f"/grapher/{tgt_slug}?{TARGET_QUERY}"

        # Create the site redirect. If one already exists for this source: leave it
        # if the target already matches, otherwise replace it (delete + recreate,
        # since there's no update endpoint).
        prior = existing.get(source)
        try:
            if prior and prior["target"] == target:
                status, note = "EXISTS", f"-> {target}"
            else:
                if prior:
                    api.delete_site_redirect(prior["id"])
                api.create_site_redirect(source, target)
                status, note = ("UPDATED" if prior else "CREATED"), f"-> {target}"
        except Exception as e:
            msg = str(getattr(getattr(e, "response", None), "text", "") or e)
            if "chained" in msg.lower():
                status, note = "CHAINED", msg[:100]
            elif "already exists" in msg.lower():
                status, note = "EXISTS", f"-> {target}"
            else:
                status, note = "ERROR", msg[:120]

        # Unpublish the source chart.
        unpub = ""
        if status in ("CREATED", "UPDATED", "EXISTS"):
            try:
                src_cfg = api.get_chart_config(src_id)
                if src_cfg.get("isPublished"):
                    src_cfg["isPublished"] = False
                    api.update_chart(src_id, src_cfg)
                    unpub = "unpublished"
                else:
                    unpub = "already unpublished"
            except Exception as e:
                unpub = f"unpublish ERROR: {str(e)[:80]}"
        action_rows.append((f"{src_id}->{tgt_id}", src_slug, status, f"{note}  [{unpub}]" if unpub else note))

    # ---- REFERENCES AUDIT ----
    print("\nREFERENCES AUDIT (of the OLD source chart)")
    print(f"{'src_slug':<58} {'src':>5} {'tgt':>5} {'manual':>7}  counts")
    print("-" * 130)
    for s, sid, tid, m, c in audit_rows:
        print(f"{str(s):<58} {str(sid):>5} {str(tid):>5} {m:>7}  {c}")
    print("\n  manual = redirect alone won't fix: explorers / narrativeCharts / dataInsights / staticViz reference the old chart directly")

    # ---- REDIRECT ACTIONS ----
    if args.apply:
        print("\nREDIRECT ACTIONS")
        print(f"{'pair':>13}  {'src_slug':<58} {'status':<8} note")
        print("-" * 140)
        for pair, slug, status, note in action_rows:
            print(f"{pair:>13}  {slug:<58} {status:<8} {note}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
