"""Show where each chart being redirected is linked or embedded, and what to replace it with.

READ-ONLY. The surface sweep itself lives in the `find-chart-references` skill — this script
is the redirect-specific consumer: it runs that sweep for the proposal's source charts,
then adds what only this workflow knows, namely the URL each reference should become.

Severity, derived from the sweep's `kind`:
  RED    embed — the redirect does NOT fix it. The surface holds the chart by id or
         slug and renders its config directly, so it breaks when the source chart is
         unpublished (which the apply CLI always does). Migrate before applying.
  YELLOW link (the 301 covers it, but the href should be updated so readers don't
         take an extra hop) or render (a key-chart slot: the topic page loses the
         chart, so re-tag the MDIM — a follow-up, not a breakage).
  INFO   the referencing page is unpublished or a draft.

Replacement URLs merge each reference's own query string over the view's dimensions,
which is what grapher's redirect handler does (functions/_common/redirectTools.ts).
That merge is also a hazard: a link carrying ?metric=… overrides an MDIM dimension of
the same name and lands the reader on the wrong view. Those collisions are flagged.

Usage:
    ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
        .claude/skills/map-charts-to-mdim/scripts/audit_references.py \
        --mapping ai/<name>-charts-mdim-mapping
"""

import argparse
import csv
import json
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qsl, urlencode

from etl.config import OWID_ENV

FIND_REFERENCES = Path(__file__).resolve().parents[2] / "find-chart-references" / "scripts" / "find_references.py"

RED, YELLOW, INFO = "RED", "YELLOW", "INFO"
# Staging admin hosts carry a tailscale suffix that is noise in a link handed to a human.
TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")

REFERENCE_COLUMNS = [
    "severity", "surface", "kind", "source_chart_slug", "where", "where_url", "context",
    "old_url", "replacement_url", "param_collisions", "fix",
]  # fmt: skip

FIXES = {
    "gdoc": "edit the article block to embed the MDIM view",
    "gdoc (url link)": "update the href in the article",
    "explorer": "repoint the explorer at the MDIM indicators, or retire the explorer",
    "narrative chart": "replace it: create a new one from the MDIM view (parentChartConfigId = "
    "that view's config_id), move the article references, then delete the old — see SKILL.md",
    "data insight": "update the data insight's grapher-url",
    "static viz": "regenerate the static visualization against the MDIM view",
    "key chart": "re-tag the MDIM so the topic page keeps a key chart",
    "wordpress": "update the link in the WordPress post",
}
LINK_FIX = "update the href"
# Link-kind references whose href is generated rather than authored — there is nothing to
# hand-edit, so the generic "update the href" would send the operator looking for a field
# that doesn't exist.
GENERATED_LINK_FIXES = {
    "narrative chart": 'nothing to edit — the generated "Explore the data" link follows the redirect; '
    "check the param collisions column",
}


def load_redirects(path_arg: str) -> list[dict]:
    """Proposed redirects, plus the charts already redirected at proposal time.

    Both sets end the same way — the source chart unpublished, by the CLI for the proposed
    rows and by hand for the already-redirected ones — so both need their embeds audited
    first. Leaving `already_done` out would silently exempt exactly the charts a human is
    told to unpublish manually.
    """
    path = Path(path_arg)
    if path.is_dir():
        path = path / "mapping.json"
    if not path.exists():
        raise SystemExit(f"Not found: {path}. Run extract_and_match.py first.")
    mapping = json.loads(path.read_text())
    redirects = mapping.get("redirects", []) + mapping.get("already_done", [])
    if not redirects:
        raise SystemExit(f"{path} has no proposed or already-applied redirects to audit.")
    return redirects


def run_find_references(redirects: list[dict]) -> tuple[list[dict], list[str]]:
    """Delegate the surface sweep to the find-chart-references skill.

    Returns its findings and the surfaces it could not sweep. The sweep fails open on
    optional surfaces (a legacy table that is absent, a subject that does not resolve), so
    a run that skipped one returns fewer references and no error — indistinguishable from a
    clean result unless the gaps travel with the findings into this audit's own report.
    """
    if not FIND_REFERENCES.exists():
        raise SystemExit(f"Missing {FIND_REFERENCES} — the find-chart-references skill provides the surface sweep.")
    slugs = ",".join(r["chart"]["slug"] for r in redirects)
    with tempfile.NamedTemporaryFile("r", suffix=".json", delete=False) as tmp:
        out_path = tmp.name
    with tempfile.NamedTemporaryFile("r", suffix=".json", delete=False) as tmp:
        gaps_path = tmp.name
    try:
        cmd = [sys.executable, str(FIND_REFERENCES), "--chart-slugs", slugs]
        cmd += ["--json", out_path, "--gaps-json", gaps_path]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise SystemExit(f"find_references.py failed:\n{proc.stdout}\n{proc.stderr}")
        print(proc.stdout.rstrip())
        gaps_raw = Path(gaps_path).read_text().strip()
        return json.loads(Path(out_path).read_text()), (json.loads(gaps_raw) if gaps_raw else [])
    finally:
        Path(out_path).unlink(missing_ok=True)
        Path(gaps_path).unlink(missing_ok=True)


def replacement_url(r: dict, query_string: str, host: str) -> tuple[str, list[str]]:
    """Target URL for a reference, plus any params that would clobber a view dimension."""
    dims = dict(r["target"]["dimensions"])
    extra = dict(parse_qsl(query_string.lstrip("?"), keep_blank_values=True)) if query_string else {}
    collisions = sorted(k for k in extra if k in dims)
    merged = {**dims, **extra}  # reference params win, mirroring grapher's own merge
    return f"{host}/grapher/{r['target']['mdimSlug']}?{urlencode(sorted(merged.items()))}", collisions


def write_csv(out: Path, findings: list[dict]) -> Path:
    path = out / "references.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=REFERENCE_COLUMNS)
        w.writeheader()
        for row in findings:
            w.writerow({k: row.get(k, "") for k in REFERENCE_COLUMNS})
    return path


def write_markdown(out: Path, findings: list[dict], redirects: list[dict], host: str, gaps: list[str]) -> Path:
    path = out / "references.md"
    red = [f for f in findings if f["severity"] == RED]
    yellow = [f for f in findings if f["severity"] == YELLOW]
    info = [f for f in findings if f["severity"] == INFO]

    lines = [
        "# What references the charts being redirected",
        "",
        f"{len(redirects)} chart(s) heading for unpublishing — proposed redirects, plus charts already "
        f"redirected whose source chart is still published. **{len(red)} reference(s) need manual work** "
        f"— a redirect does not fix them. {len(yellow)} more are hyperlinks the 301 covers but that "
        "should be updated.",
        "",
        "Replacement URLs merge each reference's own query string over the MDIM view's dimensions, "
        "the same way grapher's redirect handler does.",
        "",
    ]
    sections = [
        ("🔴 Needs manual migration", red, "These embed or resolve the chart directly and break when it is unpublished."),
        ("🟡 Hyperlinks worth updating", yellow, "The 301 handles these; updating the href avoids an extra hop."),
        ("ℹ️ Unpublished / draft", info, "No reader impact — listed for completeness."),
    ]  # fmt: skip
    for label, group, blurb in sections:
        if not group:
            continue
        lines += [f"## {label} ({len(group)})", "", blurb, ""]
        by_surface = defaultdict(list)
        for f in group:
            by_surface[f["surface"]].append(f)
        for surface in sorted(by_surface):
            lines += [f"### {surface} ({len(by_surface[surface])})", ""]
            for f in by_surface[surface]:
                where = f"[{f['where']}]({f['where_url']})" if f["where_url"] else f["where"]
                lines.append(f"- **{f['source_chart_slug']}** in {where} — {f['context']}")
                lines.append(f"    - now: {f['old_url']}")
                lines.append(f"    - should be: {f['replacement_url']}")
                if f["param_collisions"]:
                    lines.append(
                        f"    - ⚠️ query params `{f['param_collisions']}` collide with the view's dimensions "
                        "and will override them — set the dimension explicitly or drop the param"
                    )
                lines.append(f"    - fix: {f['fix']}")
            lines.append("")

    # Nothing in this audit is applied by running it, and its coverage is not total — so it
    # closes by separating what someone must act on from what nobody has checked, rather
    # than leaving a reader to infer either from the sections above.
    handed_off = (
        f"**Handed off** — {len(red)} reference(s) under *Needs manual migration* above. Each names the "
        "page or surface that holds it and the replacement URL to put there; whoever owns that page has "
        "to make the edit, because unpublishing the chart breaks it and the redirect does not cover it."
        if red
        else "**Handed off** — nothing. No reference needs manual migration before the charts are unpublished."
    )
    proposed = (
        f"**Proposed** — {len(yellow)} hyperlink(s) under *worth updating* above. The 301 keeps them working, "
        "so updating each href is a call someone still has to make, not a blocker."
        if yellow
        else "**Proposed** — nothing. No hyperlink updates are pending a decision."
    )
    unverified = (
        "**Unverified** — this audit does not cover non-ETL explorer TSVs, data insights that store the "
        "reference somewhere other than the surfaces swept here, or charts nested inside layout containers; "
        "see the `find-chart-references` skill for the full surface catalog and its known gaps. "
        f"{len(info)} unpublished or draft reference(s) were found and listed but not graded for reader impact. "
        "Whether the redirects themselves apply cleanly is checked by `preflight.py`, not here."
    )
    lines += ["## What's still open", "", handed_off, "", proposed, "", unverified, ""]
    # Gaps the sweep hit at RUN time, as opposed to the standing ones named above. Silence
    # here would read as "everything was checked", which is the one wrong signal this
    # section can send — so they are listed individually, not folded into the prose.
    if gaps:
        lines += [
            f"This run also skipped {len(gaps)} surface(s) or subject(s) outright — an empty result for these "
            "means UNKNOWN, not that nothing references them:",
            "",
            *[f"- {g}" for g in gaps],
            "",
        ]
    path.write_text("\n".join(lines))
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit what links or embeds the charts in a redirect proposal.")
    ap.add_argument("--mapping", required=True, help="mapping.json path, or the folder containing it")
    ap.add_argument("--host", default=None, help="Base URL for links (default: the DB environment's site)")
    args = ap.parse_args()

    mapping_dir = Path(args.mapping)
    if not mapping_dir.is_dir():
        mapping_dir = mapping_dir.parent
    host = (args.host or OWID_ENV.site or "https://ourworldindata.org").rstrip("/")
    admin = TAILSCALE_SUFFIX_RE.sub("", (OWID_ENV.admin_site or "https://admin.owid.io/admin").rstrip("/"))

    redirects = load_redirects(args.mapping)
    by_chart_id = {r["chart"]["id"]: r for r in redirects}

    raw, gaps = run_find_references(redirects)

    findings = []
    for ref in raw:
        r = by_chart_id.get(ref["subject_id"])
        if r is None:
            continue
        qs = ref["query_string"]
        new_url, collisions = replacement_url(r, qs, host)
        # Only an embed is broken by the redirect: it renders the chart's own config.
        severity = INFO if not ref["published"] else (RED if ref["kind"] == "embed" else YELLOW)
        if ref["kind"] == "link":
            fix = GENERATED_LINK_FIXES.get(ref["surface"], LINK_FIX)
        else:
            fix = FIXES.get(ref["surface"], "migrate this reference by hand")
        if ref["surface"] == "narrative chart":
            # The admin's create page is deep-linkable to the parent view, and the view is
            # the one this chart is being redirected to — so hand over the ready-made URL
            # rather than the id to look up.
            fix += f" — create the replacement at {admin}/narrative-charts/create?type=multiDim&chartConfigId={r['target']['viewConfigId']}"
        findings.append(
            {
                "severity": severity,
                "surface": ref["surface"],
                "kind": ref["kind"],
                "source_chart_slug": ref["subject"],
                "where": ref["where"],
                "where_url": f"{host}{ref['where_path']}" if ref["where_path"] else "",
                "context": ref["context"] + (f' — "{ref["text"][:60]}"' if ref["text"] else ""),
                "old_url": f"{host}/grapher/{ref['subject']}" + (f"?{qs.lstrip('?')}" if qs else ""),
                "replacement_url": new_url,
                "param_collisions": ",".join(collisions),
                "fix": fix,
            }
        )

    findings.sort(key=lambda f: ({RED: 0, YELLOW: 1, INFO: 2}[f["severity"]], f["surface"], f["source_chart_slug"]))

    csv_path = write_csv(mapping_dir, findings)
    md_path = write_markdown(mapping_dir, findings, redirects, host, gaps)

    counts: dict[str, int] = defaultdict(int)
    for f in findings:
        counts[f["severity"]] += 1
    print(f"\nreferences: {len(findings)}  (needs manual work: {counts[RED]} | "
          f"hyperlinks to update: {counts[YELLOW]} | unpublished: {counts[INFO]})")  # fmt: skip
    if gaps:
        print(f"  {len(gaps)} surface(s)/subject(s) were NOT swept — see 'What's still open' in the report.")
    collisions = [f for f in findings if f["param_collisions"]]
    if collisions:
        print(f"\n⚠️  {len(collisions)} reference(s) carry query params that collide with the view's dimensions:")
        for f in collisions:
            print(f"  {f['where']}: {f['param_collisions']} (would override the target view)")

    print(f"\n-> {csv_path}")
    print(f"-> {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
