"""Show where each explorer being redirected is linked or embedded, and where it will land.

READ-ONLY. The surface sweep lives in the `find-chart-references` skill — this script is the
redirect-specific consumer: it runs that sweep for N explorers, then resolves each
referencing URL through the redirect rules the mapping would create, so the report says not
just *what* points at the explorer but *which view the reader ends up on*.

Severity is derived from what actually breaks, not from the sweep's `kind`, because the
explorer timeline differs from the chart one in a way that matters:

  RED   an embedded explorer. The renderer fetches the explorer page and parses its HTML, so
        once the URL 302s to a grapher page the block renders nothing. It breaks the moment
        the redirect is CREATED — not later at unpublish, as with charts — because the
        explorer redirect is checked on every request, ahead of the page itself.
  RED   a site redirect pointing at the explorer: a blocker. The admin endpoint refuses such
        a source as a chain, and it caches per-source checks, so one row fails every entry.
  YELLOW a prose link (the 302 carries it, but the href should skip the hop) or a homepage
        explorer tile (a plain link with bake-time text: keeps working, but advertises a
        retired explorer).
  INFO  the referencing page is unpublished or a draft.

Any row can additionally be flagged ⚠️ when its params no longer resolve to the view the
author meant — see `redirect_rules.resolve_explorer_url`.

Usage:
    ENV_FILE=<prod creds> DATA_API_ENV=production .venv/bin/python \
        .claude/skills/map-explorer-to-mdim/scripts/audit_references.py \
        --mapping ai/<a>-mdim-mapping --mapping ai/<b>-mdim-mapping \
        --out ai/<combined>-redirects
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qsl

from etl.config import OWID_ENV

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "find-chart-references" / "scripts"))
from redirect_rules import build_source_rules, choice_values, resolve_explorer_url  # noqa: E402
from reference_report import (  # noqa: E402
    INFO,
    POST_TYPE_PATH,
    RED,
    TAILSCALE_SUFFIX_RE,
    YELLOW,
    archie_component,
    cell,
    deep_link,
    find_in_doc,
    page_type,
    public_page_url,
    run_sweep,
)

REFERENCE_COLUMNS = [
    "severity", "explorer", "surface", "component", "page_type", "kind", "where", "where_url",
    "doc_edit_url", "doc_preview_url", "find_in_doc", "context", "old_url",
    "match", "matched_view_id", "matched_target_mdim", "stale_params", "leftover_params",
    "replacement_url", "fix",
]  # fmt: skip

GDOC_SURFACES = ("gdoc", "gdoc (url link)", "data insight")

# What each ArchieML component does with the explorer decides whether the redirect breaks it.
# `chart`, `key-insights` and `front-matter` all resolve the explorer by fetching its page and
# parsing the HTML, so they render nothing once the URL redirects. `explorer-tiles` is the one
# EMBED-kind component that only builds an <a href>, so it survives — hence a component-level
# override rather than trusting `kind` alone.
COMPONENT_LABELS = {
    "chart": "Embedded explorers",
    "key-insights": "Embedded explorers (key insights)",
    "front-matter": "Front-matter explorer URLs",
    "span-link": "Text links",
    "explorer-tiles": "Homepage explorer tiles",
}
COMPONENT_ORDER = ("chart", "key-insights", "front-matter", "span-link", "explorer-tiles")
SURVIVES_REDIRECT = ("explorer-tiles",)

BREAKS_FIX = (
    "replace the block with the MDIM view **before** the redirect is created — the embed "
    "breaks the same instant, because it renders by fetching the explorer page"
)
COMPONENT_FIXES = {
    "chart": BREAKS_FIX,
    "key-insights": BREAKS_FIX,
    "front-matter": "update the grapher-url in the front matter before the redirect is created",
    "span-link": "update the href (the 302 covers it meanwhile)",
    "explorer-tiles": "re-point the tile at the MDIM: it keeps working, but advertises a retired explorer",
}
REDIRECT_FIX = (
    "BLOCKER — repoint or delete this site redirect first. Repoint unless it is a vanity path "
    "nothing links to: deleting a row whose source is a real URL turns that URL into a 404. "
    "Admin: /admin/site-redirects (delete) then POST /api/site-redirects/new with the MDIM target"
)


def load_mappings(paths: list[str]) -> dict[str, dict]:
    """{explorer slug -> {mapping, dir, rules, dim_names, choices}} for each --mapping dir."""
    out: dict[str, dict] = {}
    for raw in paths:
        d = Path(raw)
        if not d.is_dir():
            d = d.parent
        mapping_path = d / "mapping.json"
        if not mapping_path.exists():
            raise SystemExit(f"Not found: {mapping_path}. Run extract_views.py + build_mapping.py first.")
        mapping = json.loads(mapping_path.read_text())
        slug = mapping["explorer"]["slug"]
        if slug in out:
            raise SystemExit(f"Two --mapping dirs both describe explorer {slug!r}: {out[slug]['dir']} and {d}")
        dim_names, choices = choice_values(d)
        out[slug] = {
            "mapping": mapping,
            "dir": d,
            "rules": build_source_rules(mapping),
            "dim_names": dim_names,
            "choices": choices,
        }
    return out


def resolve_missing_slugs(runs: dict[str, dict]) -> None:
    """Fill in target MDIM slugs for runs extracted before they were recorded.

    A mapping built by an older extraction carries no `mdimSlug`, and without it every
    replacement URL would be `/grapher/`. Resolve by catalogPath rather than refusing to run,
    so an old proposal can still be audited.
    """
    missing = {r.catalog_path for run in runs.values() for r in run["rules"] if not r.mdim_slug and r.catalog_path}
    if not missing:
        return
    df = OWID_ENV.read_sql(
        "SELECT catalogPath, slug FROM multi_dim_data_pages WHERE catalogPath IN %(cps)s",
        params={"cps": tuple(sorted(missing))},
    )
    by_path = dict(zip(df["catalogPath"], df["slug"]))
    unknown = sorted(missing - set(by_path))
    if unknown:
        print(f"warning: no MDIM row for {unknown} — their replacement URLs will be incomplete")
    for run in runs.values():
        run["rules"] = [
            r if r.mdim_slug else type(r)(**{**r.__dict__, "mdim_slug": by_path.get(r.catalog_path, "")})
            for r in run["rules"]
        ]


def _where_url(ref: dict, host: str, admin: str) -> str:
    """Public URL of the referencing page, scrolled to the reference — "" when it has none."""
    ptype = page_type(ref)
    if ptype and POST_TYPE_PATH.get(ptype, "") is None:
        return ""  # fragment / homepage: no reader-facing URL exists
    return deep_link(ref["where_path"], ref.get("text") or "", host, admin) or public_page_url(
        ptype, ref["where"], host
    )


def severity_of(ref: dict, component: str) -> str:
    """RED / YELLOW / INFO from what the redirect actually does to this surface."""
    if ref["surface"] == "site redirect":
        return RED  # a blocker regardless of the referencing page's state
    if not ref["published"]:
        return INFO
    if component in SURVIVES_REDIRECT:
        return YELLOW
    return RED if ref["kind"] == "embed" else YELLOW


def build_rows(runs: dict[str, dict], raw: list[dict], host: str, admin: str) -> list[dict]:
    rows = []
    for ref in raw:
        run = runs.get(ref["subject"])
        if run is None:
            continue
        component = archie_component(ref) if ref["surface"] in GDOC_SURFACES else ref["surface"]
        query = dict(parse_qsl((ref["query_string"] or "").lstrip("?"), keep_blank_values=True))
        resolved = resolve_explorer_url(run["rules"], query, host, run["choices"], run["dim_names"])
        is_gdoc = ref["surface"] in GDOC_SURFACES and ref.get("surface_id")
        severity = severity_of(ref, component)

        if ref["surface"] == "site redirect":
            fix = REDIRECT_FIX
        else:
            fix = COMPONENT_FIXES.get(component, "migrate this reference by hand")
        if resolved.match.startswith("catch-all (stale") or resolved.match.startswith("catch-all (partial"):
            fix += " — ⚠️ and pick the view deliberately: its current params no longer select one"

        old_url = f"{host}/explorers/{ref['subject']}" + (
            f"?{(ref['query_string'] or '').lstrip('?')}" if ref["query_string"] else ""
        )
        rows.append(
            {
                "severity": severity,
                "explorer": ref["subject"],
                "surface": ref["surface"],
                "component": component,
                "page_type": page_type(ref) if is_gdoc else "",
                "kind": ref["kind"],
                "where": ref["where"],
                # No link at all for a page type with no public route (a fragment, the
                # homepage): `where_path` for those is "/" or empty, so a deep link would
                # point at the site root, which reads as a real destination and is not one.
                "where_url": _where_url(ref, host, admin),
                "doc_edit_url": f"https://docs.google.com/document/d/{ref['surface_id']}/edit" if is_gdoc else "",
                "doc_preview_url": f"{admin}/gdocs/{ref['surface_id']}/preview" if is_gdoc else "",
                "find_in_doc": find_in_doc(ref) if is_gdoc else "",
                "context": ref["context"],
                "old_url": old_url,
                "match": resolved.match,
                "matched_view_id": (resolved.rule.view_id if resolved.rule else "") or "",
                "matched_target_mdim": resolved.rule.mdim if resolved.rule else "",
                "stale_params": ", ".join(f"{k}={v}" for k, v in sorted(resolved.stale_params.items())),
                "leftover_params": ", ".join(sorted(resolved.leftover_params)),
                "replacement_url": resolved.url,
                "fix": fix,
            }
        )
    order = {RED: 0, YELLOW: 1, INFO: 2}
    rows.sort(key=lambda r: (order[r["severity"]], r["component"], r["explorer"], r["where"]))
    return rows


def write_manifest(out: Path, runs: dict[str, dict], rows: list[dict], gaps: list[str]) -> Path:
    """Record WHICH explorers were audited, and with what result.

    Necessary because a clean audit produces no rows: an explorer nothing references
    contributes nothing to references.csv, so a consumer deriving coverage from the rows alone
    cannot tell "audited, found nothing" from "never audited" — and would gate on it forever.
    The manifest is the positive record of the run.
    """
    per: dict[str, int] = {slug: 0 for slug in runs}
    for row in rows:
        per[row["explorer"]] = per.get(row["explorer"], 0) + 1
    manifest = {
        "explorers": sorted(runs),
        "referenceCounts": per,
        "total": len(rows),
        "gaps": gaps,
    }
    path = out / "references_manifest.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    return path


def write_csv(out: Path, rows: list[dict]) -> Path:
    path = out / "references.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=REFERENCE_COLUMNS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in REFERENCE_COLUMNS})
    return path


def gdoc_table(group: list[dict]) -> list[str]:
    """One table per fix: doc links, the search string, and where the reference lands."""
    by_fix: dict[str, list[dict]] = defaultdict(list)
    for row in group:
        by_fix[row["fix"]].append(row)
    lines: list[str] = []
    for fix in sorted(by_fix):
        if lines:
            lines.append("")
        lines += [f"Fix: {fix}", ""]
        lines += ["| Explorer | Where | Open | Find in doc | Lands on |", "|---|---|---|---|---|"]
        for row in by_fix[fix]:
            opens = " · ".join(
                part
                for part in (
                    f"[📄 doc]({row['doc_edit_url']})" if row["doc_edit_url"] else "",
                    f"[👁 preview]({row['doc_preview_url']})" if row["doc_preview_url"] else "",
                    f"[🔗 page]({row['where_url']})" if row["where_url"] else "",
                )
                if part
            )
            find = f"`{cell(row['find_in_doc'], 55, marker='')}`" if row["find_in_doc"] else "—"
            lands = row["match"]
            if row["replacement_url"]:
                label = cell(row["replacement_url"].split("/grapher/", 1)[-1], 52)
                lands = f"[`{label}`]({row['replacement_url']})<br>_{row['match']}_"
            if row["stale_params"]:
                lands += f"<br>⚠️ dead choice: `{cell(row['stale_params'], 60)}`"
            where = cell(row["where"] or "(no slug)", 40) + (f" _{row['page_type']}_" if row["page_type"] else "")
            lines.append(f"| `{cell(row['explorer'], 34)}` | {where} | {opens} | {find} | {lands} |")
    return lines


def rollup(runs: dict[str, dict], rows: list[dict]) -> list[str]:
    """Per-explorer go/no-go: is this one safe to redirect yet?"""
    lines = [
        "| Explorer | refs | 🔴 breaks | 🟡 links | → view | → catch-all | ⚠️ wrong view | inbound redirects | verdict |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for slug in sorted(runs):
        mine = [r for r in rows if r["explorer"] == slug]
        breaks = [r for r in mine if r["severity"] == RED and r["surface"] != "site redirect"]
        inbound = [r for r in mine if r["surface"] == "site redirect"]
        links = [r for r in mine if r["severity"] == YELLOW]
        to_view = [r for r in mine if r["match"] == "view"]
        to_catch = [r for r in mine if r["match"].startswith("catch-all")]
        wrong = [r for r in mine if r["stale_params"] or "partial" in r["match"]]
        if inbound:
            verdict = "⛔ blocked (inbound redirect)"
        elif breaks:
            verdict = f"migrate {len(breaks)} embed(s) first"
        elif wrong:
            verdict = "review the ⚠️ rows, then go"
        else:
            verdict = "✅ no blockers"
        lines.append(
            f"| `{slug}` | {len(mine)} | {len(breaks)} | {len(links)} | {len(to_view)} | "
            f"{len(to_catch)} | {len(wrong)} | {len(inbound)} | {verdict} |"
        )
    return lines


def write_markdown(out: Path, runs: dict[str, dict], rows: list[dict], gaps: list[str]) -> Path:
    path = out / "references.md"
    blockers = [r for r in rows if r["surface"] == "site redirect"]
    breaks = [r for r in rows if r["severity"] == RED and r["surface"] != "site redirect"]
    links = [r for r in rows if r["severity"] == YELLOW]
    drafts = [r for r in rows if r["severity"] == INFO]
    wrong = [r for r in rows if r["stale_params"] or "partial" in r["match"]]

    lines = [
        "# What references the explorers being redirected",
        "",
        f"{len(runs)} explorer(s) · {len(rows)} reference(s). **{len(breaks)} break the moment the "
        f"redirect exists**, {len(links)} are links the 302 covers, {len(wrong)} would land on the "
        "wrong view.",
        "",
        "> [!WARNING]",
        "> **Creating the redirect darkens a live explorer immediately.** An explorer redirect is "
        "checked on *every* request to `/explorers/*`, before the explorer page is served — so it "
        "beats the baked explorer and any site redirect, and fires while the explorer is still "
        "published. Everything that *embeds* the explorer breaks in that same instant, because "
        "those embeds render by fetching the explorer page and parsing it. There is no staged "
        "rollout, and removing redirects afterwards is one row at a time.",
        "",
    ]

    if blockers:
        lines += [
            f"## ⛔ Blockers to clear before applying ({len(blockers)})",
            "",
            "A site redirect pointing at the explorer makes the bulk endpoint reject the redirect as "
            "a chain — and it caches its per-source checks, so one row fails **every** entry for that "
            "explorer, not one row.",
            "",
        ]
        lines += gdoc_table(blockers) if all(r["doc_edit_url"] for r in blockers) else _bullets(blockers)
        lines.append("")

    doc_rows = [r for r in rows if r["severity"] in (RED, YELLOW) and r["surface"] != "site redirect"]
    if doc_rows:
        lines += [
            f"## 📝 Google Doc edits ({len(doc_rows)})",
            "",
            "Embedded explorers and text links are listed together — one editing pass per doc covers "
            "both. 🔴 sections break when the redirect is created; 🟡 sections keep working behind the "
            "302.",
            "",
        ]
        by_comp: dict[str, list[dict]] = defaultdict(list)
        for row in doc_rows:
            by_comp[row["component"]].append(row)
        ordered = [c for c in COMPONENT_ORDER if c in by_comp] + sorted(set(by_comp) - set(COMPONENT_ORDER))
        for comp in ordered:
            group = by_comp[comp]
            emoji = "🔴" if any(r["severity"] == RED for r in group) else "🟡"
            lines += [f"### {emoji} {COMPONENT_LABELS.get(comp, comp)} ({len(group)})", ""]
            lines += gdoc_table(group) if all(r["doc_edit_url"] for r in group) else _bullets(group)
            lines.append("")

    if wrong:
        lines += [
            f"## ⚠️ References that will land on the wrong view ({len(wrong)})",
            "",
            "These carry parameters that no longer select a view — either naming a choice the explorer "
            "has dropped, or only some of its dimensions. Today the explorer quietly falls back to a "
            "default; after the redirect they land on the MDIM's default view. So the fix is to author "
            "the intended MDIM view URL, not to swap the URL mechanically.",
            "",
            "| Explorer | Where | Why | Currently lands on |",
            "|---|---|---|---|",
        ]
        for row in wrong:
            why = (
                f"dead choice `{cell(row['stale_params'], 55)}`" if row["stale_params"] else "only some dimensions set"
            )
            target = f"[`{cell(row['replacement_url'].split('/grapher/', 1)[-1], 46)}`]({row['replacement_url']})"
            lines.append(f"| `{cell(row['explorer'], 30)}` | {cell(row['where'], 40)} | {why} | {target} |")
        lines.append("")

    lines += [f"## 📊 Coverage per explorer ({len(runs)})", ""]
    lines += rollup(runs, rows)
    lines.append("")

    if drafts:
        lines += [f"## ℹ️ Unpublished / draft ({len(drafts)})", "", "No reader impact — listed for completeness.", ""]
        lines += _bullets(drafts)
        lines.append("")

    lines += [
        "---",
        "",
        "**Embedded explorers** render the explorer page inside the article; **text links** are "
        "hyperlinks in prose; **front-matter explorer URLs** are the `explorer-url`/`grapher-url` "
        "field in a data insight's header; **homepage explorer tiles** are links with bake-time "
        "titles. 📄 opens the Google Doc · 👁 the admin previewer (works for drafts) · 🔗 the "
        "published page, scrolled to the reference. **Find in doc** is a copy-paste search string. "
        "**Lands on** is computed with grapher's own matching rules, so it is where the reader "
        "actually ends up once the redirect exists.",
        "",
        "## What's still open",
        "",
        (
            f"**Handed off** — {len(blockers)} site redirect(s) to repoint and {len(breaks)} embed(s) to "
            "migrate, both before anything is applied. Each names the surface and the replacement URL."
            if blockers or breaks
            else "**Handed off** — nothing. No blocker and no embed needs migrating before applying."
        ),
        "",
        (
            f"**Proposed** — {len(links)} link(s) the 302 already covers, worth updating to skip the hop"
            + (f", and {len(wrong)} reference(s) whose target view needs a decision" if wrong else "")
            + "."
            if links or wrong
            else "**Proposed** — nothing pending a decision."
        ),
        "",
        "**Unverified** — for an explorer subject the sweep covers article links and embeds only: it "
        "does **not** cover data insights that store the reference elsewhere, static visualizations, "
        "key-chart slots, or narrative charts, and it cannot enumerate a legacy CSV/TSV-backed "
        "explorer's own views. Whether the redirects themselves would be accepted is checked by "
        "`preflight.py`, not here.",
        "",
    ]
    if gaps:
        lines += [
            f"This run also skipped {len(gaps)} surface(s) or subject(s) — an empty result for these "
            "means UNKNOWN, not that nothing references them:",
            "",
            *[f"- {g}" for g in gaps],
            "",
        ]
    path.write_text("\n".join(lines))
    return path


def _bullets(group: list[dict]) -> list[str]:
    lines = []
    for row in group:
        where = f"[{row['where']}]({row['where_url']})" if row["where_url"] else row["where"]
        lines.append(f"- **{row['explorer']}** in {where} — {row['context']}")
        lines.append(f"    - now: {row['old_url']}")
        if row["replacement_url"]:
            lines.append(f"    - lands on: {row['replacement_url']} _({row['match']})_")
        lines.append(f"    - fix: {row['fix']}")
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit what links or embeds the explorers in a redirect proposal.")
    ap.add_argument("--mapping", action="append", required=True, help="A mapping dir (repeatable)")
    ap.add_argument("--out", default=None, help="Output folder (required with >1 --mapping)")
    ap.add_argument("--host", default=None, help="Base URL for links (default: the DB environment's site)")
    args = ap.parse_args()

    runs = load_mappings(args.mapping)
    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
    elif len(args.mapping) == 1:
        out = next(iter(runs.values()))["dir"]
    else:
        raise SystemExit("--out is required when more than one --mapping is given")

    host = (args.host or OWID_ENV.site or "https://ourworldindata.org").rstrip("/")
    admin = TAILSCALE_SUFFIX_RE.sub("", (OWID_ENV.admin_site or "https://admin.owid.io/admin").rstrip("/"))
    print(f"grapher DB: {OWID_ENV.name}")
    print(f"explorers: {', '.join(sorted(runs))}")

    resolve_missing_slugs(runs)
    subject_args = [arg for slug in sorted(runs) for arg in ("--explorer", slug)]
    raw, gaps = run_sweep(subject_args)

    rows = build_rows(runs, raw, host, admin)
    csv_path = write_csv(out, rows)
    md_path = write_markdown(out, runs, rows, gaps)
    manifest_path = write_manifest(out, runs, rows, gaps)

    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["severity"]] += 1
    blockers = sum(1 for r in rows if r["surface"] == "site redirect")
    print(
        f"\nreferences: {len(rows)}  (blockers: {blockers} | break on redirect: {counts[RED] - blockers} | "
        f"links: {counts[YELLOW]} | drafts: {counts[INFO]})"
    )
    wrong = [r for r in rows if r["stale_params"] or "partial" in r["match"]]
    if wrong:
        print(f"⚠️  {len(wrong)} reference(s) would land on the MDIM's default view rather than an intended one:")
        for row in wrong[:10]:
            print(f"  {row['explorer']} in {row['where']}: {row['stale_params'] or row['match']}")
    print(f"\n-> {csv_path}")
    print(f"-> {md_path}")
    print(f"-> {manifest_path}  (which explorers were audited — preflight reads this)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
