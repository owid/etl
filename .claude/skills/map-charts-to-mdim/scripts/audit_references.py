"""Show where each chart being redirected is linked or embedded, and what to replace it with.

READ-ONLY. The surface sweep itself lives in the `find-chart-references` skill — this script
is the redirect-specific consumer: it runs that sweep for the proposal's source charts,
then adds what only this workflow knows, namely the URL each reference should become.

Severity, derived from the sweep's `kind`:
  RED    embed — the redirect does NOT fix it. The surface holds the chart by id or
         slug and renders its config directly, so it breaks when the source chart is
         unpublished (which the apply CLI always does). Migrate before applying.
  YELLOW link (the 301 covers it, but the href should be updated so readers don't
         take an extra hop).
  INFO   no action required: the referencing page is unpublished/draft, or the row is
         a topic page's All charts entry — that block lists only published charts
         (GdocPost.loadRelatedCharts filters on isPublished), so the entry drops out
         on its own at the next bake.

The report is organized by what the reader does, not by severity tier: embedded charts
and text links sit adjacent in one "Google Doc edits" section (one editing pass per doc
covers both), and All charts entries collapse to a per-topic-page summary.

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
from urllib.parse import parse_qsl, quote, urlencode

from etl.config import OWID_ENV

FIND_REFERENCES = Path(__file__).resolve().parents[2] / "find-chart-references" / "scripts" / "find_references.py"

RED, YELLOW, INFO = "RED", "YELLOW", "INFO"
# Staging admin hosts carry a tailscale suffix that is noise in a link handed to a human.
TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")

REFERENCE_COLUMNS = [
    "severity", "surface", "component", "kind", "source_chart_slug", "where", "where_url",
    "doc_edit_url", "doc_preview_url", "find_in_doc", "context",
    "old_url", "replacement_url", "param_collisions", "fix",
]  # fmt: skip

# Surfaces whose fix is a Google Doc edit. For these the report renders a table with the
# doc link and a copy-paste search hint (the find-chart-references convention) — handing
# someone a page URL without saying where in the doc the reference sits makes them
# re-derive exactly what the sweep already knew. In the report they are NOT grouped by
# these sweep surfaces — a data insight IS a gdoc; what matters to the person editing the
# doc is the ArchieML component they are touching, so the tables group by that instead
# (see archie_component).
GDOC_SURFACES = ("gdoc", "gdoc (url link)", "data insight")

# The sweep's `key chart` surface is a chart_tags row with a keyChartLevel — what it feeds
# on the site is the topic page's "All charts" block ordering, so name it by what the
# reader loses, not by the DB mechanism.
SURFACE_LABELS = {"key chart": "all-charts block (topic page)"}

# Reader-facing section names for the ArchieML component tokens — "chart" and "span-link"
# mean nothing to someone who doesn't write ArchieML. The raw token stays in the CSV's
# `component` column.
COMPONENT_LABELS = {
    "chart": "Embedded charts",
    "span-link": "Text links",
    "front-matter": "Front-matter chart URLs",
}
# Blocking edits first: embedded charts and front-matter URLs break on unpublish, text
# links merely go through an extra 301 hop.
COMPONENT_ORDER = ("chart", "front-matter", "span-link")

FIXES = {
    "explorer": "repoint the explorer at the MDIM indicators, or retire the explorer",
    "static viz": "regenerate the static visualization against the MDIM view",
    # No action possible or needed: the All charts block lists only published charts
    # (GdocPost.loadRelatedCharts filters on isPublished), so entries drop out at the next
    # bake — and the block is built from charts × chart_tags only, so an MDIM cannot be
    # tagged into it as a replacement. Featuring the MDIM on the topic page is a separate
    # gdoc-authoring change, not part of this migration.
    "key chart": "no action — the entry drops out of the All charts block automatically, and the "
    "block cannot list MDIMs (it is built from charts only), so there is no replacement to add",
}
# Gdoc-backed references: the fix depends on the ArchieML component being edited (and for
# chart blocks, on whether the block embeds the chart or merely stores its URL).
COMPONENT_FIXES = {
    ("chart", "embed"): "edit the chart block to embed the MDIM view",
    ("chart", "link"): "update the chart block's grapher URL",
    ("span-link", "link"): "update the href",
    ("front-matter", "embed"): "update the grapher-url in the front matter",
    ("front-matter", "link"): "update the grapher-url in the front matter",
}
LINK_FIX = "update the href"


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


def deep_link(where_path: str, anchor: str, host: str) -> str:
    """Published-page URL scrolled to the reference via a text fragment (block embeds
    have no anchor text, so those fall back to the plain URL). Same encoding as
    find-chart-references / chart_diff citations: parentheses literal, hyphens escaped."""
    base = f"{host}{where_path}" if where_path else ""
    if not base or not anchor:
        return base
    encoded = quote(anchor[:200], safe="()").replace("-", "%2D")
    return f"{base}#:~:text={encoded}"


def archie_component(ref: dict) -> str:
    """The ArchieML component this reference lives in: chart, span-link, front-matter, …

    The sweep encodes it as the head of `context` ("chart (article)", "span-link
    (data-insight)"); the data-insight surface spells its front-matter reference out in
    prose instead, so normalize that to the same token. This is what the gdoc tables
    group by — the person editing the doc cares which construct they are touching, not
    whether the page is an article or a data insight (both are gdocs).
    """
    head = ref["context"].split(" — ")[0]
    if head.startswith("grapher-url"):
        return "front-matter"
    return head.split(" (")[0].strip()


def page_type(ref: dict) -> str:
    """article / data insight / topic-page / fragment — the parenthesized tail of context.

    The data-insight surface spells its front-matter reference out in prose with no
    parenthesized suffix, so fall back to the surface name. Without that fallback those
    rows lose the page-type marker exactly where it now matters most: articles and data
    insights share one table, and the Where column is the only thing distinguishing them.
    """
    m = re.search(r"\(([^)]+)\)", ref["context"].split(" — ")[0])
    if m:
        return m.group(1)
    return ref["surface"] if ref["surface"] == "data insight" else ""


def find_in_doc(ref: dict) -> str:
    """Copy-paste search string for the Google Doc's find box (find-chart-references
    convention): the visible anchor text for a prose hyperlink; for a block embed the doc
    holds a bare grapher URL, so the slug — exactly as the author typed it, which is what
    `posts_gdocs_links.target` stores and may be an old slug."""
    anchor = " ".join((ref.get("text") or "").split())
    return anchor or ref["subject"]


def narrative_chart_usages(names: set[str]) -> dict[str, list[dict]]:
    """{narrative chart name -> the pages embedding it}, for the repoint step.

    A second hop past the sweep, which answers "what references the CHART"; this answers
    "what references the NARRATIVE CHART hanging off it" — and that is what decides the
    recreate ORDER, so the report cannot tell someone to "update the article(s)" without
    it. The delete endpoint refuses while a PUBLISHED post references the narrative chart,
    which makes create-then-repoint-then-delete mandatory in that case; when nothing
    references it, the simpler delete-then-recreate-under-the-same-name is available.

    Both component types that can hold one are matched (`narrative-chart` blocks and
    `key-insights` slides).
    """
    if not names:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT l.target AS name, l.componentType, g.id AS gdoc_id, g.slug, g.published, g.type "
        "FROM posts_gdocs_links l JOIN posts_gdocs g ON g.id = l.sourceId "
        "WHERE l.linkType = 'narrative-chart' AND l.target IN %(names)s",
        params={"names": tuple(sorted(names))},
    )
    usages: dict[str, list[dict]] = defaultdict(list)
    for r in df.to_dict("records"):
        usages[r["name"]].append(r)
    return usages


def narrative_section(group: list[dict], usages: dict[str, list[dict]], host: str, admin: str) -> list[str]:
    """One block per narrative chart: what it is, where it is used, and the ordered steps.

    Which of the two orders applies is decided by whether a PUBLISHED page references it
    (the rule SKILL.md states): the delete only refuses for published references, and
    `create` rejects an existing name, so the name can be reused — leaving any draft
    reference resolving untouched — exactly when nothing published holds it.
    """
    lines: list[str] = []
    for f in group:
        name = f["where"]
        used_in = usages.get(name, [])
        published_uses = [u for u in used_in if u["published"]]
        lines += [
            f"### `{name}`",
            "",
            f"- parent chart: [`{f['source_chart_slug']}`]({f['old_url']}) · "
            f"[✎ open the narrative chart]({admin}/narrative-charts/{f['narrative_id']}/edit)",
            f"- replacement should render: {f['replacement_url']}",
        ]
        if f["param_collisions"]:
            lines.append(
                f"- ⚠️ its stored query params `{f['param_collisions']}` collide with the target view's "
                "dimensions and would override them"
            )
        if used_in:
            lines.append(
                f"- **used in {len(used_in)} page(s)** — "
                + (
                    "these are what the repoint step updates:"
                    if published_uses
                    else "none of them published, so reusing the name below keeps them resolving as they are:"
                )
            )
            for u in sorted(used_in, key=lambda u: (not u["published"], u["slug"] or "")):
                draft = "" if u["published"] else " ⚠️ **draft**"
                page = (
                    f"[{u['slug']}]({host}/{u['slug']})" if u["published"] and u["slug"] else f"`{u['slug'] or '(no slug)'}`"
                )  # fmt: skip
                lines.append(
                    f"    - {page} _{u['type']}_{draft} — in a `{u['componentType']}` block · "
                    f"[📄 doc](https://docs.google.com/document/d/{u['gdoc_id']}/edit) · "
                    f"[👁 preview]({admin}/gdocs/{u['gdoc_id']}/preview) · search the doc for `{name}`"
                )
        else:
            lines.append("- **not referenced by any page**")
        create = f"{admin}/narrative-charts/create?type=multiDim&chartConfigId={f['target_view_config_id']}"
        if published_uses:
            lines += [
                f"- **step 1 — create** the replacement under a NEW name (`create` rejects an existing one): {create}",
                "- **step 2 — repoint:** change the page(s) above to the new narrative chart's name",
                "- **step 3 — delete:** remove the old narrative chart — the delete succeeds once no "
                "published page references it, which is why it comes last",
            ]
        else:
            lines += [
                "- **step 1 — delete** the old narrative chart: nothing published references it, so the "
                "delete succeeds and frees the name",
                f"- **step 2 — create** the replacement, reusing the SAME name (`{name}`): {create}",
            ]
        lines.append("")
    return lines


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


def cell(value: str, limit: int = 70, marker: str = "…") -> str:
    """Table-safe cell: escape pipes and newlines, truncate runaway text.

    Pass marker="" for copy-paste search strings: an appended ellipsis is a character
    that does not exist in the doc, so the copied text would match nothing — a bare
    literal prefix still finds the spot. Pipes are escaped after truncating so the cut
    can never leave half an escape sequence behind.
    """
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        text = text[: limit - 1] + marker
    return text.replace("|", "\\|")


def gdoc_table(group: list[dict]) -> list[str]:
    """One table per fix, one row per reference: doc links + the search string that lands on it.

    The 'Replace with' cell links the full replacement URL under a readable label; the
    raw URL is in references.csv for copy-paste. Fix instructions live above each table
    rather than in a column — and because a (severity, surface) group can mix fixes (an
    unpublished doc can hold both an embed and a prose link, and the INFO section does
    not split by kind), the group is rendered as one table per fix, keeping every row
    under the instruction that applies to it.
    """
    by_fix = defaultdict(list)
    for f in group:
        by_fix[f["fix"]].append(f)
    lines: list[str] = []
    for fix in sorted(by_fix):
        if lines:
            lines.append("")
        lines += [f"Fix: {fix}", ""]
        lines += ["| Source chart | Where | Open | Find in doc | Replace with |", "|---|---|---|---|---|"]
        for f in by_fix[fix]:
            source = f"[`{cell(f['source_chart_slug'], 44)}`]({f['old_url']})"
            opens = " · ".join(
                part
                for part in (
                    f"[📄 doc]({f['doc_edit_url']})" if f["doc_edit_url"] else "",
                    f"[👁 preview]({f['doc_preview_url']})" if f["doc_preview_url"] else "",
                    f"[🔗 page]({f['where_url']})" if f["where_url"] else "",
                )
                if part
            )
            # marker="" — this cell is copied into the doc's find box verbatim, so it must
            # stay a literal prefix of the text in the doc.
            find = f"`{cell(f['find_in_doc'], 55, marker='')}`" if f["find_in_doc"] else "—"
            target_label = cell(f["replacement_url"].split("/grapher/", 1)[-1], 60)
            replace = f"[`{target_label}`]({f['replacement_url']})"
            if f["param_collisions"]:
                replace += f" ⚠️ params `{f['param_collisions']}` override view dimensions"
            # The page type moved into this cell when the sections stopped separating
            # gdocs from data insights — it changes who owns the fix.
            where = cell(f["where"], 44) + (f" _{f['page_type']}_" if f.get("page_type") else "")
            lines.append(f"| {source} | {where} | {opens} | {find} | {replace} |")
    return lines


def bullet_list(group: list[dict]) -> list[str]:
    """Fallback rendering for references without a Google Doc behind them."""
    lines: list[str] = []
    for f in group:
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
    return lines


def write_markdown(
    out: Path, findings: list[dict], redirects: list[dict], host: str, gaps: list[str], admin: str = ""
) -> Path:
    path = out / "references.md"
    # The reader's partition is by what they DO, not by severity tiers: one editing pass
    # per Google Doc covers embeds and links alike, so those sit adjacent in one section;
    # the topic-page All charts blocks need no action at all (verified: the block lists
    # only published charts) and collapse to a per-page summary.
    allcharts = [f for f in findings if f["surface"] == "key chart"]
    drafts = [f for f in findings if f["severity"] == INFO and f["surface"] != "key chart"]
    actionable = [f for f in findings if f["severity"] in (RED, YELLOW)]
    doc_edits = [f for f in actionable if f["doc_edit_url"]]
    narrative = [f for f in actionable if f["surface"] == "narrative chart"]
    other = [f for f in actionable if not f["doc_edit_url"] and f["surface"] != "narrative chart"]
    # Every RED finding blocks, wherever it lives: an explorer or static viz that embeds the
    # chart has no Google Doc behind it, but it renders the chart's own config and breaks on
    # unpublish exactly like an article embed does — and preflight gates on all of them.
    # Scoping this to doc-backed rows would have the report announce "no embeds to migrate"
    # while a blocking surface sat under Proposed being described as 301-covered.
    embeds = [f for f in actionable if f["severity"] == RED]
    links = [f for f in actionable if f["severity"] == YELLOW]

    lines = [
        "# What references the charts being redirected",
        "",
        f"{len(redirects)} chart(s) heading for unpublishing — proposed redirects, plus charts already "
        f"redirected whose source chart is still published. **{len(embeds)} embedded reference(s) break "
        f"when the charts are unpublished** and must be migrated before the CLI runs (every 🔴 section "
        f"below, doc-backed or not); {len(links)} link(s) keep working via the 301 but belong in the same "
        "editing pass. Topic-page All charts blocks update themselves — summarized below, no action needed.",
        "",
        "Replacement URLs merge each reference's own query string over the MDIM view's dimensions, "
        "the same way grapher's redirect handler does.",
        "",
    ]

    if doc_edits:
        lines += [
            f"## 📝 Google Doc edits ({len(doc_edits)})",
            "",
            "Embedded charts and text links are listed together — one editing pass per doc covers "
            "both. 🔴 sections break on unpublish (do these before the CLI runs); 🟡 sections stay "
            "functional behind the 301.",
            "",
        ]
        by_comp = defaultdict(list)
        for f in doc_edits:
            by_comp[f["component"]].append(f)
        ordered = [c for c in COMPONENT_ORDER if c in by_comp] + sorted(set(by_comp) - set(COMPONENT_ORDER))
        for comp in ordered:
            group = by_comp[comp]
            emoji = "🔴" if any(g["severity"] == RED for g in group) else "🟡"
            lines += [f"### {emoji} {COMPONENT_LABELS.get(comp, comp)} ({len(group)})", ""]
            lines += gdoc_table(group)
            lines.append("")

    if narrative:
        lines += [
            f"## 🎨 Narrative charts ({len(narrative)})",
            "",
            "A narrative chart renders its own saved config, so nothing breaks when its parent chart "
            'is unpublished — only its generated "Explore the data" link follows the redirect. The '
            "plan for each one: **recreate it manually from the target MDIM view**. There is no "
            "repointing API and no rename, and the API forces which order applies — the delete is "
            "refused while a **published** page references it, and `create` rejects a name that "
            "already exists. So each block below carries the order that fits it: **create → repoint "
            "→ delete** when a published page holds it, and the shorter **delete → create under the "
            "same name** when none does (any draft reference then keeps resolving untouched).",
            "",
        ]
        lines += narrative_section(narrative, narrative_chart_usages({f["where"] for f in narrative}), host, admin)

    if allcharts:
        by_page = defaultdict(list)
        for f in allcharts:
            by_page[f["where"]].append(f)
        lines += [
            f"## 📊 All charts blocks on topic pages ({len(allcharts)} entries — no action needed)",
            "",
            "These blocks list only published charts (grapher's `GdocPost.loadRelatedCharts` filters "
            "on `isPublished`), so the entries drop out on their own at the next bake — nothing breaks "
            "or goes stale. There is also **no replacement to add**: the block is built from "
            "`charts` × `chart_tags` only, so an MDIM cannot appear in it. If a topic page should "
            "feature the MDIM, that is a separate gdoc-authoring change, not part of this migration.",
            "",
            "| Topic page | Charts affected |",
            "|---|---|",
        ]
        for page in sorted(by_page):
            lines.append(f"| {page} | {len(by_page[page])} |")
        lines.append("")

    if other:
        by_surface = defaultdict(list)
        for f in other:
            by_surface[SURFACE_LABELS.get(f["surface"], f["surface"])].append(f)
        lines += [f"## Other surfaces ({len(other)})", ""]
        for surface in sorted(by_surface):
            group = by_surface[surface]
            # Same 🔴/🟡 marking as the doc sections: a RED row here blocks the CLI just as
            # hard, so it must not read as a lower tier merely for lacking a doc link.
            emoji = "🔴" if any(g["severity"] == RED for g in group) else "🟡"
            lines += [f"### {emoji} {surface} ({len(group)})", ""]
            lines += bullet_list(group)
            lines.append("")

    if drafts:
        lines += [f"## ℹ️ Unpublished / draft ({len(drafts)})", "", "No reader impact — listed for completeness.", ""]
        by_comp = defaultdict(list)
        for f in drafts:
            by_comp[f["component"] or SURFACE_LABELS.get(f["surface"], f["surface"])].append(f)
        for comp in sorted(by_comp):
            group = by_comp[comp]
            lines += [f"### {COMPONENT_LABELS.get(comp, comp)} ({len(group)})", ""]
            lines += gdoc_table(group) if all(f["doc_edit_url"] for f in group) else bullet_list(group)
            lines.append("")

    # Nothing in this audit is applied by running it, and its coverage is not total — so it
    # closes by separating what someone must act on from what nobody has checked, rather
    # than leaving a reader to infer either from the sections above.
    handed_off = (
        f"**Handed off** — {len(embeds)} embedded reference(s), in every 🔴 section above. Each names the "
        "surface that holds it and the replacement URL to put there; whoever owns it has to make the edit, "
        "because unpublishing the chart breaks it and the redirect does not cover it. `preflight.py` gates "
        "on this same set."
        if embeds
        else "**Handed off** — nothing. No reference needs manual migration before the charts are unpublished."
    )
    proposed = (
        f"**Proposed** — {len(links)} link-kind reference(s) in the 🟡 sections above"
        + (f", including {len(narrative)} narrative chart(s) to recreate" if narrative else "")
        + ". The 301 keeps every one of them working, so acting on each is a call someone still has to "
        "make, not a blocker."
        if links
        else "**Proposed** — nothing. No link updates are pending a decision."
    )
    unverified = (
        "**Unverified** — this audit does not cover non-ETL explorer TSVs, data insights that store the "
        "reference somewhere other than the surfaces swept here, or charts nested inside layout containers; "
        "see the `find-chart-references` skill for the full surface catalog and its known gaps. "
        f"{len(drafts)} unpublished or draft reference(s) were found and listed but not graded for reader impact. "
        "Whether the redirects themselves apply cleanly is checked by `preflight.py`, not here."
    )
    lines += [
        "---",
        "",
        "**Embedded charts** are chart blocks rendered on the page; **text links** are hyperlinks in "
        "prose; **front-matter chart URLs** are the `grapher-url` field in a data insight's header. "
        "The page type (article, data insight, …) is italicized in the Where column. In the tables, "
        "the **source chart links to the reference's own URL** (its params applied) and **Replace "
        "with links to the target MDIM view** the same reference should become. "
        "📄 opens the Google Doc to edit · 👁 opens the page in the admin previewer (works for "
        "unpublished drafts too) · 🔗 opens the published page scrolled to the reference. "
        "**Find in doc** is a copy-paste search string for the Google Doc's find box: the link text for "
        "a prose hyperlink, or the chart slug for a block embed (the doc holds a bare grapher URL "
        "there — and the slug is stored as the author typed it, so it matches even when the doc still "
        "uses an old one).",
        "",
    ]
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
        if ref["surface"] == "key chart":
            # Verified no-action: the All charts block only lists published charts, so the
            # entry disappears on its own when the CLI unpublishes the source.
            severity = INFO
        is_gdoc = ref["surface"] in GDOC_SURFACES and ref.get("surface_id")
        component = archie_component(ref) if is_gdoc else ""
        if ref["surface"] == "narrative chart":
            # The intended end-state is a manual replacement pointed back at by the same
            # articles, not a repoint of the old one (the parent columns are INSERT-only —
            # see SKILL.md). Nothing breaks meanwhile: the chart renders its own saved
            # config, and only its generated "Explore the data" link follows the redirect.
            # The admin's create page is deep-linkable to the target view, so hand over the
            # ready-made URL rather than an id to look up.
            fix = (
                "recreate it manually and point references back at the new one: "
                f"(1) create the replacement parented to the target MDIM view — {admin}/narrative-charts/create?type=multiDim&chartConfigId={r['target']['viewConfigId']} ; "
                "(2) update the article(s) that reference it to the new name; "
                "(3) delete the old one — the delete is refused while a published post still "
                "references it, which is why the order matters (see SKILL.md)"
            )
        elif is_gdoc:
            fallback = LINK_FIX if ref["kind"] == "link" else "migrate this reference by hand"
            fix = COMPONENT_FIXES.get((component, ref["kind"]), fallback)
        elif ref["kind"] == "link":
            fix = LINK_FIX
        else:
            fix = FIXES.get(ref["surface"], "migrate this reference by hand")
        findings.append(
            {
                "severity": severity,
                "surface": ref["surface"],
                "component": component,
                "page_type": page_type(ref) if is_gdoc else "",
                "kind": ref["kind"],
                "source_chart_slug": ref["subject"],
                "where": ref["where"],
                # Scrolled to the reference when the anchor text allows it.
                "where_url": deep_link(ref["where_path"], ref.get("text") or "", host),
                # posts_gdocs.id IS the Google Doc id, so the edit link is direct; the
                # admin previewer renders unpublished drafts the public URL 404s on.
                "doc_edit_url": f"https://docs.google.com/document/d/{ref['surface_id']}/edit" if is_gdoc else "",
                "doc_preview_url": f"{admin}/gdocs/{ref['surface_id']}/preview" if is_gdoc else "",
                "find_in_doc": find_in_doc(ref) if is_gdoc else "",
                # Narrative rows carry the ids their own section needs: the narrative chart
                # to open/delete, and the target view to parent the replacement to.
                "narrative_id": ref["surface_id"] if ref["surface"] == "narrative chart" else "",
                "target_view_config_id": r["target"]["viewConfigId"],
                "context": ref["context"] + (f' — "{ref["text"][:60]}"' if ref["text"] else ""),
                "old_url": f"{host}/grapher/{ref['subject']}" + (f"?{qs.lstrip('?')}" if qs else ""),
                "replacement_url": new_url,
                "param_collisions": ",".join(collisions),
                "fix": fix,
            }
        )

    findings.sort(key=lambda f: ({RED: 0, YELLOW: 1, INFO: 2}[f["severity"]], f["surface"], f["source_chart_slug"]))

    csv_path = write_csv(mapping_dir, findings)
    md_path = write_markdown(mapping_dir, findings, redirects, host, gaps, admin)

    counts: dict[str, int] = defaultdict(int)
    for f in findings:
        counts[f["severity"]] += 1
    n_allcharts = sum(1 for f in findings if f["surface"] == "key chart")
    print(f"\nreferences: {len(findings)}  (needs manual work: {counts[RED]} | "
          f"links to update: {counts[YELLOW]} | no action (all-charts blocks): {n_allcharts} | "
          f"drafts: {counts[INFO] - n_allcharts})")  # fmt: skip
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
