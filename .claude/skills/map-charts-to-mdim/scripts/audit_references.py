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
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qsl, urlencode

from etl.config import OWID_ENV

# The sweep's own skill owns the helpers every consumer of it needs (URL resolution, deep
# links, component/page-type parsing, the doc search string) so a fix lands in every
# consumer at once. `replacement_url()` below is deliberately NOT among them: its merge
# semantics are chart-specific.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "find-chart-references" / "scripts"))
from reference_report import (  # noqa: E402
    INFO,
    RED,
    TAILSCALE_SUFFIX_RE,
    YELLOW,
    archie_component,
    cell,
    find_in_doc,
    page_deep_link,
    page_type,
    public_page_url,
    run_sweep,
)

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
    """Sweep every source chart, current slug only (the sweep resolves old slugs itself)."""
    return run_sweep(["--chart-slugs", ",".join(r["chart"]["slug"] for r in redirects)])


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


# A narrative chart's authored patch, split by what a human has to redo in the new editor.
# FAUST text is copied verbatim and is the easiest thing to lose: it does not come from the
# parent, so a replacement built only from the view silently renders the VIEW's title and
# subtitle instead of the ones the article was written around.
FAUST_KEYS = ("title", "subtitle", "note", "sourceDesc", "hideAnnotationFieldsInTitle")
# Controls: everything else worth reproducing. `dimensions` and `$schema` are excluded —
# the new parent view supplies them, and re-applying the old ones would repoint the chart
# at the source chart's indicators, undoing the migration.
SKIP_OVERRIDE_KEYS = ("dimensions", "$schema", "id", "slug", "isPublished", "version")
# Grapher URL params and the config keys holding the same state. Used to keep the "set by
# hand" list from naming one setting twice in two spellings.
# Dotted entries are NESTED config keys: `region` lives at `map.region`, and only that
# subkey represents it. Matching the whole `map` object instead would drop `region` from the
# checklist whenever the patch overrode any unrelated map setting (`map.colorScale`,
# `map.hideTimeline`), leaving the replacement focused on the wrong area with nothing said.
PARAM_CONFIG_EQUIVALENT = {
    "country": ("selectedEntityNames",),
    "focus": ("focusedSeriesNames",),
    "time": ("minTime", "maxTime"),
    "region": ("map.region",),
}
# The order someone rebuilds a chart in: the chart type first, because it decides which other
# controls exist at all; then the entity selection, the most visible thing to get wrong; then
# whatever else was overridden, alphabetically.
CONTROL_ORDER = ("chartTypes", "selectedEntityNames", "country")
# Params whose values are entity CODES. Codes are what a URL carries, but names are what the
# editor's entity picker shows, so they are resolved before being handed to a human.
ENTITY_CODE_PARAMS = ("country", "focus")


def narrative_overrides(findings: list[dict]) -> dict[str, dict]:
    """{narrative chart id -> its authored overrides, minus what the target view already has}.

    Read from `chart_configs.patch` (the authored delta), not `full`: the patch is exactly
    what a human typed on top of the parent, which is exactly what has to be retyped on top
    of the new one. Each override is compared against the TARGET view's config so the report
    only asks for what actually differs — a title the view already carries needs no work.
    """
    ids = {str(f["narrative_id"]) for f in findings if f.get("narrative_id")}
    view_ids = {f["target_view_config_id"] for f in findings if f.get("target_view_config_id")}
    if not ids:
        return {}
    patches = OWID_ENV.read_sql(
        "SELECT nc.id, cc.patch FROM narrative_charts nc JOIN chart_configs cc ON cc.id = nc.chartConfigId "
        "WHERE nc.id IN %(ids)s",
        params={"ids": tuple(sorted(ids))},
    )
    views = (
        OWID_ENV.read_sql(
            "SELECT id, full FROM chart_configs WHERE id IN %(ids)s", params={"ids": tuple(sorted(view_ids))}
        )
        if view_ids
        else None
    )
    view_cfg = {r["id"]: json.loads(r["full"] or "{}") for r in views.to_dict("records")} if views is not None else {}
    by_narrative = {str(r["id"]): json.loads(r["patch"] or "{}") for r in patches.to_dict("records")}

    out: dict[str, dict] = {}
    for f in findings:
        nid = str(f.get("narrative_id") or "")
        patch = by_narrative.get(nid)
        if patch is None:
            continue
        target = view_cfg.get(f.get("target_view_config_id"), {})
        faust, controls = {}, {}
        for key, value in patch.items():
            if key in SKIP_OVERRIDE_KEYS or target.get(key) == value:
                continue
            (faust if key in FAUST_KEYS else controls)[key] = value
        out[nid] = {"faust": faust, "controls": controls}
    return out


def suggest_name(original: str, taken: set[str]) -> str:
    """A free kebab-case name for a replacement narrative chart.

    Only needed when a published page still holds the original name: `create` rejects an
    existing name and there is no rename, so the name picked here is PERMANENT — it is not a
    temporary staging name that can be tidied up after the old chart is deleted. Hence a
    suffix that stays recognisable to whoever finds it in a doc (`-mdim`, the MDIM-parented
    version) rather than a bare counter, with counters only as a fallback.

    Every candidate is checked against the names already in use, so the suggestion cannot be
    the one thing `create` refuses.
    """
    for candidate in (f"{original}-mdim", *(f"{original}-mdim-{n}" for n in range(2, 10))):
        if candidate not in taken:
            return candidate
    return ""


def taken_narrative_names() -> set[str]:
    """Every narrative chart name in use — `create` rejects a duplicate."""
    return set(OWID_ENV.read_sql("SELECT name FROM narrative_charts")["name"])


def entity_names(codes: set[str]) -> dict[str, str]:
    """{entity code -> name}, so a `country=ZWE~MDG` param can be shown the way the editor's
    entity picker shows it. Unknown codes are simply left as-is by the caller."""
    codes = {c for c in codes if c}
    if not codes:
        return {}
    df = OWID_ENV.read_sql("SELECT code, name FROM entities WHERE code IN %(c)s", params={"c": tuple(sorted(codes))})
    return dict(zip(df["code"], df["name"]))


def has_config_key(config: dict, key: str) -> bool:
    """Whether `config` carries `key`, where a dotted key means a nested subkey.

    `map.region` must match only an actual region override, not the presence of any `map`
    object: a patch that set `map.colorScale` and nothing else would otherwise be read as
    already carrying the region.
    """
    head, _, rest = key.partition(".")
    if head not in config:
        return False
    return not rest or (isinstance(config[head], dict) and has_config_key(config[head], rest))


def control_sort_key(key: str) -> tuple:
    """Chart type, then the entity selection, then everything else alphabetically."""
    return (CONTROL_ORDER.index(key), "") if key in CONTROL_ORDER else (len(CONTROL_ORDER), key)


def render_override(value) -> str:
    """Compact one-line rendering of an override value for a table cell."""
    if isinstance(value, list):
        return ", ".join(str(v) for v in value) or "(empty)"
    if isinstance(value, dict):
        return ", ".join(f"{k}={v}" for k, v in value.items())
    return str(value)


def narrative_section(
    group: list[dict], usages: dict[str, list[dict]], overrides: dict[str, dict], host: str, admin: str
) -> list[str]:
    """One block per narrative chart: what it is, where it is used, and the ordered steps.

    Which of the two orders applies is decided by whether a PUBLISHED page references it
    (the rule SKILL.md states): the delete only refuses for published references, and
    `create` rejects an existing name, so the name can be reused — leaving any draft
    reference resolving untouched — exactly when nothing published holds it.
    """
    lines = [
        "| Narrative chart | Target rendering | Set by hand after creating | Used in (what to repoint) | Steps |",
        "|---|---|---|---|---|",
    ]
    # Names already in use, so a suggested replacement name is one `create` will accept. The
    # names suggested in this run are added as they are handed out: two narrative charts on
    # the same parent would otherwise both be told to use the same free name.
    taken = taken_narrative_names()
    for f in group:
        name = f["where"]
        used_in = usages.get(name, [])
        published_uses = [u for u in used_in if u["published"]]
        ovr = overrides.get(str(f.get("narrative_id") or ""), {})

        chart_cell = (
            f"[`{cell(name, 40)}`]({admin}/narrative-charts/{f['narrative_id']}/edit) _✎ admin editor_"
            f"<br>parent: [`{cell(f['source_chart_slug'], 32)}`]({f['old_url']})"
        )
        # The link is the reference rendering to reproduce, and the page the create control
        # lives on. It is NOT a shortcut: creating from an MDIM starts the new chart at the
        # MDIM's DEFAULT view with default entities, so nothing below carries over on its own.
        render_cell = f"**[open the target view]({f['replacement_url']})** — the rendering to match"

        # Resolved before the cells are built because both the "set by hand" list and the
        # steps need it: the name is typed at creation time, and which name applies depends on
        # whether a published page still holds the original.
        if published_uses:
            new_name = suggest_name(name, taken)
            taken.add(new_name)
            name_note = (
                f"**`{new_name}`** — new (a published page still holds `{cell(name, 44)}`); checked: not in use"
                if new_name
                else f"a NEW name — a published page still holds `{cell(name, 44)}`"
            )
        else:
            new_name = name
            name_note = f"**`{name}`** — reuse the original, freed by the delete in step 1"

        # Everything the create does not carry over, in the order it gets done in the editor:
        # the name, then the view, then its controls, then the authored text.
        parts = [f"**name**:<br>{name_note}"]
        dims = f.get("target_dimensions") or {}
        if dims:
            parts.append(
                "**view dimensions** (the new chart opens at the view's defaults):<br>"
                + ", ".join(f"`{k}` = {v}" for k, v in sorted(dims.items()))
            )
        # The stored URL params and the config patch describe the same state in two
        # encodings, so a param is listed only when the patch has no equivalent config key —
        # otherwise the cell asks for the entity selection twice, as `country` and again as
        # `selectedEntityNames`. The config form wins: that is what the editor's fields are.
        config_controls = ovr.get("controls") or {}
        controls = dict(config_controls)
        for key, value in parse_qsl(f["stored_params"], keep_blank_values=True):
            if any(has_config_key(config_controls, equiv) for equiv in PARAM_CONFIG_EQUIVALENT.get(key, (key,))):
                continue
            # A URL param spells entities as codes; the editor's picker speaks names.
            if key in ENTITY_CODE_PARAMS and value:
                codes = value.split("~")
                names = entity_names(set(codes))
                value = [names.get(c, c) for c in codes]
            controls[key] = value
        if controls:
            parts.append(
                "**controls** (chart type, entity selection, tab, time — not inherited):<br>"
                + ", ".join(
                    f"`{k}` = {render_override(v) or '(empty)'}"
                    for k, v in sorted(controls.items(), key=lambda kv: control_sort_key(kv[0]))
                )
            )
        faust = ovr.get("faust") or {}
        if faust:
            parts.append(
                "**text the original overrides** — the view will NOT supply it:<br>"
                + "<br>".join(f"`{k}`: {cell(render_override(v), 110)}" for k, v in sorted(faust.items()))
            )
        else:
            parts.append("_no text overrides — FAUST is inherited, so the view's own text applies_")
        if f["param_collisions"]:
            parts.append(f"⚠️ `{f['param_collisions']}` collides with a view dimension — choose it deliberately")
        faust_cell = "<br><br>".join(parts)

        if used_in:
            uses = []
            for u in sorted(used_in, key=lambda u: (not u["published"], u["slug"] or "")):
                page_url = public_page_url(u["type"], u["slug"], host) if u["published"] else ""
                page = f"[{cell(u['slug'], 34)}]({page_url})" if page_url else f"`{cell(u['slug'] or '(no slug)', 34)}`"
                draft = " ⚠️ **draft**" if not u["published"] else ""
                uses.append(
                    f"{page} _{u['type']}_{draft}<br>`{u['componentType']}` block · "
                    f"[📄 doc](https://docs.google.com/document/d/{u['gdoc_id']}/edit) · "
                    f"[👁 preview]({admin}/gdocs/{u['gdoc_id']}/preview) · find `{cell(name, 44)}`"
                )
            used_cell = "<br><br>".join(uses)
            if not published_uses:
                used_cell += "<br>_none published — reusing the name keeps these resolving_"
        else:
            used_cell = "_not referenced by any page_"

        create_step = (
            'open the target view and use the chart\'s **"Create narrative chart"** admin control '
            "(visible when logged in)"
        )
        set_step = (
            "**set everything in the third column by hand** — the control parents the new chart to the "
            "right view, but its state opens at that view's defaults, so the dimensions, controls and "
            "authored text do not carry over; compare against the original in the admin editor"
        )
        if published_uses:
            steps = (
                f"1. **create**: {create_step}, naming it as the third column says "
                "(`create` rejects an existing name)"
                f"<br>2. {set_step}"
                f"<br>3. **repoint** the page(s) to `{cell(new_name or 'the new name', 44)}`"
                "<br>4. **delete** the old one — succeeds once no published page references it"
            )
        else:
            steps = (
                "1. **delete** the old one — nothing published references it, so this frees the name"
                f"<br>2. **create**: {create_step}, naming it as the third column says"
                f"<br>3. {set_step}"
            )
        lines.append(f"| {chart_cell} | {render_cell} | {faust_cell} | {used_cell} | {steps} |")
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
            "already exists. So each row below carries the order that fits it: **create → repoint "
            "→ delete** when a published page holds it, and the shorter **delete → create under the "
            "same name** when none does (any draft reference then keeps resolving untouched).",
            "",
            "Create the replacement from the target view itself, using the chart's **\"Create narrative "
            'chart"** admin control — not a bare `/admin/narrative-charts/create?chartConfigId=…` '
            "link, which opens a copy of the MDIM's default view.",
            "",
            "**Then expect to rebuild the state by hand.** The control gets the parent view right, "
            "but the new chart opens at **that view's defaults** — the dimension selection, the entity "
            "selection and the tab and time settings all come up at defaults, and any text the "
            "original authored on top of its parent never transfers at all. (A bare create link is "
            "worse: it also parents to the wrong view.) The **Set by hand after creating** column lists "
            "exactly those groups per chart, in the order to apply them. Miss the text and the "
            "replacement silently renders the view's own wording instead of the wording the article "
            "was written around; miss the controls and it renders the wrong countries.",
            "",
        ]
        lines += narrative_section(
            narrative,
            narrative_chart_usages({f["where"] for f in narrative}),
            narrative_overrides(narrative),
            host,
            admin,
        )

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

    # Nothing in this audit is applied by running it, so it closes with the one call to action
    # (the embeds a redirect cannot save) and then what the sweep did not reach. The counts
    # restate the sections above on purpose — the coverage note is the part a reader cannot
    # infer from them, and silence there would read as "everything was checked".
    must_act = (
        f"{len(embeds)} embedded reference(s) need a manual edit before the charts are unpublished — see every "
        "🔴 section above, each naming the surface that holds it and the replacement URL to put there. A redirect "
        "does not cover an embed. `preflight.py` gates on this same set."
        if embeds
        else "No reference needs manual migration before the charts are unpublished."
    )
    covered_by_redirect = (
        f"The 301 keeps the {len(links)} link-kind reference(s) in the 🟡 sections working"
        + (f", including {len(narrative)} narrative chart(s) to recreate" if narrative else "")
        + ", so updating each is a call someone can make later, not a blocker."
        if links
        else "No link updates are pending a decision."
    )
    not_covered = (
        "This audit does not cover non-ETL explorer TSVs, data insights that store the "
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
    lines += [must_act, "", covered_by_redirect, "", "## What this sweep didn't cover", "", not_covered, ""]
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
            # No create URL here: the admin's create route parents the new chart to the
            # MDIM's DEFAULT view, not the target view, so handing one over in a cell that
            # cannot show the surrounding caveats is how someone ends up with a replacement
            # on the wrong view. The route that does work is the view's own control.
            # The order (create-first vs delete-first) likewise depends on whether a
            # PUBLISHED page references it, which only the report's own section resolves —
            # so this cell states the goal and points there.
            fix = (
                f"recreate it manually: open the target view ({new_url}), set its controls, and use that "
                "view's \"Create narrative chart\" admin control — the create route parents to the MDIM's "
                "default view instead. Then repoint the pages that use it; see the Narrative charts "
                "section of references.md for those pages, the text to re-apply, and the order the API forces"
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
                "where_url": page_deep_link(ref, host, admin),
                # posts_gdocs.id IS the Google Doc id, so the edit link is direct; the
                # admin previewer renders unpublished drafts the public URL 404s on.
                "doc_edit_url": f"https://docs.google.com/document/d/{ref['surface_id']}/edit" if is_gdoc else "",
                "doc_preview_url": f"{admin}/gdocs/{ref['surface_id']}/preview" if is_gdoc else "",
                "find_in_doc": find_in_doc(ref) if is_gdoc else "",
                # Narrative rows carry the ids their own section needs: the narrative chart
                # to open/delete, and the target view to parent the replacement to.
                "narrative_id": ref["surface_id"] if ref["surface"] == "narrative chart" else "",
                # The narrative chart's own stored controls (entities, tab, time). The admin
                # create page cannot preset them, so the report names them for hand-copying.
                "stored_params": qs if ref["surface"] == "narrative chart" else "",
                # Creating from an MDIM starts at its DEFAULT view, so the dimension
                # selection has to be re-made by hand too — the report has to name it.
                "target_dimensions": dict(r["target"]["dimensions"]) if ref["surface"] == "narrative chart" else {},
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
        print(
            f'  {len(gaps)} surface(s)/subject(s) were NOT swept — see "What this sweep didn\'t cover" in the report.'
        )
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
