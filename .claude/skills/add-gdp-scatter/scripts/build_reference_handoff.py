"""Turn a find-chart-references sweep into the actionable handoff for retiring the sources.

The sweep says what points at the charts about to be retired; this adds the half that is
specific to this workflow — **where each reference should point instead** — and keeps the
sweep's own presentation, because that presentation is what makes a row fixable:

* **📄 doc** — the Google Doc to edit. `posts_gdocs.id` IS the Doc id, so it is a direct link.
* **👁 preview** — the article in the admin previewer, which renders unpublished drafts too.
* **🔗 page** — the published page, deep-linked to the reference with a scroll-to-text
  fragment when the reference has anchor text, under the route its page TYPE is served on.
* **Find in the doc** — a copy-paste search string: the link text for a prose hyperlink, or
  the chart slug for a block embed (the doc holds a bare grapher URL there, and the slug is
  stored as the author typed it, so it matches even when the doc still uses an old one).

Without those, a handoff row names an article and leaves the editor to hunt through it. The
formatters are **imported** from the `find-chart-references` scripts rather than
reimplemented, so the reports cannot drift apart — each one from whichever module gets it
right: `find_references.py` for the doc/preview links, and `reference_report.py` for the page
link and the search string, since it is the one that knows the page-type routes and truncates
without an ellipsis.

Usage::

    .venv/bin/python .claude/skills/add-gdp-scatter/scripts/build_reference_handoff.py \\
        --references ai/<name>_references.json \\
        --pairs ai/<name>_part2_pairs.json \\
        [--gaps ai/<name>_references_gaps.json] \\
        [--output ai/<name>_reference_handoff.md]

`--references` is the `--json` output of `find_references.py` and `--gaps` its `--gaps-json`,
which carries that run's own coverage gaps. `--pairs` is the pair list of the rows actually
being retired, so the replacement URLs are the ones the migration creates; either pair schema
works — Part 1's table rows (admin URLs) or the Part 2 payload (public grapher URLs).
"""

import argparse
import importlib.util
import json
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlencode

from etl.config import OWID_ENV

ADMIN_CHART_ID_RE = re.compile(r"/charts/(\d+)")
GRAPHER_SLUG_RE = re.compile(r"/grapher/([^/?#]+)")

# Part 1's pasted table names charts by admin URL; the Part 2 payload the applier consumes
# names them by public grapher URL. Both are valid pair lists for this handoff.
SOURCE_KEYS = ("chart_admin_url", "grapher_url")
TARGET_KEYS = ("target_chart_admin_url", "target_chart_url")

# Kept in step with TARGET_QUERY in redirect_to_scatter.py: every param stands in for an
# adjustment a tab CLICK makes and a URL-supplied tab does not get.
REDIRECT_QUERY = "tab=scatter&time=latest&country="

# The surface `find_references.sweep_articles_placing_narrative_charts` emits: an article that
# places a narrative chart by name. Deliberately not one of `fr.GDOC_SURFACES` even though the
# surface IS a gdoc — the embed/link sections filter on that tuple, and these rows reference the
# narrative chart, not the chart being retired, so counting them there would overstate both.
NARRATIVE_PLACEMENT_SURFACE = "gdoc (narrative chart)"

# The one param that varies per row. The applier never forces `yAxis.scaleType: log` on a
# target — `yAxis` is global, so it would flip the line/bar views too — it only enables the
# toggle and leaves the default linear. A source scatter that was authored on a log y axis
# therefore becomes a LINEAR scatter on the target, and its shape changes: the relationship
# the old chart was drawn to show is the reason the author chose log. `yScale=log` restores
# it for this view only, exactly as `time=latest` and `country=` restore the other two
# adjustments a URL-supplied tab does not get.
Y_SCALE_KEY = "yScale"
SITE = "https://ourworldindata.org"

MANUAL_SURFACES = {"explorer", "data insight", "static viz"}


def _load_sibling(name: str):
    """Import a script from this skill's own scripts directory."""
    return _load_module(name, Path(__file__).resolve().parent / f"{name}.py")


def _load_shared(name: str):
    """Import a find-chart-references module, so the reports share one presentation."""
    return _load_module(name, Path(__file__).resolve().parents[2] / "find-chart-references" / "scripts" / f"{name}.py")


def _load_module(name: str, path: Path):
    if not path.exists():
        raise SystemExit(f"Cannot import {name}: {path} does not exist")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


fr = _load_shared("find_references")
# The log-source question is owned by the applier, which also owns the reversed-source
# exclusion it depends on — see `log_y_axis_sources`.
applier = _load_sibling("apply_scatter_defaults")
# Two of the aids have a second, corrected implementation in `reference_report.py`, and this
# report takes each formatter from whichever module gets it right — copying either one here
# would be the drift this script exists to avoid. See `page_link` and `find_hint`.
rr = _load_shared("reference_report")


def chart_ref(url: str) -> int | str:
    """The chart id from an admin URL, or the slug from a public grapher URL."""
    admin_match = ADMIN_CHART_ID_RE.search(url)
    if admin_match:
        return int(admin_match.group(1))
    slug_match = GRAPHER_SLUG_RE.search(url)
    if slug_match:
        return slug_match.group(1)
    raise SystemExit(f"Cannot read a chart id or slug from {url!r}")


def pick(pair: dict, keys: tuple[str, ...]) -> str:
    for key in keys:
        if pair.get(key):
            return str(pair[key])
    raise SystemExit(f"Pair {pair} carries none of {keys}")


def load_pairs(path: Path) -> dict[int, int]:
    """Source chart id -> target chart id, from either pair schema.

    Slugs are resolved against the DB, so this must run **before** Part 2 unpublishes the
    sources — after that their slugs belong to `chart_slug_redirects`, not to a chart.
    """
    refs = [(chart_ref(pick(p, SOURCE_KEYS)), chart_ref(pick(p, TARGET_KEYS))) for p in json.loads(path.read_text())]
    wanted = sorted({ref for pair in refs for ref in pair if isinstance(ref, str)})
    ids: dict[str, int] = {}
    if wanted:
        df = OWID_ENV.read_sql(
            "SELECT c.id, cc.slug FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE cc.slug IN %(s)s",
            params={"s": tuple(wanted)},
        )
        ids = {r["slug"]: int(r["id"]) for r in df.to_dict("records")}
        missing = [slug for slug in wanted if slug not in ids]
        if missing:
            raise SystemExit(f"No chart on this server owns these slugs: {', '.join(missing)}")

    def resolve(ref: int | str) -> int:
        return ref if isinstance(ref, int) else ids[ref]

    return {resolve(src): resolve(tgt) for src, tgt in refs}


def key_chart_target_status(rows: list[dict], pairs: dict[int, int]) -> dict[tuple[int, int], int | None]:
    """keyChartLevel of each (tag, target) association the key-chart rows point at.

    None means the target is not tagged on that topic page at all. This answers the one
    question that decides whether a slot needs any work before the retirement: a target
    already keyed on the page means there is nothing to move.
    """
    wanted = {(int(r["surface_id"]), pairs[r["subject_id"]]) for r in rows if r["subject_id"] in pairs}
    if not wanted:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT tagId, chartId, keyChartLevel FROM chart_tags WHERE tagId IN %(t)s AND chartId IN %(c)s",
        params={"t": tuple(sorted({t for t, _ in wanted})), "c": tuple(sorted({c for _, c in wanted}))},
    )
    found = {(int(r["tagId"]), int(r["chartId"])): int(r["keyChartLevel"]) for r in df.to_dict("records")}
    return {k: found.get(k) for k in wanted}


def base_query(src_id: int, log_sources: set[int]) -> dict[str, str]:
    """The params the replacement link proposes for this row, before the reference's own."""
    base = dict(parse_qsl(REDIRECT_QUERY, keep_blank_values=True))
    if src_id in log_sources:
        base[Y_SCALE_KEY] = "log"
    return base


def params_cell(own_params: str, base_keys: set[str]) -> str:
    """The reference's own params, flagged because they defeat the redirect entirely.

    Verified against a live redirect that carries `target_query_param` — production, 2026-08-14,
    `global-forestry-area-1958-2014` → `forest-area-km?tab=line`:

        ?tab=map&country=~FRA  ->  /grapher/forest-area-km?tab=map&country=%7EFRA
        (no query string)      ->  /grapher/forest-area-km?tab=line

    The incoming query string **replaces** `target_query_param` wholesale; it is not merged
    key-by-key. So `target_query_param` only ever reaches a request that arrives with no query
    string at all, and ANY params on a reference — not just ones colliding with `tab`/`time`/
    `country` — mean the reader lands on the target's default view with the reference's own
    params and none of the retirement's. Flagging only key collisions would read as "the rest
    are fine", which is the opposite of what happens.

    `base_keys` still distinguishes the two grades: a collision is wrong twice over (the param
    is both lost and contradicted), while a non-colliding param is merely lost. It is this row's
    proposal rather than the shared constant, because `yScale` is only proposed for a log source.
    """
    query = (own_params or "").lstrip("?")
    if not query:
        return "— _(redirect applies)_"
    shown = f"`{fr.cell(query, 40)}`"
    clashing = sorted({key for key, _ in parse_qsl(query, keep_blank_values=True)} & base_keys)
    if clashing:
        return f"🔴 {shown} — kills the redirect's params, and contradicts {', '.join(clashing)}"
    return f"⚠️ {shown} — kills the redirect's params"


def page_link(r: dict, host: str, admin: str) -> str:
    """The published page, scrolled to the reference — page-TYPE aware.

    A gdoc's slug does not sit at the root for every type: a data insight is served under
    `/data-insights/` and an author page under `/team/`, while the sweep records `where_path`
    as `/<slug>` for all of them. A prose link's text fragment then attaches to the wrong
    base and makes a 404 look like a working link, so the type decides the base.
    """
    return rr.page_deep_link(r, host, admin)


def find_hint(r: dict) -> str:
    """Copy-paste search string for the doc's find box, truncated WITHOUT an ellipsis.

    A long anchor has to be cut somewhere, but `…` is not a character in the document, so a
    hint carrying one finds nothing when pasted. A literal prefix still matches, which is
    what the shared helper's `marker=""` is for.
    """
    hint = rr.cell(rr.find_in_doc(r), 55, marker="")
    return f"`{hint}`" if hint else "—"


def open_links(r: dict, host: str, admin: str) -> str:
    """The sweep's own "Open" composition, for the surfaces that are not Google Docs.

    `preview_url` is the actionable one here: on an explorer row it is the explorer page —
    the thing being re-pointed — while `admin_url` is the editor of the OLD chart the row
    points at. Dropping the preview leaves an explorer row with no way to open the explorer.
    """
    parts = []
    if r.get("admin_url"):
        # A narrative-chart row's `admin_url` is its OWN editor, not a chart's — labelling it
        # "chart admin" sends the reader looking for a chart editor that is not what opens.
        label = "narrative chart" if r["surface"] == "narrative chart" else "chart admin"
        parts.append(f"[✎ {label}]({r['admin_url']})")
    if r.get("preview_url"):
        parts.append(f"[👁 view]({r['preview_url']})")
    elif r.get("where_path"):
        parts.append(f"[🔗 open]({fr.admin_url(r['where_path'], host, admin)})")
    return " · ".join(parts) or "—"


def replacement_url(
    src_id: int, own_params: str, pairs: dict[int, int], slugs: dict[int, str], log_sources: set[int]
) -> str:
    """The target's scatter view, with the reference's own params layered on top.

    Incoming params beat the redirect's stored ones key by key, so a reference carrying its
    own `tab`/`time`/`country`/`yScale` keeps them — which is a decision, not a merge, and why
    the table prints the reference's params alongside.
    """
    tgt = pairs.get(src_id)
    if not tgt or tgt not in slugs:
        return "— no target —"
    merged = base_query(src_id, log_sources)
    merged.update(dict(parse_qsl((own_params or "").lstrip("?"), keep_blank_values=True)))
    return f"{SITE}/grapher/{slugs[tgt]}?{urlencode(merged)}"


def view_to_reproduce(src_id: int, pairs: dict[int, int], slugs: dict[int, str], log_sources: set[int]) -> str:
    """The target view a replacement narrative chart has to end up showing.

    NOT a "create it from here" link, and deliberately not `replacement_url` either:

    * There is no create-from control on a plain chart. `CreateNarrativeChartEditorPage`
      returns `NotFoundPage` unless `type === "multiDim"`, and the site-side affordance is
      gated on `manager.adminCreateNarrativeChartPath`, which only the MDIM components ever
      set (`site/multiDim/MultiDim.tsx`, `MultiDimDataPageContent.tsx`). A chart page — which
      is what every target here is — offers no such button, from the share menu or anywhere
      else. See `narrative_chart_mechanism()` for what that leaves.
    * It is the **scatter** view, with no reference params merged in. A narrative chart's
      `queryParamsForParentChart` routinely carry `tab=chart`, so a merged URL would name the
      target's line or slope view — the wrong view for a retirement that is about the scatter.
      Those params are its *"Explore the data"* href, where it sends readers to the parent, not
      what it renders; its own view comes from a materialized `configFull`.

    The old params are still printed in their own column, because nothing authored transfers to
    a replacement — FAUST, entity selection and time pins all have to be re-created by hand.
    """
    tgt = pairs.get(src_id)
    if not tgt or tgt not in slugs:
        return "— no target —"
    return f"{SITE}/grapher/{slugs[tgt]}?{urlencode(base_query(src_id, log_sources))}"


def narrative_chart_mechanism() -> list[str]:
    """The short how-to-replace intro. The full mechanism — why none of this is a UI task on a
    chart parent, with the grapher-source citations — lives in SKILL.md ("Narrative charts");
    the handoff states only what to do, because its reader is the curator, not the next agent.
    """
    return [
        "A narrative chart keeps rendering its old view from its own materialized config, so "
        "retirement breaks nothing — but it cannot be re-pointed, so each one is replaced by "
        "hand: **create the new narrative chart → edit every article below to its name → delete "
        "the old one last** (a published post still using the name blocks the delete). FAUST, "
        "entity selection and time pins do not transfer; re-create them from the params shown.",
        "",
        "How to create it depends on the target:",
        "",
        "- **Plain chart (every target here):** a chart page has no \"Create narrative chart\" "
        "control — ask a developer to create it via the API, or leave the old one in place "
        "(only its \"Explore the data\" link degrades to the target's default view).",
        "- **MDIM:** create it yourself, from the \"Create narrative chart\" control on the "
        "target view.",
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the reference handoff for an add-gdp-scatter retirement.")
    ap.add_argument("--references", required=True, type=Path, help="--json output of find_references.py")
    ap.add_argument("--pairs", required=True, type=Path, help="JSON pair list of the rows being retired")
    ap.add_argument("--gaps", type=Path, default=None, help="--gaps-json output of find_references.py")
    ap.add_argument("--output", type=Path, default=None, help="output .md (default: alongside --references)")
    ap.add_argument("--host", default=SITE, help="public site host for page links")
    args = ap.parse_args()

    rows = json.loads(args.references.read_text())
    pairs = load_pairs(args.pairs)
    # `admin_site`, NOT `admin_api`. The two differ in more than a suffix: on production
    # `admin_api` is the INTERNAL host (`owid-admin-prod.tail6e23.ts.net`), so deriving the
    # root from it produced preview links nobody outside the tailnet can open, while
    # `admin_site` is the public `https://admin.owid.io/admin`. Same expression the sweep uses
    # for its own `admin_url` values, which is why those were right in the same report these
    # were wrong in. On staging `admin_site` is already the short host, so no suffix stripping
    # is needed either.
    admin = (OWID_ENV.admin_site or "https://admin.owid.io/admin").rstrip("/")

    slugs = {}
    if pairs:
        df = OWID_ENV.read_sql(
            "SELECT c.id, cc.slug FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id IN %(i)s",
            params={"i": tuple(set(pairs.values()))},
        )
        slugs = {int(r["id"]): r["slug"] for r in df.to_dict("records")}
    log_sources = applier.log_y_axis_sources(set(pairs))

    out = [
        "# Reference handoff — retiring the old GDP scatters",
        "",
        f"{len(rows)} reference(s) to the {len(pairs)} charts about to be retired, each with where it should "
        "point instead. Do the 🔴 rows **before** Part 2 unpublishes anything.",
        "",
    ]

    # Every row a section below owns, by identity, so the catch-all at the end can list
    # whatever no section claimed instead of dropping it.
    placed: set[int] = set()

    for title, kind, blurb in [
        (
            "🔴 Embeds — a redirect does NOT fix these",
            "embed",
            "These render the old chart's own config, so they break the moment it is unpublished.",
        ),
        (
            "🟡 Links — the 301 keeps them working, but not necessarily on the scatter",
            "link",
            "None of these break. But a link carrying **any** query string loses the redirect's "
            "`tab=scatter&time=latest&country=` completely — the incoming query replaces "
            "`target_query_param` rather than merging with it (verified on production; see the "
            "Its-params column) — so those readers land on the target's default view, not the "
            "scatter this retirement moved them to. Only a param-less link inherits the scatter. "
            "Update the href and the question goes away, along with a hop nobody will remember.",
        ),
    ]:
        # Google Doc surfaces only: the shared formatters read `surface_id` AS a Doc id, and
        # on any other surface it is an explorer slug or a narrative-chart id, which would
        # render as a Doc link that resolves to nothing. Those surfaces have their own
        # sections below.
        group = [r for r in rows if r["kind"] == kind and r["surface"] in fr.GDOC_SURFACES]
        if not group:
            continue
        placed.update(id(r) for r in group)
        out += [
            f"## {title} ({len(group)})",
            "",
            blurb,
            "",
            "| Chart | Article | Open | Find in the doc | Its params | Replace with |",
            "|---|---|---|---|---|---|",
        ]
        for r in sorted(group, key=lambda r: (r["where"], r["subject"])):
            draft = "" if r["published"] else " ⚠️draft"
            ptype = r["context"].split("(")[-1].rstrip(")") if "(" in r.get("context", "") else ""
            page_type = f" _{ptype}_" if ptype else ""
            preview = f" · [👁 preview]({fr.gdoc_preview_url(r, admin)})" if r.get("surface_id") else ""
            links = f"[📄 doc]({fr.doc_url(r)}){preview}"
            page = page_link(r, args.host, admin)
            if page:
                links += f" · [🔗 page]({page})"
            if r.get("admin_url"):
                links += f" · [✎ chart admin]({r['admin_url']})"
            subject = (
                f"[`{fr.cell(r['subject_label'], 44)}`]({r['preview_url']})"
                if r.get("preview_url")
                else f"`{fr.cell(r['subject_label'], 44)}`"
            )
            own_params = r.get("query_string", "")
            base_keys = set(base_query(r["subject_id"], log_sources))
            out.append(
                f"| {subject} | {fr.cell(r['where'], 44)}{page_type}{draft} | {links} | {find_hint(r)} | "
                f"{params_cell(own_params, base_keys)} | "
                f"{replacement_url(r['subject_id'], own_params, pairs, slugs, log_sources)} |"
            )
        out.append("")

    manual = [r for r in rows if r["surface"] in MANUAL_SURFACES]
    placed.update(id(r) for r in manual)
    if manual:
        out += [
            f"## ⛔ Explorer / data-insight / static-viz references ({len(manual)}) — these BLOCK the row",
            "",
            "`redirect_to_scatter.py` converts these rows to `BLOCKED` before `--apply`, because they "
            "embed the old chart's config and no redirect covers them. Re-point them, then re-run with "
            "`--allow-manual-refs`. A data insight is itself a Google Doc, so it is listed in the table "
            "above too — with the links that locate the reference inside it.",
            "",
            "| Surface | Where | Old chart | Open |",
            "|---|---|---|---|",
        ]
        for r in sorted(manual, key=lambda r: (r["surface"], r["where"])):
            out.append(
                f"| {r['surface']} | {fr.cell(r['where'], 44)} | `{r['subject']}` | {open_links(r, args.host, admin)} |"
            )
        out.append("")

    narrative = [r for r in rows if r["surface"] == "narrative chart"]
    placements = [r for r in rows if r["surface"] == NARRATIVE_PLACEMENT_SURFACE]
    placed.update(id(r) for r in narrative + placements)
    if narrative:
        out += [
            f"## ℹ️ Narrative charts ({len(narrative)}) — replace by hand; nothing breaks meanwhile",
            "",
        ]
        out += narrative_chart_mechanism()
        for r in sorted(narrative, key=lambda r: str(r["where"])):
            own_params = r.get("query_string", "")
            mine = sorted(
                (p for p in placements if str(p["subject"]) == str(r["where"])),
                key=lambda p: str(p["where"]),
            )
            out += [
                "",
                f"### `{r['where']}` — on `{r['subject']}`",
                "",
                f"- **Replace with a narrative chart of:** {view_to_reproduce(r['subject_id'], pairs, slugs, log_sources)}",
                f"- **Its \"Explore the data\" params:** {params_cell(own_params, set(base_query(r['subject_id'], log_sources)))}",
                f"- **Open:** {open_links(r, args.host, admin)}",
                "",
            ]
            if not mine:
                # Not the same as "nothing to do": a narrative chart with no placement is
                # either unused (delete it and stop) or placed somewhere the link table does
                # not record, and saying which is not this report's job to guess.
                out.append(
                    "No article places it — `posts_gdocs_links` has no `narrative-chart` row for "
                    "this name. Either it is unused, in which case the replacement collapses to a "
                    "delete, or it is placed somewhere that table does not capture. Confirm before "
                    "deleting."
                )
                continue
            drafts = sum(1 for p in mine if not p["published"])
            note = f" ({drafts} unpublished)" if drafts else ""
            plural = f"{len(mine)} articles{note} — each one has" if len(mine) != 1 else f"1 article{note} — it has"
            out += [
                f"Placed in {plural} to be edited to the replacement's name:",
                "",
                "| Article | Open | Find in the doc |",
                "|---|---|---|",
            ]
            for p in mine:
                draft = "" if p["published"] else " ⚠️draft"
                ptype = p["context"].split("(")[-1].rstrip(")") if "(" in p.get("context", "") else ""
                page_type = f" _{ptype}_" if ptype else ""
                links = f"[📄 doc]({fr.doc_url(p)})"
                if p.get("surface_id"):
                    links += f" · [👁 preview]({fr.gdoc_preview_url(p, admin)})"
                page = page_link(p, args.host, admin)
                if page:
                    links += f" · [🔗 page]({page})"
                out.append(f"| {fr.cell(p['where'], 44)}{page_type}{draft} | {links} | {find_hint(p)} |")
        out.append("")

    # Last of the actionable sections, and deliberately so: it is the only one whose rows are
    # a tag association rather than a reference in a document, so it reads as a different kind
    # of task and interrupts the doc-editing run if it sits in the middle of it.
    key_charts = [r for r in rows if r["surface"] == "key chart"]
    placed.update(id(r) for r in key_charts)
    if key_charts:
        status = key_chart_target_status(key_charts, pairs)
        out += [
            f"## 🔴 Key-chart slots ({len(key_charts)}) — invisible to the Part 2 audit",
            "",
            "`redirect_to_scatter.py` never sees these: its audit counts wp/gdoc/explorer/narrative/"
            "dataInsight/staticViz, and a key chart is a chart↔tag association "
            "(`chart_tags.keyChartLevel`), not a row in any of them. Unpublishing the source 404s "
            "nothing — the chart simply drops out of the topic page's key-chart list, silently.",
            "",
            "**Target on the page?** says whether the slot needs any work: ✅ the target is already "
            "a key chart on that page (nothing to move — the old slot just drops out), 🟡 the target "
            "is tagged on the page but has no key-chart level (set it), 🔴 the target is not on the "
            "page at all (add the tag, then the level).",
            "",
            "| Topic page | Old chart | Level | Move the association to | Target on the page? |",
            "|---|---|---|---|---|",
        ]
        # Most critical first: a missing target, then an untagged one, then one tagged but not
        # keyed, then the rows where there is nothing to do — so the work reads top-down.
        graded = []
        for r in key_charts:
            tgt = pairs.get(r["subject_id"])
            st = status.get((int(r["surface_id"]), tgt)) if tgt else None
            if tgt is None:
                rank, verdict = 0, "— no target —"
            elif st is None:
                rank, verdict = 1, "🔴 not tagged"
            elif st == 0:
                rank, verdict = 2, "🟡 tagged, no key-chart level"
            else:
                rank, verdict = 3, f"✅ key chart, level {st}"
            graded.append((rank, r, tgt, verdict))
        for rank, r, tgt, verdict in sorted(graded, key=lambda g: (g[0], g[1]["where"], g[1]["subject"])):
            level = (r.get("context") or "").replace("keyChartLevel=", "level ")
            old_cell = f"`{r['subject']}` ([#{r['subject_id']}]({r['admin_url']}))"
            tgt_cell = f"[#{tgt}]({admin}/charts/{tgt}/edit) `{slugs.get(tgt, '?')}`" if tgt else "— no target —"
            out.append(f"| {fr.cell(r['where'], 44)} | {old_cell} | {level} | {tgt_cell} | {verdict} |")
        out.append("")

    # Anything no section above claimed — an old slug of the source, a site redirect, a
    # surface the sweep gains later. Listed rather than dropped, so the handoff cannot go
    # quiet about a reference the sweep did find.
    other = [r for r in rows if id(r) not in placed]
    if other:
        out += [
            f"## ❔ Other references ({len(other)}) — judge these by hand",
            "",
            "None of the sections above covers these. A `redirect` row is an old slug of the chart "
            "itself, which the unpublish deletes — `redirect_to_scatter.py` re-points those unless "
            "`--skip-alias-repoint` is passed.",
            "",
            "| Surface | Old chart | Where | Context | Open |",
            "|---|---|---|---|---|",
        ]
        for r in sorted(other, key=lambda r: (r["surface"], str(r["where"]))):
            out.append(
                f"| {r['surface']} | `{fr.cell(r['subject_label'], 44)}` | {fr.cell(r['where'], 44)} | "
                f"{fr.cell(r.get('context', ''), 44)} | {open_links(r, args.host, admin)} |"
            )
        out.append("")

    out += [
        "---",
        "",
        "The **chart name links to the view as that reference renders it** (its own params applied). "
        "📄 opens the Google Doc to edit · 🔗 opens the published page scrolled to the reference · "
        "👁 opens the article in the admin previewer (works for unpublished drafts too). "
        "**Find in the doc** is a copy-paste search string for the Google Doc: the link text for a "
        "prose hyperlink, or the chart slug for a block embed (the doc holds a bare grapher URL "
        "there — and the slug is stored as the author typed it, so it matches even when the doc "
        "still uses an old one). A long one is cut short but stays literal, with no `…` appended, "
        "so it can still be pasted straight into the find box.",
        "",
        "**Replace with** merges the redirect's stored params with the reference's own, and the "
        "reference's win. **Its params** is what the reference already carries, so you can see "
        f"what that merge did: a ⚠️ marks the ones overriding the retirement's own "
        f"`{REDIRECT_QUERY}` — that row lands the reader on a different tab or time range and "
        "needs a decision, not a blind paste.",
        "",
        "## Coverage boundary",
        "",
        "This handoff holds what the sweep *found*, which is not everything that exists — so a "
        "short table is not by itself a clean result, and nothing below was checked. The "
        "permanent limits are the sweep's own, restated here because this file is what gets "
        "handed on:",
        "",
    ]
    # Restated from `find_references.NOT_SEARCHED` rather than copied, for the same reason the
    # formatters are imported: a second copy drifts, and the drift reads as coverage we have.
    out += [f"- **Never swept:** {note}" for note in fr.NOT_SEARCHED]
    if args.gaps is None:
        out.append(
            "- ⚠️ **This run's own gaps were not carried over.** Re-run the sweep with `--gaps-json` "
            "and pass that file as `--gaps` to list them here — a surface that failed open (an "
            "absent table, a subject that did not resolve) is the gap a reader has no other way "
            "of knowing about."
        )
    else:
        run_gaps = json.loads(args.gaps.read_text())
        out += [f"- **Gap in this run:** {note}" for note in run_gaps]
        if not run_gaps:
            out.append("- The sweep reported no gaps of its own in this run.")
    out.append("")

    output = args.output or args.references.with_name(
        args.references.stem.replace("_references", "") + "_reference_handoff.md"
    )
    output.write_text("\n".join(out))
    tabled = {
        kind: len([r for r in rows if r["kind"] == kind and r["surface"] in fr.GDOC_SURFACES])
        for kind in ("embed", "link")
    }
    print(f"wrote {output}")
    print(
        f"  {len(rows)} reference(s): article embeds {tabled['embed']}, article links {tabled['link']}, "
        f"key charts {len(key_charts)}, manual {len(manual)}, narrative {len(narrative)} "
        f"(placed in {len(placements)} article(s)), other {len(other)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
