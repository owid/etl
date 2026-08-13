"""Turn a find-chart-references sweep into the actionable handoff for retiring the sources.

The sweep says what points at the charts about to be retired; this adds the half that is
specific to this workflow — **where each reference should point instead** — and keeps the
sweep's own presentation, because that presentation is what makes a row fixable:

* **📄 doc** — the Google Doc to edit. `posts_gdocs.id` IS the Doc id, so it is a direct link.
* **👁 preview** — the article in the admin previewer, which renders unpublished drafts too.
* **🔗 page** — the published page, deep-linked to the reference with a scroll-to-text
  fragment when the reference has anchor text.
* **Find in the doc** — a copy-paste search string: the link text for a prose hyperlink, or
  the chart slug for a block embed (the doc holds a bare grapher URL there, and the slug is
  stored as the author typed it, so it matches even when the doc still uses an old one).

Without those, a handoff row names an article and leaves the editor to hunt through it. The
formatters are **imported** from `find-chart-references/scripts/find_references.py` rather
than reimplemented, so the two reports cannot drift apart.

Usage::

    .venv/bin/python .claude/skills/add-gdp-scatter/scripts/build_reference_handoff.py \\
        --references ai/<name>_references.json \\
        --pairs ai/<name>_part2_pairs.json \\
        [--output ai/<name>_reference_handoff.md]

`--references` is the `--json` output of `find_references.py`; `--pairs` is the applier's own
JSON, so the replacement URLs are the ones the migration actually created.
"""

import argparse
import importlib.util
import json
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlencode

from etl.config import OWID_ENV

TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")

# Kept in step with TARGET_QUERY in redirect_to_scatter.py: every param stands in for an
# adjustment a tab CLICK makes and a URL-supplied tab does not get.
REDIRECT_QUERY = "tab=scatter&time=latest&country="
SITE = "https://ourworldindata.org"

MANUAL_SURFACES = {"explorer", "data insight", "static viz"}


def _load_find_references():
    """Import the sweep's own formatters so the two reports share one presentation."""
    path = Path(__file__).resolve().parents[2] / "find-chart-references" / "scripts" / "find_references.py"
    if not path.exists():
        raise SystemExit(f"Cannot find the shared formatters at {path}")
    spec = importlib.util.spec_from_file_location("find_references", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


fr = _load_find_references()


def chart_id(url: str) -> int:
    return int(url.rstrip("/").split("/")[-2])


def replacement_url(src_id: int, own_params: str, pairs: dict[int, int], slugs: dict[int, str]) -> str:
    """The target's scatter view, with the reference's own params layered on top.

    Incoming params beat the redirect's stored ones key by key, so a reference carrying its
    own `tab`/`time`/`country` keeps them — which is a decision, not a merge, and why the
    table prints the reference's params alongside.
    """
    tgt = pairs.get(src_id)
    if not tgt or tgt not in slugs:
        return "— no target —"
    merged = dict(parse_qsl(REDIRECT_QUERY, keep_blank_values=True))
    merged.update(dict(parse_qsl((own_params or "").lstrip("?"), keep_blank_values=True)))
    return f"{SITE}/grapher/{slugs[tgt]}?{urlencode(merged)}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the reference handoff for an add-gdp-scatter retirement.")
    ap.add_argument("--references", required=True, type=Path, help="--json output of find_references.py")
    ap.add_argument("--pairs", required=True, type=Path, help="the applier's JSON pair list (Part 2 set)")
    ap.add_argument("--output", type=Path, default=None, help="output .md (default: alongside --references)")
    ap.add_argument("--host", default=SITE, help="public site host for page links")
    args = ap.parse_args()

    rows = json.loads(args.references.read_text())
    pairs = {
        chart_id(p["chart_admin_url"]): chart_id(p["target_chart_admin_url"])
        for p in json.loads(args.pairs.read_text())
    }
    # The shared formatters expect an admin ROOT (".../admin"), while OWID_ENV.admin_api is
    # the API root (".../admin/api"). The tailscale suffix comes off so these links read the
    # same as the sweep's own `admin_url` values, which are already short.
    admin = TAILSCALE_SUFFIX_RE.sub("", OWID_ENV.admin_api).removesuffix("/api")

    slugs = {}
    if pairs:
        df = OWID_ENV.read_sql(
            "SELECT c.id, cc.slug FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id IN %(i)s",
            params={"i": tuple(set(pairs.values()))},
        )
        slugs = {int(r["id"]): r["slug"] for r in df.to_dict("records")}

    out = [
        "# Reference handoff — retiring the old GDP scatters",
        "",
        f"{len(rows)} reference(s) to the {len(pairs)} charts about to be retired, each with where it should "
        "point instead. Do the 🔴 rows **before** Part 2 unpublishes anything.",
        "",
    ]

    for title, kind, blurb in [
        (
            "🔴 Embeds — a redirect does NOT fix these",
            "embed",
            "These render the old chart's own config, so they break the moment it is unpublished.",
        ),
        (
            "🟡 Links — the 301 covers them, but update the href",
            "link",
            "They keep working through the redirect; updating avoids a hop nobody will remember.",
        ),
    ]:
        group = [r for r in rows if r["kind"] == kind]
        if not group:
            continue
        out += [
            f"## {title} ({len(group)})",
            "",
            blurb,
            "",
            "| Chart | Article | Open | Find in the doc | Replace with |",
            "|---|---|---|---|---|",
        ]
        for r in sorted(group, key=lambda r: (r["where"], r["subject"])):
            draft = "" if r["published"] else " ⚠️draft"
            ptype = r["context"].split("(")[-1].rstrip(")") if "(" in r.get("context", "") else ""
            page_type = f" _{ptype}_" if ptype else ""
            preview = f" · [👁 preview]({fr.gdoc_preview_url(r, admin)})" if r.get("surface_id") else ""
            links = f"[📄 doc]({fr.doc_url(r)}){preview} · [🔗 page]({fr.deep_link(r, args.host, admin)})"
            if r.get("admin_url"):
                links += f" · [✎ chart admin]({r['admin_url']})"
            subject = (
                f"[`{fr.cell(r['subject_label'], 44)}`]({r['preview_url']})"
                if r.get("preview_url")
                else f"`{fr.cell(r['subject_label'], 44)}`"
            )
            out.append(
                f"| {subject} | {fr.cell(r['where'], 44)}{page_type}{draft} | {links} | {fr.search_hint(r)} | "
                f"{replacement_url(r['subject_id'], r.get('query_string', ''), pairs, slugs)} |"
            )
        out.append("")

    key_charts = [r for r in rows if r["surface"] == "key chart"]
    if key_charts:
        out += [
            f"## 🔴 Key-chart slots ({len(key_charts)}) — invisible to the Part 2 audit",
            "",
            "`redirect_to_scatter.py` never sees these: its audit counts wp/gdoc/explorer/narrative/"
            "dataInsight/staticViz, and a key chart is a chart↔tag association "
            "(`chart_tags.keyChartLevel`), not a row in any of them. Unpublishing the source 404s "
            "nothing — the chart simply drops out of the topic page's key-chart list, silently. Move "
            "each association to the target, same tag and same level.",
            "",
            "| Topic page | Old chart | Level | Move the association to |",
            "|---|---|---|---|",
        ]
        for r in sorted(key_charts, key=lambda r: (r["where"], r["subject"])):
            tgt = pairs.get(r["subject_id"])
            level = (r.get("context") or "").replace("keyChartLevel=", "level ")
            out.append(
                f"| {fr.cell(r['where'], 44)} | `{r['subject']}` (#{r['subject_id']}) | {level} | "
                f"#{tgt} `{slugs.get(tgt, '?')}` |"
            )
        out.append("")

    manual = [r for r in rows if r["surface"] in MANUAL_SURFACES]
    if manual:
        out += [
            f"## ⛔ Explorer / data-insight / static-viz references ({len(manual)}) — these BLOCK the row",
            "",
            "`redirect_to_scatter.py` converts these rows to `BLOCKED` before `--apply`, because they "
            "embed the old chart's config and no redirect covers them. Re-point them, then re-run with "
            "`--allow-manual-refs`.",
            "",
            "| Surface | Where | Old chart | Open |",
            "|---|---|---|---|",
        ]
        for r in sorted(manual, key=lambda r: (r["surface"], r["where"])):
            link = f"[✎ admin]({r['admin_url']})" if r.get("admin_url") else "—"
            out.append(f"| {r['surface']} | {fr.cell(r['where'], 44)} | `{r['subject']}` | {link} |")
        out.append("")

    narrative = [r for r in rows if r["surface"] == "narrative chart"]
    if narrative:
        out += [f"## ℹ️ Narrative charts ({len(narrative)}) — do not block", ""]
        for r in narrative:
            out.append(
                f"- `{r['where']}` on `{r['subject']}` — params `{r.get('query_string') or '—'}`. Renders from "
                f'its own materialized config, and its "Explore the data" href is covered by the redirect; '
                f"its params override the stored ones."
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
        "still uses an old one). A `—` means there is nothing to search for.",
        "",
        "**Replace with** merges the redirect's stored params with the reference's own, and the "
        "reference's win — so a row whose params column is not `—` needs a decision, not a "
        "blind paste.",
        "",
        'This handoff covers only what the sweep found. Read its own "Not searched" section for the '
        "coverage boundary — a short table is not by itself a clean result.",
        "",
    ]

    output = args.output or args.references.with_name(
        args.references.stem.replace("_references", "") + "_reference_handoff.md"
    )
    output.write_text("\n".join(out))
    counts = {k: len([r for r in rows if r["kind"] == k]) for k in ("embed", "link", "render")}
    print(f"wrote {output}")
    print(
        f"  embeds {counts['embed']}  links {counts['link']}  render {counts['render']}"
        f"  (key charts {len(key_charts)}, manual {len(manual)}, narrative {len(narrative)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
