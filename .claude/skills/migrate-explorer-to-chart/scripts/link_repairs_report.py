"""Count the explorer links the migrations corrected, across every review folder.

An explorer URL names its view by display names (`?Metric=Confirmed+cases`), so renaming a
dimension or a choice silently breaks old links: the explorer misreads them and shows its default
view (or another one). `scan_references.py` repairs such a link before swapping it, so its new
multidim URL lands on the view the author meant; its `match` then reads "view, after repairing the
old link" and its `repairs` list says what was fixed. Multidim URLs name views by slug, which
doesn't change when a label does.

Reads every `<root>/*-review` folder (each may be a symlink into a migration worktree): the
pre-swap to-do list `references_baseline.json` (or `references_full.json` when no swap has
started), overlaid with the latest scan where both hold the same reference, so a re-scan with
newer repair logic counts. Writes `<root>/link_repairs.json` and `<root>/link_repairs_report.html`
(the page published as an artifact; republish it to the same artifact after each migration).

Usage:
    link_repairs_report.py [--root ai]
"""

import argparse
import html
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

# Surfaces a reader never sees: private WordPress copies, unused blocks, drafts, repo code.
HIDDEN_SURFACES = (
    "Legacy WordPress",
    "WordPress block (unused",
    "Google Doc (unpublished draft)",
    "Repo:",
    "Chart config (unattached)",
)
# Reference kinds that render the explorer itself, rather than link to it.
EMBED_HOWS = {"chart", "data-insight", "explorer-tiles", "pull-chart", "key-insight-slide"}
# When the scanner learned to repair links (commits on master), to say how complete a scan is.
REPAIR_LOGIC = datetime.fromisoformat("2026-11-12T17:29:16+01:00")  # #7264
FALLBACK_LOGIC = datetime.fromisoformat("2026-11-14T15:11:43+01:00")  # fc90af488d
# Repair notes from scan_references.py, by the kind of breakage they fix (first match wins).
CATEGORIES = [
    ("renamed choice", "A choice was renamed (“Sulphur dioxide (SO₂)” → “Sulfur dioxide”)", r"^renamed choice "),
    ("renamed dimension", "A dimension was renamed (“Metric” → “Indicator”)", r"^renamed “"),
    ("removed dimension", "A dimension the explorer no longer has (“Fuel=…”)", r"^dropped “[^”]*”, which the explorer no longer has"),
    ("unread parameter", "A parameter the explorer ignored, read as the one it meant", r"^read “"),
    ("value doesn't exist", "A value or combination the explorer doesn't have; it showed another view", r"doesn't exist here"),
    ("missing dimension", "The link left a dimension out; the explorer filled in its default", r"^added the missing "),
    ("extra text in a value", "A real choice followed by extra text (an old suffix like “(NOx)”, a missing “&”, a stray character)", r"^(cut|split|dropped the stray) "),
]


def is_visible(surface: str) -> bool:
    return not surface.startswith(HIDDEN_SURFACES)


def category(note: str) -> str:
    return next((name for name, _, pat in CATEGORIES if re.search(pat, note)), "other")


def load_refs(folder: Path) -> tuple[list[dict], str | None, datetime | None]:
    """The folder's references (baseline, overlaid with the latest scan), which files, and the scan time."""
    base, full = folder / "references_baseline.json", folder / "references_full.json"
    if not base.exists() and not full.exists():
        return [], None, None
    key = lambda r: (r["where"], r["old_url"], r.get("link_text"))  # noqa: E731
    latest = {key(r): r for r in json.loads(full.read_text())["references"]} if full.exists() else {}
    if base.exists():
        refs = [latest.get(key(r), r) for r in json.loads(base.read_text())["references"]]
        source = "baseline + latest scan" if full.exists() else "baseline"
    else:
        refs, source = list(latest.values()), "latest scan"
    scanned = max(datetime.fromtimestamp(p.stat().st_mtime).astimezone() for p in (base, full) if p.exists())
    return refs, source, scanned


def summarize(refs: list[dict]) -> dict:
    corrected = [r for r in refs if r.get("match", "").startswith("view, after repairing")]
    stale = [r for r in refs if "stale params" in r.get("match", "")]
    bare = [r for r in refs if r.get("match", "") == "catch-all (no params)"]
    cats = Counter(c for r in corrected for c in sorted({category(n) for n in r.get("repairs") or []}))
    return {
        "references": len(refs),
        "embeds": sum(r.get("how") in EMBED_HOWS for r in refs),
        "links": sum(r.get("how") not in EMBED_HOWS for r in refs),
        "naming_a_view": len(refs) - len(bare),
        "corrected": len(corrected),
        "corrected_embeds": sum(r.get("how") in EMBED_HOWS for r in corrected),
        "still_default": len(stale),
        "breaks_on_redirect": sum(bool(r.get("breaks_on_redirect")) for r in refs),
        "categories": dict(sorted(cats.items())),
    }


def build(root: Path) -> dict:
    status_file = root / "migration_status.json"
    status = json.loads(status_file.read_text()) if status_file.exists() else {}
    explorers = []
    for folder in sorted(root.glob("*-review")):
        slug = folder.name.removesuffix("-review")
        cfg = json.loads((folder / "review.json").read_text()) if (folder / "review.json").exists() else {}
        refs, source, scanned = load_refs(folder)
        visible = [r for r in refs if is_visible(r["surface"])]
        hidden = [r for r in refs if not is_visible(r["surface"])]
        if scanned is None:
            coverage = "not scanned"
        elif scanned < REPAIR_LOGIC:
            coverage = "scanned before link repair existed: corrected links not counted"
        elif scanned < FALLBACK_LOGIC:
            coverage = "scanned before missing-value repair: corrected links undercounted"
        else:
            coverage = "full"
        examples = [
            {"where": r["where"], "surface": r["surface"], "old_url": r["old_url"], "new_url": r["new_url"], "repairs": r.get("repairs") or []}
            for r in visible
            if r.get("match", "").startswith("view, after repairing")
        ]
        explorers.append(
            {
                "slug": slug,
                "migrated": cfg.get("stage") == "retire",
                "stage": cfg.get("stage"),
                "source": source,
                "scanned": scanned.strftime("%Y-%m-%d") if scanned else None,
                "coverage": coverage,
                "visible": summarize(visible),
                "hidden": summarize(hidden),
                "examples": examples,
            }
        )
    for m in status.get("migrated_without_review", []):
        explorers.append({"slug": m["slug"], "migrated": True, "stage": "retire", "source": None, "scanned": None,
                          "coverage": "not scanned (migrated before review pages)", "visible": summarize([]),
                          "hidden": summarize([]), "examples": []})
    explorers.sort(key=lambda e: (not e["migrated"], e["slug"]))

    def total(group: list[dict], part: str) -> dict:
        keys = ["references", "embeds", "links", "naming_a_view", "corrected", "corrected_embeds", "still_default", "breaks_on_redirect"]
        out = {k: sum(e[part][k] for e in group) for k in keys}
        cats = Counter()
        for e in group:
            cats.update(e[part]["categories"])
        out["categories"] = dict(sorted(cats.items()))
        out["explorers_affected"] = sum(1 for e in group if e[part]["corrected"])
        out["explorers"] = len(group)
        out["explorers_scanned"] = sum(1 for e in group if not e["coverage"].startswith("not scanned"))
        return out

    migrated = [e for e in explorers if e["migrated"]]
    return {
        "migrated": total(migrated, "visible"),
        "migrated_hidden": total(migrated, "hidden"),
        "all": total(explorers, "visible"),
        "categories": [{"name": n, "meaning": m} for n, m, _ in CATEGORIES],
        "explorers": explorers,
    }


def pct(a: int, b: int) -> str:
    return f"{100 * a / b:.0f}%" if b else "—"


def render(data: dict) -> str:
    e = html.escape
    m, a, h = data["migrated"], data["all"], data["migrated_hidden"]
    rows = []
    for x in data["explorers"]:
        v = x["visible"]
        badge = "Migrated" if x["migrated"] else f"In progress ({x['stage']})" if x["stage"] else "In progress"
        cov = "" if x["coverage"] == "full" else f"<div class='note'>{e(x['coverage'])}</div>"
        rows.append(
            f"<tr class='{'done' if x['migrated'] else 'wip'}'><td><b>{e(x['slug'])}</b>{cov}</td>"
            f"<td><span class='pill {'ok' if x['migrated'] else 'wip'}'>{e(badge)}</span></td>"
            f"<td class='n'>{v['references']}</td><td class='n'>{v['naming_a_view']}</td>"
            f"<td class='n strong'>{v['corrected'] or '·'}</td><td class='n'>{pct(v['corrected'], v['naming_a_view']) if v['corrected'] else ''}</td>"
            f"<td class='n'>{v['still_default'] or '·'}</td><td class='n muted'>{x['hidden']['corrected'] or '·'}</td>"
            f"<td class='muted'>{e(x['scanned'] or '—')}</td></tr>"
        )
    cat_total = sum(m["categories"].values()) or 1
    meaning = {c["name"]: c["meaning"] for c in data["categories"]}
    cat_rows = "".join(
        f"<div class='bar'><div class='bl'><b>{e(n)}</b><span>{e(meaning.get(n, ''))}</span></div>"
        f"<div class='track'><div class='fill' style='width:{100 * c / max(m['categories'].values()):.1f}%'></div></div>"
        f"<div class='bn'>{c}</div></div>"
        for n, c in sorted(m["categories"].items(), key=lambda kv: (-kv[1], kv[0]))
    ) or "<p class='muted'>No corrected links yet.</p>"
    # One example per category first, then by explorer, from migrated explorers.
    seen, examples = set(), []
    pool = [(x["slug"], ex) for x in data["explorers"] if x["migrated"] for ex in x["examples"]]
    for slug, ex in pool:
        cats = {category(n) for n in ex["repairs"]}
        if cats - seen and len(examples) < 7:
            seen |= cats
            examples.append((slug, ex))
    # Fill up with one example per explorer not shown yet.
    shown = {slug for slug, _ in examples}
    for slug, ex in pool:
        if len(examples) >= 7:
            break
        if slug not in shown:
            shown.add(slug)
            examples.append((slug, ex))
    ex_html = "".join(
        f"<div class='ex'><div class='exh'><b>{e(slug)}</b> · {e(ex['where'])}</div>"
        f"<div class='url old'><span>Before</span><code>{e(ex['old_url'])}</code></div>"
        f"<div class='url new'><span>After</span><code>{e(ex['new_url'])}</code></div>"
        f"<ul>{''.join(f'<li>{e(n)}</li>' for n in ex['repairs'])}</ul></div>"
        for slug, ex in examples
    )
    return f"""<title>Explorer Link Repairs</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&family=Space+Grotesk:wght@500;600&display=swap">
<style>
/* Layout: one reading column; summary figures, then the per-explorer ledger, breakdown, examples, method. */
:root {{
  --bg: #f6f7f9; --panel: #ffffff; --fg: #1b2330; --muted: #5d6878; --line: #dde2ea;
  --accent: #1f5fbf; --accent-soft: #e3ecfa; --ok: #1d7a4c; --ok-soft: #e1f2e9; --wip: #8a5a00; --wip-soft: #fbefd6;
  --old: #a3361f; --font-display: "Space Grotesk", "IBM Plex Sans", system-ui, sans-serif;
  --font-body: "IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif; --font-mono: "IBM Plex Mono", ui-monospace, SFMono-Regular, Menlo, monospace;
}}
@media (prefers-color-scheme: dark) {{ :root:not([data-theme="light"]) {{
  --bg: #12161d; --panel: #1a2029; --fg: #e3e8ef; --muted: #98a3b3; --line: #2c3542;
  --accent: #79a8f0; --accent-soft: #1d2b42; --ok: #6fcf9b; --ok-soft: #173326; --wip: #e5b45a; --wip-soft: #3a2d12; --old: #ef8b74; color-scheme: dark; }} }}
:root[data-theme="dark"] {{
  --bg: #12161d; --panel: #1a2029; --fg: #e3e8ef; --muted: #98a3b3; --line: #2c3542;
  --accent: #79a8f0; --accent-soft: #1d2b42; --ok: #6fcf9b; --ok-soft: #173326; --wip: #e5b45a; --wip-soft: #3a2d12; --old: #ef8b74; color-scheme: dark; }}
body {{ background: var(--bg); color: var(--fg); font: 15px/1.55 var(--font-body); }}
.wrap {{ max-width: 980px; margin: 0 auto; padding-block: 32px 64px; padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px)); display: grid; gap: 36px; }}
h1, h2 {{ font-family: var(--font-display); text-wrap: balance; margin: 0; }}
h1 {{ font-size: 2rem; font-weight: 600; }} h2 {{ font-size: 1.25rem; font-weight: 600; }}
.lede {{ color: var(--muted); max-width: 65ch; margin: 8px 0 0; }}
section {{ display: grid; gap: 14px; min-width: 0; }}
.figs {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; }}
.fig {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px; display: grid; gap: 2px; }}
.fig .v {{ font: 600 2rem/1.1 var(--font-display); font-variant-numeric: tabular-nums; }}
.fig .v.hl {{ color: var(--accent); }}
.fig .l {{ color: var(--muted); font-size: .85rem; }}
.tablebox {{ overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; background: var(--panel); }}
table {{ border-collapse: collapse; width: 100%; font-size: .88rem; }}
th, td {{ padding: 8px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
th {{ font-size: .72rem; letter-spacing: .04em; text-transform: uppercase; color: var(--muted); font-weight: 600; white-space: nowrap; }}
tr:last-child td {{ border-bottom: 0; }}
td.n {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }}
td.strong {{ font-weight: 600; color: var(--accent); }}
.muted {{ color: var(--muted); }}
.note {{ color: var(--wip); font-size: .78rem; }}
.pill {{ display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: .75rem; white-space: nowrap; }}
.pill.ok {{ background: var(--ok-soft); color: var(--ok); }} .pill.wip {{ background: var(--wip-soft); color: var(--wip); }}
.bar {{ display: grid; grid-template-columns: minmax(0, 1.4fr) minmax(80px, 1fr) 3ch; gap: 12px; align-items: center; }}
.bl {{ display: grid; min-width: 0; }} .bl span {{ color: var(--muted); font-size: .82rem; }}
.track {{ height: 10px; background: var(--accent-soft); border-radius: 5px; overflow: hidden; }}
.fill {{ height: 100%; background: var(--accent); }}
.bn {{ text-align: right; font-variant-numeric: tabular-nums; font-weight: 600; }}
.ex {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; display: grid; gap: 6px; min-width: 0; }}
.exh {{ font-size: .85rem; color: var(--muted); }} .exh b {{ color: var(--fg); }}
.url {{ display: grid; grid-template-columns: 4.5em minmax(0, 1fr); gap: 8px; align-items: baseline; }}
.url span {{ font-size: .72rem; text-transform: uppercase; letter-spacing: .04em; color: var(--muted); }}
.url code {{ font: .8rem/1.45 var(--font-mono); overflow-wrap: anywhere; }}
.url.old code {{ color: var(--old); }} .url.new code {{ color: var(--ok); }}
.ex ul {{ margin: 2px 0 0; padding-left: 18px; font-size: .84rem; }}
.method {{ max-width: 70ch; color: var(--fg); }} .method li {{ margin-bottom: 6px; }}
@media (max-width: 520px) {{ h1 {{ font-size: 1.6rem; }} .bar {{ grid-template-columns: 1fr 3ch; }} .track {{ grid-column: 1 / -1; grid-row: 2; }} }}
</style>
<div class="wrap">
<header>
<h1>Explorer Link Repairs</h1>
<p class="lede">Explorer URLs name a view by its labels, so renaming a dimension or a choice quietly breaks links to it: the explorer misreads them and shows its default view. When an explorer moves to a multidim, each link is repaired before it is swapped, so it lands on the view its author meant. Multidim URLs name views by slug, which stays the same when a label changes.</p>
</header>
<section>
<h2>Across migrated explorers</h2>
<div class="figs">
<div class="fig"><div class="v hl">{m['corrected']}</div><div class="l">links corrected: they showed the wrong view and now land on the intended one</div></div>
<div class="fig"><div class="v">{pct(m['corrected'], m['naming_a_view'])}</div><div class="l">of the {m['naming_a_view']} references that name a specific view ({pct(m['corrected'], m['references'])} of all {m['references']})</div></div>
<div class="fig"><div class="v">{m['explorers_affected']} of {m['explorers_scanned']}</div><div class="l">scanned migrated explorers had at least one corrected link ({m['explorers'] - m['explorers_scanned']} more were migrated before scans existed)</div></div>
<div class="fig"><div class="v">{m['corrected_embeds']}</div><div class="l">of the corrected references were embedded charts, the rest links</div></div>
</div>
<p class="muted" style="margin:0">Counts only what readers see: published articles, topic pages and data insights, site redirects, chart and indicator text, featured metrics. Private WordPress copies, unpublished drafts and code add another {h['corrected']} corrected references. Including explorers still being migrated: {a['corrected']} corrected of {a['naming_a_view']} references that name a view.</p>
</section>
<section>
<h2>By explorer</h2>
<div class="tablebox"><table>
<thead><tr><th>Explorer</th><th>Status</th><th class="n">References</th><th class="n">Name a view</th><th class="n">Corrected</th><th class="n">Share</th><th class="n">Still default</th><th class="n">Hidden copies</th><th>Scanned</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></div>
<p class="muted" style="margin:0">“Still default”: the old link carries parameters the scan couldn't map, so it went to the default view before and still does. “Hidden copies”: corrected references in private WordPress copies, drafts and code, not counted above.</p>
</section>
<section>
<h2>What was broken</h2>
<p class="muted" style="margin:0">Corrected links in migrated explorers, by the kind of repair. A link can need more than one.</p>
{cat_rows}
</section>
<section>
<h2>Examples</h2>
{ex_html or "<p class='muted'>No corrected links yet.</p>"}
</section>
<section class="method">
<h2>How this is counted</h2>
<ul>
<li>Each migration scans the production database and the code for every reference to the explorer. A reference is <b>corrected</b> when the live explorer misread its URL (it showed its default or another view) and the repaired multidim URL lands on a specific view.</li>
<li>The count uses each explorer's list of references from before any link was swapped, updated with its latest scan, so a later scan with better repair logic counts too.</li>
<li>The scanner learned to repair links on 2026-11-12 and to fill in missing or non-existent values on 2026-11-14. Explorers scanned earlier are marked in the table, and their corrected counts are a floor. Later scanner fixes (spelling variants, removed dimensions) may add a few more to older scans.</li>
<li>Explorers migrated before review pages existed were not scanned and count as zero.</li>
</ul>
</section>
</div>
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("ai"), help="folder holding the *-review folders")
    args = ap.parse_args()
    data = build(args.root)
    (args.root / "link_repairs.json").write_text(json.dumps(data, indent=1, ensure_ascii=False))
    (args.root / "link_repairs_report.html").write_text(render(data))
    m = data["migrated"]
    print(f"migrated explorers: {m['corrected']} corrected of {m['naming_a_view']} references naming a view ({m['references']} total), {m['explorers_affected']}/{m['explorers']} explorers")
    print(f"-> {args.root / 'link_repairs.json'}\n-> {args.root / 'link_repairs_report.html'}")


if __name__ == "__main__":
    main()
