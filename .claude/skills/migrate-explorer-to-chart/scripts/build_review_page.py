"""Build the migration review page: explorer and multidim side by side, checks, links, mapping, payload.

Inputs, all from earlier steps of the migration:
- `<review>/eval.json` and `<review>/shots/` from `compare_views.py`;
- `<review>/references_full.json` from `scan_references.py`;
- `<mapping-dir>/mapping.json` and `admin_bulk_payload.json` from `map-explorer-to-mdim`;
- optional `--notes notes.json`: `{"<check>:<field prefix>": "why this difference is expected"}`.
  A difference with a note counts as explained, not as a mismatch. Write one only after checking
  the difference against the rendered pages.

Writes `<review>/index.html`. Publish it with the Artifact tool, passing the screenshots as
`files` (`{"shots/<name>": "<review>/shots/<name>"}`), so the page can show them.

Usage:
    .venv/bin/python .claude/skills/migrate-explorer-to-chart/scripts/build_review_page.py \
        --review ai/<slug>-review --mapping-dir ai/<slug>-mdim-mapping \
        --payload-env staging|production [--notes ai/<slug>-review/notes.json] [--stage review|links|redirects]
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

READER_CHECKS = ["mapping", "indicators", "data", "metadata", "rendered"]


def dimension_summary(mapping: dict) -> list[dict]:
    """Per explorer dimension: the multidim dimension it became and each choice's slug."""
    pairs = [(r["source"]["dimensions"], (r["target"] or {}).get("dimensions") or {}) for r in mapping["redirects"]]
    out = []
    for name in mapping["explorer"]["dimensions"]:
        best = None
        for slug in {k for _, t in pairs for k in t}:
            rel = defaultdict(set)
            for s, t in pairs:
                rel[s.get(name, "")].add(t.get(slug))
            if all(len(v) == 1 for v in rel.values()):
                choices = {k: next(iter(v)) for k, v in rel.items()}
                score = len(set(choices.values()))
                if best is None or score > best[0]:
                    best = (score, slug, choices)
        out.append(
            {
                "explorer": name,
                "multidim": best[1] if best else None,
                "choices": [{"explorer": k, "multidim": v} for k, v in (best[2] if best else {}).items()],
            }
        )
    return out


def classify(pairs: list[dict], notes: dict) -> None:
    """Mark each difference as explained (has a note) or not, and give each view a verdict."""
    for p in pairs:
        for c in p["checks"]:
            key = f"{c['check'].split(':')[0]}:{c['field']}"
            c["note"] = next((v for k, v in notes.items() if key.startswith(k)), None)
        visible = [c for c in p["checks"] if c["check"].split(":")[0] in READER_CHECKS and not c["note"]]
        other = [c for c in p["checks"] if c["check"].split(":")[0] not in READER_CHECKS and not c["note"]]
        p["verdict"] = "differs" if visible else ("config" if other else "match")


def render_page(review: Path, live: bool = False) -> str:
    """The page for one review folder, from the inputs recorded in its `review.json`."""
    cfg = json.loads((review / "review.json").read_text())
    mapping_dir, payload_env, stage = Path(cfg["mapping_dir"]), cfg["payload_env"], cfg["stage"]
    notes_path = Path(cfg["notes"]) if cfg.get("notes") else None
    ev = json.loads((review / "eval.json").read_text())
    refs = json.loads((review / "references_full.json").read_text())
    mapping = json.loads((mapping_dir / "mapping.json").read_text())
    payload = json.loads((mapping_dir / "admin_bulk_payload.json").read_text())
    notes = json.loads(notes_path.read_text()) if notes_path else {}
    classify(ev["pairs"], notes)
    data = {
        "eval": ev,
        "refs": refs,
        "dims": dimension_summary(mapping),
        "payload": payload,
        "payloadEnv": payload_env,
        "stage": stage,
        "catchAll": mapping["catchAll"],
        "live": live,
    }
    title = f"{ev['explorer']['slug']} migration"
    return TEMPLATE.replace("__TITLE__", title).replace("__DATA__", json.dumps(data).replace("</", "<\\/"))


def main(review: Path, mapping_dir: Path, payload_env: str, notes_path: Path | None, stage: str) -> None:
    cfg = {
        "mapping_dir": str(mapping_dir),
        "payload_env": payload_env,
        "stage": stage,
        "notes": str(notes_path) if notes_path else None,
    }
    (review / "review.json").write_text(json.dumps(cfg, indent=1))
    # Static copy with screenshots, for sharing with someone who can't reach staging.
    (review / "index.html").write_text(render_page(review, live=False))
    ev = json.loads((review / "eval.json").read_text())
    print(f"{len(ev['pairs'])} views -> {review / 'review.json'}, {review / 'index.html'}")
    print("Live side by side: .venv/bin/python .claude/skills/migrate-explorer-to-chart/scripts/review_app.py")


TEMPLATE = r"""<title>__TITLE__</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Sans+Condensed:wght@600;700&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>
/* Layout: one column of work sections; the comparison is a two-up "light table" of screenshots. */
:root{
  --bg:#f5f6f8; --panel:#ffffff; --fg:#18202b; --muted:#5d6775; --line:#dfe3e8; --soft:#eef1f5;
  --accent:#2457a6; --accent-fg:#ffffff;
  --ok:#1e7a48; --ok-bg:#e3f3ea; --bad:#b42318; --bad-bg:#fbe7e5; --warn:#8a5a00; --warn-bg:#fbf0d6;
  --display:"IBM Plex Sans Condensed","Arial Narrow",system-ui,sans-serif;
  --body:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",sans-serif;
  --mono:"IBM Plex Mono",ui-monospace,"SFMono-Regular",Menlo,monospace;
}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){
  --bg:#12161c; --panel:#1a2029; --fg:#e6eaf0; --muted:#9aa4b2; --line:#2b3340; --soft:#202733;
  --accent:#7fa8ee; --accent-fg:#0d1420;
  --ok:#6fcf97; --ok-bg:#163322; --bad:#f28b82; --bad-bg:#3a1a18; --warn:#e8b65a; --warn-bg:#33270f; color-scheme:dark}}
:root[data-theme="dark"]{
  --bg:#12161c; --panel:#1a2029; --fg:#e6eaf0; --muted:#9aa4b2; --line:#2b3340; --soft:#202733;
  --accent:#7fa8ee; --accent-fg:#0d1420;
  --ok:#6fcf97; --ok-bg:#163322; --bad:#f28b82; --bad-bg:#3a1a18; --warn:#e8b65a; --warn-bg:#33270f; color-scheme:dark}
body{background:var(--bg);color:var(--fg);font-family:var(--body);font-size:15px;line-height:1.5}
.wrap{max-width:1320px;margin:0 auto;padding-left:max(16px,env(safe-area-inset-left,0px));padding-right:max(16px,env(safe-area-inset-right,0px));padding-block:28px 64px;display:flex;flex-direction:column;gap:36px}
h1,h2,h3{font-family:var(--display);margin:0;text-wrap:balance;letter-spacing:.005em}
h1{font-size:2rem;line-height:1.15} h2{font-size:1.35rem} h3{font-size:1rem}
a{color:var(--accent)} code,.mono{font-family:var(--mono);font-size:.84em}
.eyebrow{font-size:.74rem;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);font-weight:600}
.lede{max-width:72ch;color:var(--muted);margin:6px 0 0}
section{display:flex;flex-direction:column;gap:14px}
.sec-head{display:flex;flex-direction:column;gap:4px}
.sec-head p{margin:0;color:var(--muted);max-width:80ch}
.pill{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:2px 10px;font-size:.78rem;font-weight:600;white-space:nowrap}
.pill.ok{background:var(--ok-bg);color:var(--ok)} .pill.bad{background:var(--bad-bg);color:var(--bad)} .pill.warn{background:var(--warn-bg);color:var(--warn)} .pill.neutral{background:var(--soft);color:var(--muted)}
.stages{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}
.stage{border:1px solid var(--line);border-radius:6px;padding:6px 12px;font-size:.85rem;color:var(--muted);background:var(--panel)}
.stage.now{border-color:var(--accent);color:var(--fg);font-weight:600}
.stage.done{color:var(--ok)}
.checks{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px}
.check{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:12px 14px;display:flex;flex-direction:column;gap:6px}
.check b{font-size:.92rem}
.check span{font-size:.82rem;color:var(--muted)}
.nav{position:sticky;top:env(safe-area-inset-top,0px);z-index:5;background:var(--bg);padding-block:10px;border-bottom:1px solid var(--line);display:flex;flex-wrap:wrap;gap:10px 16px;align-items:end}
body::before{content:"";position:fixed;inset:0 0 auto;height:env(safe-area-inset-top,0px);z-index:10;background:var(--bg)}
.field{display:flex;flex-direction:column;gap:3px;min-width:0}
.field label{font-size:.72rem;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);font-weight:600}
select,button{font:inherit;font-size:.9rem;color:var(--fg);background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:6px 10px}
select{max-width:100%}
button{cursor:pointer} button:hover{border-color:var(--accent)}
button:focus-visible,select:focus-visible,a:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.seg{display:inline-flex;border:1px solid var(--line);border-radius:6px;overflow:hidden}
.seg button{border:0;border-radius:0;background:var(--panel)} .seg button[aria-pressed="true"]{background:var(--accent);color:var(--accent-fg)}
.viewbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-left:auto}
.twoup{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media (max-width:860px){.twoup{grid-template-columns:1fr}}
.pane{background:var(--panel);border:1px solid var(--line);border-radius:8px;overflow:hidden;display:flex;flex-direction:column;min-width:0}
.pane-head{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:8px 12px;border-bottom:1px solid var(--line);font-size:.85rem}
.pane-head b{font-family:var(--display);font-size:.95rem}
.pane img{display:block;width:100%;height:auto;background:#fff}
.pane iframe{display:block;width:100%;height:min(78vh,760px);border:0;background:#fff}
.pane .empty{padding:40px 16px;color:var(--muted);text-align:center}
.pane-url{padding:6px 12px;border-top:1px solid var(--line);font-family:var(--mono);font-size:.72rem;color:var(--muted);overflow-wrap:anywhere}
.difflist{display:flex;flex-direction:column;gap:8px}
.diff{background:var(--panel);border:1px solid var(--line);border-left:4px solid var(--bad);border-radius:6px;padding:10px 12px;font-size:.86rem;display:grid;grid-template-columns:auto 1fr;gap:4px 12px}
.diff.explained{border-left-color:var(--warn)}
.diff dt{color:var(--muted)} .diff dd{margin:0;min-width:0;overflow-wrap:anywhere;font-family:var(--mono);font-size:.78rem}
.okline{background:var(--ok-bg);color:var(--ok);border-radius:6px;padding:10px 12px;font-size:.9rem;font-weight:500}
.tablebox{overflow-x:auto;border:1px solid var(--line);border-radius:8px;background:var(--panel)}
table{border-collapse:collapse;width:100%;font-size:.86rem}
th,td{padding:7px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
th{font-size:.72rem;letter-spacing:.05em;text-transform:uppercase;color:var(--muted);font-weight:600;background:var(--soft);white-space:nowrap}
tbody tr:last-child td{border-bottom:0}
tr.pick{cursor:pointer} tr.pick:hover td{background:var(--soft)} tr.sel td{background:var(--soft)}
td.c{text-align:center;font-variant-numeric:tabular-nums}
.y{color:var(--ok);font-weight:600} .n{color:var(--bad);font-weight:600} .e{color:var(--warn);font-weight:600} .na{color:var(--muted)}
.url{font-family:var(--mono);font-size:.74rem;overflow-wrap:anywhere;max-width:40ch}
.find{min-width:16ch;max-width:34ch} .find b{font-weight:600}
button.mini{font-size:.72rem;padding:1px 6px;margin-left:6px;vertical-align:1px}
.group{display:flex;flex-direction:column;gap:8px}
.group-head{display:flex;flex-wrap:wrap;gap:8px 14px;align-items:baseline}
.dimgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px}
.dimcard{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:12px 14px;display:flex;flex-direction:column;gap:8px;min-width:0}
.dimcard h3{display:flex;gap:8px;align-items:baseline;flex-wrap:wrap}
.arrow{color:var(--muted)}
pre{margin:0;max-height:420px;overflow:auto;background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:12px;font-family:var(--mono);font-size:.74rem;line-height:1.45}
.banner{border-radius:8px;padding:10px 14px;font-size:.9rem}
.banner.bad{background:var(--bad-bg);color:var(--bad)} .banner.ok{background:var(--ok-bg);color:var(--ok)}
.row{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.small{font-size:.8rem;color:var(--muted)}
ol.next{margin:0;padding-left:1.3em;display:flex;flex-direction:column;gap:6px;max-width:80ch}
@media (prefers-reduced-motion: reduce){*{scroll-behavior:auto!important}}
</style>

<div class="wrap" id="app">
  <header>
    <div class="eyebrow">Explorer to multidim migration · owid/etl#6014</div>
    <h1 id="h1"></h1>
    <p class="lede" id="lede"></p>
    <div class="stages" id="stages"></div>
  </header>

  <section id="summary">
    <div class="sec-head"><h2>Checks</h2><p id="sumtext"></p></div>
    <div class="checks" id="checks"></div>
  </section>

  <section id="compare">
    <div class="sec-head"><h2>Side by side</h2><p>Production explorer on the left, staging multidim on the right. The controls pick the explorer view; the right side shows the multidim view it redirects to. Live pages need the staging server reachable (Tailscale); screenshots are from a headless browser at 1280px.</p></div>
    <div class="nav" id="nav"></div>
    <div class="twoup" id="panes"></div>
    <div class="difflist" id="vdiffs"></div>
  </section>

  <section id="matrix">
    <div class="sec-head"><h2>Every view</h2><p>✓ identical · ✗ differs · ≈ differs, explained in a note · – not rendered. Pick a row to open it above.</p></div>
    <div class="tablebox"><table id="mtable"></table></div>
  </section>

  <section id="links">
    <div class="sec-head"><h2>Links to replace</h2><p id="linkscope"></p></div>
    <div id="linkgroups" class="group"></div>
  </section>

  <section id="dims">
    <div class="sec-head"><h2>Dimension mapping</h2><p id="dimtext"></p></div>
    <div class="dimgrid" id="dimgrid"></div>
  </section>

  <section id="payload">
    <div class="sec-head"><h2>Redirects to create</h2><p>Paste into <b>Bulk-create redirects from JSON</b> at <a href="https://admin.owid.io/admin/multi-dim-redirects" target="_blank" rel="noopener">admin.owid.io/admin/multi-dim-redirects</a>. Creating them takes the explorer offline at once, and they can only be removed one at a time.</p></div>
    <div id="pbanner"></div>
    <div class="row"><button id="copyPayload">Copy JSON</button><span class="small" id="pstats"></span></div>
    <pre id="pjson"></pre>
  </section>

  <section id="nextsec">
    <div class="sec-head"><h2>What happens next</h2></div>
    <ol class="next" id="nextSteps"></ol>
  </section>
</div>

<script>
const D = __DATA__;
const E = D.eval, P = E.pairs, $ = id => document.getElementById(id);
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const show = v => v === null || v === undefined ? "—" : (typeof v === "string" ? v : JSON.stringify(v));
const dimNames = E.explorer.dimensions;
const family = c => c.check.split(":")[0];

$("h1").textContent = `${E.explorer.slug} → /grapher/${E.multidim.slug}`;
$("lede").innerHTML = `Live explorer: <a href="${E.explorer.url}" target="_blank" rel="noopener">${esc(E.explorer.url)}</a>. Staging multidim: <a href="${E.multidim.staging_url}" target="_blank" rel="noopener">${esc(E.multidim.staging_url)}</a> (<a href="${E.multidim.staging_admin}" target="_blank" rel="noopener">admin preview</a>). Catalog path <code>${esc(E.multidim.catalogPath)}</code>.`;
const STAGES = [["review","1. Review the multidim, merge the PR"],["links","2. Replace links in docs"],["redirects","3. Create the redirects"],["retire","4. Unpublish and archive"]];
const si = STAGES.findIndex(s => s[0] === D.stage);
$("stages").innerHTML = STAGES.map((s,i) => `<span class="stage ${i===si?"now":i<si?"done":""}">${i<si?"✓ ":""}${s[1]}</span>`).join("");

// Summary of checks.
const fam = {
  structure:{label:"Views and mapping", test:p=>!p.checks.some(c=>family(c)==="mapping")},
  indicators:{label:"Indicators and display names", test:p=>!p.checks.some(c=>family(c)==="indicators")},
  data:{label:"Data (checksums)", test:p=>!p.checks.some(c=>family(c)==="data")},
  metadata:{label:"Metadata (checksums)", test:p=>!p.checks.some(c=>family(c)==="metadata")},
  rendered:{label:"What readers see", test:p=>!p.checks.some(c=>family(c)==="rendered" && !c.note), rendered:true},
  config:{label:"Chart config", test:p=>!p.checks.some(c=>family(c)==="config" && !c.note)},
};
const nMatch = P.filter(p=>p.verdict==="match").length, nDiff = P.filter(p=>p.verdict==="differs").length;
$("sumtext").textContent = `${nMatch} of ${P.length} views match on every check${nDiff?`; ${nDiff} differ in something readers see`:""}. Rendered checks ran on ${E.rendered_ids.length} of ${P.length} views, on the default tab and the other main tab.`;
$("checks").innerHTML = Object.values(fam).map(f => {
  const pool = f.rendered ? P.filter(p=>E.rendered_ids.includes(p.id)) : P;
  const ok = pool.filter(f.test).length, all = pool.length;
  const explained = P.some(p=>p.checks.some(c=>c.note && (f.label.startsWith("Chart")?family(c)==="config":family(c)==="rendered")));
  const cls = ok===all ? (explained?"warn":"ok") : "bad";
  return `<div class="check"><b>${f.label}</b><span class="pill ${cls}">${ok} / ${all} views${ok===all&&explained?" · with notes":""}</span></div>`;
}).join("");

// Unified navigation.
let cur = P[0].id, tab = "default";
const choicesOf = d => [...new Set(P.map(p=>p.explorer_dims[d] ?? ""))];
$("nav").innerHTML = dimNames.map((d,i)=>`<div class="field"><label for="d${i}">${esc(d)}</label><select id="d${i}" data-d="${esc(d)}">${choicesOf(d).map(v=>`<option value="${esc(v)}">${v===""?"(blank)":esc(v)}</option>`).join("")}</select></div>`).join("")
 + `<div class="field"><label>Tab</label><div class="seg" role="group"><button id="tabDefault" aria-pressed="true">Default tab</button><button id="tabOther" aria-pressed="false">Other tab</button></div></div>`
 + (D.live ? `<div class="field"><label>Show</label><div class="seg" role="group"><button id="modeLive" aria-pressed="true">Live</button><button id="modeShot" aria-pressed="false">Screenshots</button></div></div>` : "")
 + `<div class="viewbar"><button id="prev" aria-label="Previous view">←</button><span id="vlabel" class="small"></span><button id="nextBtn" aria-label="Next view">→</button></div>`;
const selects = [...document.querySelectorAll("#nav select")];
function pickFrom(changed){
  const want = Object.fromEntries(selects.map(s=>[s.dataset.d, s.value]));
  let best = null, score = -1;
  for (const p of P){
    if (changed && (p.explorer_dims[changed] ?? "") !== want[changed]) continue;
    const s = dimNames.filter(d => (p.explorer_dims[d] ?? "") === want[d]).length;
    if (s > score){ best = p; score = s; }
  }
  if (best) go(best.id);
}
selects.forEach(s => s.addEventListener("change", () => pickFrom(s.dataset.d)));
$("tabDefault").onclick = () => { tab="default"; render(); };
$("tabOther").onclick = () => { tab="other"; render(); };
if (D.live) { $("modeLive").onclick = () => { mode="live"; render(); }; $("modeShot").onclick = () => { mode="shot"; render(); }; }
const idx = () => P.findIndex(p=>p.id===cur);
$("prev").onclick = () => go(P[(idx()-1+P.length)%P.length].id);
$("nextBtn").onclick = () => go(P[(idx()+1)%P.length].id);
document.addEventListener("keydown", e => { if (e.target.tagName==="SELECT") return; if (e.key==="ArrowLeft") $("prev").click(); if (e.key==="ArrowRight") $("nextBtn").click(); });
function go(id){ cur = id; try{ localStorage.setItem("view", id) }catch(e){} render(); }

let mode = D.live ? "live" : "shot";
function pane(side, r, url, label){
  const live = `<a href="${esc(r?r.url:url)}" target="_blank" rel="noopener">Open live ↗</a>`;
  if (mode === "live") {
    const src = r ? r.url : (tab === "other" ? null : url);
    const body = src ? `<iframe src="${esc(src)}" title="${esc(label)}" loading="lazy"></iframe>` : `<div class="empty">This view has no second tab.</div>`;
    return `<div class="pane"><div class="pane-head"><b>${label}</b>${live}</div>${body}<div class="pane-url">${esc(src||url)}</div></div>`;
  }
  const body = !r ? `<div class="empty">Not rendered for this view${tab==="other"?" (no second tab)":""}.</div>`
    : r.error ? `<div class="empty">Page failed to render: ${esc(r.error)}</div>`
    : `<img src="${esc(r.shot)}" alt="${esc(label)}, ${esc(r.facts.activeTab||"")} tab" loading="lazy">`;
  return `<div class="pane"><div class="pane-head"><b>${label}</b>${r&&r.facts?`<span class="pill neutral">${esc(r.facts.activeTab||"")}</span>`:""}${live}</div>${body}<div class="pane-url">${esc(r?r.url:url)}</div></div>`;
}
function render(){
  const p = P.find(x=>x.id===cur);
  selects.forEach(s => { s.value = p.explorer_dims[s.dataset.d] ?? ""; });
  $("tabDefault").setAttribute("aria-pressed", tab==="default"); $("tabOther").setAttribute("aria-pressed", tab==="other");
  if (D.live) { $("modeLive").setAttribute("aria-pressed", mode==="live"); $("modeShot").setAttribute("aria-pressed", mode==="shot"); }
  const rr = (p.rendered||{})[tab] || null;
  const verdict = {match:'<span class="pill ok">Match</span>', config:'<span class="pill warn">Config differs</span>', differs:'<span class="pill bad">Differs</span>'}[p.verdict];
  $("vlabel").innerHTML = `View ${idx()+1} of ${P.length} · ${verdict}`;
  $("panes").innerHTML = pane("explorer", rr&&rr.explorer, p.explorer_url, "Production explorer") + pane("multidim", rr&&rr.multidim, p.multidim_url, `Staging multidim · ${esc(p.multidim_view||"no target")}`);
  const checks = p.checks;
  $("vdiffs").innerHTML = checks.length ? checks.map(c => `<dl class="diff ${c.note?"explained":""}"><dt>Check</dt><dd>${esc(c.check)} · ${esc(c.field)}</dd><dt>Explorer</dt><dd>${esc(show(c.explorer))}</dd><dt>Multidim</dt><dd>${esc(show(c.multidim))}</dd>${c.note?`<dt>Note</dt><dd style="font-family:var(--body);font-size:.84rem">${esc(c.note)}</dd>`:""}</dl>`).join("")
    : `<div class="okline">✓ This view matches on indicators, display names, data and metadata checksums, chart config${E.rendered_ids.includes(p.id)?", and what readers see on both tabs":""}.</div>`;
  document.querySelectorAll("#mtable tr.pick").forEach(tr => tr.classList.toggle("sel", +tr.dataset.id===cur));
}

// Matrix.
const cols = [["mapping","Mapped"],["indicators","Indicators"],["data","Data"],["metadata","Metadata"],["rendered:default","Default tab"],["rendered:other","Other tab"],["config","Config"]];
function cell(p, key){
  if (key.startsWith("rendered") && !(p.rendered||{})[key.split(":")[1]]) return '<td class="c na">–</td>';
  const cs = p.checks.filter(c => key.includes(":") ? c.check===key : family(c)===key);
  if (!cs.length) return '<td class="c y">✓</td>';
  return cs.every(c=>c.note) ? '<td class="c e">≈</td>' : '<td class="c n">✗</td>';
}
$("mtable").innerHTML = `<thead><tr><th>#</th>${dimNames.map(d=>`<th>${esc(d)}</th>`).join("")}<th>Multidim view</th>${cols.map(c=>`<th>${c[1]}</th>`).join("")}</tr></thead><tbody>`
  + P.map(p => `<tr class="pick" data-id="${p.id}" tabindex="0"><td class="c">${p.id}</td>${dimNames.map(d=>`<td>${esc(p.explorer_dims[d]||"(blank)")}</td>`).join("")}<td class="mono">${esc(Object.values(p.multidim_dims||{}).join(" · "))}</td>${cols.map(c=>cell(p,c[0])).join("")}</tr>`).join("") + "</tbody>";
document.querySelectorAll("#mtable tr.pick").forEach(tr => { tr.onclick = () => { go(+tr.dataset.id); $("compare").scrollIntoView({behavior:"smooth"}); }; tr.onkeydown = e => { if (e.key==="Enter") tr.click(); }; });

// Links to replace.
const R = D.refs.references;
const breaks = R.filter(r=>r.breaks_on_redirect).length;
$("linkscope").innerHTML = `${R.length} reference${R.length===1?"":"s"} found, ${breaks} of them break the moment the redirects exist (swap those first). Scanned ${D.refs.scanned_columns} text and JSON columns in ${D.refs.scanned_tables.length} production tables, plus the ${D.refs.repos.join(" and ")} repo${D.refs.repos.length>1?"s":""}. Skipped history and analytics tables: <span class="mono">${D.refs.skipped_tables.join(", ")}</span>.`;
// What to search for in the doc: the linked words (or card title), and the text just before them.
const findCell = r => {
  if (!r.link_text && !r.context) return '<span class="small">—</span>';
  const what = r.link_text ? `<b>“${esc(r.link_text)}”</b><button class="mini" data-copy="${esc(r.link_text)}">Copy</button>` : `<span class="small">${r.how==="chart"?"embed, below:":"after:"}</span>`;
  return `${what}${r.context?`<div class="small">…${esc(r.context.trim())}</div>`:""}`;
};
const howLabel = r => r.breaks_on_redirect ? `<span class="pill bad">${esc(r.how)} · breaks</span>` : r.how==="docs example" ? `<span class="pill neutral">docs example</span>` : `<span class="pill neutral">${esc(r.how)} · redirect covers it</span>`;
const groups = {};
R.forEach(r => { const k = r.surface + "|" + r.where.replace(/:\d+$/,""); (groups[k] = groups[k] || []).push(r); });
const order = Object.entries(groups).sort((a,b) => (b[1].some(r=>r.breaks_on_redirect)-a[1].some(r=>r.breaks_on_redirect)) || (a[0].startsWith("Repo")-b[0].startsWith("Repo")));
$("linkgroups").innerHTML = order.length ? order.map(([k, rs]) => {
  const [surface, where] = k.split("|"), r0 = rs[0];
  const links = [r0.edit_url&&`<a href="${esc(r0.edit_url)}" target="_blank" rel="noopener">Edit ↗</a>`, r0.preview_url&&`<a href="${esc(r0.preview_url)}" target="_blank" rel="noopener">Admin preview ↗</a>`, r0.page_url&&`<a href="${esc(r0.page_url)}" target="_blank" rel="noopener">Live page ↗</a>`].filter(Boolean).join(" · ");
  return `<div class="group"><div class="group-head"><h3>${esc(where)}</h3><span class="small">${esc(surface)} · ${rs.length} link${rs.length===1?"":"s"}</span><span class="small">${links}</span></div>
    <div class="tablebox"><table><thead><tr><th>Use</th><th>Find in the doc</th><th>Old URL</th><th>New URL</th><th>Lands on</th><th></th></tr></thead><tbody>${rs.map((r,i)=>`<tr><td>${howLabel(r)}${r.where.match(/:\d+$/)?`<div class="small mono">line ${r.where.split(":").pop()}</div>`:""}</td><td class="find">${findCell(r)}</td><td class="url">${esc(r.old_url)}</td><td class="url">${esc(r.new_url)}</td><td>${esc(r.match)}${Object.keys(r.stale_params||{}).length?` <span class="pill warn">stale params</span>`:""}</td><td><button data-copy="${esc(r.new_url)}">Copy new</button></td></tr>`).join("")}</tbody></table></div></div>`;
}).join("") : `<div class="okline">✓ Nothing references this explorer.</div>`;
document.querySelectorAll("button[data-copy]").forEach(b => b.onclick = async () => { try{ await navigator.clipboard.writeText(b.dataset.copy); b.textContent="Copied"; }catch(e){ b.textContent="Copy failed"; } });

// Dimension mapping.
$("dimtext").innerHTML = `Explorer URLs name dimensions and choices by their labels (<code>?Measure=Conflict+deaths</code>); multidim URLs by slug (<code>?measure=deaths</code>). Each redirect rewrites one into the other and keeps every other parameter (<code>country</code>, <code>time</code>, <code>tab</code>). A bare <code>/explorers/${esc(E.explorer.slug)}</code> goes to the multidim's default view.`;
$("dimgrid").innerHTML = D.dims.map(d => `<div class="dimcard"><h3>${esc(d.explorer)} <span class="arrow">→</span> <code>${esc(d.multidim||"?")}</code></h3>
  <div class="tablebox"><table><thead><tr><th>Explorer choice</th><th>Multidim choice</th></tr></thead><tbody>${d.choices.map(c=>`<tr><td>${c.explorer===""?'<span class="small">(blank: not in the URL)</span>':esc(c.explorer)}</td><td class="mono">${esc(c.multidim)}</td></tr>`).join("")}</tbody></table></div></div>`).join("");

// Payload.
const prodPayload = D.payloadEnv === "production";
$("pbanner").innerHTML = prodPayload ? `<div class="banner ok">✓ Built and checked against production. This is the version to paste.</div>`
  : `<div class="banner bad">Preview built against staging. Each view's config id differs on production, so this version must not be pasted: it gets rebuilt and checked against production after the PR merges, and this section updates.</div>`;
const nRules = (D.payload.redirects||[]).filter(r=>r.target).length + (D.payload.catchAll?1:0);
$("pstats").textContent = `${nRules} redirect rules (${(D.payload.redirects||[]).length} views + the bare explorer URL).`;
$("pjson").textContent = JSON.stringify(D.payload, null, 2);
$("copyPayload").onclick = async e => { try{ await navigator.clipboard.writeText($("pjson").textContent); e.target.textContent="Copied"; }catch(err){ const r=document.createRange(); r.selectNodeContents($("pjson")); getSelection().removeAllRanges(); getSelection().addRange(r); e.target.textContent="Selected, press Ctrl+C"; } };

// Next steps.
const NEXT = {
  review: ["Review the side by side and the checks above. If you're happy, merge the PR.", "After the merge, publish the multidim in the admin; I check /grapher/" + E.multidim.slug + " loads.", "Then swap the links listed above and republish the docs.", "I rebuild the redirects against production and run the pre-apply check; this page updates."],
  links: ["Swap the links above that break on redirect, and the others while you're there. Republish each doc.", "Tell me when done: I rerun the scan, which must come back empty, and the pre-apply check."],
  redirects: ["Paste the JSON above. I check a few old URLs return 302 to the right views.", "Then unpublish the explorer; I open the retirement PR."],
  retire: ["Unpublish the explorer in the admin.", "Merge the retirement PR (explorer step and any orphaned dataset archived)."],
}[D.stage] || [];
$("nextSteps").innerHTML = NEXT.map(s=>`<li>${esc(s)}</li>`).join("");

try{ const v = +localStorage.getItem("view"); if (P.some(p=>p.id===v)) cur = v; }catch(e){}
render();
</script>
"""


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--review", type=Path, required=True)
    ap.add_argument("--mapping-dir", type=Path, required=True)
    ap.add_argument("--payload-env", choices=["staging", "production"], required=True)
    ap.add_argument("--notes", type=Path, default=None)
    ap.add_argument("--stage", choices=["review", "links", "redirects", "retire"], default="review")
    a = ap.parse_args()
    main(a.review, a.mapping_dir, a.payload_env, a.notes, a.stage)
