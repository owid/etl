"""Generate a self-contained HTML to review a chart → MDIM view mapping.

Consumes the output of ``extract_and_match.py`` (the ``ai/<name>-charts-mdim-mapping``
folder, specifically ``mapping_proposal.csv``) and renders a single HTML file where
a human steps through each (chart, proposed MDIM view) pair side-by-side and
approves / flags the match. Decisions persist in the browser ``localStorage`` and
can be mirrored to a JSON file on disk (Chrome/Edge File System Access) or
restored via Import.

Unlike the explorer reviewer (``review-explorer-mdim-mapping``), every row has its
own left-hand URL (each chart is its own /grapher/<slug> page), and no
``mapping_rules.py`` is involved — the proposal CSV is self-contained. Rows without
a proposed target (ambiguous / near-miss / none / conflict) show their candidate
info instead of a right-hand iframe, so they can be triaged into ``overrides.csv``.

Both panes are rendered against one base URL: by default the host recorded by
``extract_and_match.py`` in ``_sources.json`` (i.e. the environment the mapping
was extracted from — a staging-extracted mapping is reviewed against staging,
not production). ``--host`` overrides it.

Usage::

    .venv/bin/python .claude/skills/map-charts-to-mdim/scripts/build_review.py \\
        --mapping-dir ai/<name>-charts-mdim-mapping \\
        [--host https://ourworldindata.org] \\
        [--output ai/<name>_chart_mdim_review.html]
"""

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


def recorded_host(mapping_dir: Path) -> tuple[str, str] | None:
    """Host the mapping was extracted against, as recorded in _sources.json."""
    src = mapping_dir / "_sources.json"
    if src.exists():
        host = (json.loads(src.read_text()).get("host") or "").rstrip("/")
        if host:
            return host, f"recorded at extraction in {src.name}"
    return None


def parse_proposal(mapping_dir: Path) -> list[dict]:
    proposal = mapping_dir / "mapping_proposal.csv"
    if not proposal.exists():
        raise SystemExit(f"Not found: {proposal}. Run extract_and_match.py from map-charts-to-mdim first.")
    with open(proposal) as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise SystemExit(f"{proposal} has no data rows.")

    records = []
    for r in rows:
        target_url = (r.get("target_url") or "").strip()
        target_path = ""
        if target_url:
            p = urlparse(target_url)
            target_path = p.path + (f"?{p.query}" if p.query else "")
        records.append(
            {
                "id": (r.get("chart_id") or "").strip(),
                "chart_slug": (r.get("chart_slug") or "").strip(),
                "chart_title": (r.get("chart_title") or "").strip(),
                "chart_path": f"/grapher/{(r.get('chart_slug') or '').strip()}",
                "quality": (r.get("match_quality") or "").strip(),
                # Part of the decision fingerprint: an approval is made on a specific
                # version of the source chart, not just on its id.
                "config_md5": (r.get("chart_config_md5") or "").strip(),
                "target_mdim": (r.get("target_mdim_slug") or "").strip(),
                "view_id": (r.get("target_view_id") or "").strip(),
                "target_path": target_path,
                "shared_with": (r.get("shared_target_chart_ids") or "").strip(),
                "conflict": (r.get("conflict") or "").strip(),
                "candidates": (r.get("candidate_view_ids") or "").strip(),
                "near_miss": (r.get("near_miss_detail") or "").strip(),
                "csv_note": (r.get("note") or "").strip(),
            }
        )
    return records


def coverage_report(records: list[dict]) -> None:
    print("─" * 66)
    print("COVERAGE")
    print("─" * 66)
    counts = Counter(r["quality"] for r in records)
    print(f"  charts: {len(records)}   |   " + "  ".join(f"{q}: {n}" for q, n in counts.most_common()))
    with_target = [r for r in records if r["target_path"]]
    distinct_targets = {(r["target_mdim"], r["view_id"]) for r in with_target}
    shared = [r for r in with_target if r["shared_with"]]
    conflicted = [r for r in with_target if r["conflict"]]
    print(f"  with a proposed target: {len(with_target)}   |   distinct MDIM views: {len(distinct_targets)}")
    print(f"  many-to-one rows: {len(shared)}   |   conflicts: {len(conflicted)}")
    print("─" * 66)


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>__TITLE__</title>
<style>
  :root {
    --bg: #f5f6f8; --card: #fff; --line: #e2e5ea; --ink: #1a1a1a; --muted: #6b7280;
    --blue: #1d3d63; --green: #1a8f4c; --amber: #c47f00; --red: #b3261e;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
         background: var(--bg); color: var(--ink); }
  header { position: sticky; top: 0; z-index: 10; background: var(--card); border-bottom: 1px solid var(--line);
           padding: 10px 16px; }
  .topline { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
  h1 { font-size: 16px; margin: 0; font-weight: 650; }
  .counts { display: flex; gap: 8px; font-size: 13px; }
  .pill { padding: 2px 9px; border-radius: 999px; border: 1px solid var(--line); background: #fafbfc; }
  .pill.green { color: var(--green); border-color: #b6e3c6; background: #f0fbf4; }
  .pill.amber { color: var(--amber); border-color: #f0dca6; background: #fdf8ec; }
  .pill.todo  { color: var(--muted); }
  .pill.saved { color: var(--green); border-color: #b6e3c6; background: #f0fbf4; }
  .spacer { flex: 1; }
  button { font: inherit; cursor: pointer; border: 1px solid var(--line); background: #fff; border-radius: 8px;
           padding: 6px 12px; }
  button:hover { background: #f3f4f6; }
  button.primary { background: var(--blue); color: #fff; border-color: var(--blue); }
  button.approve { background: var(--green); color: #fff; border-color: var(--green); }
  button.flag { background: var(--amber); color: #fff; border-color: var(--amber); }
  button.ghost { background: transparent; }
  button.on { background: #eaf6ee; border-color: #b6e3c6; color: var(--green); }
  .filters { display: flex; gap: 6px; margin-top: 8px; font-size: 13px; flex-wrap: wrap; align-items: center; }
  .filters .chip { padding: 3px 10px; border-radius: 999px; border: 1px solid var(--line); background: #fff; cursor: pointer; }
  .filters .chip.active { background: var(--ink); color: #fff; border-color: var(--ink); }
  main { padding: 14px 16px 90px; }
  .meta { display: flex; gap: 16px; align-items: baseline; flex-wrap: wrap; margin-bottom: 10px; font-size: 13px; }
  .meta .rowid { font-weight: 700; }
  .quality-tag { font-weight: 600; padding: 1px 8px; border-radius: 999px; border: 1px solid var(--line); }
  .quality-tag.exact { color: var(--green); border-color: #b6e3c6; background: #f0fbf4; }
  .quality-tag.forced { color: var(--blue); }
  .quality-tag.ambiguous, .quality-tag.near_miss { color: var(--amber); border-color: #f0dca6; background: #fdf8ec; }
  .quality-tag.none, .quality-tag.skipped { color: var(--muted); }
  .status-tag { font-weight: 600; }
  .status-tag.approved { color: var(--green); }
  .status-tag.flagged { color: var(--amber); }
  .conflict-line { color: var(--red); font-size: 13px; margin-bottom: 8px; }
  .panes { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  @media (max-width: 900px) { .panes { grid-template-columns: 1fr; } }
  .pane { background: var(--card); border: 1px solid var(--line); border-radius: 10px; overflow: hidden; display: flex; flex-direction: column; }
  .pane h2 { font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: var(--muted);
             margin: 0; padding: 8px 12px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; }
  .pane .sel { padding: 8px 12px; font-size: 14px; }
  .pane .sel code { background: #f1f3f6; border-radius: 5px; padding: 1px 6px; font-size: 12px; }
  .pane .url { padding: 0 12px 8px; font-size: 11px; }
  .pane .url a { color: var(--blue); text-decoration: none; word-break: break-all; }
  .pane iframe { width: 100%; height: 540px; border: 0; border-top: 1px solid var(--line); background: #fff; }
  .pane .info { padding: 12px; border-top: 1px solid var(--line); font-size: 13px; color: var(--muted);
                min-height: 540px; white-space: pre-wrap; word-break: break-word; }
  footer { position: fixed; bottom: 0; left: 0; right: 0; background: var(--card); border-top: 1px solid var(--line);
           padding: 10px 16px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  .nav { display: flex; gap: 6px; align-items: center; }
  .note { flex: 1; min-width: 180px; }
  .note input { width: 100%; font: inherit; padding: 7px 10px; border: 1px solid var(--line); border-radius: 8px; }
  select { font: inherit; padding: 6px 8px; border: 1px solid var(--line); border-radius: 8px; }
  .settings { display: none; margin-top: 10px; padding: 12px; background: #fbfcfd; border: 1px solid var(--line); border-radius: 10px; }
  .settings.open { display: block; }
  .settings .warn { color: var(--muted); font-size: 13px; margin: 0 0 10px; }
  .settings label { display: block; font-size: 12px; color: var(--muted); margin: 8px 0 2px; }
  .settings input { width: 100%; font: inherit; padding: 6px 9px; border: 1px solid var(--line); border-radius: 7px; }
  .kbd { font-size: 11px; color: var(--muted); }
  .kbd b { background: #eef1f5; border-radius: 4px; padding: 1px 5px; border: 1px solid var(--line); }
  .toast { position: fixed; left: 50%; bottom: 88px; transform: translateX(-50%); background: #1a1a1a; color: #fff;
           padding: 9px 14px; border-radius: 8px; font-size: 13px; opacity: 0; transition: opacity .2s; pointer-events: none;
           z-index: 50; max-width: 82vw; box-shadow: 0 3px 12px rgba(0,0,0,.25); }
  .toast.show { opacity: .96; }
</style>
</head>
<body>
<header>
  <div class="topline">
    <h1 id="title">__TITLE__</h1>
    <div class="counts">
      <span class="pill todo"  id="c-todo">– to review</span>
      <span class="pill green" id="c-ok">0 approved</span>
      <span class="pill amber" id="c-flag">0 flagged</span>
      <span class="pill saved" id="c-saved" title="Stored in this browser and restored automatically when you reopen this file.">✓ saved</span>
    </div>
    <span class="spacer"></span>
    <span class="kbd">keys: <b>←</b>/<b>→</b> nav · <b>a</b> approve · <b>f</b> flag · <b>c</b> clear</span>
    <button class="ghost" id="btn-autosave" onclick="linkSaveFile()">🔗 Auto-save to file…</button>
    <button class="ghost" onclick="toggleSettings()">⚙ Settings</button>
    <button class="ghost" onclick="document.getElementById('importer').click()">⬆ Import</button>
    <input id="importer" type="file" accept="application/json,.json" style="display:none" onchange="importJSON(this.files && this.files[0])" />
    <button class="ghost" onclick="exportCSV()">⬇ CSV</button>
    <button class="ghost" onclick="exportJSON()">⬇ JSON</button>
  </div>
  <div class="filters">
    <span>Show:</span>
    <span class="chip active" data-f="all"       onclick="setFilter('all')">All</span>
    <span class="chip"        data-f="todo"      onclick="setFilter('todo')">To review</span>
    <span class="chip"        data-f="approved"  onclick="setFilter('approved')">Approved</span>
    <span class="chip"        data-f="flagged"   onclick="setFilter('flagged')">Flagged</span>
    <span class="chip"        data-f="matched"   onclick="setFilter('matched')">With target</span>
    <span class="chip"        data-f="unmatched" onclick="setFilter('unmatched')">No target</span>
    <span class="spacer"></span>
    <button class="ghost" onclick="resetAll()">Reset all decisions</button>
  </div>
  <div class="settings" id="settings">
    <p class="warn">Host used to build both panes. Default points to production. Edit it to compare against a
       staging server (e.g. <code>http://staging-site-&lt;branch&gt;</code>). Paths and query params come from the mapping.</p>
    <div id="settings-fields"></div>
  </div>
</header>

<main>
  <div class="meta">
    <span class="rowid" id="m-rowid"></span>
    <span class="quality-tag" id="m-quality"></span>
    <span id="m-mdim"></span>
    <span id="m-shared" style="color:var(--muted)"></span>
    <span class="status-tag" id="m-status"></span>
  </div>
  <div class="conflict-line" id="m-conflict"></div>
  <div class="panes">
    <div class="pane">
      <h2><span>Old chart</span><span id="chart-label"></span></h2>
      <div class="sel" id="chart-sel"></div>
      <div class="url" id="chart-url"></div>
      <iframe id="chart-frame" loading="lazy"></iframe>
    </div>
    <div class="pane">
      <h2><span>New MDIM view</span><span id="mdim-name"></span></h2>
      <div class="sel" id="mdim-sel"></div>
      <div class="url" id="mdim-url"></div>
      <iframe id="mdim-frame" loading="lazy" style="display:none"></iframe>
      <div class="info" id="mdim-info" style="display:none"></div>
    </div>
  </div>
</main>

<footer>
  <div class="nav">
    <button onclick="go(-1)">◀ Prev</button>
    <select id="jump" onchange="jumpTo(this.value)"></select>
    <button onclick="go(1)">Next ▶</button>
  </div>
  <button class="approve" onclick="decide('approved')">✓ Approve</button>
  <button class="flag" onclick="decide('flagged')">⚠ Flag</button>
  <button class="ghost" onclick="decide(null)">Clear</button>
  <span class="note"><input id="note" placeholder="note (optional)…" oninput="saveNote(this.value)" /></span>
</footer>

<div id="toast" class="toast"></div>

<script>
const RECORDS = __RECORDS__;
const DEFAULT_ENDPOINTS = __ENDPOINTS__;
const REVIEW_NAME = __REVIEW_NAME__;
const LS_DEC = "chart_mdim_review_v1__" + REVIEW_NAME;
const LS_EP  = "chart_mdim_endpoints_v1__" + REVIEW_NAME;

let endpoints = loadEndpoints();
let decisions = JSON.parse(localStorage.getItem(LS_DEC) || "{}");
let filter = "all";
let order = RECORDS.map((_, i) => i);
let pos = 0;

// A decision is bound to the proposal it was made on: when a re-run of the extractor
// changes a chart's proposed target OR edits the source chart itself, the saved
// approval/flag must not carry over. The source config md5 is in the fingerprint
// because a reviewer approves a specific version of the chart, not just its id —
// and once the mapping is regenerated, preflight's own configMd5 check compares the
// new proposal against the DB and so cannot see that drift either.
function fp(rec) { return (rec.target_mdim || "") + "::" + (rec.view_id || "") + "::" + (rec.config_md5 || ""); }
(function pruneStaleDecisions() {
  let n = 0;
  for (const rec of RECORDS) {
    const d = decisions[String(rec.id)];
    if (d && d.target !== fp(rec)) { delete decisions[String(rec.id)]; n++; }
  }
  if (n) {
    localStorage.setItem(LS_DEC, JSON.stringify(decisions));
    setTimeout(() => toast(n + " saved decision(s) cleared — their proposed target changed since they were made."), 400);
  }
})();

// --- persistence -----------------------------------------------------------
// localStorage is the always-on store: every decision is saved immediately and
// restored on reopen, so a refresh never loses work. Optionally, "Auto-save to
// file" mirrors each change to a real JSON on disk (Chrome/Edge File System
// Access API). Import restores/merges from any exported JSON (works everywhere).
let fileHandle = null, autoSaveActive = false, lastSaved = null, lastFileWrite = null;
let fileWriteTimer = null, toastTimer = null;
const FS_SUPPORTED = ("showSaveFilePicker" in window);

function persist() {
  localStorage.setItem(LS_DEC, JSON.stringify(decisions));
  lastSaved = new Date();
  updateSaveStatus();
  scheduleFileWrite();
}

function updateSaveStatus() {
  const pill = document.getElementById("c-saved");
  pill.textContent = "✓ saved" + (lastSaved ? " " + lastSaved.toLocaleTimeString() : "");
  const btn = document.getElementById("btn-autosave");
  if (autoSaveActive && fileHandle) {
    btn.textContent = "✓ Auto-saving → " + fileHandle.name;
    btn.classList.add("on");
    pill.title = "Saved in this browser" +
      (lastFileWrite ? " + written to " + fileHandle.name + " at " + lastFileWrite.toLocaleTimeString() : "");
  } else {
    btn.textContent = FS_SUPPORTED ? "🔗 Auto-save to file…" : "🔗 Auto-save (Chrome/Edge only)";
    btn.classList.remove("on");
    pill.title = "Stored in this browser and restored automatically when you reopen this file.";
  }
}

function toast(msg) {
  const el = document.getElementById("toast");
  el.textContent = msg; el.classList.add("show");
  clearTimeout(toastTimer); toastTimer = setTimeout(() => el.classList.remove("show"), 4000);
}

async function linkSaveFile() {
  if (!FS_SUPPORTED) {
    toast("Auto-save-to-file needs Chrome or Edge. Your review is still saved in this browser — use ⬇ JSON / ⬆ Import for portable backups.");
    return;
  }
  try {
    const h = await window.showSaveFilePicker({
      suggestedName: REVIEW_NAME + "_chart_mdim_review.json",
      types: [{ description: "JSON", accept: { "application/json": [".json"] } }],
    });
    fileHandle = h; autoSaveActive = true;
    await writeFileNow();
    toast("Auto-saving every change → " + h.name);
  } catch (e) {
    if (e.name !== "AbortError") toast("Couldn't link a file: " + e.message + " (your work is still saved in this browser).");
  }
  updateSaveStatus();
}

async function writeFileNow() {
  if (!fileHandle || !autoSaveActive) return;
  try {
    const w = await fileHandle.createWritable();
    await w.write(JSON.stringify(exportRows(), null, 2));
    await w.close();
    lastFileWrite = new Date();
    updateSaveStatus();
  } catch (e) {
    autoSaveActive = false;
    updateSaveStatus();
    toast("Lost file access — click Auto-save again to resume the disk copy. (Your work is still saved in this browser.)");
  }
}

function scheduleFileWrite() {
  if (!autoSaveActive) return;
  clearTimeout(fileWriteTimer);
  fileWriteTimer = setTimeout(writeFileNow, 300);
}

function importJSON(file) {
  if (!file) return;
  const r = new FileReader();
  r.onload = () => {
    try {
      const data = JSON.parse(r.result);
      const rows = Array.isArray(data) ? data : [];
      const byId = {};
      for (const rec of RECORDS) byId[String(rec.id)] = rec;
      let n = 0, skipped = 0;
      for (const row of rows) {
        if (!(row && row.id != null && (row.status || row.note))) continue;
        const rec = byId[String(row.id)];
        const rowFp = (row.target_mdim || "") + "::" + (row.view_id || "") + "::" + (row.config_md5 || "");
        if (!rec || rowFp !== fp(rec)) { skipped++; continue; }  // chart gone, target changed, or chart edited
        decisions[String(row.id)] = { status: row.status || null, note: row.note || "", target: rowFp };
        n++;
      }
      persist(); render();
      toast("Imported " + n + " decisions from " + file.name + "." +
            (skipped ? " Skipped " + skipped + " (chart missing or its proposed target changed)." : ""));
    } catch (e) {
      toast("Couldn't read that file: " + e.message);
    }
    document.getElementById("importer").value = "";
  };
  r.readAsText(file);
}

function loadEndpoints() {
  const saved = JSON.parse(localStorage.getItem(LS_EP) || "null");
  return Object.assign({}, DEFAULT_ENDPOINTS, saved || {});
}
function saveEndpoints() {
  for (const k of Object.keys(DEFAULT_ENDPOINTS)) {
    const el = document.getElementById("ep-" + k);
    if (el) endpoints[k] = el.value.trim();
  }
  localStorage.setItem(LS_EP, JSON.stringify(endpoints));
  render();
}
function buildEndpointInputs() {
  const root = document.getElementById("settings-fields");
  root.innerHTML = "";
  for (const k of Object.keys(DEFAULT_ENDPOINTS)) {
    const label = document.createElement("label");
    label.textContent = "Host";
    const input = document.createElement("input");
    input.id = "ep-" + k;
    input.value = endpoints[k];
    input.addEventListener("input", saveEndpoints);
    root.appendChild(label);
    root.appendChild(input);
  }
}
function toggleSettings() { document.getElementById("settings").classList.toggle("open"); }

function withHideControls(path) {
  return endpoints.host.replace(/\/$/, "") + path + (path.includes("?") ? "&" : "?") + "hideControls=true";
}
function chartUrl(rec) { return withHideControls(rec.chart_path); }
function mdimUrl(rec) { return rec.target_path ? withHideControls(rec.target_path) : ""; }

function applyFilter() {
  order = RECORDS.map((_, i) => i).filter((i) => {
    const rec = RECORDS[i];
    const st = (decisions[rec.id] || {}).status || null;
    if (filter === "all") return true;
    if (filter === "todo") return !st;
    if (filter === "matched") return !!rec.target_path;
    if (filter === "unmatched") return !rec.target_path;
    return st === filter;
  });
  if (order.length === 0) order = [0];
  if (pos >= order.length) pos = order.length - 1;
}
function setFilter(f) {
  filter = f;
  document.querySelectorAll(".filters .chip").forEach((c) => c.classList.toggle("active", c.dataset.f === f));
  pos = 0; applyFilter(); render();
}

function updateCounts() {
  let ok = 0, flag = 0;
  for (const r of RECORDS) {
    const st = (decisions[r.id] || {}).status;
    if (st === "approved") ok++; else if (st === "flagged") flag++;
  }
  document.getElementById("c-ok").textContent = ok + " approved";
  document.getElementById("c-flag").textContent = flag + " flagged";
  document.getElementById("c-todo").textContent = (RECORDS.length - ok - flag) + " to review";
}

function fillJump() {
  const sel = document.getElementById("jump");
  sel.innerHTML = "";
  order.forEach((idx, k) => {
    const r = RECORDS[idx];
    const st = (decisions[r.id] || {}).status;
    const mark = st === "approved" ? "✓ " : st === "flagged" ? "⚠ " : "";
    const target = r.target_path ? (r.target_mdim + ":" + r.view_id) : ("(" + r.quality + ")");
    const o = document.createElement("option");
    o.value = k;
    o.textContent = `${mark}#${r.id} · ${r.chart_slug} → ${target}`;
    sel.appendChild(o);
  });
  sel.value = pos;
}

function chip(label, val) { return `${label}: <code>${val}</code>`; }

function render() {
  applyFilter();
  const rec = RECORDS[order[pos]];
  const dec = decisions[rec.id] || {};

  document.getElementById("m-rowid").textContent = `Row ${pos + 1} / ${order.length}  ·  chart #${rec.id}`;
  const qEl = document.getElementById("m-quality");
  qEl.textContent = rec.quality;
  qEl.className = "quality-tag " + rec.quality;
  document.getElementById("m-mdim").innerHTML = rec.target_path
    ? `→ <b>${rec.target_mdim}</b> <span style="color:var(--muted)">(view ${rec.view_id})</span>` : "";
  document.getElementById("m-shared").textContent = rec.shared_with ? `(shared with chart ids: ${rec.shared_with})` : "";
  const stEl = document.getElementById("m-status");
  stEl.textContent = dec.status ? (dec.status === "approved" ? "✓ approved" : "⚠ flagged") : "";
  stEl.className = "status-tag " + (dec.status || "");
  document.getElementById("m-conflict").textContent = rec.conflict ? "⛔ " + rec.conflict : "";

  document.getElementById("chart-label").textContent = rec.chart_slug;
  document.getElementById("chart-sel").innerHTML = rec.chart_title;
  const cUrl = chartUrl(rec);
  document.getElementById("chart-url").innerHTML = `<a href="${cUrl}" target="_blank" rel="noopener">open ↗ ${cUrl}</a>`;
  setFrame("chart-frame", cUrl);

  const frame = document.getElementById("mdim-frame");
  const info = document.getElementById("mdim-info");
  if (rec.target_path) {
    document.getElementById("mdim-name").textContent = rec.target_mdim;
    document.getElementById("mdim-sel").innerHTML = chip("view", rec.view_id);
    const mUrl = mdimUrl(rec);
    document.getElementById("mdim-url").innerHTML = `<a href="${mUrl}" target="_blank" rel="noopener">open ↗ ${mUrl}</a>`;
    frame.style.display = ""; info.style.display = "none";
    setFrame("mdim-frame", mUrl);
  } else {
    document.getElementById("mdim-name").textContent = "no target";
    document.getElementById("mdim-sel").innerHTML = "";
    document.getElementById("mdim-url").innerHTML = "";
    frame.style.display = "none"; info.style.display = "";
    const bits = [];
    if (rec.candidates) bits.push("Candidates:\n" + rec.candidates.split(" | ").join("\n"));
    if (rec.near_miss) bits.push("Near misses:\n" + rec.near_miss.split(" | ").join("\n"));
    if (rec.csv_note) bits.push("Note: " + rec.csv_note);
    info.textContent = bits.join("\n\n") || "No MDIM view shares this chart's indicators.";
  }

  document.getElementById("note").value = dec.note || "";
  updateCounts();
  fillJump();
}

function setFrame(id, url) {
  const f = document.getElementById(id);
  if (f.getAttribute("data-src") !== url) { f.src = url; f.setAttribute("data-src", url); }
}

function go(delta) { pos = Math.max(0, Math.min(order.length - 1, pos + delta)); render(); }
function jumpTo(k) { pos = parseInt(k, 10); render(); }

function decide(status) {
  const rec = RECORDS[order[pos]];
  // Remember the next record's id *before* re-filtering: when the active filter
  // (e.g. "To review") drops the just-decided row, the new order shifts so
  // a naive pos++ would skip the row we actually want to land on.
  const nextId = (status && pos < order.length - 1) ? RECORDS[order[pos + 1]].id : null;
  const cur = decisions[rec.id] || {};
  cur.status = status;
  cur.target = fp(rec);
  decisions[rec.id] = cur;
  persist();
  if (nextId !== null) {
    applyFilter();
    const idx = order.findIndex((i) => RECORDS[i].id === nextId);
    if (idx >= 0) pos = idx;
  }
  render();
}
function saveNote(v) {
  const rec = RECORDS[order[pos]];
  const cur = decisions[rec.id] || {};
  cur.note = v;
  cur.target = fp(rec);
  decisions[rec.id] = cur;
  persist();
}
function resetAll() {
  if (!confirm("Clear all decisions and notes?")) return;
  decisions = {}; persist(); render();
}

function exportRows() {
  return RECORDS.map((r) => {
    const d = decisions[r.id] || {};
    return {
      id: r.id, chart_slug: r.chart_slug, quality: r.quality,
      target_mdim: r.target_mdim, view_id: r.view_id, config_md5: r.config_md5,
      shared_with: r.shared_with, conflict: r.conflict,
      status: d.status || "", note: d.note || "",
      chart_url: chartUrl(r), mdim_url: mdimUrl(r),
    };
  });
}
function download(name, text, type) {
  const blob = new Blob([text], { type });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob); a.download = name; a.click(); URL.revokeObjectURL(a.href);
}
function exportJSON() { download(REVIEW_NAME + "_chart_mdim_review.json", JSON.stringify(exportRows(), null, 2), "application/json"); }
function exportCSV() {
  const rows = exportRows();
  const cols = ["id", "chart_slug", "quality", "target_mdim", "view_id", "config_md5", "status", "note", "shared_with", "conflict", "chart_url", "mdim_url"];
  const esc = (v) => `"${String(v).replace(/"/g, '""')}"`;
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => esc(r[c])).join(","));
  download(REVIEW_NAME + "_chart_mdim_review.csv", lines.join("\n"), "text/csv");
}

document.addEventListener("keydown", (e) => {
  if (e.target.tagName === "INPUT" || e.target.tagName === "SELECT") return;
  if (e.key === "ArrowRight") go(1);
  else if (e.key === "ArrowLeft") go(-1);
  else if (e.key.toLowerCase() === "a") decide("approved");
  else if (e.key.toLowerCase() === "f") decide("flagged");
  else if (e.key.toLowerCase() === "c") decide(null);
});

buildEndpointInputs();
render();
updateSaveStatus();
</script>
</body>
</html>
"""


def render_html(records: list[dict], endpoints: dict[str, str], output_path: Path, name: str) -> None:
    title = f"{name} · chart → MDIM review"
    html = (
        HTML_TEMPLATE.replace("__TITLE__", title)
        .replace("__RECORDS__", json.dumps(records))
        .replace("__ENDPOINTS__", json.dumps(endpoints))
        .replace("__REVIEW_NAME__", json.dumps(name))
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the self-contained HTML to review a chart → MDIM mapping.")
    ap.add_argument("--mapping-dir", required=True, type=Path,
                    help="Folder produced by map-charts-to-mdim (contains mapping_proposal.csv).")  # fmt: skip
    ap.add_argument("--host", default=None,
                    help="Base URL for both panes (default: the host the mapping was extracted "
                         "against, from _sources.json; production if that record is missing).")  # fmt: skip
    ap.add_argument("--output", type=Path, default=None,
                    help="Output HTML path (default: ai/<mapping-dir name>_chart_mdim_review.html).")  # fmt: skip
    ap.add_argument("--no-coverage", action="store_true", help="Skip the coverage report.")
    args = ap.parse_args()

    if not args.mapping_dir.exists():
        raise SystemExit(f"Mapping directory not found: {args.mapping_dir}")

    records = parse_proposal(args.mapping_dir)
    if not args.no_coverage:
        coverage_report(records)

    # Review against the environment the mapping came from — defaulting to production
    # would silently show different charts/MDIMs for a staging-extracted mapping.
    if args.host:
        host, provenance = args.host.rstrip("/"), "--host"
    else:
        host, provenance = recorded_host(args.mapping_dir) or ("https://ourworldindata.org", "production fallback — no host recorded in _sources.json")  # fmt: skip
    print(f"pane host: {host}  ({provenance})")

    name = args.mapping_dir.name.replace("-", "_")
    endpoints = {"host": host}
    output_path = args.output or Path(f"ai/{name}_chart_mdim_review.html")
    render_html(records, endpoints, output_path, name)
    size_kb = output_path.stat().st_size // 1024
    print(f"\nWrote {output_path} ({len(records)} chart rows, {size_kb} KB)")
    print("Open in a browser to review. Decisions auto-save to localStorage.")


if __name__ == "__main__":
    main()
