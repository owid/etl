"""Generate a self-contained HTML to review an explorer → MDIM view mapping.

Consumes the output of the ``map-explorer-to-mdim`` skill (the
``ai/<slug>-mdim-mapping`` folder, containing ``mapping_proposal.csv``,
``mapping_rules.py``, and ``multidim_<short>_views.csv``) and renders a single
HTML file where a human can step through each (explorer view, proposed MDIM
view) pair side-by-side and approve / flag the match. Decisions persist in the
browser ``localStorage`` and can also be mirrored to a JSON file on disk
(Chrome/Edge File System Access) or restored via Import.

Usage::

    .venv/bin/python .claude/skills/review-explorer-mdim-mapping/scripts/build_review.py \\
        --mapping-dir ai/<slug>-mdim-mapping \\
        --explorer-slug <slug> \\
        --mdim-slug <short>=<published-grapher-slug> \\
        [--mdim-slug ...] \\
        [--host https://ourworldindata.org] \\
        [--output ai/<slug>_view_review.html] \\
        [--no-coverage]

The ``--mdim-slug`` argument maps each MDIM short name (as listed in ``MDIMS``
in ``mapping_rules.py``) to the published Grapher slug used in URLs like
``{host}/grapher/<slug>``. The skill cannot derive the slug automatically
without DB access, so it is required up front.
"""

import argparse
import csv
import importlib.util
import json
from collections import defaultdict
from pathlib import Path


def load_rules(mapping_dir: Path):
    """Load EXPLORER_DIMENSIONS and MDIMS from the mapping_rules.py the user wrote."""
    rules_path = mapping_dir / "mapping_rules.py"
    if not rules_path.exists():
        raise SystemExit(f"Not found: {rules_path}. Run the map-explorer-to-mdim skill first.")
    spec = importlib.util.spec_from_file_location("mapping_rules", rules_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for attr in ("EXPLORER_DIMENSIONS", "MDIMS"):
        if not hasattr(mod, attr):
            raise SystemExit(f"mapping_rules.py is missing `{attr}`")
    return mod


def parse_mapping(mapping_dir: Path, explorer_dim_names: list[str], mdim_shorts: list[str]) -> list[dict]:
    """Parse mapping_proposal.csv into the normalized record schema."""
    proposal = mapping_dir / "mapping_proposal.csv"
    if not proposal.exists():
        raise SystemExit(f"Not found: {proposal}. Run build_mapping.py from map-explorer-to-mdim first.")
    rows = list(csv.DictReader(open(proposal)))
    if not rows:
        raise SystemExit(f"{proposal} has no data rows.")

    cols = list(rows[0].keys())
    # Identify wide MDIM dim columns by `<short>_` prefix; ignore non-dim columns.
    mdim_dim_cols: dict[str, list[tuple[str, str]]] = defaultdict(list)
    reserved = {"id", "target_mdim", "target_view_id", "shared_target_explorer_ids"}
    reserved |= {f"dimension_{i + 1}" for i in range(len(explorer_dim_names))}
    for c in cols:
        if c in reserved:
            continue
        for short in mdim_shorts:
            prefix = f"{short}_"
            if c.startswith(prefix):
                mdim_dim_cols[short].append((c, c[len(prefix):]))
                break

    records: list[dict] = []
    for r in rows:
        explorer_params = {}
        for i, name in enumerate(explorer_dim_names, start=1):
            v = (r.get(f"dimension_{i}") or "").strip()
            if v:
                explorer_params[name] = v

        target = (r.get("target_mdim") or "").strip()
        dims: dict[str, str] = {}
        for col, dim_slug in mdim_dim_cols.get(target, []):
            v = (r.get(col) or "").strip()
            if v:
                dims[dim_slug] = v

        records.append(
            {
                "id": (r.get("id") or "").strip(),
                "explorer_params": explorer_params,
                "target_mdim": target,
                "view_id": (r.get("target_view_id") or "").strip(),
                "dims": dims,
                "shared_with": (r.get("shared_target_explorer_ids") or "").strip(),
            }
        )
    return records


def coverage_report(records: list[dict], mapping_dir: Path, mdim_shorts: list[str]) -> None:
    """Print a pre-build summary of mapping coverage."""
    print("─" * 66)
    print("COVERAGE")
    print("─" * 66)
    print(f"  mapping rows: {len(records)}   |   distinct explorer views: {len({r['id'] for r in records})}")

    distinct_targets = {(r["target_mdim"], r["view_id"]) for r in records if r["target_mdim"] and r["view_id"]}
    shared_rows = [r for r in records if r["shared_with"]]
    print(f"  distinct MDIM targets: {len(distinct_targets)}   |   many-to-one rows: {len(shared_rows)}")

    unresolved = [r for r in records if not r["view_id"]]
    if unresolved:
        print(f"  ⚠ unresolved rows (no target_view_id): {len(unresolved)}")
        for r in unresolved[:5]:
            print(f"      id={r['id']} {r['explorer_params']}")
        if len(unresolved) > 5:
            print(f"      … and {len(unresolved) - 5} more")

    print()
    targeted: dict[str, set[str]] = defaultdict(set)
    for r in records:
        if r["target_mdim"] and r["view_id"]:
            targeted[r["target_mdim"]].add(r["view_id"])
    for short in mdim_shorts:
        f = mapping_dir / f"multidim_{short}_views.csv"
        if not f.exists():
            print(f"  MDIM '{short}': multidim_{short}_views.csv missing — skipping unmapped check")
            continue
        all_ids = {row["id"] for row in csv.DictReader(open(f))}
        used = targeted.get(short, set())
        unmapped = all_ids - used
        print(f"  MDIM '{short}': {len(used)}/{len(all_ids)} views targeted; {len(unmapped)} never targeted")
        for vid in sorted(unmapped)[:5]:
            print(f"      {vid}")
        if len(unmapped) > 5:
            print(f"      … and {len(unmapped) - 5} more")
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
  .status-tag { font-weight: 600; }
  .status-tag.approved { color: var(--green); }
  .status-tag.flagged { color: var(--amber); }
  .panes { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  @media (max-width: 900px) { .panes { grid-template-columns: 1fr; } }
  .pane { background: var(--card); border: 1px solid var(--line); border-radius: 10px; overflow: hidden; display: flex; flex-direction: column; }
  .pane h2 { font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: var(--muted);
             margin: 0; padding: 8px 12px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; }
  .pane .sel { padding: 8px 12px; font-size: 14px; }
  .pane .sel .chips { margin-top: 4px; display: flex; gap: 6px; flex-wrap: wrap; }
  .pane .sel code { background: #f1f3f6; border-radius: 5px; padding: 1px 6px; font-size: 12px; }
  .pane .url { padding: 0 12px 8px; font-size: 11px; }
  .pane .url a { color: var(--blue); text-decoration: none; word-break: break-all; }
  .pane iframe { width: 100%; height: 540px; border: 0; border-top: 1px solid var(--line); background: #fff; }
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
    <span class="chip active" data-f="all"      onclick="setFilter('all')">All</span>
    <span class="chip"        data-f="todo"     onclick="setFilter('todo')">To review</span>
    <span class="chip"        data-f="approved" onclick="setFilter('approved')">Approved</span>
    <span class="chip"        data-f="flagged"  onclick="setFilter('flagged')">Flagged</span>
    <span class="spacer"></span>
    <button class="ghost" onclick="resetAll()">Reset all decisions</button>
  </div>
  <div class="settings" id="settings">
    <p class="warn">URL prefixes used to build the side-by-side charts. Defaults point to production. Edit them
       to compare against a staging server (e.g. swap the host for your <code>staging-site-&lt;branch&gt;</code>).
       Dimension query params are appended automatically.</p>
    <div id="settings-fields"></div>
  </div>
</header>

<main>
  <div class="meta">
    <span class="rowid" id="m-rowid"></span>
    <span id="m-mdim"></span>
    <span id="m-viewid" style="color:var(--muted)"></span>
    <span id="m-shared" style="color:var(--muted)"></span>
    <span class="status-tag" id="m-status"></span>
  </div>
  <div class="panes">
    <div class="pane">
      <h2><span>Old explorer</span><span id="explorer-label"></span></h2>
      <div class="sel" id="exp-sel"></div>
      <div class="url" id="exp-url"></div>
      <iframe id="exp-frame" loading="lazy"></iframe>
    </div>
    <div class="pane">
      <h2><span>New MDIM</span><span id="mdim-name"></span></h2>
      <div class="sel" id="mdim-sel"></div>
      <div class="url" id="mdim-url"></div>
      <iframe id="mdim-frame" loading="lazy"></iframe>
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
const EXPLORER_SLUG = __EXPLORER_SLUG__;
const LS_DEC = "review_decisions_v1__" + EXPLORER_SLUG;
const LS_EP  = "review_endpoints_v1__" + EXPLORER_SLUG;

document.getElementById("explorer-label").textContent = EXPLORER_SLUG;

let endpoints = loadEndpoints();
let decisions = JSON.parse(localStorage.getItem(LS_DEC) || "{}");
let filter = "all";
let order = RECORDS.map((_, i) => i);
let pos = 0;

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
      suggestedName: EXPLORER_SLUG + "_view_review.json",
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
      let n = 0;
      for (const row of rows) {
        if (row && row.id != null && (row.status || row.note)) {
          decisions[String(row.id)] = { status: row.status || null, note: row.note || "" };
          n++;
        }
      }
      persist(); render();
      toast("Imported " + n + " decisions from " + file.name + ".");
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
  // Build the Settings panel inputs dynamically from the endpoints keys.
  const root = document.getElementById("settings-fields");
  root.innerHTML = "";
  for (const k of Object.keys(DEFAULT_ENDPOINTS)) {
    const label = document.createElement("label");
    label.textContent = (k === "explorer" ? "Explorer prefix" : "MDIM prefix · " + k);
    const input = document.createElement("input");
    input.id = "ep-" + k;
    input.value = endpoints[k];
    input.addEventListener("input", saveEndpoints);
    root.appendChild(label);
    root.appendChild(input);
  }
}
function toggleSettings() { document.getElementById("settings").classList.toggle("open"); }

function qs(params) {
  return Object.entries(params).map(([k, v]) => encodeURIComponent(k) + "=" + encodeURIComponent(v)).join("&");
}
function explorerUrl(rec) {
  return endpoints.explorer + "?" + qs(Object.assign({ hideControls: "true" }, rec.explorer_params));
}
function mdimUrl(rec) {
  const base = endpoints[rec.target_mdim];
  if (!base) return "about:blank#missing-endpoint-for-" + rec.target_mdim;
  return base + "?" + qs(Object.assign({ hideControls: "true" }, rec.dims));
}

function applyFilter() {
  order = RECORDS.map((_, i) => i).filter((i) => {
    const st = (decisions[RECORDS[i].id] || {}).status || null;
    if (filter === "all") return true;
    if (filter === "todo") return !st;
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
    const summary = Object.values(r.explorer_params).slice(0, 3).join(" · ");
    const o = document.createElement("option");
    o.value = k;
    o.textContent = `${mark}#${r.id} · ${summary}`;
    sel.appendChild(o);
  });
  sel.value = pos;
}

function chip(label, val) { return `${label}: <code>${val}</code>`; }

function render() {
  applyFilter();
  const rec = RECORDS[order[pos]];
  const dec = decisions[rec.id] || {};

  document.getElementById("m-rowid").textContent = `Row ${pos + 1} / ${order.length}  ·  mapping #${rec.id}`;
  document.getElementById("m-mdim").innerHTML = `→ <b>${rec.target_mdim}</b> MDIM`;
  document.getElementById("m-viewid").textContent = rec.view_id ? `(mdim view ${rec.view_id})` : "";
  document.getElementById("m-shared").textContent = rec.shared_with ? `(shared with ids: ${rec.shared_with})` : "";
  const stEl = document.getElementById("m-status");
  stEl.textContent = dec.status ? (dec.status === "approved" ? "✓ approved" : "⚠ flagged") : "";
  stEl.className = "status-tag " + (dec.status || "");

  const ep = rec.explorer_params;
  document.getElementById("exp-sel").innerHTML =
    `<div class="chips">${Object.entries(ep).map(([k, v]) => chip(k, v)).join(" ")}</div>`;
  const eUrl = explorerUrl(rec);
  document.getElementById("exp-url").innerHTML = `<a href="${eUrl}" target="_blank" rel="noopener">open ↗ ${eUrl}</a>`;
  setFrame("exp-frame", eUrl);

  document.getElementById("mdim-name").textContent = rec.target_mdim;
  document.getElementById("mdim-sel").innerHTML =
    `<div class="chips">${Object.entries(rec.dims).map(([k, v]) => chip(k, v)).join(" ")}</div>`;
  const mUrl = mdimUrl(rec);
  document.getElementById("mdim-url").innerHTML = `<a href="${mUrl}" target="_blank" rel="noopener">open ↗ ${mUrl}</a>`;
  setFrame("mdim-frame", mUrl);

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
  const cur = decisions[rec.id] || {};
  cur.status = status;
  decisions[rec.id] = cur;
  persist();
  if (status) {
    if (pos < order.length - 1) { pos++; }
  }
  render();
}
function saveNote(v) {
  const rec = RECORDS[order[pos]];
  const cur = decisions[rec.id] || {};
  cur.note = v;
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
      id: r.id, target_mdim: r.target_mdim, view_id: r.view_id,
      explorer_params: r.explorer_params, mdim_dims: r.dims,
      shared_with: r.shared_with,
      status: d.status || "", note: d.note || "",
      explorer_url: explorerUrl(r), mdim_url: mdimUrl(r),
    };
  });
}
function download(name, text, type) {
  const blob = new Blob([text], { type });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob); a.download = name; a.click(); URL.revokeObjectURL(a.href);
}
function exportJSON() { download(EXPLORER_SLUG + "_view_review.json", JSON.stringify(exportRows(), null, 2), "application/json"); }
function exportCSV() {
  const rows = exportRows();
  const cols = ["id", "target_mdim", "view_id", "status", "note", "shared_with", "explorer_url", "mdim_url"];
  const esc = (v) => `"${String(v).replace(/"/g, '""')}"`;
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => esc(r[c])).join(","));
  download(EXPLORER_SLUG + "_view_review.csv", lines.join("\n"), "text/csv");
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


def render_html(records: list[dict], endpoints: dict[str, str], output_path: Path, explorer_slug: str) -> None:
    title = f"{explorer_slug} · explorer → MDIM review"
    html = (
        HTML_TEMPLATE.replace("__TITLE__", title)
        .replace("__RECORDS__", json.dumps(records))
        .replace("__ENDPOINTS__", json.dumps(endpoints))
        .replace("__EXPLORER_SLUG__", json.dumps(explorer_slug))
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the self-contained HTML to review an explorer → MDIM mapping.")
    ap.add_argument("--mapping-dir", required=True, type=Path,
                    help="Folder produced by the map-explorer-to-mdim skill (contains mapping_proposal.csv, mapping_rules.py).")
    ap.add_argument("--explorer-slug", required=True,
                    help="Explorer slug, e.g. 'natural-disasters' (used in the explorer URL).")
    ap.add_argument("--mdim-slug", action="append", default=[],
                    metavar="<short>=<grapher-slug>",
                    help="Published Grapher slug per MDIM short name. Repeat for each MDIM in mapping_rules.MDIMS. "
                         "Example: --mdim-slug deaths=natural-disasters-deaths")
    ap.add_argument("--host", default="https://ourworldindata.org",
                    help="Base URL for both panes (default: production).")
    ap.add_argument("--output", type=Path, default=None,
                    help="Output HTML path (default: ai/<explorer_slug>_view_review.html).")
    ap.add_argument("--no-coverage", action="store_true", help="Skip the coverage report.")
    args = ap.parse_args()

    if not args.mapping_dir.exists():
        raise SystemExit(f"Mapping directory not found: {args.mapping_dir}")

    rules = load_rules(args.mapping_dir)
    records = parse_mapping(args.mapping_dir, rules.EXPLORER_DIMENSIONS, rules.MDIMS)

    if not args.no_coverage:
        coverage_report(records, args.mapping_dir, rules.MDIMS)

    mdim_slugs: dict[str, str] = {}
    for spec in args.mdim_slug:
        if "=" not in spec:
            raise SystemExit(f"--mdim-slug must be '<short>=<grapher-slug>', got: {spec}")
        short, slug = spec.split("=", 1)
        mdim_slugs[short.strip()] = slug.strip()

    missing = [s for s in rules.MDIMS if s not in mdim_slugs]
    if missing:
        raise SystemExit(
            f"\nMissing --mdim-slug for: {missing}\n"
            f"Provide one --mdim-slug <short>=<grapher-slug> per MDIM in mapping_rules.MDIMS.\n"
            f"Example: --mdim-slug deaths=natural-disasters-deaths"
        )

    endpoints = {"explorer": f"{args.host.rstrip('/')}/explorers/{args.explorer_slug}"}
    for short, slug in mdim_slugs.items():
        endpoints[short] = f"{args.host.rstrip('/')}/grapher/{slug}"

    output_path = args.output or Path(f"ai/{args.explorer_slug.replace('-', '_')}_view_review.html")
    render_html(records, endpoints, output_path, args.explorer_slug)
    size_kb = output_path.stat().st_size // 1024
    print(f"\nWrote {output_path} ({len(records)} view pairs, {size_kb} KB)")
    print(f"Open in a browser to review. Decisions auto-save to localStorage.")


if __name__ == "__main__":
    main()
