"""Generate a self-contained HTML to review an add-gdp-scatter migration side-by-side.

Takes the same JSON pair list the applier takes — `{chart_admin_url,
target_chart_admin_url, gdp_source}` on stdin — and renders one HTML file where a human
steps through each pair, comparing the **old standalone scatter** against the **scatter
view the target gained**, and approves / flags it. Decisions persist in the browser
`localStorage`, mirror to a JSON on disk (Chrome/Edge File System Access), and can be
restored via Import. Modelled on `map-charts-to-mdim/scripts/build_review.py`.

The thing this reviewer has to make impossible to miss is that **the target's scatter is
not its main view** — unlike the old chart, where the scatter *was* the whole chart. So
every row shows the target's full tab list with its default marked, a SECONDARY badge
naming the tab readers actually land on, and a toggle between:

  * **Redirect view** — `?tab=scatter&time=latest&country=`, plus `&yScale=log` on a row
    whose retiring chart had a log y axis: exactly what a reader following that retired
    slug will get. The query comes from `redirect_to_scatter.row_query`, the function that
    writes it, so the pane cannot review a view the redirect does not serve.
  * **Default view** — what a reader opening the target normally sees first.

Those two are the states a URL can produce. A third exists and cannot be reached by URL:
the state after a reader *clicks* the scatter tab, which additionally collapses the time
handles and clears the entity selection (`adjustStateForTab`, reachable only from
`onTabChange`). To see it, open the Default view and click the scatter tab inside the
frame — that is also the check that the redirect's `time=`/`country=` params reproduce it.

Both panes are rendered against a host you can edit in Settings. They default to whatever
`OWID_ENV` resolves to — i.e. the staging server carrying the migration — because the
targets' scatter views usually do not exist on production yet.

Usage::

    echo '<JSON>' | STAGING=1 .venv/bin/python \\
        .claude/skills/add-gdp-scatter/scripts/build_review.py \\
        [--name scatter_batch1] [--output ai/scatter_review.html]
"""

import argparse
import html
import importlib.util
import json
import re
import sys
from pathlib import Path

from etl.config import OWID_ENV
from etl.db import get_engine

CHART_ID_RE = re.compile(r"/charts/(\d+)")
TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")


def _load_sibling(name: str):
    """Import a script from this skill's own scripts directory."""
    path = Path(__file__).resolve().parent / f"{name}.py"
    if not path.exists():
        raise SystemExit(f"Cannot import {name}: {path} does not exist")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


# The stored query of the part-2 redirect is taken from the script that writes it, rather than
# kept in step by hand: the whole point of the left/right comparison is to review the URL
# readers will actually land on, and one part of that query is per-row (`yScale=log` for a
# source authored on a log y axis), so a local copy would review a view nobody gets. The
# log-source predicate, with its reversed-source exclusion, is owned one level further down by
# `apply_scatter_defaults.log_y_axis_sources`.
redirect = _load_sibling("redirect_to_scatter")

# Grapher's `tab` config option -> the tab a reader lands on. Anything unrecognised is
# shown verbatim; `chart` means "the first entry in chartTypes".
TAB_LABELS = {
    "chart": "first chart type",
    "map": "Map",
    "table": "Table",
    "line": "LineChart",
    "slope": "SlopeChart",
    "bar": "DiscreteBar",
    "stacked-area": "StackedArea",
    "stacked-bar": "StackedBar",
    "stacked-discrete-bar": "StackedDiscreteBar",
    "marimekko": "Marimekko",
    "dumbbell": "Dumbbell",
    "scatter": "ScatterPlot",
}

SINGLE_TIME_ONLY_FAMILY = {"DiscreteBar", "StackedDiscreteBar", "Marimekko"}


def short_host() -> str:
    return TAILSCALE_SUFFIX_RE.sub("", OWID_ENV.admin_api).rstrip("/").removesuffix("/admin/api")


def chart_id_from_url(url: str) -> int:
    m = CHART_ID_RE.search(url)
    if not m:
        raise ValueError(f"Could not extract chart id from {url!r}")
    return int(m.group(1))


def load_charts(chart_ids: list[int]) -> dict[int, dict]:
    """chart id -> {slug, title, config, md5} straight from the environment's DB."""
    if not chart_ids:
        return {}
    df = OWID_ENV.read_sql(
        "SELECT c.id, cc.slug, cc.config, cc.configMd5 FROM charts c "
        "JOIN chart_configs cc ON c.configId = cc.id WHERE c.id IN %(ids)s",
        params={"ids": tuple(chart_ids)},
    )
    out = {}
    for r in df.to_dict("records"):
        cfg = r["config"] if isinstance(r["config"], dict) else json.loads(r["config"])
        out[int(r["id"])] = {
            "slug": r["slug"],
            "title": cfg.get("title") or "",
            "cfg": cfg,
            "md5": r["configMd5"],
        }
    return out


def dim_var(cfg: dict, prop: str) -> int | None:
    for d in cfg.get("dimensions") or []:
        if d.get("property") == prop:
            return d.get("variableId")
    return None


def default_tab_label(cfg: dict) -> str:
    """What a reader lands on when opening the target with no query string."""
    tab = cfg.get("tab")
    types = cfg.get("chartTypes") or ["LineChart", "DiscreteBar"]
    if not tab or tab == "chart":
        return types[0] if types else "LineChart"
    return TAB_LABELS.get(tab, tab)


def esc(text: object) -> str:
    """Escape config-derived text: the warning and context lists are injected with innerHTML."""
    return html.escape(str(text), quote=False)


def graded_exclusions(src_cfg: dict, tgt_cfg: dict) -> list[dict]:
    """The source's exclusions, graded by the applier against the view this reviewer shows.

    Graded here rather than re-derived: the classes and the warning/context split belong to
    `apply_scatter_defaults`, so the reviewer says the same thing the applier's run said. A
    failure degrades to one ungraded warning — better than a silent empty list, which would read
    as "this chart excluded nothing".
    """
    excluded = src_cfg.get("excludedEntityNames") or []
    if not excluded:
        return []
    applier = redirect.applier
    try:
        tgt_y = applier.find_dim(tgt_cfg, "y") or {}
        y_var_id = tgt_y.get("variableId")
        gdp_var_id = (applier.find_dim(tgt_cfg, "x") or {}).get("variableId")
        if y_var_id is None or gdp_var_id is None:
            raise ValueError("target has no y or no x dimension")
        tol = int((tgt_y.get("display") or {}).get("tolerance") or 0)
        engine = get_engine()
        year = applier.resolve_default_year(tgt_cfg, int(y_var_id), int(gdp_var_id), engine)
        if year is None:
            raise ValueError("no year where both indicators have data")
        return applier.classify_exclusions(list(excluded), int(y_var_id), int(gdp_var_id), year, tol, engine)
    except Exception as e:
        return [
            {
                "entity": ", ".join(str(x) for x in excluded),
                "cls": "ungradeable",
                "why": f"could not grade these ({e!s:.90})",
                "year": None,
                "exact": True,
            }
        ]


def row_flags(
    src_cfg: dict, tgt_cfg: dict, is_log: bool, exclusions: list[dict] | None = None
) -> tuple[list[str], list[str]]:
    """(warnings, context) for one pair, computed from the two configs.

    The split is what keeps the "With warnings" filter worth using. **Warnings** are things
    that may actually be wrong and want a decision. **Context** is what the reviewer needs
    in order to read the panes correctly but which is expected and handled — put it here
    rather than inflating every row into a warning.
    """
    warns: list[str] = []
    context: list[str] = []
    exclusions = exclusions or []
    types = tgt_cfg.get("chartTypes") or ["LineChart", "DiscreteBar"]

    if "ScatterPlot" not in types:
        warns.append("⛔ target has NO ScatterPlot tab — the migration did not apply to this chart")
    elif types[0] == "ScatterPlot":
        warns.append(
            "scatter is the PRIMARY chart type, so Grapher will not auto-collapse its time/selection on a tab click"
        )

    if tgt_cfg.get("hideTimeline") and tgt_cfg.get("minTime") != tgt_cfg.get("maxTime"):
        others = set(types) - {"ScatterPlot"}
        fixable = (
            "safe to pin minTime=maxTime='latest'" if others <= SINGLE_TIME_ONLY_FAMILY else "cannot be fixed in config"
        )
        warns.append(
            f"hideTimeline with minTime != maxTime — scatter shows a time RANGE and there is no slider ({fixable})"
        )

    if tgt_cfg.get("stackMode") == "relative":
        warns.append("stackMode=relative — the scatter opens on 'average annual change'")

    # Graded, because the two usual reasons for an exclusion have opposite consequences here: a
    # y outlier's return is a real defect, while a merely very high GDP per capita is harmless on
    # the target's log x axis. Grading them apart is what keeps this warning worth reading — an
    # unclassified list of names fires on every row that has one and decides nothing. The classes
    # and the warning/context split are the applier's (`EXCLUSION_WARN_CLASSES`).
    for e in exclusions:
        where = "" if e["exact"] or e["year"] is None else f" (measured at {e['year']}, its latest year with both)"
        line = f"old chart excluded <b>{esc(e['entity'])}</b> — {e['cls']}: {esc(e['why'])}{where}"
        if e["cls"] in redirect.applier.EXCLUSION_WARN_CLASSES:
            warns.append(f"{line}. The target never inherits exclusions, so it is back on this scatter")
        else:
            context.append(f"{line} — back on the scatter, but harmless")

    # Expected-and-handled, so context rather than a warning: both routes to the scatter
    # clear the selection (a tab click via ensureEntitySelectionIsSensibleForTab, the
    # redirect via its empty country=). It still belongs on screen, because if the panes
    # DO show highlighted entities then one of those two mechanisms failed.
    n_sel = len(tgt_cfg.get("selectedEntityNames") or [])
    if n_sel:
        context.append(
            f"target selects {n_sel} entities for its line/bar view; both the tab click and the redirect's "
            f"country= clear them, so the scatter should show none highlighted"
        )
    else:
        context.append("target has no entity selection, so its line/bar view falls back to Grapher defaults")

    if tgt_cfg.get("minTime") == tgt_cfg.get("maxTime") and tgt_cfg.get("minTime") is not None:
        context.append(f"time is pinned in the config to {tgt_cfg.get('minTime')!r} for every view")

    if is_log:
        # Two separate statements, which is why one is context and one is a warning. That the two
        # panes differ is intended and only needs explaining (context). Whether a LINEAR scatter
        # still tells the story the author chose a log axis for is a judgment nobody else in the
        # workflow makes — and the reviewer is the one person looking at both shapes at once.
        context.append(
            "the retiring chart had a LOG y axis, so the redirect carries yScale=log and the Redirect "
            "view is logarithmic — the target's own default stays linear for its other tabs"
        )
        warns.append(
            "the retiring chart's LOG y axis does not transfer: yAxis is global, so a reader who CLICKS "
            "the scatter tab gets it linear, and a key-chart slot (no query string) can never get the log "
            "at all. Compare the shapes — if linear misreads the relationship, flag the row: keeping the "
            "standalone chart is a legitimate outcome"
        )

    return warns, context


def build_records(pairs: list[dict]) -> list[dict]:
    ids = [chart_id_from_url(p["chart_admin_url"]) for p in pairs]
    ids += [chart_id_from_url(p["target_chart_admin_url"]) for p in pairs]
    charts = load_charts(sorted(set(ids)))
    # The same set the redirect script and the handoff resolve, read from the SOURCE charts:
    # the target's yAxis is deliberately left linear, so it cannot tell you what the retiring
    # chart looked like.
    log_sources = redirect.applier.log_y_axis_sources(chart_id_from_url(p["chart_admin_url"]) for p in pairs)

    records = []
    for p in pairs:
        src_id = chart_id_from_url(p["chart_admin_url"])
        tgt_id = chart_id_from_url(p["target_chart_admin_url"])
        missing = [i for i in (src_id, tgt_id) if i not in charts]
        if missing:
            print(f"  skipped {src_id} -> {tgt_id}: not in this environment's DB: {missing}", file=sys.stderr)
            continue

        src, tgt = charts[src_id], charts[tgt_id]
        types = tgt["cfg"].get("chartTypes") or ["LineChart", "DiscreteBar"]
        scatter_pos = types.index("ScatterPlot") + 1 if "ScatterPlot" in types else 0
        exclusions = graded_exclusions(src["cfg"], tgt["cfg"])
        warns, context = row_flags(src["cfg"], tgt["cfg"], is_log=src_id in log_sources, exclusions=exclusions)

        records.append(
            {
                # Keyed by the PAIR: two source scatters can resolve to the same target
                # (they share the non-GDP indicator), and keying on tgt_id alone made both
                # rows one decision — approving one silently approved the other, and the
                # differing src_md5 in fp() then pruned the shared entry as stale on reopen.
                "id": f"{src_id}-{tgt_id}",
                "src_id": src_id,
                "src_slug": src["slug"],
                "src_title": src["title"],
                "src_path": f"/grapher/{src['slug']}",
                "src_md5": src["md5"],
                "tgt_id": tgt_id,
                "tgt_slug": tgt["slug"],
                "tgt_title": tgt["title"],
                "tgt_md5": tgt["md5"],
                # The two URL-reachable states of the target, per the module docstring.
                "redirect_path": f"/grapher/{tgt['slug']}?{redirect.row_query(src_id, log_sources)}",
                "default_path": f"/grapher/{tgt['slug']}",
                # Drives the two panes' hints: for a log row the tab-click state does NOT match
                # the Redirect view (the click leaves the target's linear yAxis alone).
                "is_log": src_id in log_sources,
                "chart_types": types,
                "default_tab": default_tab_label(tgt["cfg"]),
                "scatter_pos": scatter_pos,
                "gdp_source": p.get("gdp_source", ""),
                "gdp_var_id": dim_var(tgt["cfg"], "x"),
                "n_selected": len(tgt["cfg"].get("selectedEntityNames") or []),
                "selected": ", ".join((tgt["cfg"].get("selectedEntityNames") or [])[:8]),
                "warnings": warns,
                "context": context,
            }
        )
    return records


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
  button.approve { background: var(--green); color: #fff; border-color: var(--green); }
  button.flag { background: var(--amber); color: #fff; border-color: var(--amber); }
  button.ghost { background: transparent; }
  button.on { background: #eaf6ee; border-color: #b6e3c6; color: var(--green); }
  .filters { display: flex; gap: 6px; margin-top: 8px; font-size: 13px; flex-wrap: wrap; align-items: center; }
  .filters .chip { padding: 3px 10px; border-radius: 999px; border: 1px solid var(--line); background: #fff; cursor: pointer; }
  .filters .chip.active { background: var(--ink); color: #fff; border-color: var(--ink); }
  main { padding: 14px 16px 90px; }
  .meta { display: flex; gap: 14px; align-items: baseline; flex-wrap: wrap; margin-bottom: 8px; font-size: 13px; }
  .meta .rowid { font-weight: 700; }
  .gdp-tag { font-weight: 600; padding: 1px 8px; border-radius: 999px; border: 1px solid var(--line); background: #fafbfc; }
  .status-tag { font-weight: 600; }
  .status-tag.approved { color: var(--green); }
  .status-tag.flagged { color: var(--amber); }
  .warns { margin: 0 0 10px; padding: 9px 12px; border-radius: 9px; border: 1px solid #f0dca6;
           background: #fdf8ec; font-size: 13px; color: #7a5200; }
  .warns.blocking { border-color: #f0b4b0; background: #fdeeed; color: #7d211b; }
  .ctx { margin: 0 0 10px; padding: 9px 12px; border-radius: 9px; border: 1px solid var(--line);
         background: #fbfcfd; font-size: 13px; color: var(--muted); }
  .ctx ul { margin: 4px 0 0; padding-left: 18px; }
  .warns ul { margin: 4px 0 0; padding-left: 18px; }
  .panes { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  @media (max-width: 1000px) { .panes { grid-template-columns: 1fr; } }
  .pane { background: var(--card); border: 1px solid var(--line); border-radius: 10px; overflow: hidden;
          display: flex; flex-direction: column; }
  .pane h2 { font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: var(--muted);
             margin: 0; padding: 8px 12px; border-bottom: 1px solid var(--line);
             display: flex; justify-content: space-between; gap: 8px; align-items: center; }
  .pane h2 .badge { text-transform: none; letter-spacing: 0; font-weight: 700; padding: 2px 8px; border-radius: 999px;
                    font-size: 11px; }
  .badge.primary-badge { background: #eaf1f9; color: var(--blue); border: 1px solid #c3d6ea; }
  .badge.secondary-badge { background: #fdf8ec; color: #8a5a00; border: 1px solid #f0dca6; }
  .pane .sel { padding: 8px 12px 4px; font-size: 14px; font-weight: 600; }
  .pane .tabs { padding: 2px 12px 8px; font-size: 12px; color: var(--muted); display: flex; gap: 5px;
                flex-wrap: wrap; align-items: center; }
  .tabchip { padding: 1px 7px; border-radius: 5px; border: 1px solid var(--line); background: #f7f8fa; }
  .tabchip.is-default { border-color: var(--blue); color: var(--blue); font-weight: 700; background: #eaf1f9; }
  .tabchip.is-scatter { border-color: var(--green); color: var(--green); font-weight: 700; background: #f0fbf4; }
  .viewtoggle { padding: 0 12px 8px; display: flex; gap: 6px; align-items: center; font-size: 12px; }
  .viewtoggle button { padding: 3px 9px; font-size: 12px; border-radius: 6px; }
  .viewtoggle button.on { background: var(--ink); color: #fff; border-color: var(--ink); }
  .pane .url { padding: 0 12px 8px; font-size: 11px; }
  .pane .url a { color: var(--blue); text-decoration: none; word-break: break-all; }
  .pane .hint { padding: 0 12px 8px; font-size: 11px; color: var(--muted); }
  .pane iframe { width: 100%; height: 620px; border: 0; border-top: 1px solid var(--line); background: #fff; }
  footer { position: fixed; bottom: 0; left: 0; right: 0; background: var(--card); border-top: 1px solid var(--line);
           padding: 10px 16px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  .nav { display: flex; gap: 6px; align-items: center; }
  .note { flex: 1; min-width: 180px; }
  .note input { width: 100%; font: inherit; padding: 7px 10px; border: 1px solid var(--line); border-radius: 8px; }
  select { font: inherit; padding: 6px 8px; border: 1px solid var(--line); border-radius: 8px; }
  .settings { display: none; margin-top: 10px; padding: 12px; background: #fbfcfd; border: 1px solid var(--line); border-radius: 10px; }
  .settings.open { display: block; }
  .settings p { color: var(--muted); font-size: 13px; margin: 0 0 10px; }
  .settings label { display: block; font-size: 12px; color: var(--muted); margin: 8px 0 2px; }
  .settings input[type=text] { width: 100%; font: inherit; padding: 6px 9px; border: 1px solid var(--line); border-radius: 7px; }
  .kbd { font-size: 11px; color: var(--muted); }
  .kbd b { background: #eef1f5; border-radius: 4px; padding: 1px 5px; border: 1px solid var(--line); }
  .toast { position: fixed; left: 50%; bottom: 88px; transform: translateX(-50%); background: #1a1a1a; color: #fff;
           padding: 9px 14px; border-radius: 8px; font-size: 13px; opacity: 0; transition: opacity .2s;
           pointer-events: none; z-index: 50; max-width: 82vw; box-shadow: 0 3px 12px rgba(0,0,0,.25); }
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
    <span class="kbd">keys: <b>←</b>/<b>→</b> nav · <b>a</b> approve · <b>f</b> flag · <b>c</b> clear · <b>v</b> toggle view · <b>r</b> reload frames</span>
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
    <span class="chip"        data-f="warned"   onclick="setFilter('warned')">With warnings</span>
    <span class="spacer"></span>
    <button class="ghost" onclick="resetAll()">Reset all decisions</button>
  </div>
  <div class="settings" id="settings">
    <p>Hosts used to build the panes. Both default to the environment this file was generated
       against — normally the staging server carrying the migration, because the targets'
       scatter views usually do not exist on production yet. Point the left pane at
       <code>https://ourworldindata.org</code> to compare against the live old chart.</p>
    <label>Left pane host (old scatter)</label>
    <input type="text" id="ep-src" oninput="saveEndpoints()" />
    <label>Right pane host (target)</label>
    <input type="text" id="ep-tgt" oninput="saveEndpoints()" />
    <label style="margin-top:12px"><input type="checkbox" id="ep-hide" onchange="saveEndpoints()" />
      Hide Grapher controls in both frames (compact, but hides the tab bar that shows the scatter is secondary)</label>
  </div>
</header>

<main>
  <div class="meta">
    <span class="rowid" id="m-rowid"></span>
    <span class="gdp-tag" id="m-gdp"></span>
    <span id="m-ids" style="color:var(--muted)"></span>
    <span class="status-tag" id="m-status"></span>
  </div>
  <div class="warns" id="m-warns" style="display:none"></div>
  <div class="ctx" id="m-ctx" style="display:none"></div>
  <div class="ctx" id="m-empty" style="display:none"></div>
  <div class="panes" id="panes">
    <div class="pane">
      <h2><span>Old standalone scatter</span><span class="badge primary-badge">scatter IS the chart</span></h2>
      <div class="sel" id="src-title"></div>
      <div class="tabs" id="src-tabs"></div>
      <div class="url" id="src-url"></div>
      <iframe id="src-frame" loading="lazy"></iframe>
    </div>
    <div class="pane">
      <h2><span>Target's scatter view</span><span class="badge secondary-badge" id="tgt-badge"></span></h2>
      <div class="sel" id="tgt-title"></div>
      <div class="tabs" id="tgt-tabs"></div>
      <div class="viewtoggle">
        <button id="btn-redirect" onclick="setView('redirect')">Redirect view</button>
        <button id="btn-default" onclick="setView('default')">Default view</button>
      </div>
      <div class="url" id="tgt-url"></div>
      <div class="hint" id="tgt-hint"></div>
      <iframe id="tgt-frame" loading="lazy"></iframe>
    </div>
  </div>
</main>

<footer>
  <div class="nav">
    <button onclick="go(-1)">◀ Prev</button>
    <select id="jump" onchange="jumpTo(this.value)"></select>
    <button onclick="go(1)">Next ▶</button>
    <button class="ghost" onclick="reloadFrames()" title="Refetch both charts — use after changing a config on staging">↻ Reload frames</button>
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
const LS_DEC = "scatter_review_v1__" + REVIEW_NAME;
const LS_EP  = "scatter_endpoints_v1__" + REVIEW_NAME;

let endpoints = loadEndpoints();
let decisions = JSON.parse(localStorage.getItem(LS_DEC) || "{}");
let filter = "all";
let order = RECORDS.map((_, i) => i);
let pos = 0;
let viewMode = "redirect";

// A decision is bound to the exact pair of configs it was made on. Both md5s are in the
// fingerprint because the reviewer approves a specific rendering of the old chart AND a
// specific rendering of the target's scatter — and a re-run of the applier rewrites the
// target (it re-mirrors the source's y display.name, for one), so an approval must not
// survive that. The GDP variable is in here too: swapping WDI for Maddison changes the
// x-axis the reviewer signed off on without touching either title.
function fp(rec) { return (rec.src_md5 || "") + "::" + (rec.tgt_md5 || "") + "::" + (rec.gdp_var_id || ""); }
(function pruneStaleDecisions() {
  let n = 0;
  for (const rec of RECORDS) {
    const d = decisions[String(rec.id)];
    if (d && d.target !== fp(rec)) { delete decisions[String(rec.id)]; n++; }
  }
  if (n) {
    localStorage.setItem(LS_DEC, JSON.stringify(decisions));
    setTimeout(() => toast(n + " saved decision(s) cleared — the chart config changed since they were made."), 400);
  }
})();

// --- persistence -----------------------------------------------------------
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
      suggestedName: REVIEW_NAME + "_scatter_review.json",
      types: [{ description: "JSON", accept: { "application/json": [".json"] } }],
    });
    fileHandle = h; autoSaveActive = true;
    await writeFile();
    updateSaveStatus();
    toast("Auto-saving every change to " + h.name);
  } catch (e) { if (e && e.name !== "AbortError") toast("Could not link file: " + e.message); }
}

async function writeFile() {
  if (!fileHandle) return;
  try {
    const w = await fileHandle.createWritable();
    await w.write(JSON.stringify(exportRows(), null, 2));
    await w.close();
    lastFileWrite = new Date();
    updateSaveStatus();
  } catch (e) { toast("Write to file failed: " + e.message); autoSaveActive = false; updateSaveStatus(); }
}

function scheduleFileWrite() {
  if (!autoSaveActive || !fileHandle) return;
  clearTimeout(fileWriteTimer);
  fileWriteTimer = setTimeout(writeFile, 600);
}

async function importJSON(file) {
  if (!file) return;
  try {
    const rows = JSON.parse(await file.text());
    if (!Array.isArray(rows)) throw new Error("expected a JSON array");
    const byId = new Map(RECORDS.map((r) => [String(r.id), r]));
    let applied = 0, stale = 0, unknown = 0, cleared = 0;
    for (const row of rows) {
      // Accept exports written before ids became "<src>-<tgt>": fall back to matching on
      // the pair, so a finished review is not thrown away by the key change.
      let id = String(row.id ?? "");
      let rec = byId.get(id);
      if (!rec && row.src_id && row.tgt_id) {
        id = row.src_id + "-" + row.tgt_id;
        rec = byId.get(id);
      }
      if (!rec) { unknown++; continue; }
      // A row exported with no status and no note means "undecided". Skipping it would leave
      // an older approval sitting in localStorage while the imported file says otherwise,
      // so the restored counts would not match the file. Clear it instead.
      if (!row.status && !row.note) {
        if (decisions[id]) { delete decisions[id]; cleared++; }
        continue;
      }
      // Must stay in step with fp() — a shorter fingerprint here silently skips every row.
      const rowFp = (row.src_md5 || "") + "::" + (row.tgt_md5 || "") + "::" + (row.gdp_var_id || "");
      if (rowFp && rowFp !== fp(rec)) { stale++; continue; }
      decisions[id] = { status: row.status || null, note: row.note || "", target: fp(rec) };
      applied++;
    }
    persist(); render();
    toast(`Imported ${applied} decision(s)` + (cleared ? `, cleared ${cleared} back to undecided` : "") + (stale ? `, skipped ${stale} stale` : "") + (unknown ? `, ${unknown} unknown id(s)` : ""));
  } catch (e) { toast("Import failed: " + e.message); }
}

function loadEndpoints() {
  const saved = JSON.parse(localStorage.getItem(LS_EP) || "null");
  return Object.assign({}, DEFAULT_ENDPOINTS, saved || {});
}
function saveEndpoints() {
  endpoints.src = document.getElementById("ep-src").value.trim() || DEFAULT_ENDPOINTS.src;
  endpoints.tgt = document.getElementById("ep-tgt").value.trim() || DEFAULT_ENDPOINTS.tgt;
  endpoints.hideControls = document.getElementById("ep-hide").checked;
  localStorage.setItem(LS_EP, JSON.stringify(endpoints));
  render();
}
function buildEndpointInputs() {
  document.getElementById("ep-src").value = endpoints.src;
  document.getElementById("ep-tgt").value = endpoints.tgt;
  document.getElementById("ep-hide").checked = !!endpoints.hideControls;
}
function toggleSettings() { document.getElementById("settings").classList.toggle("open"); }

function url(host, path) {
  let u = host.replace(/\/$/, "") + path;
  if (endpoints.hideControls) u += (u.includes("?") ? "&" : "?") + "hideControls=true";
  return u;
}
function srcUrl(rec) { return url(endpoints.src, rec.src_path); }
function tgtUrl(rec) { return url(endpoints.tgt, viewMode === "redirect" ? rec.redirect_path : rec.default_path); }

function applyFilter() {
  order = RECORDS.map((_, i) => i).filter((i) => {
    const rec = RECORDS[i];
    const st = (decisions[rec.id] || {}).status || null;
    if (filter === "all") return true;
    if (filter === "todo") return !st;
    if (filter === "warned") return rec.warnings.length > 0;
    return st === filter;
  });
  // No fallback to record 0. Substituting an unrelated row while the UI claims to show only
  // matching rows invites the reviewer to overwrite a decision they cannot see is off-filter
  // — e.g. picking "To review" once every row is approved. render() shows an empty state.
  if (pos >= order.length) pos = Math.max(0, order.length - 1);
}
function setFilter(f) {
  filter = f;
  document.querySelectorAll(".filters .chip").forEach((c) => c.classList.toggle("active", c.dataset.f === f));
  pos = 0; applyFilter(); render();
}
function setView(mode) { viewMode = mode; render(); }

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
    const mark = st === "approved" ? "✓ " : st === "flagged" ? "⚠ " : r.warnings.length ? "• " : "";
    const o = document.createElement("option");
    o.value = k;
    o.textContent = `${mark}${r.src_id} → ${r.tgt_id} · ${r.tgt_slug}`;
    sel.appendChild(o);
  });
  sel.value = pos;
}

function tabChips(rec) {
  const chips = rec.chart_types.map((t) => {
    const cls = ["tabchip"];
    if (t === rec.default_tab) cls.push("is-default");
    if (t === "ScatterPlot") cls.push("is-scatter");
    return `<span class="${cls.join(" ")}">${t}${t === rec.default_tab ? " \u2605" : ""}</span>`;
  });
  // The default tab can be one grapher adds outside chartTypes — Map or Table. Without this
  // the row would show no star at all and the reviewer could not tell where readers land.
  if (!rec.chart_types.includes(rec.default_tab)) {
    chips.unshift(`<span class="tabchip is-default">${rec.default_tab} \u2605</span>`);
  }
  return chips.join("");
}

function setFooterEnabled(on) {
  document.querySelectorAll("footer button, footer select, footer input").forEach((el) => { el.disabled = !on; });
}

function renderEmpty() {
  // Nothing matches the active filter. Show that plainly rather than a row that does not match.
  document.getElementById("panes").style.display = "none";
  document.getElementById("m-warns").style.display = "none";
  document.getElementById("m-ctx").style.display = "none";
  const el = document.getElementById("m-empty");
  el.style.display = "";
  el.textContent = `No rows match "${filter}". Pick another filter above.`;
  document.getElementById("m-rowid").textContent = "0 / 0";
  document.getElementById("m-gdp").textContent = "";
  document.getElementById("m-ids").textContent = "";
  document.getElementById("m-status").textContent = "";
  document.getElementById("note").value = "";
  document.getElementById("jump").innerHTML = "";
  setFooterEnabled(false);
  updateCounts();
}

function render() {
  applyFilter();
  if (order.length === 0) { renderEmpty(); return; }
  document.getElementById("panes").style.display = "";
  document.getElementById("m-empty").style.display = "none";
  setFooterEnabled(true);
  const rec = RECORDS[order[pos]];
  const dec = decisions[rec.id] || {};

  document.getElementById("m-rowid").textContent = `Row ${pos + 1} / ${order.length}`;
  document.getElementById("m-gdp").textContent = "x: " + (rec.gdp_source || "?") + (rec.gdp_var_id ? ` (#${rec.gdp_var_id})` : "");
  document.getElementById("m-ids").textContent = `old #${rec.src_id} → target #${rec.tgt_id}`;
  const stEl = document.getElementById("m-status");
  stEl.textContent = dec.status ? (dec.status === "approved" ? "✓ approved" : "⚠ flagged") : "";
  stEl.className = "status-tag " + (dec.status || "");

  const wEl = document.getElementById("m-warns");
  if (rec.warnings.length) {
    const blocking = rec.warnings.some((w) => w.startsWith("⛔"));
    wEl.className = "warns" + (blocking ? " blocking" : "");
    wEl.style.display = "";
    wEl.innerHTML = "<b>Worth checking:</b><ul>" + rec.warnings.map((w) => `<li>${w}</li>`).join("") + "</ul>";
  } else { wEl.style.display = "none"; }

  const cEl = document.getElementById("m-ctx");
  if (rec.context.length) {
    cEl.style.display = "";
    cEl.innerHTML = "<b>Context:</b><ul>" + rec.context.map((c) => `<li>${c}</li>`).join("") + "</ul>";
  } else { cEl.style.display = "none"; }

  // Left: the old chart, where the scatter was the entire chart.
  document.getElementById("src-title").textContent = rec.src_title;
  document.getElementById("src-tabs").innerHTML = `<span class="tabchip is-scatter">ScatterPlot ★</span>
      <span style="color:var(--muted)">only view · slug <code>${rec.src_slug}</code></span>`;
  const sU = srcUrl(rec);
  document.getElementById("src-url").innerHTML = `<a href="${sU}" target="_blank" rel="noopener">open ↗ ${sU}</a>`;
  setFrame("src-frame", sU, rec.id);

  // Right: the target, where the scatter is one tab among several.
  const badge = document.getElementById("tgt-badge");
  if (rec.scatter_pos === 0) {
    badge.textContent = "no scatter tab!";
  } else if (rec.chart_types.length === 1) {
    badge.textContent = "scatter is the only view";
  } else {
    badge.textContent = `SECONDARY · tab ${rec.scatter_pos} of ${rec.chart_types.length} · opens on ${rec.default_tab}`;
  }
  document.getElementById("tgt-title").textContent = rec.tgt_title;
  document.getElementById("tgt-tabs").innerHTML = tabChips(rec) +
    `<span style="color:var(--muted)">★ = what readers land on · slug <code>${rec.tgt_slug}</code></span>`;
  document.getElementById("btn-redirect").classList.toggle("on", viewMode === "redirect");
  document.getElementById("btn-default").classList.toggle("on", viewMode === "default");
  document.getElementById("btn-default").textContent = "Default view (" + rec.default_tab + ")";
  const tU = tgtUrl(rec);
  document.getElementById("tgt-url").innerHTML = `<a href="${tU}" target="_blank" rel="noopener">open ↗ ${tU}</a>`;
  document.getElementById("tgt-hint").textContent = viewMode === "redirect"
    ? "Exactly what a reader following the retired slug gets. time=latest and country= stand in for the "
      + "adjustments a tab CLICK would make — a URL-supplied tab does not get them."
      + (rec.is_log ? " yScale=log restores the retiring chart's log y axis, which no tab click would." : "")
    : "What a reader opening this chart sees first. Click the Scatter tab inside the frame to see the "
      + "tab-click state, which no URL can reproduce — it should match the Redirect view"
      + (rec.is_log ? ", except the y axis: the click leaves it linear, the redirect makes it log." : ".");
  setFrame("tgt-frame", tU, rec.id + "|" + viewMode);

  document.getElementById("note").value = dec.note || "";
  updateCounts();
  fillJump();
}

// Frames are only re-pointed when their URL changes, so an edit made on staging after this
// file was opened stays invisible behind the browser's cache. `bust` is bumped by the Reload
// button to force both frames to refetch; grapher ignores unknown query params.
let bust = 0;
function setFrame(id, u, key) {
  const f = document.getElementById(id);
  const src = bust ? u + (u.includes("?") ? "&" : "?") + "_r=" + bust : u;
  // Keyed by row as well as URL. Two records can share a target (they share the non-GDP
  // indicator), and the workflow asks the reviewer to click tabs *inside* the frame — so on a
  // URL-only key the next pair would open on the previous pair's manipulated state and could be
  // approved against it. Same reasoning for the source frame.
  const cacheKey = (key || "") + "|" + src;
  if (f.getAttribute("data-key") !== cacheKey) {
    f.src = src;
    f.setAttribute("data-key", cacheKey);
  }
}
function reloadFrames() {
  bust++;
  render();
  toast("Reloaded both frames — picks up config changes made on staging since this file was opened.");
}

function go(delta) { pos = Math.max(0, Math.min(order.length - 1, pos + delta)); render(); }
function jumpTo(k) { pos = parseInt(k, 10); render(); }

function decide(status) {
  if (order.length === 0) return;
  const rec = RECORDS[order[pos]];
  // Capture the next id before re-filtering: when the active filter drops the row we just
  // decided, the order shifts and a naive pos++ would skip the row we meant to land on.
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
  if (order.length === 0) return;
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
      id: r.id, src_id: r.src_id, src_slug: r.src_slug, tgt_id: r.tgt_id, tgt_slug: r.tgt_slug,
      gdp_source: r.gdp_source, gdp_var_id: r.gdp_var_id,
      chart_types: r.chart_types.join(" + "), default_tab: r.default_tab, scatter_pos: r.scatter_pos,
      n_selected: r.n_selected, warnings: r.warnings.join(" | "), context: r.context.join(" | "),
      src_md5: r.src_md5, tgt_md5: r.tgt_md5,
      status: d.status || "", note: d.note || "",
      redirect_url: url(endpoints.tgt, r.redirect_path),
    };
  });
}
function download(name, text, type) {
  const blob = new Blob([text], { type });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob); a.download = name; a.click(); URL.revokeObjectURL(a.href);
}
function exportJSON() { download(REVIEW_NAME + "_scatter_review.json", JSON.stringify(exportRows(), null, 2), "application/json"); }
function exportCSV() {
  const rows = exportRows();
  const cols = ["id", "src_id", "src_slug", "tgt_id", "tgt_slug", "gdp_source", "chart_types", "default_tab",
                "scatter_pos", "n_selected", "status", "note", "warnings", "src_md5", "tgt_md5", "redirect_url"];
  const esc = (v) => `"${String(v).replace(/"/g, '""')}"`;
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => esc(r[c])).join(","));
  download(REVIEW_NAME + "_scatter_review.csv", lines.join("\n"), "text/csv");
}

document.addEventListener("keydown", (e) => {
  if (e.target.tagName === "INPUT" || e.target.tagName === "SELECT") return;
  if (e.key === "ArrowRight") go(1);
  else if (e.key === "ArrowLeft") go(-1);
  else if (e.key.toLowerCase() === "a") decide("approved");
  else if (e.key.toLowerCase() === "f") decide("flagged");
  else if (e.key.toLowerCase() === "c") decide(null);
  else if (e.key.toLowerCase() === "v") setView(viewMode === "redirect" ? "default" : "redirect");
  else if (e.key.toLowerCase() === "r") reloadFrames();
});

buildEndpointInputs();
render();
updateSaveStatus();
</script>
</body>
</html>
"""


def render_html(records: list[dict], endpoints: dict, output_path: Path, name: str) -> None:
    title = f"{name} · old scatter vs. target scatter view"
    html = (
        HTML_TEMPLATE.replace("__TITLE__", title)
        .replace("__RECORDS__", json.dumps(records))
        .replace("__ENDPOINTS__", json.dumps(endpoints))
        .replace("__REVIEW_NAME__", json.dumps(name))
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")


def coverage_report(records: list[dict]) -> None:
    warned = [r for r in records if r["warnings"]]
    blocking = [r for r in records if any(w.startswith("⛔") for w in r["warnings"])]
    secondary = [r for r in records if r["scatter_pos"] and len(r["chart_types"]) > 1]
    print(f"rows: {len(records)}")
    print(f"  scatter is a secondary tab : {len(secondary)}")
    print(f"  with warnings              : {len(warned)}")
    if blocking:
        print(f"  ⛔ NO scatter tab          : {[r['tgt_id'] for r in blocking]}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the HTML to review an add-gdp-scatter migration.")
    ap.add_argument("--name", default="scatter_review", help="Review name; namespaces localStorage and output files.")
    ap.add_argument("--output", type=Path, default=None, help="Output HTML path (default ai/<name>.html).")
    ap.add_argument("--source-host", default=None, help="Left-pane host (default: this environment).")
    ap.add_argument("--target-host", default=None, help="Right-pane host (default: this environment).")
    args = ap.parse_args()

    pairs = json.load(sys.stdin)
    if not isinstance(pairs, list):
        print(
            "ERROR: stdin must be a JSON list of {chart_admin_url, target_chart_admin_url, gdp_source}", file=sys.stderr
        )
        return 2

    records = build_records(pairs)
    if not records:
        print("ERROR: no reviewable rows", file=sys.stderr)
        return 1
    coverage_report(records)

    # Default both panes to the environment we just read the configs from: rendering the
    # target against production would show a chart with no scatter tab at all until the
    # migration ships there, which reads as a broken migration rather than a stale host.
    host = short_host()
    endpoints = {
        "src": args.source_host or host,
        "tgt": args.target_host or host,
        "hideControls": False,
    }
    print(f"pane hosts: left={endpoints['src']}  right={endpoints['tgt']}")

    output_path = args.output or Path(f"ai/{args.name}.html")
    render_html(records, endpoints, output_path, args.name)
    print(f"\nWrote {output_path} ({output_path.stat().st_size // 1024} KB)")
    print("Open in a browser. Decisions auto-save to localStorage.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
