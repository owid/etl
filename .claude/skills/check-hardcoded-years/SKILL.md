---
name: check-hardcoded-years
description: Audit every surface that renders a dataset's indicators — charts, map tabs, MDim views, explorer views, narrative charts, and article embeds/links with time= parameters — for hardcoded time bounds (minTime/maxTime/timelineMinTime/timelineMaxTime/map.time pinned to a number instead of "earliest"/"latest"/absent) and grade each pin against the indicators' actual latest data point. A pinned maxTime silently hides newly added years after every dataset update until a human bumps it by hand. Runs in dataset mode (all surfaces carrying one dataset's indicators — the standard audit in /update-dataset step 7) or site-wide via the public Datasette. Use when the user asks to "check for hardcoded years/times", "find pinned minTime/maxTime", or wants to know which charts won't show newly added data.
metadata:
  internal: true
---

# Check Hardcoded Years (pinned time bounds)

A chart config can pin its default time window to literal values — `maxTime: 2023` instead of `"latest"` (or simply omitting the field). Nothing fails when the dataset later gains 2024–2025 data: the pipeline is green, the indicator carries the new years, and the chart still opens at 2023. Readers never see the update unless they drag the timeline, and the chart's edit history (admin → chart → History; `chart_revisions` on a live DB — the table is not in the public Datasette mirror) shows a human bumping the year by hand every update cycle. This skill finds every such pin, grades it against the data's real time range, and routes each finding to the fix that sticks.

Production scale (2026-07): 180 published charts carried a numeric `maxTime`, ~360 a numeric `minTime`; in a 40-chart sample of the `maxTime` pins, 21 sat **below** their indicator's latest data — e.g. `share-of-women-in-parliament-ipu` pinned at 2023 with data through 2025.

> **Called from:** `/update-dataset` step 7 (author side — standard, run after all remaps) and `/review-data-pr` §8e (reviewer side — verify the outcome). Keep those pointers in sync with this file.

## Inputs / modes

- **Dataset mode** (what `/update-dataset` runs): staging branch name + the new grapher dataset (catalogPath `<ns>/<version>/<short_name>` or `datasets.id`). Audits only surfaces carrying that dataset's indicators, and grades pins against the **new** data's latest time. Run it **after** the indicator upgrade and export re-runs, so the surfaces already reference the new variables.
- **General mode** (site-wide sweep): no inputs; query production via the public Datasette plus the repo greps below. Optionally scope to a namespace or topic.

## What counts as a pin

| Field | Default | Effect of a numeric pin |
|---|---|---|
| `minTime` | `"earliest"` | Chart opens with the start handle at that time — usually deliberate framing (start at 1990) |
| `maxTime` | `"latest"` | Chart opens with the end handle at that time — **newly added years are hidden by default** |
| `timelineMinTime` | `"earliest"` | Clamps the slider itself — earlier data becomes unreachable |
| `timelineMaxTime` | `"latest"` | Clamps the slider itself — **later data becomes unreachable, not even draggable** |
| `map.time` | `"latest"` | Map tab renders that time — a pinned map keeps showing the old year |
| `map.startTime` | — | With `map.time`, defines a map time range — same staleness risk |

A **number** is a pin; `"earliest"` / `"latest"` / absent are fine. In ETL YAML, quoted numeric strings count too (`minTime: "1433"` in the monkeypox explorer config is a pin). `map.time` / `map.startTime` only matter when the chart actually exposes the map (`hasMapTab: true` or `tab: "map"`) — on a chart without a map tab they're dormant config with no reader-facing effect (the chart-approval check gates on the same conditions), so skip them there rather than raising false findings.

**Time values are not always years.** Daily-frequency indicators use day offsets from the variable's zero day (`timelineMinTime: 600` in the covid explorer, `minTime: "1433"` in monkeypox). Always compare a pin against the variable's own time axis (below) — never eyeball "that doesn't look like a year". Negative values are BC years (`minTime: -13` exists in production), and values beyond the current year are projection charts (2100) — both legitimate.

## Availability lookup and grading

The per-variable latest time comes from the indicators API `metadata.json` → `dimensions.years.values[].id` (same endpoint, prefix rules, caching, and failed-fetch semantics as [`check-empty-entities`](../check-empty-entities/SKILL.md): staging via `OWIDEnv.from_staging(branch).indicators_url` — never hand-build `staging-site-<branch>`; production via `https://api.ourworldindata.org/v1/indicators/<id>.metadata.json`; a failed fetch is *unknown*, never graded). For a chart, grade against the max over its y-variables' latest times — **except `map.time`/`map.startTime`, which grade against the map's own indicator**: `map.columnSlug` when set (stored as a stringified variable id — cast before the lookup), else the first y variable, which is grapher's fallback (see `etl/indicator_upgrade/indicator_update.py`). A map isn't stale because a *different* y-series runs longer, and a stale map-only indicator would otherwise be missed. During a dataset update you can read the same numbers for free from the locally built grapher dataset instead.

- 🔴 **Hides data now** — `maxTime` / `map.time` / `timelineMaxTime` pinned **below** the variables' latest time. Readers open the chart and don't see the newest data; `timelineMaxTime` is the worst case (the new years can't even be reached). In dataset mode this means the update is invisible on that chart.
- 🟡 **Goes stale next cycle** — the same fields pinned **equal to** the latest time. Renders identically to `"latest"` today, but silently becomes a 🔴 at the next update — this is the manual-bump treadmill the chart history shows. Propose switching to `"latest"` now.
- ℹ️ **Probably deliberate** — report, don't push: numeric `minTime` / `timelineMinTime` / `map.startTime` (editorial start-year framing, pre-X data-quality cutoffs); `minTime == maxTime` single-year charts; pins **above** the latest time (projection headroom); narrative charts (a pinned period is often their entire point — audit the **merged parent+patch** config via `AdminAPI(...).get_narrative_chart(id)["configFull"]`, same gotcha as everywhere else).
- ℹ️ **Projection-series exception** — when the reference indicator's latest time lies in the **future**, a hiding-field pin below it is usually the last-estimates cutoff, not staleness: UN WPP MDim views deliberately pin `map.time` to the last estimate year so the map doesn't open deep in the projection tail (see `etl/steps/export/multidim/un/latest/view_edits.py`). Grade ℹ️ and never propose `"latest"` there — that change is actively harmful. Apply the is-it-future test only to year axes; a day-offset axis needs the `zeroDay` conversion first.
- ℹ️ **Incomplete-latest-year exception** — when the source's newest year is only partially reported, export steps deliberately pin `maxTime` / `timelineMaxTime` / `map.time` at the last fully reported year, usually via a named constant with an explanatory comment (`LAST_FULLY_REPORTED_YEAR` in `etl/steps/export/multidim/natural_disasters/latest/shared.py` — the EM-DAT MDim). Switching those to `"latest"` would surface data the step explicitly declares misleading. So **before proposing `"latest"` for any 🔴/🟡 on an ETL-defined surface, read the pin's source** (the export step's YAML/Python): a pin set on purpose with a documented reason stays ℹ️, its correct maintenance is bumping the constant at the next update, and the audit's only job there is to flag when the constant has visibly fallen behind the last *complete* year.

**Deliberate-pin signal:** the pinned value appears in the chart's `title`, `subtitle`, `note`, or slug (comparison charts like `...-2020-vs-1980`). Those pins are *coupled* to FAUST text — if anyone ever bumps the pin, the text must change with it — so never auto-fix them, and say so in the report. The heuristic only makes sense for year-valued time axes, not day offsets.

## Surfaces and where the fix lives

| Surface | Where the pins are | Where the fix goes |
|---|---|---|
| Charts | `chart_dimensions` → `charts` → `chart_configs.full` | Chart config edit — `AdminAPI` (`apps/chart_sync/admin_api.py`): `get_chart_config(id)`, set the field to `"latest"`, `update_chart(id, cfg)`. In an update, apply on **staging** with user sign-off — the edit rides Chart Diff to production at merge. General mode: hand the list to the user or get explicit sign-off per batch before touching production. |
| MDim views | `multi_dim_x_chart_configs` → `chart_configs` (resolved configs) | The MDim YAML in `etl/steps/export/multidim/...` (view `config:` blocks / `common_view_config`), then re-run the export step. A DB-side fix is **overwritten at the next rebuild** — the YAML is the source of truth. |
| Explorer views | `explorer_views` → `chart_configs` (join `explorers` for `isPublished`) | ETL-based explorers: `etl/steps/export/explorers/.../*.config.yml` + export re-run. Non-ETL explorers (no `explorer_views` rows; definition lives in `explorers.config`): the explorer admin TSV (`minTime`/`maxTime` columns in the graphers block, `time=` in `defaultView`) — report as a coverage caveat if you don't parse it. |
| Narrative charts | merged parent+patch via `AdminAPI.get_narrative_chart` | Usually deliberate (ℹ️). If a pin must move, edit the narrative chart's patch via `AdminAPI.update_narrative_chart` — with user sign-off, as with any reader-facing change. |
| Indicator-level configs | `presentation.grapher_config` in garden/grapher `.meta.yml` (repo grep below) | The `.meta.yml` + step re-run with `--grapher`. These propagate into every thin MDim/explorer view that inherits the indicator's config, so one pinned field here fans out to many views. |
| Article references | `posts_gdocs_links` (join `posts_gdocs pg ON pg.id = pgl.sourceId` — the FK is `sourceId`, not a gdoc-named column — and filter `pg.published = 1`) whose `queryString` carries `time=` — embeds and hyperlinks to charts, MDims, and explorers alike | Gdoc edit (content follow-up). Same plumbing as `check-empty-entities` §4: resolve `target` through `chart_slug_redirects`, link each citation with a scroll-to-highlight URL via `find_chart_citations_in_content`, and verify fixes on the live article page, not the lagging Datasette mirror. |

**Parsing `time=` in URLs:** the grapher time param is a single value (`time=2019`) or a range (`time=1990..2020`, `time=earliest..2023`). Each **numeric** component is a pin — grade the end bound like `maxTime` (an embed with `time=..2019` keeps showing the old window after the data reaches 2025), the start bound like `minTime`. `earliest`/`latest` components are fine, and daily charts use ISO dates (`time=2020-01-01..latest`) — convert those to day offsets before grading: a day-axis variable advertises `display.zeroDay` in the same `metadata.json` (its `dimensions.years.values[].id` are day offsets from that date), so the component's comparable value is `(date.fromisoformat(part) - date.fromisoformat(zero_day)).days`. Skip `$time`-style template placeholders in country-page dynamic embeds. Article time pins are often deliberate framing of the surrounding prose, so default them to 🟡-at-worst and always hand them to content follow-up rather than editing configs.

Repo scan for the ETL-side sources (also catches pins that haven't reached any DB yet). The flat keys grep directly; the **nested map bounds (`map:` → `time`/`startTime`) never match a flat pattern** — walk the YAML for those:

```bash
rg -n '"?(minTime|maxTime|timelineMinTime|timelineMaxTime)"?:' \
    etl/steps/export/multidim etl/steps/export/explorers \
    etl/steps/data/garden etl/steps/data/grapher -g "*.yml"
```

```python
import yaml
from pathlib import Path

def map_pins(node, trail=()):
    if isinstance(node, dict):
        m = node.get("map")
        if isinstance(m, dict):
            for f in ("time", "startTime"):
                if f in m and str(m[f]) not in ("earliest", "latest"):
                    yield "/".join(trail), f, m[f]
        for k, v in node.items():
            yield from map_pins(v, trail + (str(k),))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from map_pins(v, trail + (str(i),))

for p in Path("etl/steps").rglob("*.yml"):
    try:
        for trail, f, v in map_pins(yaml.safe_load(p.read_text()) or {}):
            print(f"{p}: map.{f} = {v}  (at {trail})")
    except yaml.YAMLError:
        if "map:" in p.read_text():
            print(f"UNPARSED (likely Jinja-templated): {p} — inspect its map: block manually")
```

Don't drop the unparsed files silently — Jinja-templated garden metas fail `safe_load`, and one of them can be exactly the file carrying the pin (e.g. `pisa.meta.yml` nests `map: time: 2022` under `presentation.grapher_config`).

## Script skeleton

Same loop shape as `check-empty-entities` — one config pass, grading inline:

```python
import json
from datetime import date
from etl.config import OWIDEnv
from etl.http import session as http_session

env = OWIDEnv.from_staging("<branch>")
PREFIX = env.indicators_url  # normalized container name — never hand-build staging-site-<branch>
CURRENT_YEAR = date.today().year
cache = {}

def latest_time(var_id, prefix=PREFIX):
    """(latest time id, zeroDay) — a set zeroDay means the axis is day offsets, not years."""
    key = (prefix, var_id)
    if key not in cache:
        r = http_session.get(f"{prefix}/{var_id}.metadata.json", timeout=60)
        if r.ok:
            m = r.json()
            vals = m["dimensions"]["years"]["values"]
            cache[key] = (max(v["id"] for v in vals) if vals else None, (m.get("display") or {}).get("zeroDay"))
        else:
            cache[key] = (None, None)  # unknown, never grade on it
    return cache[key]

HIDING = {"maxTime", "timelineMaxTime", "map.time"}

def pins(cfg):
    fields = ["minTime", "maxTime", "timelineMinTime", "timelineMaxTime"]
    if cfg.get("hasMapTab") or cfg.get("tab") == "map":
        fields += ["map.time", "map.startTime"]  # dormant when no map is exposed — skip to avoid false findings
    for path in fields:
        v = cfg
        for k in path.split("."):
            v = (v or {}).get(k) if isinstance(v, dict) else None
        if isinstance(v, str) and v not in ("earliest", "latest"):
            try:
                v = float(v)  # quoted numeric pins ('maxTime: "2023"') count too
            except ValueError:
                continue
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            yield path, v

cfgs = env.read_sql("""
    SELECT DISTINCT c.id AS chart_id, cc.slug, cc.full AS config
    FROM chart_dimensions cd
    JOIN variables v ON cd.variableId = v.id
    JOIN charts c ON c.id = cd.chartId
    JOIN chart_configs cc ON cc.id = c.configId
    WHERE v.datasetId = %(d)s""", params={"d": DATASET_ID})

for _, row in cfgs.iterrows():
    cfg = json.loads(row["config"])
    y_ids = [d["variableId"] for d in cfg.get("dimensions", []) if d.get("property") == "y"]
    fetched = [latest_time(v) for v in y_ids]
    t_max = max((t for t, _ in fetched if t is not None), default=None)
    day_axis = any(zd for _, zd in fetched)
    # the map renders its own indicator: map.columnSlug (stringified variable id) when set, else the first y variable
    map_slug = (cfg.get("map") or {}).get("columnSlug")
    t_map = latest_time(int(map_slug))[0] if str(map_slug or "").isdigit() else (fetched[0][0] if fetched else None)
    faust = " ".join(str(cfg.get(k, "")) for k in ("title", "subtitle", "note")) + " " + (row["slug"] or "")
    for field, val in pins(cfg):
        ref = t_map if field.startswith("map.") else t_max
        if ref is None:
            continue  # unknown availability — coverage caveat, not a finding
        if field in HIDING and not day_axis and ref > CURRENT_YEAR:
            sev = "ℹ️"  # projection series — the pin is usually the estimates cutoff, not staleness
        elif field in HIDING and val < ref:
            sev = "🔴"
        elif field in HIDING and val == ref:
            sev = "🟡"
        else:
            sev = "ℹ️"
        deliberate = str(int(val)) in faust  # year-axis heuristic only — skip for day-offset variables
        ...  # collect (sev, surface, chart_id, slug, field, val, ref, deliberate)
```

Repeat over `multi_dim_x_chart_configs`, `explorer_views`, narrative-chart merged configs, and parsed `posts_gdocs_links` query strings (`time=` components), then fold in the repo grep. Query gotchas: pymysql `%`-formats break on quoted literals — parameterize everything. **The public Datasette mirror is DuckDB**, not SQLite/MySQL — `json_type()` returns DuckDB type names (`UBIGINT`, `VARCHAR`, …), so don't filter numerics by type name in SQL; use `TRY_CAST(json_extract_string(cc.full, '$.maxTime') AS DOUBLE) IS NOT NULL` or pull `json_extract_string(...)` values and classify client-side.

## Report format

One table per severity — chart/view id, surface, admin or grapher link (staging links via `OWIDEnv.from_staging(branch).chart_site(slug)` / short admin URLs), pinned field + value, the variables' latest time, deliberate-signal flags, and the fix route from the surfaces table. Then:

- **🔴 in dataset mode = the update is invisible on that surface** — propose the `"latest"` fix before merge (charts on staging, MDims/explorers in their YAML). Reader-facing default-view changes always get user sign-off first.
- **🟡** — propose the same fix; it costs nothing now and removes the next cycle's manual bump.
- **ℹ️ / deliberate** — list only. Where a pin is coupled to FAUST text, say so explicitly so nobody bumps the number without the words.
- **Coverage caveats** — variables whose metadata fetch failed; non-ETL explorers whose TSV wasn't parsed.

Pins are almost always **pre-existing** (updates don't create them), so there's no production-grading pass here — the question is only whether the new data now extends past the pin.
