---
name: check-empty-entities
description: Audit every surface that renders a dataset's indicators — charts, map tabs, MDim views, narrative charts, and article references — for views whose pinned entity selection has no data in the new indicators (they render as empty charts with no error anywhere). Grades findings against production to separate update regressions from pre-existing gaps. Use after an indicator upgrade on staging, when the user asks to "check for empty entities/views/charts", or as the audit step called from /update-dataset (step 7) and /review-data-pr (§8d).
metadata:
  internal: true
---

# Check Empty Entities

After an indicator upgrade, a view can end up pinned to entities that have no data in the new indicators. Nothing fails: the pipeline is green, chart-diff renders, and the chart silently opens **empty**. This skill audits every surface that stores an entity selection against the entities that actually have data, and grades each finding against production so regressions and pre-existing gaps aren't conflated.

> **Called from:** `/update-dataset` step 7 (author side — fix regressions before merge) and `/review-data-pr` §8d (reviewer side — verify the outcome). Keep those pointers in sync with this file.

## Inputs

- Staging branch name (e.g. `Marigold/wdi-update`) — determines the DB (`OWIDEnv.from_staging(branch)`) and the indicators API prefix.
- The new grapher dataset: catalogPath `<ns>/<version>/<short_name>` or its `datasets.id` on staging.

## Availability lookup (used by every check)

A variable's entities-with-data come from the indicators API `metadata.json` — **not** MySQL (indicator data lives outside the DB):

- Staging: `https://api-staging.owid.io/staging-site-<branch>/v1/indicators/<id>.metadata.json` → `dimensions.entities.values[].name`
- Production: `https://api.ourworldindata.org/v1/indicators/<id>.metadata.json`

Cache per variable id and fetch in parallel (a large dataset means hundreds of variables). The path prefix is `staging-site-<branch>`, not the bare branch name — a wrong prefix silently serves another environment.

## Checks

### 1. Charts

For every chart on the new dataset (`chart_dimensions` → `variables.datasetId`), parse `chart_configs.full`:

- `selectedEntityNames` must intersect the union of the chart's y-variables' entities-with-data. Zero overlap on a non-empty selection = the chart renders empty.
- **Skip ScatterPlot and Marimekko** — they legitimately have no `selectedEntityNames` (they plot all entities).
- A **missing/empty selection** on other chart types is only a finding if production's config differs — the upgrader never touches entity selections, so an empty selection is almost always pre-existing. Verify via public Datasette before flagging.
- Also flag any y-variable whose entity list is entirely empty (a broken indicator, not just a broken view).

### 2. Map tabs

For every chart with `hasMapTab`, assert `map.columnSlug` is one of the chart's dimension variable ids. It's stored as a **string** — str-cast before comparing (the int-vs-string mismatch is exactly how the upgrader left map tabs pinned to old variables for years; see #6457). A `columnSlug` that resolves to a variable outside the chart's dimensions, or to a dangling id, is a finding.

### 3. MDim views and narrative charts

Same selection-vs-availability check on their configs:

- MDim views: `multi_dim_x_chart_configs` → `chart_configs` (all MDims, not just the dataset's own — other MDims can carry this dataset's variables in y-dimensions).
- Narrative charts: `narrative_charts.chartConfigId` → `chart_configs`.

### 4. Article references (gdoc embeds and hyperlinks)

`posts_gdocs_links` rows (`linkType='grapher'`, **published** gdocs only) whose `queryString` carries `country=` pin entities in the URL — the upgrader never rewrites these. Parsing rules learned the hard way:

- Entities are `~`-separated; **legacy URLs use `+`**, which `parse_qs` decodes to spaces — a chunk with spaces may itself be one entity name ("South Asia"), so try a full-chunk match first, then greedy multi-word matching against the `entities` table.
- Skip `$entityCode` / `$entityName` template placeholders (country-page dynamic embeds).
- Resolve ISO codes via the `entities` table (`code` → `name`).
- Match `posts_gdocs_links.target` through `chart_slug_redirects` too — embeds often use old slugs.
- Only a **fully dead selection** is a finding — the link opens an empty chart. A partial gap still renders the remaining entities (report at most as an aside).
- For content hand-off, link each citation with a scroll-to-highlight URL — reuse `find_chart_citations_in_content` from `apps/wizard/app_pages/chart_diff/citations.py`. Caveats: its embedded-chart pass only scans **top-level** body blocks (recurse yourself for charts nested in layout containers), most `country=` references turn out to be *hyperlinks* (its second pass, which does recurse), and data insights may store the chart reference where neither pass looks — fall back to the data-insight page URL. Wrap fragment URLs in `<angle brackets>` in markdown (they can contain parentheses).

### 5. Grade against production

For every finding, fetch the same chart's **production** y-variables (chart ids are shared; get prod `chart_dimensions` via public Datasette) and their entity lists from the production API:

- Selection had data on production, none on staging → **regression from this update**. Author: fix before merge (remap the view or restore the entities). Reviewer: 🔴.
- Gap identical on production → **pre-existing**. List it in the PR body (or hand to content follow-up for gdoc edits); don't block the PR. Reviewer: 🟢, confirm it's documented.
- Public Datasette covers only ~80% of chart ids — when a chart has no baseline, say so instead of silently classifying it as pre-existing.

## Script skeleton

One pass over staging, then a grading pass against production:

```python
import json
from concurrent.futures import ThreadPoolExecutor
from etl.config import OWIDEnv
from etl.http import session as http_session

env = OWIDEnv.from_staging("<branch>")
cache = {}

def entities(var_id, prefix="https://api-staging.owid.io/staging-site-<branch>"):
    key = (prefix, var_id)
    if key not in cache:
        r = http_session.get(f"{prefix}/v1/indicators/{var_id}.metadata.json", timeout=60)
        cache[key] = {e["name"] for e in r.json()["dimensions"]["entities"]["values"]} if r.ok else None
    return cache[key]

cfgs = env.read_sql("""
    SELECT DISTINCT c.id AS chart_id, cc.slug, cc.full AS config
    FROM chart_dimensions cd
    JOIN variables v ON cd.variableId = v.id
    JOIN charts c ON c.id = cd.chartId
    JOIN chart_configs cc ON cc.id = c.configId
    WHERE v.datasetId = %(d)s""", params={"d": DATASET_ID})

for _, row in cfgs.iterrows():
    cfg = json.loads(row["config"])
    types = cfg.get("chartTypes", ["LineChart"])
    sel = cfg.get("selectedEntityNames") or []
    y_ids = [d["variableId"] for d in cfg.get("dimensions", []) if d.get("property") == "y"]
    if cfg.get("hasMapTab"):
        assert str((cfg.get("map") or {}).get("columnSlug")) in {str(d["variableId"]) for d in cfg["dimensions"]}
    if types and types[0] in ("ScatterPlot", "Marimekko"):
        continue
    avail = set().union(*(entities(v) or set() for v in y_ids)) if y_ids else set()
    if sel and not (set(sel) & avail):
        ...  # finding -> grade against production
```

Repeat the loop shape over `multi_dim_x_chart_configs`/`narrative_charts` configs and over parsed `posts_gdocs_links` query strings. Query gotcha: pymysql `%`-formats break on quoted literals and `LIKE` patterns — parameterize everything (`params={...}`), and use `CHAR_LENGTH(x) = 0` instead of `x = ''`.

## Report format

- **Regressions** (block): view, surface, entities lost, prod evidence.
- **Pre-existing gaps** (document): table of citation (scroll-to-highlight link), chart (staging grapher link, `http://staging-site-<branch>/grapher/<slug>`), and dead entities — the common pattern is old URLs using unsuffixed WB region / income-group names while data lives under `(WB)`-suffixed entities.
- **Coverage caveats**: charts with no production baseline; variables whose metadata fetch failed (don't count fetch failures as empty).

Reference run (WDI 2026-07, PR #6439): 480 charts, 37 MDim views, 18 narrative charts, 585 gdoc references — zero regressions, 10 pre-existing gaps handed to content follow-up.
