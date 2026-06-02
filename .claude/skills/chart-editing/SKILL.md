---
name: chart-editing
description: Create or edit an ETL-authored Grapher chart — a single-chart `.config.yml` in `etl/steps/export/multidim/`. Use when the user wants to author a chart from ETL, edit one, change its title/subtitle/colors/map settings, or preview an ETL-authored chart on staging. For charts with dropdowns (multi-dimensional), use the `create-multidim` skill instead.
metadata:
  internal: true
---

# Chart Editing (ETL-authored single charts)

ETL-authored single charts are stored as zero-dimension mdim collections — a `.config.yml` with `dimensions: []` and exactly one view. ETL pushes them to Grapher's `chart_configs.etlConfig` column; admin edits land in `chart_configs.patch`; the two layers never collide.

This skill covers creating and editing those `.config.yml` files, pushing them to staging, and previewing the result.

## File layout

```
etl/steps/export/multidim/<namespace>/latest/<short_name>.config.yml
```

Examples (this repo, as of Phase 1):

- `etl/steps/export/multidim/animal_welfare/latest/banning_of_chick_culling.config.yml`
- `etl/steps/export/multidim/animal_welfare/latest/hens_by_housing_system.config.yml`

The chart's public slug is auto-derived from the short name with underscores replaced by dashes (`hens_by_housing_system` → `hens-by-housing-system`).

## Minimum viable config

```yaml
title:
  title: "Your chart title"
  title_variant: ""
default_selection:
  - "United States"
topic_tags:
  - "Animal Welfare"
dimensions: []
views:
  - dimensions: {}
    indicators:
      y:
        - catalogPath: "<dataset_short_name>#<indicator_short_name>"
    config:
      $schema: "https://files.ourworldindata.org/schemas/grapher-schema.009.json"
      title: "Your chart title"
      subtitle: "One-line context for the chart."
      note: "Any caveats, sources of bias, methodology notes."
      originUrl: "/your-topic-page"
      tab: "chart"
      chartTypes:
        - "LineChart"  # or StackedArea, DiscreteBar, etc.
      yAxis:
        min: 0
      selectedEntityNames:
        - "United States"
```

Key fields:

- `dimensions: []` and exactly one view → this YAML pushes as a single chart, not an mdim page.
- `views[0].indicators.y` — list of indicator catalog paths. For multi-series, list more than one.
- `views[0].config` — the grapher config that becomes the chart's `etlConfig` in `chart_configs`. Same shape as a chart-admin export.

## DAG entry

Add to `dag/<namespace>.yml`:

```yaml
  #
  # <Chart description> — chart authored in ETL.
  #
  export://multidim/<namespace>/latest/<short_name>:
    - data://grapher/<namespace>/<version>/<dataset_short_name>
```

The dependency is the upstream `grapher` step whose dataset contains the indicators referenced in `views[0].indicators.y`.

## Indicators with custom display names

The legend label defaults to the indicator's full title. For better legends, pass each indicator as an object with `display.name`:

```yaml
indicators:
  y:
    - catalogPath: "eggs_and_hens_statistics#number_of_hens_in_cages"
      display:
        name: "Cages"
    - catalogPath: "eggs_and_hens_statistics#number_of_hens_in_barns"
      display:
        name: "Barns"
```

Other useful `display` fields: `unit`, `shortUnit`, `numDecimalPlaces`, `roundingMode`, `numSignificantFigures`, `tolerance`, `zeroDay`.

## Common chart-config edits

Inside `views[0].config`:

| What | Field | Notes |
|---|---|---|
| Chart type | `chartTypes: ["LineChart"]` | `LineChart`, `ScatterPlot`, `StackedArea`, `DiscreteBar`, `StackedDiscreteBar`, `SlopeChart`, `StackedBar`, `Marimekko` |
| Default tab | `tab: "chart"` | `chart`, `map`, `table`, `line`, `slope`, `discrete-bar`, `marimekko` |
| Map tab visible? | `hasMapTab: true` | Set with `tab: "map"` for map-by-default charts |
| Y-axis range | `yAxis: { min: 0, max: 100 }` | Use `"auto"` for auto-scaling |
| Default entities | `selectedEntityNames: ["United States"]` | List of country / region names |
| Footer note | `note: "..."` | Caveats, methodology, source notes |
| Origin URL | `originUrl: "/topic-page-slug"` | Links the chart to its topic page |
| Map colors | `map.colorScale.customCategoryColors: {...}` | For categorical indicators on a map |
| Color scheme | `map.colorScale.baseColorScheme: "BinaryMapPaletteA"` | See grapher schema for valid values |
| Hide map timeline | `map.hideTimeline: true` | For point-in-time map charts |
| Topic page tag | `topic_tags: ["Animal Welfare"]` | Top-level field, outside `views` |

For the authoritative list, the grapher schema is at the URL in `$schema:` — currently `grapher-schema.009.json`.

## Pushing to staging

```bash
.venv/bin/etlr export://multidim/<namespace>/latest/<short_name> --export --force --only
```

`--force --only` re-pushes even when nothing changed (useful when iterating on the YAML). The step prints `admin_url=http://staging-site-<branch>/admin/charts/<id>/edit` on success.

## Previewing

Use the `check-chart-preview` skill — its `get_chart_png_url.py` helper resolves the chart slug to a draft-friendly PNG URL via `/grapher/by-uuid/<UUID>.png` (works for unpublished charts):

```bash
URL=$(.venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py <slug>)
curl -o ai/chart_preview.png "$URL"
```

Pass extra grapher query params with `--key=value`:

```bash
.venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py <slug> --tab=map
.venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py <slug> --tab=chart --time=2020 --country=USA~GBR~FRA
```

Read the resulting PNG with the `Read` tool to view the chart.

## Editing workflow

1. Read the current `.config.yml` and the upstream dataset's `.meta.yml` (so you know what indicators exist and their default titles/units).
2. Edit the YAML using the `Edit` tool. Preserve comments with `ruamel` if needed (see `etl.files.ruamel_load/dump`).
3. Push: `.venv/bin/etlr export://multidim/<namespace>/latest/<short_name> --export --force --only`.
4. Preview the PNG (see above) and iterate.
5. Once the chart looks right, commit the `.config.yml` (and the DAG entry if newly added) on the working branch.

## Admin edits coexist with ETL edits

Once a chart is on staging, an admin (human) can edit it in the chart editor. Those edits land in `chart_configs.patch` and survive subsequent ETL pushes — the layered model is exactly:

```
chart_configs.full = merge(variableETL, etlConfig, patch)
```

Admin overrides always win on a per-field basis. To "unlink" a field back to the ETL-authored value, click the chip next to the field in the admin editor — it clears that field from `patch`.

## When NOT to use this skill

- **Chart with dropdowns / dimension selectors** → use `create-multidim`. Single-chart `.config.yml` files have `dimensions: []`; multi-dim ones don't.
- **Brand-new chart from scratch and you want the structure auto-generated** → use `create-multidim` even for single charts; it writes the YAML skeleton, and you set `dimensions: []` after.
- **Editing a chart that exists only in the admin (no `.config.yml`)** → adopt it into ETL first, then edit here. Adoption tooling (`chart_pull` CLI) is a Phase 1 follow-up.

## Related skills

- `check-chart-preview` — for previewing the rendered chart (PNG or browser screenshot).
- `create-multidim` — for charts with dropdowns.
- `chart-preview` VSCode extension — interactive preview pane while you edit.
