---
name: chart-editing
description: Create or edit an ETL-authored Grapher chart — a single-chart `.config.yml` in `etl/steps/export/multidim/`. Use when the user wants to author a chart from ETL, edit one, change its title/subtitle/colors/map settings, or preview an ETL-authored chart on staging. For charts with dropdowns (multi-dimensional), use the `create-multidim` skill instead.
metadata:
  internal: true
---

# Chart Editing (ETL-authored single charts)

ETL-authored single charts are stored as zero-dimension mdim collections — a `.config.yml` with `dimensions: []` and exactly one view. Each layer of a chart is its own `chart_configs` row: ETL pushes to the one named by `charts.patchConfigIdETL`, admin edits land in the one named by `charts.patchConfigId`, and the two never collide.

This skill covers creating and editing those `.config.yml` files, pushing them to staging, and previewing the result.

## File layout

```
etl/steps/export/multidim/<namespace>/latest/<short_name>.config.yml
```

Reference example in this repo:

- `etl/steps/export/multidim/animal_welfare/latest/banning_of_chick_culling.config.yml`

The chart's public slug is auto-derived from the short name with underscores replaced by dashes (`banning_of_chick_culling` → `banning-of-chick-culling`).

## Minimum viable config

```yaml
chart_config_id: "0191b6c7-5595-70b2-8d30-fa03fccd7add"
topic_tags:
  - "Animal Welfare"
dimensions: []
views:
  - dimensions: {}
    indicators:
      y:
        - catalogPath: "<dataset_short_name>#<indicator_short_name>"
    config:
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

- `chart_config_id` — **required**, the chart's identity in grapher (`charts.configId`). See "The chart's identity" below.
- `dimensions: []` and exactly one view → this YAML pushes as a single chart, not an mdim page.
- `views[0].indicators.y` — list of indicator catalog paths. For multi-series, list more than one.
- `views[0].config` — the grapher config that becomes the chart's `etlConfig` in `chart_configs`. Same shape as a chart-admin export. Omit `$schema` — the current default (`DEFAULT_GRAPHER_SCHEMA` in `etl/config.py`) is applied automatically, so pinning a version only ages badly.
- No top-level `title:` or `default_selection:` block — those exist only for multidim data pages and are ignored for single charts.

## The chart's identity (`chart_config_id`)

At push time ETL addresses a chart only by its config UUID (`charts.configId`) — never by slug or numeric id — and it never looks the chart up per environment. The YAML must therefore declare the UUID, and the same YAML then targets the same chart on local, staging, and production. (Slug and numeric id are still how *you* find the UUID once, while authoring; see `lookup` below.)

Use `etl chart-config-id` to write the field — it validates that the target really is a single-chart config and refuses to clobber an existing UUID:

```bash
# New chart: mint a UUIDv7.
.venv/bin/etl chart-config-id new <config.yml>

# Existing chart moving into ETL: take the UUID from the chart already in grapher, so the
# config lands on it instead of creating a duplicate. Name the chart by slug or by the
# numeric id from its admin URL — exactly one of the two.
.venv/bin/etl chart-config-id lookup <config.yml> --slug banning-of-chick-culling
.venv/bin/etl chart-config-id lookup <config.yml> --chart-id 7118
```

`lookup` queries the configured grapher DB (`OWID_ENV`); pass `--env <staging-branch>` (or `--env <path/to/.env>`) to look elsewhere. The chart is never inferred from the file name — picking the wrong chart is the failure this field exists to prevent, so you name it explicitly.

Never change `chart_config_id` once it's committed — a changed UUID means "a different chart", so the push creates a new draft chart and abandons the old one. That's why both subcommands require `--force` to overwrite.

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
    - catalogPath: "<dataset>#<indicator_a>"
      display:
        name: "Short label A"
    - catalogPath: "<dataset>#<indicator_b>"
      display:
        name: "Short label B"
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

For the authoritative list, see the schema at `DEFAULT_GRAPHER_SCHEMA` (`etl/config.py`).

## Pushing to staging

```bash
.venv/bin/etlr export://multidim/<namespace>/latest/<short_name> --export
```

Editing the YAML is enough to trigger a re-run — ETL's change detection picks it up, so no extra flags are needed. Reserve `--force --only` for re-pushing when *nothing* changed (and note `--only` skips dependency resolution, so it fails unless the upstream datasets are already built locally). The step prints `admin_url=http://staging-site-<branch>/admin/charts/<id>/edit` on success.

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
3. Push: `.venv/bin/etlr export://multidim/<namespace>/latest/<short_name> --export`.
4. Preview the PNG (see above) and iterate.
5. Once the chart looks right, commit the `.config.yml` (and the DAG entry if newly added) on the working branch.

## Admin edits coexist with ETL edits

Once a chart is on staging, an admin (human) can edit it in the chart editor. Those edits land in the chart's admin layer (`charts.patchConfigId`) and survive subsequent ETL pushes — the layered model is exactly:

```
the rendered config (charts.configId) = merge(indicator config, ETL layer, admin layer)
```

Admin overrides always win on a per-field basis. To "unlink" a field back to the ETL-authored value, click the chip next to the field in the admin editor — it clears that field from the admin layer.

## When NOT to use this skill

- **Chart with dropdowns / dimension selectors** → use `create-multidim`. Single-chart `.config.yml` files have `dimensions: []`; multi-dim ones don't.
- **Brand-new chart from scratch and you want the structure auto-generated** → use `create-multidim` even for single charts; it writes the YAML skeleton, and you set `dimensions: []` after. `create-multidim` knows nothing about `chart_config_id` (the field is rejected on real mdims), so once the config is single-chart, run `etl chart-config-id new <config.yml>` before pushing — otherwise the first run fails validation.
- **Editing a chart that exists only in the admin (no `.config.yml`)** → adopt it into ETL first: write the `.config.yml`, then point it at the existing chart with `etl chart-config-id lookup <config.yml> --chart-id <id>` (see "The chart's identity"), and edit here. Tooling to generate the rest of the YAML from the live config (`chart_pull` CLI) is a Phase 1 follow-up.

## Related skills

- `check-chart-preview` — for previewing the rendered chart (PNG or browser screenshot).
- `create-multidim` — for charts with dropdowns.
- `chart-preview` VSCode extension — interactive preview pane while you edit.
