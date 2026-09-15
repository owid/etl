---
name: create-chart
description: >-
  Create or edit a Grapher chart authored in ETL: a `viz://chart` step in `etl/steps/viz/chart/`,
  either a single chart (`dimensions: []`, one view) or a multidim (a chart with dropdown dimension
  selectors, also called MDIM). Use when the user wants to author a chart from ETL, build a multidim,
  combine several charts into one with dimension toggles, edit an ETL-authored chart's config (title,
  subtitle, colors, map settings, default entities), adopt an admin-only chart into ETL, or mentions
  "multidim", "MDIM" or "viz://chart". For explorers use `create-explorer`.
allowed-tools:
  - WebFetch
  - "Bash(.venv/bin/etl:*)"
  - "Bash(.venv/bin/etlr:*)"
  - "Bash(mkdir:*)"
metadata:
  internal: true
---

# Creating charts from ETL

A chart authored in ETL is a `viz://chart` step: a `.config.yml` next to a small Python step in
`etl/steps/viz/chart/<namespace>/latest/`, registered in the DAG and pushed to the grapher DB with
`--grapher`. The same step type covers two shapes:

- **Single chart** — `dimensions: []` and exactly one view. Pushes as a plain Grapher chart with a slug,
  addressed by its `chart_config_id`. Example: `etl/steps/viz/chart/animal_welfare/latest/banning_of_chick_culling.config.yml`.
- **Multidim** — one or more `dimensions` with dropdown choices and one view per combination (e.g. a Sex
  dropdown showing life expectancy for males or females). Publishes as a multidim data page. Example:
  `etl/steps/viz/chart/wid/latest/wealth_wid.config.yml`.

"Chart" is the umbrella term and the one the step type uses. People, and parts of the code
(`etl/viz/chart/`), still say "multidim" or "MDIM" for the second shape; treat the words as
interchangeable. Explorers (`viz://explorer`) are the sibling with their own skill, `create-explorer`.

## Overview

Every chart step needs three things:

1. A **Python step** file (minimal boilerplate)
2. A **config YAML** file (views, chart settings; dimensions for multidims)
3. A **DAG entry** in the appropriate `dag/*.yml` file

## Step 1: Identify the indicators

If the user provides chart URLs, fetch their metadata to discover the indicator names and catalog paths. If
creating from scratch, find the relevant grapher dataset and its indicators.

```
# Get indicator shortNames and structure
https://ourworldindata.org/grapher/{chart-slug}.metadata.json

# Get the full catalogPath for each indicator (from fullMetadata URL in above response)
https://api.ourworldindata.org/v1/indicators/{id}.metadata.json
```

**Reference indicators by the short `{table}#{variable_name}` form** (e.g.
`child_labor#share_child_labor__sex_total__age_5_17`). PathFinder resolves the namespace/version/dataset
from the step's DAG dependency, so the config never hardcodes the version — when the dataset version bumps,
only the DAG entry changes. See `etl/steps/viz/chart/wid/latest/wealth_wid.config.yml` for a real example.

The full form `grapher/{namespace}/{version}/{dataset}/{table}#{variable_name}` is valid too, but only reach
for it to disambiguate when two DAG dependencies both contain a table of the same name. Never hardcode the
version just to "be explicit" — it rots on the next update.

Look at the indicator shortNames to identify the dimensional structure. For example:
- `life_expectancy__sex_female__age_0__type_period` → dimensions: sex, age
- `weekly_cases` vs `weekly_deaths` → dimension: indicator (cases/deaths)

## Step 2: Choose the shape and design the dimensions

Ask one question: **does the reader need to switch between views?**

- **No** → single chart. One view, `dimensions: []`. Several indicators can still share the chart as
  separate lines (see "Several indicators as separate lines").
- **Yes** → multidim. Decide which aspects become dropdown dimensions and which stay as multiple lines:
  - **As separate views (dropdown dimension):** when switching changes what the chart is about. Example:
    toggling between Males and Females.
  - **As multiple y-indicators on one chart:** when all values should be visible simultaneously for
    comparison. Example: life expectancy at different ages (birth, 10, 25, 65) as separate lines on one chart.

## Step 3: Create the files

### Directory structure

```
etl/steps/viz/chart/{namespace}/latest/
├── {short_name}.py
└── {short_name}.config.yml
```

Create the directory if it doesn't exist:
```bash
mkdir -p etl/steps/viz/chart/{namespace}/latest
```

The chart's public slug is derived from the short name with underscores replaced by dashes
(`banning_of_chick_culling` → `banning-of-chick-culling`).

### Python file (same boilerplate for both shapes)

```python
from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    c = paths.create_chart(config=paths.load_config())
    c.save()
```

This is sufficient for config-driven charts (explicit views in YAML). For more advanced patterns
(programmatic view generation from table data, combining charts, grouping views, post-processing the config
before saving — `banning_of_chick_culling.py` expands its map colors from the data), look at existing
examples in `etl/steps/viz/chart/`.

### Config YAML file

Use the template for the shape you chose: "Config YAML: single chart" or "Config YAML: multidim" below.

## Step 4: Single charts only — set the chart's identity (`chart_config_id`)

At push time ETL addresses a single chart only by its config UUID (`charts.configId`) — never by slug or
numeric id — and it never looks the chart up per environment. The YAML must therefore declare the UUID, and
the same YAML then targets the same chart on local, staging and production. (Slug and numeric id are still
how *you* find the UUID once, while authoring; see `lookup` below.)

Use `etl chart-config-id` to write the field — it validates that the target really is a single-chart config
and refuses to clobber an existing UUID:

```bash
# New chart: mint a UUIDv7.
.venv/bin/etl chart-config-id new <config.yml>

# Existing chart moving into ETL: take the UUID from the chart already in grapher, so the
# config lands on it instead of creating a duplicate. Name the chart by slug or by the
# numeric id from its admin URL — exactly one of the two.
.venv/bin/etl chart-config-id lookup <config.yml> --slug banning-of-chick-culling
.venv/bin/etl chart-config-id lookup <config.yml> --chart-id 7118
```

`lookup` queries the configured grapher DB (`OWID_ENV`); pass `--env <staging-branch>` (or
`--env <path/to/.env>`) to look elsewhere. The chart is never inferred from the file name — picking the
wrong chart is the failure this field exists to prevent, so you name it explicitly.

Never change `chart_config_id` once it's committed — a changed UUID means "a different chart", so the push
creates a new draft chart and abandons the old one. That's why both subcommands require `--force` to
overwrite.

**Multidims must not carry `chart_config_id`**: the field is rejected on configs with dimensions. Its
absence on a single chart fails validation on the first run, so mint it before pushing.

## Step 5: Register in the DAG

Add to the appropriate `dag/*.yml` file (find it by searching for the grapher dataset dependency), right
after the grapher step it depends on:

```yaml
  #
  # <Chart description> — chart authored in ETL.
  #
  viz://chart/{namespace}/latest/{short_name}:
    - data://grapher/{namespace}/{version}/{dataset}
```

The dependency is the upstream `grapher` step whose dataset contains the indicators referenced in the views.

## Step 6: Run and verify

**Always run the step after creating it** — schema validation only happens at runtime, so errors (like
invalid fields in `config`, or a missing `chart_config_id`) won't surface until the step is executed. CI
will catch these, but fix them locally first.

```bash
# Chart steps write to the grapher DB, so they need the --grapher flag
.venv/bin/etlr viz://chart/{namespace}/latest/{short_name} --grapher
```

Editing the YAML is enough to trigger a re-run — ETL's change detection picks it up, so no extra flags are
needed. Reserve `--force --only` for re-pushing when *nothing* changed, and note that `--only` skips
dependency resolution, so it fails unless the upstream datasets are already built locally.

On success the step prints where to look:

- single chart: `admin_url=http://staging-site-<branch>/admin/charts/<id>/edit`
- multidim: `PREVIEW: http://staging-site-{branch}/admin/grapher/{namespace}%2Flatest%2F{short_name}%23{short_name}/`

To see the rendered chart, use the `check-chart-preview` skill: its `get_chart_png_url.py` helper resolves
a slug to a PNG URL that works for unpublished charts, and it can also take a browser screenshot.

## Config YAML: single chart

```yaml
grapher_schema: "011"  # QUOTED — a bare 011 is YAML octal
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

- `grapher_schema` — **required**, and there is no fallback: the grapher chart-config schema version this
  config is written against, which becomes the chart's `$schema` and is what lets grapher migrate the
  config after a breaking schema change. Use the version in `DEFAULT_GRAPHER_SCHEMA` (`etl/config.py`) when
  authoring, then leave it alone. Quote it — an unquoted `011` is YAML octal.
- `chart_config_id` — **required** for single charts, the chart's identity in grapher (`charts.configId`).
  See Step 4.
- `topic_tags` — **required**, see "Topic tags".
- `dimensions: []` and exactly one view → this YAML pushes as a single chart, not a multidim page.
- `views[0].indicators.y` — list of indicator catalog paths. For multi-series, list more than one.
- `views[0].config` — the grapher config that becomes the chart's `etlConfig` in `chart_configs`. Same shape
  as a chart-admin export. Never put `$schema` in here: it would override the top-level `grapher_schema`
  while being far less visible (ETL warns when it does).
- No top-level `title:`, `default_selection:` or `default_dimensions:` block — those exist only for multidim
  pages and are ignored for single charts.

## Config YAML: multidim

```yaml
# REQUIRED — grapher chart-config schema the view configs below are written against, as a
# QUOTED string (a bare `011` is YAML octal). There is no fallback: ETL fails without it. Use the
# current DEFAULT_GRAPHER_SCHEMA version (etl/config.py) when authoring a new chart, then leave it
# alone: it is what lets Grapher migrate the config forward after a breaking schema change.
grapher_schema: "011"
# Never put `$schema` inside a view's `config` block: Grapher lets the view value override this
# chart-level pin, so the two silently disagree. ETL warns when that happens.

title:
  title: "Chart Title"
  title_variant: ""

# REQUIRED — one or more topic tags (see "Topic tags" section below)
topic_tags:
  - tag 1
  - tag 2

default_selection:
  - World

# Pre-select dimension values (use slug values)
default_dimensions:
  sex: female

# Shared config applied to all views
definitions:
  common_views:
    - config:
        originUrl: ourworldindata.org/topic-page
        hasMapTab: true        # or false for multi-indicator line charts
        tab: line              # or map
        chartTypes:
          - LineChart
        yAxis:
          min: 0
      metadata:
        description_key:
          - First key point about this data.
          - Second key point about methodology.

dimensions:
  - slug: sex
    name: Sex
    choices:
      - slug: female
        name: Females
      - slug: male
        name: Males

views:
  - dimensions:
      sex: female
    indicators:
      y:
        - catalogPath: table#variable_female
    config:
      title: "Title for females view"
      subtitle: "Subtitle for females view"

  - dimensions:
      sex: male
    indicators:
      y:
        - catalogPath: table#variable_male
    config:
      title: "Title for males view"
      subtitle: "Subtitle for males view"
```

### Topic tags (required)

Every chart **must** declare at least one `topic_tags` entry — it's a top-level key in the config (right
after `title` on multidims, after `chart_config_id` on single charts).

```yaml
topic_tags:
  - tag 1
  - tag 2
```

Rules:
- Each entry must **exactly match** one of the valid tag names below (case- and spelling-sensitive, e.g.
  `War & Peace`, not `war and peace`).
- The **first** tag is the primary topic — order it deliberately.
- Reuse the tags of the charts/topic the chart is built from; a new chart rarely needs a brand-new tag.

Valid topic tags (from `topic_tags` in `schemas/dataset-schema.json`):

The schema enum is a static snapshot; if a tag seems missing, the canonical live list is this [Datasette query](https://datasette-public.owid.io/owid?sql=SELECT%0D%0A++DISTINCT+t.name%0D%0AFROM%0D%0A++tag_graph+tg%0D%0A++LEFT+JOIN+tags+t+ON+tg.childId+%3D+t.id%0D%0A++LEFT+JOIN+posts_gdocs+p+ON+t.slug+%3D+p.slug%0D%0A++AND+p.published+%3D+1%0D%0A++AND+p.type+IN+%28%27article%27%2C+%27topic-page%27%2C+%27linear-topic-page%27%29%0D%0AWHERE%0D%0A++p.slug+IS+NOT+NULL%0D%0AUNION%0D%0ASELECT%0D%0A++%27Uncategorized%27%0D%0AORDER+BY%0D%0A++t.name).

`Access to Energy`, `Age Structure`, `Agricultural Production`, `Air Pollution`, `Alcohol Consumption`, `Animal Welfare`, `Antibiotics & Antibiotic Resistance`, `Artificial Intelligence`, `Biodiversity`, `Books`, `Burden of Disease`, `CO2 & Greenhouse Gas Emissions`, `COVID-19`, `Cancer`, `Cardiovascular Diseases`, `Causes of Death`, `Child & Infant Mortality`, `Child Labor`, `Clean Water`, `Clean Water & Sanitation`, `Climate Change`, `Corruption`, `Crop Yields`, `Democracy`, `Diarrheal Diseases`, `Diet Compositions`, `Economic Growth`, `Economic Inequality`, `Economic Inequality by Gender`, `Education Spending`, `Electricity Mix`, `Employment in Agriculture`, `Energy`, `Energy Mix`, `Environmental Impacts of Food Production`, `Eradication of Diseases`, `Famines`, `Farm Size`, `Fertility Rate`, `Fertilizers`, `Fish & Overfishing`, `Food Prices`, `Food Supply`, `Foreign Aid`, `Forests & Deforestation`, `Fossil Fuels`, `Gender Ratio`, `Global Education`, `Global Health`, `Government Spending`, `HIV/AIDS`, `Happiness & Life Satisfaction`, `Healthcare Spending`, `Homelessness`, `Homicides`, `Housing`, `Human Development Index (HDI)`, `Human Height`, `Human Rights`, `Hunger & Undernourishment`, `Illicit Drug Use`, `Indoor Air Pollution`, `Influenza`, `Internet`, `LGBT+ Rights`, `Land Use`, `Lead Pollution`, `Life Expectancy`, `Light at Night`, `Literacy`, `Loneliness & Social Connections`, `Malaria`, `Marriages & Divorces`, `Maternal Mortality`, `Meat & Dairy Production`, `Medicine & Biotechnology`, `Mental Health`, `Metals & Minerals`, `Micronutrient Deficiency`, `Migration`, `Military Personnel & Spending`, `Mpox (monkeypox)`, `Natural Disasters`, `Neglected Tropical Diseases`, `Nuclear Energy`, `Nuclear Weapons`, `Obesity`, `Oil Spills`, `Outdoor Air Pollution`, `Ozone Layer`, `Pandemics`, `Pesticides`, `Plastic Pollution`, `Pneumonia`, `Polio`, `Population Growth`, `Poverty`, `Religion`, `Renewable Energy`, `Research & Development`, `Sanitation`, `Smallpox`, `Smoking`, `Space Exploration & Satellites`, `State Capacity`, `Suicides`, `Taxation`, `Technological Change`, `Terrorism`, `Tetanus`, `Time Use`, `Tourism`, `Trade & Globalization`, `Transport`, `Trust`, `Tuberculosis`, `Uncategorized`, `Urbanization`, `Vaccination`, `Violence Against Children & Children's Rights`, `War & Peace`, `Waste Management`, `Water Use & Stress`, `Wildfires`, `Women's Employment`, `Women's Rights`, `Work & Employment`, `Working Hours`

### Several indicators as separate lines

The legend label defaults to the indicator's full title. For better legends, pass each indicator as an
object with `display.name`. Works the same in a single chart's only view and in any multidim view:

```yaml
views:
  - dimensions:
      sex: female        # {} on a single chart
    indicators:
      y:
        - catalogPath: tb#indicator_a
          display:
            name: "Label for line A"
        - catalogPath: tb#indicator_b
          display:
            name: "Label for line B"
    config:
      title: "Chart with multiple lines"
      subtitle: "Description"
      selectedFacetStrategy: entity   # Important for multi-indicator line charts
      hasMapTab: false                # Map doesn't work well with multiple indicators
```

Other useful `display` fields: `unit`, `shortUnit`, `numDecimalPlaces`, `roundingMode`,
`numSignificantFigures`, `tolerance`, `zeroDay`.

### Dimension-specific common_views overrides

Override settings for specific dimension combinations:

```yaml
definitions:
  common_views:
    - config:
        # Base config for all views
        hasMapTab: true
        chartTypes: ["LineChart"]
    - dimensions:
        indicator: share
      config:
        # Override just for "share" indicator views
        note: "Share values sum to 100%"
        map:
          colorScale:
            binningStrategy: manual
```

### Per-view FAUST: inherit from garden, don't re-type it

A view's chart config can omit `title`/`subtitle`/`note` — each view then inherits FAUST from the
indicator's `presentation.grapher_config` in the **garden** `.meta.yml` (templated by dimension).
Inheritance is from `grapher_config` only — there is no fallback to the indicator
`title`/`description_short`/`display.name`. So to replicate an existing chart's FAUST across many views, set
`grapher_config.title`/`subtitle`/`note` once in the garden metadata (e.g. age-aware via a Jinja `<% if %>`
template), rebuild the grapher step, and leave the view configs thin. To verify what will actually render,
read the resolved per-view config from `multi_dim_x_chart_configs` → `chart_configs` in the staging DB.

## Chart config options

Key fields for `config` in views or `common_views`:

| What | Field | Notes |
|---|---|---|
| Chart type | `chartTypes: ["LineChart"]` | `LineChart`, `ScatterPlot`, `StackedArea`, `DiscreteBar`, `StackedDiscreteBar`, `SlopeChart`, `StackedBar`, `Marimekko` |
| Default tab | `tab: "chart"` | `chart`, `map`, `table`, `line`, `slope`, `discrete-bar`, `marimekko` |
| Map tab visible? | `hasMapTab: true` | Set with `tab: "map"` for map-by-default charts; avoid with multi-indicator views |
| Facet strategy | `selectedFacetStrategy: entity` | `entity`, `metric`, `none` — how to facet multi-indicator charts |
| Y-axis range | `yAxis: { min: 0, max: 100 }` | Use `"auto"` for auto-scaling |
| Default entities | `selectedEntityNames: ["United States"]` | List of country / region names (single charts; multidims use top-level `default_selection`) |
| Footer note | `note: "..."` | Caveats, methodology, source notes |
| Origin URL | `originUrl: "/topic-page-slug"` | Links the chart to its topic page |
| Map colors | `map.colorScale.customCategoryColors: {...}` | For categorical indicators on a map |
| Color scheme | `map.colorScale.baseColorScheme: "BinaryMapPaletteA"` | See grapher schema for valid values |
| Hide map timeline | `map.hideTimeline: true` | For point-in-time map charts |

For the authoritative list, see the schema at `DEFAULT_GRAPHER_SCHEMA` (`etl/config.py`).

## Common dimension patterns

| Domain | Dimension | Typical choices |
|--------|-----------|----------------|
| Demographics | sex | female, male, both_sexes |
| Demographics | age | at_birth, at_10, at_15, at_25, at_45, at_65, at_80 |
| Economics | metric | absolute, per_capita, share_of_gdp |
| Time series | frequency | annual, monthly, weekly |
| Statistics | estimate | central, low, high |

## Editing an existing chart

1. Read the current `.config.yml` and the upstream dataset's `.meta.yml` (so you know what indicators exist
   and their default titles/units).
2. Edit the YAML with the `Edit` tool. Preserve comments with `ruamel` if needed (see
   `etl.files.ruamel_load/dump`).
3. Push: `.venv/bin/etlr viz://chart/<namespace>/latest/<short_name> --grapher`.
4. Preview (see Step 6) and iterate.
5. Once the chart looks right, commit the `.config.yml` (and the DAG entry if newly added) on the working
   branch.

Reader-facing text (title, subtitle, note, units, `description_key`) has its own router: the
`edit-faust-metadata` skill decides whether the change belongs in the garden `.meta.yml`, the chart config
or the admin layer, and reports which other charts it touches. Use it for text edits; this skill covers the
config file mechanics.

### Admin edits coexist with ETL edits

Each layer of a single chart is its own `chart_configs` row: ETL pushes to the one named by
`charts.patchConfigIdETL`, admin edits land in the one named by `charts.patchConfigId`, and the two never
collide. Once a chart is on staging, an admin (human) can edit it in the chart editor; those edits survive
subsequent ETL pushes — the layered model is exactly:

```
the rendered config (charts.configId) = merge(indicator config, ETL layer, admin layer)
```

Admin overrides always win on a per-field basis. To "unlink" a field back to the ETL-authored value, click
the chip next to the field in the admin editor — it clears that field from the admin layer.

### Adopting a chart that exists only in the admin

Write the `.config.yml` (single-chart shape), then point it at the existing chart with
`etl chart-config-id lookup <config.yml> --chart-id <id>` (see Step 4), and edit here from then on. Tooling
to generate the rest of the YAML from the live config (`chart_pull` CLI) is a follow-up.

## Troubleshooting

**Chart built but not on staging**: without `--grapher`, `etlr viz://chart/...` only writes the config under
`viz/chart/` and logs `chart.not_upserted`; pass `--grapher` to upsert.

**Validation fails on `chart_config_id`**: a single chart (`dimensions: []`) without it → run
`etl chart-config-id new <config.yml>`; a multidim with it → remove the field, multidims are not addressed by
UUID.

**Step not found in DAG**: check that the entry is under the `steps:` key in the correct `dag/*.yml` file,
and that the file is included from `dag/main.yml`.

**Preview URL shows errors**: verify that the catalogPaths in your config match actual indicators in the
grapher dataset. Check by running the grapher step first: `.venv/bin/etlr data://grapher/{namespace}/{version}/{dataset} --grapher`.

**`config must not contain {'description_key'}` or similar**: view-level metadata like `description_key`,
`description_short`, and `presentation` belong under `metadata`, not `config`. The `config` block is for
chart settings only (title, subtitle, chartTypes, etc.).

## Related skills

- `create-explorer` — the `viz://explorer` sibling: same engine and YAML schema, different channel and
  top-level block.
- `check-chart-preview` — render the chart on staging (PNG URL or browser screenshot).
- `edit-faust-metadata` — routes reader-facing text edits to the right layer.
- `chart-preview` VSCode extension — interactive preview pane while you edit.
