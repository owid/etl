---
name: "create-multidim"
description: "Create multi-dimensional (multidim/MDIM) chart configurations in the OWID ETL pipeline. Use this skill when the user wants to create a new multidim, build a multi-dimensional chart, combine multiple charts into one with dimension toggles, or mentions 'multidim' or 'MDIM'."
allowed-tools:
- "WebFetch"
- "Bash(.venv/bin/etl:*)"
- "Bash(mkdir:*)"
metadata:
  internal: true
---

# Creating Multidim Charts

A multidim (multi-dimensional chart) is an interactive chart with dropdown selectors for different dimensions of the data. It can be created from scratch or by combining existing charts. For example, a single multidim with a Sex dropdown showing life expectancy for males or females.

## Overview

A multidim requires three things:
1. A **Python step** file (minimal boilerplate)
2. A **config YAML** file (dimensions, views, chart settings)
3. A **DAG entry** in the appropriate `dag/*.yml` file

All files live in `etl/steps/export/multidim/{namespace}/latest/`.

## Step-by-Step Process

### Step 1: Identify the indicators

If the user provides chart URLs, fetch their metadata to discover the indicator names and catalog paths. If creating from scratch, find the relevant grapher dataset and its indicators.

```
# Get indicator shortNames and structure
https://ourworldindata.org/grapher/{chart-slug}.metadata.json

# Get the full catalogPath for each indicator (from fullMetadata URL in above response)
https://api.ourworldindata.org/v1/indicators/{id}.metadata.json
```

**Reference indicators by the short `{table}#{variable_name}` form** (e.g. `child_labor#share_child_labor__sex_total__age_5_17`). PathFinder resolves the namespace/version/dataset from the step's DAG dependency, so the config never hardcodes the version — when the dataset version bumps, only the DAG entry changes. See `etl/steps/export/multidim/wid/latest/wealth_wid.config.yml` for a real example.

The full form `grapher/{namespace}/{version}/{dataset}/{table}#{variable_name}` is valid too, but only reach for it to disambiguate when two DAG dependencies both contain a table of the same name. Never hardcode the version just to "be explicit" — it rots on the next update.

Look at the indicator shortNames to identify the dimensional structure. For example:
- `life_expectancy__sex_female__age_0__type_period` → dimensions: sex, age
- `weekly_cases` vs `weekly_deaths` → dimension: indicator (cases/deaths)

### Step 2: Design the dimensions

Decide which aspects become dropdown dimensions vs. multi-line indicators on a single chart.

**As separate views (dropdown dimension):** When switching between them changes what the chart is about. Example: toggling between Males and Females.

**As multiple y-indicators on one chart:** When all values should be visible simultaneously for comparison. Example: life expectancy at different ages (birth, 10, 25, 65) shown as separate lines on one chart.

### Step 3: Create the files

#### Directory structure
```
etl/steps/export/multidim/{namespace}/latest/
├── {short_name}.py
└── {short_name}.config.yml
```

Create the directory if it doesn't exist:
```bash
mkdir -p etl/steps/export/multidim/{namespace}/latest
```

#### Python file (always the same boilerplate)

```python
from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    c = paths.create_collection(
        config=paths.load_collection_config(),
        short_name="{short_name}",
    )
    c.save()
```

This is sufficient for config-driven multidims (explicit views in YAML). For more advanced patterns (programmatic view generation from table data, combining collections, grouping views), look at existing examples in `etl/steps/export/multidim/` for reference.

#### Config YAML file

See below for the config structure and examples.

### Step 4: Register in the DAG

Add to the appropriate `dag/*.yml` file (find it by searching for the grapher dataset dependency):

```yaml
export://multidim/{namespace}/latest/{short_name}:
  - data://grapher/{namespace}/{version}/{dataset}
```

Place it right after the grapher step it depends on.

### Step 5: Run and verify

**Always run the step after creating it** — schema validation only happens at runtime, so errors (like invalid fields in `config`) won't surface until the step is executed. CI will catch these, but it's better to fix them locally first.

```bash
# Export steps are excluded by default — the --export flag is required
.venv/bin/etl run {short_name} --export --only --private
```

This outputs a preview URL like:
```
PREVIEW: http://staging-site-{branch}/admin/grapher/{namespace}%2Flatest%2F{short_name}%23{short_name}/
```

The ETL has built-in change detection — if you modify the config, it will automatically re-run on the next invocation without needing `--force`.

## Config YAML Structure

```yaml
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

Every multidim **must** declare at least one `topic_tags` entry — it's a top-level key in the config (right after `title`).

```yaml
topic_tags:
  - tag 1
  - tag 2
```

Rules:
- Each entry must **exactly match** one of the valid tag names below (case- and spelling-sensitive, e.g. `War & Peace`, not `war and peace`).
- The **first** tag is the primary topic — order it deliberately.
- Reuse the tags of the charts/topic the mdim is built from; a new mdim rarely needs a brand-new tag.

Valid topic tags (from `topic_tags` in `schemas/dataset-schema.json`):

The schema enum is a static snapshot; if a tag seems missing, the canonical live list is this [Datasette query](https://datasette-public.owid.io/owid?sql=SELECT%0D%0A++DISTINCT+t.name%0D%0AFROM%0D%0A++tag_graph+tg%0D%0A++LEFT+JOIN+tags+t+ON+tg.childId+%3D+t.id%0D%0A++LEFT+JOIN+posts_gdocs+p+ON+t.slug+%3D+p.slug%0D%0A++AND+p.published+%3D+1%0D%0A++AND+p.type+IN+%28%27article%27%2C+%27topic-page%27%2C+%27linear-topic-page%27%29%0D%0AWHERE%0D%0A++p.slug+IS+NOT+NULL%0D%0AUNION%0D%0ASELECT%0D%0A++%27Uncategorized%27%0D%0AORDER+BY%0D%0A++t.name).

`Access to Energy`, `Age Structure`, `Agricultural Production`, `Air Pollution`, `Alcohol Consumption`, `Animal Welfare`, `Antibiotics & Antibiotic Resistance`, `Artificial Intelligence`, `Biodiversity`, `Books`, `Burden of Disease`, `CO2 & Greenhouse Gas Emissions`, `COVID-19`, `Cancer`, `Cardiovascular Diseases`, `Causes of Death`, `Child & Infant Mortality`, `Child Labor`, `Clean Water`, `Clean Water & Sanitation`, `Climate Change`, `Corruption`, `Crop Yields`, `Democracy`, `Diarrheal Diseases`, `Diet Compositions`, `Economic Growth`, `Economic Inequality`, `Economic Inequality by Gender`, `Education Spending`, `Electricity Mix`, `Employment in Agriculture`, `Energy`, `Energy Mix`, `Environmental Impacts of Food Production`, `Eradication of Diseases`, `Famines`, `Farm Size`, `Fertility Rate`, `Fertilizers`, `Fish & Overfishing`, `Food Prices`, `Food Supply`, `Foreign Aid`, `Forests & Deforestation`, `Fossil Fuels`, `Gender Ratio`, `Global Education`, `Global Health`, `Government Spending`, `HIV/AIDS`, `Happiness & Life Satisfaction`, `Healthcare Spending`, `Homelessness`, `Homicides`, `Housing`, `Human Development Index (HDI)`, `Human Height`, `Human Rights`, `Hunger & Undernourishment`, `Illicit Drug Use`, `Indoor Air Pollution`, `Influenza`, `Internet`, `LGBT+ Rights`, `Land Use`, `Lead Pollution`, `Life Expectancy`, `Light at Night`, `Literacy`, `Loneliness & Social Connections`, `Malaria`, `Marriages & Divorces`, `Maternal Mortality`, `Meat & Dairy Production`, `Medicine & Biotechnology`, `Mental Health`, `Metals & Minerals`, `Micronutrient Deficiency`, `Migration`, `Military Personnel & Spending`, `Mpox (monkeypox)`, `Natural Disasters`, `Neglected Tropical Diseases`, `Nuclear Energy`, `Nuclear Weapons`, `Obesity`, `Oil Spills`, `Outdoor Air Pollution`, `Ozone Layer`, `Pandemics`, `Pesticides`, `Plastic Pollution`, `Pneumonia`, `Polio`, `Population Growth`, `Poverty`, `Religion`, `Renewable Energy`, `Research & Development`, `Sanitation`, `Smallpox`, `Smoking`, `Space Exploration & Satellites`, `State Capacity`, `Suicides`, `Taxation`, `Technological Change`, `Terrorism`, `Tetanus`, `Time Use`, `Tourism`, `Trade & Globalization`, `Transport`, `Trust`, `Tuberculosis`, `Uncategorized`, `Urbanization`, `Vaccination`, `Violence Against Children & Children's Rights`, `War & Peace`, `Waste Management`, `Water Use & Stress`, `Wildfires`, `Women's Employment`, `Women's Rights`, `Work & Employment`, `Working Hours`



When a view should show several indicators as separate lines:

```yaml
views:
  - dimensions:
      sex: female
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

## Per-view FAUST: inherit from garden, don't re-type it

A view's chart config can omit `title`/`subtitle`/`note` — each view then inherits FAUST from the indicator's `presentation.grapher_config` in the **garden** `.meta.yml` (templated by dimension). Inheritance is from `grapher_config` only — there is no fallback to the indicator `title`/`description_short`/`display.name`. So to replicate an existing chart's FAUST across many views, set `grapher_config.title`/`subtitle`/`note` once in the garden metadata (e.g. age-aware via a Jinja `<% if %>` template), rebuild the grapher step, and leave the view configs thin. To verify what will actually render, read the resolved per-view config from `multi_dim_x_chart_configs` → `chart_configs` in the staging DB.

## Common Dimension Patterns

| Domain | Dimension | Typical choices |
|--------|-----------|----------------|
| Demographics | sex | female, male, both_sexes |
| Demographics | age | at_birth, at_10, at_15, at_25, at_45, at_65, at_80 |
| Economics | metric | absolute, per_capita, share_of_gdp |
| Time series | frequency | annual, monthly, weekly |
| Statistics | estimate | central, low, high |

## Chart Config Options

Key fields for `config` in views or `common_views`:

| Field | Values | Notes |
|-------|--------|-------|
| `tab` | `line`, `map`, `table` | Default tab shown |
| `chartTypes` | `["LineChart"]`, `["DiscreteBar"]`, `["StackedBar"]`, `["ScatterPlot"]` | Chart visualization type |
| `hasMapTab` | `true`/`false` | Show map tab (avoid with multi-indicator views) |
| `selectedFacetStrategy` | `entity`, `metric`, `none` | How to facet multi-indicator charts |
| `yAxis.min` | number | Y-axis minimum |
| `originUrl` | URL path | Links back to topic page |
| `note` | string | Footer note on chart |

## Troubleshooting

**"No steps matched"**: Export steps need the `--export` flag. Without it, they're excluded from matching.

**Step not found in DAG**: Check that the entry is under the `steps:` key in the correct `dag/*.yml` file, and that the file is included from `dag/main.yml`.

**Preview URL shows errors**: Verify that the catalogPaths in your config match actual indicators in the grapher dataset. Check by running the grapher step first: `.venv/bin/etl run {dataset} --grapher --private`.

**`config must not contain {'description_key'}` or similar**: View-level metadata like `description_key`, `description_short`, and `presentation` belong under `metadata`, not `config`. The `config` block is for chart settings only (title, subtitle, chartTypes, etc.).
