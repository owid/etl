# Creating a Multidim from Existing Chart Slugs

This guide walks through building a new multidimensional indicator (MDIM) that combines
the data behind two or more existing standalone Grapher charts, using a single dropdown
to switch between them.

## Overview

The workflow is:

1. **Query datasette** to find chart configs and variable catalog paths
2. **Create three files** — a Python step, a YAML config, and a DAG entry
3. **Run the export step** to push the MDIM to the database

---

## Step 1: Query Datasette to Identify Source Data

A datasette instance runs at `http://analytics` and exposes the `private` database with
tables including `charts` and `variables`.

### Finding chart configs by slug

Use the JSON API with filter parameters. The `_shape=array` parameter returns a flat
array instead of nested objects:

```bash
# Fetch specific charts by slug
curl -s "http://analytics/private/charts.json?\
slug__in=death-rate-from-air-pollution-per-100000,share-deaths-air-pollution\
&_shape=array\
&_col=id&_col=slug&_col=config&_col=title"
```

Key datasette filter operators:
- `column__exact=value` — exact match (default, so `column=value` also works)
- `column__in=a,b,c` — match any of the listed values
- `column__contains=text` — substring match
- `column__gt=`, `column__lt=` — comparison operators

Column selection:
- `_col=name` — include only specific columns (repeat for multiple)
- `_nocol=name` — exclude specific columns

The `config` column contains a JSON string. Parse it to extract:
- `dimensions[].variableId` — the variable IDs used in the chart
- `title`, `subtitle`, `note` — text shown on the chart
- `tab` — default tab (`map` or `chart`)
- `hasMapTab` — whether the map tab is available
- `yAxis` — axis configuration (min/max)
- `selectedEntityNames` — default entity selection
- `type` — chart type (null means LineChart)

Example Python snippet to extract chart details:

```python
import json, urllib.request

url = (
    "http://analytics/private/charts.json"
    "?slug__in=death-rate-from-air-pollution-per-100000,share-deaths-air-pollution"
    "&_shape=array&_col=id&_col=slug&_col=config"
)
charts = json.loads(urllib.request.urlopen(url).read())
for c in charts:
    config = json.loads(c["config"])
    print(c["slug"], [d["variableId"] for d in config["dimensions"]])
```

### Looking up variable catalog paths

Once you have variable IDs, query the `variables` table to get their `catalogPath`:

```bash
curl -s "http://analytics/private/variables.json?\
id__in=1173270,1172183\
&_shape=array\
&_col=id&_col=name&_col=catalogPath&_col=unit&_col=shortUnit"
```

The `catalogPath` tells you:
- Which ETL dataset produces the variable (e.g. `grapher/ihme_gbd/2026-02-07/gbd_risk_deaths/gbd_risk_deaths`)
- The exact flattened column name after the `#` (e.g. `value__metric_rate__measure_deaths__rei_air_pollution__age_age_standardized__cause_all_causes`)

### Other useful datasette queries

```bash
# Search for charts by title substring
curl -s "http://analytics/private/charts.json?title__contains=air+pollution&_shape=array&_col=slug&_col=title"

# List all tables in the private database
curl -s "http://analytics/private.json"

# Get variable metadata including dataset info
curl -s "http://analytics/private/variables.json?id=1173270&_shape=array"

# Paginate large result sets
curl -s "http://analytics/private/charts.json?_size=100&_next=TOKEN"
```

---

## Step 2: Create the Export Step

You need three things: a YAML config, a Python step, and a DAG entry.

### Directory structure

```
etl/steps/export/multidim/<namespace>/latest/
├── <short_name>.config.yml
└── <short_name>.py
```

### YAML config file

The config defines the MDIM title, default entity selection, dimension dropdowns, and
views. Each view maps a dimension choice to specific indicator catalog paths and chart
configuration.

```yaml
title:
  title: Deaths from air pollution
  title_variant: death rate and share of deaths
default_selection:
  - World
  - China
  - India

dimensions:
  - slug: metric
    name: Metric
    choices:
      - slug: rate
        name: Death rate
        description: Age-standardized death rate per 100,000 people
      - slug: share
        name: Share of deaths
        description: Share of total deaths attributed to air pollution

views:
  - dimensions:
      metric: rate
    indicators:
      y:
        - grapher/ihme_gbd/2026-02-07/gbd_risk_deaths/gbd_risk_deaths#value__metric_rate__measure_deaths__rei_air_pollution__age_age_standardized__cause_all_causes
    config:
      title: Death rate from air pollution
      subtitle: Estimated annual number of deaths attributed to air pollution per 100,000 people.
      tab: map
      hasMapTab: true
      yAxis:
        min: 0
        max: 0
      selectedEntityNames:
        - High-middle SDI
        - World

  - dimensions:
      metric: share
    indicators:
      y:
        - grapher/ihme_gbd/2026-02-07/gbd_risk_deaths/gbd_risk_deaths#value__metric_percent__measure_deaths__rei_air_pollution__age_age_standardized__cause_all_causes
    config:
      title: Share of deaths attributed to air pollution
      subtitle: "Share of deaths from any cause attributed to air pollution."
      tab: map
      hasMapTab: true
      yAxis:
        min: 0
        max: 20
      selectedEntityNames:
        - World
```

The indicator catalog paths come from the `catalogPath` field in the variables table.
The view `config` fields are copied from the original chart configs.

### Python step file

Two approaches:

**A) Fully YAML-driven (simplest — no dataset loading needed):**

When views in the YAML already have full catalog paths, `create_collection` doesn't
need a table:

```python
from etl.helpers import PathFinder

paths = PathFinder(__file__)

def run() -> None:
    config = paths.load_collection_config()
    c = paths.create_collection(config=config)
    c.save()
```

**B) Table-driven (auto-expands dimensions):**

When the source dataset has dimensional structure, you can load the table metadata and
let `create_collection` auto-generate views. The YAML config then only needs to define
dimension names/descriptions and any view-level overrides:

```python
from etl.helpers import PathFinder

paths = PathFinder(__file__)

def run() -> None:
    config = paths.load_collection_config()
    ds = paths.load_dataset("gbd_risk_deaths")
    tb = ds.read("gbd_risk_deaths", load_data=False)  # metadata only

    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["value"],
        dimensions={
            "metric": ["Rate", "Percent"],
            "measure": ["Deaths"],
            "rei": ["Air pollution"],
            "age": ["Age-standardized"],
            "cause": ["All causes"],
        },
    )
    c.save()
```

The `dimensions` dict filters which dimension values to include. Use `"*"` to include
all choices for a dimension. Auto-generated views are merged with any views defined in
the YAML (YAML takes precedence for matching dimension combinations).

### DAG entry

Add the dependency in the appropriate `dag/<topic>.yml` file:

```yaml
export://multidim/<namespace>/latest/<short_name>:
  - data-private://grapher/<namespace>/<version>/<dataset>
```

For private datasets, use `data-private://`. The export step depends on the grapher-level
dataset so the full pipeline runs when needed.

---

## Step 3: Run and Preview

```bash
# Dry run to verify DAG resolution
uv run etlr multidim/<namespace>/latest/<short_name> --export --private --dry-run

# Run the full pipeline (builds all dependencies)
uv run etlr multidim/<namespace>/latest/<short_name> --export --private

# Run only the export step (if grapher data already built)
uv run etlr multidim/<namespace>/latest/<short_name> --export --private --only
```

Preview the result at: `http://staging-site-<branch>/admin/multi-dims`

---

## Mapping Chart Config Fields to View Config

| Original chart field | View config field | Notes |
|---------------------|-------------------|-------|
| `title` | `title` | Chart title |
| `subtitle` | `subtitle` | Shown below title |
| `note` | `note` | Footnote text |
| `tab` | `tab` | `"map"` or `"chart"` |
| `hasMapTab` | `hasMapTab` | Boolean |
| `type` | `chartTypes` | In views, use `chartTypes: ["LineChart"]` |
| `yAxis.min/max` | `yAxis.min/max` | Axis bounds |
| `selectedEntityNames` | `selectedEntityNames` | Default entities |
| `map.colorScale.*` | `map.colorScale.*` | Map color config |

---

## Tips

- Use `load_data=False` when reading tables to only load metadata — much faster.
- When both charts use variables from the **same dataset**, a single DAG dependency suffices.
- When combining charts from **different datasets**, add multiple DAG dependencies and
  either use multiple `tb` inputs or specify full catalog paths in the YAML views.
- The `common_views` YAML field lets you set shared config across views at different
  granularity levels (see `docs/guides/data-work/mdims.md` for details).
- Check existing examples in `etl/steps/export/multidim/` — simpler ones include
  `owid/latest/ig_countries` and `lis/latest/gini_lis`.
