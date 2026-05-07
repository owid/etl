---
name: create-explorer
description: Author or modify an Our World in Data explorer (multi-dimensional dashboard with dropdown selectors, published from ETL via `export://explorers/<ns>/latest/<short>`). Trigger when the user wants to build a new explorer, add/remove views or dimensions on an existing one, change the explorer's chart text or selection defaults, or finish an explorer migration once the snapshot/garden/grapher chain is already in place.
metadata:
  internal: true
---

# Creating an Explorer

Explorers are OWID's multi-dimensional dashboards (e.g. `ourworldindata.org/explorers/food-prices`). They're authored as YAML in this repo and published by ETL at `export://explorers/<ns>/latest/<short>`.

This skill is the explorer-flavored sibling of `/create-multidim`. They use the same `paths.create_collection(...)` engine and the same YAML schema for `dimensions` / `views` / `definitions.common_views`. The differences are:

| | Multidim | Explorer |
|---|---|---|
| Channel | `export://multidim/...` | `export://explorers/...` |
| Step file location | `etl/steps/export/multidim/<ns>/latest/` | `etl/steps/export/explorers/<ns>/latest/` |
| `create_collection` flag | (default) | `explorer=True` |
| Top-level config block | `title:`, `default_selection:`, `default_dimensions:` | `config:` block carrying legacy explorer settings (`explorerTitle`, `explorerSubtitle`, `selection`, `subNavId`, `entityType`, …) |
| Slug convention | underscores in file paths and `short_name` | underscores in file path, **hyphens** in URL slug and `short_name` argument |
| Verification | preview URL on staging | preview URL on staging + diff against `owid-grapher/explorers/<slug>.explorer.tsv` |
| Save call | `c.save()` | `c.save(tolerate_extra_indicators=True)` (upstream grapher datasets usually have more indicators than the explorer references) |

If you're modifying an existing explorer (adjusting chart text, swapping a catalogPath, adding a dimension choice, reordering views), most of the deeper sections below don't apply — find the existing `<short>.config.yml`, edit, run `etlr`, done. The full structure is documented below for new explorers and substantial reshapes.

## When to use this skill

- After a `/migrate-explorer-csv`, `/migrate-explorer-grapher`, or `/migrate-explorer-indicator-legacy` skill has produced (or already located) the upstream snapshot/meadow/garden/grapher chain, and now needs the export step.
- For a brand-new explorer where the data is already in ETL (skip directly to step 1).
- When porting an existing explorer's view layout (e.g. full-YAML → table-driven, or moving FAUST text from per-view YAML up into indicator metadata).

## Step 1 — Files & directories

Every explorer is exactly two files plus a DAG entry:

```
etl/steps/export/explorers/<ns>/latest/
├── <short>.py              # Python uses snake_case
└── <short>.config.yml
```

```bash
mkdir -p etl/steps/export/explorers/<ns>/latest
```

**Hyphens vs underscores** (recurring source of confusion):

- The Python file path uses **underscores**: `food_footprints.py`, `crop_yields.py`.
- The explorer slug used in the URL and the `short_name=` argument keeps **hyphens**: `food-footprints`, `crop-yields`.
- `paths.create_collection(short_name="<short-with-hyphens>")` — pass the hyphenated slug.

## Step 2 — Pick a construction style

The two ends of the spectrum, plus everything in between:

| | Full-YAML | Programmatic / table-driven |
|---|---|---|
| Where views live | Hand-listed in `<short>.config.yml` under `views:` | Auto-expanded by `paths.create_collection(tb=tb, ...)` from columns whose `m.dimensions` is set |
| Where chart text (FAUST) lives | Per-view `view.config.{title, subtitle, note}` in YAML | `presentation.{title_public, grapher_config}` on each indicator's garden metadata; common defaults via `definitions.common.presentation.grapher_config` |
| Where chart-level config lives (`hasMapTab`, `tab`, `yAxis`, `chartTypes`) | Per-view `view.config` | Indicator's `presentation.grapher_config` — single source of truth, same as for any standalone chart on that indicator |
| Map color scale | `view.indicators.y[i].display.{colorScaleScheme, colorScaleNumericBins}` (semicolon-string form, explorer-flavored override) | `presentation.grapher_config.map.colorScale.{baseColorScheme, binningStrategy, customNumericValues}` (canonical grapher form, inherited at chart render time) |
| Python step content | Trivial: `paths.create_collection(config=config, short_name=..., explorer=True).save(...)` | Loops columns to set `m.dimensions`, optionally post-processes (`sort_choices`, `group_views`, per-view `display` tweaks) |

It's a spectrum, not a switch. Mix freely: use table-driven for the bulk of views, hand-list a handful of bespoke ones; or stay full-YAML but still push title/subtitle for the single-indicator views into garden metadata to remove duplication.

**Strong fit for table-driven:**

- **Single-indicator views dominate.** Each view is a thin wrapper around one indicator → that indicator's metadata is the right home for chart text. Avoids duplication between explorer YAML and the equivalent standalone chart, and keeps both in sync forever.
- **Many views (>20)** following the cartesian product of a few dimensions. Hand-listing them is repetitive; auto-expansion plus YAML dimensions is significantly less code.
- **Upstream is a dimensional table** (one row per country/year × dim_a × dim_b × …) with one indicator. `create_collection(tb=tb, indicator_names=..., dimensions=...)` matches this shape directly — model: `migration/latest/migration_flows.py`.
- **Same indicators back standalone grapher charts.** Pushing FAUST upstream means explorer view and standalone chart inherit the same text — no drift over time.

**Stick with full-YAML when:**

- **Few views (<10)**, all bespoke (different chart types / data sources / hand-tuned text).
- **Multi-indicator views dominate.** FAUST cannot live on any single indicator when a view shows multiple indicators — you have to write it explicitly per view in the explorer YAML (or build views via `c.group_views(...)`).
- **Single-shot migration with no plan to maintain.** The duplication of full-YAML doesn't matter if no one will edit it again.

## Step 3 — The Python step

### Full-YAML variant

```python
"""<one-line description of what this explorer surfaces>."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    config = paths.load_collection_config()
    c = paths.create_collection(
        config=config,
        short_name="<short-with-hyphens>",  # explorer slug
        explorer=True,
    )
    c.save(tolerate_extra_indicators=True)
```

`tolerate_extra_indicators=True` is the common case: the upstream grapher dataset usually carries more indicators than the explorer references, and without this flag `c.save()` errors on the unused ones.

### Table-driven variant

```python
"""<one-line description>."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map column → dimension tuple. "na" is the conventional empty slot for conditional dimensions
# (e.g. cost_metric is meaningful only when type=cost; affordability views set cost_metric="na").
COLUMN_DIMENSIONS: dict[str, dict[str, str]] = {
    "<col_a>": {"dim1": "value_a1", "dim2": "value_a2"},
    "<col_b>": {"dim1": "value_b1", "dim2": "value_b2"},
    # ...
}


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("<grapher_dataset>")
    tb = ds.read("<table>", load_data=False)  # metadata only — faster, we don't need values

    for column, dims in COLUMN_DIMENSIONS.items():
        tb[column].m.dimensions = dims
        tb[column].m.original_short_name = "<unifying_indicator_name>"

    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["<unifying_indicator_name>"],
        dimensions={
            "dim1": ["value_a1", "value_b1", ...],   # explicit choice order
            "dim2": ["value_a2", "value_b2", ...],
        },
        # common_view_config={...},                   # only if not in indicator metadata
        short_name="<short-with-hyphens>",
        explorer=True,
    )

    # Optional post-processing — see "Post-processing" below.
    # c.sort_choices({"dim1": lambda x: sorted(x)})
    # c.group_views([...])

    c.save(tolerate_extra_indicators=True)
```

Key APIs (see `etl/collection/core/expand.py` and `etl/collection/core/create.py`):

- `tb[col].m.dimensions: dict[str, str]` — required per column. Each entry says "this column represents the (dim1=value, dim2=value) cell." Columns without `m.dimensions` are ignored by the expander.
- `tb[col].m.original_short_name: str` — the unifying indicator name. With `indicator_names=[that_name]` and a single name, the expander treats all N columns as one logical indicator with N dimension combinations and drops the auto-added "indicator" pseudo-dimension.
- `dimensions=` accepts:
  - `None` → all dimensions found, arbitrary order.
  - `list[str]` → restricts and orders dimensions, all values shown.
  - `dict[str, list[str] | "*"]` → restricts and orders both dimensions and choices. Use `"*"` for "all values, arbitrary order."
- `common_view_config=` is applied uniformly to every auto-expanded view. Use it for fields that are truly shared and don't live at indicator level. **Prefer indicator-level `presentation.grapher_config`** for anything that should also flow to standalone charts.

## Step 4 — The config YAML

> **Always block style.** Mappings and lists in explorer config YAML must use block style — one key per line, list items on their own line under `-`. Never use flow style (`{ key: value, ... }` or `[a, b, c]`) even for tiny per-view `dimensions:` blocks. PR review on a 45-view file is unreadable when half the views collapse to a single flow line. The only exception is markdown links inside a quoted-scalar `subtitle:`/`note:` (those `[text](url)` brackets are content, not YAML structure).

```yaml
config:
  # Explorer settings rows — keys map verbatim from the legacy TSV settings section.
  explorerTitle: ...
  explorerSubtitle: ...
  isPublished: true
  hasMapTab: false
  hideAlertBanner: true
  hideAnnotationFieldsInTitle: true
  entityType: country         # or "food", "region", etc.
  thumbnail: https://assets.ourworldindata.org/uploads/...
  wpBlockId: "12345"
  subNavId: explorers
  subNavCurrentId: <slug>
  selection:
    - <default selected entity>
    - <another>
  pickerColumnSlugs: []        # an empty list is OK; non-empty must be block-style
  yAxisMin: 0
  # ...

definitions:
  # Shared config applied to all views. Use this list (with optional `dimensions:`
  # filter per entry) — NOT YAML anchors and `<<:` merge keys. The framework merges
  # entries at expansion time; per-view `config:` blocks override anything here.
  common_views:
    - config:
        type: DiscreteBar
        hasMapTab: false
    # Dimension-filtered overrides apply only to matching views:
    # - dimensions:
    #     metric: share
    #   config:
    #     note: "Share values sum to 100%"

dimensions:
  # one entry per dropdown / radio / checkbox the user toggles
  - slug: <snake_case>          # e.g. "metric"
    name: <human label>          # e.g. "Metric"
    presentation:
      type: dropdown             # or radio / checkbox
    choices:
      - slug: <choice_snake>
        name: "<as shown in widget>"
      - slug: <another>
        name: "..."

views:
  # one entry per (dim1=x, dim2=y, …) tuple
  - dimensions:
      <dim_slug>: <choice_slug>
      # ...
    indicators:
      y:
        - catalogPath: <table>#<short>      # short form — see "catalogPath — short forms accepted" below
          display:                          # per-view, per-indicator overrides
            colorScaleNumericBins: 0;1;2
            colorScaleScheme: PuBu
    config:
      # Only per-view overrides here. Common stuff lives in definitions.common_views.
      # No `<<:` merge keys, no `&anchor`s.
      title: ...
      subtitle: ...
      type: <chart type>         # LineChart, DiscreteBar, "LineChart DiscreteBar", StackedArea, …
      hasMapTab: false
      minTime: 1990
      yAxisMin: 0
```

For table-driven explorers, `views:` should still be present but is typically `views: []` — the explorer JSON schema requires the key, and `create_collection(tb=tb, ...)` populates the views at runtime.

#### `catalogPath` — short forms accepted

The `Indicator.is_a_valid_path` check (`etl/collection/model/view.py:62`) accepts three forms; pick the shortest one that still unambiguously resolves:

| Form | Example | When to use |
|---|---|---|
| `table#indicator` | `global_carbon_budget#emissions_total` | Default. Resolved against the explorer's DAG dependencies via `tables_by_name` — fine as long as no two dependencies expose a table with the same name. |
| `dataset/table#indicator` | `global_carbon_budget/global_carbon_budget#emissions_total` | When two upstream datasets happen to expose tables with the same `short_name`. |
| `grapher/<ns>/<v>/<dataset>/<table>#<indicator>` | `grapher/gcp/2025-11-13/global_carbon_budget/global_carbon_budget#emissions_total` | Only when you need to pin a specific dataset version *separate from the one in the DAG* — almost never the right form to write by hand. |

Short forms are expanded at `c.save()` time by `Indicator.expand_path(tables_by_name)`. If the table name doesn't exist in any dependency it raises `Table name '<x>' not found in dependency tables`; if multiple dependencies expose the same table name, it raises and asks you to disambiguate with the medium form.

**Default to `table#indicator`** when authoring YAML. The full path is verbose, drifts when upstream versions bump, and is only needed for genuinely ambiguous cases.

### Top-level `config:` settings — the most common keys

| Key | Type | Notes |
|---|---|---|
| `explorerTitle` | string | Page title above the explorer. |
| `explorerSubtitle` | string | One-liner under the title. |
| `isPublished` | bool | `true` to publish; `false` keeps it draft. |
| `hasMapTab` | bool | Whether any view shows the map tab by default. |
| `entityType` | string | `country` (default), or `food`, `region`, `species`, etc. — controls picker labels. |
| `selection` | list[str] | Default selected entities. |
| `pickerColumnSlugs` | list[str] | Picker columns shown alongside the entity name. |
| `subNavId` | string | Almost always `explorers`. |
| `subNavCurrentId` | string | The slug — appears as the active nav item. |
| `wpBlockId` | string | WordPress block ID for embedding (legacy). Stringify even when numeric. |
| `thumbnail` | string | URL of preview image. |
| `hideAlertBanner` | bool | Suppress the OWID-wide banner. |
| `hideAnnotationFieldsInTitle` | bool | Drop time/entity from auto-titles. |
| `yAxisMin` | number/string | Default Y-axis floor. |
| `yScaleToggle` | bool | Allow user to toggle linear/log. |
| `originUrl` | string | Path back to the topic page (e.g. `/environmental-impacts-of-food`). |

### Dimension presentation types

- `dropdown`: shown as `<select>`. Use for >4 choices or when the choices have long labels.
- `radio`: shown as a row of pills. Use for ≤4 mutually-exclusive choices.
- `checkbox`: shown as a single toggle. Two choices only — usually "off" (slug like `combined`/`absolute`/`no`) and "on" (slug like the field name). Pair with `presentation.choice_slug_true: <on_slug>` so the framework knows which slug means "checked."

```yaml
- slug: by_stage
  name: By stage of supply chain
  presentation:
    type: checkbox
    choice_slug_true: stages
  choices:
    - slug: combined
      name: ""
    - slug: stages
      name: By stage of supply chain
```

### Conditional dimensions (the "na" pattern)

When a dimension is only meaningful for some rows (e.g. `cost_metric` matters only when `type=cost`, not when `type=affordability`), include the dimension everywhere with an `"na"` slot:

- Tag the columns/views that don't use it with `<dim>: "na"`.
- Declare the `na` choice with `name: ""` so the widget renders as empty when applicable.
- Model: `agriculture/latest/food_prices.{py,config.yml}`.

```yaml
- slug: cost_metric
  name: Cost metric
  presentation:
    type: radio
  choices:
    - slug: na
      name: ""
    - slug: dollars_per_day
      name: $ per day
```

## Step 5 — Push FAUST upstream (recommended for single-indicator views)

For single-indicator views, the rendered chart inherits the indicator's stored `grapher_config` from MySQL at render time (both standalone-chart and explorer-view paths). Push:

| Per-view config | → indicator garden metadata |
|---|---|
| `title` | `presentation.grapher_config.title` |
| `subtitle` | `presentation.grapher_config.subtitle` |
| `note` | `presentation.grapher_config.note` |
| `map.colorScale` | `presentation.grapher_config.map.colorScale.{baseColorScheme, binningStrategy, customNumericValues}` |
| `hasMapTab`, `tab`, `yAxis`, `chartTypes`, `hideRelativeToggle`, `selectedFacetStrategy`, … | `presentation.grapher_config.<field>` |

For the chart-heading flow specifically, the priority is `grapher_config.title > title_public > display.name > title` (see `docs/architecture/metadata/faqs.md`). When migrating a chart-wrapping explorer, the chart's bespoke heading text belongs in `grapher_config.title` — `title_public` is the *human-readable replacement for a dimensional `indicator.title`*, not the chart's heading.

Cross-cutting baselines (e.g. `hasMapTab: true` for every indicator in a dataset) go under `definitions.common.presentation.grapher_config` in the **garden** `.meta.yml`. The catalog merge is recursive on `presentation` and `grapher_config` (`lib/catalog/owid/catalog/core/yaml_metadata.py:_merge_variable_metadata`), so each indicator inherits the common defaults plus its own overrides without manual `<<: *anchor` repetition.

DRY for repeated text fragments via dynamic-yaml interpolation:

```yaml
definitions:
  prefix: "Long shared phrase about diet X."
  suffix: "Common closing sentence about methodology."

# In the indicator block:
subtitle: "{definitions.prefix} {definitions.suffix}"
```

This composes N unique full strings from a handful of building blocks. Verified via `dynamic_yaml_to_dict` (`lib/catalog/owid/catalog/core/utils.py`).

## Step 6 — Post-processing the collection (table-driven only)

After `paths.create_collection()` returns the collection `c`, you can mutate it before `c.save()`:

- **`c.sort_choices({dim_slug: lambda x: sorted(x)})`** — control the order of dimension dropdowns. Useful when slugs sort poorly alphabetically.

- **`c.group_views(groups=[...])`** — bundle multiple existing views into a new multi-indicator view. Each entry in `groups`:
  - `dimension`: the dimension whose choices are being collapsed.
  - `choices`: the choice slugs to combine (omit for all).
  - `choice_new_slug`: name for the new collapsed choice (e.g. `combined`, `total`, `breakdown`).
  - `view_config`: chart-level config for the new view (`chartTypes`, `title`, `subtitle`, `selectedFacetStrategy`, …). Title can be a template like `"Population aged {age}"` evaluated against `params`.
  - `view_metadata`: data-page metadata (`description_key`, etc.) for the new view.
  - `replace=True` to drop the originals; default keeps both.
  - `overwrite_dimension_choice=True` if `choice_new_slug` collides with an existing choice and you want grouped views to win.
  - **Use cases**: an explorer with `sex={female, male}` views — `group_views` adds `sex=combined` showing both timeseries on one chart. Same pattern works for age brackets, region groups, conflict types, or any dimension where users may want a single multi-line chart.

- **Manual loop over `c.views`** — set per-view `display` settings when those can't live in indicator metadata (e.g. when the same indicator is shown with different color scales in different views). Pattern: `migration_flows.py`'s `add_display_settings(c)`. Avoid this if the same settings can flow from indicator metadata instead.

- **`choice_renames={dim: {slug: display_name, ...}}`** (passed directly to `create_collection`) — map slug → display name when you need to derive the display label programmatically. Model: `multidim/minerals/latest/minerals.py`.

## Step 7 — DAG entry

In `dag/<ns>.yml`:

```yaml
export://explorers/<ns>/latest/<short>:
  - data://grapher/<ns1>/<v1>/<dataset1>
  - data://grapher/<ns2>/<v2>/<dataset2>
  # ... one line per unique upstream grapher dataset
```

Place near related explorer entries (or alongside the upstream grapher steps) for discoverability.

## Step 8 — Verify

Hand off to the user:

1. `.venv/bin/etlr export://explorers/<ns>/latest/<short>` — runs the step and writes the TSV.
2. Open `http://staging-site-<branch>/admin/explorers/preview/<slug>` and spot-check:
   - default view (no dimensions toggled)
   - every dimension switch
   - map tab (if applicable)
   - country/entity picker
   - default selection
3. **For migrations:** diff the resulting TSV against the legacy `owid-grapher/explorers/<slug>.explorer.tsv`. Cosmetic differences (column ordering, whitespace) are acceptable; structural differences (missing views, swapped dimension orderings) are not. The Wizard's `apps/wizard/app_pages/explorer_diff/` page does this comparison interactively for staging vs production.
4. `make check`.

Hand the user the exact `etlr` command — don't run it yourself.

## Common pitfalls

- **`build_views` does not propagate per-view `display` from indicator metadata** — see TODO at `etl/collection/core/expand.py:313`. Each auto-expanded view gets `indicators.y[0]` with only `catalogPath`, no `display`. If you need different color scales per view, either (a) put them in the indicator's `presentation.grapher_config.map.colorScale` so they apply at chart render time, or (b) post-process `c.views` in Python.
- **`type: LineChart` is not a valid `grapher_config` field** when authoring via indicator metadata — use `chartTypes: ["LineChart"]` (the schema is an array). Per-view `config.type` in the explorer YAML still accepts strings.
- **`tb.read(..., load_data=False)`** is essential when you only need column metadata to set dimensions; loading data unnecessarily slows the step.
- **YAML schema requires `views:` key** in the explorer config even when empty. Pass `views: []`.
- **Indicator must be re-published to MySQL with the new `grapher_config`** before the explorer view can inherit it. Re-run `etlr --grapher data://grapher/...` after editing garden metadata; the explorer step alone won't refresh the indicator's stored config.
- **YAML anchors / merge keys** (`&common_view`, `<<: *common_view`) — don't use them. They can't filter by dimension and add per-view noise. Use `definitions.common_views` instead.
- **Hyphens in `short_name`** — file is `food_footprints.py` but `short_name="food-footprints"`. Mismatch produces a published explorer whose URL doesn't match the legacy slug.
- **`tolerate_extra_indicators`** — usually want `True` for explorers since you're cherry-picking indicators from larger upstream datasets.

## Reference examples

Full-YAML (each view hand-listed):

- `etl/steps/export/explorers/agriculture/latest/crop_yields.{py,config.yml}` — large indicator-based explorer with many dimensions.
- `etl/steps/export/explorers/agriculture/latest/food_prices.{py,config.yml}` — small grapher-chart-based migration (12 chart IDs unwrapped to 12 single-indicator views) with conditional dimensions ("na" pattern).
- `etl/steps/export/explorers/agriculture/latest/fertilizers.{py,config.yml}` — checkbox dimension with `choice_slug_true`; multi-namespace dependencies.
- `etl/steps/export/explorers/food/latest/food_footprints.{py,config.yml}` — hybrid (16 grapher-chart views + 29 CSV-backed views) showing dimension-filtered `common_views` for differing `sourceDesc` per view-type.
- `etl/steps/export/explorers/war/latest/countries_in_conflict_data.{py,config.yml}` — uses `na`-named choices to model conditional dimensions.
- `etl/steps/export/explorers/emissions/latest/ipcc_scenarios.{py,config.yml}` — moderate-size YAML-driven.

Table-driven (views auto-expanded from a dimensional table):

- `etl/steps/export/explorers/migration/latest/migration_flows.{py,config.yml}` — passes `tb=tb, indicator_names=[...], dimensions=[...]` to `create_collection`; YAML carries only the static config and dimension presentation. Includes `add_display_settings(c)` post-processing.
- `etl/steps/export/multidim/minerals/latest/minerals.py` — same APIs in the multidim channel; useful read for `choice_renames`.

## Follow-up

Once an explorer is on `create_collection(explorer=True)`, it's a candidate for the Track-B port to MDIM (`export://multidim/...`) once feature parity is reached. See umbrella issue #6014.
