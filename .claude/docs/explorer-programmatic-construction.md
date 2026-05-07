# Programmatic / table-driven explorer construction

When migrating an existing explorer or building a new one, you choose how much of the explorer is YAML vs Python. The right choice depends on the explorer's shape (single-indicator vs multi-indicator views, count of views, dimensional fit) and on whether the same indicators are reused by standalone grapher charts. This document is the shared reference for that decision; the migration skills (`migrate-explorer-grapher`, `migrate-explorer-csv`, `migrate-explorer-indicator-legacy`) link here instead of duplicating the mechanics.

## The two ends of the spectrum

| | Full-YAML | Programmatic / table-driven |
|---|---|---|
| Where views live | Hand-listed in `<short>.config.yml` under `views:` | Auto-expanded by `paths.create_collection(tb=tb, ...)` from columns whose `m.dimensions` is set |
| Where chart text (FAUST) lives | Per-view `view.config.{title, subtitle, note}` in YAML | `presentation.{title_public, grapher_config}` on each indicator's garden metadata; common defaults via `definitions.common.presentation.grapher_config` |
| Where chart-level config lives (`hasMapTab`, `tab`, `yAxis`, `chartTypes`) | Per-view `view.config` (or hoisted via YAML anchors) | Indicator's `presentation.grapher_config` — single source of truth, same as for any standalone chart on that indicator |
| Map color scale | `view.indicators.y[i].display.{colorScaleScheme, colorScaleNumericBins}` (semicolon-string form, explorer-flavored override) | `presentation.grapher_config.map.colorScale.{baseColorScheme, binningStrategy, customNumericValues}` (canonical grapher form, inherited at chart render time) |
| Python step content | Trivial: `paths.create_collection(config=config, short_name=..., explorer=True).save(...)` | Loops columns to set `m.dimensions`, optionally post-processes (`sort_choices`, `group_views`, per-view `display` tweaks) |

It's a spectrum, not a switch. You can mix: use table-driven expansion for the bulk of views, then add a handful of bespoke views in YAML; or use full-YAML but still push title/subtitle for the single-indicator views into garden metadata to remove duplication.

## When to go programmatic

Strong fit:
- **Single-indicator views dominate.** Each view is a thin wrapper around one indicator → that indicator's metadata is the right home for chart text. Avoids duplication between explorer YAML and the equivalent standalone chart, and keeps both in sync forever.
- **Many views (>20)** following the cartesian product of a few dimensions. Hand-listing them is repetitive; auto-expansion plus YAML dimensions is significantly less code.
- **Upstream is a dimensional table** (one row per country/year × dim_a × dim_b × …) with one indicator. `create_collection(tb=tb, indicator_names=..., dimensions=...)` matches this shape directly — model: `migration/latest/migration_flows.py`.
- **Same indicators back standalone grapher charts.** Pushing FAUST upstream means explorer view and standalone chart inherit the same text — no drift over time.

Weaker fit (full-YAML is fine):
- **Few views (<10)**, all bespoke (different chart types / data sources / hand-tuned text).
- **Multi-indicator views dominate** the explorer. FAUST cannot live on any single indicator when a view shows multiple indicators — you have to write it explicitly per view in the explorer YAML (or build views via `group_views`, see below).
- **Single-shot migration with no plan to maintain.** The duplication of full-YAML doesn't matter if no one will edit it again.

## Anatomy of a table-driven explorer step

```python
"""<one-line description of what this explorer surfaces>."""
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
        # common_view_config={...},                  # only if not in indicator metadata
        short_name="<short-with-hyphens>",
        explorer=True,
    )

    # Optional post-processing — see "Post-processing the collection" below.
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

## YAML config shape (table-driven)

The YAML is a thin shell:

```yaml
config:
  explorerTitle: ...
  explorerSubtitle: ...
  isPublished: true
  selection: [...]
  subNavId: explorers
  subNavCurrentId: <slug>

dimensions:
  - slug: dim1
    name: <human label>
    presentation: { type: dropdown }   # or radio / checkbox
    choices:
      - { slug: value_a1, name: <label> }
      - { slug: value_b1, name: <label> }

views: []   # explorer schema requires the key; views are auto-expanded by create_collection
```

The explorer JSON schema requires `views:` to be present. Pass `views: []` when going table-driven.

## FAUST migrated up to indicator metadata

For single-indicator views, the rendered chart inherits the indicator's stored `grapher_config` from MySQL at render time (both standalone-chart and explorer-view paths). You can therefore push:

- `title` → `presentation.title_public`
- `subtitle` → `presentation.grapher_config.subtitle`
- `note` → `presentation.grapher_config.note`
- `map.colorScale` → `presentation.grapher_config.map.colorScale.{baseColorScheme, binningStrategy, customNumericValues}`
- `hasMapTab`, `tab`, `yAxis`, `chartTypes`, `hideRelativeToggle`, `selectedFacetStrategy`, … → `presentation.grapher_config.<field>`

Cross-cutting baselines (e.g. `hasMapTab: true` for every indicator in a dataset) go under `definitions.common.presentation.grapher_config` in the garden `.meta.yml`. The catalog merge is recursive on `presentation` and `grapher_config` (`lib/catalog/owid/catalog/core/yaml_metadata.py:_merge_variable_metadata`), so each indicator inherits the common defaults plus its own overrides without manual `<<: *anchor` repetition.

DRY for repeated text fragments: dynamic-yaml interpolation supports `"{definitions.<key>}"` inline, including string concatenation:

```yaml
definitions:
  prefix: "Long shared phrase about diet X."
  suffix: "Common closing sentence about methodology."

# In the indicator block:
subtitle: "{definitions.prefix} {definitions.suffix}"
```

This composes N unique full strings from a handful of building blocks. Verified via `dynamic_yaml_to_dict` (`lib/catalog/owid/catalog/core/utils.py`).

## Post-processing the collection

After `paths.create_collection()` returns the collection `c`, you can mutate it before `c.save()`:

- **`c.sort_choices({dim_slug: lambda x: sorted(x)})`** — control the order of dimension dropdowns. Useful when slugs sort poorly alphabetically, or you want a non-trivial ordering (e.g. by a separate sort key).

- **`c.group_views(groups=[...])`** — bundle multiple existing views into a new multi-indicator view. This is the primary way to introduce multi-line / multi-indicator views into a table-driven explorer without dropping back to full-YAML. Each entry in `groups`:
  - `dimension`: the dimension whose choices are being collapsed.
  - `choices`: the choice slugs to combine (omit for all).
  - `choice_new_slug`: name for the new collapsed choice (e.g. `combined`, `total`, `breakdown`).
  - `view_config`: chart-level config for the new view (`chartTypes`, `title`, `subtitle`, `selectedFacetStrategy`, …). Title can be a template like `"Population aged {age}"` evaluated against `params`.
  - `view_metadata`: data-page metadata (`description_key`, etc.) for the new view.
  - `replace=True` to drop the originals and keep only the grouped view; default keeps both.
  - `overwrite_dimension_choice=True` if `choice_new_slug` collides with an existing choice and you want grouped views to win.
  - **Use cases**: an explorer with `sex={female, male}` views — `group_views` adds `sex=combined` showing both timeseries on one chart. Same pattern works for age brackets, region groups, conflict types (see `countries_in_conflict_data` for the YAML-only equivalent), or any dimension where users may want a single multi-line chart.

- **Manual loop over `c.views`** — set per-view `display` settings when those can't live in indicator metadata (e.g. when the same indicator is shown with different color scales in different views). Pattern: `migration_flows.py`'s `add_display_settings(c)`. Avoid this if the same settings can flow from indicator metadata instead.

- **`choice_renames={dim: {slug: display_name, ...}}`** (passed directly to `create_collection`) — map slug → display name when you need to derive the display label programmatically. Model: `multidim/minerals/latest/minerals.py`.

## Common pitfalls

- **`build_views` does not propagate per-view `display` from indicator metadata** — see TODO at `etl/collection/core/expand.py:313`. Each auto-expanded view gets `indicators.y[0]` with only `catalogPath`, no `display`. If you need different color scales per view, either (a) put them in the indicator's `presentation.grapher_config.map.colorScale` so they apply at chart render time, or (b) post-process `c.views` in Python.
- **`type: LineChart` is not a valid `grapher_config` field** — use `chartTypes: ["LineChart"]` (the schema is an array). The default is `["LineChart", "DiscreteBar"]`.
- **`tb.read(..., load_data=False)`** is essential when you only need column metadata to set dimensions; loading data unnecessarily slows the step.
- **YAML schema requires `views:` key** in the explorer config even when empty. Pass `views: []`.
- **Indicator must be re-published to MySQL with the new `grapher_config`** before the explorer view can inherit it. Re-run `etlr --grapher data://grapher/...` after editing garden metadata; the explorer step alone won't refresh the indicator's stored config.
- **Conditional dimensions** (e.g. `cost_metric` only meaningful when `type=cost`): include the conditional dimension in `dimensions=` with an `"na"` slot, tag the columns that don't have it with `"<dim>": "na"`, and declare the `na` choice with `name: ""` in the explorer YAML so the widget collapses gracefully. Model: `agriculture/latest/food_prices.{py,config.yml}`.

## Reference examples

Single-namespace dimensional explorer (clean fit, table-driven via `create_collection(tb=tb, ...)`): `etl/steps/export/multidim/minerals/latest/minerals.py`. Note: same APIs are used by `multidim/` and `explorers/` — only the `explorer=True` flag differs.

Single-indicator explorer with conditional dimensions: `etl/steps/export/explorers/agriculture/latest/food_prices.{py,config.yml}` plus the indicator metadata in `etl/steps/data/garden/wb/2025-08-04/food_prices_for_nutrition.meta.yml` showing the `definitions.common.presentation.grapher_config` baseline + per-indicator `presentation.grapher_config.{subtitle, map.colorScale, note}`.

Table-driven with sort + display post-processing: `etl/steps/export/explorers/migration/latest/migration_flows.py`.
