---
name: migrate-explorer-indicator-legacy
description: >-
  Migrate a legacy ETL explorer step to the modern
  `export://explorers/<ns>/latest/<short>` shape that uses
  `paths.create_collection(explorer=True)`. Covers two legacy shapes: (A)
  `data://explorers/<ns>/<v>/<short>` steps that write wide CSV tables for the
  legacy explorer infra (e.g. poverty_inequality, lis, wid, wb, emdat,
  monkeypox); (B) `export://explorers/<ns>/latest/<short>` steps that already
  exist but build the TSV programmatically via
  `paths.create_explorer_legacy(df_graphers, df_columns)` (minerals,
  air_pollution, migration/2024-08-05,
  minerals_supply_and_demand_prospects). Trigger when the user says "migrate
  indicator-legacy explorer <short>", "convert create_explorer_legacy to
  create_collection", or "move data://explorers/... to export://explorers/...".
metadata:
  internal: true
---

# Migrate a Legacy ETL Explorer Step to YAML-driven `create_collection`

Modernize an explorer step that already exists in this repo but doesn't use the YAML-driven `paths.create_collection(explorer=True)` API yet. After this skill the step lives at `export://explorers/<ns>/latest/<short>`, is configured by `<short>.config.yml`, and the legacy shape is archived.

## Inputs

Required:
- The legacy step path. Either:
  - **Pattern A** — `etl/steps/data/explorers/<ns>/<v>/<short>.py` (writes wide CSV tables under `data://explorers/...`); or
  - **Pattern B** — `etl/steps/export/explorers/<ns>/<v>/<short>.py` that calls `paths.create_explorer_legacy(...)`.
- Target `<ns>` and `<short>` for the new step (Python uses snake_case; explorer slug keeps hyphens).

> **Not covered:** explorers that don't have any ETL step yet (e.g. `plastic-pollution`, `conflict-data`, `conflict-data-source`, `countries-in-conflict-data` referenced in umbrella issue #6014). For those use `migrate-explorer-grapher` (their TSVs reference indicator IDs and need fresh authoring as YAML, not modernization of an existing step).

## Step 0 — Identify which pattern you have

```bash
# Pattern A: legacy data://explorers/ writing wide CSVs
ls etl/steps/data/explorers/

# Pattern B: existing export://explorers/ using create_explorer_legacy
grep -l create_explorer_legacy etl/steps/export/explorers/ -r
```

| Pattern | Examples in repo today | What it produces |
|---|---|---|
| **A.** `data://explorers/<ns>/<v>/<short>` | `poverty_inequality`, `lis/luxembourg_income_study`, `wid/world_inequality_database`, `wb/world_bank_pip`, `emdat/natural_disasters`, `who/monkeypox`, `dummy` | A wide CSV at `data/explorers/<ns>/<v>/<short>/...` that the live explorer TSV (`owid-grapher/explorers/<short>.explorer.tsv`) reads via `tableSlug`. |
| **B.** `paths.create_explorer_legacy(df_graphers, df_columns, ...)` | `minerals/latest/minerals`, `minerals/latest/minerals_supply_and_demand_prospects`, `emissions/latest/air_pollution`, `migration/2024-08-05/migration` | A complete explorer TSV pushed straight to MySQL — no live TSV file in `owid-grapher/`, the ETL step is the source of truth. |

The two patterns share the destination (`export://explorers/<ns>/latest/<short>` with YAML config) but differ in starting state.

## Pattern B workflow (most common today — `create_explorer_legacy` → `create_collection`)

The legacy step builds two pandas DataFrames programmatically:
- `df_graphers` — one row per view, with dimension columns (`<Name> Dropdown` / `<Name> Radio` / `<Name> Checkbox`), `yVariableIds` (or catalog-path strings), and per-view chart settings (`hasMapTab`, `minTime`, `yAxisMin`, `defaultView`, …).
- `df_columns` — per-(view, indicator) display overrides (color scales, tolerances, units, …). Sometimes omitted.

You're going to delete the DataFrame plumbing and replace it with a YAML config plus, optionally, a table-driven `create_collection(tb=tb, ...)` call when the dimensions map cleanly onto an upstream multidim grapher dataset.

### 1. Read the legacy step end-to-end

Identify:
- Static settings that go to `config:` (title, subtitle, selection, isPublished, hasMapTab, …).
- The list of dimensions and their choices (the columns in `df_graphers` that end with `Dropdown`/`Radio`/`Checkbox`, and the unique values per column).
- The mapping from (dimension choices) → (catalog path, per-view config). This is the bulk of the legacy code.
- Any post-processing that's hard to express in YAML (e.g. sorting choices, setting per-metric display defaults, conditional `hasMapTab=False`). Keep this as Python.

### 2. Decide between full-YAML and table-driven

- **Full YAML** when each view is bespoke (different titles, subtitles, configs per dimension combination). Model after `crop_yields.{py,config.yml}`.
- **Table-driven** when the upstream is a multidim grapher dataset and dimensions map onto its indicator dimensions. Model after `migration/latest/migration_flows.py` — the YAML carries the static config, and `paths.create_collection(tb=tb, indicator_names=[...], dimensions=[...], common_view_config=...)` expands views automatically. Post-process in Python for the residual logic (e.g. per-metric `display` settings via `add_display_settings(c)`).

### 3. Scaffold the new step

```bash
mkdir -p etl/steps/export/explorers/<ns>/latest
```

`<short>.py` (full-YAML variant):

```python
"""Load grapher dataset and create an explorer tsv file."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    config = paths.load_collection_config()
    c = paths.create_collection(
        config=config,
        short_name="<short-with-hyphens>",
        explorer=True,
    )
    # Optional Python post-processing for things YAML can't express:
    # - c.sort_choices({"<dim_slug>": lambda x: sorted(x)})
    # - per-view display tweaks looping over c.views
    c.save(tolerate_extra_indicators=True)
```

`<short>.py` (table-driven variant):

```python
def run() -> None:
    config = paths.load_collection_config()
    ds = paths.load_dataset("<grapher_dataset>")
    tb = ds.read("<table>", load_data=False)

    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=[...],            # restrict to relevant indicators
        dimensions=["<dim_a>", "<dim_b>"],
        common_view_config={...},         # shared per-view config
        short_name="<short-with-hyphens>",
        explorer=True,
    )
    c.save(tolerate_extra_indicators=True)
```

### 4. Translate the legacy DataFrames to `<short>.config.yml`

| Legacy code | Goes to |
|---|---|
| `config = {...}` dict in legacy `run()` | top-level `config:` block in YAML |
| `df_graphers["<Name> Dropdown"]` unique values | one entry under `dimensions:` (slug = snake-case of `<Name>`, presentation type = dropdown/radio/checkbox) |
| Each row of `df_graphers` | one entry under `views:` with `dimensions:` map and `indicators.y[].catalogPath` |
| Per-row `hasMapTab`, `minTime`, `yAxisMin`, `defaultView`, `type`, … | `view.config` (or hoist into a `definitions: &common_view_config` if shared) |
| `df_columns` rows | `view.indicators.y[].display` (per-view, per-indicator) |
| Loops setting `display` on every "Production" view, etc. | Either fold into YAML (DRY via anchors) or keep as Python post-processing |

### 5. DAG

In `dag/<ns>.yml`, change the existing `export://explorers/<ns>/<v>/<short>:` block to point to the new version (typically `latest/`) — the dependencies (the upstream `data://grapher/...`) usually stay the same.

If the version is changing (`<v>` → `latest`), move the old block to `dag/archive/<ns>.yml` and update any consumers.

### 6. Archive the legacy step

```bash
mkdir -p etl/steps/archive/export/explorers/<ns>/<v>/
git mv etl/steps/export/explorers/<ns>/<v>/<short>.py etl/steps/archive/export/explorers/<ns>/<v>/
```

## Pattern A workflow (`data://explorers/<ns>/<v>/<short>` writing wide CSVs)

This pattern is older. The legacy step produces a wide CSV with one column per (indicator, dimension) combination, and the live explorer TSV in `owid-grapher/explorers/<short>.explorer.tsv` reads from it via `tableSlug` blocks.

### 1. Read the legacy step

`etl/steps/data/explorers/<ns>/<v>/<short>.py` typically pivots a long table into wide form:

```python
def run() -> None:
    ds = paths.load_dataset("<garden_dataset>")
    tb = ds.read("<table>")
    tb_wide = tb.pivot(...)
    ds_explorer = paths.create_dataset(tables=[tb_wide])
    ds_explorer.save()
```

### 2. Read the live explorer TSV

`owid-grapher/explorers/<short>.explorer.tsv` defines dimensions, views, and per-indicator display configs. Use the same translation strategy as `migrate-explorer-grapher`:

- Settings rows → top-level `config:` block in the new YAML.
- `graphers` table → `views:` (one per row).
- `columns` table → per-view `indicators[*].display`.

### 3. Map column slugs in the wide CSV to grapher catalog paths

Each `tableSlug`/column reference in the live TSV corresponds to one wide-CSV column (`metric_dimension1_dimension2`). Find the underlying garden/grapher dataset that produces those indicators (the legacy step's input `paths.load_dataset(...)`), and rewrite each TSV column reference as a `catalogPath` pointing at that grapher dataset.

If indicators don't exist as separate grapher columns (e.g. the legacy CSV had a custom pivoted shape), you may need to either expose them in the upstream grapher step, or keep a thin "explorer table" grapher dataset.

### 4. Scaffold, configure, and DAG-wire

Same as Pattern B steps 3–5, except DAG changes are heavier:
- **Add** `export://explorers/<ns>/latest/<short>:` with the upstream grapher datasets as deps.
- **Find consumers** of `data://explorers/<ns>/<v>/<short>` (search `dag/*.yml`). Update them to point at the new export step or remove if no longer needed.
- **Move** the legacy `data://explorers/<ns>/<v>/<short>` block into `dag/archive/<ns>.yml`.

### 5. Archive

```bash
mkdir -p etl/steps/archive/data/explorers/<ns>/<v>/
git mv etl/steps/data/explorers/<ns>/<v>/<short>.py etl/steps/archive/data/explorers/<ns>/<v>/
# plus any sibling YAMLs
```

## Verify (both patterns)

Hand off to the user:
1. `.venv/bin/etlr export://explorers/<ns>/latest/<short>` — runs the new step.
2. Diff the resulting TSV against the live `owid-grapher/explorers/<short>.explorer.tsv` (Pattern A) or against the previously published TSV in MySQL `explorers.config` (Pattern B). The Wizard's `apps/wizard/app_pages/explorer_diff/` page does this comparison interactively for staging vs production.
3. Open `http://staging-site-<branch>/explorers/<short>` and spot-check: default view, dimension switches, map tab, picker columns, country selection.
4. `make check`.

## Reference: existing migrations

- **Pattern B → table-driven**: `etl/steps/export/explorers/migration/latest/migration_flows.py` (the new shape that replaced `migration/2024-08-05/migration.py`).
- **Pattern B → full-YAML**: `etl/steps/export/explorers/agriculture/latest/crop_yields.{py,config.yml}` shows the YAML-only style.
- **Reference for legacy-to-modern diff**: compare `migration/2024-08-05/migration.py` (still on `create_explorer_legacy`) with `migration/latest/migration_flows.py` (modern).

## Follow-up

Once the explorer is on `create_collection(explorer=True)`, it's a candidate for the Track-B port to MDIM (`export://multidim/...`) once feature parity is reached. See umbrella issue #6014.
