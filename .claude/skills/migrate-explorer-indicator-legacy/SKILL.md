---
name: migrate-explorer-indicator-legacy
description: >-
  Modernize an explorer step that already exists in this repo but doesn't use
  the YAML-driven `paths.create_collection(explorer=True)` API yet. Covers two
  legacy shapes: (A) `data://explorers/<ns>/<v>/<short>` steps that write wide
  CSV tables for the legacy explorer infra (e.g. poverty_inequality, lis, wid,
  wb, emdat, monkeypox); (B) `export://explorers/<ns>/latest/<short>` steps
  that already exist but build the TSV programmatically via
  `paths.create_explorer_legacy(df_graphers, df_columns)` (minerals,
  air_pollution, migration/2024-08-05, minerals_supply_and_demand_prospects).
  This skill extracts the dimensions/views/config from the legacy shape and
  hands them to `/create-explorer` to write the modern step. Trigger when the
  user says "migrate indicator-legacy explorer <short>", "convert
  create_explorer_legacy to create_collection", or "move data://explorers/...
  to export://explorers/...".
metadata:
  internal: true
---

# Modernize a Legacy ETL Explorer Step

Take an explorer step that already lives in this repo but uses the old DataFrame-driven plumbing, extract the dimensions/views/config, and hand off to `/create-explorer` to write the modern YAML-driven step. Then archive the legacy code.

> **Scope of this skill:** identifying which legacy shape you have, reading the legacy code, mapping its DataFrames into the dimension/view structure that `/create-explorer` consumes, and archiving the old step. The export-step authoring (Python skeleton, YAML schema, full-YAML vs table-driven, FAUST upstream, post-processing, DAG, verification) lives in `/create-explorer`. Don't duplicate that content here.

## Inputs

Required:
- The legacy step path. Either:
  - **Pattern A** — `etl/steps/data/explorers/<ns>/<v>/<short>.py` (writes wide CSV tables under `data://explorers/...`); or
  - **Pattern B** — `etl/steps/export/explorers/<ns>/<v>/<short>.py` that calls `paths.create_explorer_legacy(...)`.
- Target `<ns>` and `<short>` for the new step.

> **Not covered:** explorers that don't have any ETL step yet (e.g. `plastic-pollution`, `conflict-data`, `conflict-data-source`, `countries-in-conflict-data` referenced in umbrella issue #6014). For those use `/migrate-explorer-grapher` (their TSVs reference indicator IDs and need fresh authoring as YAML, not modernization of an existing step).

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

## Pattern B workflow (most common today)

The legacy step builds two pandas DataFrames programmatically:
- `df_graphers` — one row per view, with dimension columns (`<Name> Dropdown` / `<Name> Radio` / `<Name> Checkbox`), `yVariableIds` (or catalog-path strings), and per-view chart settings (`hasMapTab`, `minTime`, `yAxisMin`, `defaultView`, …).
- `df_columns` — per-(view, indicator) display overrides (color scales, tolerances, units, …). Sometimes omitted.

You're going to delete the DataFrame plumbing and replace it with a YAML config plus, optionally, a table-driven `create_collection(tb=tb, ...)` call when the dimensions map cleanly onto an upstream multidim grapher dataset.

### 1. Read the legacy step end-to-end

Identify:
- Static settings that go to `config:` (title, subtitle, selection, isPublished, hasMapTab, …).
- The list of dimensions and their choices (the columns in `df_graphers` that end with `Dropdown`/`Radio`/`Checkbox`, and the unique values per column).
- The mapping from (dimension choices) → (catalog path, per-view config). This is the bulk of the legacy code.
- Any post-processing that's hard to express in YAML (e.g. sorting choices, setting per-metric display defaults, conditional `hasMapTab=False`). Worth noting so it can be ported as Python in the new step.

### 2. Map the legacy DataFrames to the explorer YAML structure

| Legacy code | Where it goes |
|---|---|
| `config = {...}` dict in legacy `run()` | top-level `config:` block in YAML |
| `df_graphers["<Name> Dropdown"]` unique values | one entry under `dimensions:` (slug = snake-case of `<Name>`, presentation type = dropdown/radio/checkbox) |
| Each row of `df_graphers` | one entry under `views:` with `dimensions:` map and `indicators.y[].catalogPath` |
| Per-row `hasMapTab`, `minTime`, `yAxisMin`, `defaultView`, `type`, … | `view.config` (or factored into `definitions.common_views` if shared across many views) |
| Per-row `title`/`subtitle`/`note` for single-indicator views | Prefer `presentation.grapher_config` in the indicator's garden metadata; only put in `view.config` if the view has multiple indicators or the text genuinely differs from the indicator's metadata |
| `df_columns` rows | `view.indicators.y[].display` (per-view, per-indicator) |
| Loops setting `display` on every "Production" view, etc. | Either fold into `definitions.common_views` (with a dimension filter) or keep as Python post-processing on `c.views` |

A legacy step that already builds dimensions from a multidim grapher dataset (rather than hand-stitching variable IDs) is usually a clean fit for the table-driven variant in `/create-explorer`. If many of the legacy views are multi-indicator already, you'll either keep them in YAML or rebuild them via `c.group_views(...)` after auto-expansion (see `/create-explorer` Step 6).

### 3. Hand off to `/create-explorer`

Invoke `/create-explorer` with:
- the top-level `config:` settings extracted in step 1
- the dimensions list (slugs, names, presentation types, choices)
- the views list (per-view dimension tuples + catalogPaths + per-view config overrides)
- any post-processing requirements you flagged in step 1 (`sort_choices`, custom display loops)
- the upstream `data://grapher/...` deps for the DAG entry — same as the legacy step's deps in `dag/<ns>.yml`

### 4. DAG

In `dag/<ns>.yml`, the `/create-explorer` skill writes the new `export://explorers/<ns>/latest/<short>:` block. You then need to:

- If a previous `export://explorers/<ns>/<v>/<short>:` block existed (Pattern B with non-`latest` version), move it to `dag/archive/<ns>.yml`.
- For Pattern A: also remove the legacy `data://explorers/<ns>/<v>/<short>:` block from `dag/<ns>.yml` and move it to `dag/archive/`. Find any consumers (`grep -rn 'data://explorers/<ns>/<v>/<short>' dag/`) and update them.

### 5. Archive the legacy step

```bash
# Pattern B
mkdir -p etl/steps/archive/export/explorers/<ns>/<v>/
git mv etl/steps/export/explorers/<ns>/<v>/<short>.py etl/steps/archive/export/explorers/<ns>/<v>/

# Pattern A (also archive the wide-CSV writer)
mkdir -p etl/steps/archive/data/explorers/<ns>/<v>/
git mv etl/steps/data/explorers/<ns>/<v>/<short>.py etl/steps/archive/data/explorers/<ns>/<v>/
# plus any sibling YAMLs under that dir
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

`owid-grapher/explorers/<short>.explorer.tsv` defines dimensions, views, and per-indicator display configs. Use the same translation strategy as `/migrate-explorer-grapher` step 4:

- Settings rows → top-level `config:` block.
- `graphers` table → `views:` (one per row).
- `columns` table → per-view `indicators[*].display`.

### 3. Map column slugs in the wide CSV to grapher catalog paths

Each `tableSlug`/column reference in the live TSV corresponds to one wide-CSV column (`metric_dimension1_dimension2`). Find the underlying garden/grapher dataset that produces those indicators (the legacy step's input `paths.load_dataset(...)`), and rewrite each TSV column reference as a `catalogPath` pointing at that grapher dataset.

If indicators don't exist as separate grapher columns (e.g. the legacy CSV had a custom pivoted shape), you may need to either expose them in the upstream grapher step, or keep a thin "explorer table" grapher dataset.

### 4. Hand off to `/create-explorer`, then archive

Same as Pattern B steps 3–5.

## Verify (both patterns)

Hand off to the user the verification commands per `/create-explorer` Step 8. For Pattern A specifically, also confirm the legacy `data://explorers/...` step is no longer being built (DAG-wise) and the live TSV in `owid-grapher/` is no longer being read by the rendered explorer.

## Reference: existing migrations

- **Pattern B → table-driven**: `etl/steps/export/explorers/migration/latest/migration_flows.py` (the new shape that replaced `migration/2024-08-05/migration.py`).
- **Pattern B → full-YAML**: `etl/steps/export/explorers/agriculture/latest/crop_yields.{py,config.yml}` shows the YAML-only style.
- **Reference for legacy-to-modern diff**: compare `migration/2024-08-05/migration.py` (still on `create_explorer_legacy`) with `migration/latest/migration_flows.py` (modern).

## Follow-up

Once on `create_collection(explorer=True)`, the explorer is a candidate for the Track-B port to MDIM (`export://multidim/...`) once feature parity is reached. See umbrella issue #6014.
