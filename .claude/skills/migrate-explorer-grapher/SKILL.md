---
name: migrate-explorer-grapher
description: >-
  Translate a non-ETL explorer (lives in MySQL `explorers` table; TSV mirrored
  in `owid-grapher/explorers/`) whose `graphers` block references variable IDs
  or chart IDs into the inputs `/create-explorer` needs. Resolves IDs to
  catalog paths, pulls per-chart config when needed, and produces the dimension
  + view layout. Trigger when the user says "bring <slug> explorer into ETL",
  "migrate grapher explorer <slug>", or refers to one of: energy, co2,
  democracy, global-health, food-prices, fertilizers, natural-disasters,
  food-footprints, plastic-pollution, conflict-data, conflict-data-source,
  countries-in-conflict-data.
metadata:
  internal: true
---

# Translate a non-ETL grapher-style explorer for `/create-explorer`

The TSV in `owid-grapher/explorers/<slug>.explorer.tsv` (and the live config in MySQL `explorers.config`) is the legacy source of truth. This skill pulls the IDs out of it, resolves them to ETL catalog paths, recovers any chart-stored config, and hands the result to `/create-explorer` to write the actual `export://explorers/<ns>/latest/<short>` step.

> **Scope of this skill:** the ID-resolution and TSV-translation half. The export-step authoring (Python skeleton, YAML schema, full-YAML vs table-driven choice, FAUST upstream, post-processing, DAG, verification) lives in `/create-explorer` and is shared with the other migrate-explorer-* skills and with `/create-multidim`. Don't duplicate that content here.

## Inputs

Required:
- `<slug>` — explorer slug (matches the `.explorer.tsv` filename and the `slug` column in MySQL `explorers`)
- `<ns>` — target namespace under `etl/steps/export/explorers/<ns>/latest/`
- `<short>` — file/short name for the new step

## Step 0 — Classify the explorer

Different `graphers` columns require different lookups. Sample the legacy config:

```bash
cat <<'EOF' > /tmp/classify_explorer.py
from etl.config import OWID_ENV
import json, sys
slug = sys.argv[1]
df = OWID_ENV.read_sql(f"SELECT config FROM explorers WHERE slug = '{slug}'")
cfg = json.loads(df["config"].iloc[0]) if isinstance(df["config"].iloc[0], str) else df["config"].iloc[0]
blocks = cfg.get("blocks", [])
print("blocks:", [b.get("type") for b in blocks])
gb = next((b for b in blocks if b.get("type") == "graphers"), None)
if gb:
    cols = set().union(*(r.keys() for r in gb.get("block", [])))
    print("graphers cols:", sorted(cols))
EOF
.venv/bin/python /tmp/classify_explorer.py <slug>
```

| TSV columns | Sub-case | Path |
|---|---|---|
| `yVariableIds` (and optional `xVariableId` / `colorVariableId` / `sizeVariableId`) | **Indicator-based** | This skill — direct ID→catalogPath mapping. |
| `grapherId` only | **Grapher-chart-based** | This skill — two-step lookup; the chart's stored config (title, subtitle, type, hasMapTab, …) becomes per-view config. |
| `tableSlug` (with `table` blocks) — pure CSV | **Wrong skill** | Use `/migrate-explorer-csv` instead. |
| Mixed `grapherId` + `tableSlug` | **Hybrid** | Rare (`natural-disasters`, `food-footprints`). Migrate the grapherId rows here; for the tableSlug rows, do the snapshot/garden chain via `/migrate-explorer-csv` (steps 1–5) and merge the resulting catalogPaths into the same explorer config. |

## Step 1 — Read the legacy explorer config

Two sources: the TSV in `owid-grapher/explorers/<slug>.explorer.tsv`, and the live config in MySQL `explorers.config`. **Prefer the DB if they disagree.** The TSV has three sections:

- **Settings rows** at the top (key/value): `explorerTitle`, `explorerSubtitle`, `isPublished`, `hasMapTab`, `selection`, `pickerColumnSlugs`, `subNavId`, `subNavCurrentId`, `wpBlockId`, `entityType`, `originUrl`, `googleSheet`, `downloadDataLink`, `hideAlertBanner`, `thumbnail`, `yScaleToggle`, `yAxisMin`, `hideAnnotationFieldsInTitle`, … These map verbatim into the explorer's top-level `config:` block (see `/create-explorer` for the schema).
- **`graphers` table**: one row per view. Columns include dimension widget names (e.g. `Metric Dropdown`, `Source Radio`, `Per capita Checkbox`), the ID column(s) from Step 0, plus per-view chart config (`title`, `subtitle`, `type`, `hasMapTab`, `minTime`, `yAxisMin`, …).
- **`columns` table** (optional): per-(view, indicator) display overrides.

## Step 2 — Map IDs to catalog paths

```bash
# yVariableIds (and xVariableId, colorVariableId, sizeVariableId) — direct lookup
make query SQL="SELECT id, catalogPath FROM variables WHERE id IN (...)"

# grapherId — two-step lookup. Chart config lives in chart_configs (joined via charts.configId).
make query SQL="SELECT c.id, JSON_EXTRACT(cc.full, '\$.dimensions[0].variableId') AS yVarId FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id IN (...)"
# then feed the resulting variable IDs into the variables query above.

# Pull the full chart config in one go (variable IDs + title/subtitle/type/map/etc.) for grapher-chart explorers:
make query SQL="SELECT c.id, cc.full FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id IN (...)"
```

Variables without `catalogPath` are not yet in ETL — their underlying datasets need to be migrated first (use the `/migrate-dataset` skill). **Halt and report which datasets are missing rather than guessing.**

For multi-indicator charts (e.g. stacked bars), `dimensions[0]` only gives the first y variable. Pull all dimensions from `chart_configs.full` and treat each one as an entry in `view.indicators.y[]`.

## Step 3 — Identify upstream grapher datasets

For each catalog path (`<ns>/<v>/<dataset>/<table>#<short>`), the underlying step is `data://grapher/<ns>/<v>/<dataset>`. Collect the unique set — these become the explorer step's DAG dependencies (handed to `/create-explorer` in step 5).

> **Mental model for grapher-chart-based explorers**: each grapherId is a thin wrapper around an indicator (or a few). The migration "unwraps" the chart, recovers its underlying indicators, and rebuilds the explorer directly from those indicators. Anything the chart has stored — title, subtitle, color scale, map config — either flows from the indicator's garden metadata (preferred for single-indicator views; see `/create-explorer` Step 5) or has to be re-stated in the explorer YAML.

## Step 4 — Translate the legacy tables

| Legacy TSV element | Goes to (in the YAML produced by `/create-explorer`) |
|---|---|
| Settings rows | top-level `config:` block, keys verbatim |
| `graphers` row | one `views:` entry |
| Dimension widget columns (e.g. `Metric Dropdown`) | `dimensions:` entries (slug = snake-case of widget name without the `Dropdown`/`Radio`/`Checkbox` suffix) and `view.dimensions:` map |
| `yVariableIds` (space-separated) | `view.indicators.y[]` — one item per ID, each with `catalogPath:` |
| `xVariableId` | `view.indicators.x[]` |
| `colorVariableId` | `view.indicators.color[]` |
| `sizeVariableId` | `view.indicators.size[]` |
| Per-view chart settings (`title`, `subtitle`, `type`, `hasMapTab`, `minTime`, `yAxisMin`, …) | `view.config` — but for single-indicator views, prefer pushing `title`/`subtitle`/`note` into the indicator's garden `presentation.grapher_config` (see `/create-explorer` Step 5) |
| `columns` table row | `view.indicators.<axis>[i].display` (color scales, tolerance, units, …) |

For grapher-chart-based explorers, also pull each chart's stored config (`SELECT cc.full FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id = ?`) and merge `title`/`subtitle`/`type`/`hasMapTab`/`yAxis`/`map.colorScale`/etc. into `view.config` — or, for single-indicator views, into the indicator's garden metadata.

## Step 5 — Hand off to `/create-explorer`

Invoke `/create-explorer` with the materials prepared above:
- the top-level `config:` settings (from TSV settings rows)
- the dimensions list (slugs, names, presentation types, choices)
- the views list (per-view dimension tuples + catalogPaths + per-view config overrides)
- the upstream `data://grapher/...` deps for the DAG entry

`/create-explorer` covers: the Python skeleton (full-YAML vs table-driven), the YAML schema (`config:`, `definitions.common_views`, `dimensions:`, `views:`), block-style YAML rule, hyphens vs underscores, conditional dimensions, FAUST upstream, post-processing (`sort_choices`, `group_views`), DAG entry, verification, common pitfalls, reference examples.

## Reference: existing migrations to model after

(see `/create-explorer` for the canonical list — examples that originated as `/migrate-explorer-grapher` runs include `food_prices`, `fertilizers`, `food_footprints`, `countries_in_conflict_data`)

## Follow-up

Once the explorer is in ETL, it's a candidate for the Track-B port to MDIM (`export://multidim/...`) once feature parity is reached. See umbrella issue #6014.
