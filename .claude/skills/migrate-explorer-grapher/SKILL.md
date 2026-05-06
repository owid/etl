---
name: migrate-explorer-grapher
description: >-
  Migrate a non-ETL explorer (lives in MySQL `explorers` table; TSV mirrored in
  `owid-grapher/explorers/`) into an ETL-managed
  `export://explorers/<ns>/latest/<short>` step. Trigger when the user says
  "bring <slug> explorer into ETL", "migrate grapher explorer <slug>", or refers
  to one of: energy, co2, democracy, global-health, food-prices, fertilizers,
  natural-disasters, food-footprints, plastic-pollution, conflict-data,
  conflict-data-source, countries-in-conflict-data.
metadata:
  internal: true
---

# Migrate Non-ETL Explorer into ETL

Bring an explorer that today lives in MySQL (DB-managed, source-of-truth TSV in `owid-grapher/explorers/<slug>.explorer.tsv`) into an ETL-managed `export://explorers/<ns>/latest/<short>` step. After this skill, the explorer is authored as YAML in this repo and republished by ETL on every run.

## Inputs

Required:
- `<slug>` — the explorer slug (matches the `.explorer.tsv` filename and the `slug` column in MySQL `explorers`)
- `<ns>` — target namespace under `etl/steps/export/explorers/<ns>/latest/`
- `<short>` — file/short name for the new step (Python files use snake_case; the explorer slug used by `create_collection(short_name=...)` keeps hyphens — see "Hyphens vs underscores" below)

## Step 0 — Classify the explorer first

Different TSV columns require different migration paths. Read a sample of the explorer's `graphers` block and check which ID columns are present:

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

Match the result against this table:

| TSV columns | Sub-case | Migration path |
|---|---|---|
| `yVariableIds` (sometimes also `xVariableId`, `colorVariableId`, `sizeVariableId`) | **Indicator-based** | Workflow below — straight ID-to-catalogPath mapping. |
| `grapherId` only | **Grapher-chart-based** | Same workflow, but each `grapherId` first has to be resolved to a primary variable ID via MySQL `charts` (`SELECT config FROM charts WHERE id = ?` then read `dimensions[0].variableId`) before mapping to a catalog path. The chart's `config` (title, subtitle, type, hasMapTab, …) becomes the per-view `config:` block. |
| `tableSlug` (with `table` blocks) — pure CSV-backed | **Wrong skill** | Use `migrate-explorer-csv` instead. |
| Mixed `grapherId` + `tableSlug` | **Hybrid** | Rare (`natural-disasters`, `food-footprints`). Migrate the grapherId rows as above; the tableSlug rows need a snapshot/garden chain (escalate per the CSV skill). |

## Workflow (indicator-based and grapher-chart-based)

### 1. Read the legacy explorer config

The TSV in `owid-grapher/explorers/<slug>.explorer.tsv` is one source; the live config in MySQL `explorers.config` is another (these can drift — prefer the DB if they disagree). The TSV has three sections:

- **Settings rows** at the top (key/value): `explorerTitle`, `explorerSubtitle`, `isPublished`, `hasMapTab`, `selection`, `pickerColumnSlugs`, `subNavId`, `subNavCurrentId`, `wpBlockId`, `entityType`, `originUrl`, `googleSheet`, `downloadDataLink`, `hideAlertBanner`, `thumbnail`, `yScaleToggle`, `yAxisMin`, `hideAnnotationFieldsInTitle`, …
- **`graphers` table**: one row per view. Columns include dimension widget names (e.g. `Metric Dropdown`, `Source Radio`, `Per capita Checkbox`), the ID column(s) from Step 0, plus per-view chart config (`title`, `subtitle`, `type`, `hasMapTab`, `minTime`, `yAxisMin`, …).
- **`columns` table** (optional): per-(view, indicator) display overrides.

### 2. Map IDs to catalog paths

For each unique ID, query MySQL to recover the catalog path:

```bash
# yVariableIds (and xVariableId, colorVariableId, sizeVariableId) — direct lookup
make query SQL="SELECT id, catalogPath FROM variables WHERE id IN (...)"

# grapherId — two-step lookup. The chart config lives in chart_configs (joined via charts.configId).
make query SQL="SELECT c.id, JSON_EXTRACT(cc.full, '\$.dimensions[0].variableId') AS yVarId FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id IN (...)"
# then feed the resulting variable IDs into the variables query above

# Pull the full chart config in one go (variable IDs + title/subtitle/type/map/etc.) for grapher-chart explorers:
make query SQL="SELECT c.id, cc.full FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id IN (...)"
```

Variables without `catalogPath` are not yet in ETL — their underlying datasets need to be migrated first (use the `migrate-dataset` skill). **Halt and report which datasets are missing rather than guessing.**

### 3. Identify the upstream grapher datasets

For each catalog path (`<ns>/<v>/<dataset>/<table>#<short>`), the underlying step is `data://grapher/<ns>/<v>/<dataset>`. Collect the unique set — these become DAG dependencies for the new explorer step.

> **Mental model for grapher-chart-based explorers**: each grapherId is just a thin wrapper around an indicator (or a few). The migration "unwraps" the chart, recovers its underlying indicators, and rebuilds the explorer directly from those indicators. Anything the chart has stored — title, subtitle, color scale, map config — either flows from the indicator's garden metadata (preferred for single-indicator views; see step 3.5) or has to be re-stated in the explorer YAML.

### 3.5. Decide the construction style

Two orthogonal choices before scaffolding: **how views are produced** (full-YAML vs table-driven via `paths.create_collection(tb=tb, ...)`) and **where chart text/config lives** (per-view in YAML vs each indicator's `presentation.grapher_config` in garden metadata). Multi-indicator views — including bundling several indicators into one chart via `c.group_views(...)` — are also part of this decision space.

For grapher-chart-based migrations the default is full-YAML, because each grapherId typically points at a different indicator from a non-dimensional table. Table-driven becomes attractive when the views are mostly single-indicator and the explorer's dimensions happen to match a multidim upstream dataset (or you can build one).

**Read `.claude/docs/explorer-programmatic-construction.md` for the full mechanics**: the API contract for `tb[col].m.dimensions` / `original_short_name` / `indicator_names` / `dimensions=`, FAUST inheritance from indicator metadata, post-processing with `sort_choices` and `group_views`, common pitfalls, and reference examples. The migration steps below assume you've made the choice; what changes between full-YAML and table-driven is mostly the shape of `<short>.py` and `<short>.config.yml`.

### 4. Scaffold the ETL step

```bash
mkdir -p etl/steps/export/explorers/<ns>/latest
```

`<short>.py`:

```python
"""Load grapher datasets and create an explorer tsv file."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    config = paths.load_collection_config()
    c = paths.create_collection(
        config=config,
        short_name="<short-with-hyphens>",  # explorer slug; usually matches <slug>
        explorer=True,
    )
    c.save(tolerate_extra_indicators=True)
```

**`tolerate_extra_indicators=True`** is the common case: the upstream grapher dataset usually has more indicators than the explorer references, and without this flag `c.save()` errors on the unused ones. Existing examples: `crop_yields.py`, `migration_flows.py`, `ipcc_scenarios.py`.

`<short>.config.yml` skeleton:

> **Always block style.** Mappings and lists in explorer config YAML must use block style (one key per line, list items on their own line with `-`). Never use flow style (`{ key: value, ... }` or `[a, b, c]`) even when a view's dimensions look "small enough to fit on one line." Reason: the user wants every config diffable line-by-line and consistent across explorers; PR review on a 45-view file is unreadable when half the views collapse to a single flow line. The only exception is markdown links inside a quoted-scalar `subtitle:`/`note:` (those `[text](url)` brackets are content, not YAML).
>
> The same rule applies to `presentation: { type: dropdown }` and `choices: [...]` — write them as block mappings/lists.

```yaml
config:
  # settings rows from the legacy TSV — keys verbatim
  explorerTitle: ...
  explorerSubtitle: ...
  isPublished: 'true'    # YAML strings, matching how legacy TSV stores them
  selection:
    - <default selected entities>
  hasMapTab: 'true'
  yAxisMin: '0'
  pickerColumnSlugs: []  # an empty list is OK; non-empty must be block-style
  hideAlertBanner: 'true'
  subNavId: explorers
  subNavCurrentId: <slug>
  # ...

definitions:
  # Shared config applied to all views. Use `definitions.common_views` (a list of
  # entries each with `config:` and an optional `dimensions:` filter) — NOT YAML
  # anchors and `<<:` merge keys. The framework merges these at expansion time;
  # the same convention is documented in detail in `/create-multidim`. Per-view
  # `config:` blocks can still override anything that comes from common_views.
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
  # one entry per dropdown/radio/checkbox column in the legacy graphers table
  - slug: <snake_case_of_widget_name>     # e.g. "metric"
    name: <human name>                     # e.g. "Metric"
    presentation:
      type: dropdown                        # or radio / checkbox
    choices:
      - slug: <snake>
        name: "<as shown in widget>"

views:
  # one entry per row in the legacy graphers table
  - dimensions:
      <dim_slug>: <choice_slug>
      # ...
    indicators:
      y:
        - catalogPath: <ns>/<v>/<dataset>/<table>#<short>
          display:                          # per-view, per-indicator overrides from columns block
            colorScaleNumericBins: 0;1;2
            colorScaleScheme: PuBu
    config:
      title: ...
      subtitle: ...
      type: <legacy chart type>             # LineChart, DiscreteBar, "LineChart DiscreteBar", StackedArea, …
      # Anything common to all views lives in definitions.common_views above; only
      # per-view overrides go here. No `<<:` merge keys, no anchors.
      hasMapTab: ...
      minTime: ...
      # other per-view overrides from the graphers table
```

### 5. Translate the legacy tables row-by-row

| Legacy TSV element | Goes to |
|---|---|
| Settings rows | top-level `config:` block, keys verbatim |
| `graphers` row | one `views:` entry |
| Dimension widget columns (e.g. `Metric Dropdown`) | `view.dimensions` (slug = snake-case of widget name without the `Dropdown`/`Radio`/`Checkbox` suffix) |
| `yVariableIds` (space-separated) | `view.indicators.y[]` — one item per ID, each with `catalogPath:` |
| `xVariableId` | `view.indicators.x[]` |
| `colorVariableId` | `view.indicators.color[]` |
| `sizeVariableId` | `view.indicators.size[]` |
| Per-view chart settings (`title`, `subtitle`, `type`, `hasMapTab`, `minTime`, `yAxisMin`, …) | `view.config` — but for single-indicator views, prefer pushing `title`/`subtitle`/`note` into the indicator's garden `presentation.grapher_config` (see step 3.5) |
| `columns` table row | `view.indicators.<axis>[i].display` (color scales, tolerance, units, …) |

For grapher-chart-based explorers, also pull the chart's stored config (`SELECT cc.full FROM charts c JOIN chart_configs cc ON c.configId = cc.id WHERE c.id = ?`) and merge `title`/`subtitle`/`type`/`hasMapTab`/`yAxis`/`map.colorScale`/etc. into `view.config` — or, for single-indicator views, into the indicator's garden metadata.

### 6. Hyphens vs underscores

- The Python file path uses underscores (`fish_stocks.py`, `crop_yields.py`).
- The explorer slug used in the URL and the `short_name` argument keep hyphens (`fish-stocks`, `crop-yields`).
- `paths.create_collection(short_name="<short-with-hyphens>")` — pass the hyphenated slug.

### 7. Add the DAG entry

In `dag/<ns>.yml`:

```yaml
export://explorers/<ns>/latest/<short>:
  - data://grapher/<ns1>/<v1>/<dataset1>
  - data://grapher/<ns2>/<v2>/<dataset2>
  # ... one line per unique upstream grapher dataset
```

### 8. Verify

Hand off to the user:
1. `.venv/bin/etlr export://explorers/<ns>/latest/<short>` — runs the step and writes the TSV.
2. Diff the resulting TSV against the live `owid-grapher/explorers/<slug>.explorer.tsv`. Expect functional equivalence; cosmetic differences (column ordering, whitespace) are acceptable. The Wizard's `explorer-diff` page (`apps/wizard/app_pages/explorer_diff/`) compares production vs staging.
3. Open `http://staging-site-<branch>/explorers/<slug>` and spot-check: default view, dimension switches, map tab, picker, country selection.
4. `make check`.

## Reference: existing migrations to model after

Full-YAML (each view hand-listed):
- `etl/steps/export/explorers/agriculture/latest/crop_yields.{py,config.yml}` — large indicator-based explorer with many dimensions.
- `etl/steps/export/explorers/agriculture/latest/food_prices.{py,config.yml}` — small grapher-chart-based migration (12 chart IDs unwrapped to 12 single-indicator views).
- `etl/steps/export/explorers/war/latest/countries_in_conflict_data.{py,config.yml}` — uses `na`-named choices to model conditional dimensions.
- `etl/steps/export/explorers/emissions/latest/ipcc_scenarios.{py,config.yml}` — moderate-size YAML-driven.

Table-driven (views auto-expanded from a dimensional table):
- `etl/steps/export/explorers/migration/latest/migration_flows.{py,config.yml}` — passes `tb=tb, indicator_names=[...], dimensions=[...]` to `create_collection`; YAML carries only the static config and dimension presentation.

## Follow-up

Once the explorer is in ETL, it's a candidate for the Track-B port to MDIM (`export://multidim/...`) once feature parity is reached. See umbrella issue #6014.
