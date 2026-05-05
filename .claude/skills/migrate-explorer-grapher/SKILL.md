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

Two orthogonal choices before scaffolding:

**View construction — full-YAML vs table-driven**

- **Full-YAML** (default for grapher-chart-based migrations): hand-list every view under `views:` in the YAML. Use when views are bespoke (different titles, subtitles, indicators per dimension combination — typical when each grapherId points at a different indicator). Model: `crop_yields.{py,config.yml}`, `countries_in_conflict_data.{py,config.yml}`, the `food_prices` migration above.
- **Table-driven**: pass `tb=tb, indicator_names=[...], dimensions=[...]` to `create_collection` and let it auto-expand views from the dimensional indicators of an upstream multidim grapher dataset. Use when the explorer's dimensions map cleanly onto the indicator dimensions of one upstream dataset (e.g. metric × gender × age). YAML carries only the static config + dimension definitions; views are generated. Model: `migration/latest/migration_flows.py`.

  ```python
  ds = paths.load_dataset("<grapher_dataset>")
  tb = ds.read("<table>", load_data=False)
  c = paths.create_collection(
      config=config,
      tb=tb,
      indicator_names=["migrants"],
      dimensions=["country_select", "metric", "gender"],
      common_view_config={"type": "LineChart DiscreteBar", "hasMapTab": True, "tab": "map"},
      short_name="<short-with-hyphens>",
      explorer=True,
  )
  c.sort_choices({"country_select": lambda x: sorted(x)})  # optional post-processing
  c.save()
  ```

  Most grapher-chart-based migrations don't qualify for table-driven because each grapherId tends to reference a different indicator from a non-dimensional table. Prefer table-driven only when you can verify the upstream is genuinely multidim and the explorer's dimensions match.

**Where FAUST lives — garden metadata vs explorer config**

For **single-indicator views**, prefer pushing the chart text (title, subtitle, note, description_short, …) upstream into the indicator's garden metadata under `presentation.grapher_config` (or the equivalent top-level fields like `title_public`). The explorer view then inherits it automatically — no duplication, and standalone charts on the same indicator pick up the same text.

For **multi-indicator views** (one view, multiple `y[]` entries, or x/y/color/size combos), FAUST cannot live on any single indicator. Put it in the explorer view's `config:` block.

When migrating a grapher-chart-based explorer:
1. For each chart's `title`/`subtitle`/`note`, first check whether the underlying indicator's garden metadata (`presentation.grapher_config.title`, etc.) already carries it. If yes, leave it off the explorer view — it inherits.
2. If the chart's text differs from the indicator's metadata and the indicator is otherwise unused elsewhere, consider updating the garden metadata instead of duplicating in the explorer.
3. Only put FAUST in `view.config` when the view has multiple indicators, or when the indicator metadata is shared across charts that use different text.

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
  pickerColumnSlugs: []
  hideAlertBanner: 'true'
  subNavId: explorers
  subNavCurrentId: <slug>
  # ...

definitions:
  # Hoist anchors here when many views share config
  common_chart_config: &common_chart_config
    type: LineChart DiscreteBar
    hasMapTab: true

dimensions:
  # one entry per dropdown/radio/checkbox column in the legacy graphers table
  - slug: <snake_case_of_widget_name>     # e.g. "metric"
    name: <human name>                     # e.g. "Metric"
    presentation:
      type: dropdown                        # or radio / checkbox
    choices:
      - { slug: <snake>, name: "<as shown in widget>" }

views:
  # one entry per row in the legacy graphers table
  - dimensions: { <dim_slug>: <choice_slug>, ... }
    indicators:
      y:
        - catalogPath: <ns>/<v>/<dataset>/<table>#<short>
          display:                          # per-view, per-indicator overrides from columns block
            colorScaleNumericBins: 0;1;2
            colorScaleScheme: PuBu
    config:
      <<: *common_chart_config              # if shared
      title: ...
      subtitle: ...
      type: <legacy chart type>             # LineChart, DiscreteBar, "LineChart DiscreteBar", StackedArea, …
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
