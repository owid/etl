---
name: migrate-explorer-grapher
description: Migrate a non-ETL grapher-based explorer (its TSV lives in owid-grapher/explorers/) into an ETL-based export://explorers/<ns>/latest/<short> step. Trigger when the user says "bring <slug> explorer into ETL", "migrate grapher explorer <slug>", or refers to one of the manual grapher-based explorers (energy, co2, democracy, global-health, food-prices, fertilizers, natural-disasters, food-footprints).
metadata:
  internal: true
---

# Migrate Grapher-based Explorer into ETL

Bring an explorer that currently lives in `owid-grapher/explorers/<slug>.explorer.tsv` (manually maintained, references existing grapher charts via variable IDs) into an ETL-managed `export://explorers/<ns>/latest/<short>` step. After this skill, the explorer is authored as YAML in this repo and republished by ETL.

## Inputs

Required:
- `<slug>` — the explorer slug (matches the `.explorer.tsv` filename)
- `<ns>` — target namespace under `etl/steps/export/explorers/`
- `<short>` — short name for the new step (typically the slug with hyphens preserved)

## Workflow

### 1. Read the legacy explorer TSV

Open `owid-grapher/explorers/<slug>.explorer.tsv` (or fetch from `https://ourworldindata.org/admin/explorers/<slug>` if local checkout isn't available).

The TSV has three sections:
- **Settings rows** at the top (key/value): `explorerTitle`, `explorerSubtitle`, `isPublished`, `hasMapTab`, `selection`, `pickerColumnSlugs`, `subNavId`, etc.
- **`graphers` table**: one row per view. Columns include dimension widget names (e.g. `Metric Dropdown`, `Source Radio`), `yVariableIds`, optional `xVariableId`, `colorVariableId`, plus per-view chart config (`title`, `subtitle`, `type`, `hasMapTab`, ...).
- **`columns` table** (optional): per-(view, indicator) display overrides.

### 2. Map variable IDs to catalog paths

Each `yVariableIds` cell is a space-separated list of integer variable IDs. For each unique ID, query MySQL to recover the catalog path:

```bash
make query SQL="SELECT id, catalogPath FROM variables WHERE id IN (...)"
```

Variables without `catalogPath` are not yet in ETL — their underlying datasets need to be migrated first (use the `migrate-dataset` skill). Halt and report which datasets are missing rather than guessing.

### 3. Identify the upstream grapher datasets

For each catalog path (`<ns>/<v>/<dataset>#<short>`), the underlying grapher step is `data://grapher/<ns>/<v>/<dataset>`. Collect the unique set — these become DAG dependencies.

### 4. Scaffold the ETL step

Create the directory:

```bash
mkdir -p etl/steps/export/explorers/<ns>/latest
```

`<short>.py`:

```python
from etl.helpers import PathFinder
paths = PathFinder(__file__)

def run() -> None:
    config = paths.load_collection_config()
    c = paths.create_collection(config=config, short_name="<short-with-hyphens>", explorer=True)
    c.save()
```

`<short>.config.yml` skeleton (filled in below):

```yaml
config:
  # settings rows from the legacy TSV
  explorerTitle: ...
  isPublished: true
  # ...

definitions:
  common_views:
    # if many views share config, hoist it here

dimensions:
  # one entry per dropdown/radio/checkbox column in the legacy graphers table
  - slug: <snake_case_of_widget_name>
    name: <human name>
    presentation:
      type: dropdown  # or radio / checkbox
    choices:
      - { slug: <snake>, name: "<as shown in widget>" }

views:
  # one entry per row in the legacy graphers table
  - dimensions: { <dim_slug>: <choice_slug>, ... }
    indicators:
      y:
        - catalogPath: <ns>/<v>/<dataset>#<short>
    config:
      title: ...
      subtitle: ...
      type: <legacy chart type>  # LineChart, DiscreteBar, "LineChart DiscreteBar", etc.
      hasMapTab: ...
      # per-view overrides from the graphers table
```

### 5. Translate the legacy tables row-by-row

- **Settings** → top-level `config:` block (verbatim keys: `explorerTitle`, `isPublished`, `selection`, `subNavId`, `pickerColumnSlugs`, `hasMapTab`, `entityType`, `wpBlockId`, `originUrl`, `googleSheet`, `downloadDataLink`, `hideAlertBanner`, `thumbnail`, `subNavCurrentId`, ...).
- **graphers table** → one `views:` entry per row. The dimension widget columns become `view.dimensions`. `yVariableIds` becomes `view.indicators.y` (one item per ID, mapped to `catalogPath`). Remaining columns (title, subtitle, type, hasMapTab, ...) go in `view.config`.
- **columns table** → per-view, per-indicator `display:` overrides (color scale, tolerance, etc.). Inline these into the matching `view.indicators.y[i].display` block.

### 6. Add the DAG entry

In `dag/<ns>.yml`:

```yaml
export://explorers/<ns>/latest/<short>:
  - data://grapher/<ns1>/<v1>/<dataset1>
  - data://grapher/<ns2>/<v2>/<dataset2>
  # ... one line per unique upstream grapher dataset
```

### 7. Verify

Hand off to the user:
1. `.venv/bin/etlr export://explorers/<ns>/latest/<short>` — runs the step, uploads the legacy TSV.
2. Diff the resulting TSV against `owid-grapher/explorers/<slug>.explorer.tsv`. Expect functional equivalence; cosmetic differences are acceptable.
3. Open `http://staging-site-<branch>/explorers/<slug>` and spot-check.
4. `make check`.

## Follow-up

Once the explorer is in ETL, it's a candidate for the Track-B port to MDIM-derived (`migrate-explorer-to-mdim`).
