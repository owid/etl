---
name: migrate-explorer-indicator-legacy
description: Migrate an indicator-legacy explorer step (data://explorers/<ns>/<v>/<short>, typically writing wide CSV tables for the explorer surface) into a modern export://explorers/<ns>/latest/<short> step that uses paths.create_collection with explorer=True. Trigger when the user says "migrate indicator-legacy explorer <short>", "move data://explorers/... to export://explorers/...", or refers to the legacy indicator-based explorers (migration, minerals, air-pollution, plastic-pollution, conflict-data, conflict-data-source, countries-in-conflict-data).
metadata:
  internal: true
---

# Migrate Indicator-legacy Explorer to ETL Export

Move an explorer that today lives at `data://explorers/<ns>/<v>/<short>` (writes intermediate wide CSVs that the legacy explorer infra reads) to the modern `export://explorers/<ns>/latest/<short>` shape (uses `paths.create_collection(explorer=True)` directly over grapher indicators).

## Inputs

Required:
- `<ns>`, `<v>`, `<short>` — fully qualifying the legacy `data://explorers/<ns>/<v>/<short>` step

## Workflow

### 1. Read the legacy step

Open `etl/steps/data/explorers/<ns>/<v>/<short>.py`. Typical pattern:

```python
def run() -> None:
    ds = paths.load_dataset("<garden_dataset>")
    tb = ds.read("<table>")
    # reshape into a wide table the explorer can consume
    tb_wide = tb.pivot(...)
    ds_explorer = paths.create_dataset(tables=[tb_wide])
    ds_explorer.save()
```

The output is a CSV at `data/explorers/<ns>/<v>/<short>/...`, referenced by the live explorer TSV in `owid-grapher/explorers/<short>.explorer.tsv`.

### 2. Identify upstream

Find the underlying garden/grapher dataset(s) the legacy step reads from. They become the new explorer step's DAG dependencies.

### 3. Read the live explorer TSV

`owid-grapher/explorers/<short>.explorer.tsv` defines the dimensions, views, and per-indicator display configs. Use the same translation strategy as `migrate-explorer-grapher`:

- Settings rows → top-level `config:` block in the new YAML.
- `graphers` table → `views:` (one per row). Map `yVariableIds` to `catalogPath` of the new grapher dataset.
- `columns` table → per-view `indicators[*].display` overrides.

### 4. Scaffold the new step

`etl/steps/export/explorers/<ns>/latest/<short>.py`:

```python
from etl.helpers import PathFinder
paths = PathFinder(__file__)

def run() -> None:
    config = paths.load_collection_config()
    c = paths.create_collection(config=config, short_name="<short-with-hyphens>", explorer=True)
    c.save()
```

`<short>.config.yml`: full YAML with `config:`, `dimensions:`, `definitions:`, `views:` blocks.

### 5. DAG: replace the legacy step

In `dag/<ns>.yml`:

- **Add** `export://explorers/<ns>/latest/<short>:` with the upstream grapher datasets as deps.
- **Find consumers** of `data://explorers/<ns>/<v>/<short>` (search the DAG for that URI). Update them to point at the new export step or remove if no longer needed.
- **Move** the legacy `data://explorers/<ns>/<v>/<short>` block into `dag/archive/<ns>.yml`.

### 6. Archive the legacy code

```bash
mkdir -p etl/steps/archive/data/explorers/<ns>/<v>/
git mv etl/steps/data/explorers/<ns>/<v>/<short>.py etl/steps/archive/data/explorers/<ns>/<v>/
```

(plus any sibling YAMLs)

### 7. Verify

Hand off to the user:
1. `.venv/bin/etlr export://explorers/<ns>/latest/<short>` — runs the new step.
2. Diff the resulting TSV against the live `owid-grapher/explorers/<short>.explorer.tsv`.
3. Open `http://staging-site-<branch>/explorers/<short>` and spot-check.
4. `make check`.

## Follow-up

Once in ETL, port to MDIM-derived via `migrate-explorer-to-mdim`.
