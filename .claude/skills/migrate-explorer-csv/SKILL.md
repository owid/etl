---
name: migrate-explorer-csv
description: Migrate a non-ETL CSV-based explorer (its data is a static CSV hosted in owid-grapher/public/explorers/ or on GitHub) into ETL by adding a snapshot → meadow → garden → grapher → export chain. Trigger when the user says "bring CSV explorer <slug> into ETL", or refers to a static-CSV-backed explorer like fish-stocks.
metadata:
  internal: true
---

# Migrate CSV-based Explorer into ETL

Bring an explorer that today reads from a static CSV (e.g. hosted in `owid-grapher/public/explorers/<slug>/...` or a GitHub raw URL) into ETL. After this skill, the data is snapshot-tracked and flows through the standard ETL stages, and the explorer is published via `export://explorers/<ns>/latest/<short>`.

## Inputs

Required:
- `<slug>` — explorer slug
- CSV source URL or repo path
- `<ns>` — target namespace under `snapshots/` and `etl/steps/data/`
- `<short>` — short name for all stages

## Workflow

### 1. Locate the source CSV

Look in `owid-grapher/public/explorers/<slug>/...` first; otherwise fetch from the URL the explorer points at. Record:
- exact URL
- date last modified (use as the snapshot version, format `YYYY-MM-DD`)

### 2. Create the snapshot

Use the `create-snapshot` skill (or hand-write `snapshots/<ns>/<date>/<short>.csv.dvc` plus the matching script). The DVC file should record `url_main`, `url_download`, `date_published`, license, citation, and any source metadata. Run `.venv/bin/etls <ns>/<date>/<short>.csv` to fetch and upload the snapshot.

### 3. Meadow step

`etl/steps/data/meadow/<ns>/<date>/<short>.py`:

```python
from etl.helpers import PathFinder
paths = PathFinder(__file__)

def run() -> None:
    snap = paths.load_snapshot()
    tb = snap.read_csv()
    # minimal cleaning; let garden do harmonisation
    tb = tb.format(short_name=paths.short_name)
    ds = paths.create_dataset(tables=[tb])
    ds.save()
```

### 4. Garden step

`etl/steps/data/garden/<ns>/<date>/<short>.py`:
- harmonize country/region names via `paths.regions.harmonize_names(...)`
- convert types and units to ETL conventions
- optionally compute per-capita or per-region aggregates
- write `*.meta.yml` with origins, descriptions, and per-indicator metadata

### 5. Grapher step

`etl/steps/data/grapher/<ns>/<date>/<short>.py`:
- read the garden table, format it for grapher (long-format `country/year/value`), save.

### 6. Explorer step

`etl/steps/export/explorers/<ns>/latest/<short>.py` + `<short>.config.yml` — same shape as `migrate-explorer-grapher`, but `views[].indicators.y[].catalogPath` references the grapher dataset created above.

### 7. DAG

`dag/<ns>.yml`:

```yaml
data://meadow/<ns>/<date>/<short>:
  - snapshot://<ns>/<date>/<short>.csv
data://garden/<ns>/<date>/<short>:
  - data://meadow/<ns>/<date>/<short>
data://grapher/<ns>/<date>/<short>:
  - data://garden/<ns>/<date>/<short>
export://explorers/<ns>/latest/<short>:
  - data://grapher/<ns>/<date>/<short>
```

### 8. Verify

Hand off to the user:
1. `.venv/bin/etls <ns>/<date>/<short>.csv` (snapshot — only if not already uploaded).
2. `.venv/bin/etlr export://explorers/<ns>/latest/<short>` — runs the chain end-to-end.
3. Open `http://staging-site-<branch>/explorers/<slug>` and spot-check.
4. `make check`.

## Follow-up

Once in ETL, port to MDIM-derived via `migrate-explorer-to-mdim`.
