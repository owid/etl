---
name: migrate-explorer-csv
description: Migrate a non-ETL CSV-based explorer (data lives in a static CSV hosted in `owid-grapher/public/explorers/` or on GitHub; explorer config has `tableSlug`/`table` blocks) into ETL by adding a snapshot → meadow → garden → grapher → export chain. Trigger when the user says "bring CSV explorer <slug> into ETL", or refers to fish-stocks (the only remaining target).
metadata:
  internal: true
---

# Migrate CSV-based Explorer into ETL

Bring an explorer that today reads from a static CSV (e.g. hosted under `owid-grapher/public/explorers/<slug>/...` or a GitHub raw URL) into ETL. After this skill, the data is snapshot-tracked and flows through the standard ETL stages, and the explorer is published via `export://explorers/<ns>/latest/<short>`.

## Inputs

Required:
- `<slug>` — explorer slug (matches the `slug` column in MySQL `explorers`)
- CSV source URL or repo path
- `<ns>` — target namespace under `snapshots/` and `etl/steps/data/`
- `<short>` — short name for all stages (Python files use snake_case; explorer slug keeps hyphens)

## Step 0 — Confirm it really is CSV-based

CSV-based explorers have `table` and `columns` blocks alongside the `graphers` block, with a `tableSlug` column in the `graphers` table. Quick check:

```bash
cat <<'EOF' > /tmp/classify_explorer.py
from etl.config import OWID_ENV
import json, sys
slug = sys.argv[1]
df = OWID_ENV.read_sql(f"SELECT config FROM explorers WHERE slug = '{slug}'")
cfg = json.loads(df["config"].iloc[0]) if isinstance(df["config"].iloc[0], str) else df["config"].iloc[0]
print("blocks:", [b.get("type") for b in cfg.get("blocks", [])])
EOF
.venv/bin/python /tmp/classify_explorer.py <slug>
```

Block types should include `table` and `tableSlug` should appear in the graphers columns. If you see only `graphers` (with `yVariableIds` or `grapherId`), use `migrate-explorer-grapher` instead. As of mid-2026 the only remaining CSV explorer is **`fish-stocks`** — most of the others (poverty/inequality variants, etc.) are slated for removal per umbrella issue #6014.

## Workflow

### 1. Locate the source CSV

Look first in `owid-grapher/public/explorers/<slug>/...` (the live site path). Otherwise fetch from the URL the explorer points at. Record:

- exact URL (becomes `url_main` / `url_download` in the snapshot DVC)
- date last modified — if the CSV is in `owid-grapher`, use `git log -1 --format=%ad path/to/csv` to find the latest update; format as `YYYY-MM-DD` for the snapshot version
- citation / license / source metadata (often in a sibling README or in the explorer's settings rows)

### 2. Create the snapshot

Use the `create-snapshot` skill (or hand-write `snapshots/<ns>/<date>/<short>.csv.dvc` plus the matching script). The DVC file should record `url_main`, `url_download`, `date_published`, license, citation. Run `.venv/bin/etls <ns>/<date>/<short>.csv` to fetch and upload.

### 3. Meadow step

`etl/steps/data/meadow/<ns>/<date>/<short>.py`:

```python
from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot()
    tb = snap.read_csv()
    # Minimal cleaning here; let garden do harmonisation.
    tb = tb.format(short_name=paths.short_name)
    ds = paths.create_dataset(tables=[tb])
    ds.save()
```

### 4. Garden step

`etl/steps/data/garden/<ns>/<date>/<short>.py`:
- harmonize country/region names via `paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)`
- convert types and units to ETL conventions (long format with `country`/`year` index)
- optionally compute per-capita or per-region aggregates
- write `*.meta.yml` with origins, descriptions, per-indicator metadata

### 5. Grapher step

`etl/steps/data/grapher/<ns>/<date>/<short>.py` — read the garden table and save as a grapher dataset (long format `country`/`year`/`value`).

### 6. Explorer step — leverage the migration helper

Before hand-translating the legacy config, try the programmatic helper in `etl/collection/explorer/migration.py`:

```python
# In a notebook or one-off script
from etl.collection.explorer.migration import migrate_csv_explorer
config = migrate_csv_explorer("<slug>")  # reads from MySQL
import yaml
print(yaml.safe_dump(config, sort_keys=False))
```

`migrate_csv_explorer` only handles CSV-flavored explorers (it raises if the explorer mixes types), and it returns a partial YAML config with `dimensions`, `views`, settings, and per-indicator display overrides already mapped from the TSV blocks. The catch: catalog paths in the output point at the **explorer table URI** in the DB, which usually isn't the grapher dataset path. You need to rewrite each `catalogPath` to match the grapher dataset created in step 5 (e.g. `<ns>/<date>/<short>/<table>#<short>`).

The shape it produces (paste into `<short>.config.yml`):

```yaml
config:
  explorerTitle: ...
  isPublished: true
  selection: [...]
  hasMapTab: true

dimensions:
  - slug: <snake>
    name: ...
    presentation: { type: dropdown }
    choices:
      - { slug: ..., name: ... }

views:
  - dimensions: { ... }
    indicators:
      y:
        - catalogPath: <REWRITE THIS to <ns>/<date>/<short>#<indicator>>
          display:
            colorScaleNumericBins: ...
    config:
      type: LineChart
      title: ...
```

`<short>.py`:

```python
"""Load grapher dataset and create an explorer tsv file."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    config = paths.load_collection_config()
    c = paths.create_collection(
        config=config,
        short_name="<short-with-hyphens>",  # explorer slug; hyphens, not underscores
        explorer=True,
    )
    c.save(tolerate_extra_indicators=True)
```

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
3. Diff the resulting TSV against `owid-grapher/explorers/<slug>.explorer.tsv`. Cosmetic differences acceptable; structural ones (missing views, swapped dimension orderings) are not.
4. Open `http://staging-site-<branch>/explorers/<slug>` and spot-check.
5. `make check`.

## Reference: existing migrations

There are no fully CSV-backed explorers in ETL today (fish-stocks is the lone remaining target). For the indicator-based pieces of the workflow, model after:

- `etl/steps/export/explorers/agriculture/latest/crop_yields.{py,config.yml}`
- `etl/steps/export/explorers/migration/latest/migration_flows.py`

## Follow-up

Once in ETL, the explorer is a candidate for the Track-B port to MDIM (`export://multidim/...`) once feature parity is reached. See umbrella issue #6014.
