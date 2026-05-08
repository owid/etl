---
name: migrate-explorer-csv
description: Migrate a non-ETL CSV-based explorer (data lives in a static CSV hosted in `owid-grapher/public/explorers/` or on GitHub; explorer config has `tableSlug`/`table` blocks) into ETL by adding a snapshot → meadow → garden → grapher chain, then handing off to `/create-explorer` for the export step. Trigger when the user says "bring CSV explorer <slug> into ETL", or refers to fish-stocks (the only remaining target).
metadata:
  internal: true
---

# Migrate CSV-based Explorer into ETL

Bring an explorer that today reads from a static CSV (e.g. hosted under `owid-grapher/public/explorers/<slug>/...` or a GitHub raw URL) into ETL. After this skill, the data is snapshot-tracked and flows through standard ETL stages, and `/create-explorer` writes the final `export://explorers/<ns>/latest/<short>` step.

> **Scope of this skill:** the data-pipeline half — locating the source CSV and producing the `snapshot → meadow → garden → grapher` chain. The export-step authoring (YAML schema, dimensions/views, full-YAML vs table-driven, FAUST upstream, post-processing, DAG, verification) lives in `/create-explorer` and is shared with the other migrate-explorer-* skills and with `/create-multidim`. Don't duplicate that content here.

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

## Step 1 — Locate the source CSV

Look first in `owid-grapher/public/explorers/<slug>/...` (the live site path). Otherwise fetch from the URL the explorer points at. Record:

- exact URL (becomes `url_main` / `url_download` in the snapshot DVC)
- date last modified — if the CSV is in `owid-grapher`, use `git log -1 --format=%ad path/to/csv` to find the latest update; format as `YYYY-MM-DD` for the snapshot version
- citation / license / source metadata (often in a sibling README or in the explorer's settings rows)

## Step 2 — Create the snapshot

Use the `create-snapshot` skill (or hand-write `snapshots/<ns>/<date>/<short>.csv.dvc` plus the matching script). The DVC file should record `url_main`, `url_download`, `date_published`, license, citation. Run `.venv/bin/etls <ns>/<date>/<short>.csv` to fetch and upload.

## Step 3 — Meadow step

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

## Step 4 — Garden step

`etl/steps/data/garden/<ns>/<date>/<short>.py`:

- harmonize country/region names via `paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)` (skip if entities are non-geographic, e.g. food products)
- convert types and units to ETL conventions (long format with `country`/`year` index)
- optionally compute per-capita or per-region aggregates
- write `*.meta.yml` with origins, descriptions, per-indicator metadata

**FAUST hint.** While authoring indicator metadata, push chart text/config (`title_public`, `subtitle`, `note`, `map.colorScale`, `hasMapTab`, `tab`, `yAxis`, …) into each indicator's `presentation.{title_public, grapher_config}`. Cross-cutting baselines go under `definitions.common.presentation.grapher_config` (recursive merge). The explorer view inherits this text at chart render time, so the `<short>.config.yml` in the next skill stays minimal. See `/create-explorer` Step 5 for the full mechanics and the `dynamic-yaml` `"{definitions.<key>}"` interpolation pattern.

CSV-explorer migrations have a slight advantage: *you control the garden output*, so you can shape the new garden table to be dimensional (one row per country/year × dim_a × dim_b) and the export step in the next skill can use `paths.create_collection(tb=tb, indicator_names=..., dimensions=...)` to expand views directly.

## Step 5 — Grapher step

`etl/steps/data/grapher/<ns>/<date>/<short>.py` — read the garden table and save as a grapher dataset (long format `country`/`year`/`value`).

## Step 6 — Optional: pre-fill the explorer YAML from the legacy config

Before invoking `/create-explorer`, you can seed `<short>.config.yml` with a partial config extracted from the legacy MySQL explorer:

```python
# In a notebook or one-off script
from etl.collection.explorer.migration import migrate_csv_explorer
config = migrate_csv_explorer("<slug>")  # reads from MySQL
import yaml
print(yaml.safe_dump(config, sort_keys=False))
```

`migrate_csv_explorer` only handles CSV-flavored explorers (it raises if the explorer mixes types), and it returns a partial YAML config with `dimensions`, `views`, settings, and per-indicator display overrides already mapped from the TSV blocks. **The catch:** catalog paths in the output point at the **explorer table URI** in the DB, which usually isn't the grapher dataset path. Rewrite each `catalogPath` to match the grapher dataset created in step 5 (e.g. `<ns>/<date>/<short>/<table>#<short>`).

## Step 7 — Hand off to `/create-explorer`

Invoke `/create-explorer` to author `etl/steps/export/explorers/<ns>/latest/<short>.{py,config.yml}` and the DAG entry. That skill covers:

- the Python skeleton (full-YAML or table-driven)
- the YAML schema (`config:`, `definitions.common_views`, `dimensions:`, `views:`)
- block-style YAML rule, hyphens vs underscores
- DAG entry, verification, common pitfalls

If `/create-explorer` was already invoked once and you only need to refine the layout, you don't need it again — edit the YAML directly.

## Reference: existing migrations

There are no fully CSV-backed explorers in ETL today (fish-stocks is the lone remaining target). For the indicator-based pieces of the workflow that the export step will lean on, see the reference examples listed in `/create-explorer`.

## Follow-up

Once in ETL, the explorer is a candidate for the Track-B port to MDIM (`export://multidim/...`) once feature parity is reached. See umbrella issue #6014.
