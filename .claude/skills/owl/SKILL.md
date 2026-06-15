---
name: owl
description: Working with Owl — OWID's lightweight, single-folder pipeline runner for small datasets (an alternative to the full snapshot → meadow → garden → grapher chain). Use whenever the user mentions Owl, `owl_steps/`, the `owl` CLI (`owl new`/`run`/`snapshot`/`viz`), or wants to create, migrate, run, or debug a small dataset with Owl.
metadata:
  internal: true
---

# Owl

Owl is OWID's lightweight pipeline runner: one step folder (`owl_steps/<namespace>/<dataset>/vYYYYMMDD/`) holds the snapshot, transform, and metadata, and writes a normal `owid.catalog` dataset under `data/garden/`. It replaces the full snapshot → meadow → garden → grapher chain for **small** datasets.

## Read the README first

**The authoritative reference is `lib/owl/owl/README.md` — read it before doing anything with Owl.** It covers the commands, project layout, `@Snapshot`/`@Dataset`/`@Action` API, snapshot capture helpers, `meta.yml`, loading existing ETL datasets, the Grapher action, and current limitations. Do not duplicate it from memory — open it.

The framework is small and readable; for API details beyond the README, go to the source:

| File | What's in it |
|------|--------------|
| `lib/owl/owl/cli.py` | `owl new / run / snapshot / viz` commands and flags |
| `lib/owl/owl/snapshot.py` | `@Snapshot`, `SnapshotCapture` (`download`/`add`/`write_bytes`/`write_text`), lock behavior |
| `lib/owl/owl/dataset.py` | `@Dataset`, `@Action`, `ColumnMeta`, `DatasetMeta`, metadata merging, staleness |
| `lib/owl/owl/catalog.py` | `load_snapshot` (Table + origins), `export` |
| `lib/owl/owl/etl_dataset.py` | `ETLDataset` (read-only deps on main-ETL datasets) |

## When to use Owl vs. classic ETL

- **Owl** — small/single-source datasets, prototypes, demos, agent-assisted creation.
- **Classic ETL** (`/create-etl-steps`, `/update-dataset`) — mature, high-traffic, or multi-stage datasets that need real DAG integration. Owl is experimental, has limited metadata validation, and no DAG integration.

If a request doesn't clearly fit Owl, say so and recommend classic ETL instead of forcing it.

## Tasks

### Create a new step
`.venv/bin/owl new <namespace>/<dataset> [--date YYYY-MM-DD]`, then fill in `step.py` and `meta.yml`, then `owl snapshot` → `owl run`. Follow the README's "Creating a new step", "Snapshots", and "Datasets" sections.

### Migrate a classic ETL dataset to Owl
Collapse the snapshot → meadow → garden → grapher chain into one Owl step folder:
1. Scaffold the step (`owl new`, or hand-create `vYYYYMMDD/`), using a `--date` matching the source's access date.
2. Move the snapshot fetch into a `@Snapshot` (e.g. `snap.download(URL, suffix=".csv")`).
3. Collapse the meadow + garden transforms into one `@Dataset` — load with `load_snapshot(...)`, harmonize, `tb.format([...])`.
4. Port the garden `.meta.yml` into the Owl `meta.yml`: the `datasets.<name>` block accepts the **standard catalog shape** (`definitions`, `tables` → `variables`), so most metadata moves over verbatim. Snapshot origin goes under `snapshots.<name>.origin`.
5. If it was a grapher step, add an `@Action(kind="grapher", default=False)` that calls `upsert_dataset(...)`.
6. Remove the classic files (snapshot `.py`/`.dvc`, meadow, garden + meta, grapher) **and their active DAG entries**.
7. `owl snapshot` + `owl run`, then verify the output dataset matches the old one.

### Run / debug
`owl run <pattern>` (regex; rebuilds stale steps + upstream deps — no `--force` needed after edits), `owl run <pattern> --grapher` for Grapher actions, `owl run <pattern> --force` to rebuild regardless, `owl viz <pattern>` to render the dependency DAG. `owl snapshot` is the only way to fetch data — `run` never fetches.

## Recommended `step.py` pattern (real datasets)

For anything beyond a toy dataset, use the catalog-Table pattern (preserves origins/metadata properly), rather than the lightweight inline-`DatasetMeta` scaffold:

```python
from pathlib import Path

from owid.catalog import Table
from owl import Action, Dataset, Snapshot, SnapshotCapture
from owl.catalog import load_snapshot
from owl.grapher import upsert_dataset

from etl.data_helpers import geo

URL_DOWNLOAD = "https://example.com/data.csv"
COUNTRIES_FILE = Path(__file__).with_name("my_dataset.countries.json")


@Snapshot
def raw_data(snap: SnapshotCapture) -> None:
    snap.download(URL_DOWNLOAD, suffix=".csv")


@Dataset
def my_dataset(raw_data: Snapshot) -> Table:
    tb = load_snapshot(raw_data)                       # Table with origins from meta.yml `origin`
    tb = tb.rename(columns={"country_name": "country"})
    tb = geo.harmonize_countries(df=tb, countries_file=COUNTRIES_FILE)
    tb = tb.format(["country", "year"])
    return tb


@Action(kind="grapher", default=False)
def upsert_to_grapher(my_dataset: Dataset) -> None:
    upsert_dataset(my_dataset)
```

Dependencies wire **by parameter name** (a `raw_data` parameter resolves to the `@Snapshot`/`@Dataset` named `raw_data` in the module).

## Repo conventions that still apply inside `step.py`

- **Preserve metadata/origins** in transforms: no `np.where`, no `pd.concat`/`pd.to_numeric` (use `pr.*` from `owid.catalog.processing`), no `pd.DataFrame(tb)`. See the "Preserving metadata/origins" rules in `CLAUDE.md`.
- Run `make check` before committing. Don't commit, push, or open PRs unless explicitly told to.
