# Owl

Owl is a lightweight pipeline runner for small OWID datasets. It lives inside the ETL repo, but keeps a simple authoring model: one step folder, one Python file, optional metadata, and catalog-compatible output.

Owl is experimental, but useful for demonstrating a faster path from a raw source to a first-class catalog dataset.

## Why Owl?

Standard ETL is powerful, but creating a small dataset often means touching several files across snapshot, meadow, garden, grapher, and DAG configuration. Owl is meant for cases where that overhead is not worth it yet.

Owl keeps the important parts:

- explicit raw snapshots;
- reproducible dataset functions;
- metadata next to the code;
- outputs written as normal `owid.catalog` datasets under `data/garden/`;
- optional links to existing ETL datasets, such as regions.

And removes the early ceremony:

- no DAG edit for a simple local run;
- no separate snapshot/meadow/garden files for tiny datasets;
- a scaffold command creates a runnable example.

## Try it in 5 minutes

From the ETL repo on this branch:

```bash
.venv/bin/owl new demo/my_dataset
.venv/bin/owl snapshot demo/my_dataset
.venv/bin/owl run demo/my_dataset
```

This creates:

```txt
owl_steps/demo/my_dataset/vYYYYMMDD/
  step.py
  meta.yml
  snapshot.lock.yml      # after owl snapshot
```

and writes a catalog dataset to:

```txt
data/garden/demo/YYYY-MM-DD/my_dataset/
```

Run it again:

```bash
.venv/bin/owl run demo/my_dataset
```

If nothing changed, Owl reports the dataset as up to date.

## Project structure

```txt
lib/owl/                         # Owl framework package
owl_steps/                       # Owl step code
  <namespace>/
    <dataset>/
      vYYYYMMDD/
        step.py                  # Snapshots, datasets, actions
        meta.yml                 # Snapshot + dataset metadata
        snapshot.lock.yml        # Content-addressed snapshot lock
data/
  snapshots/by-md5/              # Local raw snapshot cache
  garden/<namespace>/YYYY-MM-DD/<dataset>/
```

The `vYYYYMMDD` folder is translated to the normal ETL dataset version `YYYY-MM-DD`.

## Creating a new step

```bash
.venv/bin/owl new biodiversity/cherry_blossom --date 2026-04-16
```

The generated `step.py` is intentionally small:

```python
import pandas as pd

from owl import ColumnMeta, Dataset, DatasetMeta, Snapshot, SnapshotCapture


@Snapshot
def raw_data(snap: SnapshotCapture) -> pd.DataFrame:
    # Replace with snap.download("https://example.com/data.csv") and return None, or return a DataFrame.
    return pd.DataFrame({"country": ["World"], "year": [2026], "value": [1]})


@Dataset
def my_dataset(raw_data: Snapshot) -> tuple[pd.DataFrame, DatasetMeta]:
    df = raw_data.load()
    return df, DatasetMeta(
        title="TODO: dataset title",
        description="TODO: dataset description",
        columns={
            "country": ColumnMeta(title="Country", role="entity"),
            "year": ColumnMeta(title="Year", role="time"),
            "value": ColumnMeta(title="Value", unit="TODO"),
        },
    )
```

Replace the snapshot body with one of:

```python
snap.download("https://example.com/data.csv")
```

or:

```python
return pd.DataFrame(...)
```

Then run:

```bash
.venv/bin/owl snapshot biodiversity/cherry_blossom
.venv/bin/owl run biodiversity/cherry_blossom
```

## Snapshots

A `@Snapshot` captures one raw file. It can:

- download a URL with `snap.download(...)`;
- register a file with `snap.add(...)`;
- write bytes/text with `snap.write_bytes(...)` or `snap.write_text(...)`;
- return a pandas DataFrame, which Owl stores as parquet.

`owl snapshot` writes the file into the local content-addressed cache:

```txt
data/snapshots/by-md5/<md5><suffix>
```

and records the content identity in `snapshot.lock.yml`.

Keep both files under version control, but treat them differently:

- `meta.yml` is human-authored metadata: source descriptions, licenses, dataset and column metadata.
- `snapshot.lock.yml` is generated snapshot identity: exact captured bytes (`md5`, `size`, `suffix`, `captured_at`).

Keeping the generated lock separate from authored metadata makes reviews cleaner: data changes show up in the lock file, while copy/metadata changes stay in `meta.yml`.

### Snapshot lock behavior

`snapshot.lock.yml` is a content lock, not a fetch log. If you run `owl snapshot` again and the bytes are unchanged, Owl leaves the lock file untouched and reports:

```txt
📦 unchanged raw_data: <md5>.csv (... bytes)
```

The lock only changes when the snapshot identity changes: `md5`, `size`, or `suffix`.

## Datasets

A `@Dataset` function returns either:

```python
df
```

or:

```python
df, DatasetMeta(...)
```

Owl writes the result using `owid.catalog`, so the output is a normal catalog dataset:

```txt
data/garden/<namespace>/<version>/<dataset>/
```

Dataset functions can depend on snapshots or other Owl datasets by naming them as parameters:

```python
@Dataset
def my_dataset(raw_data: Snapshot) -> pd.DataFrame:
    df = raw_data.read_csv()
    return df
```

## Loading existing ETL datasets

Use `ETLDataset` for read-only dependencies on datasets produced by the main ETL pipeline:

```python
import pandas as pd

from owl import Dataset, ETLDataset

regions = ETLDataset("data://garden/regions/2023-01-01/regions")

@Dataset
def my_dataset(regions: ETLDataset) -> pd.DataFrame:
    tb_regions = regions.read("regions")
    ...
```

The shorter form defaults to the `garden` channel:

```python
regions = ETLDataset("regions/2023-01-01/regions")
```

## Actions and Grapher

Owl also has side-effect actions. The current main use is uploading a produced dataset to Grapher.

```python
from owl import Action, Dataset
from owl.grapher import upsert_dataset


@Action(kind="grapher", default=False)
def upsert_to_grapher(my_dataset: Dataset) -> None:
    upsert_dataset(my_dataset)
```

Run default actions with `owl run`, or Grapher actions explicitly with:

```bash
.venv/bin/owl run namespace/dataset --grapher
```

## Useful commands

```bash
# Create a runnable scaffold
.venv/bin/owl new namespace/dataset

# Fetch/update raw snapshots
.venv/bin/owl snapshot namespace/dataset

# Build catalog datasets
.venv/bin/owl run namespace/dataset

# Force a rebuild even if up to date
.venv/bin/owl run namespace/dataset --force

# Run Grapher actions
.venv/bin/owl run namespace/dataset --grapher

# Run all Owl steps
.venv/bin/owl run
```

The `namespace/dataset` argument is a regex pattern, so partial matches work too.

## Current limitations

Owl is not a replacement for the full ETL pipeline yet.

Known limitations:

- experimental API;
- limited metadata validation;
- no full ETL DAG integration;
- snapshot support is intentionally single-file per snapshot;
- Grapher upload support exists, but is still basic;
- staleness checks are simple and based mostly on source/metadata/dependency mtimes.

For mature, high-traffic, multi-stage datasets, standard ETL is still the safer default. For small datasets, prototypes, demos, and agent-assisted dataset creation, Owl should be much faster to read and write.
