# Owl

A lightweight pipeline runner that can live alongside OWID's main ETL package while writing first-class ETL artifacts.

## Project structure

```txt
lib/owl/                         # Owl framework package
owl_steps/                       # Owl step code
  <namespace>/
    <dataset>/
      vYYYYMMDD/
        step.py                  # Snapshots, datasets, actions
        meta.yml                 # Snapshot + dataset metadata
data/
  snapshots/<namespace>/<snapshot-version>/
  garden/<namespace>/YYYY-MM-DD/<dataset>/
```

The `vYYYYMMDD` folder is translated to the normal ETL dataset version `YYYY-MM-DD`.

## Quick start

```bash
owl snapshot biodiversity/cherry_blossom
owl run biodiversity/cherry_blossom
```

## Step example

```python
from owl import Dataset, Snapshot

@Snapshot(version="2024-01-25")
def raw_data():
    return fetch_dataframe()

@Dataset
def my_dataset(raw_data: Snapshot):
    df = raw_data.load()
    return df
```

`@Snapshot` writes versioned raw inputs under `data/snapshots/`. `@Dataset` writes catalog datasets under `data/garden/` using the same `owid.catalog` dataset format, so existing tooling such as ETL publish can reuse the outputs.
