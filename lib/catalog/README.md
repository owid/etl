[![Build status](https://badge.buildkite.com/66cc67fc572120ca97b9ffff288d5d73cb33e019dd70323053.svg)](https://buildkite.com/our-world-in-data/owid-catalog-unit-tests)
[![PyPI version](https://badge.fury.io/py/owid-catalog.svg)](https://badge.fury.io/py/owid-catalog)
![](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue.svg)

# owid-catalog

_A Pythonic API for working with OWID's data catalog._

Status: experimental, APIs likely to change

## Overview

Our World in Data is building a new data catalog, with the goal of our datasets being reproducible and transparent to the general public. That project is our [etl](https://github.com/owid/etl), which going forward will contain the recipes for all the datasets we republish.

This library allows you to query our data catalog programmatically, and get back data in the form of Pandas data frames, perfect for data pipelines or Jupyter notebook explorations.

```mermaid
graph TB

etl -->|reads| walden[upstream datasets]
etl -->|generates| s3[data catalog]
catalog[owid-catalog-py] -->|queries| s3
```

We would love feedback on how we can make this library and overall data catalog better. Feel free to send us an email at info@ourworldindata.org, or start a [discussion](https://github.com/owid/etl/discussions) on Github.

## Quickstart

Install with `pip install owid-catalog`. Then you can begin exploring the experimental data catalog:

```python
from owid import catalog

# look for Covid-19 data, return a data frame of matches
catalog.find('covid')

# load Covid-19 data from the Our World in Data namespace as a data frame
df = catalog.find('covid', namespace='owid').load()

# load data from other than the default `garden` channel
lung_cancer_tables = catalog.find('lung_cancer_deaths_per_100000_men', channels=['open_numbers'])
df = lung_cancer_tables.iloc[0].load()
```

## Development

You need Python 3.8+, `poetry` and `make` installed. Clone the repo, then you can simply run:

```
# run all unit tests and CI checks
make test

# watch for changes, then run all checks
make watch
```

## Data types

### Catalog

A catalog is an arbitrarily deep folder structure containing datasets inside. It can be local on disk, or remote.

#### Load the remote catalog

```python
# find the default OWID catalog and fetch the catalog index over HTTPS
cat = RemoteCatalog()

# get a list of matching tables in different datasets
matches = cat.find('population')

# fetch a data frame for a specific match over HTTPS
t = cat.find_one('population', namespace='gapminder')

# load other channels than `garden`
cat = RemoteCatalog(channels=('garden', 'meadow', 'open_numbers'))
```

### Datasets

A dataset is a folder of tables containing metadata about the overall collection.

- Metadata about the dataset lives in `index.json`
- All tables in the folder must share a common format (CSV or Feather)

#### Create a new dataset

```python
# make a folder and an empty index.json file
ds = Dataset.create('/tmp/my_data')
```

```python
# choose CSV instead of feather for files
ds = Dataset.create('/tmp/my_data', format='csv')
```

#### Add a table to a dataset

```python
# serialize a table using the table's name and the dataset's default format (feather)
# (e.g. /tmp/my_data/my_table.feather)
ds.add(table)
```

#### Remove a table from a dataset

```python
ds.remove('table_name')
```

#### Access a table

```python
# load a table including metadata into memory
t = ds['my_table']
```

#### List tables

```python
# the length is the number of datasets discovered on disk
assert len(ds) > 0
```

```python
# iterate over the tables discovered on disk
for table in ds:
    do_something(table)
```

#### Add metadata

```python
# you need to manually save your changes
ds.title = "Very Important Dataset"
ds.description = "This dataset is a composite of blah blah blah..."
ds.save()
```

#### Copy a dataset

```python
# copying a dataset copies all its files to a new location
ds_new = ds.copy('/tmp/new_data_path')

# copying a dataset is identical to copying its folder, so this works too
shutil.copytree('/tmp/old_data', '/tmp/new_data_path')
ds_new = Dataset('/tmp/new_data_path')
```

### Tables

Tables are essentially pandas DataFrames but with metadata. All operations on them occur in-memory, except for loading from and saving to disk. On disk, they are represented by tabular file (feather or CSV) and a JSON metadata file.

Columns of `Table` have attribute `VariableMeta`, including their type, description, and unit. Be carful when manipulating them, not all operations are currently supported. Supported are: adding a column, renaming columns. Not supported: direct assignment to `t.columns = ...` or to index names `t.columns.index = ...`.

#### Make a new table

```python
# same API as DataFrames
t = Table({
    'gdp': [1, 2, 3],
    'country': ['AU', 'SE', 'CH']
}).set_index('country')
```

#### Add metadata about the whole table

```python
t.title = 'Very important data'
```

#### Add metadata about a field

```python
t.gdp.description = 'GDP measured in 2011 international $'
t.sources = [
    Source(title='World Bank', url='https://www.worldbank.org/en/home')
]
```

#### Add metadata about all fields at once

```python
# sources and licenses are actually stored a the field level
t.sources = [
    Source(title='World Bank', url='https://www.worldbank.org/en/home')
]
t.licenses = [
    License('CC-BY-SA-4.0', url='https://creativecommons.org/licenses/by-nc/4.0/')
]
```

#### Save a table to disk

```python
# save to /tmp/my_table.feather + /tmp/my_table.meta.json
t.to_feather('/tmp/my_table.feather')

# save to /tmp/my_table.csv + /tmp/my_table.meta.json
t.to_csv('/tmp/my_table.csv')
```

#### Load a table from disk

These work like normal pandas DataFrames, but if there is also a `my_table.meta.json` file, then metadata will also get read. Otherwise it will be assumed that the data has no metadata:

```python
t = Table.read_feather('/tmp/my_table.feather')

t = Table.read_csv('/tmp/my_table.csv')
```

## Changelog

- `dev`
- `v0.3.8`
  - Switch from isort & black & fake8 to ruff
- `v0.3.8`
  - Pin dataclasses-json==0.5.8 to fix error with python3.9
- `v0.3.7`
  - Fix bugs.
  - Improve metadata propagation.
  - Improve metadata YAML file handling, to have common definitions.
  - Remove `DatasetMeta.origins`.
- `v0.3.6`
  - Fixed tons of bugs
  - `processing.py` module with pandas-like functions that propagate metadata
  - Support for Dynamic YAML files
  - Support for R2 alongside S3
- `v0.3.5`
  - Remove `catalog.frames`; use `owid-repack` package instead
  - Relax dependency constraints
  - Add optional `channel` argument to `DatasetMeta`
  - Stop supporting metadata in Parquet format, load JSON sidecar instead
  - Fix errors when creating new Table columns
- `v0.3.4`
  - Bump `pyarrow` dependency to enable Python 3.11 support
- `v0.3.3`
  - Add more arguments to `Table.__init__` that are often used in ETL
  - Add `Dataset.update_metadata` function for updating metadata from YAML file
  - Python 3.11 support via update of `pyarrow` dependency
- `v0.3.2`
  - Fix a bug in `Catalog.__getitem__()`
  - Replace `mypy` type checker by `pyright`
- `v0.3.1`
  - Sort imports with `isort`
  - Change black line length to 120
  - Add `grapher` channel
  - Support path-based indexing into catalogs
- `v0.3.0`
  - Update `OWID_CATALOG_VERSION` to 3
  - Support multiple formats per table
  - Support reading and writing `parquet` files with embedded metadata
  - Optional `repack` argument when adding tables to dataset
  - Underscore `|`
  - Get `version` field from `DatasetMeta` init
  - Resolve collisions of `underscore_table` function
  - Convert `version` to `str` and load json `dimensions`
- `v0.2.9`
  - Allow multiple channels in `catalog.find` function
- `v0.2.8`
  - Update `OWID_CATALOG_VERSION` to 2
- `v0.2.7`
  - Split datasets into channels (`garden`, `meadow`, `open_numbers`, ...) and make garden default one
  - Add `.find_latest` method to Catalog
- `v0.2.6`
  - Add flag `is_public` for public/private datasets
  - Enforce snake_case for table, dataset and variable short names
  - Add fields `published_by` and `published_at` to Source
  - Added a list of supported and unsupported operations on columns
  - Updated `pyarrow`
- `v0.2.5`
  - Fix ability to load remote CSV tables
- `v0.2.4`
  - Update the default catalog URL to use a CDN
- `v0.2.3`
  - Fix methods for finding and loading data from a `LocalCatalog`
- `v0.2.2`
  - Repack frames to compact dtypes on `Table.to_feather()`
- `v0.2.1`
  - Fix key typo used in version check
- `v0.2.0`
  - Copy dataset metadata into tables, to make tables more traceable
  - Add API versioning, and a requirement to update if your version of this library is too old
- `v0.1.1`
  - Add support for Python 3.8
- `v0.1.0`
  - Initial release, including searching and fetching data from a remote catalog
