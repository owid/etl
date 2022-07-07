![build status](https://github.com/owid/etl/actions/workflows/python-package.yml/badge.svg) ![](https://img.shields.io/badge/python-3.9-blue.svg)

# etl

_A compute graph for loading and transforming data for OWID._

**Status: work in progress**

## Overview

This project is the spiritual successor to [importers](https://github.com/owid/importers), meant to eventually replace it. Its job is to assemble and republish OWID's _data science catalog_, the richest and easiest to use version of the datasets we can create. The catalog it creates is also meant to eventually replace Grapher's database as our source of truth for all data.

## Getting started

#### Installations

You need to install the following:

* Python 3.9+. Guide for all platforms [here](https://realpython.com/installing-python/). If you are using multiple different versions of Python on your machine, you may want to use `pyenv` to manage them (instructions [here](https://github.com/pyenv/pyenv)).
* `poetry`, for managing dependencies, virtual envs, and packages. Installation guide [here](https://python-poetry.org/docs/#installation).
* `make`. You likely already have this, but otherwise it can be installed using your usual package manager (e.g. on Mac, `brew install make`).
* MYSQL client (and Python dev headers). If you don't have this already,
  * On Ubuntu: `sudo apt install python3.9-dev mysql-client`
  * On Mac: `brew install mysql-client`
* AWS CLI and you should have an `~/.aws/config` file configured so that you can upload to walden etc.


#### Creating and using the virtual environment

We use `poetry` to manage the virtual environment for the project, and you'll typically do your work within that virtual environment.

1. Run `poetry install`, which creates a virtual environment in `.venv` using `make`
2. Activate the virtual env with `poetry shell`

#### Example commands

To run all the checks and make sure you have everything set up correctly, try

```
make test
```
if `make test` fails report it in #data-architecture or #tech-issues. The `etl` is undergoing constant development so it may not be your local setup causing `make test` to fail and therefore shouldn't stop you progressing to the next step.


To run a subset of examples, you can try (for example)

```
poetry run etl covid
```

or

```
poetry run etl examples
```

These will generate files in `./data` directory according to their recipes in `./etl/steps` (with their dependencies defined in `dag.yml`).

Scripts in `./etl/steps/data/examples/examples/latest` showcase basic functionality and can help you get started (mainly `script_example.py`).

You can also build all known data tables into the `data/` folder with:

```
poetry run etl
```

However, processing all the datasets will take a long time and memory.

*Note*: `poetry run` runs commands from within the virtual environment. You can also activate it with `poetry shell` and then simply run `etl ...`.

#### Creating the pipeline of a new dataset

These are the steps to create a data pipeline for a dataset called `example_dataset`, from an institution called
`example_institution`, with version `YYYY-MM-DD` (where this date tag can typically be the current date when the dataset
is being added to `etl`, or the date when the source data was released or updated):

0. Activate the virtual environment (running `poetry shell`).
1. **Create a new branch in the `walden` submodule**.
2. **Create an ingest script** (e.g. `etl/vendor/walden/ingests/example_institution.py`) to download the data from its
original source and upload it as a new data snapshot into the S3 `walden` bucket.
This step can also be done manually (although it is preferable to do it via script, to have a record of how the data was
obtained, and to be able to repeat the process in the future, for instance if another version of the data is released).
Keep in mind that, if there is additional metadata, it should also be ingested into `walden` as part of the snapshot.
If the data is in a single file for which you have a download link, this script may not be required: you can add
this link directly in the index file (see next point).
3. **Create an index file** `etl/vendor/walden/index/example_institution/YYYY-MM-DD/example_dataset.json` for the new
dataset.
You can simply copy another existing index file and adapt its content.
This can be done manually, or, alternatively, the ingest script can also write one (or multiple) index files.
4. **Run `make test` in `walden`** and make changes to ensure the new files in the repository have the right style and structure.
5. **Create a pull request** to merge the new branch with the master branch in `walden`. When getting started with the `etl` you should request a code review from a more experienced `etl` user.
7. **Create a new branch in `etl`**.
8. **Create a new `meadow` step file** (e.g. `etl/etl/steps/data/meadow/example_institution/YYYY-MM-DD/example_dataset.py`).
The step must contain a `run(dest_dir)` function that loads data from the `walden` bucket in S3 and creates a dataset
(a `catalog.Dataset` object) with one or more tables (`catalog.Table` objects) containing the raw data.
Keep in mind that both the dataset and its table(s) should contain metadata. Additionally, all of the column names must be snake case before uploading to `meadow`. There is a function in the `owid.catalog.utils` module that will do this for you: `tb = underscore_table(Table(full_df))`.
8. **Add the new meadow step to the dag**, including its dependencies.
9. **Run `make test` in `etl`** and  ensure the step runs well.
10. **Create a new garden step** (e.g. `etl/etl/steps/data/garden/example_institution/YYYY-MM-DD/example_dataset.py`).
The step must contain a `run(dest_dir)` function that loads data from the last `meadow` step, processes the data and
creates a dataset with one or more tables and the necessary metadata.
Country names must be harmonized (for which the `harmonize` tool of `etl` can be used).
Add plenty of assertions and sanity checks to the step (if possible, compare the data with its previous version and
check for abrupt changes).
11. **Add the new garden step to the dag**, including its dependencies.
12. **Run `make test` in `etl`** and  ensure the step runs well.
13. **Create a new grapher step** (e.g. `etl/etl/steps/grapher/example_institution/YYYY-MM-DD/example_dataset.py`).
The step must contain a `get_grapher_dataset()` function and a `get_grapher_tables()` function.
To test the step, you can run it on the grapher `staging` database, or using
[a local grapher](https://github.com/owid/owid-grapher/blob/master/docs/docker-compose-mysql.md).
14. **Create a pull request** to merge the new branch with the master branch in `etl`.
At this point, some further editing of the step files may be required before merging the branch with master.

## Reporting problems

Please file any bugs or issues at https://github.com/owid/etl/issues. We are not currently seeking external contributions, but any member of the public should be able to run this codebase and recreate our catalog.

## Architecture

The `etl` project is the heart of OWID's future data architecture, containing all data transformations and publishing rules to take data from the raw snapshots kept in [walden](https://github.com/owid/walden) to the MySQL copy kept by [grapher](https://github.com/owid/grapher) for publishing the OWID charts and site.

![Architectural overview](docs/future-architecture.png)

The ETL is the place where several key steps can be done:

- **Syntactic harmonization**: get data from institutional formats into a common OWID format with as few changes as possible
- **Semantic harmonization**: harmonize dimension fields like country, region, gender, and others to create OWID's reference data set
- **Remixing**: generating combined datasets and indicators, e.g. taking population from one dataset and using it to transforming another indicator into a per-capita version
- **Republishing** (OWID only): export OWID's reference data set for a variety of consumers

## Design principles

- **Dependencies listed**: all transformation steps and their dependencies are collected in a single file `dag.yml`
- **URI style steps**: all steps have a URI style format, e.g. `data://garden/who/2021-07-01/gho`
- **Filenames by convention**: We use convention to reduce the amount of config required
  - `walden://<path>` steps match data snapshots in the [walden](https://github.com/owid/walden) index at `<path>`, and download the snapshots locally when executed
  - `data://<path>` steps are defined by a Python or Jupyter notebook script in `etl/steps/data/<path>.{py,ipynb}`, and generate a new dataset in the `data/<path>` folder when executed
- **Data stored outside Git**: unlike the importers repo, only tiny reference datasets and metadata is kept in git; all actual datasets are not stored and are instead regenerated on demand from the raw data in Walden

## Data formats

The core formats used are the `Dataset` and `Table` formats from [owid-catalog-py](https://github.com/owid/owid-catalog-py).

- Dataset: a folder full of data files (e.g. `my_dataset/`), with overall metadata in `index.json` (e.g. `my_dataset/index.json`)
- Table: a CSV or Feather file (e.g. `my_table.feather`) with table and variable metadata in a `.meta.json` file (e.g. `my_table.meta.json`)

Visit the `owid-catalog-py` project for more details on these formats or their Python API.

## Step types

### Walden (`walden://...`)

[Walden](https://github.com/owid/walden) is OWID's data store for bulk data snapshots. It consists of a data index stored in git, with the snapshots themselves stored in S3 and available over HTTPS.

Walden steps, when executed, find the matching data snapshot in Walden's index and ensure its files are downloaded, by calling `ensure_downloaded()` on it. Walden stores such locally cached files in `~/.owid/walden`.

### Data (`data://...`)

Each data step is defined by its output, it must create a new folder in `data/` containing a dataset at the matching path. The name also indicates where the script to run for this step lives.

For example, suppose we have a step `data://a/b/c`. Then:

- The step must create when run a dataset at `data/a/b/c`
- It must have a Python script at `etl/steps/data/a/b/c.py`, a module at `etl/steps/data/a/b/c/__init__.py`, or a Jupyter notebook at `etl/steps/data/a/b/c.ipynb`

Data steps can have any dependencies you like. The ETL system will make sure all dependencies are run before the script starts, but the script itself is responsible for finding and consuming those dependencies.

### Github (`github://...`)

An empty step used only to mark a dependency on a Github repo, and trigger a rebuild of later steps whenever that repo changes. This is useful since Github is a good store for data that updates too frequently to be snapshotted into Walden, e.g. OWID's [covid-19-data](https://github.com/owid/covid-19-data).

Example: `github://owid/covid-19-data/master`

The most recent commit hash of the given branch will be used to determine whether the data has been updated. This way, the ETL will be triggered to rebuild any downstream steps each time the data is changed.

NOTE: Github rate-limits unauthorized API requests to 60 per hour, so we should be sparing with the use of this step as it is implemented today.

### ETag (`etag://...`)

A step used to mark dependencies on HTTPS resources. The path is interpreted as an HTTPS url, and a HEAD request is made against the URL and checked for its `ETag`. This can be used to trigger a rebuild each time the resource changes.

Example: `etag://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv`

### Grapher (`grapher://...`)

A step to load a dataset into the grapher mysql database. Similar to `data` steps the path is interpreted as the path to a python script. The job of this script is to make the input dataset fit the constrained grapher data model where we only have the exact dimensions of year and entity id. The latter is the numeric id of the entity (usually the country) and the former can also be the number of days since a reference date.

The python script that does this re-fitting of the data for the grapher datamodel only has to reformat the data and ensure the metadata exists. Actually interfacing with the database is taken care of by the ETL library.

The script you supply has to have two functions, the first of which is `get_grapher_dataset` which should return a `owid.catalog.Dataset` instance. The `.metadata.namespace` field will be used to decide what namespace to upsert this data to (or whether to create this namespace if it does not exist) and the `.metadata.short_name` field will be used to decide what dataset to upsert to (again, this will be created if no dataset with this name in this namespace exists, otherwise the existing dataset will be upserted to). `.metadata.sources` will be used to upsert the entries for the sources table.

The other required function is `get_grapher_tables`. It is passed the Dataset instance created by the `get_grapher_dataset` function and should return an iterable of `owid.catalog.Table` instances that should be upserted as an entry in the variables table and then the data points in the data_values table. Every table that is yielded by this function has to adhere to the limited grapher data schema. This means that if you have e.g. a variable with dimensions `year`, `country` and `sex` with the latter having values [male, female, both] then to fit this into grapher we have to created 3 separate variables, one for every sex value. I.e. that this function would have to yield 3 Table instances with year, entityId and value.

See [the GHE example](./etl/steps/grapher/who/2021-07-01/ghe.py) as an example. If you need to look at the code implementing the actual database upserts (e.g. to look up metadata fields required) they can be found [here](./etl/grapher_import.py)

## Writing a new ETL step

Firstly, edit `dag.yml` and add a new step under the `steps` section. Your step name should look like `data://<path>`, meaning it will generate a dataset in a folder at `data/<path>`. You should list as dependencies any ingredients your dataset will need to build.

### Python step

To define a Python data step, create a new python module at `etl/steps/data/<path>.py`. Your module must define a `run()` method, here is the minimal one:

```python
from owid.catalog import Dataset

def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = 'my_dataset'
    ds.save()
```

You can use `make etl` to rebuild everything, including your new table. Or you can run:

`.venv/bin/etl data://<path>`

just to run your step alone.

### Jupyter notebook step

A Jupyter notebook step is nearly identical to the Python step above, but instead of a Python module you create a new notebook at `etl/steps/data/<path>.ipynb`.

Run `make lab` to start the Juypter Lab environment, then navigate to the folder you want, start a new notebook and rename it to match name in `<path>`.

Your notebook must contain a cell of parameters, containing `dest_dir`, like this:

```python
dest_dir = '/tmp/my_dataset'
```

If we tag this cell correctly, at runtime `dest_dir` will get filled in by the ETL system. To tag the cell, click on the cell, then the cog on the top right of the Jupyter interface. Add the tag `parameters` to the cell.

Add a second cell containing the minimal code to create a dataset:

```
from owid.catalog import Dataset

ds = Dataset.create_empty(dest_dir)
ds.metadata.short_name = 'my_dataset'
ds.save()
```

Then run `make etl` to check that it's working correctly before fleshing out your ETL step.

### Examples

Frequently used patterns are provided in the `etl/steps/data/examples` folder. You can run them with

```
etl examples --force
```

or use them as starting templates for your own steps.

## Harmonizing countries

An interactive country harmonizing tool is available, which can generate a country mapping file.

```
.venv/bin/harmonize <path/to/input.feather> <country-field> <path/to/output.mapping.json>
```

## Private steps

Most of the steps have private versions with `-private` suffix (e.g. `data-private://...`, `walden-private://...`) that remember and enforce a dataset level `is_public` flag.

When publishing, the index is public, but tables in the index that are private are only available over s3 with appropriate credentials. `owid-catalog-py` is also updated to support these private datasets and supports fetching them over s3 transparently.

### Uploading private data to walden

It is possible to upload private data to walden so that only those with S3 credentials would be able to download it.

```python
from owid.walden import Dataset

local_file = 'private_test.csv'
metadata = {
  'name': 'private_test',
  'short_name': 'private_test',
  'description': 'testing private walden data',
  'source_name': 'test',
  'url': 'test',
  'license_url': 'test',
  'date_accessed': '2022-02-07',
  'file_extension': 'csv',
  'namespace': 'private',
  'publication_year': 2021,
}

# upload the local file to Walden's cache
dataset = Dataset.copy_and_create(local_file, metadata)
# upload it as private file to S3
url = dataset.upload(public=False)
# update PUBLIC walden index with metadata
dataset.save()
```

## Running private ETL

`--private` flag rebuilds everything including private datasets

```
etl --private
reindex
publish --private
```

## Release process

CRON job is running on a server every 5 minutes looking for changes on master on the etl repo. If there are changes it will run `make publish` which is equivalent to running

```
1. `etl` (build/rebuild anything that’s missing/out of date)
2. `reindex` (generate a catalog index in `data/` for each channel)
3. `publish` (rsync the `data/` folder to an s3 bucket s3://owid-catalog/)

Then the s3 bucket has a CloudFlare proxy on top (https://catalog.ourworldindata.org/). If you use the [owid-catalog-py](https://github.com/owid/owid-catalog-py) project from Python and call `find()` or `find_one()` you will be doing HTTP requests against the static files in the catalog.

## Backporting

Datasets from our production grapher database can be backported to ETL catalog. The first step is getting them to Walden using

```
bulk_backport
```

(specify `--limit` to make it process only a subset of datasets). It goes through all public datasets with at least one variable used in a chart and uploads them to Walden catalog (or skip them if they're already there and have the same checksum). If you set `--skip-upload` flag, it will only persist the datasets locally. **You need S3 credentials to upload them to Walden.**

Note that you still have to commit those updates to [Walden index](https://github.com/owid/walden/tree/master/owid/walden/index), otherwise others won't be able to rerun the ETL. (If you don't commit them, running `etl` and `publish` steps will still work for you in your local repository, but not for others).

Backported walden datasets can be processed with ETL using

```
etl --backport
```

(or `etl backport --backport`). This will transform original datasets from long format to wide format, optimize their data types, convert metadata and add them to the catalog. Then you can run `publish` to publish the datasets as usual.


### Fasttrack

Fastrack is a service that polls grapher database for dataset updates and backports them as fast as possible. When it detects a change it will:

1. Run `backport` to get the backported dataset to **local** Walden catalog (without uploading to S3)
2. Run ETL on the backported dataset
3. Publish new ETL catalog to S3

All these steps have been optimized to run in a few seconds (except of huge datasets) and make them available through [data-api](https://github.com/owid/data-api).
