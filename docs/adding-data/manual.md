# Manually add a dataset to the ETL

## Get set up

Before you begin, make sure you've set up the ETL as described in [Getting Started](../getting-started/index.md).

## Adding steps to the ETL

You will need to add a step to the ETL for each stage of data processing you need to do. The steps are:

```mermaid
graph LR

snapshot --> meadow --> garden --> grapher
```

## Decide on names

The ETL uses a naming convention to identify datasets. To add a dataset, you will need to choose a short name for the data provider (e.g. `un`, `who`), which will serve as the namespace to add it to.

You will also need to choose a short name for the dataset itself (e.g. `population`), which will be used to identify the dataset in the ETL.

!!! tip "What's a short name?"

    Short names must be unique within a namespace. They must be in lowercase and separated only with underscores. They must not contain any special characters, and should not be too long.

    - ✓ `population`
    - ✓ `electricity_demand`
    - ✗ `Electricity Demand`
    - ✗ `electricity-demand`
    - ✗ `really_long_elaborate_description_of_the_variable_in_question`

## Add ETL steps

!!! warning

    These instructions use `walden`, our old system for snapshotting data. They need to be updated to use `snapshots` instead.

0. Activate the virtual environment (running `poetry shell`).

1. **Create a new branch in the `walden` submodule**.

2. **Create an ingest script** (e.g. `etl/vendor/walden/ingests/example_institution.py`) to download the data from its
    original source and upload it as a new data snapshot into the S3 `walden` bucket.

    This step can also be done manually (although it is preferable to do it via script, to have a record of how the data was
    obtained, and to be able to repeat the process in the future, for instance if another version of the data is released).
    
    Keep in mind that, if there is additional metadata, it should also be ingested into `walden` as part of the snapshot.

    If the data is in a single file for which you have a download link, this script may not be required: you can add
    this link directly in the index file (see next point). There is guidance on how to upload to `walden` manually in the [`walden` README](https://github.com/owid/walden#manually).

3. **Create an index file** `etl/vendor/walden/index/example_institution/YYYY-MM-DD/example_dataset.json` for the new
   dataset.
   You can simply copy another existing index file and adapt its content.
   This can be done manually, or, alternatively, the ingest script can also write one (or multiple) index files.

4. **Run `make test` in `walden`** and make changes to ensure the new files in the repository have the right style and structure.

5. **Create a pull request** to merge the new branch with the master branch in `walden`. When getting started with the `etl` you should request a code review from a more experienced `etl` user.

6. **Create a new branch in `etl`**.

7. **Create a new `meadow` step file** (e.g. `etl/steps/data/meadow/example_provider/YYYY-MM-DD/example_dataset.py`).
    The step must contain a `run(dest_dir)` function that loads data from the `walden` bucket in S3 and creates a dataset
    (a `catalog.Dataset` object) with one or more tables (`catalog.Table` objects) containing the raw data.

    Keep in mind that both the dataset and its table(s) should contain metadata. Additionally, all of the column names must be snake case before uploading to `meadow`. There is a function in the `owid.catalog.utils` module that will do this for you: `tb = underscore_table(Table(full_df))`.

8. **Add the new meadow step to the dag**, including its dependencies.

9. **Run `make test` in `etl`** and ensure the step runs well. To run the step: `etl data://meadow/example_institution/YYYY-MM-DD/example_dataset`

10. **Create a new garden step** (e.g. `etl/etl/steps/data/garden/example_institution/YYYY-MM-DD/example_dataset.py`).
    The step must contain a `run(dest_dir)` function that loads data from the last `meadow` step, processes the data and
    creates a dataset with one or more tables and the necessary metadata.
    Country names must be harmonized (for which the `harmonize` tool of `etl` can be used).
    Add plenty of assertions and sanity checks to the step (if possible, compare the data with its previous version and
    check for abrupt changes).

11. **Add the new garden step to the dag**, including its dependencies.

12. **Run `make test` in `etl`** and ensure the step runs well.

13. **Create a new grapher step** (e.g. `etl/etl/steps/data/grapher/example_institution/YYYY-MM-DD/example_dataset.py`).
    The step must contain a `run(dest_dir)` function that loads data from the last `garden` step, processes the data and
    creates a dataset with one or more tables and the necessary metadata.
    Add `--grapher` flags to `etl` command to upsert data into grapher database.
    To test the step, you can run it on the grapher `staging` database, or using
    [a local grapher](https://github.com/owid/owid-grapher/blob/master/docs/docker-compose-mysql.md).

14. **Create a pull request** to merge the new branch with the master branch in `etl`.
    At this point, some further editing of the step files may be required before merging the branch with master.