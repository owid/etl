# Manually add a dataset to the ETL
## Get set up

Before you begin, make sure you've set up the ETL as described in [Getting Started](../../getting-started/index.md).

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

### Create a new branch in `etl`

```bash
git checkout -b data/new-dataset
```

### Create Snapshot step

#### Create an ingest snapshot ingest script

- Create a script in `snapshots/<namespace>/<version>/<dataset_short_name>.py`
- Create the corresponding metadata DVC file in `snapshots/<namespace>/<version>/<dataset_short_name>.<extension>.dvc`
- Run `make format && make test` to ensure that the step runs well and is well formatted.

#### Add snapshot data

```bash
poetry run python snapshots/<namespace>/<version>/<dataset_short_name>.py
```

### Create Meadow step

#### Create a new `meadow` step file

- Path of the step should be similar to `etl/steps/data/meadow/<namespace>/<version>/<dataset_short_name>.py`.
- The step must contain a `run(dest_dir)` function that loads data from the `snapshot` and creates a dataset
(a `catalog.Dataset` object) with one or more tables (`catalog.Table` objects) containing the raw data.
- Run `make format && make test` to ensure that the step runs well and is well formatted.

#### Add the new meadow step to the dag, including its dependencies.
Add the dependencies for the dataset to the appropriate dag file.

#### Run the meadow step

```
poetry run etl data://meadow/<namespace>/<version>/<dataset_short_name>
```

### Create Garden step

#### Create a new `Garden` step file

- Path of the step should be similar to `etl/steps/data/garden/<namespace>/<version>/<dataset_short_name>.py`.
- The step must contain a `run(dest_dir)` function that loads data from the last `meadow` step, processes the data and
creates a dataset with one or more tables and the necessary metadata.
- Country names must be harmonized (for which the [harmonize](../architecture/workflow/harmonization.md) tool of `etl` can be used).
- Add plenty of assertions and sanity checks to the step (if possible, compare the data with its previous version and
check for abrupt changes).
- Run `make format && make test` to ensure that the step runs well and is well formatted.

#### Add the new garden step to the dag, including its dependencies.
Add the dependencies for the dataset to the appropriate dag file.

#### Run the garden step

```
poetry run etl data://garden/<namespace>/<version>/<dataset_short_name>
```

### Create Grapher step

#### Create a new grapher step
- Path of the step should be similar to `etl/steps/data/grapher/<namespace>/<version>/<dataset_short_name>.py`.
- The step must contain a `run(dest_dir)` function that loads data from the last `garden` step, processes the data and
creates a dataset with one or more tables and the necessary metadata.
- Run `make format && make test` to ensure that the step runs well and is well formatted.


#### Run the grapher step
```
poetry run etl data://grapher/<namespace>/<version>/<dataset_short_name>
```

Add `--grapher` flags to `etl` command to upsert data into grapher database.

```
poetry run etl data://grapher/<namespace>/<version>/<dataset_short_name> --grapher
```

To test the step, you can run it on the grapher `staging` database, or using
[a local grapher](https://github.com/owid/owid-grapher/blob/master/docs/docker-compose-mysql.md).

!!! warning "The `grapher` step to import the dataset to Grapher is now automatic"
    We have automatic deploys to grapher database from ETL. This means that whenever we push to master, `etl --grapher` is automatically run and pushes your data to MySQL. This means:

    - **You don't have to manually push to grapher**. Just merge and wait for CI status on master to turn green.
    - You can still manually push new datasets (a new dataset doesn't have recipe in master yet). This is useful if you want to explore it in grapher, get feedback, iterate on a PR, etc. However, if you manually deploy an existing dataset, it'll be overwritten by the version in master

    Automatic deploys will run on both production and staging. This process is not final, we are still iterating.

#### Create a pull request to merge the new branch with the master branch in `etl`.
At this point, some further editing of the step files may be required before merging the branch with master.
