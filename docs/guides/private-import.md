---
tags:
  - 👷 Staff
---

While most of the data at OWID is publicly available, some datasets are added to our catalogue with some restrictions. These include datasets that are not redistributable, or that are not meant to be shared with the public. This can happen due to a strict license by the data provider, or because the data is still in a draft stage and not ready for public consumption.

Various privacy configurations are available:

- Skip re-publishing to GitHub.
- Disable data downloading options on Grapher.
- Disable public access to the original file (snapshot).
- Hide the dataset from our public catalog (accessible vua `owid-catalog-py`).

In the following, we explain how to create private steps in the ETL pipeline and how to run them.

## Create a private step

### Snapshot

To create a private snapshot step, set the `meta.is_public` property in the snapshot .dvc file to false:

```yaml
meta:
  is_public: false

  origin:
    # Data product / Snapshot
    title: World Population Prospects
    ...
```

This will prevent the file to be publicly accessible without the appropriate credentials.

### Meadow, Garden, Grapher

Creating a private data step means that the data is not listed in the public catalog. Also, private datasets will not be re-published to GitHub.

To create a private data step (meadow, garden or grapher) simply use `data-private` prefix in the step name in the DAG. For example, the step `grapher/ihme_gbd/2024-06-10/leading_causes_deaths` (this is from [health.yml](https://github.com/owid/etl/blob/master/dag/health.yml)) is private:

```yaml
# IHME GBD Leading cause of  deaths - update
data-private://meadow/ihme_gbd/2024-06-10/cause_hierarchy:
  - snapshot-private://ihme_gbd/2024-06-10/cause_hierarchy.csv
data-private://garden/ihme_gbd/2024-06-10/leading_causes_deaths:
  - data-private://garden/ihme_gbd/2024-05-20/gbd_cause
  - data-private://meadow/ihme_gbd/2024-06-10/cause_hierarchy
data-private://grapher/ihme_gbd/2024-06-10/leading_causes_deaths:
  - data-private://garden/ihme_gbd/2024-06-10/leading_causes_deaths
```

!!! note "Make the data non-downloadable"

    To make the data non-downloadable on Grapher, set the `non_redistributable` property in the dataset metadata to `true`:

    ```yaml
    dataset:
      non_redistributable: true
    ```

## Running private ETL

To run a private step, you need to use the `--private` flag. Otherwise, private steps are not detected by `etl` command:

```
etl run run [step-name] --private
```

## Bringing private data to public

If you want to make a private step public simply follow the steps below:

- **In the DAG:** Replace `data-private/` prefix with `data/`.
- **In the snapshot DVC file**: Set `meta.is_public` to `true` (or simply remove `is_public` property).
- (Optional) **Allow for Grapher downloads**: Set `dataset.non_redistributable` to `false` in the dataset garden metadata (or simply remove the property from the metadata).

After this, re-run the snapshot step and commit your changes.
