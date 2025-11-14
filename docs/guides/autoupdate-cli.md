---
tags:
  - 👷 Staff
icon: lucide/calendar-clock
---

# Automatic snapshot updates

Our ETL system includes an automatic update system that runs daily to update snapshots that are marked for automatic updating. This system identifies snapshots by looking for the `autoupdate`  metadata field in their `.dvc` files, runs the corresponding Python scripts to fetch the latest data, and creates pull requests when data has changed.

!!! info "It relies on [etl autoupdate](etl-cli/#etl-autoupdate){ data-preview } command"

It is then up to you to review, optionally edit and merge that PR. Use `chart-diff` and enable **Show all charts** in Options to check all affected charts. `data-diff` can also be helpful. Once you merge the PR, autoupdate will automatically create a new PR when there's new data available.

## Overview

The automatic update process:

1. Runs daily via a scheduled job
2. Identifies snapshots with `autoupdate` metadata in their `.dvc` files
3. Executes these snapshots' Python scripts to fetch the latest data
4. Checks if the data has actually changed (by comparing MD5 hashes and file sizes)
5. Creates a GitHub pull request when changes are detected for review and merging

!!! warning "Caution: Overwrites Existing Versions"
    The autoupdate workflow creates new snapshots in R2, but keeps all ETL versions the same. This means ETL and grapher datasets are overwritten on every update. If your updated steps are dependencies for other steps, those will be updated automatically as well—without a version bump.
    **Recommendation:** Use autoupdate only for "isolated", regularly updated datasets that use the "latest" version.
    If an update cascades unexpectedly, you can review all data changes using chart diff.

!!! info "Work in Progress"
    We are still working on a generalized autoupdate workflow for larger datasets that properly respects versioning. :face_with_monocle:

## Enabling Autoupdate for a Snapshot

To enable automatic updates for a snapshot, you need to add the `autoupdate` field to its `.dvc` file:

```yaml
autoupdate:
  name: Update name
meta:
  # Origin metadata...
outs:
  # Outputs...
```

The `name` field is used to group related snapshots together. Multiple snapshots can share the same update name, which means they will be processed together and included in the same pull request.


## Advanced approach

!!! danger "Only for advanced users"
    Sometimes, you might want more control over the update process than what the automatic system provides. For those cases, you can follow the steps below.

    Our vision is that the advanced approach will eventually be deprecated in favor of the automatic update system. But we are keeping it for now.

    This approach is really meant only when there are no alternatives.


### Differences between standard and advanced approaches
The automatic update system provides an alternative approach to the scheduled update scripts described in [Auto Regular Updates](auto-regular-updates.md). Here's how they compare:

| Feature | Standard | Advanced |
|---------|-----------------|-------------------|
| Update mechanism | Uses `autoupdate` field in `.dvc` files | Dedicated bash script for each dataset |
| PR creation | Always creates PRs for review | Creates commit directly on master |
| Grouping | Groups related snapshots in single PR | One script per dataset update |
| Discovery | Automatic discovery of autoupdate-enabled snapshots | Manual maintenance of update scripts |
| Integration | Runs automatically daily | Scheduled individually via Buildkite |


### Create the data pipeline using `latest` version

Firstly, create the necessary steps to build the dataset (i.e. snapshot, meadow, garden, etc.). Use version `latest` for all of them, to avoid adding duplicate code.

Make sure to add these steps to the DAG. For instance, in the example below, we want to keep the `cases_deaths` dataset up-to-date with the latest data.

```yaml
# WHO - Cases and deaths
data://meadow/covid/latest/cases_deaths:
  - snapshot://covid/latest/cases_deaths.csv
data://garden/covid/latest/cases_deaths:
  - data://meadow/covid/latest/cases_deaths
  - data://garden/regions/2023-01-01/regions
  - data://garden/wb/2024-03-11/income_groups
  - data://garden/demography/2024-07-15/population
data://grapher/covid/latest/cases_deaths:
  - data://garden/covid/latest/cases_deaths
```

### Create the update script

Create an update script and save it in the [scripts/](https://github.com/owid/etl/tree/master/scripts) directory. This script must be a bash script, which basically needs to run the necessary code to update the snapshot. In the example below, we user [].

```bash title="scripts/update-covid-cases-deaths.sh" linenums="1"
--8<-- "scripts/update-covid-cases-deaths.sh"
```

In the example above, you need to replace the code in line 14. Optionally, edit the text in lines 12 and 20 to better log the update.

### Schedule update in Buildkite

Finally, you need to schedule the regular update. To do so, go to [Buildkite](https://buildkite.com/our-world-in-data/etl-automatic-dataset-updates-master/settings/steps) and edit the instructions in the file.

Simply add a

```yaml
- label: "Update <step>"
    command:
    - "sudo su - owid -c 'bash /home/owid/etl/scripts/update-<step>.sh'"
```

