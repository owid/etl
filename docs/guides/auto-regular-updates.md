---
tags:
  - 👷 Staff
---

!!! warning "This is a work in progress"

    Things might change in the near-future. To stay up-to-date with the latest updates, [join the discussion on GitHub!](https://github.com/owid/etl/issues/3016)

ETL was initially conceived to maintain and publish datasets that are updated once a year. This is still the most common kind of update that we do this days.

However, sometimes we need to update datasets more frequently. This is the case for instance for the COVID-19 dataset, and other examples, which need weekly or monthly updates.

In such cases, the processing code remains the same, but the origin data needs to be updated. Put simply, the ETL process is the same, but with an updated snapshot of the data.

If you want to keep a dataset up-to-date with the latest data, follow the steps below.

## Create the data pipeline using `latest` version

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

## Create the update script

Create an update script and save it in the [scripts/](https://github.com/owid/etl/tree/master/scripts) directory. This script must be a bash script, which basically needs to run the necessary code to update the snapshot. In the example below, we user [].

```bash title="scripts/update-covid-cases-deaths.sh" linenums="1"
--8<-- "scripts/update-covid-cases-deaths.sh"
```

In the example above, you need to replace the code in line 14. Optionally, edit the text in lines 12 and 20 to better log the update.

## Schedule update in Buildkite.

Finally, you need to schedule the regular update. To do so, go to [Buildkite](https://buildkite.com/our-world-in-data/etl-automatic-dataset-updates-master/settings/steps) and edit the instructions in the file.

Simply add a

```yaml
- label: "Update <step>"
    command:
    - "sudo su - owid -c 'bash /home/owid/etl/scripts/update-<step>.sh'"
```
