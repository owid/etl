---
status: new
---

!!! warning "Export steps are a work in progress"

Export steps are defined in `etl/steps/export` directory and have similar structure to regular steps. They are run with the `--export` flag:

```bash
etlr export://explorers/minerals/latest/minerals --export
```

The `def run(dest_dir):` function doesn't save a dataset, but calls a method that performs the action. For instance `create_explorer(...)` or `gh.commit_file_to_github(...)`. Once the step is executed successfully, it won't be run again unless its code or dependencies change (it won't be "dirty").

## Creating explorers

TSV files for explorers are created using the `create_explorer` function, usually from a configuration YAML file

```py
# Create a new explorers dataset and tsv file.
ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
ds_explorer.save()
```

!!! info "Creating explorers on staging servers"

    Explorers can be created or edited on staging servers and then manually migrated to production. Each staging server creates a branch in the `owid-content` repository. Editing explorers in Admin or running the `create_explorer` function pushes changes to that branch. Once the PR is merged, the branch gets pushed to the `owid-content` repository (not to the `master` branch, but its own branch). You then need to manually create a PR from that branch and merge it into `master`.


## Creating multi-dimensional indicators

Multi-dimensional indicators are powered by a configuration that is typically created from a YAML file. The structure of the YAML file looks like this:

```yaml title="etl/steps/export/multidim/covid/latest/covid.deaths.yaml"
definitions:
  table: {definitions.table}

title:
  title: COVID-19 deaths
  titleVariant: by interval
defaultSelection:
  - World
  - Europe
  - Asia
topicTags:
  - COVID-19

dimensions:
  - slug: interval
    name: Interval
    choices:
      - slug: weekly
        name: Weekly
        description: null
      - slug: biweekly
        name: Biweekly
        description: null

  - slug: metric
    name: Metric
    choices:
      - slug: absolute
        name: Absolute
        description: null
      - slug: per_capita
        name: Per million people
        description: null
      - slug: change
        name: Change from previous interval
        description: null

views:
  - dimensions:
      interval: weekly
      metric: absolute
    indicators:
      y: "{definitions.table}#weekly_deaths"
  - dimensions:
      interval: weekly
      metric: per_capita
    indicators:
      y: "{definitions.table}#weekly_deaths_per_million"
  - dimensions:
      interval: weekly
      metric: change
    indicators:
      y: "{definitions.table}#weekly_pct_growth_deaths"

  - dimensions:
      interval: biweekly
      metric: absolute
    indicators:
      y: "{definitions.table}#biweekly_deaths"
  - dimensions:
      interval: biweekly
      metric: per_capita
    indicators:
      y: "{definitions.table}#biweekly_deaths_per_million"
  - dimensions:
      interval: biweekly
      metric: change
    indicators:
      y: "{definitions.table}#biweekly_pct_growth_deaths"
```

The `dimensions` field specifies selectors, and the `views` field defines views for the selection. Since there are numerous possible configurations, `views` are usually generated programmatically. However, it's a good idea to create a few of them manually to start.

You can also combine manually defined views with generated ones. See the `etl.multidim` module for available helper functions or refer to examples from `etl/steps/export/multidim/`. Feel free to add or modify the helper functions as needed.

The export step loads the YAML file, adds `views` to the config, and then calls the function.

```python title="etl/steps/export/multidim/covid/latest/covid.py"
def run(dest_dir: str) -> None:
    engine = get_engine()

    # Load YAML file
    config = paths.load_mdim_config("covid.deaths.yaml")

    multidim.upsert_multidim_data_page("mdd-energy", config, engine)
```

To see the multi-dimensional indicator in Admin, run

```bash
etlr export://multidim/energy/latest/energy --export
```

and check out the preview at http://staging-site-my-branch/admin/grapher/mdd-name.


## Exporting data to GitHub

One common use case for the `export` step is to commit a dataset to a GitHub repository. This is useful when we want to make a dataset available to the public. The pattern for this looks like this:

```python
if os.environ.get("CO2_BRANCH"):
    dry_run = False
    branch = os.environ["CO2_BRANCH"]
else:
    dry_run = True
    branch = "master"

gh.commit_file_to_github(
    combined.to_csv(),
    repo_name="co2-data",
    file_path="owid-co2-data.csv",
    commit_message=":bar_chart: Automated update",
    branch=branch,
    dry_run=dry_run,
)
```

This code will commit the dataset to the `co2-data` repository on GitHub if you specify the `CO2_BRANCH` environment variable, i.e.

```bash
CO2_BRANCH=main etlr export://co2/latest/co2 --export
```
