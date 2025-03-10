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

```yaml title="etl/steps/export/multidim/energy/latest/energy_prices.yaml"
title:
  title: "Energy prices"
  titleVariant: "by energy source"
defaultSelection:
  - "European Union (27)"
topic_tags:
  - "Energy"
dimensions:
  - slug: "frequency"
    name: "Frequency"
    choices:
      - slug: "annual"
        name: "Annual"
        description: "Annual data"
      - slug: "monthly"
        name: "Monthly"
        description: "Monthly data"
  - slug: "source"
    name: "Energy source"
    choices:
      - slug: "electricity"
        name: "Electricity"
      - slug: "gas"
        name: "Gas"
  - slug: "unit"
    name: "Unit"
    choices:
      - slug: "euro"
        name: "Euro"
        description: "Price in euros"
      - slug: "pps"
        name: "PPS"
        description: "Price in Purchasing Power Standard"
views:
  # Views will be filled out programmatically.
  []

```

The `dimensions` field specifies selectors, and the `views` field defines views for the selection. Since there are numerous possible configurations, `views` are usually generated programmatically (using function `etl.collections.multidim.generate_views_for_dimensions`).

You can also combine manually defined views with generated ones. See the `etl.collections.multidim` module for available helper functions or refer to examples from `etl/steps/export/multidim/`. Feel free to add or modify the helper functions as needed.

The export step loads the data dependencies and the config YAML file, adds `views` to the config, and then pushes the configuration to the database.

```python title="etl/steps/export/multidim/energy/latest/energy_prices.py"
def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load data on energy prices.
    ds_grapher = paths.load_dataset("energy_prices")

    # Read table of prices in euros.
    tb_annual = ds_grapher.read("energy_prices_annual")
    tb_monthly = ds_grapher.read("energy_prices_monthly")

    #
    # Process data.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Create views.
    config["views"] = multidim.generate_views_for_dimensions(
        dimensions=config["dimensions"],
        tables=[tb_annual, tb_monthly],
        dimensions_order_in_slug=("frequency", "source", "unit"),
        warn_on_missing_combinations=False,
        additional_config={"chartTypes": ["LineChart"], "hasMapTab": True, "tab": "map"},
    )

    #
    # Save outputs.
    #
    multidim.upsert_multidim_data_page(config=config, paths=paths)

```

To see the multi-dimensional indicator in Admin, run

```bash
etlr export://multidim/energy/latest/energy_prices --export
```

and check out the preview at: http://staging-site-my-branch/admin/grapher/mdd-energy-prices


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
