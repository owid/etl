---
tags:
  - Data Workflow
icon: lucide/forward
---

# Viz and export steps

Viz steps (`viz://`) produce visualizations and export steps (`export://`) ship files to external destinations. They are defined in the `etl/steps/viz` and `etl/steps/export` directories and have a similar structure to regular steps. Viz steps publish with `--grapher`; export steps write to R2 or GitHub with `--export`. Either kind, named by its URI and run without its flag, only builds its output locally: handy for checking a chart config or an export file before publishing it.

The channel of a viz step says what it produces:

- `viz://chart/`: a chart or an MDIM (a chart is just an MDIM with no dimensions), upserted to the grapher DB. A chart step can be a bare `<name>.config.yml` with no Python file.
- `viz://explorer/`: an explorer, upserted to the grapher DB.
- `viz://static/`: a static image (PNG/SVG) rendered with matplotlib next to the recipe and committed.
- `viz://bespoke/`: the data feed of a bespoke interactive visualization. The step only writes JSON files into `viz/bespoke/<ns>/<version>/<name>/`; the framework syncs that folder to R2, see [Bespoke feeds](#bespoke-feeds).

Chart and explorer steps also write their expanded config to the gitignored `viz/<channel>/...` folder, like `data/` for data steps.

```bash
etlr viz://explorer/minerals/latest/minerals --grapher
```

The `def run():` function doesn't save a dataset, but calls a method that performs the action. For instance `paths.create_chart(...)` or `gh.commit_file_to_github(...)`. Once the step is executed successfully, it won't be run again unless its code or dependencies change (it won't be "dirty").

## Creating explorers

Explorers are created with `paths.create_explorer(config=...)` from a configuration YAML file, and upserted to the grapher DB by `.save()`. They follow the same structure as MDIMs, see [MDIMs and Explorers](mdims.md).

## Creating multi-dimensional indicators

Multi-dimensional indicators are powered by a configuration that is typically created from a YAML file. The structure of the YAML file looks like this:

```yaml title="etl/steps/viz/chart/energy/latest/energy_prices.yaml"
title:
  title: "Energy prices"
  title_variant: "by energy source"
default_selection:
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

The `dimensions` field specifies selectors, and the `views` field defines views for the selection. Since there are numerous possible configurations, `views` are usually generated programmatically (using function `etl.viz.expand_config`).

You can also combine manually defined views with generated ones. See the `etl.viz` module for available helper functions or refer to examples from `etl/steps/viz/chart/`. Feel free to add or modify the helper functions as needed.

The chart step loads the data dependencies and the config YAML file, adds `views` to the config, and then pushes the configuration to the database.

```python title="etl/steps/viz/chart/energy/latest/energy_prices.py"
def run() -> None:
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
    config = paths.load_config()

    # Create views.
    config["views"] = expand_config(
        tb_annual,
        dimensions=["frequency", "source", "unit"],
        additional_config={"chartTypes": ["LineChart", "DiscreteBar"], "hasMapTab": True, "tab": "map"},
    )

    #
    # Save outputs.
    #
    mdim = paths.create_chart(config=config)
    mdim.save()

```

To see the multi-dimensional indicator in Admin, run

```bash
etlr viz://chart/energy/latest/energy_prices --grapher
```

and check out the preview at: http://staging-site-my-branch/admin/grapher/mdd-energy-prices


## Bespoke feeds

A `viz://bespoke/` step produces the JSON feed that one of owid-grapher's `bespoke/projects/` bundles
fetches in the browser. The step writes files into its own output folder and nothing else; after it
runs, the framework syncs that folder to

| Environment | Feed location |
| --- | --- |
| production | `s3://owid-api/v1/bespoke/<ns>/<version>/<name>/`, served at `https://api.ourworldindata.org/v1/bespoke/...` |
| staging server, laptop | `s3://owid-api-staging/<env>/v1/bespoke/<ns>/<version>/<name>/`, served at `https://api-staging.owid.io/<env>/v1/bespoke/...` |

the same split that already applies to the baked indicator JSONs (`DATA_API_URL` in `etl/config.py`)
and to MDIM download packages. So a staging server builds its own copy of a feed a branch changes,
and an article preview shows the change before it is merged. Nothing seeds a new staging
environment: the `owid-api-staging` worker falls back to the production bucket for any file the
environment doesn't have, so a staging server that never ran the step serves production's feed.

Files that the run no longer produces are deleted from the feed, so a dropped entity doesn't linger.
Without `--grapher` the step writes its files and skips the sync.

`etl.viz.bespoke.build_feed_metadata()` derives the feed's provenance — the source line, citations,
last and next update — from the garden columns the feed is built on, in the shape the MDIM download
packages publish, so it doesn't have to be typed into the step and go stale at the next data update:

```python
from etl.viz.bespoke import build_feed_metadata, write_feed_metadata

feed_metadata = build_feed_metadata(
    title="Causes of death",
    columns={"deaths": tb["value"]},
    update_period_days=ds_garden.metadata.update_period_days,
)
write_feed_metadata(paths.output_dir, feed_metadata)
```

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
CO2_BRANCH=main etlr export://github/co2_data/latest/owid_co2 --export
```
