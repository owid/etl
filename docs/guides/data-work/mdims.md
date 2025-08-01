---
status: new
---

!!! warning "MDIMs and Explorer steps are a work in progress"
    We are currently working to align MDIMs and Explorers as much as possible. While they are presented in different ways on our site, the underlying data structure should be as similar as possible (if not identical). Track the progress of this work in [this issue](https://github.com/owid/etl/issues/3992).

    For recent information please refer to the Slack channel `#proj-explorers-mdims-convergence`.

    Summary of the status of Explorers/MDIMs convergence in [this slides](https://docs.google.com/presentation/d/1A2xUmlueRKZRlmVyHfOIONZCSoWEtqoL-bw-MfJc4-c/edit?usp=sharing) (March 2025).

MDIMs and Explorer (simply referred t as "MDIMs" from now on) steps are [export steps](export-data.md) that create multi-dimensional indicators and explorers, respectively.
These have a similar structure to regular steps and are run with the `--export` flag:

```bash
etlr export://explorers/minerals/latest/minerals --export
```

Instead of creating a `Dataset` object, these create `MDIM` or `Explorer` objects and save them to the database. Both `MDIM` and `Explorer` are based on the same underlying data structure (class `Collection`, see in [`etl.collection.model`](https://github.com/owid/etl/blob/master/etl/collection/model.py)), but have minor differences due to legacy reasons.

!!! question "Why do we need both?"
    We understand MDIMs as a more powerful version of explorers, and will likely replace them in the long-run from a technical perspective. MDIMs still are still missing some key features to fully be able to replace explorers.

    [Learn more :material-arrow-right:](#mdims-pending-issues)

## Creating an MDIM (or Explorer)
MDIMs are defined by a collection of indicators that are presented in different views. Each view is defined by a specific dimension value (e.g. `sex="female"`). Ultimately, an MDIM is presented as a chart with additional selection options to switch from one view to another.

To implement an MDIM, you need to create a new step script in the `etl/steps/export/multidim/` directory along with a YAML config file and add a reference to the DAG (just like with any other data step). The step script should load the data dependencies and the default YAML configuration, and save the MDIM to the database.

For details on how to implement an explorer step, first read this section and then continue to [Nuanaces of creating an Explorer](#nuances-of-creating-an-explorer).

!!! note "Currently there is no Wizard template"
    We will have templating soon, same as we already have for data steps. For now, we suggest looking at some of the examples.

!!! info "See the complete list of MDIMs and Explorers"
    - [MDIMs :material-arrow-right:](https://github.com/owid/etl/blob/master/etl/steps/export/multidim/)
    - [Explorers :material-arrow-right:](https://github.com/owid/etl/blob/master/etl/steps/export/explorers/)

### A simple MDIM
MDIMs can be crafted in various ways, going from totally manual to fully automated. The most common way is to use a combination of manual and automated steps. The automated steps are usually generated by helper functions from the `etl.collection` module.

Below is a simple MDIM configuration file (modified for demo purposes):

```yaml title="etl/steps/export/multidim/covid/latest/covid.deaths.yml"
title:
  title: COVID-19 confirmed deaths
  title_variant: ""
default_selection:
  - World
topic_tags:
  - COVID-19

dimensions:
  - slug: period
    name: Period
    choices:
      - slug: weekly
        name: Weekly
        description: null
  - slug: metric
    name: Indicator
    choices:
      - slug: absolute
        name: Absolute number
        description: null
      - slug: per_capita
        name: Per million people
        description: Normalized by million people living in the country or region
views:
  - dimensions:
      period: weekly
      metric: absolute
    indicators:
      y: "cases_deaths#weekly_deaths"
  - dimensions:
      period: weekly
      metric: per_capita
    indicators:
      y: "cases_deaths#weekly_deaths_per_million"
```

This example is fairly simple, and is entirely manual. The corresponding step script could simply look like:

```python
from etl.collection import multidim
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config(fname)
    # Create MDIM object
    mdim = paths.create_mdim(config)
    # Save to DB
    mdim.save()
```

As you can see, there are top-level fields (`title`, `default_selection`, `topic_tags`) which define the MDIM name and other details, and then we have `dimensions` and `views` fields. The `dimensions` field defines the selectors, and the `views` field defines the views for the selection. Each view has a reference to the dimensions it represents.

In this example, we note that we can group together indicators from any dataset. While we may present them as "dimensional", the underlying data structure may not be.

!!! tip "Learn more about the structure of MDIMs in [their schema](https://github.com/owid/etl/blob/master/schemas/multidim-schema.json)"
    There are more options available in the schema that are not covered here. E.g. you can set chart configurations for each view (`.config`), or indicator-level display settings (`.display`). You can even tweak the presentation fields like `description_key` for a specific view (`.metadata`).

    Proper documentation of all available options will be available soon.

### Automated MDIMs
MDIMs are very suitable for those datasets that have a lot of dimensions in them. For these, one can leverage functions like `etl.collection.multidim.expand_config`, which programmatically generates all possible views from an indicator (or multiple ones) in a table.

!!! question "What does it mean for a dataset to have dimensions?"
    Datasets with dimensions are those that have additional index (e.g. `age`) in Garden, beyond the standard ones (`year` or `date`, and `country`).
    While Grapher steps will transform the shape of your data so that each indicator-dimension has one column, each column preserves the dimensional information under the `.metadata.dimensions` property.

    If you didn't create a dimensional indicator, you can still create an MDIM manually. Or, if you want, you can also add the dimensional information a posteriori ([example :material-arrow-right:](https://github.com/owid/etl/blob/master/etl/steps/export/multidim/energy/latest/energy_prices.py)).


```yaml title="etl/steps/export/multidim/energy/latest/energy_prices.yaml"
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

The corresponding step script looks like:

```python title="etl/steps/export/multidim/energy/latest/energy_prices.py"
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
    config = paths.load_mdim_config()

    # Create views.
    config["views"] = multidim.expand_config(
        tb_annual,
        dimensions=["frequency", "source", "unit"],
        additional_config={"chartTypes": ["LineChart", "DiscreteBar"], "hasMapTab": True, "tab": "map"},
    )

    #
    # Save outputs.
    #
    mdim = paths.create_mdim(config=config)
    mdim.save()

```

You can also combine manually defined views with generated ones. See the `etl.collection.multidim` module for available helper functions or refer to examples from `etl/steps/export/multidim/`.


### Defining view configurations
To set the config of views of MDIMs, you can use different strategies.

#### Manual
You can do it manually in the YAML file if there are few views.

#### Programmatically
However, you can also do it programmatically after creating an MDIM object.

```python
def add_display_settings(mdim):
  for view in mdim.views:
      # Set default display settings
      if view.dimensions["indicator"] == "emigrants":
          view.config = CUSTOM_CONFIG_1
      elif view.dimensions["indicator"] == "immigrants":
          view.config = CUSTOM_CONFIG_1
```

Also, you can use the fields `common_views` in the YAML file to define a common configuration for certain subset of views. This is very useful (and recommended) when you want to define different-level of granularity of configs. For instance, imagine you want to define the same subtitle for all views with dimension `sex=age`, but then if you have a view with `sex=age, year=2020`, you want to be able to a different subtitle. etc.

As a reference, you can look at the explorer config of the [Population and Demography explorer](https://github.com/owid/etl/blob/master/etl/steps/export/explorers/un/latest/un_wpp.manual.config.yml). Below is a snippet of the `common_views` field. In it, we define different general configurations: (i) one for views with `indicator=age_structure`, (ii) one for views with `indicator=age_structure` and `sex=female`, and (iii) one for views with `indicator=age_structure` and `sex=male`.

```yaml title="etl/steps/export/explorers/un/latest/un_wpp.yaml"
common_views:
  # Age structure
  - dimensions:
      indicator: age_structure
    config:
      title: Age structure
      type: StackedDiscreteBar
      hasMapTab: false
      stackMode: relative
  - dimensions:
      indicator: age_structure
      sex: female
    config:
      title: Female age structure
  - dimensions:
      indicator: age_structure
      sex: male
    config:
      title: Male age structure
```

You can also define general configs if you skip the `dimensions` bit:

```yaml title="etl/steps/export/explorers/who/latest/monkeypox.yaml"
common_views:
- config:
    hideAnnotationFieldsInTitle: "true"
    yScaleToggle: "true"
```

### Save and preview an MDIM
To see the multi-dimensional indicator in Admin, run

```bash
etlr export://multidim/energy/latest/energy_prices --export
```

and check out the preview at: http://staging-site-my-branch/admin/multi-dims

### Experimental tooling
There is on going work in `etl.collection.beta`. In there you can find interesting methods like `combine_explorers`, which lets you combine two explorer objects into one. Since this works also with MDIMs, it can be extremely useful to combine MDIMs automatically-generated from different tables.

## Nuances of creating an Explorer
While Explorer and MDIM steps are almost identical, there are some subtle implementation differences. We hope that this is just a temporary situation and that we will be able to merge them into a single step in the future.

Historically, explorers, contrary to MDIMs, have relied on data that is not currently tracked by our ETL or ETL DAG. Their configs also have a different stuctures in Admin. However, we new explorer updates should consist of migrating existing legacy configurations to the new MDIM-like format.

Some key differences are:

- **Top-level config fields are structured differently**: Corresponds to the config parameters at the top of the legacy Explorer config.
- **View config has different fields:** MDIMs rely on our standard grapher configs. Instead, Explorers rely on a custom configuration. Fields available are equivalent to the columns that are available in the `grapher` table of a legacy explorer config in admin.
- **Indicator display config**: Explorers also rely on a custom configuration. Fields available are equivalent to the columns that are available in the `columns` table of a legacy explorer config in admin.

Also, instead of relying on `etl.collection.multidim`, you should instead use the functions from `etl.collection.explorer`.

!!! note "There is no schema for explorers"


### Explorer examples:
- [Population and Demography](https://github.com/owid/etl/blob/master/etl/steps/export/explorers/un/latest/un_wpp.py): Configuration automatically generated (mostly) from dimensions, using `expand_config` and other tools. This one is a good example for explorers with a large number of views.
- [COVID-19](https://github.com/owid/etl/blob/master/etl/steps/export/explorers/covid/latest/covid.config.yml): Manually crafted.
- [Monkeypox](https://github.com/owid/etl/blob/master/etl/steps/export/explorers/who/latest/monkeypox.py): Manually crafted.
- [Climate change](https://github.com/owid/etl/blob/master/etl/steps/export/explorers/climate/latest/climate_change.config.yml): Manually crafted.

### How are explorers saved in admin?
While MDIMs are pushed to our database, explorers need to go through the old legacy channel of `owid-content` repository. Hence, when calling `Explorer.save`, the configuration is re-shaped and stored as a TSV file in `owid-content`.


!!! info "Creating explorers on staging servers"
    Explorers can be created or edited on staging servers and then manually migrated to production. Each staging server creates a branch in the `owid-content` repository. Editing explorers in Admin or running the `create_explorer` function pushes changes to that branch. Once the PR is merged, the branch gets pushed to the `owid-content` repository (not to the `master` branch, but its own branch). You then need to manually create a PR from that branch and merge it into `master`.

## MDIM pending features
For better reference, read [this issue](https://github.com/owid/etl/issues/3992), or follow the latest news in `#proj-explorers-mdims-convergence`.

- MDIMs are not yet public.
- MDIMs are not yet embeddable.
- MDIMs only support dropdowns (explorers support other UI elements: radio buttons, checkboxes)
