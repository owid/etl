---
tags:
  - Development
icon: lucide/lightbulb
---

# ETL tips and tricks

!!! info "Help us improve this page!"

    Contribute by [documenting](../dev/docs/){data-preview} your tricks and tips!.


## Browse and search ETL steps interactively

Use `etl` (with no arguments) to open an interactive browser with fuzzy search. This is useful when you don't remember the exact step name.

```bash
# Open the interactive browser
etl
```

<figure markdown="span">
    <img src="../../assets/etl-browser.png" alt="ETL Browser" style="width:80%;">
    <figcaption>The ETL browser with fuzzy search and filter support.</figcaption>
</figure>

Once you select a step, it will be executed. The browser persists between runs, so your options stay set.

### Quick reference

| Input | Action |
|-------|--------|
| `?` | Show help (mode, options, filters) |
| `/` | Show commands (mode switching, exit) |
| `@` | Set options (e.g., `@dry-run`, `@force`) |
| `@@` | Reset all options to defaults |

### Filter prefixes

Use **filter prefixes** to narrow results by specific attributes. Filters can be combined with search terms: `n:who v:2024 population` finds WHO steps from 2024 containing "population".

| Prefix | Filters by | Example |
|--------|------------|---------|
| `n:` | namespace | `n:who` |
| `c:` | channel | `c:garden` |
| `v:` | version | `v:2024` |
| `d:` | dataset | `d:gho` |

### Options

Set CLI options directly in the browser using `@` prefix:

```
@dry-run          # Toggle dry-run mode
@force            # Toggle force re-run
@workers 4        # Set parallel workers
@dry-run @force   # Set multiple options at once
@@                # Reset all options
```

Active options are shown in the status line and persist across step executions.

### Mode switching

Switch between steps and snapshots using `/steps` or `/snapshots` commands.



## Interpolate values
Sometimes, you may have empty values in your dataset. In general, a solution for these cases is to use interpolation to fill those gaps based on previous and following values. In `data_helpers.misc` module, you will find the function `interpolate_table` that can be used to interpolate values in a table.

!!! note "Assumptions on the structure of `tb`"

    The function assumes that the input table has an entity column (typically for country) and a time column (year or date).

A simple call can be done as follows:

```python
from etl.data_helpers.misc import interpolate_table

tb = interpolate_table(
    tb,
    entity_col="country",
    time_col="year",
)
```

This will interpolate all the columns in the table `tb` for each country and year. It will use all years between the minimum and maximum years present in `tb`. It will use "linear" interpolation.

You can adapt the function to your needs, and perform very different kind of interpolations.

=== "Other interpolations"

    You can use any [:octicons-link-external-16: method from pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.interpolate.html).

    ```python
    tb = interpolate_table(
        tb,
        entity_col="country",
        time_col="year",
        method="quadratic"
    )
    ```

=== "Interpolate within the year range of each country"

    Sometimes, you may have different time ranges for each country. You can interpolate within the year range of each country. That is, if one country has data from 2000 to 2010, and another from 2005 to 2015, the interpolation will be done within those ranges for each country.

    ```python
    tb = interpolate_table(
        tb,
        entity_col="country",
        time_col="year",
        mode="full_range_entity",
    )
    ```

---

## Expand a timeseries for all years
Sometimes, you may need to expand a timeseries to include all years within a specific range, even if some years are missing in the original data. The `expand_time_column` function in the `data_helpers.misc` module can help you achieve this.


A simple call can be done as follows:

```python
from etl.data_helpers.misc import expand_time_column

tb = expand_time_column(
    tb,
    entity_col="country",
    time_col="year",
)
```

This will expand the table `tb` to include all years between the minimum and maximum years present in `tb` for each country. Missing years will be filled with NaN values.

You can adapt the function to your needs, and perform different kinds of expansions.

=== "Expand to full range for each entity"

    Expand the timeseries to include all years within the minimum and maximum years present in the data for each entity (e.g., country). Missing years will be filled with NaN values.

    ```python
    # Expand timeseries
    tb = expand_time_column(
        tb,
        entity_col="country",
        time_col="year",
        method="full_range_entity"
    )
    ```

=== "Expand to a specific range for ell entities"

    Expand the timeseries to include all years from 2000 to 2020 for all entities. Missing years will be filled with NaN values.

    ```python
    tb = expand_time_column(
        tb,
        entity_col="country",
        time_col="year",
        method="full_range",
        since_time=2000,
        until_time=2020
    )
    ```

=== "Expand with Custom Fill Value"

    Expand the timeseries to include all years within the minimum and maximum years present in the data for each entity, and fill missing years with a custom value (e.g., 0).

    ```python
    tb = expand_time_column(
        tb,
        entity_col="country",
        time_col="year",
        method="full_range_entity",
        fillna_value=0
    )
    ```

=== "Expand to Observed Years"

    Expand the timeseries to include all years that appear in the data for any entity. This ensures that all entities have rows for all observed years.

    ```python
    tb = expand_time_column(
        tb,
        entity_col="country",
        time_col="year",
        method="observed"
    )
    ```

## Deprecate code
Our codebase has lots of code. Some of it may no longer be maintained or used. To avoid confusion, it is a good practice to slowly deprecate code. This can be done by adding a deprecation warning to the code, and then removing it after a certain period of time:

```python
from deprecated import deprecated

@deprecated("This function is deprecated and will be removed in the future. Please use this other function.")
```

Make sure to point users to an alternative function or method that they can use instead.

Please deprecate function with care, and make sure to check if the function is widely used, and communicate the deprecation to the team.

## Add entity annotations to your dataset
Just add the field `display.entityAnnotationsMap` to the desired indicator.

```yaml
display:
    entityAnnotationsMap: |-
        Spain: Some annotation
        France: Another annotation
```

!!! note "Space is limited"

    The space for annotations in charts is limited. Please be mindful and keep the annotations short and to the point. 2-4 words is usually enough, ideally 2.


<figure markdown="span">
    <img src="../../assets/annotations-chart.png" alt="OWID chart with annotations" style="width:80%;">
    <figcaption>Example chart with entity annotations. Note that the space for annotations.</figcaption>
</figure>

## Which population indicator to use?

We use our population data both as (1) a primary indicator (e.g. to create charts about population growth), and (2) an auxiliary indicator (e.g. to create charts of per capita indicators).

Our population data is built as a combination of multiple origins. When using population as (1), we want to show all those origins in our charts. However, when using population as (2), we don't want those origins to pollute the limited space we have to display sources. For this reason, we decided to create two separate `population` indicators in our population garden dataset, to cater for those two use cases:

(1) `population_original#population`. This indicator has various origins (Hyde, Gapminder, UN WPP). These origins are what we see in charts about population, e.g. [Population by world region](https://ourworldindata.org/grapher/population-regions-with-projections). In fact, the grapher dataset that generates [our Population grapher dataset](https://admin.owid.io/admin/datasets/6621) only uses `population_original`.

  - Note that in most of these charts, population can also be considered as "auxiliary" (e.g. used to define the size of the bubbles in scatter charts). However, in all these cases, population is still shown as a primary indicator, with its own metadata. In other words, **the metadata of `population_original#population` is shown directly in our charts (in the sources tab), as a primary indicator**.

(2) `population#population`. This indicator has only one collapsed origin, with attribution "Population based on various sources (2024)". This is what we see e.g. in our chart [Per capita electricity demand](https://ourworldindata.org/grapher/per-capita-electricity-demand).

  - Importantly, in these charts, **the metadata of `population#population` is always shown indirectly in our charts, propagated to other indicators**.

In the majority of cases, you may want to use population as an auxiliary indicator, and therefore use (2).

## Reading from zipped snapshots

When a snapshot is a zip/tar archive containing multiple files, use `extracted()` to access its contents.

### Basic usage

```python
snap = paths.load_snapshot("my_archive.zip")

with snap.extracted() as archive:
    # List all files in the archive
    print(archive.files)  # ['data/2020.csv', 'data/2021.csv', 'metadata.json']

    # Read a specific file
    tb = archive.read("data/2020.csv")
```

### Finding files with glob patterns

```python
with snap.extracted() as archive:
    # Find all CSVs anywhere in the archive
    csv_files = archive.glob("**/*.csv")

    # Find files in a specific folder
    data_files = archive.glob("data/*")

    # Read all matching files
    tables = [archive.read(f) for f in archive.glob("**/*.csv")]
```

### Checking if a file exists

```python
with snap.extracted() as archive:
    if "optional_file.csv" in archive:
        tb = archive.read("optional_file.csv")
```

### Error handling

If you try to read a file that doesn't exist, you'll get a helpful error message listing available files:

```
FileNotFoundError: File 'wrong_name.csv' not found in archive.
Available files:
  - data/2020.csv
  - data/2021.csv
  - metadata.json
```

### Accessing raw path for custom operations

For non-tabular data or custom file operations, you can access the underlying path:

```python
with snap.extracted() as archive:
    # For custom file operations (e.g., non-tabular data)
    with open(archive.path / "readme.txt") as f:
        content = f.read()

    # Or use pathlib operations
    json_path = archive.path / "config.json"
    if json_path.exists():
        import json
        with open(json_path) as f:
            config = json.load(f)
```
