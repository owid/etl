# Regions data

## Description of the dataset
This dataset contains useful information about the countries and regions of OWID datasets.

There are no `snapshot` or `meadow` steps for this data, only a `garden` and a `grapher` step. The latter feeds the charts on [the region definitions page](https://ourworldindata.org/world-region-map-definitions).

All tables in the dataset are indexed by `code`, which is defined as the ISO alpha-3 code, when it exists, and otherwise as a custom OWID code. For example, since Europe doesn't have an ISO code, it has the code `OWID_EUR`. There are no specific rules for how to define these custom codes, but as a guideline:

* The code following `OWID_` should not exist already as an ISO code. This rule has not been followed in the past, and we have `OWID_NAM` for `North America` (while `NAM` is also the code of `Namibia`). This can lead to confusion, and hence we should try to apply this rule in the future.
* If the region is a sub-region of a country, append its code after the country's code. For example, the code of `Madrid` (which is a region in Spain) could be `OWID_ESP_MAD`, given that `MAD` does not exist (`Madagascar`'s code is `MDG`).

Tables contained in the `regions` dataset:

* `aliases`: Region aliases (i.e. variants of the region name). Columns:
    * `alias`: Alternative name for a region. For example, for `United States`, there is a row for the alias `US` and another row for the alias `USA`.
* `definitions`: Region definitions. Columns:
    * `name`: Name of the region (that will be shown in most charts).
    * `short_name`: Short version of the name of the region (that will be shown in specific charts that have limited space).
    * `region_type`: Region type. Currently, the options are:
        * `country`: Country (e.g. 'France'). The official status of a region may be unclear in some cases, but we tend to include as many countries as possible.
        * `continent`: Inhabited continent (namely 'Africa', 'Asia', 'Europe', 'North America', 'Oceania', and 'South America').
        * `aggregate`: Region that is not a country and includes other countries (e.g. 'Channel Islands', 'European Union (27)', 'Melanesia', 'Polynesia', 'World').
        * `other`: Regions that may not be considered countries by certain data providers, or that have a custom definition (like 'Serbia excluding Kosovo') and that are not aggregates of other countries.
    * `is_historical`: True if the region does not exist anymore, and False otherwise.
    * `defined_by`: Institution that contained the region in a dataset. For example, if a region `North America (BP)` is added to the `regions` dataset, `defined_by` would be `bp` (the namespace that dataset belongs to).
* `legacy_codes`: Legacy codes. Columns:
    * `cow_code`: Correlates of War numeric code.
    * `cow_letter`: Correlates of War letter code.
    * `imf_code`: International Monetary Fund code.
    * `iso_alpha2`: 2-letter International Organization for Standardization alpha-2 code.
    * `iso_alpha3`: 3-letter International Organization for Standardization alpha-3 code.
    * `kansas_code`: TODO: Describe this code.
    * `legacy_country_id`: TODO: Describe this code.
    * `legacy_entity_id`: TODO: Describe this code.
    * `marc_code`: MARC (Machine Readable Cataloging) code.
    * `ncd_code`: TODO: Describe this code.
    * `penn_code`: Country code for the Penn World Tables.
    * `unctad_code`: UNCTAD (United Nations Conference on Trade and Development) code.
    * `wikidata_code`: Wikidata code. To create the URL of the wikidata page of the region, append the wikidata code to: http://www.wikidata.org/entity/
* `members`: Region members (roughly, sub-regions that would need to be added up when aggregating data for the region). Columns:
    * `member`: Region member. For example, region `Africa` contains one row for each region in Africa (including historical regions).
* `related`: Other possible region members to be aware of. This includes regions with an unclear official status that may be
  members of another region according to some data providers, but not according to others. Columns:
    * `member`: Related region (e.g. an overseas territory).
* `transitions`: Historical transitions between regions. Columns:
    * `end_year`: Last year the historical region existed.
    * `successor`: Country that existed (from `end_year` on) in the same geographical space as the historical region.

## Working with regions data in ETL steps

The `Regions` class provides convenient methods for working with the regions dataset. In ETL steps, you can access this object through `paths.regions` (where `paths` is an instance of `PathFinder`).

### Accessing region information

Query region data programmatically:

```python
# Get region members
regions = Regions()
regions.get_region("Europe")["members"]

# Get multiple regions with their members
regions.get_regions(["Africa", "High-income countries"], only_members=True)
```

### Country harmonization

Harmonize country names in your data to match OWID's standardized country names:

```python
# Run interactive harmonizer to create mapping file
paths.regions.harmonizer(tb)

# Apply harmonization using the created mapping file
tb = paths.regions.harmonize_names(tb)
```

### Region aggregation

Add region aggregates (continents, income groups, World) to your data:

```python
# Simple region aggregation
tb = paths.regions.add_aggregates(tb)

# Custom regions with custom aggregation with specific regions and aggregation methods
tb = paths.regions.add_aggregates(
    tb,
    regions={
        "Asia": {},  # No need to define anything, since it is a default region.
        "Asia excluding China": {  # Custom region that must be defined based on other known regions and countries.
            "additional_regions": ["Asia"],
            "excluded_members": ["China"],
        },
        "High-income countries": {},
    },
    aggregations={"population": "sum", "gdp": "mean_weighted_by_population"}
)
```

Note that we have a custom aggregate method `mean_weighted_by_X` which creates a mean weighted by `X`, where `X` is a given column in the table. If `X` is `population`, and it's not in the table, it will be added automatically (if so, the population dataset needs to be among the dependencies of the current data step).

### Per capita calculations

Create per capita indicators for your data:

```python
# Add per capita indicators for all numeric columns
tb = paths.regions.add_per_capita(tb)

# Add per capita indicators for specific columns
tb = paths.regions.add_per_capita(tb, columns=["gdp", "co2_emissions"])

# Only include countries that have data (useful for regional aggregates)
tb = paths.regions.add_per_capita(
    tb,
    columns=["gdp"],
    only_informed_countries_in_regions=True
)
```

### Multi-operation workflows

For better performance when doing both aggregation and per capita calculations:

```python
# Create aggregator for efficient multi-operation workflow
agg = paths.regions.aggregator(regions=["World"], aggregations={"gdp": "sum"})
tb = agg.add_aggregates(tb)
tb = agg.add_per_capita(tb, columns=["gdp"])
```

!!! tip "Advanced usage"

    For more advanced features of `Regions` and `RegionAggregator`, like region overlap detection and custom region definitions, see the docstrings of the `Regions` class in `etl/data_helpers/geo.py`.


## How to make changes to the dataset

New aliases and short names can be added to the dataset without creating a new dataset version. For that, we can use the `harmonize` tool in `etl`.

For any other type of change to the dataset:

* If the change does not affect existing datasets, it can be done without creating a new dataset version. For example, adding a new region for a particular institution (e.g. `North America (BP)`) does not affect any other existing dataset.
* If the change does affect existing datasets, then a new version needs to be created. For example, if sub-regions of a country are added, and they are also added as the members of a continent, this could affect existing datasets (that happened to have data for those sub-regions). However, if it's clear that the changes do not affect existing countries, then there is no need to update the dataset version.
