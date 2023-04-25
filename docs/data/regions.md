# Regions data

## Description of the dataset
This dataset contains useful information about the countries and regions of OWID datasets.

There are no `snapshot` or `meadow` steps for this data, only a `garden` step.
We could consider creating a `grapher` step to feed the charts on [the region](https://ourworldindata.org/world-region-map-definitions) definitions page](https://ourworldindata.org/world-region-map-definitions).

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

## How to make changes to the dataset

New aliases and short names can be added to the dataset without creating a new dataset version. For that, we can use the `harmonize` tool in `etl`.
TODO: Close issue: https://github.com/owid/etl/issues/845

For any other type of change to the dataset:

* If the change does not affect existing datasets, it can be done without creating a new dataset version. For example, adding a new region for a particular institution (e.g. `North America (BP)`) does not affect any other existing dataset.
* If the change does affect existing datasets, then a new version needs to be created. For example, if sub-regions of a country are added, and they are also added as the members of a continent, this could affect existing datasets (that happened to have data for those sub-regions). However, if it's clear that the changes do not affect existing countries, then there is no need to update the dataset version.
