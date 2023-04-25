# FAOSTAT data

## Overview of the original data

[FAOSTAT (Food and Agriculture Organization Corporate Statistical Database)](https://www.fao.org/faostat/en/#home)
provides free access to food and agriculture data from 1961 to the most recent year available.

The data is distributed among different domains, each one with a unique dataset code (e.g. `qcl`).
We create a new dataset (named, e.g. `faostat_qcl`) for each domain, although we are not including all FAOSTAT
domains, only the ones listed below in section [Output datasets](##output-datasets).

The main data from each domain is downloaded as a file.
The location of each file and the date of their latest update is found by querying
[this catalog](http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json).
Further (additional) metadata for each domain is downloaded using
[an API](https://fenixservices.fao.org/faostat/api/v1/en/definitions/domain).
The process to ingest data and metadata is carried out by
[this script](https://github.com/owid/etl/blob/master/etl/scripts/faostat/create_new_snapshots.py).

Each FAO dataset is typically given as a long table with the following columns:

* `Area Code`: Identifier code of the country/region.
* `Area`: FAO name of the country/region. It contains countries and continents, but also geographical regions (e.g.
  Polynesia), and other aggregations (e.g. Developing regions).
* `Item Code`: Identifier code for items.
* `Item`: Item name (e.g. 'Olive oil').
* `Element Code`: Identifier code for element-units. It is possible to find multiple elements with the same element
  code, if there are multiple units for the same element (e.g. `faostat_qcl` has element codes 5513 and 5510 for element
  'Production', because they have different units, namely 'thousand number' and 'tonnes').
* `Element`: Variable measured (e.g. 'Area harvested').
* `Year Code`: Identifier code for year. We ignore this field, since it is almost identical to `Year`, and does not
  add any information to it.
* `Year`: Year. It is given almost always as an integer value. But sometimes (e.g. in the `faostat_fs` dataset) it is a
  range of years (e.g. '2010-2012').
* `Unit`: Unit of measurement of the specific element (e.g. 'hectares').
* `Value`: Actual value of the specific data point.
* `Flag`: Short text (e.g. `A`) informing of the source or of possible issues of a data point. We use these flags
  only to prioritize data points in cases of ambiguity.

However, there are some datasets with different columns, namely:

* `faostat_fa` contains `Recipient Country` and `Recipient Country Code` instead of `Area` and `Area Code`.
* Datasets `faostat_fa`, `faostat_fs`, and `faostat_sdgb` contain an additional field called `Note`, which we disregard.

The data can be manually inspected on [their website](https://www.fao.org/faostat/en/#data/domains_table).
The dataset code can be typed on the search bar to select data of a specific domain.
Additionally, on their [definitions page](https://www.fao.org/faostat/en/#definitions) one can find definitions and
standards related to countries/regions, elements, items, units and flags.

## Output datasets

### Meadow (unprocessed) datasets

List of meadow datasets that are generated, and their titles:

* `faostat_ef`: Land, Inputs and Sustainability: Fertilizers indicators.
* `faostat_ei`: Climate Change: Emissions intensities.
* `faostat_ek`: Land, Inputs and Sustainability: Livestock Patterns.
* `faostat_el`: Land, Inputs and Sustainability: Land use indicators.
* `faostat_emn`: Land, Inputs and Sustainability: Livestock Manure.
* `faostat_ep`: Land, Inputs and Sustainability: Pesticides indicators.
* `faostat_esb`: Land, Inputs and Sustainability: Soil nutrient budget.
* `faostat_fa`: Discontinued archives and data series: Food Aid Shipments (WFP).
* `faostat_fbs`: Food Balances: Food Balances (2010-).
* `faostat_fbsh`: Food Balances: Food Balances (-2013, old methodology and population).
* `faostat_fo`: Forestry: Forestry Production and Trade.
* `faostat_fs`: Food Security and Nutrition: Suite of Food Security Indicators.
* `faostat_gn`: Climate Change: Energy Use.
* `faostat_ic`: Investment: Credit to Agriculture.
* `faostat_lc`: Land, Inputs and Sustainability: Land Cover.
* `faostat_qcl`: Production: Crops and livestock products.
* `faostat_qi`: Production: Production Indices.
* `faostat_qv`: Production: Value of Agricultural Production.
* `faostat_rfb`: Land, Inputs and Sustainability: Fertilizers by Product.
* `faostat_rfn`: Land, Inputs and Sustainability: Fertilizers by Nutrient.
* `faostat_rl`: Land, Inputs and Sustainability: Land Use.
* `faostat_rp`: Land, Inputs and Sustainability: Pesticides Use.
* `faostat_rt`: Land, Inputs and Sustainability: Pesticides Trade.
* `faostat_scl`: Food Balances: Supply Utilization Accounts.
* `faostat_sdgb`: SDG Indicators: SDG Indicators.
* `faostat_tcl`: Trade: Crops and livestock products.
* `faostat_ti`: Trade: Trade Indices.
* `faostat_wcad`: World Census of Agriculture: Structural data from agricultural censuses.

Each dataset `faostat_*` contains only one table:

* `faostat_*`: Raw data in long format.
    * Indexes:
        * `year`: Year (usually an integer, but sometimes a range, e.g. '2010-2012').
        * `element_code`: FAO element code.
        * `item_code`: FAO item code.
        * `area_code`: FAO country/region code.
  * Columns:
        * `area`: FAO country/region name.
        * `item`: FAO item name.
        * `element`: FAO element name.
        * `unit`: FAO unit name (a short name).
        * `value`: Data value.
        * `flag`: Flag for current data point.

Exceptionally, some datasets have different column names:

* `faostat_fa` contains `recipient_country_code` instead of `area_code` in the index, and column `recipient_country` instead of `area`. Also, `faostat_wcad` has columns `wca_round` and `census_year` instead of `year`.

There is an additional dataset:

* `faostat_metadata`: FAOSTAT (additional) metadata dataset (originally ingested using the FAOSTAT API).
  This dataset contains as many tables as domain-categories (e.g. 'faostat_qcl_area', 'faostat_fbs_item', ...).
  All categories are defined in `category_structure` in the meadow `faostat_metadata.py` step file.

### Garden (processed) datasets

The list of garden datasets is identical to the list of meadow datasets, except for the following changes:

* Datasets `faostat_fbsh` and `faostat_fbs` are not present.
* `faostat_fbsc`: Food Balances Combined. This dataset is the combination of `faostat_fbsh` and `faostat_fbs`.
* `faostat_food_explorer`: Dataset that generates the data that is ingested by the food explorer step
  (see [below](###explorers)). It uses data from the `faostat_fbsc` and `faostat_qcl` garden datasets.
* `faostat_metadata`: This dataset mainly feeds from the meadow `faostat_metadata`, but it also loads the `custom_*`
  files, and each individual meadow dataset.

All datasets `faostat_*` (except `faostat_metadata`) contain two tables:

* `faostat_*`: Processed data in long format.
    * Indexes:
        * `area_code`: Area code (identical to the original FAO area code).
        * `year`: Year.
        * `item_code`: Harmonized item code.
        * `element_code`: Harmonized element code.
    * Columns:
        * `country`: Harmonized country/region name.
        * `fao_country`: Original FAO country/region name.
        * `item`: Customized item name.
        * `fao_item`: Original FAO item name.
        * `element`: Customized element name.
        * `fao_element`: Original FAO element name. This is only meaningful if the variable existed in the original dataset;
          for example, OWID-generated per-capita variables did not exist in the original dataset, therefore their
          `fao_element` is irrelevant.
        * `unit`: Customized unit name (long version).
        * `unit short name`: Customized unit name (short version).
        * `fao_unit_short_name`: Original FAO unit name (short version).
        * `item_description`: Customized item description.
        * `element_description`: Customized element description.
        * `flag`: Original FAO flag (with some modifications).
        * `population_with_data`: Population of a country, or, for aggregate regions, the population of countries in the
          region for which there was data to aggregate.
        * `value`: Actual data value.
* `faostat_*_flat`: Flattened table in wide format.
    * Indexes:
        * `country`: Harmonized country/region name.
        * `year`: Year.
    * Columns: As many columns as combinations of harmonized element codes and harmonized item codes (plus an additional
      column for area code).

The `faostat_metadata` contains the following tables:

* `countries`: Metadata related to countries/regions.
    * Indexes:
        * `area_code`: Area code.
    * Columns:
        * `fao_country`: Original FAO country/region name.
        * `country`: Harmonized country/region name.
        * `members`: Members of each country/region, if any (e.g. the list of members for 'Europe' will contain the
        harmonized names of all current European countries).
* `datasets`: Metadata related to dataset titles and descriptions.
    * Indexes:
        * `dataset`: Dataset short name.
    * Columns:
        * `fao_dataset_title`: Original FAO title for the dataset.
        * `owid_dataset_title`: Customized dataset title.
        * `fao_dataset_description`: Original FAO dataset description.
        * `owid_dataset_description`: Customized dataset description.
  * `elements`: Metadata related to elements and units.
    * Indexes:
       * `element_code`: Harmonized element code.
    * Columns:
        * `dataset`: dataset short name.
        * `element`: Customized element name.
        * `fao_element`: Original FAO element name.
        * `element_description`: Customized element description.
        * `fao_element_description`: Original FAO element description.
        * `unit`: Customized unit name (long version).
        * `fao_unit`: Original FAO unit name (long version).
        * `unit_short_name`: Customized unit name (short version).
        * `fao_unit_short_name`: Original FAO unit (short version).
        * `owid_unit_factor`: Factor by which data should be multiplied to.
        * `owid_aggregation`: Type of aggregation to do to construct regions.
        * `was_per_capita`: 1 if a particular element was given per capita in the original data (0 otherwise).
        * `make_per_capita`: 1 to construct a per-capita element for a given element (0 otherwise).
  * `items`: Metadata related to items.
    * Indexes:
        * `item_code`: Harmonized item code.
    * Columns:
        * `dataset`: Dataset short name.
        * `item`: Customized item name.
        * `fao_item`: Original FAO name.
        * `item_description`: Customized item description.
        * `fao_item_description`: Original FAO item description.

### Explorers

Using data from garden, we create an additional dataset in the `explorers` channel:

* `food_explorer`: Global food explorer. It loads the data from the garden `faostat_food_explorer` (although not all of
  its data is used), but, instead of containing a big table with all products, each individual product is stored as a
  `csv` file. This data, stored in S3, is the one that feeds our
  [Global food explorer](https://ourworldindata.org/explorers/global-food).

## Workflow to keep data up-to-date

These are the steps OWID follows to ensure that FAOSTAT data is up-to-date, or to update one or more datasets for
which there is new data (let us call the new dataset version to be created `YYYY-MM-DD`):

0. Activate the etl poetry environment (from the root folder of the etl repository):
```bash
  poetry shell
```
1. Execute the ingestion script, to fetch data for any dataset that may have been updated in FAOSTAT.
If no dataset requires an update, the workflow stops here.

    !!! note

        This can be executed with the `-r` flag to simply check for updates without writing anything.

    ```bash
    python etl/scripts/faostat/create_new_snapshots.py
    ```

2. Create new meadow steps.

    ```bash
    python etl/scripts/faostat/create_new_steps.py -c meadow
    ```
3. Run the new etl meadow steps, to generate the meadow datasets.

    ```bash
    etl meadow/faostat/YYYY-MM-DD
    ```

4. Create new garden steps.

    ```bash
    python etl/scripts/faostat/create_new_steps.py -c garden
    ```

5. Run the new etl garden steps, to generate the garden datasets.

    ```bash
    etl garden/faostat/YYYY-MM-DD
    ```

    Optionally, set `INSPECT_ANOMALIES=True`, to visualize if anomalies that were detected in the previous version of the data are still present in the current version.

    ```bash
    INSPECT_ANOMALIES=True etl garden/faostat/YYYY-MM-DD
    ```

6. Inspect and update any possible changes of dataset/item/element/unit names and descriptions.

    ```bash
    python etl/scripts/faostat/update_custom_metadata.py
    ```

7. Create new grapher steps.

    ```bash
    python etl/scripts/faostat/create_new_steps.py -c grapher
    ```

8. Run the new etl grapher steps, to generate the grapher charts.

    ```bash
    etl faostat/YYYY-MM-DD --grapher
    ```

9. Generate chart revisions (showing a chart using an old version of a variable and the same chart using the new
version) for each dataset, to replace variables of a dataset from its second latest version to its latest version.

    ```bash
    python etl/scripts/faostat/create_chart_revisions.py -e
    ```

    !!! note

        This step may raise errors (because of limitations in our chart revision tool). If so, continue to the next step and come back to this one again. Keep repeating these two steps until there are no more errors (which may happen after two iterations).

10. Use OWID's internal approval tool to visually inspect changes between the old and new versions of updated charts, and
accept or reject changes.

11. Create a new explorers step. For the moment, this has to be done manually:
    * Duplicate the latest step in `etl/etl/steps/data/explorers/faostat/` and use the current date as the new version.
    * Duplicate entry of explorers step in the dag, and replace versions (of the step itself and its dependencies) by the corresponding latest versions.

12. Run the new etl explorers step, to generate the csv files for the global food explorer.

    ```bash
    etl explorers/faostat/YYYY-MM-DD/food_explorer
    ```

    Run internal sanity checks on the generated files.

13. Manually create a new garden dataset of additional variables `additional_variables` for the new version, and update its metadata. Then create a new grapher dataset too.
    !!! note

        In the future this could be handled automatically by one of the existing scripts.

14. Archive unnecessary DB datasets, and move old, unnecessary etl steps in the dag to the archive dag.
    ```bash
    python etl/scripts/faostat/archive_old_datasets.py -e
    ```

    !!! note

        This step may needs to be run several times, since archiving of steps currently needs to be done manually.

## Workflow to make changes to a dataset

### Removing outliers

If a new outlier in a dataset is detected, it can be added to `detected_outliers.json` in the latest garden version
folder.
Then run again the corresponding etl step.

### Adding or removing a domain

To add or remove a new domain, in principle it should be enough to add or remove its dataset code from the
`INCLUDED_DATASETS_CODES` list in `etl/scripts/faostat/shared.py`.
With this change, the next time that script is executed, it will only search for updates for the new list of datasets.
And then, the `create_new_steps.py` should only create steps for datasets (among those in the new lists) that have been
updated.


!!! note

    This workflow will add or remove a dataset from a new version. To add or remove a dataset from the current version, the dataset needs to be manually added or removed from the dag, and the appropriate step files need to be created or removed from the meadow, garden or grapher step folders.

### Customizing individual fields in a dataset

To customize a certain field (e.g. an element description) of a dataset:
1. Edit the `custom_*.csv` file in the latest garden folder.
2. Copy the garden step files of the affected datasets (the ones that were customized) to the latest garden folder, if
   they are not already in the latest version folder.
3. Force the execution of the garden `faostat_metadata` step, and of the affected garden datasets.

!!! note

    The current workflow will create a new version folder of steps if at least one dataset has been updated. When this happens, the common files (e.g. `shared.py` module, or the `custom_*.csv` files) will be copied from the previous onto the new folder. This implies that:

    * If something has to be changed in `shared.py` (for example, if a bug has been detected, or if a new feature is
      introduced), that file should be edited in all relevant versions (i.e. the ones that are the latest version of at
      least one dataset). Alternatively, consider creating a new folder with all datasets (even those that were not
      updated). This can be achieved using the `-a` flag of the `create_new_steps` script.
    * If the previous point happens often and becomes inconvenient, we could consider using the `-a` flag on every update.
      The downside of this approach is that we would be creating many datasets that are identical to their previous
      versions.

#### Customizing datasets

To customize the title or description of a dataset, edit the `custom_datasets.csv` file in the latest garden folder,
which contains the following columns:

* `dataset`: Dataset short name (e.g. `faostat_qcl`).
* `fao_dataset_title`: Dataset title in the original FAOSTAT data.
* `owid_dataset_title`: Customized dataset title.
* `fao_dataset_description`: Dataset description in the original FAOSTAT data.
* `owid_dataset_description`: Customized dataset description.

!!! note

    * The `fao_*` columns should only be customized if the titles or descriptions in the original FAOSTAT data have changed.
    * In the `custom_datasets.csv` file, all datasets are included (unlike other `custom_*.csv` files, where only customized
      fields are included).
    * Any empty `owid_*` field in the file will be assumed to be replaced with its corresponding `fao_*` field.

#### Customizing item names and descriptions

To customize the name or description of an item, edit the `custom_items.csv` file in the latest garden folder, which
contains the following columns:

* `dataset`: Dataset short name (e.g. `faostat_qcl`).
* `item_code`: FAOSTAT item code (after OWID harmonization).
* `fao_item`: Original FAOSTAT item name.
* `owid_item`: Customized item name.
* `fao_item_description`: Original FAOSTAT item description. This field is sometimes missing.
* `owid_item_description`: Customized item description.

!!! note

    * In the `custom_items.csv` files, for convenience, only fields that have been customized are included.
    * Any empty `owid_*` field in the file will be assumed to be replaced with its corresponding `fao_*` field.

#### Customizing element and unit names and descriptions

To customize various aspects of elements and units, edit the `custom_elements_and_units.csv` file in the latest garden
folder, which contains the following columns:

* `dataset`: Dataset short name (e.g. `faostat_qcl`).
* `element_code`: FAOSTAT element code (after OWID harmonization).
* `fao_element`: Original FAOSTAT element name.
* `owid_element`: Customized element name.
* `fao_unit`: Original FAOSTAT unit name (long version, e.g. `hectares`). This field is originally taken from the
  (additional) metadata, where it is considered a unit description (not a name). However, they are usually long versions
  of the name, not detailed descriptions. When this field is missing (because it was not returned by the API) the
  corresponding value from `fao_unit_short_name` will be taken (which is always given).
* `fao_unit_short_name`: Original FAOSTAT unit name (short version, e.g. `ha`). This field is originally taken from the
  actual data file of a dataset, where it is the unit name itself. But, since it is usually short (in contrast to the
  unit description above), we use this field for the abbreviation of the unit. This field is always given.
* `owid_unit`: Customized unit name (long version).
* `owid_unit_short_name`: Customized unit name (short version).
* `owid_unit_factor`: This is the number that will be multiplied to all values of the corresponding element-unit.
* `fao_element_description`: Original FAOSTAT element description. This field is sometimes missing.
* `owid_element_description`: Customized element description.
* `owid_aggregation`: Operation to apply to the data when creating region aggregates. It is either empty or it
  determines the operation to perform when aggregating (e.g. `sum` or `mean`). If this is empty, the element will not
  be present in region aggregates (meaning continents and income groups will miss this element). If it is `sum`, the
  values of the elements of the members of the region will be added together for each year.
* `was_per_capita`: 1 if the original element was given per capita, 0 otherwise. If it is 1, it will be multiplied by
  the population given by FAOSTAT (if this population is given, otherwise the execution of the corresponding garden step
  will raise an assertion error, in which case the variable should be kept as per-capita).
* `make_per_capita`: 1 to create an additional per-capita element, 0 otherwise. If it is 1, the element will be divided
  by the OWID population. The element code of the new variable will be the same as the original element code, but with
  the letters `pc` prepended (e.g. the per-capita variable of the original element with code `001234` will be `pc1234`).

!!! note
    * If a unit factor is applied, consider changing the unit names appropriately. For example, when multiplying by 1000,
      change the unit name from `thousand tonnes` to `tonnes`.
    * When making `was_per_capita` 1, the unit name should be changed accordingly from, e.g. `grams per capita` to `grams`.
    * In the `custom_elements_and_units.csv` file, for convenience, only fields that have been customized are included.
    * Any empty `owid_*` field in the file will be assumed to be replaced with its corresponding `fao_*` field.

## Data harmonization

In the garden steps, several fields are harmonized.

### Harmonization of year

Normally, year is just an integer (e.g. 2010, as expected).
But sometimes it is given as a range of exactly 2 years (e.g. '2010-2012').
When this happens, we replace this range by the intermediate year (e.g. 2011).

### Harmonization of country name

In the latest garden step, there should always be a file called `faostat.countries.json`, which contains a mapping
from FAO names to OWID names.
Of the hundreds of countries and regions in the FAOSTAT datasets:

* Some countries/regions are ignored (e.g. 'FAO Major Fishing Area'). They simply do not appear in our mapping, or in
  the output data.

* Some countries/regions are mapped to OWID harmonized country names (e.g. `"Viet Nam": "Vietnam"`).

    * Countries whose names are already harmonized OWID country names are also included in the mapping (e.g.
      `"Uganda": "Uganda"`).

* Some countries/regions for which there is no OWID harmonized name are kept the same, but with `* (FAO)` at the end of
  the name (e.g. `"Developing regions": "Developing regions (FAO)"`).

    * The same happens to continents: We create our own region aggregates (e.g. `Africa`), but want to keep the original
      FAO aggregates for comparison (e.g. `"Africa": "Africa (FAO)"`).

    !!! note

        * FAO defines China as the aggregate of Mainland China, Hong Kong, Macao and Taiwan.
          For consistency with OWID's common definition of China (which is Mainland China), we map:
            * `"China": "China (FAO)"`.
            * `"China, Hong Kong SAR": "Hong Kong"`.
            * `"China, Macao SAR": "Macao"`.
            * `"China, Taiwan Province of": "Taiwan"`.
            * `"China, mainland": "China"`.
        * As a justification for this mapping, we can see the following example:
          For item `Fats, Animals, Raw` and element `Feed`, dataset `faostat_fbs` has data for Taiwan, but not for Hong Kong,
          Macao, or Mainland China.
          Therefore, FAO's definition of China is showing only Taiwan's data.
          In such a case it seems more appropriate to simply have no data for China, and keep the data for Taiwan as a separate
          entity.
        * Dataset `faostat_fa` does not include any of the previous countries, but only `"China (excluding Hong Kong & Macao)"`.
          We do the mapping `"China (excluding Hong Kong & Macao)": "China"`, although it is unclear whether Taiwan is included.
          Therefore, for this dataset, we may be including Taiwan's data as part of China (which would be inconsistent with
          other datasets).

### Harmonization of item code

Most FAOSTAT datasets have items identified by a simple integer number (e.g. item code 15 corresponds to item `Wheat`).

Although for the same item code one can find slightly different item names in different datasets, they seem to always
refer to the same product.
For example, item code 27 has FAO item name `Rice, paddy` in datasets `faostat_ei`, `faostat_qcl`, `faostat_qi`, and
`faostat_tcl`, but item name `Rice` in dataset `faostat_qv`.

To be able to accommodate the numerical item codes of `faostat_scl` and the default item code, we convert item codes of
all datasets to a string of 8 characters (e.g. 15 becomes "00000015").
This number of characters is defined by `N_CHARACTERS_ITEM_CODE` in the `shared.py` module of the garden folder.

### Harmonization of element code

We have not identified issues with element codes like those in item codes.
However, for consistency (and just in case similar issues occur in the future), we also convert element codes into
strings of 6 digits (e.g. 1234 -> '001234').
This number of characters is defined by `N_CHARACTERS_ELEMENT_CODE` in the `shared.py` module of the garden folder.
