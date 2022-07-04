# FAOSTAT data

## Overview of the data

[FAOSTAT (Food and Agriculture Organization Corporate Statistical Database)](https://www.fao.org/faostat/en/#home) 
provides free access to food and agriculture data from 1961 to the most recent year available.

The data is distributed among different domains, each one with a unique dataset code (e.g. `qcl`).
We will create a new dataset (named, e.g. `faostat_qcl`) for each domain, although we are not including all FAOSTAT
domains, only the ones listed below.

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

Each dataset contains only one table (with the same name as the dataset itself).

There is an additional dataset:
* `faostat_metadata`: FAOSTAT (additional) metadata dataset (originally ingested in walden using the FAOSTAT API).
  This dataset contains as many tables as domain-categories (e.g. 'faostat_qcl_area', 'faostat_fbs_item', ...).
  All categories are defined in `category_structure` in the meadow `faostat_metadata.py` step file.

# Garden (processed) datasets

The list of garden datasets is identical to the list of meadow datasets, except for the following changes:
* Datasets `faostat_fbsh` and `faostat_fbs` are not be present.
* `faostat_fbsc`: Food Balances Combined. This dataset is the combination of `faostat_fbsh` and `faostat_fbs`.
* `faostat_food_explorer`: Dataset feeding the global food explorer.
  It uses data from the `faostat_fbsc` and `faostat_qcl` garden datasets.
* `faostat_metadata`: This dataset mainly feeds from the meadow `faostat_metadata`, but it also loads the `custom_*`
  files, and each individual meadow dataset.

All datasets (except `faostat_metadata`) contain two tables:
* One with the same name as the original dataset.
* A flat table, named after the original dataset, appended by `_flat`.
  This table is indexed by only `country` and `year`.
Exceptionally, the `faostat_metadata` contains the following tables:
* countries:
* datasets:
* items:
* elements:

# Explorers

Using data from garden, we create an additional dataset in the `explorers` channel:
* `food_explorer`: Global food explorer. It contains the same data as the garden `faostat_food_explorer`. But, instead
  of having a bit table with all products, each individual product is stored as a `csv` file. This data, stored in S3,
  is the one that feeds our [Global food explorer](https://ourworldindata.org/explorers/global-food).

## Workflow to keep data up-to-date

These are the steps OWID follows to ensure that FAOSTAT data is up-to-date, or to update one or more datasets for
which there is new data (let us call the new dataset version to be created `YYYY-MM-DD`):

0. Activate the etl poetry environment (from the root folder of the etl repository):
```bash
  poetry shell
```
1. Execute the walden ingest script, to fetch data for any dataset that may have been updated in FAOSTAT.
If no dataset requires an update, the workflow stops here.

    Note: This can be executed with the `-r` flag to simply check for updates without writing anything.
```bash
python vendor/walden/ingests/faostat.py
```
2. Execute this script for the meadow channel.
```bash
python etl/scripts/faostat/create_new_steps.py -c meadow
```
3. Run the new etl meadow steps, to generate the meadow datasets.
```bash
etl meadow/faostat/YYYY-MM-DD
```
4. Run this script again for the garden channel.
```bash
python etl/scripts/faostat/create_new_steps.py -c garden
```
5. Run the new etl garden steps, to generate the garden datasets.
```bash
etl garden/faostat/YYYY-MM-DD
```
6. Run this script again for the grapher channel.
```bash
python etl/scripts/faostat/create_new_steps.py -c grapher
```
7. Run the new etl grapher steps, to generate the grapher charts.
```bash
etl faostat/YYYY-MM-DD --grapher
```
8. Use OWID's internal approval tool to visually inspect changes between the old and new versions of updated charts, and
accept or reject changes.


## Workflow to make changes to a dataset

### Adding outliers

If a new outlier in a dataset is detected, it can be added to `OUTLIERS_TO_REMOVE` in the latest garden `shared.py`
module.
Since that module is not a data step itself, `etl` will not recognise that there has been a change.
Therefore, the garden step of the dataset with the outlier has to be forced (using the `--force` flag).

### Customizing individual fields in a dataset

#### Customizing datasets

To customize the title or description of a dataset, edit the `custom_datasets.csv` file in the latest garden folder,
which contains the following columns:
* `dataset`: Dataset short name (e.g. `faostat_qcl`).
* `fao_dataset_title`: Dataset title in the original FAOSTAT data.
* `owid_dataset_title`: Customized dataset title.
* `fao_dataset_description`: Dataset description in the original FAOSTAT data.
* `owid_dataset_description`: Customized dataset description.

Specifically, the only customizable columns are `owid_dataset_title` and `owid_dataset_description`.
The other `fao_*` columns should only be customized if the titles or descriptions in the original FAOSTAT data have
changed.

After the file has been edited, force the execution of the garden `faostat_metadata` step.
This will trigger the execution of all other garden datasets.
But only those datasets whose titles or descriptions were edited will experiment changes.

Please read the [General notes on customization](####general-notes-on-customization) section below. 

#### Customizing item names and descriptions

To customize the name or description of an item, edit the `custom_items.csv` file in the latest garden folder, which
contains the following columns:
* `dataset`: Dataset short name (e.g. `faostat_qcl`).
* `item_code`: FAOSTAT item code (after OWID harmonization).
* `fao_item`: Original FAOSTAT item name. 
* `owid_item`: Customized item name.
* `fao_item_description`: Original FAOSTAT item description. This field is sometimes missing.
* `owid_item_description`: Customized item description.

Please read the [General notes on customization](####general-notes-on-customization) section below. 

#### Customizing element and unit names and descriptions

To customize various aspects of elements and units, edit the `custom_elements_and_units.csv` file in the latest garden
folder, which contains the following columns:
* `dataset`: Dataset short name (e.g. `faostat_qcl`).
* `element_code`: FAOSTAT element code (after OWID harmonization).
* `fao_element`: Original FAOSTAT element name.
* `owid_element`: Customized element name.
* `fao_unit`: Original FAOSTAT unit name (long version, e.g. `hectares`). This field is originally taken from the
  (additional) metadata, where it is considered a unit description (not a name). However, they are usually long versions
  of the name, not detailed descriptions. This field is sometimes missing.
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
  the population given by FAOSTAT (if it is given, otherwise the execution of the corresponding garden step will raise
  an assertion error, in which case the variable should be kept as per-capita).
* `make_per_capita`: 1 to create an additional per-capita element, 0 otherwise. If it is 1, the element will be divided
  by the OWID population. The element code of the new variable will be the same as the original element code, but with
  the letters `pc` prepended (e.g. the per-capita variable of the original element with code `001234` will be `pc1234`).

NOTES:
* TODO: Explain how missing unit fields are filled (check code).
* If a unit factor is applied, consider changing the unit names appropriately. For example, when multiplying by 1000,
  change the unit name from `thousand tonnes` to `tonnes`.
* When making `was_per_capita` 1, the unit name should be changed accordingly from, e.g. `grams per capita` to `grams`.

Please read further notes in the [General notes on customization](####general-notes-on-customization) section below. 

#### General notes on customization

TODO: Explain what happens if only one dataset is updated, in its own folder. If something changes that affects a new dataset, it should be manually moved to the new folder.
* In the `custom_datasets.csv` file, all datasets are included (unlike other `custom_*.csv` files, where only customized
  fields are included).
* In the `custom_items.csv` and `custom_elements_and_units.csv` files, for convenience, only fields that have been
  customized are included.
* Any empty `owid_*` field in the file will be assumed to be replaced with its corresponding `fao_*` field.

