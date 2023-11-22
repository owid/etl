# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
# ---
# pyright: reportUnusedExpression=false

# %% [markdown]
# # Food Explorer
#
# _Open as notebook with jupytext_
#
# Produced using garden-level FAOstat datasets.
#
# So far the following datasets have been processed:
#
# - [x] QCL
# - [x] FBSC (FBS, FBSH)
#
#
# We process both datasets in parallel, until the _Final Processing_ section, where we actually merge the datasets.

# %% [markdown]
# ## 1. Imports & paths
# Import the required libraries and define paths to load files (including data files and standardisation mappings for item and element names).

# %%
import json
from typing import Any, Dict, List, Optional, cast

import numpy as np
import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta

from etl.paths import BASE_DIR, DATA_DIR

# %%
HERE = BASE_DIR / "etl/steps/data/explorers/owid/2021"

# %%
PATH_DATASET_QCL = DATA_DIR / "garden/faostat/2021-03-18/faostat_qcl"
PATH_DATASET_FBSC = DATA_DIR / "garden/faostat/2021-04-09/faostat_fbsc"
PATH_DATASET_POPULATION = DATA_DIR / "garden/owid/latest/key_indicators"
PATH_DATASET_POPULATION_GAPMINDER = DATA_DIR / "open_numbers/open_numbers/latest/gapminder__systema_globalis"  # add

PATH_MAP_ITEM = HERE / "food_explorer.items.std.csv"
PATH_MAP_ELEM = HERE / "food_explorer.elements.std.csv"
PATH_REGIONS = HERE / "food_explorer.regions.json"
PATH_OUTLIERS = HERE / "food_explorer.outliers.json"

# %% [markdown]
# ## 2. Load garden dataset
# In this step we load the required datasets from Garden: QCL and FBSC.

# %%
qcl_garden = catalog.Dataset(PATH_DATASET_QCL)
fbsc_garden = catalog.Dataset(PATH_DATASET_FBSC)

# %% [markdown]
# We obtain table `bulk` from the dataset, which contains the data itself.

# %%
# Bulk data and items metadata
qcl_bulk = qcl_garden["bulk"]
fbsc_bulk = fbsc_garden["bulk"]

# %% [markdown]
# In the following step we discard column `variable_name`, which although useful for its clarity we don't actually need it in this process. Also, we reset the index as this will be needed in following operations.

# %%
# QCL
qcl_bulk = qcl_bulk.reset_index()
qcl_bulk = qcl_bulk.drop(columns=["variable_name"])
# FBSC
fbsc_bulk = fbsc_bulk.reset_index()
fbsc_bulk = fbsc_bulk.drop(columns=["variable_name"])

# %% [markdown]
# Brief overview of the data.

# %%
# QCL
print(qcl_bulk.shape)
qcl_bulk.head()

# %%
# FBSC
print(fbsc_bulk.shape)
fbsc_bulk.head()

# %% [markdown]
# ### Group some items
# We know from Garden process to generate the FBSC dataset, that there are some items that "changed" its ID from one dataset to another:
#
# - `2556 Groundnuts (Shelled Eq)` --> `2552 Groundnuts`
# - `2805 Rice (Milled Equivalent)` --> `2807 Rice and products`


# %%
def group_item_codes(
    df: pd.DataFrame, ids_old: List[int], ids_new: List[int], assign_to_old: List[bool]
) -> pd.DataFrame:
    # Check
    msk = df["item_code"].isin(ids_old + ids_new)
    x = df[msk].groupby("item_code").agg({"year": ["min", "max"]})
    for id_old, id_new in zip(ids_old, ids_new):
        assert x.loc[id_new, ("year", "min")] > x.loc[id_old, ("year", "max")]
    # Replace
    if isinstance(assign_to_old, list):
        id_map = dict((n, o) if f else (o, n) for o, n, f in zip(ids_old, ids_new, assign_to_old))
    elif assign_to_old:
        id_map = dict(zip(ids_new, ids_old))
    else:
        id_map = dict(zip(ids_old, ids_new))
    print(id_map)
    df["item_code"] = df["item_code"].replace(id_map).astype(int)
    return df


# %%
fbsc_bulk = group_item_codes(fbsc_bulk, ids_old=[2556, 2805], ids_new=[2552, 2807], assign_to_old=[True, True])

# %% [markdown]
# ## 3. Select flags
# There are cases where we have more than just one entry for a `country`, `item_code`, `element_code` and `year`. This is due to the fact that there are multiple ways of reporting the data. All these different methodologies are identified by the field `flag`, which tells us how a data point was obtained (see table below). This is given by FAOstat.
#
# |flag   |description                                                                        |
# |-------|-----------------------------------------------------------------------------------|
# |`*`      |       Unofficial figure                                                           |
# |`NaN`    | Official data                                                                     |
# |`A`      |       Aggregate; may include official; semi-official; estimated or calculated data|
# |`F`      |       FAO estimate                                                                |
# |`Fc`     |      Calculated data                                                              |
# |`Im`     |      FAO data based on imputation methodology                                     |
# |`M`      |       Data not available                                                          |
# |`S`      |       Standardised                                                                |
# |`SD`     |       Statistical Discrepancy                                                     |
# |`R`      |       Estimated data using trading partners database                              |
#
#
# The following cell examines how many datapoints would be removed if we did _flag-prioritisation_. As per the output, we see that we would eliminate 30,688 rows (~1% of the data).


# %%
def check_flags_1(df: pd.DataFrame) -> None:
    i_og = df.index.tolist()
    i_ne = df.drop_duplicates(subset=["country", "item_code", "element_code", "year"]).index.tolist()
    print(
        f"Number of datapoints: {len(i_og)}\nNumber of datapoints (after dropping duplicates): {len(i_ne)}\nTotal datapoints removed: {len(i_og)-len(i_ne)}"
    )
    check_flags_2(df, i_og, i_ne)


def check_flags_2(df: pd.DataFrame, i_og: List[int], i_ne: List[int]) -> None:
    """Prints `[number of datapoints eliminated], True`"""
    df = df.set_index(["country", "item_code", "element_code", "year"])
    dups = df.index.duplicated()
    print(f"{dups.sum()}, {len(i_ne) == len(i_og)-dups.sum()}")
    # dups = qcl_bulk.index.duplicated(keep=False)
    df = df.reset_index()


check_flags_1(qcl_bulk)
print()
check_flags_1(fbsc_bulk)

# %% [markdown]
# ### Flag prioritzation

# %% [markdown]
# In this step we define a flag prioritisation rank, which allows us to discard duplicate entries based on which flag we "prefer". We do this by assigning a weight to each datapoint based on their `flag` value (the higher, the more prioritised it is). On top of flag prioritisation, we always prefer non-`NaN` values regardless of their associated `flag` value (we assign weight -1 to this datapoints). The weighting was shared and discussed with authors.
#
# The weight is added to the dataframe as a new column `flag_priority`.
#
# #### Example 1
#
#     country, year, product, value, flag
#     Afghanistan, 1993, Apple, 100, F
#     Afghanistan, 1993, Apple, 120, A
#
# We would choose first row, with flag F.
#
# #### Example 2:
#
#     country, year, product, value, flag
#     Afghanistan, 1993, Apple, NaN, F
#     Afghanistan, 1993, Apple, 120, A
#
# We would choose second row, as first row is `NaN`.
#
#
# In the following cell we filter rows based on `FLAG_PRIORITIES`.

# %%
# Create flag priority (add to df) More info at https://www.fao.org/faostat/en/#definitions
FLAG_PRIORITIES = {
    "M": 0,  # Data not available
    "SD": 10,  # Statistical Discrepancy
    "*": 20,  # Unofficial figure
    "R": 30,  # Estimated data using trading partners database
    "Fc": 40,  # Calculated data
    "S": 60,  # Standardized data
    "A": 70,  # Aggregate; may include official; semi-official; estimated or calculated data
    "Im": 80,  # FAO data based on imputation methodology
    "F": 90,  # FAO estimate
    np.nan: 100,  # Official data
}


def filter_by_flag_priority(df: pd.DataFrame) -> pd.DataFrame:
    # Add flag priority column
    df.loc[:, "flag_priority"] = df.flag.replace(FLAG_PRIORITIES).tolist()
    df.loc[df.value.isna(), "flag_priority"] = -1
    # Remove duplicates based on flag value
    df = df.sort_values("flag_priority")
    df = df.drop_duplicates(subset=["country", "item_code", "element_code", "year"], keep="last")
    return df.drop(columns=["flag_priority", "flag"])


# %%
# QCL
qcl_bulk = filter_by_flag_priority(qcl_bulk)
print(qcl_bulk.shape)

# %%
# FBSC
fbsc_bulk = filter_by_flag_priority(fbsc_bulk)
print(fbsc_bulk.shape)

# %% [markdown]
# ## 4. Element Overview
# This serves as an initial check on the meaning of `element_code` values. In particular, we note that each `element_code` value corresponds to a unique pair of _element name_  and _element unit_. Note, for instance, that _element_name_ "production" can come in different flavours (i.e. units): "production -- tones" and "production -- 1000 No".
#
# Based on the number of occurrences of each element_code, we may want to keep only those that rank high.
#
# **Note: This step uses file `PATH_MAP_ELEM`, which is a file that was generated using the code in a later cell.**


# %%
# Where do each element appear?
def get_stats_elements(df: pd.DataFrame) -> pd.DataFrame:
    res = df.reset_index().groupby("element_code")["item_code"].nunique()
    df_elem = pd.read_csv(PATH_MAP_ELEM, index_col="code")
    elem_map = df_elem["name"] + " -- " + df_elem["unit"] + " -- " + df_elem.index.astype(str)
    res = res.rename(index=elem_map.to_dict()).sort_values(ascending=False)
    return cast(pd.DataFrame, res)


# %%
# QCL
get_stats_elements(qcl_bulk)

# %%
# FBSC
get_stats_elements(fbsc_bulk)

# %% [markdown]
# ## 5. Reshape dataset
# This step is simple and brief. It attempts to pivot the dataset in order to have three identifying columns (i.e. "keys") and several "value" columns based on the `element_code` and `Value` columns.
#
# This format is more Grapher/Explorer friendly, as it clearly divides the dataset columns into: Entities, year, [Values].


# %%
def reshape_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()
    df = df.pivot(index=["country", "item_code", "year"], columns="element_code", values="value")
    return df


# %%
# QCL
qcl_bulk = reshape_df(qcl_bulk)
# FBSC
fbsc_bulk = reshape_df(fbsc_bulk)

# %%
print("QCL:", qcl_bulk.shape)
print("FBSC:", fbsc_bulk.shape)

# %% [markdown]
# ## 6. Standardise Element and Item names (OPTIONAL)
# In the following cells we obtain tables with the code, current name and number of occurrences of all the Items and Elements present in our dataset.
#
# Based on this tables, Hannah (or another researcher), will revisit these and:
# - Select those Items and Elements that we are interested in.
# - Standardise naming proposals of Items and Elements.
#
# Notes:
# - We obtain the number of occurrences as this can assist the researcher in prioritising Items or Elements.

# %% [markdown]
# ### Elements
# Here we obtain a table with the current namings for Elements (plus other variables). Note that we also propagate the unit names, as these may also be standardised (or even changed).

# %%
# Load table from dataset containing Element information
qcl_elem = qcl_garden["meta_qcl_element"]
fbsc_elem = fbsc_garden["meta_fbs_element"]


# %%
def get_elements_to_standardize(df: pd.DataFrame, df_elem: pd.DataFrame) -> pd.DataFrame:
    # Obtain number of occurrences for each element_code (each column is an element)
    elements = pd.DataFrame(df.notna().sum()).reset_index()
    elements = elements.sort_values(0, ascending=False)  # type: ignore
    # Add names and unit info to the table
    elements = elements.merge(
        df_elem[["element", "unit", "unit_description"]],
        left_on="element_code",
        right_index=True,
    )
    # Rename column names
    elements = elements.rename(
        columns={
            "element_code": "code",
            0: "number_occurrences",
            "element": "name",
            "unit": "unit",
            "unit_description": "unit_description",
        }
    )[["code", "name", "unit", "unit_description", "number_occurrences"]]
    return elements


# %%
elements_qcl = get_elements_to_standardize(qcl_bulk, qcl_elem).assign(dataset="QCL")
elements_fbsc = get_elements_to_standardize(fbsc_bulk, fbsc_elem).assign(dataset="FBSC")

assert elements_qcl.merge(elements_fbsc, on="code").empty

# %% [markdown]
# Once the table is obtained, we take a look at it and export it. Note that we use a filename starting with `ign.`, as these are note git-tracked.

# %%
elements = pd.concat([elements_qcl, elements_fbsc])
elements.head()

# %%
# elements.to_csv("ign.food.elements.csv", index=False)

# %% [markdown]
# ### Items
# Here we obtain a table with the current namings for Items (plus other variables).

# %%
# Load table from dataset containing Item information
qcl_item = qcl_garden["meta_qcl_item"]
fbsc_item = fbsc_garden["meta_item"]

# %% [markdown]
# As the following cell shows, this table comes with a multi-index, as codes may actually be referring to "item_groups" or "Items".

# %%
qcl_item.head()

# %% [markdown]
# Therefore, in the next cell we attempt to flatten code to name mappings.
#
# To this end:
# - We first create two separate dictionaries, mapping `item_group_code --> item_group` and `item_code --> Item`, respectively.
# - We note, however, that some codes appear both as "Items" and "item_groups". This might be due to the fact that there are more than one level of items. That is, an Item can "belong" to an item_group, which in turn belongs to yet a higher up item_group. Therefore, we remove these codes from the item dictionary so they only appear in the item_group dictionary.
# - Next, we create a table with all items, their occurrences, whether they are item_groups, and their FAO original namings.


# %%
def get_items_to_standardize(df: pd.DataFrame, df_item: pd.DataFrame) -> pd.DataFrame:
    # Group
    map_item_g = dict(
        zip(
            df_item.index.get_level_values("item_group_code").astype(str),
            df_item["item_group"],
        )
    )
    # Item
    map_item = dict(zip(df_item.index.get_level_values("item_code").astype(str), df_item["item"]))

    # Correct
    map_item = {k: v for k, v in map_item.items() if k not in map_item_g}

    # Load item occurences
    items = (
        pd.DataFrame(df.reset_index()["item_code"].value_counts())
        .reset_index()
        .astype(str)
        .rename(
            columns={
                "index": "code",
                "item_code": "number_occurences",
            }
        )
    )
    # Add flag for groups
    items["type"] = items["code"].isin(map_item_g).apply(lambda x: "Group" if x else None)
    # Add name
    map_item_all = {**map_item, **map_item_g}
    items["name"] = items.code.replace(map_item_all)
    # Order columns
    items = items[["code", "name", "type", "number_occurences"]]
    return items


# %%
items_qcl = get_items_to_standardize(qcl_bulk, qcl_item).assign(dataset="QCL")
items_fbsc = get_items_to_standardize(fbsc_bulk, fbsc_item).assign(dataset="FBSC")
items = pd.concat([items_qcl, items_fbsc])

# %% [markdown]
# Once the table is obtained, we take a look at it and export it. Note that we use a filename starting with `ign.`, as these are note git-tracked.

# %%
items.head()

# %%
# items.to_csv("ign.food.items.csv", index=False)

# %% [markdown]
# ## 7. Renaming Items and Elements
# After the previous step, where we shared files `ign.food.items.csv` and `ign.food.elements.csv` with a researcher, they will review them and add the standardisation namings for all items and elements that we intend to use. Note that if no standardised name is provided, the item or element will be discarded.
#
# Their proposals come in two files: `food_explorer.items.std.csv` and `food_explorer.elements.std.csv`. Note that we prefer working with the mapping `"item/element_code" ---> "new standardised item/element name"`.

# %% [markdown]
# ### Element

# %% [markdown]
# First of all, we load the standardisation table and remove NaN values (these belong to to-be-discarded elements).

# %%
# Get standardised values
df = pd.read_csv(PATH_MAP_ELEM, index_col="code")
df = df.dropna(subset=["name_standardised"])

# %% [markdown]
# If we display the content of the standardisation element file we observe that:
# - Only some elements are preserved.
# - There is the column `unit_name_standardised_with_conversion` and `unit_factor`, which provide the new unit and the factor to convert the old one into the new one.
# - Multiple codes are assigned to the same `name_standardised` and `unit_name_standardised_with_conversion`, which means that we will have to merge them. In particular, element "Yield" with unit "kg/animal" appears with four different codes!

# %%
# Show
df

# %% [markdown]
# We keep columns in data file that belong to the "elements of interest" (those with renaming).

# %%
# Filter elements of interest
qcl_bulk = qcl_bulk[[col for col in df.index if col in qcl_bulk.columns]]
fbsc_bulk = fbsc_bulk[[col for col in df.index if col in fbsc_bulk.columns]]

# %% [markdown]
# We modify the values of some elements, based on the new units and `unit_factor` values.

# %%
# Factor
qcl_bulk = qcl_bulk.multiply(df.loc[qcl_bulk.columns, "unit_factor"])
fbsc_bulk = fbsc_bulk.multiply(df.loc[fbsc_bulk.columns, "unit_factor"])

# %% [markdown]
# Next, we merge codes into single codes:
# - **Yield**: `5417, 5420, 5424, 5410 ---> 5417` (QCL)
# - **Animals slaughtered**: `5320, 5321 ---> 5320` (QCL)
#
# As previously highlighted, all of them are mapped to the same (name, unit) tupple.

# %%
# QCL
item_code_merge = {
    5417: [5420, 5424, 5410],
    5320: [5321],
}
items_drop = [ii for i in item_code_merge.values() for ii in i]
for code_new, codes_old in item_code_merge.items():
    for code_old in codes_old:
        qcl_bulk[code_new] = qcl_bulk[code_new].fillna(qcl_bulk[code_old])
qcl_bulk = qcl_bulk.drop(columns=items_drop)

# %% [markdown]
# Finally, we rename the column names (so far element_codes) to more prosaic element identifiers (`[element-name]__[unit]`).

# %%
# Build element name
a = df["name_standardised"].apply(lambda x: x.lower().replace(" ", "_")).astype(str)
b = df["unit_name_standardised_with_conversion"].apply(lambda x: x.lower().replace(" ", "_")).astype(str)
df["element_name"] = (a + "__" + b).tolist()
# Obtain dict element_code -> element name
map_elem = df["element_name"].to_dict()

# %%
# Change columns names
qcl_bulk = qcl_bulk.rename(columns=map_elem)
fbsc_bulk = fbsc_bulk.rename(columns=map_elem)

# %%
# Show dataframe with standardised element names
qcl_bulk.head()

# %% [markdown]
# ### Item
# We now load the standardisation item table and remove `NaN` values (these belong to to-be-discarded items).

# %%
# Get standardised values
df = pd.read_csv(PATH_MAP_ITEM, index_col="code")
map_item_std = df.dropna(subset=["name_standardised"])["name_standardised"].to_dict()

# %% [markdown]
# Briefly display first 10 mappings.

# %%
{k: v for (k, v) in list(map_item_std.items())[:10]}

# %% [markdown]
# Next, we do a simple check of item name uniqueness. Note that we can have multiple codes assigned to the same `name_standardised`, as part of the standardisation process, BUT these should be in different datasets so we don't have any element conflicts.

# %%
# Show "fused" products from QCL and FBSC
x = pd.DataFrame.from_dict(map_item_std, orient="index", columns=["name"]).reset_index()
x = x.groupby("name").index.unique().apply(list)
x = x[x.apply(len) > 1]
print("There are", len(x), "fused products:\n", x)

# %%
# Check `code` --> `name_standardised` is unique in each dataset
assert (
    df.dropna(subset=["name_standardised"]).reset_index().groupby(["dataset", "name_standardised"]).code.nunique().max()
    == 1
)

# %% [markdown]
# Next, we filter out items that we are not interested in and add a new column (`product`) with the standardised item names.


# %%
def standardise_product_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()
    df = df[df["item_code"].isin(map_item_std)]
    df.loc[:, "product"] = df["item_code"].replace(map_item_std).tolist()
    df = df.drop(columns=["item_code"])
    # Set back index
    df = df.set_index(["product", "country", "year"])
    return df


# %%
qcl_bulk = standardise_product_names(qcl_bulk)
fbsc_bulk = standardise_product_names(fbsc_bulk)

# %% [markdown]
# ## 8. Dataset merge
# Here we add the final processing steps:
# - Merge datasets `QCL` + `FBSC`
# - Discard products (former items) that do not contain any value for the "elements of interest".

# %%
# Merge datasets
fe_bulk = pd.merge(qcl_bulk, fbsc_bulk, how="outer", left_index=True, right_index=True)

# %%
print("QCL // shape:", qcl_bulk.shape, "/ not-NaN:", qcl_bulk.notna().sum().sum())
print("FBSC // shape:", fbsc_bulk.shape, "/ not-NaN:", fbsc_bulk.notna().sum().sum())
print("FE // shape:", fe_bulk.shape, "/ not-NaN:", fe_bulk.notna().sum().sum())

# %%
# Drop nulls (some products dont have any value for the elements of interest)
fe_bulk = fe_bulk.dropna(how="all")
print("FE (after NaN-drop):", fe_bulk.shape)

# %%
print(fe_bulk.shape)
fe_bulk.head()

# %% [markdown]
# ## 9. Post processing
# In this section we obtain the metrics for all regions and add per-capita counterparts. So far, we include income groups by the World Bank, continents as defined by OWID and World. The values for these entities are obtained using only data present in the dataset (i.e. some countries may be missing).
#
#
# - Normalize metrics
#     - Add population column
#     - Weight columns
#     - Rename columns
# - Obtain metrics for regions
# - Add population column, including regions

# %%
# fe_bulk_orig = fe_bulk.copy()

# %%
fe_bulk = fe_bulk.reset_index()

# %% [markdown]
# ### 9.0 Build population table

# %%
# Load population dataset
indicators = catalog.Dataset(PATH_DATASET_POPULATION)
population = indicators["population"][["population"]].reset_index()

# %%
# Load from gapminder (former countries)
# more info: https://github.com/open-numbers/ddf--gapminder--systema_globalis/blob/master/ddf--entities--geo--country.csv
gapminder = catalog.Dataset(PATH_DATASET_POPULATION_GAPMINDER)
population_gap = (
    gapminder["total_population_with_projections"]
    .reset_index()
    .rename(columns={"time": "year", "total_population_with_projections": "population"})
)

gapminder_country_codes = {
    "ussr": "USSR",
    "cheslo": "Czechoslovakia",
    "yug": "Yugoslavia",
    "eri_a_eth": "Ethiopia (former)",
    "scg": "Serbia and Montenegro",
}
former_states = list(gapminder_country_codes.values())

population_gap = population_gap[population_gap.geo.isin(gapminder_country_codes)]
population_gap = population_gap.assign(country=population_gap.geo.map(gapminder_country_codes)).drop(columns=["geo"])

# Filter years (former states only for past interval, not overlapping with current countries)
date_window = (
    fe_bulk[fe_bulk.country.isin(former_states)].groupby("country").year.agg(["min", "max"]).to_dict(orient="index")
)
population_ = []
for state, dates in date_window.items():
    df_ = population_gap[
        (population_gap.country == state)
        & (population_gap.year >= dates["min"])
        & (population_gap.year <= dates["max"])
    ]
    population_.append(df_)

population_gap = pd.concat(population_, ignore_index=True)

# Index
population_gap = population_gap.set_index(["country", "year"], verify_integrity=True)

# %%
# Ensure no overlapping
former_to_current = {
    "USSR": [
        "Lithuania",
        "Georgia",
        "Estonia",
        "Latvia",
        "Ukraine",
        "Moldova",
        "Kyrgyzstan",
        "Uzbekistan",
        "Tajikistan",
        "Armenia",
        "Azerbaijan",
        "Turkmenistan",
        "Belarus",
        "Russia",
        "Kazakhstan",
    ],
    "Yugoslavia": [
        "Croatia",
        "Slovenia",
        "North Macedonia",
        "Bosnia and Herzegovina",
        "Serbia",
        "Montenegro",
    ],
    "Czechoslovakia": ["Czechia", "Slovakia"],
    "Ethiopia (former)": ["Ethiopia", "Eritrea"],
    "Serbia and Montenegro": ["Serbia", "Montenegro"],
    "Sudan (former)": ["Sudan", "South Sudan"],
}
former_states = list(former_to_current.keys())

for former, current in former_to_current.items():
    msk = fe_bulk.country.isin(current)
    current_start = fe_bulk.loc[msk, "year"].min()
    former_end = fe_bulk.loc[fe_bulk.country == former, "year"].max()
    assert former_end < current_start

# %%
# Estimate Sudan (former)
msk = population.country.isin(["South Sudan", "Sudan"]) & (population.year < 2012)
pop_sudan = population[msk].groupby("year", as_index=False).population.sum().assign(country="Sudan (former)")
population = pd.concat([pop_sudan, population], ignore_index=True)
date_window = date_window | {"Sudan (former)": {"min": 1961, "max": 2011}}

# %%
# Filter current states that did not exist
msk = None
for former, current in former_to_current.items():
    if msk is None:
        msk = population.country.isin(former_to_current[former]) & (population.year <= date_window[former]["max"])
    else:
        msk |= population.country.isin(former_to_current[former]) & (population.year <= date_window[former]["max"])
population = population[(population.year >= fe_bulk.year.min()) & (population.year <= fe_bulk.year.max())].astype(
    {"year": int}
)
population = population.loc[~msk]  # type: ignore
population = population.set_index(["country", "year"], verify_integrity=True)

# %%
# Merge
population = pd.concat([population, population_gap])

# %% [markdown]
# ### 9.1 Normalize metrics
# In this section, we undo the _per_capita_ part of some metrics. We do this so we can aggregate countries into regions and later normalize by the total population.

# %% [markdown]
# #### Add population column

# %%
countries_pop = set(population.index.levels[0])  # type: ignore
countries = set(fe_bulk.country)
print(f"Missing {len(countries_missing := countries.difference(countries_pop))} countries: {countries_missing}")
if len(countries_missing) > 17:
    raise ValueError("More countries missing than expected!")

# %%
shape_first = fe_bulk.shape[0]
fe_bulk = fe_bulk.merge(population, left_on=["country", "year"], right_on=["country", "year"])
print(f"Decrease of {round(100*(1-fe_bulk.shape[0]/shape_first))}% rows")

# %% [markdown]
# #### Weight columns

# %%
# Define which columns will be weighted
keyword = "_per_capita"
columns_per_capita = {col: col.replace(keyword, "") for col in fe_bulk.columns if keyword in col}
# Normalize and rename columns
fe_bulk[list(columns_per_capita)] = fe_bulk[list(columns_per_capita)].multiply(fe_bulk["population"], axis=0)
fe_bulk = fe_bulk.rename(columns=columns_per_capita).drop(columns=["population"])

# %% [markdown]
# ### 9.2 Add regions
# Here we obtain the metrics for each region (continents, income groups and World). We avoid computing the aggregates for metrics relative to land use and animal use, as for these we would need the number of land and animals used per country. We can estimate `yield__tonnes_per_ha`, with other available metrics but will leave `yield__kg_per_animal` as NaN for all regions.

# %% [markdown]
# #### Create mappings Country ---> Region

# %%
# Load region map
with open(PATH_REGIONS, "r") as f:
    regions = json.load(f)
regions_all = ["World"] + list(regions)

income = [
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
]
continents = [
    "Antarctica",
    "Africa",
    "Asia",
    "Europe",
    "South America",
    "North America",
    "Oceania",
]
country2continent = {vv: k for k, v in regions.items() for vv in v if k in continents}
country2income = {vv: k for k, v in regions.items() for vv in v if k in income}

# %%
# Ensure former states presence
country2continent["Sudan (former)"] = "Africa"
country2income = {
    **country2income,
    "Czechoslovakia": "High-income countries",
    "Ethiopia (former)": "Low-income countries",
    "Serbia and Montenegro": "Upper-middle-income countries",
    "Yugoslavia": "Upper-middle-income countries",
    "USSR": "Upper-middle-income countries",
    "Sudan (former)": "Low-income countries",
}
for state in former_states:
    assert state in country2continent

# %% [markdown]
# #### Remove default regions (if any)

# %%
fe_bulk = fe_bulk.loc[~fe_bulk.country.isin(regions_all)].reset_index(drop=True)

# %% [markdown]
# #### Function and variables to get metrics for regions
# Definition of functions recurrently needed and some variables


# %%
def get_df_regions(
    df: pd.DataFrame,
    mapping: Dict[Any, Any],
    column_location: str,
    columns_index: List[str],
    columns_aggregate: Optional[List[str]] = None,
) -> pd.DataFrame:
    # TODO: flag whenever all (production__tonnes, area_harvested__ha) are available
    # Continents
    df_regions = df.assign(**{column_location: df[column_location].replace(mapping)})
    if columns_aggregate is not None:
        df_regions = df_regions.groupby(columns_index, as_index=False)[columns_aggregate].sum(min_count=1)
    else:
        df_regions = df_regions.groupby(columns_index, as_index=False).sum(min_count=1)
    # Only keep new regions
    msk = df_regions[column_location].isin(set(mapping.values()))
    df_regions = df_regions.loc[msk]
    print(f"{round(100*df_regions.shape[0]/df.shape[0], 2)}% increase in rows")
    return df_regions


# %%
columns_index = ["product", "country", "year"]
columns_exclude = columns_index + ["yield__tonnes_per_ha", "yield__kg_per_animal"]
columns_aggregate = [col for col in fe_bulk.columns if col not in columns_exclude]

# %% [markdown]
# #### Estimate region data

# %%
# World
fe_bulk_world = (
    fe_bulk.groupby(["product", "year"], as_index=False)[columns_aggregate].sum(min_count=1).assign(country="World")
)
print(f"{round(100*fe_bulk_world.shape[0]/fe_bulk.shape[0], 2)}% increase in rows")
# Continents
fe_bulk_continent = get_df_regions(fe_bulk, country2continent, "country", columns_index, columns_aggregate)
# Income groups
fe_bulk_income = get_df_regions(fe_bulk, country2income, "country", columns_index, columns_aggregate)

# %% [markdown]
# #### Merge

# %%
# Concatenate
fe_bulk = pd.concat([fe_bulk, fe_bulk_world, fe_bulk_continent, fe_bulk_income])

# %% [markdown]
# #### Add missing metrics for regions

# %%
msk = (
    (fe_bulk.country.isin(regions_all)) & (fe_bulk["area_harvested__ha"] != 0) & (~fe_bulk["area_harvested__ha"].isna())
)
fe_bulk.loc[msk, "yield__tonnes_per_ha"] = (
    fe_bulk.loc[msk, "production__tonnes"] / fe_bulk.loc[msk, "area_harvested__ha"]
)

# %% [markdown]
# ### 9.3 Population
# Next, we will add a column with the population of each country (or region). Note that some regions are not present in the population dataset, hence we first need to add these.

# %%
# Load population dataset
population = population.reset_index()

# %%
# Remove regions
population = population[~population.country.isin(set(country2continent.values()))]
# Remove income groups
population = population[~population.country.isin(set(country2income.values()))]

# %% [markdown]
# #### Obtain continent and income group populations

# %%
population_continent = get_df_regions(population, country2continent, "country", ["country", "year"])
population_income = get_df_regions(population, country2income, "country", ["country", "year"])

# %%
# Concatenate
population = pd.concat([population, population_continent, population_income])
population = population.set_index(["country", "year"])

# %% [markdown]
# #### Add `population` column

# %%
fe_bulk = fe_bulk.merge(population, left_on=["country", "year"], right_index=True)

# %%
fe_bulk = fe_bulk.set_index(["product", "country", "year"], verify_integrity=True).sort_index()

# %% [markdown]
# ### 9.4 Value checks

# %% [markdown]
# #### Remove values for _food_available_for_consumption__kcal_per_day
# We remove values for metric `food_available_for_consumption__kcal_per_day` whenever they seem wrong. Our criteria is to find out if for a given `(item,country)` this metric only has few values. We define _few_ as below a pre-defined threshold `th`.
#
# Note, here removing means assigning `NaN` to this metric for the rows considered.

# %%
# Overview of the distribution of different metric values
res = fe_bulk.groupby(
    [fe_bulk.index.get_level_values(0), fe_bulk.index.get_level_values(1)]
).food_available_for_consumption__kcal_per_day.nunique()
res[res != 0].value_counts(normalize=True).cumsum().head(10)

# %%
# Get valid (item,country)
threshold = 5
idx_keep = res[res < threshold].index
# Assign NaNs
index_2 = pd.Index([i[:2] for i in fe_bulk.index])
msk = index_2.isin(idx_keep)
fe_bulk.loc[msk, "food_available_for_consumption__kcal_per_day"] = pd.NA

# %% [markdown]
# #### Remove outliers
# Remove outliers (i.e. subsitute the values with `NaN`).

# %%
# Define for each column (metric) which indices should be 'removed'
with open(PATH_OUTLIERS, "r") as f:
    outliers = json.load(f)

# %%
for datapoints in outliers:
    fe_bulk.loc[datapoints["index"], datapoints["column"]] = pd.NA

# %% [markdown]
# ### 9.5 Correct region entities
# For some `product`, `metric` and `year` no value can be estimated for certain regions. This is because a big chunk of the region's population (i.e. countries) are missing. In this section we filter these entries out.

# %% [markdown]
# For this processing step, we melt the dataframe and divide it into two parts:
# - Country data
# - Region data (continents, income groups)

# %%
fe_bulk_orig = fe_bulk.copy()
fe_bulk_melted = fe_bulk.reset_index().melt(id_vars=["product", "country", "year", "population"], var_name="metric")

# %%
# Drop nan values
fe_bulk_melted = fe_bulk_melted.dropna(subset="value")
# Exclude regions
regions_ = continents + income + ["World"]
msk = fe_bulk_melted.country.isin(regions_)
fe_bulk_melted_countries = fe_bulk_melted[~msk]
fe_bulk_melted_regions = fe_bulk_melted[msk]

# %% [markdown]
# Next, we build a dataframe `x` which contains the _population difference_ for each region given a product, metric and year.


# %%
def build_df(x: pd.DataFrame, ncountries: bool = True) -> pd.DataFrame:
    # add number of countries and population in present countries
    population_ = x.groupby(["product", "metric", "region", "year"]).population.sum().tolist()
    x = x.groupby(["product", "metric", "region", "year"], as_index=False).country.nunique()
    x = x.assign(
        population=population_,
    )

    # add real population
    population_ = population.reset_index().astype({"year": float})
    x = x.merge(population_, left_on=["region", "year"], right_on=["country", "year"]).rename(
        columns={"population_y": "population_gt", "population_x": "population"}
    )
    if ncountries:
        # add real number of countries
        region_size = []
        for r, members in regions.items():
            region_size.append({"region": r, "ncountries_gt": len(members)})
        r = pd.DataFrame(region_size)
        x = x.merge(r, left_on="region", right_on="region")
    # build df
    x = pd.DataFrame(x)
    # diff population
    x = x.assign(
        population_diff=x.population_gt - x.population,
        population_diff_perc=(x.population_gt - x.population) / x.population_gt,
    )
    return x


# %%
# continents
x_cont = build_df(fe_bulk_melted_countries.assign(region=fe_bulk_melted_countries.country.map(country2continent)))
# income groups
x_inco = build_df(fe_bulk_melted_countries.assign(region=fe_bulk_melted_countries.country.map(country2income)))
# world
x_world = build_df(fe_bulk_melted_countries.assign(region="World"), ncountries=False)
# merge
x = pd.concat([x_cont, x_inco, x_world], ignore_index=True)

# %% [markdown]
# We now merge `x` with `fe_bulk_melted_regions` and filter out all entries that have a `population difference` greater than `t1`.

# %%
# Merge
cols_merge = ["product", "region", "year", "metric"]
fe_bulk_melted_regions = fe_bulk_melted_regions.merge(
    x[cols_merge + ["population", "population_diff_perc"]],
    left_on=["product", "country", "year", "metric"],
    right_on=["product", "region", "year", "metric"],
    how="left",
)
fe_bulk_melted_regions = fe_bulk_melted_regions.rename(columns={"population_x": "population"})

# %%
# Checks after merge
msk = fe_bulk_melted_regions.isna().any(axis=1)
values_to_remove = fe_bulk_melted_regions.loc[msk, "value"].unique()
if not all(values_to_remove == [0.011428571428571429, 0.0]) or msk.sum() > 60:
    raise ValueError(f"Re-check merge: {msk.sum()}, {values_to_remove}")
# Filter NaNs (controlled)
fe_bulk_melted_regions = fe_bulk_melted_regions[~msk]

# %%
# Filter all samples with > T1
## Threshold
t1 = 0.24  # Selected such that no datapoint for product='Total' is lost
t1_backup = fe_bulk_melted_regions[(fe_bulk_melted_regions["product"] == "Total")].population_diff_perc.max()
assert t1 > t1_backup
## Only apply to these metrics
metrics = [
    "food_available_for_consumption__fat_g_per_day",
    "food_available_for_consumption__kcal_per_day",
    "food_available_for_consumption__kg_per_year",
    "food_available_for_consumption__protein_g_per_day",
    "other_uses__tonnes",
    "waste_in_supply_chain__tonnes",
    "feed__tonnes",
]

fe_bulk_melted_regions = fe_bulk_melted_regions[
    ~((fe_bulk_melted_regions.population_diff_perc >= t1) & (fe_bulk_melted_regions.metric.isin(metrics)))
    | (fe_bulk_melted_regions["product"] == "Total")
]

# %%
# Fix population for > 0
fe_bulk_melted_regions = fe_bulk_melted_regions.assign(population_per_capita=fe_bulk_melted_regions.population)
msk = (fe_bulk_melted_regions.population_per_capita > 0) & (fe_bulk_melted_regions.metric.isin(metrics))
fe_bulk_melted_regions.loc[msk, "population_per_capita"] = fe_bulk_melted_regions.loc[msk, "population_y"]

# %% [markdown]
# Next, we estimate per capita values

# %%
# Estimate per_capita
fe_bulk_melted_regions = pd.DataFrame(fe_bulk_melted_regions)
fe_bulk_melted_regions = fe_bulk_melted_regions.assign(
    metric_capita=fe_bulk_melted_regions.metric + "__per_capita",
    value_capita=fe_bulk_melted_regions.value / fe_bulk_melted_regions.population_per_capita,
)
fe_bulk_melted_countries = pd.DataFrame(fe_bulk_melted_countries)
fe_bulk_melted_countries = fe_bulk_melted_countries.assign(
    metric_capita=fe_bulk_melted_countries.metric + "__per_capita",
    value_capita=fe_bulk_melted_countries.value / fe_bulk_melted_countries.population,
)

# %% [markdown]
# Time to pivot back

# %%
cols = [
    "product",
    "country",
    "year",
    "metric",
    "population",
    "value",
    "metric_capita",
    "value_capita",
]
r = pd.concat([fe_bulk_melted_countries[cols], fe_bulk_melted_regions[cols]], ignore_index=True)

# %%
# Pivot
fe_bulk_absolute = (
    r.pivot(
        index=["product", "country", "year", "population"],
        columns="metric",
        values="value",
    )
    .reset_index()
    .set_index(["product", "country", "year"])
)
fe_bulk_capita = (
    r.pivot(
        index=["product", "country", "year", "population"],
        columns="metric_capita",
        values="value_capita",
    )
    .reset_index()
    .set_index(["product", "country", "year"])
    .drop(columns=["population"])
)

# %% [markdown]
# Build `fe_bulk` back again.

# %%
fe_bulk = pd.merge(fe_bulk_absolute, fe_bulk_capita, left_index=True, right_index=True, how="outer")

# %%
# CHECK
# fe_bulk.loc["Maize", "Asia"]["food_available_for_consumption__kcal_per_day__per_capita"]

# %% [markdown]
# ### 9.6 Remove former countries
# We want the values reported for former states to account for regions (continents, income groups), but not that they appear on themselves on the explorer. Therefore, we eliminate these from the final dataset.

# %%
fe_bulk = fe_bulk.reset_index()
fe_bulk = fe_bulk.loc[~fe_bulk.country.isin(former_states)]

# %% [markdown]
# #### Set index

# %%
fe_bulk = fe_bulk.set_index(["product", "country", "year"], verify_integrity=True).sort_index()

# %% [markdown]
# ### 9.7 Remove unnused columns

# %%
# Remove unnused columns (https://github.com/owid/etl/pull/134#issuecomment-1076883200)
columns_remove = [
    "food_available_for_consumption__fat_g_per_day",
    "food_available_for_consumption__kcal_per_day",
    "food_available_for_consumption__kg_per_year",
    "food_available_for_consumption__protein_g_per_day",
    "yield__kg_per_animal__per_capita",
    "yield__tonnes_per_ha__per_capita",
]

fe_bulk = fe_bulk.drop(columns=columns_remove)

# %% [markdown]
# ### 9.8 Remove all zero series
# Here we detect all `(country, product, metric)` which timeseries is all zeroes and set it to `NaN`. This way, this metric will be ignored in Grapher for the given country and product.

# %%
# Unpivot
x = fe_bulk.melt(var_name="metric", ignore_index=False).reset_index()

# %%
# Find (product, country, metric) with all zeros (or NaNs)
res = x.groupby(["product", "country", "metric"]).agg(value_sum=("value", "sum"), value_nunique=("value", "nunique"))
msk = (res["value_nunique"] == 1) & (res["value_sum"] == 0) & (res.index.get_level_values(2) != "population")
idx = msk[msk].index

# %%
# Replace with NaNs
xx = x.set_index(["product", "country", "metric"])
xx.loc[idx, "value"] = np.nan
xx = xx.reset_index()

# %%
# Pivot back
fe_bulk = xx.pivot(index=["product", "country", "year"], columns="metric", values="value").astype(fe_bulk.dtypes)

# %% [markdown]
# ## 10. Export
# Time to export the shining brand new dataset!
#
# We export it in two flavours: bulk and file-per-product formats. The former is the standard format, while the later is intended to power OWID tools such as explorers.

# %% [markdown]
# ### Define metadata
# Prior to export, we need to create the metadata content for this dataset. It basically propagates the metadata from its building pieces (QCL so far).
#
# For this dataset, we use namespace `explorers`, which is intended for datasets aimed at powering explorers (this may change).


# %%
metadata = DatasetMeta(
    namespace="explorers",
    short_name="food_explorer",
    title="Food Explorer: Livestock & Crops, Food Balances - FAO (2017, 2021)",
    description=(
        "This dataset has been created by Our World in Data, merging existing FAOstat datsets. In particular, we have used 'Crops and livestock products' (QCL) and 'Food Balances' (FBSH and FBS) datasets. Each row contains all the "
        "metrics for a specific combination of (country, product, year). The metrics may come from different datasets."
    ),
    sources=qcl_garden.metadata.sources + fbsc_garden.metadata.sources,
    licenses=qcl_garden.metadata.licenses + fbsc_garden.metadata.licenses,
)

# %% [markdown]
# ### In bulk

# %% [markdown]
# Preserve the bulk file for QA or manual analysis.

# %% [markdown]
# #### Create metadata for fields
# Here we create the content for `field` metadata field, which contains metric-specific information.

# %%
# Load table from dataset containing Element information
qcl_elem = qcl_garden["meta_qcl_element"]
fbsc_elem = fbsc_garden["meta_fbs_element"]
qcl_elem["name_std"] = qcl_elem.index.map(map_elem)
fbsc_elem["name_std"] = fbsc_elem.index.map(map_elem)
element_metadata = pd.concat([qcl_elem.dropna().assign(dataset="QCL"), fbsc_elem.dropna().assign(dataset="FBS")])
# Final patch
patch = {
    "food_available_for_consumption__fat_g_per_day_per_capita": "food_available_for_consumption__fat_g_per_day",
    "food_available_for_consumption__protein_g_per_day_per_capita": "food_available_for_consumption__protein_g_per_day",
    "food_available_for_consumption__kcal_per_day_per_capita": "food_available_for_consumption__kcal_per_day",
    "food_available_for_consumption__kg_per_capita_per_year": "food_available_for_consumption__kg_per_year",
}
element_metadata["name_std"] = element_metadata["name_std"].replace(patch)


# %%
# Fill 'easy' fields
def _get_source_ids(dataset_code: str) -> List[int]:
    res = [i for i, source in enumerate(metadata.sources) if f"{dataset_code}" in source.owid_data_url]
    return res


def _build_description_extra(fe_bulk: pd.DataFrame, col: str) -> str:
    num_products = len(set(fe_bulk[col].dropna().index.get_level_values(0)))
    num_countries = len(set(fe_bulk[col].dropna().index.get_level_values(1)))
    description = f"This metric is present in {num_products} products and {num_countries} countries."
    return description


def _get_sources_and_licenses(dataset_code: str) -> Dict[str, Any]:
    source_ids = _get_source_ids(dataset_code)
    sources = [metadata.sources[i] for i in source_ids]
    licenses = [metadata.licenses[i] for i in source_ids]
    return {"sources": sources, "licenses": licenses}


fields = {}
columns = list(fe_bulk.columns) + fe_bulk.index.names
for col in columns:
    msk = element_metadata.name_std == col
    if msk.sum() == 0:
        if "__per_capita" in col:
            msk = element_metadata.name_std == col.replace("__per_capita", "")
        if msk.sum() == 0:
            msk = element_metadata.name_std == f"{col}_per_capita"

    if msk.sum() == 1:
        dataset_code = element_metadata.loc[msk, "dataset"].item()
        description = element_metadata.loc[msk, "description"].item()
        fields[col] = catalog.VariableMeta(
            title="",
            description=description,
            **_get_sources_and_licenses(dataset_code),
            display={"description_extra": _build_description_extra(fe_bulk, col)},
        )
    elif msk.sum() > 1:
        dataset_codes = element_metadata.loc[msk, "dataset"]
        if dataset_codes.nunique() != 1:
            raise ValueError(f"Merged metrics should all be from the same dataset! Check {col}")
        dataset_code = dataset_codes.unique()[0]
        fields[col] = catalog.VariableMeta(
            title="",
            description="",
            **_get_sources_and_licenses(dataset_code),
            display={"description_extra": _build_description_extra(fe_bulk, col)},
        )
    else:
        fields[col] = catalog.VariableMeta()

# %%
# Check missing fields
cols_missing = [f for f, v in fields.items() if v.description == ""]
cols_missing_check = {
    "exports__tonnes",
    "imports__tonnes",
    "producing_or_slaughtered_animals__animals",
    "yield__kg_per_animal",
    "exports__tonnes__per_capita",
    "food_available_for_consumption__fat_g_per_day__per_capita",
    "food_available_for_consumption__kcal_per_day__per_capita",
    "food_available_for_consumption__kg_per_year__per_capita",
    "food_available_for_consumption__protein_g_per_day__per_capita",
    "imports__tonnes__per_capita",
    "producing_or_slaughtered_animals__animals__per_capita",
}
assert set(cols_missing) == cols_missing_check

# %%
# fields['exports__tonnes']['description'] =
# fields['imports__tonnes']['description'] =
# fields['producing_or_slaughtered_animals__animals']['description'] =
# fields['yield__kg_per_animal']['description'] = "Yield is measured as the quantity produced per unit area of land used to grow it."
# fields['food_available_for_consumption__fat_g_per_day']['description'] =
# fields['food_available_for_consumption__kcal_per_day']['description'] =
# fields['food_available_for_consumption__kg_per_year']['description'] =
# fields['food_available_for_consumption__protein_g_per_day']['description'] =

# %% [markdown]
# #### Create table

# %%
table_bulk = catalog.Table(fe_bulk).copy()
table_bulk.metadata.short_name = "bulk"
table_bulk._fields = fields

# %% [markdown]
# ### One file per product

# %% [markdown]
# To work in an explorer, we need to add the table in CSV format. To make it more scalable for use, we want
# to split that dataset into many small files, one per product.


# %%
def to_short_name(raw: str) -> str:
    return raw.lower().replace(" ", "_").replace(",", "").replace("(", "").replace(")", "").replace(".", "")


# the index contains values like "Asses" which have already been filtered out from the data,
# let's remove them
fe_bulk.index = fe_bulk.index.remove_unused_levels()  # type: ignore

tables_products = {}

for product in sorted(fe_bulk.index.levels[0]):  # type: ignore
    short_name = to_short_name(product)
    print(f"{product} --> {short_name}.csv")

    t = catalog.Table(fe_bulk.loc[[product]])
    t.metadata.short_name = short_name

    tables_products[product] = t


# %% [markdown]
# ### Create dataset and fill it with tables and metadata

# %%
### One file per product


def run(dest_dir: str) -> None:
    # Initialize dataset
    fe_garden = catalog.Dataset.create_empty(dest_dir)
    fe_garden.metadata = metadata
    fe_garden.save()

    # Add bulk table
    fe_garden.add(table_bulk)

    # Add products
    for _, t in tables_products.items():
        fe_garden.add(t, formats=["csv"])  # <-- note we include CSV format here


# %% [markdown]
# Let's check that the biggest files are still an ok size for an explorer.

# %%
# !du -hs {dest_dir}/*.csv | sort -hr | head -n 10

# %% [markdown]
# The biggest is 3.1MB (csv), we should be ok ✓

# %%
# # Comparison with previous (live) export
# product = 'vegetables'
# df_new = pd.read_csv(f'/tmp/food_explorer/{product}.csv')
# df_old = pd.read_csv(f'https://owid-catalog.nyc3.digitaloceanspaces.com/garden/explorers/2021/food_explorer/{product}.csv')

# %%
# # Plot metric
# import matplotlib.pyplot as plt
# plt.rcParams['figure.figsize'] = [10, 7]
# plt.rcParams['figure.dpi'] = 100 # 200 e.g. is really fine, but slower
# metric = "food_available_for_consumption__kcal_per_day"
# # country = "Europe"
# country = "High-income countries"
# product = "Total"
# (
#     fe_bulk.loc[(product, country), metric]
#     / fe_bulk.loc[(product, country), "population"]
# ).plot(x="year", title=f"Food Supply in {country} ({product})", ylim=[0,3500])

# %%
# for former, current in former_to_current.items():
#     print(former)
#     for c in current:
#         print(c, country2income[c])
#     print('---')
