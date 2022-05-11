"""FAOstat: Food Balances Combined.

Combine the old and new food balances datasets:
* Old (historical) dataset: "faostat_fbsh".
* Current dataset: "faostat_fbs".
Into a new (combined) dataset: "fabosta_fbsc".

This is because a new version of the _Food Balances_ dataset was launched in 2014 with a slightly new methodology
([more info](https://fenixservices.fao.org/faostat/static/documents/FBS/New%20FBS%20methodology.pdf)).

"""

import json
from typing import List

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta
from owid.datautils import geo

from etl.paths import DATA_DIR, BASE_DIR, STEP_DIR
from etl.scripts.faostat.create_new_steps import find_latest_version_for_step
from shared import NAMESPACE, VERSION

# TODO: Clean up code, which has been imported from the latest notebook.

# Path to countries mapping file.
COUNTRIES_FILE = STEP_DIR / "data" / "garden" / NAMESPACE / VERSION / f"{NAMESPACE}.countries.json"

# Some items seem to have been renamed from fbsh to fbs. Ensure the old names are mapped to the new ones.
# TODO: Check that this mapping makes sense.
ITEMS_MAPPING = {
    "Groundnuts (Shelled Eq)": "Groundnuts",
    "Rice (Milled Equivalent)": "Rice and products",
}

assert COUNTRIES_FILE.is_file()


def run(dest_dir: str) -> None:

    # Load latest faostat_fbs dataset from meadow.
    fbs_version = find_latest_version_for_step(channel="meadow", step_name="faostat_fbs", namespace=NAMESPACE)
    fbs_file = DATA_DIR / "meadow" / NAMESPACE / fbs_version / "faostat_fbs"
    fbs_dataset = catalog.Dataset(fbs_file)

    # Load latest faostat_fbsh dataset from meadow.
    fbsh_version = find_latest_version_for_step(channel="meadow", step_name="faostat_fbsh", namespace=NAMESPACE)
    fbsh_file = DATA_DIR / "meadow" / NAMESPACE / fbsh_version / "faostat_fbsh"
    fbsh_dataset = catalog.Dataset(fbsh_file)

    # Load latest faostat_metadata from meadow.
    metadata_version = find_latest_version_for_step(channel="meadow", step_name="faostat_metadata", namespace=NAMESPACE)
    metadata_file = DATA_DIR / "meadow" / NAMESPACE / metadata_version / "faostat_metadata"
    metadata_dataset = catalog.Dataset(metadata_file)

    # Load dataframes for fbs and fbsh datasets.
    fbs = pd.DataFrame(fbs_dataset["faostat_fbs"]).reset_index()
    fbsh = pd.DataFrame(fbsh_dataset["faostat_fbsh"]).reset_index()

    # Sanity checks.
    assert fbsh["year"].min() < fbsh["year"].max()
    assert fbs["year"].min() < fbs["year"].max()

    if (fbsh["year"].max() >= fbs["year"].min()):
        print("WARNING: There is overlapping data between fbsh and fbs datasets. Prioritising fbs over fbsh.")
        fbsh = fbsh[fbsh["year"] < fbs["year"].min()].reset_index(drop=True)    

    if (fbsh["year"].max() + 1) < fbs["year"].min():
        print("WARNING: Data is missing for one or more years between fbsh and fbs datasets.")

    # Harmonize country names in both dataframes.
    fbs = geo.harmonize_countries(df=fbs, countries_file=COUNTRIES_FILE, country_col="area",
                                  warn_on_unused_countries=False).rename(columns={"area": "country"})
    fbsh = geo.harmonize_countries(df=fbsh, countries_file=COUNTRIES_FILE, country_col="area",
                                   warn_on_unused_countries=False).rename(columns={"area": "country"})

    # Remove unused columns.
    unused_columns = ["area_code", "item_code", "element_code"]
    fbs = fbs.drop(columns=unused_columns)
    fbsh = fbsh.drop(columns=unused_columns)

    # There are items in fbsh that are not in fbs and vice versa.
    # We manually created a mapping from old to new names (define above).

    # Ensure the elements that are in fbsh but not in fbs are covered in the mapping.
    error = "Mismatch between items in fbsh and fbs. Redefine items mapping."
    assert set(fbsh["item"]) - set(fbs["item"]) == set(ITEMS_MAPPING), error
    assert set(fbs["item"]) - set(fbsh["item"]) == set(ITEMS_MAPPING.values()), error

    # Some elements are found in fbs but not in fbsh. This is understandable, since fbs is more recent and may have
    # additional elements. However, ensure that there are no elements in fbsh that are not in fbs.
    error = "There are elements in fbsh that are not in fbs."
    assert set(fbsh["element"]) < set(fbs["element"]), error

    # Add description of each element (from metadata) to fbs and to fbsh.
    # Add also "unit", just to check that data in the original dataset and in metadata coincide.
    fbsh = pd.merge(fbsh, metadata_dataset["meta_fbsh_element"].rename(columns={'unit': 'unit_check'}),
                    on="element", how="left")
    fbs = pd.merge(fbs, metadata_dataset["meta_fbs_element"].rename(columns={'unit': 'unit_check'}),
                    on="element", how="left")

    # Check that units of elements in fbsh and in the corresponding metadata coincide.
    error = "Elements in fbsh have different units in dataset and in its corresponding metadata."
    assert (fbsh["unit"] == fbsh["unit_check"]).all(), error
    fbsh = fbsh.drop(columns="unit_check")

    # Check that units of elements in fbs and in the corresponding metadata coincide.
    error = "Elements in fbs have different units in dataset and in its corresponding metadata."
    assert (fbs["unit"] == fbs["unit_check"]).all(), error
    fbs = fbs.drop(columns="unit_check")

    # Concatenate old and new dataframes.
    fbsc = pd.concat([fbsh, fbs]).sort_values(["country", "year"]).reset_index(drop=True)

    # Map old item names to new item names.
    fbsc["item"] = fbsc["item"].replace(ITEMS_MAPPING)

    # Ensure that each element has only one unit.
    error = "Some elements in the combined dataset have more than one unit."
    assert fbsc.groupby("element")["unit"].nunique().max() == 1, error

    # Ensure that each element has only one unit.
    error = "Some elements in the combined dataset have more than one unit."
    assert fbsc.groupby("element")["description"].nunique().max() == 1, error

    # TODO: Continue here. Deal with duplicated data.

    warning = "WARNING: Some elements in the combined dataset have more than one description."
    assert fbsc.groupby("element")["description"].nunique().max() == 1, warning

    ####################################################################################################################
    # Flag
    # Next, we compare which flags appear in each dataset. We observe that some flags only appear in one of the
    # datasets. This is fine.
    # In particular:
    # - `Im` (Imputed) ist most common in new dataset, whereas `S` (Standardized data) was in the old one.
    # - `Im` (Imputed) and `*` (Unofficial) appear first in new FBS.
    # - `nan` (Official data), `SD` (Statistical Discrepancy) and `F` (FAO estimate) appear only in old FBSH.
    # Get unique codes
    codes_fbs = set(fbs_bulk.index.get_level_values("flag"))
    codes_fbsh = set(fbsh_bulk.index.get_level_values("flag"))
    # Find missing codes
    miss_in_fbs = codes_fbsh.difference(codes_fbs)
    miss_in_fbsh = codes_fbs.difference(codes_fbsh)
    # print("- FBSH but not FBS:", miss_in_fbs)
    # print("- FBS but not FBSH:", miss_in_fbsh)
    # pd.value_counts(fbsh_bulk.index.get_level_values("flag").fillna("nan"))
    # pd.value_counts(fbs_bulk.index.get_level_values("flag").fillna("nan"))

    ####################################################################################################################
    # Merge dataset
    # The moment has arrived. Now we attempt to merge both FBS and FBSH datasets into one: FBSC dataset. For this, we
    # will be merging several files:
    # - **bulk file**: The data itself.
    # - **item file**: The file containing the mapping from item code to item name.
    # - **element file**: The file containing the mapping from element to element name and unit.
    #
    # In addition, we will transition from `Area Code ---> Country`.

    # Area
    # In this step, we standardise the country names. We first go from `Area Code` to `Area` (country name as per the
    # FAO), and then `Area` to `Country`, using our country standardisation file.
    # Load our country standardisation file
    with open(COUNTRY_MAPPING) as f:
        country_mapping = json.load(f)
    # Merge both datasets Area Code -> Area mapping dataframe
    fbsc_area = pd.concat([fbs_area, fbsh_area]).drop_duplicates(subset=["country"])
    # fbsc_area[fbsc_area.country.apply(lambda x: "sudan" in x.lower())]
    # Check which countries will be discarded based on our country standardisation file (those without a mapped
    # standardised name)
    msk = fbsc_area.country.isin(country_mapping)
    # print(fbsc_area.loc[-msk, "country"].tolist())
    # Finally, we build the `Area Code ---> Country` mapping dictionary.
    map_area = (
        fbsc_area.loc[msk, "country"].replace(country_mapping).sort_index().to_dict()
    )

    # Item
    # Merging the item dataframe is straight forward. There are some exceptions, which we accept, due to the renaming
    # of items such as Groundnuts and Rice.
    fbsc_item = pd.concat([fbs_item, fbsh_item]).drop_duplicates(
        subset=["item_group", "item"]
    )
    # Check differences are as expected
    a = fbs_item.index
    b = fbsh_item.index
    c = fbsc_item.index
    assert not {cc for cc in c if cc not in a}.difference(
        {
            (2905, 2805),
            (2901, 2805),
            (2903, 2805),
            (2901, 2556),
            (2913, 2556),
            (2903, 2556),
            (2960, 2769),
        }
    )
    assert not {cc for cc in c if cc not in b}.difference(
        {
            (2905, 2807),
            (2901, 2807),
            (2903, 2807),
            (2901, 2552),
            (2913, 2552),
            (2903, 2552),
            (2961, 2769),
        }
    )
    # fbsh_item.loc[2960, 2769]
    # fbs_item.loc[2961, 2769]
    fbsc_item = fbsc_item[["item_group", "item"]]

    # Element
    # We merge element and unit dataframes, in order to obtain all the info in one. Next, we combine both FBS and FBSH
    # datasets.
    # Load unit table
    fbs_unit = metadata["meta_fbs_unit"]
    fbsh_unit = metadata["meta_fbsh_unit"]
    # Merge element and unit
    fbs_element_unit = fbs_element.merge(
        fbs_unit.rename(columns={"description": "unit_description"}),
        left_on="unit",
        right_index=True,
    )
    assert fbs_element_unit.shape[0] == fbs_element.shape[0]
    fbsh_element_unit = fbsh_element.merge(
        fbsh_unit.rename(columns={"description": "unit_description"}),
        left_on="unit",
        right_index=True,
    )
    assert fbsh_element_unit.shape[0] == fbsh_element.shape[0]

    # Merge
    fbsc_element_unit = pd.concat(
        [fbs_element_unit, fbsh_element_unit]
    ).drop_duplicates(subset=["element", "unit", "unit_description"])
    assert fbsc_element_unit.shape == fbsh_element_unit.shape == fbs_element_unit.shape

    # Bulk
    # Time to merge the core of the dataset, the bulk file! We do this by:
    # - Concatenating both datasets
    # - Renaming `Area Code --> Country`
    # - Drop unused columns (`Unit`, `Area Code`)
    # - Drop data related to population (`2501`) item.
    # - Add `variable_name` column, with some more verbosity about each row info.
    fbsc_bulk = pd.concat([fbs_bulk, fbsh_bulk])
    # Filter countries + Area Code -> Country
    col_map = {"area_code": "country"}
    index_new = [col_map.get(x, x) for x in fbsc_bulk.index.names]
    fbsc_bulk = fbsc_bulk.loc[map_area].reset_index()
    fbsc_bulk[col_map["area_code"]] = fbsc_bulk["area_code"].replace(map_area).tolist()
    fbsc_bulk = fbsc_bulk.set_index(index_new)
    # Drop Unit, Area Code
    fbsc_bulk = fbsc_bulk.drop(columns=["unit", "area_code"])
    # Drop population (2501) item
    msk = fbsc_bulk.index.get_level_values("item_code").isin([2501])
    fbsc_bulk = fbsc_bulk[~msk]

    # Variable name
    # Variable name is built using the name of the item, element and unit: `item - element - [unit]`
    # Get item names
    fbsc_item_ = build_item_all_df(fbsc_item)
    map_items = fbsc_item_.astype(str)["name"].to_dict()  # type: ignore
    item_names = [map_items[i] for i in fbsc_bulk.index.get_level_values("item_code")]  # type: ignore
    # Get Element + Unit names
    x = fbsc_element_unit.reset_index()
    y = list(x["element"].astype(str) + " [" + x["unit"].astype(str) + "]")
    map_elems = dict(zip(x["element_code"], y))
    elem_names = [map_elems[el] for el in fbsc_bulk.index.get_level_values(2)]
    # Construct variable name
    variable_names = [f"{i} - {e}" for i, e in zip(item_names, elem_names)]
    # Add variable name to index
    fbsc_bulk["variable_name"] = variable_names
    fbsc_bulk = fbsc_bulk.reset_index()
    fbsc_bulk = fbsc_bulk.set_index(
        ["country", "item_code", "element_code", "variable_name", "year", "flag"]
    )
    # fbsc_bulk.head()

    ####################################################################################################################
    # Create Garden dataset

    # Metadata
    # First, we create the metadata for this new dataset FBSC. Most of its content comes from concatenating FBS and
    # FBSH fields. Checksum field is left to `None`, as it is unclear what we should use here (TODO).
    # Check description field in FBS and FBSH
    assert fbsh_meadow.metadata.description == fbs_meadow.metadata.description
    # Define metadata
    metadata = DatasetMeta(
        namespace="faostat",
        short_name="faostat_fbsc",
        title="Food Balance: Food Balances (-2013 old methodology and 2014-) - FAO (2017, 2021)",
        description=fbsh_meadow.metadata.description,
        sources=fbsh_meadow.metadata.sources + fbs_meadow.metadata.sources,
        licenses=fbsh_meadow.metadata.licenses + fbs_meadow.metadata.licenses,
    )
    # Create dataset and add tables
    # Finally, we add the tables to the dataset.
    fbsc_garden = catalog.Dataset.create_empty(dest_dir)
    # Propagate metadata
    fbsc_garden.metadata = metadata
    fbsc_garden.save()
    # Add bulk table
    fbsc_bulk.metadata.short_name = "bulk"
    fbsc_garden.add(fbsc_bulk)
    # Add table items
    fbsc_item.metadata.short_name = "meta_item"
    fbsc_garden.add(fbsc_item)
    # Add table elements
    fbsc_element_unit.metadata = fbs_element.metadata
    fbsc_element_unit.metadata.description = (
        "List of elements, with their units and the respective descriptions of "
        "both. It also includes the element codes."
    )
    fbsc_garden.add(fbsc_element_unit)
    fbsc_garden.save()
