"""FAOstat: Food Balances Combined.

This step integrates two FAOstat datasets (previously imported to _meadow_) into a single _garden_ dataset.
This is because a new version of the _Food Balances_ dataset was launched in 2014 with a slightly new methodology
([more info](https://fenixservices.fao.org/faostat/static/documents/FBS/New%20FBS%20methodology.pdf)).
The new dataset is named FBSC (Food Balances Combined).

"""

import json
from typing import List

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta

from etl.paths import DATA_DIR, BASE_DIR

# TODO: Clean up code, which has been imported from the latest notebook.

# side-car file containing manual country mapping
COUNTRY_MAPPING = (
    BASE_DIR / "etl/steps/data/garden/faostat/2021-04-09/faostat_fbsc.country_std.json"
)


def run(dest_dir: str) -> None:
    # Load meadow datasets
    # In this step we load the required datasets from Garden: FBS and FBSH

    # Read datasets
    fbs_meadow = catalog.Dataset(DATA_DIR / "meadow/faostat/2021-04-09/faostat_fbs")
    fbsh_meadow = catalog.Dataset(DATA_DIR / "meadow/faostat/2017-12-11/faostat_fbsh")
    metadata = catalog.Dataset(DATA_DIR / "meadow/faostat/2022-02-10/faostat_metadata")

    # Bulk data and items metadata
    fbs_bulk = fbs_meadow["bulk"]
    fbsh_bulk = fbsh_meadow["bulk"]

    # Sanity checks.
    # As we are fusing two different datasets, we will be doing some checks to ensure the consistency of the dataset.
    # Specially in the identifying fields (i.e. `Year`, `Area Code`, `Item Code`, `Element Code`, `Flag`)
    # Check data files

    ####################################################################################################################
    # Year
    # Check if the time window of both datasets is disjoint, otherwise we could end up with duplicates.
    fbs_year_min, fbs_year_max = (
        fbs_bulk.index.get_level_values("year").min(),
        fbs_bulk.index.get_level_values("year").max(),
    )
    fbsh_year_min, fbsh_year_max = (
        fbsh_bulk.index.get_level_values("year").min(),
        fbsh_bulk.index.get_level_values("year").max(),
    )
    # Year disjoints
    assert (fbsh_year_min < fbsh_year_max) & (
        fbsh_year_max + 1 == fbs_year_min < fbs_year_max
    )

    ####################################################################################################################
    # Area
    # Here we check which Areas (i.e. countries/regions) appear in one dataset but not in the other.
    # We observe that former countries only appear in FBSH (USSR, Serbia and Montenegro, Sudan (fromer),
    # Belgium-Luxembourg, Checkoslovakia, Netherland Antilles, Yugoslavia, Ethiopia PDR), which makes sense.
    # There are some special cases where countries stopped or started appearing (Bermuda, Brunei and Papua New Guinea,
    # Seychelles and Comoros).
    fbsh_area = metadata["meta_fbsh_area"]
    fbs_area = metadata["meta_fbs_area"]
    # Get unique codes
    codes_fbs = set(fbs_bulk.index.get_level_values("area_code"))
    codes_fbsh = set(fbsh_bulk.index.get_level_values("area_code"))
    # Find missing codes
    miss_in_fbs = codes_fbsh.difference(codes_fbs)
    miss_in_fbsh = codes_fbs.difference(codes_fbsh)
    if len(miss_in_fbs) > 0:
        print(
            "- FBSH but not FBS:",
            fbsh_area.loc[sorted(miss_in_fbs), "country"].to_dict(),
        )
    if len(miss_in_fbsh) > 0:
        print(
            "- FBS but not FBSH:",
            fbs_area.loc[sorted(miss_in_fbsh), "country"].to_dict(),
        )
    # Next, we check that all codes correspond to the same country name in both datasets.
    x = fbs_area.merge(fbsh_area, left_index=True, right_index=True)
    assert (x.country_x.astype(str) == x.country_y.astype(str)).all()

    ####################################################################################################################
    # Item
    # Here we check which items appear and disappear from dataset to dataset.
    # It seems that some elements were deprecated in favour of others:  `Groundnuts (Shelled Eq) --> Groundnuts` and
    # `Rice (Milled Equivalent) --> Rice and products`
    fbsh_item = metadata["meta_fbsh_item"]
    fbs_item = metadata["meta_fbs_item"]

    def build_item_all_df(df: pd.DataFrame) -> pd.DataFrame:
        """Flatten item dataframe."""

        def _process_df(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
            return (
                df.drop_duplicates(cols)[cols]
                .rename(columns=dict(zip(cols, ["code", "name"])))
                .set_index("code")
            )

        df = df.reset_index()
        a = _process_df(df, ["item_group_code", "item_group"])
        b = _process_df(df, ["item_code", "item"])
        df = pd.concat([a, b])
        assert df.index.value_counts().max() == 1
        return df

    # Build flattened version (item group, item in same column)
    fbsh_item_ = build_item_all_df(fbsh_item)
    fbs_item_ = build_item_all_df(fbs_item)
    # Get unique codes
    codes_fbs = set(fbs_bulk.index.get_level_values("item_code"))
    codes_fbsh = set(fbsh_bulk.index.get_level_values("item_code"))
    # Find missing codes
    miss_in_fbs = codes_fbsh.difference(codes_fbs)
    miss_in_fbsh = codes_fbs.difference(codes_fbsh)
    # print("- FBSH but not FBS:", fbsh_item_.loc[sorted(miss_in_fbs), "name"].to_dict())
    # print("- FBS but not FBSH:", fbs_item_.loc[sorted(miss_in_fbsh), "name"].to_dict())
    # fbsh_item.reset_index().set_index(["item_code", "item_group_code"]).loc[2805]
    # fbs_item.reset_index().set_index(["item_code", "item_group_code"]).loc[2807]
    # We check that all codes are mapped to the same names.
    x = fbs_item_.merge(fbsh_item_, left_index=True, right_index=True)
    assert (x.name_x.astype(str) == x.name_y.astype(str)).all()
    # print(x[x.name_x != x.name_y])

    ####################################################################################################################
    # Element
    # We see that two items were introduced in FBS (not present in FBSH): `Residuals` and `Tourist consumption`.
    # Load element info
    fbsh_element = metadata["meta_fbsh_element"]
    fbs_element = metadata["meta_fbs_element"]
    # Get unique codes
    codes_fbs = set(fbs_bulk.index.get_level_values("element_code"))
    codes_fbsh = set(fbsh_bulk.index.get_level_values("element_code"))
    # Find missing codes
    miss_in_fbs = codes_fbsh.difference(codes_fbs)
    miss_in_fbsh = codes_fbs.difference(codes_fbsh)
    # print("- FBSH but not FBS:", fbsh_element.loc[miss_in_fbs, "element"].to_dict())
    # print("- FBS but not FBSH:", fbs_element.loc[miss_in_fbsh, "element"].to_dict())
    # First, we check if all element codes just have one unit associated. Next, we verify that in both datasets we have
    # the same mappings `code -> name`, `code -> unit` and `code -> description`.
    # Only one unit per element code
    assert fbs_bulk.reset_index().groupby("element_code").unit.nunique().max() == 1
    assert fbsh_bulk.reset_index().groupby("element_code").unit.nunique().max() == 1
    # Given an element code, we have the same element name, unit and description in fbs and fbsh
    x = fbs_element.merge(fbsh_element, left_index=True, right_index=True)
    assert (x.element_x.astype(str) == x.element_y.astype(str)).all()
    assert (x.unit_x.astype(str) == x.unit_y.astype(str)).all()
    assert (x.description_x.astype(str) == x.description_y.astype(str)).all()

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
    index_new = [
        (col_map := {"area_code": "country"}).get(x, x) for x in fbsc_bulk.index.names
    ]
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
