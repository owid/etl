"""FAOstat: Crops and livestock products.

"""

import json
import pandas as pd

from owid import catalog

from etl.paths import BASE_DIR, DATA_DIR
from .shared import run


########################################################################################################################
# TODO: Import any necessary things from here, which is the old processing code (ignored for the moment).
COUNTRY_MAPPING = (
    BASE_DIR / "etl/steps/data/garden/faostat/2022-04-26/faostat.countries.json"
)


def old_run(dest_dir: str) -> None:
    # Load meadow dataset
    qcl_meadow = catalog.Dataset(DATA_DIR / "meadow/faostat/2021-03-18/faostat_qcl")
    metadata = catalog.Dataset(DATA_DIR / "meadow/faostat/2022-02-10/faostat_metadata")

    # Bulk data and items metadata
    qcl_bulk = qcl_meadow["bulk"]

    ####################################################################################################################
    # Area
    # Filtering and mapping
    # Prepare for Country Tool
    # ds = qcl_area.Country.drop_duplicates()
    # ds.to_csv("ign.countries.csv", index=False)
    qcl_area = metadata["meta_qcl_area"]
    with open(COUNTRY_MAPPING) as f:
        country_mapping = json.load(f)
    # Check which countries will be discarded based on our country standardisation file (those without a mapped
    # standardised name)
    msk = qcl_area.country.isin(country_mapping)
    print(qcl_area.loc[-msk, "country"].tolist())
    # Finally, we build the `Area Code ---> Country` mapping dictionary.
    area_codes_discard = [140, 259, 260]
    if set(qcl_bulk.index.levels[0]).intersection(area_codes_discard):
        raise ValueError(
            "There are some changes in the bulk data! Codes that are being discarded might probably be needed"
        )
    # Discard
    qcl_area = qcl_area.loc[~qcl_area.index.isin(area_codes_discard)]
    map_area = (
        qcl_area.loc[msk, "country"].replace(country_mapping).sort_index().to_dict()
    )
    ####################################################################################################################
    # Item
    qcl_item = metadata["meta_qcl_item"]
    # Find Item Groups with more than one Code (legacy?)
    x = qcl_item.reset_index()
    _ = x.groupby(["item_group"]).agg(
        {"item_group_code": [lambda x: x.nunique(), lambda x: x.unique().tolist()]}
    )
    # __ = _["item_group_code"]["<lambda_0>"]
    # _[__ > 1]
    # Check if there are codes in bulk that *only* have a group code associated that is to be delete (sanity check
    # before deletion)
    codes_present = (
        qcl_bulk.index.get_level_values("item_code").unique().astype(str).tolist()
    )
    msk = x["item_code"].astype(str).isin(codes_present)
    y = x[msk]
    yy = y.groupby("item_code")["item_group_code"].agg(set)
    ll = yy[yy == {"QC"}].index.tolist()  # Change to see other groups with unique childs
    x[x["item_code"].isin(ll)].head()
    qcl_item = qcl_item[["item_group", "item"]]
    ####################################################################################################################
    # Element
    qcl_element = metadata["meta_qcl_element"]
    qcl_unit = metadata["meta_qcl_unit"]
    qcl_element_unit = qcl_element.merge(
        qcl_unit.rename(columns={"description": "unit_description"}),
        left_on="unit",
        right_index=True,
    )
    assert qcl_element_unit.shape[0] == qcl_element.shape[0]
    ####################################################################################################################
    # Bulk
    # Filter countries + Area Code -> Country
    qcl_bulk = qcl_bulk.loc[map_area].rename(index=map_area, level=0)
    name_map = {"area_code": "country"}
    qcl_bulk.index.names = [name_map.get(n, n) for n in qcl_bulk.index.names]
    # Drop Unit
    qcl_bulk = qcl_bulk.drop(columns=["unit"])
    # Variable name
    # qcl_bulk.head()
    # qcl_item.head()
    # Get Item names
    x = qcl_item.reset_index()
    a = (
        x[["item_group_code", "item_group"]]
        .drop_duplicates()
        .rename(columns={"item_group_code": "code", "item_group": "name"})
    )
    b = (
        x[["item_code", "item"]]
        .drop_duplicates()
        .rename(columns={"item_code": "code", "item": "name"})
    )
    c = pd.concat([a, b])
    map_items = dict(zip(c.code, c.name))
    # manually add some missing names to the map that were removed from the API
    missing = {
        1067: "Eggs, hen, in shell (number)",
        1092: "Eggs, other bird, in shell (number)",
        1731: "Oilcrops",
    }
    for k in missing:
        assert k not in map_items
        map_items[k] = missing[k]
    item_names = [map_items[it] for it in qcl_bulk.index.get_level_values(1)]
    # Get Element + Unit names
    x = qcl_element_unit.reset_index()
    y = list(x["element"].astype(str) + " (" + x["unit"].astype(str) + ")")
    map_elems = dict(zip(x["element_code"], y))
    elem_names = [map_elems[el] for el in qcl_bulk.index.get_level_values(2)]
    # Construct variable name
    variable_names = [f"{i} - {e}" for i, e in zip(item_names, elem_names)]
    # Add variable name to index
    qcl_bulk["variable_name"] = variable_names
    qcl_bulk = qcl_bulk.reset_index()
    qcl_bulk = qcl_bulk.set_index(
        ["country", "item_code", "element_code", "variable_name", "year", "flag"]
    )
    ####################################################################################################################
    # Create Garden dataset
    qcl_garden = catalog.Dataset.create_empty(dest_dir)
    # Propagate metadata
    qcl_garden.metadata = qcl_meadow.metadata
    qcl_garden.save()
    # Add bulk table
    qcl_garden.add(qcl_bulk)
    # Add table items
    qcl_garden.add(qcl_item)
    # Add table elements
    qcl_element_unit.metadata = qcl_element.metadata
    qcl_garden.add(qcl_element_unit)
    qcl_garden.save()


########################################################################################################################
