"""FAOSTAT garden step for faostat_metadata dataset.

This step reads from:
* The (additional) metadata dataset. The only crucial ingredients from here (that will be used later on in other garden
  steps are element, item and units descriptions, and country groups (used to check that we do not double count
  countries when aggregating data for regions).
* Custom datasets file ('./custom_datasets.csv').
* Custom elements and units file ('./custom_elements_and_units.csv').
* Custom items file ('./custom_items.csv').
* Value amendments file ('./value_amendments.csv').
* Each of the individual meadow datasets. They are loaded to extract their countries, items, elements and units, and
  some sanity checks are performed.

This step will:
* Output a dataset (to be loaded by all garden datasets) with tables 'countries, 'datasets', 'elements', 'items'
  and 'amendments'.
* Apply sanity checks to countries, elements, items, and units.
* Apply custom names and descriptions to datasets, elements, items and units.
* Check that spurious values in value_amendments.csv are in the data, and whether there are new spurious values.
* Harmonize country names.
* Find countries that correspond to aggregates of other countries (e.g. 'Melanesia').
* Ensure there are no degeneracies within a dataset (i.e. ensure each index is unique).
* Ensure there are no degeneracies between datasets (using dataset, item_code, element_code as keys).

There are some non-trivial issues with the definitions of items at FAOSTAT:
* Some item codes in the data are missing in the metadata, and vice versa.
* The mapping item_code -> item in the data files is sometimes different from the mapping item_code -> item
  in the (additional) metadata dataset. Some examples:
  * In dataset qv, item code 221 in the data corresponds to item "Almonds, in shell", whereas in the metadata,
    item code 221 corresponds to item "Almonds, with shell", which is the same item, but with a slightly different
    name. This happens with many items. On the website (https://www.fao.org/faostat/en/?#data/QV) they seem to be
    using the naming from the metadata. We can safely ignore this issue, and stick to the names in the data.

There are several cases in which one or a few item codes in the data are missing in the metadata. Also, there are
several cases in which an item code in the data has an item name slightly different in the metadata. But these are not
important issues (since we use item_code to merge different datasets, and we use metadata only to fetch descriptions).

"""

import json
import sys
from copy import deepcopy
from typing import Dict, List, Tuple

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.datautils import dataframes, io
from shared import (
    CURRENT_DIR,
    ELEMENTS_IN_FBSH_MISSING_IN_FBS,
    FAOSTAT_METADATA_SHORT_NAME,
    FLAGS_RANKING,
    N_CHARACTERS_ELEMENT_CODE,
    N_CHARACTERS_ITEM_CODE,
    N_CHARACTERS_ITEM_CODE_EXTENDED,
    NAMESPACE,
    harmonize_elements,
    harmonize_items,
    log,
    optimize_table_dtypes,
    prepare_dataset_description,
)
from tqdm.auto import tqdm

from etl.helpers import PathFinder

# Minimum number of issues in the comparison of items and item codes from data and metadata to raise a warning.
N_ISSUES_ON_ITEMS_FOR_WARNING = 1


def create_dataset_descriptions_table_for_domain(table: Table, dataset_short_name: str) -> Table:
    """Create a single row table with the dataset name, title and description, for a given domain.

    Parameters
    ----------
    table : Table
        Latest table for considered domain.
    dataset_short_name : str
        Dataset short name (e.g. 'faostat_qcl').

    Returns
    -------
    tb_dataset_descriptions : Table
        Table of name, title and description of a domain.

    """
    tb_dataset_descriptions = Table(
        {
            "dataset": [dataset_short_name],
            "fao_dataset_title": [table.metadata.dataset.title],
            "fao_dataset_description": [table.metadata.dataset.description],
        }
    )

    return tb_dataset_descriptions


def clean_global_dataset_descriptions_table(tb_datasets: Table, tb_custom_datasets: Table) -> Table:
    """Apply sanity checks to the table gathered from the data of each individual datasets, and add custom dataset
    titles and descriptions.

    Parameters
    ----------
    tb_datasets : Table
        Table of descriptions gathered from the data of each individual dataset.
    tb_custom_datasets : Table
        Data from the custom_datasets.csv file.

    Returns
    -------
    tb_datasets : Table
        Clean table of dataset titles and descriptions (customized and original FAO ones).

    """
    tb_datasets = tb_datasets.copy()

    # Check that the dataset descriptions of fbsh and fbs are identical.
    error = (
        "Datasets fbsh and fbs have different descriptions. "
        "This may happen in the future: Simply check that nothing significant has changed and remove this assertion."
    )
    assert (
        tb_datasets[tb_datasets["dataset"] == "faostat_fbsh"]["fao_dataset_description"].item()
        == tb_datasets[tb_datasets["dataset"] == "faostat_fbs"]["fao_dataset_description"].item()
    ), error
    # Drop row for fbsh, and rename "fbs" to "fbsc" (since this will be the name for the combined dataset).
    tb_datasets = tb_datasets[tb_datasets["dataset"] != "faostat_fbsh"].reset_index(drop=True)
    tb_datasets.loc[tb_datasets["dataset"] == "faostat_fbs", "dataset"] = "faostat_fbsc"

    # Add custom dataset titles.
    tb_datasets = tb_datasets.merge(
        tb_custom_datasets,
        on="dataset",
        how="left",
        suffixes=("_new", "_old"),
    )

    changed_titles = tb_datasets[
        tb_datasets["fao_dataset_title_old"].fillna("") != tb_datasets["fao_dataset_title_new"].fillna("")
    ]
    changed_descriptions = tb_datasets[
        tb_datasets["fao_dataset_description_old"].fillna("") != tb_datasets["fao_dataset_description_new"].fillna("")
    ]

    if len(changed_titles) > 0:
        log.warning(f"{len(changed_titles)} domains have changed titles, consider updating custom_datasets.csv.")
    if len(changed_descriptions) > 0:
        log.warning(
            f"{len(changed_descriptions)} domains have changed descriptions. " f"Consider updating custom_datasets.csv."
        )
    tb_datasets = tb_datasets.drop(columns=["fao_dataset_title_old", "fao_dataset_description_old"]).rename(
        columns={
            "fao_dataset_title_new": "fao_dataset_title",
            "fao_dataset_description_new": "fao_dataset_description",
        }
    )

    tb_datasets["owid_dataset_title"] = tb_datasets["owid_dataset_title"].fillna(tb_datasets["fao_dataset_title"])
    error = "Custom titles for different datasets are equal. Edit custom_datasets.csv file."
    assert len(set(tb_datasets["dataset"])) == len(set(tb_datasets["owid_dataset_title"])), error

    # The final description will be the owid description (if there is any) followed by the original FAO description
    # (if there is any).
    tb_datasets["owid_dataset_description"] = [
        prepare_dataset_description(
            fao_description=dataset["fao_dataset_description"],
            owid_description=dataset["owid_dataset_description"],
        )
        for _, dataset in tb_datasets.fillna("").iterrows()
    ]

    # Reorder columns.
    tb_datasets = tb_datasets[
        [
            "dataset",
            "fao_dataset_title",
            "owid_dataset_title",
            "fao_dataset_description",
            "owid_dataset_description",
        ]
    ]

    return tb_datasets


def check_that_item_and_element_harmonization_does_not_trim_codes(
    data: Table, dataset_short_name: str, category: str
) -> None:
    # Ensure that the number of digits of all item and element codes is smaller than the limits defined
    # at the beginning of the garden shared module, by N_CHARACTERS_ITEM_CODE and N_CHARACTERS_ELEMENT_CODE,
    # respectively.

    # Set the maximum number of characters for item_code.
    if dataset_short_name in [f"{NAMESPACE}_sdgb", f"{NAMESPACE}_fs"]:
        n_characters_item_code = N_CHARACTERS_ITEM_CODE_EXTENDED
    else:
        n_characters_item_code = N_CHARACTERS_ITEM_CODE

    n_characters = {"element": N_CHARACTERS_ELEMENT_CODE, "item": n_characters_item_code}
    error = (
        f"{category.capitalize()} codes found with more characters than expected for {dataset_short_name}. "
        f"This parameter (N_CHARACTERS_*_CODE*) is defined in garden shared module and may need to be increased. "
        f"This would change how {category} codes are harmonized, increasing the length of variable names. "
        f"It may have further unwanted consequences, so do it with caution."
    )
    assert all([len(str(code)) <= n_characters[category] for code in data[f"{category}_code"].unique()]), error


def create_items_table_for_domain(table: Table, metadata: Dataset, dataset_short_name: str) -> Table:
    """Apply sanity checks to the items of a table in a dataset, and to the items from the metadata, harmonize all item
    codes and items, and add item descriptions.

    Parameters
    ----------
    table : Table
        Data for a given domain.
    metadata: Dataset
         Metadata dataset from meadow.
    dataset_short_name : str
        Dataset short name (e.g. 'faostat_qcl').

    Returns
    -------
    items_from_data : Table
        Item names and descriptions (customized ones and FAO original ones) for a particular domain.

    """
    tb = table.reset_index()

    # Load items from data.
    items_from_data = (
        tb.rename(columns={"item": "fao_item"})[["item_code", "fao_item"]].drop_duplicates().reset_index(drop=True)
    )
    # Sanity check.
    check_that_item_and_element_harmonization_does_not_trim_codes(
        data=tb, dataset_short_name=dataset_short_name, category="item"
    )
    # Ensure items are well constructed and amend already known issues (defined in shared.ITEM_AMENDMENTS).
    items_from_data = harmonize_items(tb=items_from_data, dataset_short_name=dataset_short_name, item_col="fao_item")

    # Load items from metadata.
    items_columns = {
        "item_code": "item_code",
        "item": "fao_item",
        "description": "fao_item_description",
    }
    _metadata = metadata[f"{dataset_short_name}_item"].reset_index()
    if not (set(items_columns) <= set(_metadata.columns)):
        # This is the case for faostat_qv since last version.
        return items_from_data

    _tb_items = (
        _metadata[list(items_columns)]
        .rename(columns=items_columns)
        .drop_duplicates()
        .sort_values(list(items_columns.values()))
        .reset_index(drop=True)
    )
    _tb_items = harmonize_items(tb=_tb_items, dataset_short_name=dataset_short_name, item_col="fao_item")
    _tb_items["fao_item_description"] = _tb_items["fao_item_description"].astype("string")

    # Add descriptions (from metadata) to items (from data).
    items_from_data = (
        items_from_data.merge(_tb_items, on=["item_code", "fao_item"], how="left")
        .sort_values(["item_code", "fao_item"])
        .reset_index(drop=True)
    )
    items_from_data["dataset"] = dataset_short_name
    items_from_data["fao_item_description"] = items_from_data["fao_item_description"].fillna("")

    # Sanity checks for items in current dataset:

    # Check that in data, there is only one item per item code.
    n_items_per_item_code = items_from_data.groupby("item_code")["fao_item"].transform("nunique")
    error = f"Multiple items for a given item code in dataset {dataset_short_name}."
    assert items_from_data[n_items_per_item_code > 1].empty, error

    # Check that all item codes in data are defined in metadata, and check that the mapping item code -> item in
    # the data is the same as in the metadata (which often is not the case).
    compared = items_from_data[["item_code", "fao_item"]].merge(
        _tb_items[["item_code", "fao_item"]],
        on="item_code",
        how="left",
        suffixes=("_in_data", "_in_metadata"),
    )
    different_items = compared[compared["fao_item_in_data"] != compared["fao_item_in_metadata"]]
    missing_item_codes = set(items_from_data["item_code"]) - set(_tb_items["item_code"])
    if len(missing_item_codes) > 0:
        log.warning(f"{len(missing_item_codes)} item codes in {dataset_short_name} missing in metadata. ")
    if len(different_items) > 0:
        _frac_different = len(different_items) / len(set(compared["fao_item_in_data"]))
        log.warning(
            f"{len(different_items)} item codes in data ({_frac_different:.2%}) mapping to different items in metadata."
        )

    return items_from_data


def clean_global_items_table(tb_items: Table, custom_items: Table) -> Table:
    """Apply global sanity checks to items gathered from all datasets, and create a clean global items table.

    Parameters
    ----------
    tb_items : Table
        Items table gathered from all domains.
    custom_items : Table
        Data from custom_items.csv file.

    Returns
    -------
    tb_items : Table
        Clean global items table.

    """
    tb_items = tb_items.copy()

    # Check that fbs and fbsh have the same contributions, remove one of them, and rename the other to fbsc.
    check = (
        tb_items[tb_items["dataset"] == "faostat_fbsh"]
        .reset_index(drop=True)[["item_code", "fao_item"]]
        .merge(
            tb_items[tb_items["dataset"] == "faostat_fbs"].reset_index(drop=True)[["item_code", "fao_item"]],
            how="outer",
            on=["item_code"],
            suffixes=("_fbsh", "_fbs"),
        )
    )
    assert (check["fao_item_fbsh"] == check["fao_item_fbs"]).all()
    # Drop all rows for fbsh, and rename "fbs" to "fbsc" (since this will be the name for the combined dataset).
    tb_items = tb_items[tb_items["dataset"] != "faostat_fbsh"].reset_index(drop=True)
    tb_items.loc[tb_items["dataset"] == "faostat_fbs", "dataset"] = "faostat_fbsc"

    # Add custom item names.
    tb_items = tb_items.merge(
        custom_items.rename(columns={"fao_item": "fao_item_check"}),
        on=["dataset", "item_code"],
        how="left",
        suffixes=("_new", "_old"),
    )

    changed_descriptions = tb_items[
        (tb_items["fao_item_description_old"] != tb_items["fao_item_description_new"])
        & (tb_items["fao_item_description_old"].notnull())
    ]
    if len(changed_descriptions) > 0:
        log.warning(
            f"{len(changed_descriptions)} domains have changed item descriptions. "
            f"Consider updating custom_items.csv."
        )

    tb_items = tb_items.drop(columns="fao_item_description_old").rename(
        columns={"fao_item_description_new": "fao_item_description"}
    )

    # Check that item names have not changed.
    # NOTE: This condition used to raise an error if not fulfilled. Consider making it an assertion.
    if not (
        tb_items[tb_items["fao_item_check"].notnull()]["fao_item_check"]
        == tb_items[tb_items["fao_item_check"].notnull()]["fao_item"]
    ).all():
        log.warning("Item names may have changed with respect to custom items file. Update custom items file.")
    tb_items = tb_items.drop(columns=["fao_item_check"])

    # Assign original FAO name to all owid items that do not have a custom name.
    tb_items["owid_item"] = tb_items["owid_item"].fillna(tb_items["fao_item"])

    # Add custom item descriptions, and assign original FAO descriptions to items that do not have a custom description.
    tb_items["owid_item_description"] = tb_items["owid_item_description"].fillna(tb_items["fao_item_description"])

    # Check that we have not introduced ambiguities when assigning custom item names.
    n_owid_items_per_item_code = tb_items.groupby(["dataset", "item_code"])["owid_item"].transform("nunique")
    error = "Multiple owid items for a given item code in a dataset."
    assert tb_items[n_owid_items_per_item_code > 1].empty, error

    tb_items = (
        tb_items[
            [
                "dataset",
                "item_code",
                "fao_item",
                "owid_item",
                "fao_item_description",
                "owid_item_description",
            ]
        ]
        .sort_values(["dataset", "item_code"])
        .reset_index(drop=True)
    )

    return tb_items


def create_elements_table_for_domain(table: Table, metadata: Dataset, dataset_short_name: str) -> Table:
    """Apply sanity checks to the elements and units of a table in a dataset, and to the elements and units from the
    metadata, harmonize all element code, and add descriptions.

    Parameters
    ----------
    table : Table
        Data for a given domain.
    metadata: Dataset
         Additional metadata dataset from meadow.
    dataset_short_name : str
        Dataset short name (e.g. 'faostat_qcl').

    Returns
    -------
    elements_from_data : Table
        Element names and descriptions and unit names and descriptions (customized ones and FAO original ones) for a
        particular domain.

    """

    tb = table.reset_index()
    # Load elements from data.
    elements_from_data = (
        tb.rename(columns={"element": "fao_element", "unit": "fao_unit_short_name"})[
            ["element_code", "fao_element", "fao_unit_short_name"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    # Sanity check.
    check_that_item_and_element_harmonization_does_not_trim_codes(
        data=tb, dataset_short_name=dataset_short_name, category="element"
    )
    # Ensure element_code is always a string of a fix number of characters.
    elements_from_data = harmonize_elements(
        tb=elements_from_data,
        dataset_short_name=dataset_short_name,
        element_col="fao_element",
        unit_col="fao_unit_short_name",
    )

    # Load elements from metadata.
    elements_columns = {
        "element_code": "element_code",
        "element": "fao_element",
        "description": "fao_element_description",
    }
    _metadata = metadata[f"{dataset_short_name}_element"].reset_index()
    if not (set(elements_columns) <= set(_metadata.columns)):
        # This is the case for faostat_qv since last version.
        return elements_from_data

    _tb_elements = (
        _metadata[list(elements_columns)]
        .rename(columns=elements_columns)
        .drop_duplicates()
        .sort_values(list(elements_columns.values()))
        .reset_index(drop=True)
    )
    _tb_elements = harmonize_elements(
        tb=_tb_elements, dataset_short_name=dataset_short_name, element_col="fao_element", unit_col=None
    )
    _tb_elements["fao_element_description"] = _tb_elements["fao_element_description"].astype("string")

    # Load units metadata.
    units_columns = {
        "unit_name": "fao_unit_short_name",
        "description": "fao_unit",
    }
    _tb_units = (
        metadata[f"{dataset_short_name}_unit"]
        .reset_index()[list(units_columns)]
        .rename(columns=units_columns)
        .drop_duplicates()
        .sort_values(list(units_columns.values()))
        .reset_index(drop=True)
    )
    _tb_units["fao_unit"] = _tb_units["fao_unit"].astype("string")

    # Add element descriptions (from metadata).
    elements_from_data = (
        elements_from_data.merge(
            _tb_elements,
            on=["element_code", "fao_element"],
            how="left",
        )
        .sort_values(["element_code", "fao_element"])
        .reset_index(drop=True)
    )
    elements_from_data["dataset"] = dataset_short_name
    elements_from_data["fao_element_description"] = elements_from_data["fao_element_description"].fillna("")

    # Add unit descriptions (from metadata).
    elements_from_data = (
        elements_from_data.merge(_tb_units, on=["fao_unit_short_name"], how="left")
        .sort_values(["fao_unit_short_name"])
        .reset_index(drop=True)
    )
    elements_from_data["fao_unit"] = elements_from_data["fao_unit"].fillna(elements_from_data["fao_unit_short_name"])

    # Sanity checks:

    # Check that in data, there is only one unit per element code.
    n_units_per_element_code = tb.groupby("element_code")["unit"].transform("nunique")
    error = f"Multiple units for a given element code in dataset {dataset_short_name}."
    assert tb[n_units_per_element_code > 1].empty, error

    # Check that in data, there is only one element per element code.
    n_elements_per_element_code = elements_from_data.groupby("element_code")["fao_element"].transform("nunique")
    error = f"Multiple elements for a given element code in dataset {dataset_short_name}."
    assert elements_from_data[n_elements_per_element_code > 1].empty, error

    return elements_from_data


def clean_global_elements_table(tb_elements: Table, custom_elements: Table) -> Table:
    """Apply global sanity checks to elements and units gathered from all datasets, and create a clean global elements
    and units table.

    Parameters
    ----------
    tb_elements : Table
        Elements and units table gathered from all domains.
    custom_elements : Table
        Data from custom_element_and_units.csv file.

    Returns
    -------
    tb_elements : Table
        Clean global elements and units table.

    """
    tb_elements = tb_elements.copy()

    # Check that all elements of fbsh are in fbs (although fbs may contain additional elements).
    assert (
        set(tb_elements[tb_elements["dataset"] == "faostat_fbsh"]["element_code"])
        - set(tb_elements[tb_elements["dataset"] == "faostat_fbs"]["element_code"])
        == ELEMENTS_IN_FBSH_MISSING_IN_FBS
    ), "There are new elements in fbsh that are not in fbs. Add them to ELEMENTS_IN_FBSH_MISSING_IN_FBS."
    # Drop all rows for fbsh, and rename "fbs" to "fbsc" (since this will be the name for the combined dataset).
    tb_elements = tb_elements[tb_elements["dataset"] != "faostat_fbsh"].reset_index(drop=True)
    tb_elements.loc[tb_elements["dataset"] == "faostat_fbs", "dataset"] = "faostat_fbsc"

    tb_elements = tb_elements.merge(
        custom_elements.rename(
            columns={
                "fao_element": "fao_element_check",
                "fao_unit_short_name": "fao_unit_short_name_check",
            }
        ),
        on=["dataset", "element_code"],
        how="left",
        suffixes=("_new", "_old"),
    )

    changed_units = tb_elements[
        (tb_elements["fao_unit_new"] != tb_elements["fao_unit_old"]) & (tb_elements["fao_unit_old"].notnull())
    ]
    if len(changed_units) > 0:
        log.warning(f"{len(changed_units)} domains have changed units, consider updating custom_elements.csv.")

    changed_descriptions = tb_elements[
        (tb_elements["fao_element_description_new"] != tb_elements["fao_element_description_old"])
        & (tb_elements["fao_element_description_old"].notnull())
    ]
    if len(changed_descriptions) > 0:
        log.warning(
            f"{len(changed_descriptions)} domains have changed element descriptions. "
            f"Consider updating custom_elements.csv."
        )

    tb_elements = tb_elements.drop(columns=["fao_unit_old", "fao_element_description_old"]).rename(
        columns={
            "fao_element_description_new": "fao_element_description",
            "fao_unit_new": "fao_unit",
        }
    )

    # Check if element or unit names have changed with respect to the custom elements and units file.
    # NOTE: This raises an error instead of a warning because further steps will (certainly?) fail.
    changed_elements = tb_elements[
        tb_elements["fao_element_check"].notnull() & (tb_elements["fao_element_check"] != tb_elements["fao_element"])
    ][["fao_element_check", "fao_element"]]
    if len(changed_elements) > 0:
        log.error(
            f"{len(changed_elements)} element names have changed with respect to custom elements file. Use `update_custom_metadata.py` to update custom elements file."
        )
    tb_elements = tb_elements.drop(columns=["fao_element_check"])

    changed_units = tb_elements[
        tb_elements["fao_unit_short_name_check"].notnull()
        & (tb_elements["fao_unit_short_name_check"] != tb_elements["fao_unit_short_name"])
    ][["fao_unit_short_name_check", "fao_unit_short_name"]]
    if len(changed_units) > 0:
        log.error(
            f"{len(changed_units)} unit names have changed with respect to custom elements file. Use `update_custom_metadata.py` to update custom elements file."
        )
    tb_elements = tb_elements.drop(columns=["fao_unit_short_name_check"])

    # Assign original FAO names where there is no custom one.
    tb_elements["owid_element"] = tb_elements["owid_element"].fillna(tb_elements["fao_element"])
    tb_elements["owid_unit"] = tb_elements["owid_unit"].fillna(tb_elements["fao_unit"])
    tb_elements["owid_element_description"] = tb_elements["owid_element_description"].fillna(
        tb_elements["fao_element_description"]
    )
    tb_elements["owid_unit_short_name"] = tb_elements["owid_unit_short_name"].fillna(tb_elements["fao_unit_short_name"])

    # Assume variables were not per capita, if was_per_capita is not informed, and make boolean.
    tb_elements["was_per_capita"] = tb_elements["was_per_capita"].fillna("0").replace({"0": False, "1": True})

    # Idem for variables to make per capita.
    tb_elements["make_per_capita"] = tb_elements["make_per_capita"].fillna("0").replace({"0": False, "1": True})

    # Check that we have not introduced ambiguities when assigning custom element or unit names.
    n_owid_elements_per_element_code = tb_elements.groupby(["dataset", "element_code"])["owid_element"].transform(
        "nunique"
    )
    error = "Multiple owid elements for a given element code in a dataset."
    assert tb_elements[n_owid_elements_per_element_code > 1].empty, error

    # Check that we have not introduced ambiguities when assigning custom element or unit names.
    n_owid_units_per_element_code = tb_elements.groupby(["dataset", "element_code"])["owid_unit"].transform("nunique")
    error = "Multiple owid elements for a given element code in a dataset."
    assert tb_elements[n_owid_units_per_element_code > 1].empty, error

    # NOTE: We assert that there is one element for each element code. But the opposite may not be true: there can be
    # multiple element codes with the same element. And idem for items.

    return tb_elements


def check_countries_to_exclude_or_harmonize(
    countries_in_data: Table, excluded_countries: List[str], countries_harmonization: Dict[str, str]
) -> None:
    # Check that all excluded countries are in the data.
    unknown_excluded_countries = set(excluded_countries) - set(countries_in_data["fao_country"])
    error = (
        f"Uknown excluded countries (to be removed from faostat.excluded_countries.json): {unknown_excluded_countries}"
    )
    assert len(unknown_excluded_countries) == 0, error

    # Check that all countries to be harmonized are in the data.
    unknown_countries_to_harmonize = set(countries_harmonization) - set(countries_in_data["fao_country"])
    error = f"Unknown countries to be harmonized (to be removed or edited in faostat.countries.json): {unknown_countries_to_harmonize}"
    assert len(unknown_countries_to_harmonize) == 0, error

    # Check that all countries in the data are either to be excluded or to be harmonized.
    unknown_countries = set(countries_in_data["fao_country"]) - set(excluded_countries) - set(countries_harmonization)
    error = f"Unknown countries in the data (to be added either to faostat.excluded_countries.json or to faostat.countries.json): {unknown_countries}"
    assert len(unknown_countries) == 0, error


def clean_global_countries_table(
    countries_in_data: Table,
    country_groups: Dict[str, List[str]],
    countries_harmonization: Dict[str, str],
    excluded_countries: List[str],
) -> Table:
    """Clean table of countries gathered from the data of the individual domains, harmonize country names (and
    country names of members of regions), and create a clean global countries table.

    Parameters
    ----------
    countries_in_data : Table
        Countries gathered from the data of all domains.
    country_groups : dict
        Countries and their members, gathered from the data.
    countries_harmonization : dict
        Mapping of country names (from FAO names to OWID names).
    excluded_countries : list
        Country names to be ignored.

    Returns
    -------
    tb_countries : Table
        Clean global countries table.

    """
    tb_countries = countries_in_data.copy()

    # Sanity checks.
    check_countries_to_exclude_or_harmonize(
        countries_in_data=countries_in_data,
        excluded_countries=excluded_countries,
        countries_harmonization=countries_harmonization,
    )

    # Harmonize country groups and members.
    country_groups_harmonized = {
        countries_harmonization[group]: sorted([countries_harmonization[member] for member in country_groups[group]])
        for group in country_groups
        if group in countries_harmonization
    }

    # Harmonize country names.
    tb_countries["country"] = dataframes.map_series(
        series=tb_countries["fao_country"],
        mapping=countries_harmonization,
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=False,
        make_unmapped_values_nan=True,
        show_full_warning=True,
    )

    # Add country members to countries table.
    tb_countries["members"] = dataframes.map_series(
        series=tb_countries["country"],
        mapping=country_groups_harmonized,
        make_unmapped_values_nan=True,
    )

    # Feather does not support object types, so convert column of lists to column of strings.
    tb_countries["members"] = [
        json.dumps(members) if isinstance(members, list) else members for members in tb_countries["members"]
    ]

    return tb_countries


def create_table(tb: Table, short_name: str, index_cols: List[str]) -> Table:
    """Create a table with optimal format and basic metadata, out of a table.

    Parameters
    ----------
    tb : Table
        Input table.
    short_name : str
        Short name to add in the metadata of the new table.
    index_cols : list
        Columns to use as indexes of the new table.

    Returns
    -------
    table : Table
        New table.

    """
    table = Table(tb).copy()

    # Optimize column dtypes before storing feather file, and ensure codes are categories (instead of ints).
    table = optimize_table_dtypes(table)

    # Set indexes and other necessary metadata.
    table = table.set_index(index_cols, verify_integrity=True)

    table.metadata.short_name = short_name
    table.metadata.primary_key = index_cols

    return table


def check_that_flag_definitions_in_dataset_agree_with_those_in_flags_ranking(
    metadata: Dataset,
) -> None:
    """Check that the definition of flags in the additional metadata for current dataset agree with the ones we have
    manually written down in our flags ranking (raise error otherwise).

    Parameters
    ----------
    metadata : Dataset
        Additional metadata dataset (that must contain one table for current dataset).

    """
    for table_name in metadata.table_names:
        if ("flag" in table_name) and ("flags" in metadata[table_name].columns):
            tb_flag = metadata[table_name].reset_index()
            comparison = FLAGS_RANKING.merge(tb_flag, on="flag", how="inner")
            error_message = (
                f"Flag definitions in file {table_name} are different to those in our flags ranking. "
                f"Redefine shared.FLAGS_RANKING."
            )
            assert (comparison["description"] == comparison["flags"]).all(), error_message


def check_that_all_flags_in_dataset_are_in_ranking(table: Table, metadata_for_flags: Table) -> None:
    """Check that all flags found in current dataset are defined in our flags ranking (raise error otherwise).

    Parameters
    ----------
    table : Table
        Data table for current dataset.
    metadata_for_flags : Table
        Flags for current dataset, as defined in dataset of additional metadata.

    """
    if not set(table["flag"]) < set(FLAGS_RANKING["flag"]):
        missing_flags = set(table["flag"]) - set(FLAGS_RANKING["flag"])
        flags_data = metadata_for_flags.reset_index()
        if set(missing_flags) < set(flags_data["flag"]):
            message = "Missing flags. Copy the following lines to FLAGS_RANKING (and put them in the right order):"
            for i, j in metadata_for_flags.loc[list(missing_flags)].iterrows():
                message += f"\n{(i, j['flags'])},"
            log.warning(message)
        else:
            log.warning(
                f"Missing flags. {missing_flags} are not defined in additional metadata. Get definition from "
                f"https://www.fao.org/faostat/en/#definitions"
            )
        raise AssertionError("Flags in dataset not found in FLAGS_RANKING. Manually add those flags.")


def check_definitions_in_value_amendments(table: Table, dataset_short_name: str, value_amendments: Table) -> None:
    """Check definitions in the value_amendments.csv file.

    This function will assert that:
    * All spurious values defined in the file are still found in the data.
    * There are no unexpected spurious values in the data.

    Spurious values are only searched for in "value" column if it has "category" dtype.
    See regular expression below used to search for spurious values.

    Parameters
    ----------
    table : Table
        _description_
    dataset_short_name : str
        _description_
    value_amendments : Table
        _description_
    """
    # Regular expression used to search for spurious values in the "value" column.
    regex_spurious_values = "<|,|N"

    # Select value amendments for the specified dataset.
    _value_amendments = value_amendments[value_amendments["dataset"] == dataset_short_name]
    if not _value_amendments.empty:
        # Check that spurious values defined in value_amendments.csv are indeed found in the data.
        expected_spurious_values_not_found = set(_value_amendments["spurious_value"]) - set(table["value"])
        error = (
            f"Expected spurious values {expected_spurious_values_not_found} not found in {dataset_short_name}. "
            f"Remove them from value_amendments.csv."
        )
        assert len(expected_spurious_values_not_found) == 0, error

    # Search for additional spurious values (only if data values are of "category" type).
    if table["value"].dtype == "category":
        # Find any possible spurious values in the data.
        spurious_values = (
            table[table["value"].astype(str).str.contains(regex_spurious_values, regex=True)]["value"].unique().tolist()
        )
        # Find if any of those were not accounted for already in value_amendments.
        new_spurious_values = set(spurious_values) - set(_value_amendments["spurious_value"])
        error = f"Unexpected spurious values found in {dataset_short_name}. Add the following values to value_amendments.csv: {new_spurious_values}"
        assert len(new_spurious_values) == 0, error


def process_metadata(
    paths: PathFinder,
    metadata: Dataset,
    custom_datasets: Table,
    custom_elements: Table,
    custom_items: Table,
    countries_harmonization: Dict[str, str],
    excluded_countries: List[str],
    value_amendments: Table,
) -> Tuple[Table, Table, Table, Table]:
    """Apply various sanity checks, gather data (about dataset, item, element and unit names and descriptions) from all
    domains, compare with data from its corresponding metadata file, and create clean tables of metadata about
    dataset, elements, units, items, and countries.

    Parameters
    ----------
    metadata : Dataset
        Additional metadata dataset from meadow.
    custom_datasets : Table
        Data from custom_datasets.csv file.
    custom_elements : Table
        Data from custom_elements_and_units.csv file.
    custom_items : Table
        Data from custom_items.csv file.
    countries_harmonization : dict
        Data from faostat.countries.json file.
    excluded_countries : list
        Data from faostat.excluded_countries.json file.
    value_amendments : Table
        Data from value_amendments.csv file.

    Returns
    -------
    tb_countries : Table
        Clean table of global countries.
    tb_datasets : Table
        Clean table of global dataset names and descriptions.
    tb_elements : Table
        Clean table of global element and unit names and descriptions.
    tb_items : Table
        Clean table of global item names and descriptions.

    """
    # Check if flags definitions need to be updated.
    check_that_flag_definitions_in_dataset_agree_with_those_in_flags_ranking(metadata)

    # List all FAOSTAT dataset short names.
    dataset_short_names = sorted(
        set([NAMESPACE + "_" + table_name.split("_")[1] for table_name in metadata.table_names])
    )

    # Initialise table of dataset descriptions, items, and element-units.
    # We cannot remove "dataset" from the items and elements tables, because it can happen that, for a given
    # item code, the item name is slightly different in two different datasets.
    tb_datasets = Table({"dataset": [], "fao_dataset_title": [], "fao_dataset_description": []})
    tb_items = Table({"dataset": [], "item_code": [], "fao_item": [], "fao_item_description": []})
    tb_elements = Table(
        {
            "dataset": [],
            "element_code": [],
            "fao_element": [],
            "fao_element_description": [],
            "fao_unit": [],
            "fao_unit_short_name": [],
        }
    )

    # Initialise list of all countries in all datasets, and all country groups.
    countries_in_data = Table({"area_code": [], "fao_country": []}).astype({"area_code": "Int64"})
    country_groups_in_data: Dict[str, List[str]] = {}

    # Gather all variables from the latest version of each meadow dataset.
    for dataset_short_name in tqdm(dataset_short_names, file=sys.stdout):
        # Load latest meadow table for current dataset.
        ds_latest = paths.load_dataset(dataset_short_name)
        table = ds_latest[dataset_short_name]
        tb = table.reset_index().rename(
            columns={
                "area": "fao_country",
                "recipient_country": "fao_country",
                "recipient_country_code": "area_code",
            }
        )[["area_code", "fao_country"]]

        tb["area_code"] = tb["area_code"].astype("Int64")

        if f"{dataset_short_name}_flag" in metadata.table_names:
            check_that_all_flags_in_dataset_are_in_ranking(
                table=table, metadata_for_flags=metadata[f"{dataset_short_name}_flag"]
            )

        # Check if spurious values defined in value_amendments.csv are still in the data,
        # and whether there are new spurious values to be amended.
        check_definitions_in_value_amendments(
            table=table, dataset_short_name=dataset_short_name, value_amendments=value_amendments
        )

        # Gather dataset descriptions, items, and element-units for current domain.
        datasets_from_data = create_dataset_descriptions_table_for_domain(
            table=table, dataset_short_name=dataset_short_name
        )

        items_from_data = create_items_table_for_domain(
            table=table, metadata=metadata, dataset_short_name=dataset_short_name
        )

        elements_from_data = create_elements_table_for_domain(
            table=table, metadata=metadata, dataset_short_name=dataset_short_name
        )

        # Add countries in this dataset to the list of all countries.
        countries_in_data = pr.concat([countries_in_data, tb]).drop_duplicates()

        # Get country groups in this dataset.
        _metadata = metadata[f"{dataset_short_name}_area_group"].reset_index()
        if set(["country_group", "country"]) <= set(_metadata.columns):
            country_groups = (
                _metadata.drop_duplicates(subset=["country_group", "country"])
                .groupby("country_group")
                .agg({"country": list})
                .to_dict()["country"]
            )
            # Add new groups to country_groups_in_data; if they are already there, ensure they contain all members.
            for group in list(country_groups):
                if group not in countries_in_data["fao_country"]:
                    # This should not happen, but skip just in case.
                    continue
                if group in list(country_groups_in_data):
                    all_members = set(country_groups_in_data[group]) | set(country_groups[group])
                    country_groups_in_data[group] = list(all_members)
                else:
                    country_groups_in_data[group] = country_groups[group]

        # Add dataset descriptions, items, and element-units from current dataset to global tables.
        tb_datasets = dataframes.concatenate([tb_datasets, datasets_from_data], ignore_index=True)
        tb_items = dataframes.concatenate([tb_items, items_from_data], ignore_index=True)
        tb_elements = dataframes.concatenate([tb_elements, elements_from_data], ignore_index=True)

    tb_datasets = clean_global_dataset_descriptions_table(tb_datasets=tb_datasets, tb_custom_datasets=custom_datasets)
    tb_items = clean_global_items_table(tb_items=tb_items, custom_items=custom_items)

    tb_elements = clean_global_elements_table(tb_elements=tb_elements, custom_elements=custom_elements)
    tb_countries = clean_global_countries_table(
        countries_in_data=countries_in_data,
        country_groups=country_groups_in_data,
        countries_harmonization=countries_harmonization,
        excluded_countries=excluded_countries,
    )

    return tb_countries, tb_datasets, tb_elements, tb_items


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = f"{NAMESPACE}_metadata"

    # Define path to current step file.
    current_step_file = (CURRENT_DIR / dataset_short_name).with_suffix(".py")

    # Get paths and naming conventions for current data step.
    paths = PathFinder(current_step_file.as_posix())

    # Path to file with custom dataset titles and descriptions.
    custom_datasets_file = paths.directory / "custom_datasets.csv"
    # Path to file with custom item names and descriptions.
    custom_items_file = paths.directory / "custom_items.csv"
    # Path to file with custom element and unit names and descriptions.
    custom_elements_and_units_file = paths.directory / "custom_elements_and_units.csv"
    # Path to file with mapping from FAO names to OWID harmonized country names.
    countries_file = paths.directory / f"{NAMESPACE}.countries.json"
    # Path to file with list of excluded countries and regions.
    excluded_countries_file = paths.directory / f"{NAMESPACE}.excluded_countries.json"
    # Path to file with spurious values and amendments.
    value_amendments_file = paths.directory / "value_amendments.csv"

    # Load metadata from meadow.
    metadata = paths.load_dataset()

    # Load custom dataset names, items, element-unit names, and value amendments.
    custom_datasets = pr.read_csv(custom_datasets_file, dtype=str)
    custom_elements = pr.read_csv(custom_elements_and_units_file, dtype=str)
    custom_items = pr.read_csv(custom_items_file, dtype=str)
    value_amendments = pr.read_csv(value_amendments_file, dtype=str)

    # Load country mapping and excluded countries files.
    countries_harmonization = io.load_json(countries_file)
    excluded_countries = io.load_json(excluded_countries_file)

    #
    # Process data.
    #
    tb_countries, tb_datasets, tb_elements, tb_items = process_metadata(
        paths=paths,
        metadata=metadata,
        custom_datasets=custom_datasets,
        custom_elements=custom_elements,
        custom_items=custom_items,
        countries_harmonization=countries_harmonization,
        excluded_countries=excluded_countries,
        value_amendments=value_amendments,
    )

    #
    # Save outputs.
    #
    # Initialize new garden dataset.
    dataset_garden = Dataset.create_empty(dest_dir)
    dataset_garden.short_name = FAOSTAT_METADATA_SHORT_NAME
    # Keep original dataset's metadata from meadow.
    dataset_garden.metadata = deepcopy(metadata.metadata)
    # Create new dataset in garden.
    dataset_garden.save()

    # Create new garden dataset with all dataset descriptions, items, element-units, and countries.
    datasets_table = create_table(tb=tb_datasets, short_name="datasets", index_cols=["dataset"])
    items_table = create_table(tb=tb_items, short_name="items", index_cols=["dataset", "item_code"])
    elements_table = create_table(tb=tb_elements, short_name="elements", index_cols=["dataset", "element_code"])
    countries_table = create_table(tb=tb_countries, short_name="countries", index_cols=["area_code"])
    amendments_table = Table(value_amendments, short_name="amendments").set_index(
        ["dataset", "spurious_value"], verify_integrity=True
    )

    # Add tables to dataset.
    dataset_garden.add(datasets_table, repack=False)
    dataset_garden.add(items_table, repack=False)
    dataset_garden.add(elements_table, repack=False)
    dataset_garden.add(countries_table, repack=False)
    dataset_garden.add(amendments_table, repack=False)
