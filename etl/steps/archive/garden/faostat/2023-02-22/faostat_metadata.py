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
from pathlib import Path
from typing import Dict, List, Tuple, cast

import pandas as pd
from owid import catalog
from owid.datautils import dataframes, io
from shared import (
    CURRENT_DIR,
    FAOSTAT_METADATA_SHORT_NAME,
    FLAGS_RANKING,
    N_CHARACTERS_ELEMENT_CODE,
    N_CHARACTERS_ITEM_CODE,
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


def create_dataset_descriptions_dataframe_for_domain(table: catalog.Table, dataset_short_name: str) -> pd.DataFrame:
    """Create a single row dataframe with the dataset name, title and description, for a given domain.

    Parameters
    ----------
    table : catalog.Table
        Latest table for considered domain.
    dataset_short_name : str
        Dataset short name (e.g. 'faostat_qcl').

    Returns
    -------
    dataset_descriptions_df : pd.DataFrame
        Dataframe of name, title and description of a domain.

    """
    dataset_descriptions_df = pd.DataFrame(
        {
            "dataset": [dataset_short_name],
            "fao_dataset_title": [table.metadata.dataset.title],
            "fao_dataset_description": [table.metadata.dataset.description],
        }
    )

    return dataset_descriptions_df


def clean_global_dataset_descriptions_dataframe(
    datasets_df: pd.DataFrame, custom_datasets: pd.DataFrame
) -> pd.DataFrame:
    """Apply sanity checks to the dataframe gathered from the data of each individual datasets, and add custom dataset
    titles and descriptions.

    Parameters
    ----------
    datasets_df : pd.DataFrame
        Dataframe of descriptions gathered from the data of each individual dataset.
    custom_datasets : pd.DataFrame
        Data from the custom_datasets.csv file.

    Returns
    -------
    datasets_df : pd.Dataframe
        Clean dataframe of dataset titles and descriptions (customized and original FAO ones).

    """
    datasets_df = datasets_df.copy()

    # Check that the dataset descriptions of fbsh and fbs are identical.
    error = (
        "Datasets fbsh and fbs have different descriptions. "
        "This may happen in the future: Simply check that nothing significant has changed and remove this assertion."
    )
    assert (
        datasets_df[datasets_df["dataset"] == "faostat_fbsh"]["fao_dataset_description"].item()
        == datasets_df[datasets_df["dataset"] == "faostat_fbs"]["fao_dataset_description"].item()
    ), error
    # Drop row for fbsh, and rename "fbs" to "fbsc" (since this will be the name for the combined dataset).
    datasets_df = datasets_df[datasets_df["dataset"] != "faostat_fbsh"].reset_index(drop=True)
    datasets_df.loc[datasets_df["dataset"] == "faostat_fbs", "dataset"] = "faostat_fbsc"

    # Add custom dataset titles.
    datasets_df = pd.merge(
        datasets_df,
        custom_datasets,
        on="dataset",
        how="left",
        suffixes=("_new", "_old"),
    )

    changed_titles = datasets_df[
        datasets_df["fao_dataset_title_old"].fillna("") != datasets_df["fao_dataset_title_new"].fillna("")
    ]
    changed_descriptions = datasets_df[
        datasets_df["fao_dataset_description_old"].fillna("") != datasets_df["fao_dataset_description_new"].fillna("")
    ]

    if len(changed_titles) > 0:
        log.warning(f"{len(changed_titles)} domains have changed titles, consider updating custom_datasets.csv.")
    if len(changed_descriptions) > 0:
        log.warning(
            f"{len(changed_descriptions)} domains have changed descriptions. " f"Consider updating custom_datasets.csv."
        )
    datasets_df = datasets_df.drop(columns=["fao_dataset_title_old", "fao_dataset_description_old"]).rename(
        columns={
            "fao_dataset_title_new": "fao_dataset_title",
            "fao_dataset_description_new": "fao_dataset_description",
        }
    )

    datasets_df["owid_dataset_title"] = datasets_df["owid_dataset_title"].fillna(datasets_df["fao_dataset_title"])
    error = "Custom titles for different datasets are equal. Edit custom_datasets.csv file."
    assert len(set(datasets_df["dataset"])) == len(set(datasets_df["owid_dataset_title"])), error

    # The final description will be the owid description (if there is any) followed by the original FAO description
    # (if there is any).
    datasets_df["owid_dataset_description"] = [
        prepare_dataset_description(
            fao_description=dataset["fao_dataset_description"],
            owid_description=dataset["owid_dataset_description"],
        )
        for _, dataset in datasets_df.fillna("").iterrows()
    ]

    # Reorder columns.
    datasets_df = datasets_df[
        [
            "dataset",
            "fao_dataset_title",
            "owid_dataset_title",
            "fao_dataset_description",
            "owid_dataset_description",
        ]
    ]

    return datasets_df


def check_that_item_and_element_harmonization_does_not_trim_codes(data: pd.DataFrame, category: str) -> None:
    # Ensure that the number of digits of all item and element codes is smaller than the limits defined
    # at the beginning of the garden shared module, by N_CHARACTERS_ITEM_CODE and N_CHARACTERS_ELEMENT_CODE,
    # respectively.
    n_characters = {"element": N_CHARACTERS_ELEMENT_CODE, "item": N_CHARACTERS_ITEM_CODE}
    error = (
        f"{category.capitalize()} codes found with more than N_CHARACTERS_{category.upper()}_CODE digits. "
        f"This parameter is defined in garden shared module and may need to be increased. "
        f"This would change how {category} codes are harmonized, increasing the length of variable names. "
        f"It may have further unwanted consequences, so do it with caution."
    )
    assert all([len(str(code)) <= n_characters[category] for code in data[f"{category}_code"].unique()]), error


def create_items_dataframe_for_domain(
    table: catalog.Table, metadata: catalog.Dataset, dataset_short_name: str
) -> pd.DataFrame:
    """Apply sanity checks to the items of a table in a dataset, and to the items from the metadata, harmonize all item
    codes and items, and add item descriptions.

    Parameters
    ----------
    table : catalog.Table
        Data for a given domain.
    metadata: catalog.Dataset
         Metadata dataset from meadow.
    dataset_short_name : str
        Dataset short name (e.g. 'faostat_qcl').

    Returns
    -------
    items_from_data : pd.Dataframe
        Item names and descriptions (customized ones and FAO original ones) for a particular domain.

    """
    df = pd.DataFrame(table).reset_index()

    # Load items from data.
    items_from_data = (
        df.rename(columns={"item": "fao_item"})[["item_code", "fao_item"]].drop_duplicates().reset_index(drop=True)
    )
    # Sanity check.
    check_that_item_and_element_harmonization_does_not_trim_codes(data=df, category="item")
    # Ensure items are well constructed and amend already known issues (defined in shared.ITEM_AMENDMENTS).
    items_from_data = harmonize_items(df=items_from_data, dataset_short_name=dataset_short_name, item_col="fao_item")

    # Load items from metadata.
    items_columns = {
        "item_code": "item_code",
        "item": "fao_item",
        "description": "fao_item_description",
    }
    _items_df = (
        metadata[f"{dataset_short_name}_item"]
        .reset_index()[list(items_columns)]
        .rename(columns=items_columns)
        .drop_duplicates()
        .sort_values(list(items_columns.values()))
        .reset_index(drop=True)
    )
    _items_df = harmonize_items(df=_items_df, dataset_short_name=dataset_short_name, item_col="fao_item")
    _items_df["fao_item_description"] = _items_df["fao_item_description"].astype("string")

    # Add descriptions (from metadata) to items (from data).
    items_from_data = (
        pd.merge(items_from_data, _items_df, on=["item_code", "fao_item"], how="left")
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
    compared = pd.merge(
        items_from_data[["item_code", "fao_item"]],
        _items_df[["item_code", "fao_item"]],
        on="item_code",
        how="left",
        suffixes=("_in_data", "_in_metadata"),
    )
    different_items = compared[compared["fao_item_in_data"] != compared["fao_item_in_metadata"]]
    missing_item_codes = set(items_from_data["item_code"]) - set(_items_df["item_code"])
    if (len(different_items) + len(missing_item_codes)) > N_ISSUES_ON_ITEMS_FOR_WARNING:
        log.warning(
            f"{len(missing_item_codes)} item codes in {dataset_short_name} missing in metadata. "
            f"{len(different_items)} item codes in data mapping to different items in metadata."
        )

    return items_from_data


def clean_global_items_dataframe(items_df: pd.DataFrame, custom_items: pd.DataFrame) -> pd.DataFrame:
    """Apply global sanity checks to items gathered from all datasets, and create a clean global items dataframe.

    Parameters
    ----------
    items_df : pd.DataFrame
        Items dataframe gathered from all domains.
    custom_items : pd.DataFrame
        Data from custom_items.csv file.

    Returns
    -------
    items_df : pd.DataFrame
        Clean global items dataframe.

    """
    items_df = items_df.copy()

    # Check that fbs and fbsh have the same contributions, remove one of them, and rename the other to fbsc.
    check = pd.merge(
        items_df[items_df["dataset"] == "faostat_fbsh"].reset_index(drop=True)[["item_code", "fao_item"]],
        items_df[items_df["dataset"] == "faostat_fbs"].reset_index(drop=True)[["item_code", "fao_item"]],
        how="outer",
        on=["item_code"],
        suffixes=("_fbsh", "_fbs"),
    )
    assert (check["fao_item_fbsh"] == check["fao_item_fbs"]).all()
    # Drop all rows for fbsh, and rename "fbs" to "fbsc" (since this will be the name for the combined dataset).
    items_df = items_df[items_df["dataset"] != "faostat_fbsh"].reset_index(drop=True)
    items_df.loc[items_df["dataset"] == "faostat_fbs", "dataset"] = "faostat_fbsc"

    # Add custom item names.
    items_df = pd.merge(
        items_df,
        custom_items.rename(columns={"fao_item": "fao_item_check"}),
        on=["dataset", "item_code"],
        how="left",
        suffixes=("_new", "_old"),
    )

    changed_descriptions = items_df[
        (items_df["fao_item_description_old"] != items_df["fao_item_description_new"])
        & (items_df["fao_item_description_old"].notnull())
    ]
    if len(changed_descriptions) > 0:
        log.warning(
            f"{len(changed_descriptions)} domains have changed item descriptions. "
            f"Consider updating custom_items.csv."
        )

    items_df = items_df.drop(columns="fao_item_description_old").rename(
        columns={"fao_item_description_new": "fao_item_description"}
    )

    # Check that item names have not changed.
    # NOTE: This condition used to raise an error if not fulfilled. Consider making it an assertion.
    if not (
        items_df[items_df["fao_item_check"].notnull()]["fao_item_check"]
        == items_df[items_df["fao_item_check"].notnull()]["fao_item"]
    ).all():
        log.warning("Item names may have changed with respect to custom items file. Update custom items file.")
    items_df = items_df.drop(columns=["fao_item_check"])

    # Assign original FAO name to all owid items that do not have a custom name.
    items_df["owid_item"] = items_df["owid_item"].fillna(items_df["fao_item"])

    # Add custom item descriptions, and assign original FAO descriptions to items that do not have a custom description.
    items_df["owid_item_description"] = items_df["owid_item_description"].fillna(items_df["fao_item_description"])

    # Check that we have not introduced ambiguities when assigning custom item names.
    n_owid_items_per_item_code = items_df.groupby(["dataset", "item_code"])["owid_item"].transform("nunique")
    error = "Multiple owid items for a given item code in a dataset."
    assert items_df[n_owid_items_per_item_code > 1].empty, error

    items_df = (
        items_df[
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

    return items_df


def create_elements_dataframe_for_domain(
    table: catalog.Table, metadata: catalog.Dataset, dataset_short_name: str
) -> pd.DataFrame:
    """Apply sanity checks to the elements and units of a table in a dataset, and to the elements and units from the
    metadata, harmonize all element code, and add descriptions.

    Parameters
    ----------
    table : catalog.Table
        Data for a given domain.
    metadata: catalog.Dataset
         Additional metadata dataset from meadow.
    dataset_short_name : str
        Dataset short name (e.g. 'faostat_qcl').

    Returns
    -------
    elements_from_data : pd.Dataframe
        Element names and descriptions and unit names and descriptions (customized ones and FAO original ones) for a
        particular domain.

    """

    df = pd.DataFrame(table).reset_index()
    # Load elements from data.
    elements_from_data = (
        df.rename(columns={"element": "fao_element", "unit": "fao_unit_short_name"})[
            ["element_code", "fao_element", "fao_unit_short_name"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    # Sanity check.
    check_that_item_and_element_harmonization_does_not_trim_codes(data=df, category="element")
    # Ensure element_code is always a string of a fix number of characters.
    elements_from_data = harmonize_elements(df=elements_from_data, element_col="fao_element")

    # Load elements from metadata.
    elements_columns = {
        "element_code": "element_code",
        "element": "fao_element",
        "description": "fao_element_description",
    }
    _elements_df = (
        metadata[f"{dataset_short_name}_element"]
        .reset_index()[list(elements_columns)]
        .rename(columns=elements_columns)
        .drop_duplicates()
        .sort_values(list(elements_columns.values()))
        .reset_index(drop=True)
    )
    _elements_df = harmonize_elements(df=_elements_df, element_col="fao_element")
    _elements_df["fao_element_description"] = _elements_df["fao_element_description"].astype("string")

    # Load units metadata.
    units_columns = {
        "unit_name": "fao_unit_short_name",
        "description": "fao_unit",
    }
    _units_df = (
        metadata[f"{dataset_short_name}_unit"]
        .reset_index()[list(units_columns)]
        .rename(columns=units_columns)
        .drop_duplicates()
        .sort_values(list(units_columns.values()))
        .reset_index(drop=True)
    )
    _units_df["fao_unit"] = _units_df["fao_unit"].astype("string")

    # Add element descriptions (from metadata).
    elements_from_data = (
        pd.merge(
            elements_from_data,
            _elements_df,
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
        pd.merge(elements_from_data, _units_df, on=["fao_unit_short_name"], how="left")
        .sort_values(["fao_unit_short_name"])
        .reset_index(drop=True)
    )
    elements_from_data["fao_unit"] = elements_from_data["fao_unit"].fillna(elements_from_data["fao_unit_short_name"])

    # Sanity checks:

    # Check that in data, there is only one unit per element code.
    n_units_per_element_code = df.groupby("element_code")["unit"].transform("nunique")
    error = f"Multiple units for a given element code in dataset {dataset_short_name}."
    assert df[n_units_per_element_code > 1].empty, error

    # Check that in data, there is only one element per element code.
    n_elements_per_element_code = elements_from_data.groupby("element_code")["fao_element"].transform("nunique")
    error = f"Multiple elements for a given element code in dataset {dataset_short_name}."
    assert elements_from_data[n_elements_per_element_code > 1].empty, error

    return elements_from_data


def clean_global_elements_dataframe(elements_df: pd.DataFrame, custom_elements: pd.DataFrame) -> pd.DataFrame:
    """Apply global sanity checks to elements and units gathered from all datasets, and create a clean global elements
    and units dataframe.

    Parameters
    ----------
    elements_df : pd.DataFrame
        Elements and units dataframe gathered from all domains.
    custom_elements : pd.DataFrame
        Data from custom_element_and_units.csv file.

    Returns
    -------
    elements_df : pd.DataFrame
        Clean global elements and units dataframe.

    """
    elements_df = elements_df.copy()

    # Check that all elements of fbsh are in fbs (although fbs may contain additional elements).
    assert set(elements_df[elements_df["dataset"] == "faostat_fbsh"]["element_code"]) <= set(
        elements_df[elements_df["dataset"] == "faostat_fbs"]["element_code"]
    )
    # Drop all rows for fbsh, and rename "fbs" to "fbsc" (since this will be the name for the combined dataset).
    elements_df = elements_df[elements_df["dataset"] != "faostat_fbsh"].reset_index(drop=True)
    elements_df.loc[elements_df["dataset"] == "faostat_fbs", "dataset"] = "faostat_fbsc"

    elements_df = pd.merge(
        elements_df,
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

    changed_units = elements_df[
        (elements_df["fao_unit_new"] != elements_df["fao_unit_old"]) & (elements_df["fao_unit_old"].notnull())
    ]
    if len(changed_units) > 0:
        log.warning(f"{len(changed_units)} domains have changed units, consider updating custom_elements.csv.")

    changed_descriptions = elements_df[
        (elements_df["fao_element_description_new"] != elements_df["fao_element_description_old"])
        & (elements_df["fao_element_description_old"].notnull())
    ]
    if len(changed_descriptions) > 0:
        log.warning(
            f"{len(changed_descriptions)} domains have changed element descriptions. "
            f"Consider updating custom_elements.csv."
        )

    elements_df = elements_df.drop(columns=["fao_unit_old", "fao_element_description_old"]).rename(
        columns={
            "fao_element_description_new": "fao_element_description",
            "fao_unit_new": "fao_unit",
        }
    )

    error = "Element names have changed with respect to custom elements file. Update custom elements file."
    assert (
        elements_df[elements_df["fao_element_check"].notnull()]["fao_element_check"]
        == elements_df[elements_df["fao_element_check"].notnull()]["fao_element"]
    ).all(), error
    elements_df = elements_df.drop(columns=["fao_element_check"])

    error = "Unit names have changed with respect to custom elements file. Update custom elements file."
    assert (
        elements_df[elements_df["fao_unit_short_name_check"].notnull()]["fao_unit_short_name_check"]
        == elements_df[elements_df["fao_unit_short_name_check"].notnull()]["fao_unit_short_name"]
    ).all(), error
    elements_df = elements_df.drop(columns=["fao_unit_short_name_check"])

    # Assign original FAO names where there is no custom one.
    elements_df["owid_element"] = elements_df["owid_element"].fillna(elements_df["fao_element"])
    elements_df["owid_unit"] = elements_df["owid_unit"].fillna(elements_df["fao_unit"])
    elements_df["owid_element_description"] = elements_df["owid_element_description"].fillna(
        elements_df["fao_element_description"]
    )
    elements_df["owid_unit_short_name"] = elements_df["owid_unit_short_name"].fillna(elements_df["fao_unit_short_name"])

    # Assume variables were not per capita, if was_per_capita is not informed, and make boolean.
    elements_df["was_per_capita"] = elements_df["was_per_capita"].fillna("0").replace({"0": False, "1": True})

    # Idem for variables to make per capita.
    elements_df["make_per_capita"] = elements_df["make_per_capita"].fillna("0").replace({"0": False, "1": True})

    # Check that we have not introduced ambiguities when assigning custom element or unit names.
    n_owid_elements_per_element_code = elements_df.groupby(["dataset", "element_code"])["owid_element"].transform(
        "nunique"
    )
    error = "Multiple owid elements for a given element code in a dataset."
    assert elements_df[n_owid_elements_per_element_code > 1].empty, error

    # Check that we have not introduced ambiguities when assigning custom element or unit names.
    n_owid_units_per_element_code = elements_df.groupby(["dataset", "element_code"])["owid_unit"].transform("nunique")
    error = "Multiple owid elements for a given element code in a dataset."
    assert elements_df[n_owid_units_per_element_code > 1].empty, error

    # NOTE: We assert that there is one element for each element code. But the opposite may not be true: there can be
    # multiple element codes with the same element. And idem for items.

    return elements_df


def check_countries_to_exclude_or_harmonize(
    countries_in_data: pd.DataFrame, excluded_countries: List[str], countries_harmonization: Dict[str, str]
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


def clean_global_countries_dataframe(
    countries_in_data: pd.DataFrame,
    country_groups: Dict[str, List[str]],
    countries_harmonization: Dict[str, str],
    excluded_countries: List[str],
) -> pd.DataFrame:
    """Clean dataframe of countries gathered from the data of the individual domains, harmonize country names (and
    country names of members of regions), and create a clean global countries dataframe.

    Parameters
    ----------
    countries_in_data : pd.DataFrame
        Countries gathered from the data of all domains.
    country_groups : dict
        Countries and their members, gathered from the data.
    countries_harmonization : dict
        Mapping of country names (from FAO names to OWID names).
    excluded_countries : list
        Country names to be ignored.

    Returns
    -------
    countries_df : pd.DataFrame
        Clean global countries dataframe.

    """
    countries_df = countries_in_data.copy()

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
    countries_df["country"] = dataframes.map_series(
        series=countries_df["fao_country"],
        mapping=countries_harmonization,
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=False,
        make_unmapped_values_nan=True,
        show_full_warning=True,
    )

    # Add country members to countries dataframe.
    countries_df["members"] = dataframes.map_series(
        series=countries_df["country"],
        mapping=country_groups_harmonized,
        make_unmapped_values_nan=True,
    )

    # Feather does not support object types, so convert column of lists to column of strings.
    countries_df["members"] = [
        json.dumps(members) if isinstance(members, list) else members for members in countries_df["members"]
    ]

    return countries_df


def create_table(df: pd.DataFrame, short_name: str, index_cols: List[str]) -> catalog.Table:
    """Create a table with optimal format and basic metadata, out of a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    short_name : str
        Short name to add in the metadata of the new table.
    index_cols : list
        Columns to use as indexes of the new table.

    Returns
    -------
    table : catalog.Table
        New table.

    """
    table = catalog.Table(df).copy()

    # Optimize column dtypes before storing feather file, and ensure codes are categories (instead of ints).
    table = optimize_table_dtypes(table)

    # Set indexes and other necessary metadata.
    table = table.set_index(index_cols, verify_integrity=True)

    table.metadata.short_name = short_name
    table.metadata.primary_key = index_cols

    return cast(catalog.Table, table)


def check_that_flag_definitions_in_dataset_agree_with_those_in_flags_ranking(
    metadata: catalog.Dataset,
) -> None:
    """Check that the definition of flags in the additional metadata for current dataset agree with the ones we have
    manually written down in our flags ranking (raise error otherwise).

    Parameters
    ----------
    metadata : catalog.Dataset
        Additional metadata dataset (that must contain one table for current dataset).

    """
    for table_name in metadata.table_names:
        if "flag" in table_name:
            flag_df = metadata[table_name].reset_index()
            comparison = pd.merge(FLAGS_RANKING, flag_df, on="flag", how="inner")
            error_message = (
                f"Flag definitions in file {table_name} are different to those in our flags ranking. "
                f"Redefine shared.FLAGS_RANKING."
            )
            assert (comparison["description"] == comparison["flags"]).all(), error_message


def check_that_all_flags_in_dataset_are_in_ranking(table: catalog.Table, metadata_for_flags: catalog.Table) -> None:
    """Check that all flags found in current dataset are defined in our flags ranking (raise error otherwise).

    Parameters
    ----------
    table : pd.DataFrame
        Data table for current dataset.
    metadata_for_flags : catalog.Table
        Flags for current dataset, as defined in dataset of additional metadata.

    """
    if not set(table["flag"]) < set(FLAGS_RANKING["flag"]):
        missing_flags = set(table["flag"]) - set(FLAGS_RANKING["flag"])
        flags_data = pd.DataFrame(metadata_for_flags).reset_index()
        if set(missing_flags) < set(flags_data["flag"]):
            message = "Missing flags. Copy the following lines to FLAGS_RANKING (and put them in the right order):"
            for i, j in pd.DataFrame(metadata_for_flags).loc[list(missing_flags)].iterrows():
                message += f"\n{(i, j['flags'])},"
            log.warning(message)
        else:
            log.warning(
                f"Missing flags. {missing_flags} are not defined in additional metadata. Get definition from "
                f"https://www.fao.org/faostat/en/#definitions"
            )
        raise AssertionError("Flags in dataset not found in FLAGS_RANKING. Manually add those flags.")


def check_definitions_in_value_amendments(
    table: catalog.Table, dataset_short_name: str, value_amendments: pd.DataFrame
) -> None:
    """Check definitions in the value_amendments.csv file.

    This function will assert that:
    * All spurious values defined in the file are still found in the data.
    * There are no unexpected spurious values in the data.

    Spurious values are only searched for in "value" column if it has "category" dtype.
    See regular expression below used to search for spurious values.

    Parameters
    ----------
    table : catalog.Table
        _description_
    dataset_short_name : str
        _description_
    value_amendments : pd.DataFrame
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
    metadata: catalog.Dataset,
    custom_datasets: pd.DataFrame,
    custom_elements: pd.DataFrame,
    custom_items: pd.DataFrame,
    countries_harmonization: Dict[str, str],
    excluded_countries: List[str],
    value_amendments: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply various sanity checks, gather data (about dataset, item, element and unit names and descriptions) from all
    domains, compare with data from its corresponding metadata file, and create clean dataframes of metadata about
    dataset, elements, units, items, and countries.

    Parameters
    ----------
    metadata : catalog.Dataset
        Additional metadata dataset from meadow.
    custom_datasets : pd.DataFrame
        Data from custom_datasets.csv file.
    custom_elements : pd.DataFrame
        Data from custom_elements_and_units.csv file.
    custom_items : pd.DataFrame
        Data from custom_items.csv file.
    countries_harmonization : dict
        Data from faostat.countries.json file.
    excluded_countries : list
        Data from faostat.excluded_countries.json file.
    value_amendments : pd.DataFrame
        Data from value_amendments.csv file.

    Returns
    -------
    countries_df : pd.DataFrame
        Clean dataframe of global countries.
    datasets_df : pd.DataFrame
        Clean dataframe of global dataset names and descriptions.
    elements_df : pd.DataFrame
        Clean dataframe of global element and unit names and descriptions.
    items_df : pd.DataFrame
        Clean dataframe of global item names and descriptions.

    """
    # Check if flags definitions need to be updated.
    check_that_flag_definitions_in_dataset_agree_with_those_in_flags_ranking(metadata)

    # List all FAOSTAT dataset short names.
    dataset_short_names = sorted(
        set([NAMESPACE + "_" + table_name.split("_")[1] for table_name in metadata.table_names])
    )

    # Initialise dataframe of dataset descriptions, items, and element-units.
    # We cannot remove "dataset" from the items and elements dataframes, because it can happen that, for a given
    # item code, the item name is slightly different in two different datasets.
    datasets_df = pd.DataFrame({"dataset": [], "fao_dataset_title": [], "fao_dataset_description": []})
    items_df = pd.DataFrame({"dataset": [], "item_code": [], "fao_item": [], "fao_item_description": []})
    elements_df = pd.DataFrame(
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
    countries_in_data = pd.DataFrame({"area_code": [], "fao_country": []}).astype({"area_code": "Int64"})
    country_groups_in_data: Dict[str, List[str]] = {}

    # Gather all variables from the latest version of each meadow dataset.
    for dataset_short_name in tqdm(dataset_short_names, file=sys.stdout):
        # Load latest meadow table for current dataset.
        ds_latest: catalog.Dataset = paths.load_dependency(dataset_short_name)
        table = ds_latest[dataset_short_name]
        df = pd.DataFrame(table.reset_index()).rename(
            columns={
                "area": "fao_country",
                "recipient_country": "fao_country",
                "recipient_country_code": "area_code",
            }
        )[["area_code", "fao_country"]]

        df["area_code"] = df["area_code"].astype("Int64")

        # Temporary patch.
        if dataset_short_name == "faostat_wcad":
            error = (
                "Dataset faostat_wcad had 'French Guiana' for area code 69 (unlike other datasets, that had "
                "'French Guyana'). But this may no longer the case, so this patch in the code can be removed."
            )
            assert "French Guiana" in df["fao_country"].unique(), error
            df["fao_country"] = dataframes.map_series(df["fao_country"], mapping={"French Guiana": "French Guyana"})

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
        datasets_from_data = create_dataset_descriptions_dataframe_for_domain(
            table, dataset_short_name=dataset_short_name
        )

        items_from_data = create_items_dataframe_for_domain(
            table=table, metadata=metadata, dataset_short_name=dataset_short_name
        )

        elements_from_data = create_elements_dataframe_for_domain(
            table=table, metadata=metadata, dataset_short_name=dataset_short_name
        )

        # Add countries in this dataset to the list of all countries.
        countries_in_data = pd.concat([countries_in_data, df]).drop_duplicates()

        # Get country groups in this dataset.
        area_group_table_name = f"{dataset_short_name}_area_group"
        if area_group_table_name in metadata:
            country_groups = (
                metadata[f"{dataset_short_name}_area_group"]
                .reset_index()
                .drop_duplicates(subset=["country_group", "country"])
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

        # Add dataset descriptions, items, and element-units from current dataset to global dataframes.
        datasets_df = dataframes.concatenate([datasets_df, datasets_from_data], ignore_index=True)
        items_df = dataframes.concatenate([items_df, items_from_data], ignore_index=True)
        elements_df = dataframes.concatenate([elements_df, elements_from_data], ignore_index=True)

    datasets_df = clean_global_dataset_descriptions_dataframe(datasets_df=datasets_df, custom_datasets=custom_datasets)
    items_df = clean_global_items_dataframe(items_df=items_df, custom_items=custom_items)

    elements_df = clean_global_elements_dataframe(elements_df=elements_df, custom_elements=custom_elements)
    countries_df = clean_global_countries_dataframe(
        countries_in_data=countries_in_data,
        country_groups=country_groups_in_data,
        countries_harmonization=countries_harmonization,
        excluded_countries=excluded_countries,
    )

    return countries_df, datasets_df, elements_df, items_df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = Path(dest_dir).name

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
    metadata: catalog.Dataset = paths.load_dependency(FAOSTAT_METADATA_SHORT_NAME)

    # Load custom dataset names, items, element-unit names, and value amendments.
    custom_datasets = pd.read_csv(custom_datasets_file, dtype=str)
    custom_elements = pd.read_csv(custom_elements_and_units_file, dtype=str)
    custom_items = pd.read_csv(custom_items_file, dtype=str)
    value_amendments = pd.read_csv(value_amendments_file, dtype=str)

    # Load country mapping and excluded countries files.
    countries_harmonization = io.load_json(countries_file)
    excluded_countries = io.load_json(excluded_countries_file)

    #
    # Process data.
    #
    countries_df, datasets_df, elements_df, items_df = process_metadata(
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
    dataset_garden = catalog.Dataset.create_empty(dest_dir)
    dataset_garden.short_name = FAOSTAT_METADATA_SHORT_NAME
    # Keep original dataset's metadata from meadow.
    dataset_garden.metadata = deepcopy(metadata.metadata)
    # Create new dataset in garden.
    dataset_garden.save()

    # Create new garden dataset with all dataset descriptions, items, element-units, and countries.
    datasets_table = create_table(df=datasets_df, short_name="datasets", index_cols=["dataset"])
    items_table = create_table(df=items_df, short_name="items", index_cols=["dataset", "item_code"])
    elements_table = create_table(df=elements_df, short_name="elements", index_cols=["dataset", "element_code"])
    countries_table = create_table(df=countries_df, short_name="countries", index_cols=["area_code"])
    amendments_table = catalog.Table(value_amendments, short_name="amendments").set_index(
        ["dataset", "spurious_value"], verify_integrity=True
    )

    # Add tables to dataset.
    dataset_garden.add(datasets_table, repack=False)
    dataset_garden.add(items_table, repack=False)
    dataset_garden.add(elements_table, repack=False)
    dataset_garden.add(countries_table, repack=False)
    dataset_garden.add(amendments_table, repack=False)
