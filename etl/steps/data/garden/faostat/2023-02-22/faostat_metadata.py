"""FAOSTAT garden step for faostat_metadata dataset.

This step reads from:
* The (additional) metadata dataset. The only crucial ingredients from here (that will be used later on in other garden
  steps are element, item and units descriptions, and country groups (used to check that we do not double count
  countries when aggregating data for regions).
* Custom datasets file ('./custom_datasets.csv').
* Custom elements and units file ('./custom_elements_and_units.csv').
* Custom items file ('./custom_items.csv').
* Each of the individual meadow datasets. They are loaded to extract their countries, items, elements and units, and
  some sanity checks are performed.

This step will:
* Output a dataset (to be loaded by all garden datasets) with tables 'countries, 'datasets', 'elements' and 'items'.
* Apply sanity checks to countries, elements, items, and units.
* Apply custom names and descriptions to datasets, elements, items and units.
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
  * In dataset sdgb, item codes have very unusual names, and they are not found in the metadata. We haven't figured
    out the root of the issue yet.

There are several cases in which one or a few item codes in the data are missing in the metadata. Also, there are
several cases in which an item code in the data has an item name slightly different in the metadata. But these are not
important issues (since we use item_code to merge different datasets, and we use metadata only to fetch descriptions).
However, for some domains there are too many differences between items in the data and in the metadata (as explained
above). For this reason, raise a warning only when there is a reasonable number of issues found.

"""

import json
import sys
from copy import deepcopy
from typing import Dict, List, Tuple, cast

import pandas as pd
from owid import catalog
from owid.datautils import dataframes, io
from shared import (
    FLAGS_RANKING,
    LATEST_VERSIONS_FILE,
    NAMESPACE,
    VERSION,
    harmonize_elements,
    harmonize_items,
    log,
    optimize_table_dtypes,
)
from tqdm.auto import tqdm

from etl.paths import DATA_DIR, STEP_DIR

# Define short name for output dataset.
DATASET_SHORT_NAME = f"{NAMESPACE}_metadata"

# Minimum number of issues in the comparison of items and item codes from data and metadata to raise a warning.
N_ISSUES_ON_ITEMS_FOR_WARNING = 10


def load_latest_data_table_for_dataset(dataset_short_name: str) -> catalog.Table:
    """Load data table (in long format) from the latest version of a dataset for a given domain.

    Parameters
    ----------
    dataset_short_name : str
        Dataset short name (e.g. 'faostat_qcl').

    Returns
    -------
    table : catalog.Table
        Latest version of table in long format for given domain.

    """
    # Path to folder with all versions of meadow datasets for FAOSTAT.
    meadow_dir = DATA_DIR / "meadow" / NAMESPACE
    # Load file of versions.
    latest_versions = pd.read_csv(LATEST_VERSIONS_FILE).set_index(["channel", "dataset"])
    # Find latest meadow version for given dataset.
    dataset_version = latest_versions.loc["meadow", dataset_short_name].item()
    # Path to latest dataset folder.
    dataset_path = meadow_dir / dataset_version / dataset_short_name
    assert dataset_path.is_dir(), f"Dataset {dataset_short_name} not found in meadow."
    # Load dataset.
    dataset = catalog.Dataset(dataset_path)
    assert len(dataset.table_names) == 1
    # Load table in long format from dataset.
    table = dataset[dataset_short_name]

    return table


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

    changed_titles = datasets_df[datasets_df["fao_dataset_title_old"] != datasets_df["fao_dataset_title_new"]]
    changed_descriptions = datasets_df[
        datasets_df["fao_dataset_description_old"] != datasets_df["fao_dataset_description_new"]
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

    # Add custom descriptions.
    datasets_df["owid_dataset_description"] = datasets_df["owid_dataset_description"].fillna(
        datasets_df["fao_dataset_description"]
    )

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
            f"WARNING: {len(changed_descriptions)} domains have changed descriptions. "
            f"Consider updating custom_items.csv."
        )

    items_df = items_df.drop(columns="fao_item_description_old").rename(
        columns={"fao_item_description_new": "fao_item_description"}
    )

    error = "Item names may have changed with respect to custom items file. Update custom items file."
    assert (
        items_df[items_df["fao_item_check"].notnull()]["fao_item_check"]
        == items_df[items_df["fao_item_check"].notnull()]["fao_item"]
    ).all(), error
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
        Data from custom_items.csv file.

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
            f"{len(changed_descriptions)} domains have changed descriptions. " f"Consider updating custom_elements.csv."
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


def clean_global_countries_dataframe(
    countries_in_data: pd.DataFrame,
    country_groups: Dict[str, List[str]],
    countries_harmonization: Dict[str, str],
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

    Returns
    -------
    countries_df : pd.DataFrame
        Clean global countries dataframe.

    """
    countries_df = countries_in_data.copy()

    # Remove duplicates of area_code and fao_country, ensuring to keep m49_code when it is given.
    if "m49_code" in countries_df.columns:
        # Sort so that nans in m49_code are at the bottom, and then keep only the first duplicated row.
        countries_df = countries_df.sort_values("m49_code")
    countries_df = (
        countries_df.drop_duplicates(subset=["area_code", "fao_country"], keep="first")
        .sort_values(["area_code"])
        .reset_index(drop=True)
    )

    countries_not_harmonized = sorted(set(countries_df["fao_country"]) - set(countries_harmonization))
    if len(countries_not_harmonized) > 0:
        log.info(
            f"{len(countries_not_harmonized)} countries not included in countries file. "
            f"They will not have data after countries are harmonized in a further step."
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
        warn_on_unused_mappings=True,
        make_unmapped_values_nan=True,
        show_full_warning=False,
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


def process_metadata(
    metadata: catalog.Dataset,
    custom_datasets: pd.DataFrame,
    custom_elements: pd.DataFrame,
    custom_items: pd.DataFrame,
    countries_harmonization: Dict[str, str],
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
        table = load_latest_data_table_for_dataset(dataset_short_name=dataset_short_name)
        df = pd.DataFrame(table.reset_index()).rename(
            columns={
                "area": "fao_country",
                "recipient_country": "fao_country",
                "recipient_country_code": "area_code",
            }
        )[["area_code", "fao_country"]]

        # Column 'area_code' in faostat_sdgb is float instead of integer, and it does not agree with the usual area
        # codes. For example, Afghanistan has area code 4.0 in faostat_sdgb, whereas in other dataset it is 2.
        # It seems to be the UN M49 code.
        # So we add this code as a new column to the countries dataframe, to be able to map sdgb area codes later on.
        if df["area_code"].dtype == "float64":
            sdgb_codes_df = (
                metadata["faostat_sdgb_area"]
                .reset_index()[["country_code", "m49_code"]]
                .rename(columns={"country_code": "area_code"})
            )
            df = pd.merge(
                df.rename(columns={"area_code": "m49_code"}),
                sdgb_codes_df,
                on="m49_code",
                how="left",
            )

        df["area_code"] = df["area_code"].astype("Int64")

        check_that_all_flags_in_dataset_are_in_ranking(
            table=table, metadata_for_flags=metadata[f"{dataset_short_name}_flag"]
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
    )

    return countries_df, datasets_df, elements_df, items_df


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Path to latest garden version for FAOSTAT.
    garden_code_dir = STEP_DIR / "data" / "garden" / NAMESPACE / VERSION
    # Path to file with custom dataset titles and descriptions.
    custom_datasets_file = garden_code_dir / "custom_datasets.csv"
    # Path to file with custom item names and descriptions.
    custom_items_file = garden_code_dir / "custom_items.csv"
    # Path to file with custom element and unit names and descriptions.
    custom_elements_and_units_file = garden_code_dir / "custom_elements_and_units.csv"

    # Load file of versions.
    latest_versions = pd.read_csv(LATEST_VERSIONS_FILE).set_index(["channel", "dataset"])

    # Find latest meadow version of dataset of FAOSTAT metadata.
    metadata_version = latest_versions.loc["meadow", DATASET_SHORT_NAME].item()
    metadata_path = DATA_DIR / "meadow" / NAMESPACE / metadata_version / DATASET_SHORT_NAME

    # Countries file, with mapping from FAO names to OWID harmonized country names.
    countries_file = garden_code_dir / f"{NAMESPACE}.countries.json"

    ####################################################################################################################
    # Load and process data.
    ####################################################################################################################

    # Load metadata from meadow.
    assert metadata_path.is_dir()
    metadata = catalog.Dataset(metadata_path)

    # Load custom dataset names, items, and element-unit names.
    custom_datasets = pd.read_csv(custom_datasets_file, dtype=str)
    custom_elements = pd.read_csv(custom_elements_and_units_file, dtype=str)
    custom_items = pd.read_csv(custom_items_file, dtype=str)

    # Load countries file.
    countries_harmonization = io.load_json(countries_file)

    countries_df, datasets_df, elements_df, items_df = process_metadata(
        metadata=metadata,
        custom_datasets=custom_datasets,
        custom_elements=custom_elements,
        custom_items=custom_items,
        countries_harmonization=countries_harmonization,
    )

    ####################################################################################################################
    # Save outputs.
    ####################################################################################################################

    # Initialize new garden dataset.
    dataset_garden = catalog.Dataset.create_empty(dest_dir)
    dataset_garden.short_name = DATASET_SHORT_NAME
    # Keep original dataset's metadata from meadow.
    dataset_garden.metadata = deepcopy(metadata.metadata)
    # Create new dataset in garden.
    dataset_garden.save()

    # Create new garden dataset with all dataset descriptions, items, element-units, and countries.
    datasets_table = create_table(df=datasets_df, short_name="datasets", index_cols=["dataset"])
    items_table = create_table(df=items_df, short_name="items", index_cols=["dataset", "item_code"])
    elements_table = create_table(df=elements_df, short_name="elements", index_cols=["dataset", "element_code"])

    countries_table = create_table(df=countries_df, short_name="countries", index_cols=["area_code"])

    # Add tables to dataset (no need to repack, since columns already have optimal dtypes).
    dataset_garden.add(datasets_table, repack=False)
    dataset_garden.add(items_table, repack=False)
    dataset_garden.add(elements_table, repack=False)
    dataset_garden.add(countries_table, repack=False)
