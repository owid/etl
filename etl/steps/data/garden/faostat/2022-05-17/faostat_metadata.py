"""Harmonization of variables (items, elements and units).

This step will:
* Fix some known issues with items.
* Adds descriptions to items, elements and units.
* Apply custom definitions of items, elements and units.
* Ensures there are no degeneracies within a dataset.
* Ensures there are no degeneracies between datasets (using dataset, item_code, element_code as keys).
* Outputs a dataset that will be loaded by all garden datasets, with tables:
    * items.
    * elements.
    * units.
    * countries?

There are some non-trivial issues with the definitions of items at FAOSTAT:
* Some item codes in the data are missing in the metadata, and vice versa.
* The mapping item_code -> item in the data files is sometimes different from the mapping item_code -> item
  in the metadata retrieved with the API. Some examples:
  * For the scl dataset, it seems that item_code in the data corresponds to cpc_code in the metadata. For example,
    item "Wheat" in the data has item code "0111", but in the metadata, "Wheat" has item code 15 (and cpc code "0111").
    This does not affect the data values, but if we wanted to merge this dataset with another one using item code,
    we would get wrong results. Also, descriptions fetched from the metadata may be wrong for this dataset.
    The issue could be solved in this script.
    TODO: Consider fixing this issue by mapping item code in data to cpc code in metadata, and retrieving item code
     from metadata (after checking that it is indeed correct).
  * In dataset qv, item code 221 in the data corresponds to item "Almonds, in shell", whereas in the metadata,
    item code 221 corresponds to item "Almonds, with shell", which is the same item, but with a slightly different
    name. This happens with many items. On the website (https://www.fao.org/faostat/en/?#data/QV) they seem to be
    using the naming from the metadata. We can safely ignore this issue, and stick to the names in the data.
  * In dataset sdgb, item codes have very unusual names, and they are not found in the metadata. We haven't figured
    out the root of the issue yet.
Given all this, we decided to use the metadata only to fetch descriptions, but we trust that the item code -> item
mapping in the data is the correct one (except possibly in the examples mentioned above).

"""

from copy import deepcopy
from typing import List

import pandas as pd
from owid import catalog
from owid.datautils import dataframes
from tqdm.auto import tqdm

from etl.paths import DATA_DIR, STEP_DIR
from .shared import FLAGS_RANKING, harmonize_elements, harmonize_items, VERSION, optimize_table_dtypes

# Define namespace and short name for output dataset.
NAMESPACE = "faostat"
DATASET_SHORT_NAME = f"{NAMESPACE}_metadata"

# There are several cases in which one or a few item codes in the data are missing in the metadata. Also, there are
# several cases in which an item code in the data has an item name slightly different in the metadata. But these are not
# important issues (since we use item_code to merge different datasets, and we use metadata only to fetch descriptions).
# However, for some domains there are too many differences between items in the data and in the metadata (as explained
# above). For this reason, raise a warning only when there is a reasonable number of issues found.
# Minimum number of issues in the comparison of items and item codes from data and metadata to raise a warning.
N_ISSUES_ON_ITEMS_FOR_WARNING = 10


def load_latest_data_table_for_dataset(dataset_short_name: str) -> catalog.Table:
    meadow_dir = DATA_DIR / "meadow" / NAMESPACE
    dataset_version = sorted(meadow_dir.glob(f"*/{dataset_short_name}"))[-1].parent.name
    dataset_path = meadow_dir / dataset_version / dataset_short_name
    assert dataset_path.is_dir(), f"Dataset {dataset_short_name} not found in meadow."
    dataset = catalog.Dataset(dataset_path)
    assert len(dataset.table_names) == 1
    table = dataset[dataset_short_name]

    return table


def create_dataset_descriptions_dataframe_for_domain(table: catalog.Table, dataset_short_name: str) -> pd.DataFrame:
    dataset_descriptions_df = pd.DataFrame({"dataset": [dataset_short_name],
                                            "fao_dataset_title": [table.metadata.dataset.title],
                                            "fao_dataset_description": [table.metadata.dataset.description]})

    return dataset_descriptions_df


def clean_global_dataset_descriptions_dataframe(datasets_df: pd.DataFrame,
                                                custom_datasets: pd.DataFrame) -> pd.DataFrame:
    datasets_df = datasets_df.copy()

    # Check that the dataset descriptions of fbsh and fbs are identical.
    error = "Datasets fbsh and fbs have different descriptions. " \
        "This may happen in the future: Simply check that nothing significant has changed and remove this assertion."
    assert datasets_df[datasets_df["dataset"] == "faostat_fbsh"]["fao_dataset_description"].item() ==\
        datasets_df[datasets_df["dataset"] == "faostat_fbs"]["fao_dataset_description"].item(), error
    # Drop row for fbsh, and rename "fbs" to "fbsc" (since this will be the name for the combined dataset).
    datasets_df = datasets_df[datasets_df["dataset"] != "faostat_fbsh"].reset_index(drop=True)
    datasets_df.loc[datasets_df["dataset"] == "faostat_fbs", "dataset"] = "faostat_fbsc"

    # Add custom dataset titles.
    datasets_df = pd.merge(datasets_df, custom_datasets, on="dataset", how="left", suffixes=("_new", "_old"))

    changed_titles = datasets_df[datasets_df["fao_dataset_title_old"] != datasets_df["fao_dataset_title_new"]]
    changed_descriptions = datasets_df[datasets_df["fao_dataset_description_old"] !=
                                       datasets_df["fao_dataset_description_new"]]
    if len(changed_titles) > 0:
        print(f"WARNING: {len(changed_titles)} domains have changed titles, consider updating custom_datasets.csv.")
    if len(changed_descriptions) > 0:
        print(f"WARNING: {len(changed_descriptions)} domains have changed descriptions. "
              f"Consider updating custom_datasets.csv.")

    datasets_df = datasets_df.drop(columns=["fao_dataset_title_old", "fao_dataset_description_old"]).rename(columns={
        "fao_dataset_title_new": "fao_dataset_title", "fao_dataset_description_new": "fao_dataset_description"})

    datasets_df["owid_dataset_title"] = datasets_df["owid_dataset_title"].fillna(datasets_df["fao_dataset_title"])
    error = "Custom titles for different datasets are equal. Edit custom_datasets.csv file."
    assert len(set(datasets_df["dataset"])) == len(set(datasets_df["owid_dataset_title"])), error

    # Add custom descriptions.
    datasets_df["owid_dataset_description"] = datasets_df["owid_dataset_description"].\
        fillna(datasets_df["fao_dataset_description"])

    # Reorder columns.
    datasets_df = datasets_df[["dataset", "fao_dataset_title", "owid_dataset_title", "fao_dataset_description",
                               "owid_dataset_description"]]

    return datasets_df


def create_items_dataframe_for_domain(table: catalog.Table, metadata: catalog.Dataset, dataset_short_name: str
                                      ) -> pd.DataFrame:
    df = pd.DataFrame(table).reset_index()

    # Load items from data.
    items_from_data = df.rename(columns={"item": "fao_item"})[["item_code", "fao_item"]].drop_duplicates().\
        reset_index(drop=True)
    # Ensure items are well constructed and amend already known issues (defined in shared.ITEM_AMENDMENTS).
    items_from_data = harmonize_items(df=items_from_data, dataset_short_name=dataset_short_name, item_col="fao_item")

    # Load items from metadata.
    items_columns = {
        "item_code": "item_code",
        "item": "fao_item",
        "description": "fao_item_description",
    }
    _items_df = metadata[f"{dataset_short_name}_item"].reset_index()[list(items_columns)].\
        rename(columns=items_columns).drop_duplicates().sort_values(list(items_columns.values())).\
        reset_index(drop=True)
    _items_df = harmonize_items(df=_items_df, dataset_short_name=dataset_short_name, item_col="fao_item")
    _items_df["fao_item_description"] = _items_df["fao_item_description"].astype(str)

    # Add descriptions (from metadata) to items (from data).
    items_from_data = pd.merge(items_from_data, _items_df, on=["item_code", "fao_item"], how="left").\
        sort_values(["item_code", "fao_item"]).reset_index(drop=True)
    items_from_data["dataset"] = dataset_short_name
    items_from_data["fao_item_description"] = items_from_data["fao_item_description"].fillna("")

    # Sanity checks for items in current dataset:

    # Check that in data, there is only one item per item code.
    n_items_per_item_code = items_from_data.groupby("item_code")["fao_item"].transform("nunique")
    error = f"Multiple items for a given item code in dataset {dataset_short_name}."
    assert items_from_data[n_items_per_item_code > 1].empty, error

    # Check that all item codes in data are defined in metadata, and check that the mapping item code -> item in
    # the data is the same as in the metadata (which often is not the case).
    compared = pd.merge(items_from_data[["item_code", "fao_item"]], _items_df[["item_code", "fao_item"]],
                        on="item_code", how="left", suffixes=("_in_data", "_in_metadata"))
    different_items = compared[compared["fao_item_in_data"] != compared["fao_item_in_metadata"]]
    missing_item_codes = set(items_from_data["item_code"]) - set(_items_df["item_code"])
    if (len(different_items) + len(missing_item_codes)) > N_ISSUES_ON_ITEMS_FOR_WARNING:
        print(f"WARNING: {len(missing_item_codes)} item codes in {dataset_short_name} missing in metadata. "
              f"{len(different_items)} item codes in data mapping to different items in metadata.")

    return items_from_data


def clean_global_items_dataframe(items_df: pd.DataFrame, custom_items: pd.DataFrame) -> pd.DataFrame:
    items_df = items_df.copy()

    # Check that fbs and fbsh have the same contributions, remove one of them, and rename the other to fbsc.
    check = pd.merge(items_df[items_df["dataset"] == "faostat_fbsh"].reset_index(drop=True)[["item_code", "fao_item"]],
                     items_df[items_df["dataset"] == "faostat_fbs"].reset_index(drop=True)[["item_code", "fao_item"]],
                     how="outer", on=["item_code"], suffixes=("_fbsh", "_fbs"))
    assert (check["fao_item_fbsh"] == check["fao_item_fbs"]).all()
    # Drop all rows for fbsh, and rename "fbs" to "fbsc" (since this will be the name for the combined dataset).
    items_df = items_df[items_df["dataset"] != "faostat_fbsh"].reset_index(drop=True)
    items_df.loc[items_df["dataset"] == "faostat_fbs", "dataset"] = "faostat_fbsc"

    # Add custom item names.
    items_df = pd.merge(items_df, custom_items.rename(columns={"fao_item": "fao_item_check"}),
                        on=["dataset", "item_code"], how="left", suffixes=("_new", "_old"))

    changed_descriptions = items_df[(items_df["fao_item_description_old"] != items_df["fao_item_description_new"]) &
                                    (items_df["fao_item_description_old"].notnull())]
    if len(changed_descriptions) > 0:
        print(f"WARNING: {len(changed_descriptions)} domains have changed descriptions. "
              f"Consider updating custom_items.csv.")

    items_df = items_df.drop(columns="fao_item_description_old").rename(
        columns={"fao_item_description_new": "fao_item_description"})

    error = f"Item names may have changed with respect to custom items file. Update custom items file."
    assert (items_df[items_df["fao_item_check"].notnull()]["fao_item_check"] ==
            items_df[items_df["fao_item_check"].notnull()]["fao_item"]).all(), error
    items_df = items_df.drop(columns=["fao_item_check"])

    # Assign original FAO name to all owid items that do not have a custom name.
    items_df["owid_item"] = items_df["owid_item"].fillna(items_df["fao_item"])

    # Add custom item descriptions, and assign original FAO descriptions to items that do not have a custom description.
    items_df["owid_item_description"] = items_df["owid_item_description"].\
        fillna(items_df["fao_item_description"])

    # Check that we have not introduced ambiguities when assigning custom item names.
    n_owid_items_per_item_code = items_df.groupby(["dataset", "item_code"])["owid_item"].transform("nunique")
    error = "Multiple owid items for a given item code in a dataset."
    assert items_df[n_owid_items_per_item_code > 1].empty, error

    items_df = items_df[["dataset", "item_code", "fao_item", "owid_item", "fao_item_description",
                         "owid_item_description"]].sort_values(["dataset", "item_code"]).reset_index(drop=True)

    return items_df


def create_elements_dataframe_for_domain(table: catalog.Table, metadata: catalog.Dataset, dataset_short_name: str
                                         ) -> pd.DataFrame:
    df = pd.DataFrame(table).reset_index()
    # Load elements from data.
    elements_from_data = df.rename(columns={"element": "fao_element", "unit": "fao_unit_short_name"})[[
        "element_code", "fao_element", "fao_unit_short_name"]].drop_duplicates().reset_index(drop=True)
    # Ensure element_code is always a string of a fix number of characters.
    elements_from_data = harmonize_elements(df=elements_from_data, element_col="fao_element")

    # Load elements from metadata.
    elements_columns = {
        "element_code": "element_code",
        "element": "fao_element",
        "description": "fao_element_description",
    }
    _elements_df = metadata[f"{dataset_short_name}_element"].reset_index()[list(elements_columns)].\
        rename(columns=elements_columns).drop_duplicates().sort_values(list(elements_columns.values())).\
        reset_index(drop=True)
    _elements_df = harmonize_elements(df=_elements_df, element_col="fao_element")
    _elements_df["fao_element_description"] = _elements_df["fao_element_description"].astype(str)

    # Load units metadata.
    units_columns = {
        "unit_name": "fao_unit_short_name",
        "description": "fao_unit",
    }
    _units_df = metadata[f"{dataset_short_name}_unit"].reset_index()[list(units_columns)].\
        rename(columns=units_columns).drop_duplicates().sort_values(list(units_columns.values())).\
        reset_index(drop=True)
    _units_df["fao_unit"] = _units_df["fao_unit"].astype(str)

    # Add element descriptions (from metadata).
    elements_from_data = pd.merge(elements_from_data, _elements_df, on=["element_code", "fao_element"], how="left").\
        sort_values(["element_code", "fao_element"]).reset_index(drop=True)
    elements_from_data["dataset"] = dataset_short_name
    elements_from_data["fao_element_description"] = elements_from_data["fao_element_description"].fillna("")

    # Add unit descriptions (from metadata).
    elements_from_data = pd.merge(elements_from_data, _units_df, on=["fao_unit_short_name"], how="left").\
        sort_values(["fao_unit_short_name"]).reset_index(drop=True)
    elements_from_data["fao_unit"] = elements_from_data["fao_unit"].fillna("")

    # Sanity checks:

    # Check that in data, there is only one unit per element code.
    n_units_per_element_code = df.groupby("element_code")["unit"].transform("nunique")
    error = f"Multiple units for a given element code in dataset {dataset_short_name}."
    assert df[n_units_per_element_code > 1].empty, error

    # Check that in data, there is only one element per element code.
    n_elements_per_element_code = elements_from_data.groupby("element_code")["fao_element"].\
        transform("nunique")
    error = f"Multiple elements for a given element code in dataset {dataset_short_name}."
    assert elements_from_data[n_elements_per_element_code > 1].empty, error

    return elements_from_data


def clean_global_elements_dataframe(elements_df: pd.DataFrame, custom_elements: pd.DataFrame) -> pd.DataFrame:
    elements_df = elements_df.copy()

    # Check that all elements of fbsh are in fbs (although fbs may contain additional elements).
    assert set(elements_df[elements_df["dataset"] == "faostat_fbsh"]["element_code"]) <= \
           set(elements_df[elements_df["dataset"] == "faostat_fbs"]["element_code"])
    # Drop all rows for fbsh, and rename "fbs" to "fbsc" (since this will be the name for the combined dataset).
    elements_df = elements_df[elements_df["dataset"] != "faostat_fbsh"].reset_index(drop=True)
    elements_df.loc[elements_df["dataset"] == "faostat_fbs", "dataset"] = "faostat_fbsc"

    elements_df = pd.merge(elements_df, custom_elements.rename(columns={
        "fao_element": "fao_element_check", "fao_unit_short_name": "fao_unit_short_name_check"}),
             on=["dataset", "element_code"], how="left", suffixes=("_new", "_old"))

    changed_units = elements_df[(elements_df["fao_unit_new"] != elements_df["fao_unit_old"]) &
                                (elements_df["fao_unit_old"].notnull())]
    if len(changed_units) > 0:
        print(f"WARNING: {len(changed_units)} domains have changed units, consider updating custom_elements.csv.")

    changed_descriptions = elements_df[(elements_df["fao_element_description_new"] !=
                                        elements_df["fao_element_description_old"]) &
                                       (elements_df["fao_element_description_old"].notnull())]
    if len(changed_descriptions) > 0:
        print(f"WARNING: {len(changed_descriptions)} domains have changed descriptions. "
              f"Consider updating custom_elements.csv.")

    elements_df = elements_df.drop(columns=["fao_unit_old", "fao_element_description_old"]).rename(
        columns={"fao_element_description_new": "fao_element_description", "fao_unit_new": "fao_unit"})

    error = f"Element names have changed with respect to custom elements file. Update custom elements file."
    assert (elements_df[elements_df["fao_element_check"].notnull()]["fao_element_check"] ==
            elements_df[elements_df["fao_element_check"].notnull()]["fao_element"]).all(), error
    elements_df = elements_df.drop(columns=["fao_element_check"])

    error = f"Unit names have changed with respect to custom elements file. Update custom elements file."
    assert (elements_df[elements_df["fao_unit_short_name_check"].notnull()]["fao_unit_short_name_check"] ==
            elements_df[elements_df["fao_unit_short_name_check"].notnull()]["fao_unit_short_name"]).all(), error
    elements_df = elements_df.drop(columns=["fao_unit_short_name_check"])

    # Assign original FAO names where there is no custom one.
    elements_df["owid_element"] = elements_df["owid_element"].fillna(elements_df["fao_element"])
    elements_df["owid_unit"] = elements_df["owid_unit"].fillna(elements_df["fao_unit"])
    elements_df["owid_element_description"] = elements_df["owid_element_description"].\
        fillna(elements_df["fao_element_description"])
    elements_df["owid_unit_short_name"] = elements_df["owid_unit_short_name"].\
        fillna(elements_df["fao_unit_short_name"])

    # Assume variables were not per capita, if was_per_capita is not informed, and make boolean.
    elements_df["was_per_capita"] = elements_df["was_per_capita"].fillna("0").replace({"0": False, "1": True})

    # Idem for variables to make per capita.
    elements_df["make_per_capita"] = elements_df["make_per_capita"].fillna("0").replace({"0": False, "1": True})

    # Check that we have not introduced ambiguities when assigning custom element or unit names.
    n_owid_elements_per_element_code = elements_df.groupby(["dataset", "element_code"])["owid_element"].\
        transform("nunique")
    error = "Multiple owid elements for a given element code in a dataset."
    assert elements_df[n_owid_elements_per_element_code > 1].empty, error

    # Check that we have not introduced ambiguities when assigning custom element or unit names.
    n_owid_units_per_element_code = elements_df.groupby(["dataset", "element_code"])["owid_unit"].\
        transform("nunique")
    error = "Multiple owid elements for a given element code in a dataset."
    assert elements_df[n_owid_units_per_element_code > 1].empty, error

    # NOTE: We assert that there is one element for each element code. But the opposite may not be true: there can be
    # multiple element codes with the same element. And idem for items.

    return elements_df


def create_table(df: pd.DataFrame, short_name: str, index_cols: List[str]) -> catalog.Table:
    table = catalog.Table(df).copy()

    # Optimize column dtypes before storing feather file, and ensure codes are categories (instead of ints).
    table = optimize_table_dtypes(table)

    # Set indexes and other necessary metadata.
    table = table.set_index(index_cols)
    table.metadata.short_name = short_name
    table.metadata.primary_key = index_cols

    return table


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
            assert (
                comparison["description"] == comparison["flags"]
            ).all(), error_message


def check_that_all_flags_in_dataset_are_in_ranking(
    table: catalog.Table,
    metadata_for_flags: catalog.Table,
) -> None:
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
            print(
                "Manually copy the following lines to FLAGS_RANKING (and put them in the right order):"
            )
            for i, j in (
                pd.DataFrame(metadata_for_flags)
                .loc[list(missing_flags)]
                .iterrows()
            ):
                print(f"{(i, j['flags'])},")
        else:
            print(
                f"Not all flags ({missing_flags}) are defined in additional metadata. Get their definition from "
                f"https://www.fao.org/faostat/en/#definitions"
            )
        raise AssertionError(
            "Flags in dataset not found in FLAGS_RANKING. Manually add those flags."
        )


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

    # Find latest meadow version of dataset of FAOSTAT metadata.
    metadata_version = sorted((DATA_DIR / "meadow" / NAMESPACE).glob(f"*/{DATASET_SHORT_NAME}"))[-1].parent.name
    metadata_path = DATA_DIR / "meadow" / NAMESPACE / metadata_version / DATASET_SHORT_NAME

    ####################################################################################################################
    # Load and process data.
    ####################################################################################################################

    # Get metadata from meadow.
    assert metadata_path.is_dir()
    metadata = catalog.Dataset(metadata_path)

    # Check if flags definitions need to be updated.
    check_that_flag_definitions_in_dataset_agree_with_those_in_flags_ranking(metadata)

    # Load custom dataset names, items, and element-unit names.
    custom_datasets = pd.read_csv(custom_datasets_file, dtype=str)
    custom_elements = pd.read_csv(custom_elements_and_units_file, dtype=str)
    custom_items = pd.read_csv(custom_items_file, dtype=str)

    # List all FAOSTAT dataset short names.
    dataset_short_names = sorted(set([NAMESPACE + "_" + table_name.split("_")[1]
                                      for table_name in metadata.table_names]))

    # Initialise dataframe of dataset descriptions, items, and element-units.
    # We cannot remove "dataset" from the items and elements dataframes, because it can happen that, for a given
    # item code, the item name is slightly different in two different datasets.
    datasets_df = pd.DataFrame({"dataset": [], "fao_dataset_title": [], "fao_dataset_description": []})
    items_df = pd.DataFrame({"dataset": [], "item_code": [], "fao_item": [], "fao_item_description": []})
    elements_df = pd.DataFrame({"dataset": [], "element_code": [], "fao_element": [], "fao_element_description": [],
                                "fao_unit": [], "fao_unit_short_name": []})

    # Gather all variables from the latest version of each meadow dataset.
    for dataset_short_name in tqdm(dataset_short_names):
        # Load latest meadow table for current dataset.
        table = load_latest_data_table_for_dataset(dataset_short_name=dataset_short_name)

        check_that_all_flags_in_dataset_are_in_ranking(
            table=table, metadata_for_flags=metadata[f"{dataset_short_name}_flag"])

        # Gather dataset descriptions, items, and element-units for current domain.
        datasets_from_data = create_dataset_descriptions_dataframe_for_domain(
            table, dataset_short_name=dataset_short_name)

        items_from_data = create_items_dataframe_for_domain(
            table=table, metadata=metadata, dataset_short_name=dataset_short_name)

        elements_from_data = create_elements_dataframe_for_domain(
            table=table, metadata=metadata, dataset_short_name=dataset_short_name)

        # Add dataset descriptions, items, and element-units from current dataset to global dataframes.
        datasets_df = dataframes.concatenate([datasets_df, datasets_from_data], ignore_index=True)
        items_df = dataframes.concatenate([items_df, items_from_data], ignore_index=True)
        elements_df = dataframes.concatenate([elements_df, elements_from_data], ignore_index=True)

    datasets_df = clean_global_dataset_descriptions_dataframe(
        datasets_df=datasets_df, custom_datasets=custom_datasets)
    items_df = clean_global_items_dataframe(items_df=items_df, custom_items=custom_items)
    elements_df = clean_global_elements_dataframe(
        elements_df=elements_df, custom_elements=custom_elements)

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

    # Create new garden dataset with all dataset descriptions, items, and element-units.
    datasets_table = create_table(df=datasets_df, short_name="datasets", index_cols=["dataset"])
    items_table = create_table(df=items_df, short_name="items", index_cols=["item_code"])
    elements_table = create_table(df=elements_df, short_name="elements", index_cols=["element_code"])

    # Add tables to dataset (no need to repack, since columns already have optimal dtypes).
    dataset_garden.add(datasets_table, repack=False)
    dataset_garden.add(items_table, repack=False)
    dataset_garden.add(elements_table, repack=False)
