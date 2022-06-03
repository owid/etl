"""FAOSTAT food explorer.

Load the qcl and fbsc (combination of fbsh and fbs) datasets, and create a combined dataset of food products.

"""

from copy import deepcopy

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta
from owid.datautils import geo

from etl.paths import DATA_DIR
from .shared import NAMESPACE, harmonize_elements, harmonize_items

# Dataset name and title.
DATASET_TITLE = "Food Explorer"
DATASET_SHORT_NAME = f"{NAMESPACE}_food_explorer"


def combine_qcl_and_fbsc(
    qcl_table: catalog.Table, fbsc_table: catalog.Table
) -> pd.DataFrame:
    qcl = pd.DataFrame(qcl_table).reset_index()
    qcl["value"] = qcl["value"].astype(float)
    qcl["element"] = [element for element in qcl["element"]]
    qcl["unit"] = [unit for unit in qcl["unit"]]
    qcl["item"] = [item for item in qcl["item"]]
    fbsc = pd.DataFrame(fbsc_table).reset_index()
    fbsc["value"] = fbsc["value"].astype(float)
    fbsc["element"] = [element for element in fbsc["element"]]
    fbsc["unit"] = [unit for unit in fbsc["unit"]]
    fbsc["item"] = [item for item in fbsc["item"]]

    ##########################################
    # TODO: Remove these lines once item_code and element_code are stored with the right format.
    qcl = harmonize_items(qcl, dataset_short_name=f"{NAMESPACE}_qcl", item_col="fao_item")
    qcl = harmonize_elements(qcl, element_col="fao_element")
    fbsc = harmonize_items(fbsc, dataset_short_name=f"{NAMESPACE}_fbsc", item_col="fao_item")
    fbsc = harmonize_elements(fbsc, element_col="fao_element")
    ##########################################

    # TODO: Before combining, select the products we need.

    columns = ['country', 'year', 'item_code', 'element_code', 'item', 'element', 'unit', 'unit_short_name', 'value',
               'unit_factor']
    rename_columns = {"item": "product"}
    combined = (
        pd.concat([qcl[columns], fbsc[columns]], ignore_index=True)
        .rename(columns=rename_columns)
        .reset_index(drop=True)
    )
    # Sanity checks.
    assert len(combined) == (
        len(qcl) + len(fbsc)
    ), "Unexpected number of rows after combining qcl and fbsc datasets."
    assert len(combined[combined["value"].isnull()]) == 0, "Unexpected nan values."

    n_items_per_item_code = combined.groupby("item_code")["product"].transform("nunique")
    assert combined[n_items_per_item_code > 1].empty, "There are item codes with multiple items."

    n_elements_per_element_code = combined.groupby("element_code")["element"].transform("nunique")
    assert combined[n_elements_per_element_code > 1].empty, "There are element codes with multiple elements."

    n_units_per_element_code = combined.groupby("element_code")["unit"].transform("nunique")
    assert combined[n_units_per_element_code > 1].empty, "There are element codes with multiple units."

    error = "There are unexpected duplicate rows."
    assert combined[combined.duplicated(subset=["product", "country", "year", "element", "unit"])].empty, error

    return combined


def process_combined_data(data: pd.DataFrame) -> pd.DataFrame:
    # For the food explorer we have to multiply data by the unit factor, since these conversions
    # will not be applied in grapher.
    rows_to_convert_mask = data["unit_factor"].notnull()
    data.loc[rows_to_convert_mask, "value"] = data[rows_to_convert_mask]["value"] * \
        data[rows_to_convert_mask]["unit_factor"]

    # Join element and unit into one title column.
    data["title"] = data["element"] + "-" + data["unit"]

    # This will create a table with just one column and country-year as index.
    index_columns = ["product", "country", "year"]
    data_wide = data.pivot(
        index=index_columns, columns=["title"], values="value"
    ).reset_index()

    # Add column for population.
    data_wide = geo.add_population_to_dataframe(df=data_wide, warn_on_missing_countries=False)

    assert (
        len(data_wide.columns[data_wide.isnull().all(axis=0)]) == 0
    ), "Unexpected columns with only nan values."

    # Set a reasonable index.
    data_wide = data_wide.set_index(index_columns)

    return data_wide


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Path to latest qcl and fbsc datasets in garden.
    qcl_latest_dir = sorted((DATA_DIR / "garden" / NAMESPACE).glob(f"*/{NAMESPACE}_qcl*"))[-1]
    fbsc_latest_dir = sorted((DATA_DIR / "garden" / NAMESPACE).glob(f"*/{NAMESPACE}_fbsc*"))[-1]

    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Load qcl dataset and keep its metadata.
    qcl_dataset = catalog.Dataset(qcl_latest_dir)
    fbsc_dataset = catalog.Dataset(fbsc_latest_dir)

    # Get qcl long table inside qcl dataset.
    qcl_table = qcl_dataset[f"{NAMESPACE}_qcl"]
    # Idem for fbsc.
    fbsc_table = fbsc_dataset[f"{NAMESPACE}_fbsc"]

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    data = combine_qcl_and_fbsc(qcl_table=qcl_table, fbsc_table=fbsc_table)

    data = process_combined_data(data=data)

    # TODO: Add per capita variables. It seems in the old explorer we multiply per capita variables by *our*
    #  population, and then divide again. It makes more sense to multiply by FAOSTAT population and then divide by
    #  our population. Or maybe we could just drop those per capita variables, if we have the analogous
    #  non-per-capita variables.

    ####################################################################################################################
    # Save outputs.
    ####################################################################################################################

    # Initialize new garden dataset.
    explorer_dataset = catalog.Dataset.create_empty(dest_dir)
    # Define metadata for new garden dataset (by default, take metadata from fbsc dataset).
    explorer_sources = deepcopy(fbsc_dataset.metadata.sources[0])
    explorer_sources.source_data_url = None
    explorer_sources.owid_data_url = None
    explorer_dataset.metadata = DatasetMeta(
        namespace=NAMESPACE,
        short_name=DATASET_SHORT_NAME,
        title=DATASET_TITLE,
        # TODO: Add custom description and combine sources.
        description=fbsc_dataset.metadata.description,
        sources=fbsc_dataset.metadata.sources + qcl_dataset.metadata.sources,
        licenses=fbsc_dataset.metadata.licenses + qcl_dataset.metadata.licenses,
    )
    # Create new dataset in garden.
    explorer_dataset.save()
    # Create table of products.
    # TODO: Decide if storing one big table, or one small table per product.
    table = catalog.Table(data)
    # Make all column names snake_case.
    table = catalog.utils.underscore_table(table)
    # Add metadata for the table.
    table.metadata.short_name = "all_products"
    table.metadata.primary_key = list(table.index)
    # Add table to dataset.
    explorer_dataset.add(table)
