"""FAOSTAT food explorer.

Load the qcl and fbsc (combination of fbsh and fbs) datasets, and create a combined dataset of food products.

"""

from copy import deepcopy

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta
from owid.datautils import dataframes, geo

from etl.paths import DATA_DIR, STEP_DIR
from .shared import NAMESPACE, VERSION

# Dataset name and title.
DATASET_TITLE = "Food Explorer"
DATASET_SHORT_NAME = f"{NAMESPACE}_food_explorer"
DATASET_DESCRIPTION = "This dataset has been created by Our World in Data, merging existing FAOstat datsets. In " \
                      "particular, we have used 'Crops and livestock products' (QCL) and 'Food Balances' (FBSH and " \
                      "FBS) datasets. Each row contains all the metrics for a specific combination of (country, " \
                      "product, year). The metrics may come from different datasets."

# List of columns from combined (qcl + fbsc) dataframe that should have a per capita variable.
# Note: Some of the columns may already be given as "per capita" variables in the original data.
# In those cases we will divide by FAO population and multiply by OWID population, for consistency.
PER_CAPITA_COLUMNS = [
    'Area harvested (ha)',
    'Domestic supply (tonnes)',
    'Exports (tonnes)',
    'Feed (tonnes)',
    'Food (tonnes)',
    'Food available for consumption (grams of fat per day per capita)',
    'Food available for consumption (grams of protein per day per capita)',
    'Food available for consumption (kilocalories per day per capita)',
    'Food available for consumption (kilograms per year per capita)',
    'Imports (tonnes)',
    'Other uses (tonnes)',
    'Producing or slaughtered animals (animals)',
    'Production (tonnes)',
    'Waste in Supply Chain (tonnes)',
]


def combine_qcl_and_fbsc(
    qcl_table: catalog.Table, fbsc_table: catalog.Table
) -> pd.DataFrame:
    columns = ['country', 'year', 'item_code', 'element_code', 'item', 'element', 'unit', 'unit_short_name', 'value',
               'unit_factor', 'population_with_data']
    qcl = pd.DataFrame(qcl_table).reset_index()[columns]
    qcl["value"] = qcl["value"].astype(float)
    qcl["element"] = [element for element in qcl["element"]]
    qcl["unit"] = [unit for unit in qcl["unit"]]
    qcl["item"] = [item for item in qcl["item"]]
    fbsc = pd.DataFrame(fbsc_table).reset_index()[columns]
    fbsc["value"] = fbsc["value"].astype(float)
    fbsc["element"] = [element for element in fbsc["element"]]
    fbsc["unit"] = [unit for unit in fbsc["unit"]]
    fbsc["item"] = [item for item in fbsc["item"]]

    rename_columns = {"item": "product"}
    combined = (
        dataframes.concatenate([qcl, fbsc], ignore_index=True)
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

    error = "There are unexpected duplicate rows. Rename items in custom_items.csv to avoid clashes."
    assert combined[combined.duplicated(subset=["product", "country", "year", "element", "unit"])].empty, error

    return combined


def get_fao_population(combined: pd.DataFrame) -> pd.DataFrame:
    fao_population_item_name = "Population"
    fao_population_element_name = "Total Population - Both sexes"

    fao_population = combined[(combined["product"] == fao_population_item_name) &
                              (combined["element"] == fao_population_element_name)].reset_index(drop=True)

    # Check that population is given in "1000 persons" and convert to persons.
    error = "FAOSTAT population changed item, element, or unit."
    assert fao_population["unit"].unique().tolist() == ["1000 persons"], error
    fao_population["value"] *= 1000

    fao_population = fao_population[["country", "year", "value"]].dropna(how="any").\
        rename(columns={"value": "fao_population"})

    return fao_population


def process_combined_data(combined: pd.DataFrame, custom_products: pd.DataFrame) -> pd.DataFrame:
    combined = combined.copy()

    # Get FAO population from data (it is given as another item).
    fao_population = get_fao_population(combined=combined)

    # Create a mapping from product name in data to product name in explorer (for those products that need renaming).
    products_renaming = custom_products[custom_products["product_in_explorer"].notnull()].\
        set_index("product_in_data")["product_in_explorer"].to_dict()

    # Rename products.
    combined["product"] = dataframes.map_series(combined["product"], mapping=products_renaming,
                                                warn_on_unused_mappings=True)

    # Get list of products that will be used in food explorer.
    products = sorted(custom_products["product_in_explorer"].fillna(custom_products["product_in_data"]).
                      unique().tolist())

    # Check that all expected products are included in the data.
    missing_products = sorted(set(products) - set(set(combined["product"])))
    assert len(missing_products) == 0, f"{len(missing_products)} missing products for food explorer."

    # Select relevant products for the food explorer.
    combined = combined[combined["product"].isin(products)].reset_index(drop=True)

    # For the food explorer we have to multiply data by the unit factor, since these conversions
    # will not be applied in grapher.
    rows_to_convert_mask = combined["unit_factor"].notnull()
    combined.loc[rows_to_convert_mask, "value"] = combined[rows_to_convert_mask]["value"] * \
        combined[rows_to_convert_mask]["unit_factor"]

    # Join element and unit into one title column.
    combined["title"] = combined["element"] + " (" + combined["unit"] + ")"

    # This will create a table with just one column and country-year as index.
    index_columns = ["product", "country", "year"]
    data_wide = combined.pivot(
        index=index_columns, columns=["title"], values="value"
    ).reset_index()

    # Add column for FAO population.
    data_wide = pd.merge(data_wide, fao_population, on=["country", "year"], how="left")

    # Add column for OWID population.
    data_wide = geo.add_population_to_dataframe(df=data_wide, warn_on_missing_countries=False)
    # Add population of countries for which we have data.
    population_with_data = combined.pivot(index=index_columns, columns=["title"],
                                          values="population_with_data").reset_index()

    # TODO: Check that population and population with data are exactly the same for non-regions.
    # TODO: Warn on regions where population with data is much smaller than total population.

    # Add per capita variables.
    for column in PER_CAPITA_COLUMNS:
        if "per capita" in column.lower():
            # Some variables are already given per capita in the FAOSTAT dataset.
            # But, since their population may differ with ours, for consistency, we multiply their per capita variables
            # by their population, to obtain the total variable, and then we divide by our own population.
            data_wide[column] = data_wide[column] * data_wide["fao_population"] / population_with_data[column]
        else:
            # Create a new column with the same name, but adding "per capita" to the unit.
            data_wide[column[:-1] + " per capita)"] = data_wide[column] / population_with_data[column]

    assert (
        len(data_wide.columns[data_wide.isnull().all(axis=0)]) == 0
    ), "Unexpected columns with only nan values."

    # Set a reasonable index.
    data_wide = data_wide.set_index(index_columns, verify_integrity=True)

    return data_wide


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Path to latest qcl and fbsc datasets in garden.
    qcl_latest_dir = sorted((DATA_DIR / "garden" / NAMESPACE).glob(f"*/{NAMESPACE}_qcl*"))[-1]
    fbsc_latest_dir = sorted((DATA_DIR / "garden" / NAMESPACE).glob(f"*/{NAMESPACE}_fbsc*"))[-1]
    # Path to file with custom product names for the food explorer.
    custom_products_file = STEP_DIR / "data" / "garden" / NAMESPACE / VERSION / "custom_food_explorer_products.csv"

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

    # Load names of products used in food explorer.
    custom_products = pd.read_csv(custom_products_file, dtype=str)

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    combined = combine_qcl_and_fbsc(qcl_table=qcl_table, fbsc_table=fbsc_table)
    data = process_combined_data(combined=combined, custom_products=custom_products)

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
        description=DATASET_DESCRIPTION,
        sources=fbsc_dataset.metadata.sources + qcl_dataset.metadata.sources,
        licenses=fbsc_dataset.metadata.licenses + qcl_dataset.metadata.licenses,
        version=VERSION,
    )
    # Create new dataset in garden.
    explorer_dataset.save()
    # Create table of products.
    table = catalog.Table(data)
    # Make all column names snake_case.
    table = catalog.utils.underscore_table(table)
    # Add metadata for the table.
    table.metadata.short_name = "all_products"
    table.metadata.primary_key = list(table.index)
    # Add table to dataset.
    explorer_dataset.add(table)
