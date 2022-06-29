"""FAOSTAT Production: Crops and livestock products.

"""

from copy import deepcopy

import pandas as pd
from owid import catalog
from owid.datautils import dataframes

from etl.paths import DATA_DIR
from .shared import NAMESPACE, VERSION, ADDED_TITLE_TO_WIDE_TABLE, FLAG_MULTIPLE_FLAGS, harmonize_elements,\
    harmonize_items, clean_data, add_regions, add_per_capita_variables, prepare_long_table, prepare_wide_table


# FAO item name, element name, and unit name for population.
FAO_POPULATION_ITEM_NAME = "Population"
FAO_POPULATION_ELEMENT_NAME = "Total Population - Both sexes"
FAO_POPULATION_UNIT = "1000 persons"
# OWID item name for total meat.
TOTAL_MEAT_ITEM = "Meat, total"
# OWID element name, unit name, and unit short name for number of slaughtered animals.
SLAUGHTERED_ANIMALS_ELEMENT = "Producing or slaughtered animals"
SLAUGHTERED_ANIMALS_UNIT = "animals"
SLAUGHTERED_ANIMALS_UNIT_SHORT_NAME = "animals"
SLAUGHTERED_ANIMALS_PER_CAPITA_ELEMENT = "Producing or slaughtered animals (animals per capita)"


def add_slaughtered_animals_to_meat_total(data):
    # There is no FAO data on slaughtered animals for total meat.
    # We construct this data by aggregating that element for the following items (which corresponds to all meat
    # items removing redundancies):
    items_to_aggregate = [
        'Meat, ass',
        'Meat, beef and buffalo',
        'Meat, camel',
        'Meat, horse',
        'Meat, lamb and mutton',
        'Meat, mule',
        'Meat, pig',
        'Meat, poultry',
        'Meat, rabbit',
        'Meat, sheep and goat',
    ]
    error = f"Some items required to get the aggregate '{TOTAL_MEAT_ITEM}' are missing in data."
    assert set(items_to_aggregate) < set(data["item"]), error
    assert SLAUGHTERED_ANIMALS_ELEMENT in data["element"].unique()
    assert SLAUGHTERED_ANIMALS_UNIT in data["unit"].unique()

    # For some reason, there are two element codes for the same element (they have different items assigned).
    error = "Element codes for 'Producing or slaughtered animals' may have changed."
    assert data[(data["element"] == SLAUGHTERED_ANIMALS_ELEMENT) &
                ~(data["element_code"].str.contains("pc"))]["element_code"].unique().tolist() == \
           ['005320', '005321'], error    

    # Similarly, there are two items for meat total.
    error = f"Item codes for '{TOTAL_MEAT_ITEM}' may have changed."
    assert data[data["item"] == TOTAL_MEAT_ITEM]["item_code"].unique().tolist() == ['00001765'], error    

    # We arbitrarily choose the first element code and the first item code.
    slaughtered_animals_element_code = "005320"
    total_meat_item_code = "00001765"

    # Check that, indeed, this variable is not given in the original data.
    assert data[(data["item"] == TOTAL_MEAT_ITEM) &
                (data["element"] == SLAUGHTERED_ANIMALS_ELEMENT) &
                (data["unit"] == SLAUGHTERED_ANIMALS_UNIT)].empty

    # Select the subset of data to aggregate.
    data_to_aggregate = data[(data["element"] == SLAUGHTERED_ANIMALS_ELEMENT) &
                             (data["unit"] == SLAUGHTERED_ANIMALS_UNIT) &
                             (data["item"].isin(items_to_aggregate))].\
        dropna(subset="value").reset_index(drop=True)

    # Create a dataframe with the total number of animals used for meat.
    animals = dataframes.groupby_agg(data_to_aggregate, groupby_columns=[
        "area_code", "fao_country", "fao_element", "country", "year", "population_with_data"], aggregations={
        "value": "sum", "flag": lambda x: x if len(x) == 1 else FLAG_MULTIPLE_FLAGS}).reset_index()

    # Get element description for selected element code.
    slaughtered_animals_element_description = data[
        data["element_code"] == slaughtered_animals_element_code]["element_description"].unique()
    assert len(slaughtered_animals_element_description) == 1
    slaughtered_animals_element_description = slaughtered_animals_element_description[0]

    # Get item description for selected item code.
    total_meat_item_description = data[data["item_code"] == total_meat_item_code]["item_description"].unique()
    assert len(total_meat_item_description) == 1
    total_meat_item_description = total_meat_item_description[0]
    
    # Get FAO item name for selected item code.
    total_meat_fao_item = data[data["item_code"] == total_meat_item_code]["fao_item"].unique()
    assert len(total_meat_fao_item) == 1
    total_meat_fao_item = total_meat_fao_item[0]

    # Get FAO unit for selected item code.
    total_meat_fao_unit = data[data["item_code"] == total_meat_item_code]["fao_unit"].unique()
    assert len(total_meat_fao_unit) == 1
    total_meat_fao_unit = total_meat_fao_unit[0]

    # Manually include the rest of columns.
    animals["element"] = SLAUGHTERED_ANIMALS_ELEMENT
    animals["element_description"] = slaughtered_animals_element_description
    animals["unit"] = SLAUGHTERED_ANIMALS_UNIT
    animals["unit_short_name"] = SLAUGHTERED_ANIMALS_UNIT_SHORT_NAME
    animals["element_code"] = slaughtered_animals_element_code
    animals["item_code"] = total_meat_item_code
    animals["item"] = TOTAL_MEAT_ITEM
    animals["item_description"] = total_meat_item_description
    animals["fao_item"] = total_meat_fao_item
    animals["fao_unit"] = total_meat_fao_unit

    # Check that we are not missing any column.
    assert set(data.columns) == set(animals.columns)

    # Add animals data to the original dataframe.
    combined_data = pd.concat([data, animals], ignore_index=True).reset_index(drop=True).\
        astype({"element_code": "category", "item_code": "category", "fao_item": "category",
                "fao_unit": "category", "flag": "category", "item": "category",
                "item_description": "category", "element": "category", "unit": "category",
                "element_description": "category", "unit_short_name": "category"})

    return combined_data


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Dataset short name.
    dataset_short_name = f"{NAMESPACE}_qcl"
    # Path to latest dataset in meadow for current FAOSTAT domain.
    meadow_data_dir = sorted((DATA_DIR / "meadow" / NAMESPACE).glob(f"*/{dataset_short_name}"))[-1].parent /\
        dataset_short_name
    # Path to dataset of FAOSTAT metadata.
    garden_metadata_dir = DATA_DIR / "garden" / NAMESPACE / VERSION / f"{NAMESPACE}_metadata"

    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Load meadow dataset and keep its metadata.
    dataset_meadow = catalog.Dataset(meadow_data_dir)
    # Load main table from dataset.
    data_table_meadow = dataset_meadow[dataset_short_name]
    data = pd.DataFrame(data_table_meadow).reset_index()

    # Load dataset of FAOSTAT metadata.
    metadata = catalog.Dataset(garden_metadata_dir)

    # Load and prepare dataset, items, element-units, and countries metadata.
    datasets_metadata = pd.DataFrame(metadata["datasets"]).reset_index()
    datasets_metadata = datasets_metadata[datasets_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    items_metadata = pd.DataFrame(metadata["items"]).reset_index()
    items_metadata = items_metadata[items_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    elements_metadata = pd.DataFrame(metadata["elements"]).reset_index()
    elements_metadata = elements_metadata[elements_metadata["dataset"] == dataset_short_name].reset_index(drop=True)
    countries_metadata = pd.DataFrame(metadata["countries"]).reset_index()

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    # Harmonize items and elements, and clean data.
    data = harmonize_items(df=data, dataset_short_name=dataset_short_name)
    data = harmonize_elements(df=data)

    # Prepare data.
    data = clean_data(data=data, items_metadata=items_metadata, elements_metadata=elements_metadata,
                      countries_metadata=countries_metadata)

    # Include number of slaughtered animals in total meat (which is missing).
    data = add_slaughtered_animals_to_meat_total(data=data)

    # Add data for aggregate regions.
    data = add_regions(data=data, elements_metadata=elements_metadata)

    # Add per-capita variables.
    data = add_per_capita_variables(data=data, elements_metadata=elements_metadata)

    # TODO: Add yield (production / area) variable.

    # TODO: Run more sanity checks (i.e. compare with previous version of the same domain).

    # Create a long table (with item code and element code as part of the index).
    data_table_long = prepare_long_table(data=data)

    # Create a wide table (with only country and year as index).
    data_table_wide = prepare_wide_table(data=data, dataset_title=datasets_metadata["owid_dataset_title"].item())

    ####################################################################################################################
    # Save outputs.
    ####################################################################################################################

    # Initialize new garden dataset.
    dataset_garden = catalog.Dataset.create_empty(dest_dir)
    # Prepare metadata for new garden dataset (starting with the metadata from the meadow version).
    dataset_garden_metadata = deepcopy(dataset_meadow.metadata)
    dataset_garden_metadata.version = VERSION
    dataset_garden_metadata.description = datasets_metadata["owid_dataset_description"].item()
    dataset_garden_metadata.title = datasets_metadata["owid_dataset_title"].item()
    # Add metadata to dataset.
    dataset_garden.metadata = dataset_garden_metadata
    # Create new dataset in garden.
    dataset_garden.save()

    # Prepare metadata for new garden long table (starting with the metadata from the meadow version).
    data_table_long.metadata = deepcopy(data_table_meadow.metadata)
    data_table_long.metadata.title = dataset_garden_metadata.title
    data_table_long.metadata.description = dataset_garden_metadata.description
    data_table_long.metadata.primary_key = list(data_table_long.index.names)
    data_table_long.metadata.dataset = dataset_garden_metadata
    # Add long table to the dataset (no need to repack, since columns already have optimal dtypes).
    dataset_garden.add(data_table_long, repack=False)

    # Prepare metadata for new garden wide table (starting with the metadata from the long table).
    # Add wide table to the dataset.
    data_table_wide.metadata = deepcopy(data_table_long.metadata)

    data_table_wide.metadata.title += ADDED_TITLE_TO_WIDE_TABLE
    data_table_wide.metadata.short_name += "_flat"
    data_table_wide.metadata.primary_key = list(data_table_wide.index.names)

    # Add wide table to the dataset (no need to repack, since columns already have optimal dtypes).
    dataset_garden.add(data_table_wide, repack=False)
