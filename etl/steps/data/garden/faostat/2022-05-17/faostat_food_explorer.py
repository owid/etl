"""FAOSTAT food explorer.

Load the qcl and fbsc (combination of fbsh and fbs) datasets, and create a combined dataset of food products.

"""

from copy import deepcopy

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta
from owid.datautils import dataframes, geo

from etl.paths import DATA_DIR
from .shared import NAMESPACE, VERSION

# Dataset name and title.
DATASET_TITLE = "Food Explorer"
DATASET_SHORT_NAME = f"{NAMESPACE}_food_explorer"
DATASET_DESCRIPTION = "This dataset has been created by Our World in Data, merging existing FAOSTAT datasets. In " \
                      "particular, we have used 'Crops and livestock products' (QCL) and 'Food Balances' (FBSH and " \
                      "FBS) datasets. Each row contains all the metrics for a specific combination of (country, " \
                      "product, year). The metrics may come from different datasets."

# List of items (OWID names) to include in the global food explorer.
# Note: The names of the products will be further edited in owid-content, following to the following file:
# https://github.com/owid/owid-content/blob/master/scripts/global-food-explorer/foods.csv
PRODUCTS = [
    'Almonds',
    'Animal fats',
    'Apples',
    'Apricots',
    'Areca nuts',
    'Artichokes',
    'Asparagus',
    'Avocados',
    'Bananas',
    'Barley',
    'Beans, dry',
    'Beeswax',
    'Blueberries',
    'Brazil nuts, with shell',
    'Broad beans',
    'Buckwheat',
    'Buffalo hides',
    'Butter and ghee',
    'Cabbages',
    'Canary seed',
    'Carrots and turnips',
    'Cashew nuts',
    'Cassava',
    'Castor oil seed',
    'Cattle hides',
    'Cauliflowers and broccoli',
    'Cereals',
    'Cheese',
    'Cherries',
    'Chestnut',
    'Chickpeas',
    'Chillies and peppers',
    'Citrus Fruit',
    'Cocoa beans',
    'Coconut oil',
    'Coconuts',
    'Coffee, green',
    'Cotton',
    'Cottonseed',
    'Cottonseed oil',
    'Cow peas',
    'Cranberries',
    'Cucumbers and gherkins',
    'Currants',
    'Dates',
    'Eggplants',
    'Eggs',
    'Eggs from hens',
    'Eggs from other birds (excl. hens)',
    'Fat, buffaloes',
    'Fat, camels',
    'Fat, cattle',
    'Fat, goats',
    'Fat, pigs',
    'Fat, sheep',
    'Fibre crops',
    'Fish and seafood',
    'Flax fibre',
    'Fruit',
    'Garlic',
    'Grapefruit',
    'Grapes',
    'Beans, green',
    'Green maize',
    'Groundnut oil',
    'Groundnuts',
    'Hazelnuts',
    'Hempseed',
    'Herbs (e.g. fennel)',
    'Honey',
    'Jute',
    'Karite nuts',
    'Kiwi',
    'Kola nuts',
    'Leeks',
    'Lemons and limes',
    'Lentils',
    'Lettuce',
    'Linseed',
    'Linseed oil',
    'Maize',
    'Maize oil',
    'Mangoes',
    'Margarine',
    'Meat, total',
    'Meat, ass',
    'Meat, beef',
    'Meat, beef and buffalo',
    'Meat, buffalo',
    'Meat, camel',
    'Meat, chicken',
    'Meat, duck',
    'Meat, game',
    'Meat, goat',
    'Meat, goose and guinea fowl',
    'Meat, horse',
    'Meat, lamb and mutton',
    'Meat, mule',
    'Meat, pig',
    'Meat, poultry',
    'Meat, rabbit',
    'Meat, sheep and goat',
    'Meat, turkey',
    'Melon',
    'Melonseed',
    'Milk',
    'Millet',
    'Mixed grains',
    'Molasses',
    'Mushrooms',
    'Mustard seed',
    'Nuts',
    'Oats',
    'Offals',
    'Offals, buffaloes',
    'Offals, camels',
    'Offals, cattle',
    'Offals, goats',
    'Offals, horses',
    'Offals, pigs',
    'Offals, sheep',
    'Oilcrops',
    'Oilcrops, Cake Equivalent',
    'Oilcrops, Oil Equivalent',
    'Okra',
    'Olive oil',
    'Olives',
    'Onions',
    'Oranges',
    'Palm fruit oil',
    'Palm kernel oil',
    'Palm kernels',
    'Palm oil',
    'Papayas',
    'Peaches and nectarines',
    'Pears',
    'Peas, dry',
    'Peas, green',
    'Pepper',
    'Pigeon peas',
    'Pineapples',
    'Pistachios',
    'Plantains',
    'Plums',
    'Poppy seeds',
    'Pork',
    'Potatoes',
    'Pulses',
    'Quinoa',
    'Rapeseed',
    'Rapeseed oil',
    'Raspberries',
    'Rice',
    'Roots and tubers',
    'Rye',
    'Safflower oil',
    'Safflower seed',
    'Seed cotton',
    'Sesame oil',
    'Sesame seed',
    'Silk',
    'Skins, goat',
    'Skins, sheep',
    'Sorghum',
    'Soybean oil',
    'Soybeans',
    'Spinach',
    'Strawberries',
    'String beans',
    'Sugar (raw)',
    'Sugar beet',
    'Sugar cane',
    'Sugar crops',
    'Sunflower oil',
    'Sunflower seed',
    'Sweet potatoes',
    'Tangerines',
    'Tea',
    'Tobacco',
    'Tomatoes',
    'Total',
    'Treenuts',
    'Vegetables',
    'Walnuts',
    'Watermelons',
    'Wheat',
    'Whey',
    'Wine',
    'Wool',
    'Yams',
]
# OWID item name, element name, and unit name for population (as given in faostat_qcl and faostat_fbsc datasets).
FAO_POPULATION_ITEM_NAME = "Population"
FAO_POPULATION_ELEMENT_NAME = "Total Population - Both sexes"
FAO_POPULATION_UNIT = "1000 persons"
# OWID item name for total meat  (as given in faostat_qcl and faostat_fbsc datasets).
TOTAL_MEAT_ITEM = "Meat, total"
# OWID element name, unit name, and unit short name for number of slaughtered animals  (as given in faostat_qcl and
# faostat_fbsc datasets).
SLAUGHTERED_ANIMALS_ELEMENT = "Producing or slaughtered animals"
SLAUGHTERED_ANIMALS_UNIT = "animals"
SLAUGHTERED_ANIMALS_UNIT_SHORT_NAME = "animals"
SLAUGHTERED_ANIMALS_PER_CAPITA_ELEMENT = "Producing or slaughtered animals (animals per capita)"

slaughtered_animals_element = "Producing or slaughtered animals (animals)"


def combine_qcl_and_fbsc(
    qcl_table: catalog.Table, fbsc_table: catalog.Table
) -> pd.DataFrame:

    columns = ['country', 'year', 'item_code', 'element_code', 'item', 'element', 'unit', 'unit_short_name', 'value',
               'population_with_data']
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
    assert len(combined) == (len(qcl) + len(fbsc)), "Unexpected number of rows after combining qcl and fbsc datasets."

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
    fao_population = combined[(combined["product"] == FAO_POPULATION_ITEM_NAME) &
                              (combined["element"] == FAO_POPULATION_ELEMENT_NAME)].reset_index(drop=True)

    # Check that population is given in "1000 persons" and convert to persons.
    error = "FAOSTAT population changed item, element, or unit."
    assert fao_population["unit"].unique().tolist() == [FAO_POPULATION_UNIT], error
    fao_population["value"] *= 1000

    fao_population = fao_population[["country", "year", "value"]].dropna(how="any").\
        rename(columns={"value": "fao_population"})

    return fao_population


def add_slaughtered_animals_to_meat_total(combined):
    # There is no FAO data on slaughtered animals for total meat.
    # We construct this data by aggregating that element for the following items (which corresponds to all meat
    # products removing redundancies):
    products_to_aggregate = [
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
    assert set(products_to_aggregate) < set(combined["product"]), error
    assert SLAUGHTERED_ANIMALS_ELEMENT in combined["element"].unique()
    assert SLAUGHTERED_ANIMALS_UNIT in combined["unit"].unique()

    # For some reason, there are two element codes for the same element (they have different items assigned).
    error = "Element codes for 'Producing or slaughtered animals' may have changed."
    assert combined[(combined["element"] == SLAUGHTERED_ANIMALS_ELEMENT) &
                    ~(combined["element_code"].str.contains("pc"))]["element_code"].unique().tolist() == \
           ['005320', '005321'], error
    # Similarly, there are two items for meat total.
    error = f"Item codes for '{TOTAL_MEAT_ITEM}' may have changed."
    assert combined[combined["product"] == TOTAL_MEAT_ITEM]["item_code"].unique().tolist() == \
           ['00001765', '00002943'], error
    # We arbitrarily choose the first element code and the first item code.
    slaughtered_animals_element_code = "005320"
    total_meat_item_code = "00001765"

    # Check that, indeed, this variable is not given in the original data.
    assert combined[(combined["product"] == TOTAL_MEAT_ITEM) &
                    (combined["element"] == SLAUGHTERED_ANIMALS_ELEMENT) &
                    (combined["unit"] == SLAUGHTERED_ANIMALS_UNIT)].empty

    # Select the subset of data to aggregate.
    data_to_aggregate = combined[(combined["element"] == SLAUGHTERED_ANIMALS_ELEMENT) &
                                 (combined["unit"] == SLAUGHTERED_ANIMALS_UNIT) &
                                 (combined["product"].isin(products_to_aggregate))].\
        dropna(subset="value").reset_index(drop=True)

    # Create a dataframe with the total number of animals used for meat.
    animals = dataframes.groupby_agg(data_to_aggregate, groupby_columns=["country", "year"],
                                     aggregations={"value": "sum"}).reset_index()

    # Manually include the rest of columns.
    animals["element"] = SLAUGHTERED_ANIMALS_ELEMENT
    animals["unit"] = SLAUGHTERED_ANIMALS_UNIT
    animals["unit_short_name"] = SLAUGHTERED_ANIMALS_UNIT_SHORT_NAME
    animals["element_code"] = slaughtered_animals_element_code
    animals["item_code"] = total_meat_item_code
    animals["product"] = TOTAL_MEAT_ITEM

    # Add animals data to the original dataframe.
    combined_data = pd.concat([combined, animals], ignore_index=True).reset_index(drop=True)

    return combined_data


def add_slaughtered_animals_per_capita_to_meat_total(data_wide):
    # Check that there is no data for slaughtered animals per capita in the total meat item.
    assert data_wide[data_wide["product"] == TOTAL_MEAT_ITEM][SLAUGHTERED_ANIMALS_PER_CAPITA_ELEMENT].dropna().empty

    # Add per capita slaughtered animals.

    total_meat_item_mask = data_wide["product"] == TOTAL_MEAT_ITEM

    data_wide.loc[total_meat_item_mask, SLAUGHTERED_ANIMALS_PER_CAPITA_ELEMENT] =\
        data_wide[total_meat_item_mask][slaughtered_animals_element] / data_wide[total_meat_item_mask]["population"]

    return data_wide


def process_combined_data(combined: pd.DataFrame) -> pd.DataFrame:
    combined = combined.copy()

    # Get FAO population from data (it is given as another item).
    fao_population = get_fao_population(combined=combined)

    # Include number of slaughtered animals in total meat (which is missing).
    combined = add_slaughtered_animals_to_meat_total(combined)

    # Check that all expected products are included in the data.
    missing_products = sorted(set(PRODUCTS) - set(set(combined["product"])))
    assert len(missing_products) == 0, f"{len(missing_products)} missing products for food explorer."

    # Select relevant products for the food explorer.
    combined = combined[combined["product"].isin(PRODUCTS)].reset_index(drop=True)    

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

    # Fill gaps in OWID population with FAO population (for "* (FAO)" countries, i.e. countries that were not
    # harmonized and for which there is no OWID population).
    # Then drop "fao_population", since it is no longer needed.
    data_wide["population"] = data_wide["population"].fillna(data_wide["fao_population"])
    data_wide = data_wide.drop(columns="fao_population")

    # Add per capita number of slaughtered animals for total meat.
    data_wide = add_slaughtered_animals_per_capita_to_meat_total(data_wide)

    assert len(data_wide.columns[data_wide.isnull().all(axis=0)]) == 0, "Unexpected columns with only nan values."

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

    data = process_combined_data(combined=data)

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
