"""Dataset feeding the global food explorer.

Load the qcl and fbsc (combination of fbsh and fbs) datasets, and create a combined dataset of food items (now called
products).

The resulting dataset will later be loaded by the `explorer/food_explorer` which feeds our
[Global food explorer](https://ourworldindata.org/explorers/global-food).

"""

from copy import deepcopy
from typing import cast

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta
from owid.datautils import dataframes
from shared import LATEST_VERSIONS_FILE, NAMESPACE, VERSION

from etl.data_helpers import geo
from etl.paths import DATA_DIR

# Dataset name and title.
DATASET_TITLE = "Food Explorer"
DATASET_SHORT_NAME = f"{NAMESPACE}_food_explorer"
DATASET_DESCRIPTION = (
    "This dataset has been created by Our World in Data, merging existing FAOSTAT datasets. In "
    "particular, we have used 'Crops and livestock products' (QCL) and 'Food Balances' (FBSH and "
    "FBS) datasets. Each row contains all the metrics for a specific combination of (country, "
    "product, year). The metrics may come from different datasets."
)

# List of items (OWID names) to include in the global food explorer.
# Note: The names of the products will be further edited in owid-content, following to the following file:
# https://github.com/owid/owid-content/blob/master/scripts/global-food-explorer/foods.csv
PRODUCTS = [
    "Almonds",
    "Animal fats",
    "Apples",
    "Apricots",
    "Areca nuts",
    "Artichokes",
    "Asparagus",
    "Avocados",
    "Bananas",
    "Barley",
    "Beans, dry",
    "Beeswax",
    "Blueberries",
    "Brazil nuts, with shell",
    "Broad beans",
    "Buckwheat",
    "Buffalo hides",
    "Butter and ghee",
    "Cabbages",
    "Canary seed",
    "Carrots and turnips",
    "Cashew nuts",
    "Cassava",
    "Castor oil seed",
    "Cattle hides",
    "Cauliflowers and broccoli",
    "Cereals",
    "Cheese",
    "Cherries",
    "Chestnut",
    "Chickpeas",
    "Chillies and peppers",
    "Citrus Fruit",
    "Cocoa beans",
    "Coconut oil",
    "Coconuts",
    "Coffee, green",
    "Cotton",
    "Cottonseed",
    "Cottonseed oil",
    "Cow peas",
    "Cranberries",
    "Cucumbers and gherkins",
    "Currants",
    "Dates",
    "Eggplants",
    "Eggs",
    "Eggs from hens",
    "Eggs from other birds (excl. hens)",
    "Fat, buffaloes",
    "Fat, camels",
    "Fat, cattle",
    "Fat, goats",
    "Fat, pigs",
    "Fat, sheep",
    "Fibre crops",
    "Fish and seafood",
    "Flax fibre",
    "Fruit",
    "Garlic",
    "Grapefruit",
    "Grapes",
    "Beans, green",
    "Green maize",
    "Groundnut oil",
    "Groundnuts",
    "Hazelnuts",
    "Hempseed",
    "Herbs (e.g. fennel)",
    "Honey",
    "Jute",
    "Karite nuts",
    "Kiwi",
    "Kola nuts",
    "Leeks",
    "Lemons and limes",
    "Lentils",
    "Lettuce",
    "Linseed",
    "Linseed oil",
    "Maize",
    "Maize oil",
    "Mangoes",
    "Margarine",
    "Meat, total",
    "Meat, ass",
    "Meat, beef",
    "Meat, beef and buffalo",
    "Meat, buffalo",
    "Meat, camel",
    "Meat, chicken",
    "Meat, duck",
    "Meat, game",
    "Meat, goat",
    "Meat, goose and guinea fowl",
    "Meat, horse",
    "Meat, lamb and mutton",
    "Meat, mule",
    "Meat, pig",
    "Meat, poultry",
    "Meat, rabbit",
    "Meat, sheep and goat",
    "Meat, turkey",
    "Melon",
    "Melonseed",
    "Milk",
    "Millet",
    "Mixed grains",
    "Molasses",
    "Mushrooms",
    "Mustard seed",
    "Nuts",
    "Oats",
    "Offals",
    "Offals, buffaloes",
    "Offals, camels",
    "Offals, cattle",
    "Offals, goats",
    "Offals, horses",
    "Offals, pigs",
    "Offals, sheep",
    "Oilcrops",
    "Oilcrops, Cake Equivalent",
    "Oilcrops, Oil Equivalent",
    "Okra",
    "Olive oil",
    "Olives",
    "Onions",
    "Oranges",
    "Palm fruit oil",
    "Palm kernel oil",
    "Palm kernels",
    "Palm oil",
    "Papayas",
    "Peaches and nectarines",
    "Pears",
    "Peas, dry",
    "Peas, green",
    "Pepper",
    "Pigeon peas",
    "Pineapples",
    "Pistachios",
    "Plantains",
    "Plums",
    "Poppy seeds",
    "Pork",
    "Potatoes",
    "Pulses",
    "Quinoa",
    "Rapeseed",
    "Rapeseed oil",
    "Raspberries",
    "Rice",
    "Roots and tubers",
    "Rye",
    "Safflower oil",
    "Safflower seed",
    "Seed cotton",
    "Sesame oil",
    "Sesame seed",
    "Silk",
    "Skins, goat",
    "Skins, sheep",
    "Sorghum",
    "Soybean oil",
    "Soybeans",
    "Spinach",
    "Strawberries",
    "String beans",
    "Sugar (raw)",
    "Sugar beet",
    "Sugar cane",
    "Sugar crops",
    "Sunflower oil",
    "Sunflower seed",
    "Sweet potatoes",
    "Tangerines",
    "Tea",
    "Tobacco",
    "Tomatoes",
    "Total",
    "Treenuts",
    "Vegetables",
    "Walnuts",
    "Watermelons",
    "Wheat",
    "Whey",
    "Wine",
    "Wool",
    "Yams",
]
# OWID item name, element name, and unit name for population (as given in faostat_qcl and faostat_fbsc datasets).
FAO_POPULATION_ITEM_NAME = "Population"
FAO_POPULATION_ELEMENT_NAME = "Total Population - Both sexes"
FAO_POPULATION_UNIT = "1000 persons"

# List of element codes to consider from faostat_qcl.
ELEMENT_CODES_QCL = [
    "005312",
    "005313",
    "005314",
    "005318",
    "005320",
    "005321",
    "005410",
    "005413",
    "005417",
    "005419",
    "005420",
    "005422",
    "005424",
    "005510",
    "005513",
    "5312pc",
    "5320pc",
    "5321pc",
    "5510pc",
]
# List of element codes to consider from faostat_fbsc.
ELEMENT_CODES_FBSC = [
    "000645",
    "000664",
    "000674",
    "000684",
    "005072",
    "005123",
    "005131",
    "005142",
    "005154",
    "005170",
    "005171",
    "005301",
    # Element 'Production' (in tonnes, originally given in 1000 tonnes) is taken from qcl.
    # Although fbsc has items for this element that are not in qcl, they overlap in a number of items with slightly
    # different values. To avoid this issue, we ignore the element from fbsc and use only the one in qcl.
    # '005511',
    "005521",
    "005527",
    "005611",
    "005911",
    "0645pc",
    "0664pc",
    "0674pc",
    "0684pc",
    "5123pc",
    "5142pc",
    "5154pc",
    "5301pc",
    "5521pc",
    "5611pc",
    "5911pc",
    # The following element code is for population.
    "000511",
]


def combine_qcl_and_fbsc(qcl_table: catalog.Table, fbsc_table: catalog.Table) -> pd.DataFrame:
    """Combine garden `faostat_qcl` and `faostat_fbsc` datasets.

    Parameters
    ----------
    qcl_table : catalog.Table
        Main table (in long format) of the `faostat_qcl` dataset.
    fbsc_table : catalog.Table
        Main table (in long format) of the `faostat_fbsc` dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined data (as a dataframe, not a table).

    """
    columns = [
        "country",
        "year",
        "item_code",
        "element_code",
        "item",
        "element",
        "unit",
        "unit_short_name",
        "value",
        "population_with_data",
    ]
    qcl = pd.DataFrame(qcl_table).reset_index()[columns]
    # Select relevant element codes.
    qcl = qcl[qcl["element_code"].isin(ELEMENT_CODES_QCL)].reset_index(drop=True)
    qcl["value"] = qcl["value"].astype(float)
    qcl["element"] = [element for element in qcl["element"]]
    qcl["unit"] = [unit for unit in qcl["unit"]]
    qcl["item"] = [item for item in qcl["item"]]
    fbsc = pd.DataFrame(fbsc_table).reset_index()[columns]
    # Select relevant element codes.
    fbsc = fbsc[fbsc["element_code"].isin(ELEMENT_CODES_FBSC)].reset_index(drop=True)
    fbsc["value"] = fbsc["value"].astype(float)
    fbsc["element"] = [element for element in fbsc["element"]]
    fbsc["unit"] = [unit for unit in fbsc["unit"]]
    fbsc["item"] = [item for item in fbsc["item"]]

    rename_columns = {"item": "product"}
    combined = (
        dataframes.concatenate([qcl, fbsc], ignore_index=True).rename(columns=rename_columns).reset_index(drop=True)
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

    return cast(pd.DataFrame, combined)


def get_fao_population(combined: pd.DataFrame) -> pd.DataFrame:
    """Extract the FAO population data from data (in long format).

    Parameters
    ----------
    combined : pd.DataFrame
        Combination of `faostat_qcl` and `faostat_fbsc` data (although this function could also be applied to just
        `faostat_fbsc` data, since `faostat_qcl` does not contain FAO population data).

    Returns
    -------
    fao_population : pd.DataFrame
        Population (by country and year) according to FAO, extracted from the `faostat_fbsc` dataset.

    """
    # Select the item and element that corresponds to population values.
    fao_population = combined[
        (combined["product"] == FAO_POPULATION_ITEM_NAME) & (combined["element"] == FAO_POPULATION_ELEMENT_NAME)
    ].reset_index(drop=True)

    # Check that population is given in "1000 persons" and convert to persons.
    error = "FAOSTAT population changed item, element, or unit."
    assert list(fao_population["unit"].unique()) == [FAO_POPULATION_UNIT], error
    fao_population["value"] *= 1000

    # Drop missing values and prepare output dataframe.
    fao_population = (
        fao_population[["country", "year", "value"]].dropna(how="any").rename(columns={"value": "fao_population"})
    )

    return fao_population


def process_combined_data(combined: pd.DataFrame) -> pd.DataFrame:
    """Process combined data (combination of `faostat_qcl` and `faostat_fbsc` data) to have the content and format
    required by the food explorer.

    Parameters
    ----------
    combined : pd.DataFrame
        Combination of `faostat_qcl` and `faostat_fbsc` data.

    Returns
    -------
    data_wide : pd.DataFrame
        Processed data (in wide format).

    """
    combined = combined.copy()

    # Get FAO population from data (it is given as another item).
    fao_population = get_fao_population(combined=combined)

    # Check that all expected products are included in the data.
    missing_products = sorted(set(PRODUCTS) - set(set(combined["product"])))
    assert len(missing_products) == 0, f"{len(missing_products)} missing products for food explorer."

    # Select relevant products for the food explorer.
    combined = combined[combined["product"].isin(PRODUCTS)].reset_index(drop=True)

    # Join element and unit into one title column.
    combined["title"] = combined["element"] + " (" + combined["unit"] + ")"

    # This will create a table with just one column and country-year as index.
    index_columns = ["product", "country", "year"]
    data_wide = combined.pivot(index=index_columns, columns=["title"], values="value").reset_index()

    # Add column for FAO population.
    data_wide = pd.merge(data_wide, fao_population, on=["country", "year"], how="left")

    # Add column for OWID population.
    data_wide = geo.add_population_to_dataframe(df=data_wide, warn_on_missing_countries=False)

    # Fill gaps in OWID population with FAO population (for "* (FAO)" countries, i.e. countries that were not
    # harmonized and for which there is no OWID population).
    # Then drop "fao_population", since it is no longer needed.
    data_wide["population"] = data_wide["population"].fillna(data_wide["fao_population"])
    data_wide = data_wide.drop(columns="fao_population")

    assert len(data_wide.columns[data_wide.isnull().all(axis=0)]) == 0, "Unexpected columns with only nan values."

    # Set a reasonable index.
    data_wide = data_wide.set_index(index_columns, verify_integrity=True)

    return data_wide


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Load file of versions.
    latest_versions = pd.read_csv(LATEST_VERSIONS_FILE).set_index(["channel", "dataset"])

    # Path to latest qcl and fbsc datasets in garden.
    qcl_latest_version = latest_versions.loc["garden", f"{NAMESPACE}_qcl"].item()
    qcl_latest_dir = DATA_DIR / "garden" / NAMESPACE / qcl_latest_version / f"{NAMESPACE}_qcl"
    fbsc_latest_version = latest_versions.loc["garden", f"{NAMESPACE}_fbsc"].item()
    fbsc_latest_dir = DATA_DIR / "garden" / NAMESPACE / fbsc_latest_version / f"{NAMESPACE}_fbsc"

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

    # Combine `faostat_qcl` and `faostat_fbsc` data.
    data = combine_qcl_and_fbsc(qcl_table=qcl_table, fbsc_table=fbsc_table)

    # Prepare data in the format required by the food explorer.
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
    table.metadata.primary_key = list(table.index.names)
    # Add table to dataset.
    explorer_dataset.add(table)
