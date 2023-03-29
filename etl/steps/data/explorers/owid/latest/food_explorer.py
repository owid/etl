"""Food explorer data step.

Loads the latest faostat_food_explorer dataset from garden and stores a table (as a csv file) for each food product.

NOTE: It will overwrite csv files inside "data/explorers/owid/latest/food_explorer".

"""

import sys
from copy import deepcopy

from owid import catalog
from tqdm.auto import tqdm

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Rename columns to be used by the food explorer.
# Note: Include here all columns, even if the name is not changed.
EXPECTED_COLUMNS = {
    "population": "population",
    "area_harvested__hectares": "area_harvested__ha",
    "area_harvested__hectares_per_capita": "area_harvested__ha__per_capita",
    "domestic_supply__tonnes": "domestic_supply__tonnes",
    "domestic_supply__tonnes_per_capita": "domestic_supply__tonnes__per_capita",
    "exports__tonnes": "exports__tonnes",
    "exports__tonnes_per_capita": "exports__tonnes__per_capita",
    "feed__tonnes": "feed__tonnes",
    "feed__tonnes_per_capita": "feed__tonnes__per_capita",
    "food__tonnes": "food__tonnes",
    "food__tonnes_per_capita": "food__tonnes__per_capita",
    "food_available_for_consumption__grams_of_fat_per_day_per_capita": "food_available_for_consumption__fat_g_per_day__per_capita",
    "food_available_for_consumption__kilocalories_per_day_per_capita": "food_available_for_consumption__kcal_per_day__per_capita",
    "food_available_for_consumption__kilograms_per_year_per_capita": "food_available_for_consumption__kg_per_year__per_capita",
    "food_available_for_consumption__grams_of_protein_per_day_per_capita": "food_available_for_consumption__protein_g_per_day__per_capita",
    "imports__tonnes": "imports__tonnes",
    "imports__tonnes_per_capita": "imports__tonnes__per_capita",
    "other_uses__tonnes": "other_uses__tonnes",
    "other_uses__tonnes_per_capita": "other_uses__tonnes__per_capita",
    "producing_or_slaughtered_animals__animals": "producing_or_slaughtered_animals__animals",
    "producing_or_slaughtered_animals__animals_per_capita": "producing_or_slaughtered_animals__animals__per_capita",
    "production__tonnes": "production__tonnes",
    "production__tonnes_per_capita": "production__tonnes__per_capita",
    "waste_in_supply_chain__tonnes": "waste_in_supply_chain__tonnes",
    "waste_in_supply_chain__tonnes_per_capita": "waste_in_supply_chain__tonnes__per_capita",
    "yield__kilograms_per_animal": "yield__kg_per_animal",
    "yield__tonnes_per_hectare": "yield__tonnes_per_ha",
}


def run(dest_dir: str) -> None:
    # Load the dataset for FAOSTAT food explorer from garden.
    dataset_garden: catalog.Dataset = paths.load_dependency("faostat_food_explorer")

    # Get the table of all food products.
    table_garden = dataset_garden["all_products"]

    # Initialize new garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir)
    # Add dataset metadata.
    dataset.metadata = deepcopy(dataset_garden.metadata)
    dataset.metadata.namespace = "owid"
    dataset.metadata.short_name = "food_explorer"
    dataset.metadata.version = "latest"
    # Create new dataset in garden.
    dataset.save()

    # List all products in table
    products = sorted(table_garden.index.get_level_values("product").unique().tolist())

    for product in tqdm(products, file=sys.stdout):
        # Save a table (as a separate csv file) for each food product.
        table_product = table_garden.loc[product].copy()
        # Update table metadata.
        table_product.title = product

        # Rename columns, select the required ones, and sort columns and rows conveniently.
        table_product = table_product[list(EXPECTED_COLUMNS)].rename(columns=EXPECTED_COLUMNS)
        table_product = table_product[
            ["population"] + [column for column in sorted(table_product.columns) if column not in ["population"]]
        ]
        table_product = table_product.sort_index()

        table_product.metadata.short_name = (
            catalog.utils.underscore(name=product, validate=True).replace("__", "_").replace("_e_g_", "_eg_")
        )
        # Add table to dataset. Force publication in csv.
        dataset.add(table_product, formats=["csv"])
