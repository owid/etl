"""Food explorer data step.

Loads the faostat_food_explorer dataset from garden and stores a table (as a csv file) for each food product.

"""

import sys
from typing import List

from owid.catalog import Dataset, Table, utils
from tqdm.auto import tqdm

from etl.helpers import PathFinder, create_dataset

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


def create_table_for_each_product(tb_garden: Table) -> List[Table]:
    """Create a list of tables, one for each product found in a garden table.

    Parameters
    ----------
    tb_garden : Table
        Table of products from garden dataset.

    Returns
    -------
    tables : List[Table]
        List of tables, one for each product.

    """
    # List all products in table
    products = sorted(tb_garden.index.get_level_values("product").unique().tolist())

    tables = []
    for product in tqdm(products, file=sys.stdout):
        # Save a table for each food product.
        table_product = tb_garden.loc[product].copy()

        # Update table metadata.
        table_product.title = product

        # Rename columns, select the required ones, and sort columns and rows conveniently.
        table_product = table_product[list(EXPECTED_COLUMNS)].rename(columns=EXPECTED_COLUMNS)
        table_product = table_product[
            ["population"] + [column for column in sorted(table_product.columns) if column not in ["population"]]
        ]
        table_product = table_product.sort_index()

        table_product.metadata.short_name = (
            utils.underscore(name=product, validate=True).replace("__", "_").replace("_e_g_", "_eg_")
        )

        # Add table to list of all tables to include in the explorers dataset.
        tables.append(table_product)

    return tables


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load the dataset for FAOSTAT food explorer from garden.
    ds_garden: Dataset = paths.load_dependency("faostat_food_explorer")

    # Get the table of all food products.
    tb_garden = ds_garden["faostat_food_explorer"]

    #
    # Process data.
    #
    tables = create_table_for_each_product(tb_garden=tb_garden)

    #
    # Save outputs.
    #
    # Initialize new explorers dataset.
    ds_explorers = create_dataset(
        dest_dir=dest_dir, tables=tables, default_metadata=ds_garden.metadata, formats=["csv"]
    )
    ds_explorers.metadata.short_name = "food_explorer"

    # Create new explorers dataset.
    ds_explorers.save()
