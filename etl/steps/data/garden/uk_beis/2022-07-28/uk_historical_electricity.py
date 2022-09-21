import pandas as pd
from owid import catalog
from shared import CURRENT_DIR
from owid.datautils import dataframes

from etl.helpers import Names
from etl.paths import DATA_DIR

DATASET_TITLE = "UK historical electricity data"
DATASET_SHORT_NAME = "uk_historical_electricity"
N = Names(str(CURRENT_DIR / DATASET_SHORT_NAME))
MEADOW_PATH = DATA_DIR / f"meadow/uk_beis/2022-07-28/{DATASET_SHORT_NAME}"

# Conversion factor from million tonnes of oil equivalent to terawatt-hours.
MTOE_TO_TWH = 11.63


def combine_tables(tb_fuel_input: catalog.Table, tb_supply: catalog.Table, tb_efficiency: catalog.Table) -> catalog.Table:
    tb_fuel_input = tb_fuel_input.copy()
    tb_supply = tb_supply.copy()
    tb_efficiency = tb_efficiency.copy()

    # Create convenient dataframes.
    df_fuel_input = pd.DataFrame(tb_fuel_input)
    df_supply = pd.DataFrame(tb_supply)
    df_efficiency = pd.DataFrame(tb_efficiency)

    # Remove rows with duplicated year.
    df_fuel_input = df_fuel_input.drop_duplicates(subset="year", keep="last").reset_index(drop=True)
    df_supply = df_supply.drop_duplicates(subset="year", keep="last").reset_index(drop=True)
    df_efficiency = df_efficiency.drop_duplicates(subset="year", keep="last").reset_index(drop=True)

    # Convert units of fuel input data.
    for column in df_fuel_input.set_index("year").columns:
        df_fuel_input[column] *= MTOE_TO_TWH
    
    # Add column for the sum of electricity generation and net imports.
    df_supply["electricity_generation_plus_net_imports"] = df_supply["electricity_generation"] + df_supply["net_imports"]

    # Combine dataframes.
    df_combined = dataframes.multi_merge(dfs=[df_fuel_input, df_supply, df_efficiency], how="outer", on="year")

    # Prepare metadata using one of the original tables.
    tb_combined_metadata = tb_fuel_input.metadata
    tb_combined_metadata.short_name = DATASET_SHORT_NAME
    tb_combined_metadata.title = DATASET_TITLE

    # Create a new table with metadata from any of the tables.
    tb_combined = catalog.Table(df_combined, metadata=tb_combined_metadata)

    # Add a country column (even if there is only one country) and set an appropriate index.
    tb_combined["country"] = "United Kingdom"
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return tb_combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read dataset from meadow.
    ds_meadow = catalog.Dataset(MEADOW_PATH)
    # Load tables from meadow dataset.
    tb_fuel_input = ds_meadow["fuel_input"]
    tb_supply = ds_meadow["supply"]
    tb_efficiency = ds_meadow["efficiency"]

    #
    # Process data.
    #
    # Clean and combine tables.
    tb_garden = combine_tables(tb_fuel_input=tb_fuel_input, tb_supply=tb_supply, tb_efficiency=tb_efficiency)

    #
    # Save outputs.
    #
    # Create new dataset.
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    # Get metadata from yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, DATASET_SHORT_NAME)

    ds_garden.add(tb_garden)
    ds_garden.save()

