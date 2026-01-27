"""Load a snapshot and create a meadow dataset."""

from typing import Dict

from owid.catalog import Table
from pyarrow.tests.test_dataset_encryption import COLUMNS

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names
COLUMNS_PRICE_OF_LIGHTING = {
    "Unnamed: 0": "year",
    "Average": "lighting_prices_average_2025prices",
    "Electricity": "lighting_prices_electricity_2025prices",
    "Kerosene": "lighting_prices_kerosene_2025prices",
    "Gas": "lighting_prices_gas_2025prices",
    "Candles": "lighting_prices_candles_2025prices",
    "Whale Oil": "lighting_prices_whale_oil_2025prices",
    "Average.1": "lighting_prices_average_2000prices",
    "Electricity.1": "lighting_prices_electricity_2000prices",
    "Kerosene.1": "lighting_prices_kerosene_2000prices",
    "Gas.1": "lighting_prices_gas_2000prices",
    "Candles.1": "lighting_prices_candles_2000prices",
    "Whale Oil.1": "lighting_prices_whale_oil_2000prices",
}

COLUMNS_ENERGY_CONSUMPTION = {
    "Unnamed: 0": "year",
    "Light (Electricity)": "energy_consumption_electricity",
    "Light (Kerosene)": "energy_consumption_kerosene",
    "Light (Gas)": "energy_consumption_gas",
    "Light (Candles Tallow)": "energy_consumption_candles",
    "Light (All Whale Oil)": "energy_consumption_whale_oil",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("energy_innovation_long_run.xlsx")

    # Load data from snapshot.
    tb_lighting_prices = snap.read(
        sheet_name="Fig 3-4",
        skiprows=2,
        header=0,
        usecols="A:H,N:T",
    )

    tb_energy_consumption = snap.read(
        sheet_name="Fig 3-4",
        skiprows=2,
        header=0,
        usecols="A,AB:AF",
    )

    #
    # Process data.
    #
    tb_lighting_prices = process_table(tb=tb_lighting_prices, column_list=COLUMNS_PRICE_OF_LIGHTING)
    tb_energy_consumption = process_table(tb=tb_energy_consumption, column_list=COLUMNS_ENERGY_CONSUMPTION)

    # Improve tables format.
    tables = [
        tb_lighting_prices.format(["country", "year"], short_name="lighting_prices"),
        tb_energy_consumption.format(["country", "year"], short_name="energy_consumption"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def process_table(tb: Table, column_list: Dict[str, str]) -> Table:
    """Process the input table by renaming columns and dropping empty rows/columns.

    Args:
        tb (Table): The input table to be processed.
        column_list (Dict[str, str]): A dictionary mapping old column names to new column names.

    Returns:
        Table: The processed table.
    """
    # Drop empty rows in Unnamed: 0
    tb = tb.dropna(subset=["Unnamed: 0"]).reset_index(drop=True)

    # Drop columns where all values are missing
    tb = tb.dropna(axis=1, how="all")

    # Rename columns
    tb = tb.rename(columns=column_list, errors="raise")

    # Drop rows where all values except year are missing
    tb = tb.dropna(how="all", subset=[col for col in tb.columns if col != "year"])

    # Add country column
    tb["country"] = "United Kingdom"

    return tb
