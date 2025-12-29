"""Load a snapshot and create a meadow dataset."""

from docutils.nodes import header

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names
COLUMNS = {
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

    #
    # Process data.
    #

    # Drop empty rows in Unnamed: 0
    tb_lighting_prices = tb_lighting_prices.dropna(subset=["Unnamed: 0"]).reset_index(drop=True)

    # Drop columns where all values are missing
    tb_lighting_prices = tb_lighting_prices.dropna(axis=1, how="all")

    # Rename columns
    tb_lighting_prices = tb_lighting_prices.rename(columns=COLUMNS, errors="raise")

    # Drop rows where all values except year are missing
    tb_lighting_prices = tb_lighting_prices.dropna(
        how="all", subset=[col for col in tb_lighting_prices.columns if col != "year"]
    )

    # Add country column
    tb_lighting_prices["country"] = "United Kingdom"

    # Improve tables format.
    tables = [tb_lighting_prices.format(["country", "year"], short_name="lighting_prices")]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
