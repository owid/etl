"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the "Electricity generation by fuel" sheet, and how to rename them.
# Values are in GWh.
COLUMNS = {
    "Year": "year",
    "Total estimated generation": "total_generation",
    "Coal estimated generation": "coal_generation",
    "Oil estimated generation [Note 2]": "oil_generation",
    "Natural gas estimated generation [Note 3]": "gas_generation",
    "Nuclear estimated generation": "nuclear_generation",
    "Wind, wave, solar and hydro estimated generation [Note 4]": "wind_wave_solar_and_hydro_generation",
    "Coke and breeze estimated generation": "coke_and_breeze_generation",
    "Other fuels estimated generation [Note 5]": "other_fuels_generation",
    "Pumped storage estimated generation [Note 6]": "pumped_storage_generation",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and load the sheet on estimated electricity generation by fuel.
    snap = paths.load_snapshot("uk_electricity_capacity_and_generation.xlsx")
    tb = snap.read_excel(sheet_name="Electricity generation by fuel", skiprows=4)

    #
    # Process data.
    #
    # Select and rename columns (the sheet also contains share columns, which are not needed).
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Sanity checks.
    error = "File structure has changed."
    assert tb["year"].iloc[0] == 1920, error
    assert tb["year"].iloc[-1] == 2020, error
    assert all(pd.api.types.is_numeric_dtype(tb[column]) for column in tb.columns), error

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
