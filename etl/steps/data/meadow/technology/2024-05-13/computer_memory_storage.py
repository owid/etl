"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset
from owid.catalog import Table

import pandas as pd

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

PARSING_INSTRUCTIONS = {
    "MEMORY": {"skiprows": 4, "usecols": [0, 1]},
    "DDRIVES": {"skiprows": 4, "usecols": [1, 4]},
    "SSD": {"skiprows": 4, "usecols": [1, 3]},
    "FLASH": {"skiprows": 4, "usecols": [1, 5]},
}


def read_sheet(snapshot, sheet_name):
    """Read a sheet from a snapshot."""
    tb = snapshot.read_excel(sheet_name=sheet_name, header=None, **PARSING_INSTRUCTIONS[sheet_name])
    tb.columns = ["year", "price"]
    tb["type"] = sheet_name.lower()
    return tb


def clean_data(tb):
    # Remove NA years
    tb = tb.dropna(subset=["year"])

    # Convert year to integer
    tb["year"] = tb["year"].astype(int)

    # Convert price to float
    tb["price"] = tb["price"].astype(float)

    # Keep cheapest price per year
    tb = tb.groupby(["year", "type"]).min().reset_index()

    # Sort by year
    tb = tb.sort_values(["year", "type"])

    # For each type, keep cheapest value over time use cummin
    tb["price"] = tb.groupby("type")["price"].cummin()

    # Convert prices to $/TB instead of $/MB
    tb["price"] = tb.price.mul(1000000).round(2)

    # Add country World
    tb["country"] = "World"

    return tb


def reshape_data(tb):
    # Move type to columns
    tb = tb.pivot(index=["country", "year"], columns="type", values="price").reset_index()
    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("computer_memory_storage.xlsx")

    # Load data from snapshot.
    data = []
    for sheet_name in PARSING_INSTRUCTIONS.keys():
        data.append(read_sheet(snap, sheet_name))
    tb = pd.concat(data)

    #
    # Process data.
    #
    tb = clean_data(tb)
    tb = reshape_data(tb)
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])
    tb.metadata.short_name = paths.short_name

    # Ensure metadata is correctly associated.
    for column in tb.columns:
        tb[column].metadata.origins = [snap.metadata.origin]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
