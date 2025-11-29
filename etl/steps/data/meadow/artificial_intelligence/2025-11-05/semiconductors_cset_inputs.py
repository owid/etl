"""Load a snapshot and create a meadow dataset."""

import re

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("semiconductors_cset_inputs.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("semiconductors_cset_inputs.csv")

    # Read snapshot
    tb = snap.read()

    #
    # Process data.
    #
    # Clean column names
    columns = ["input_name", "year", "market_share_chart_global_market_size_info", "description"]
    tb = tb[columns]
    # Clean up market_share_chart_global_market_size_info column
    # Extract numeric value and convert to float
    if "market_share_chart_global_market_size_info" in tb.columns:
        tb["market_size_value"] = tb["market_share_chart_global_market_size_info"].apply(extract_market_size)

    tb = tb.dropna(subset=["year"])

    tb = tb.drop(columns=["market_share_chart_global_market_size_info"])
    #
    # Create a new table.
    #
    tb = tb.format(["input_name", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

    paths.log.info("semiconductors_cset_inputs.end")


def extract_market_size(value: str) -> float:
    """Extract numeric market size value from string like '$574.1 billion (2022)'."""
    if pd.isna(value) or value == "":
        return None

    # Extract number and unit (billion/million)
    pattern = r"\$?([\d,.]+)\s*(billion|million)"
    match = re.search(pattern, value, re.IGNORECASE)

    if match:
        number = float(match.group(1).replace(",", ""))
        unit = match.group(2).lower()

        # Convert to billions
        if unit == "million":
            return number / 1000
        else:  # billion
            return number

    return None
