"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("air_traffic")

    # Read table from meadow dataset.
    tb = ds_meadow.read("air_traffic")

    #
    # Process data.
    #
    # Identify numeric data columns (suffixed) and convert to float.
    # Meadow may store them as strings with comma-formatted numbers (e.g. '6,600').
    # Preserve origins across pd.to_numeric calls (which return plain pandas Series).
    numeric_cols = [c for c in tb.columns if c.endswith("__mils") or c.endswith("__000")]
    for col in numeric_cols:
        origins = tb[col].metadata.origins
        tb[col] = pd.to_numeric(tb[col].astype(str).str.replace(",", "", regex=False), errors="coerce")
        tb[col].metadata.origins = origins

    # Convert columns ending in '__mils' to millions
    for col in tb.columns[tb.columns.str.endswith("__mils")]:
        tb[col.replace("__mils", "")] = tb[col] * 1_000_000
        tb = tb.drop(columns=[col])

    # Convert columns ending in '__000' to thousands
    for col in tb.columns[tb.columns.str.endswith("__000")]:
        tb[col.replace("__000", "")] = tb[col] * 1_000
        tb = tb.drop(columns=[col])

    # Convert passenger load factor: stored as percentage strings (e.g. '60.9%'), convert to float.
    # Preserve origins across pd.to_numeric (which strips Variable metadata).
    plf_origins = tb["plf"].metadata.origins
    tb["plf"] = pd.to_numeric(tb["plf"].astype(str).str.replace("%", "", regex=False), errors="coerce")
    tb["plf"].metadata.origins = plf_origins
    tb["plf_empty"] = 100 - tb["plf"]

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
