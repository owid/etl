"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("soil_transmitted_helminthiases.xlsx")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)
    tb = tb.drop(columns="country_code")

    #
    # Process data.
    #
    values_to_replace = [
        "To be defined",
        "No PC required",
        "No data",
        "No data available",
        " ",
    ]

    # Columns in which to replace the values - so that they can be floats rather than objects
    columns_to_check = [
        "Population requiring PC for STH, Pre-SAC",
        "Programme coverage, Pre-SAC (%)",
        "Population requiring PC for STH, SAC",
        "Programme coverage, SAC (%)",
    ]

    # Replacing the values
    tb[columns_to_check] = tb[columns_to_check].replace(values_to_replace, np.nan)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    cols = ["country", "year", "Drug combination, Pre-SAC", "Drug combination, SAC"]
    tb = tb.format(cols)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
