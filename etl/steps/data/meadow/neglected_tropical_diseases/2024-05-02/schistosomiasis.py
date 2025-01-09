"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("schistosomiasis.xlsx")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Values to replace with NaN
    values_to_replace = [
        "To be defined",
        "No PC required",
        "Surveillance",
        " ",
    ]  # List of values you want to replace with NaN

    # Columns in which to replace the values
    columns_to_check = [
        "Population requiring PC for SCH annually",
        "SAC population requiring PC for SCH annually",
        "Programme coverage (%)",
    ]

    # Replacing the values
    tb[columns_to_check] = tb[columns_to_check].replace(values_to_replace, np.nan)
    tb["Programme coverage (%)"] = tb["Programme coverage (%)"].astype(float)
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
