"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("self_inflicted_injuries.csv")

    # Load data from snapshot.
    tb = snap.read(skiprows=5, index_col=False)

    #
    # Process data.
    #

    columns_to_keep = [
        "Country Name",
        "Year",
        "Sex",
        "Age Group",
        "Number",
        "Percentage of cause-specific deaths out of total deaths",
        "Age-standardized death rate per 100 000 standard population",
        "Death rate per 100 000 population",
    ]
    tb = tb[columns_to_keep]

    tb = tb.rename(columns={"Country Name": "country"})
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year", "sex", "age_group"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
