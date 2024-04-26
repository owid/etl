"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("avian_influenza_ah5n1.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Unpivot
    tb = tb.melt(id_vars=["Range", "Month"], var_name="country", value_name="avian_cases")

    # Remove unnamed
    tb = tb[~tb["country"].str.contains("Unnamed")]

    # Dtypes
    tb["avian_cases"] = tb["avian_cases"].astype("int")

    # Create a new table and ensure all columns are snake-case.
    tb = tb.format(["range", "month", "country"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
