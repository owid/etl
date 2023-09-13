"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("avian_influenza_ah5n1.csv"))

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    # Unpivot
    df = df.melt(id_vars=["Range", "Month"], var_name="country", value_name="avian_cases")

    # Remove unnamed
    df = df[~df["country"].str.contains("Unnamed")]

    # Dtypes
    df["avian_cases"] = df["avian_cases"].astype("int")

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.set_index(["range", "month", "country"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
