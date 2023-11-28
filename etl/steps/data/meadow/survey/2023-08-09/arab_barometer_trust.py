"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def replace_country_codes(df: pd.DataFrame) -> pd.DataFrame:
    """The exported file keeps some country codes instead of names. In this function I replace them."""
    country_dict = {
        "17": "Saudi Arabia",
        "22": "Yemen",
        "2": "Palestine",
        "3": "Algeria",
        "4": "Morocco",
        "6": "Lebanon",
    }

    df["country"] = df["country"].replace(country_dict)

    # Set indices, verify integrity and sort.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    return df


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("arab_barometer_trust.csv"))

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    # Replace country codes with country names.
    df = replace_country_codes(df)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
