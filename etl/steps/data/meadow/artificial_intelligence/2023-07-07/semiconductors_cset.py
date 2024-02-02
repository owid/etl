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
    snap = cast(Snapshot, paths.load_dependency("semiconductors_cset.csv"))

    # Load data from snapshot.
    df = pd.read_csv(snap.path)
    # Keep relevant columns (provider name - country/organisation, provided_name - stage of development and share of semincoductors provided provided)
    columns_to_keep = ["provider_name", "provided_name", "share_provided", "provided_id"]
    df = df[columns_to_keep]

    # Remove percentage sign from share provided
    df["share_provided"] = df["share_provided"].str.replace("%", "")

    # Rename provider_name to country
    df.rename(columns={"provider_name": "country"}, inplace=True)

    # Selecting rows where "provided_id" is "S1", "S2", or "S3"
    df_filtered = df[df["provided_id"].isin(["S1", "S2", "S3"])]

    # Resetting the DataFrame index
    df_filtered = df_filtered.reset_index(drop=True)
    # Dropping rows with NaN values in the "share_provided" column
    df_filtered = df_filtered.dropna(subset=["share_provided"])

    # Resetting the DataFrame index
    df_filtered = df_filtered.reset_index(drop=True)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df_filtered, short_name=paths.short_name, underscore=True)
    tb.set_index(["country", "provided_name"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
