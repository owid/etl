"""Load a snapshot and create a meadow dataset."""

import os
import tempfile
import zipfile
from typing import cast

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("gbd_mental_health_prevalence_rate.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("gbd_mental_health_prevalence_rate.zip")

    # Load data from snapshot.
    df = read_df_from_snapshot_zips(snap)

    #
    # Process data.
    #
    # Sanity checks
    assert set(df["measure"]) == {"Prevalence"}, "More than one measure found! Should only be 'Prevalence'."
    assert set(df["metric"]) == {"Rate"}, "More than one metric found! Should only be 'Rate'."
    # Set index, organize columns
    df = df.rename(columns={"val": "prevalence_rate", "location": "country"})
    df = df.set_index(["country", "year", "sex", "age", "cause"], verify_integrity=True).sort_index()
    df = df[["prevalence_rate"]]

    # Create table
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("gbd_mental_health_prevalence_rate.end")


def read_df_from_snapshot_zips(snap: Snapshot) -> pd.DataFrame:
    """Build dataframe from zipped csvs in snapshot."""
    dfs = []
    with tempfile.TemporaryDirectory() as temp_dir:
        z = zipfile.ZipFile(snap.path)
        z.extractall(temp_dir)
        for f in os.listdir(temp_dir):
            if f.endswith(".csv"):
                df_ = pd.read_csv(os.path.join(temp_dir, f))
                dfs.append(df_)
    df = cast(pd.DataFrame, pd.concat(dfs, ignore_index=True))
    return df
