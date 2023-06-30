"""Load a snapshot and create a meadow dataset."""

import os
import zipfile
from typing import cast

import numpy as np
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
    log.info("wrp_2021.start")
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("wrp_2021.zip"))
    dest_dir_zip = os.path.dirname(snap.path)

    with zipfile.ZipFile(snap.path, "r") as zip_file:
        # Extract the desired file
        zip_file.extract("lrf_wrp_2021_full_data.csv", dest_dir_zip)

    # Load data from snapshot.
    df = pd.read_csv(dest_dir_zip + "/" + "lrf_wrp_2021_full_data.csv", low_memory=False)
    countries_both_years = df["country.in.both.waves"] == 1
    df.replace(" ", np.nan, inplace=True)
    df = df[countries_both_years]
    df.reset_index(drop=True, inplace=True)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wrp_2021.end")
