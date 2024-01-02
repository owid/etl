"""Load a snapshot and create a meadow dataset."""

import os
from pathlib import Path

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from owid.datautils.dataframes import multi_merge

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("all_indicators.zip")

    # Load data from snapshot.
    with snap.extract_to_tempdir() as tmpdir:
        tbs = []
        files = os.listdir(tmpdir)
        for fname in files:
            # Sanity check
            assert Path(fname).suffix == ".txt", "All files in the directory should be .txt files!"
            # Only read country files
            if "_c.txt" in fname:
                # Read frame
                tb = pr.read_csv(
                    Path(tmpdir) / fname,
                    sep=" ",
                    metadata=snap.to_table_metadata(),
                    origin=snap.metadata.origin,
                )
                # Format frame
                tb = tb.melt(id_vars="region", var_name="year", value_name=fname.replace(".txt", ""))
                # Append frame to list of frames
                tbs.append(tb)

    # Merge all tables with metadata
    tb = tbs[0]
    for tb_ in tbs[1:]:
        tb = pr.merge(tb, tb_, how="outer", on=["region", "year"])

    #
    # Process data.
    #
    # Rename
    tb = tb.rename({"region": "country"}, axis=1)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
