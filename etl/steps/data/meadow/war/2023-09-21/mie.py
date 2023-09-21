"""Load a snapshot and create a meadow dataset."""

import os
import tempfile

import pandas as pd
from owid.catalog import Table
from owid.datautils.io import decompress_file

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("mie.zip")

    # Extract data
    with tempfile.TemporaryDirectory() as tmpdir:
        decompress_file(snap.path, tmpdir)
        path = os.path.join(tmpdir, "mie-1.0.csv")  # other file: "INTRA-STATE_State_participants v5.1 CSV.csv"
        df = pd.read_csv(path)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.set_index(["micnum", "eventnum"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
