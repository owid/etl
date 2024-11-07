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
    snap = paths.load_snapshot("geodist.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive("dist_cepii.xls")

    # change columns to float
    for col in ["distw", "distwces"]:
        tb[col] = pd.to_numeric(tb[col], errors="coerce")
        tb[col].metadata = tb["iso_o"].metadata

    # format table
    tb = tb.format(["iso_o", "iso_d"])

    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
