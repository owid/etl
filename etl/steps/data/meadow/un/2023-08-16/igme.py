"""Load a snapshot and create a meadow dataset."""

import zipfile
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
    snap = cast(Snapshot, paths.load_dependency("igme.zip"))

    # Load data from snapshot.
    zf = zipfile.ZipFile(snap.path)
    df = pd.read_csv(zf.open("UN IGME 2022.csv"), low_memory=False)
    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.rename(columns={"geographic_area": "country", "ref_date": "year"})
    tb = tb.set_index(
        ["country", "year", "indicator", "sex", "wealth_quintile", "series_name", "regional_group"],
        verify_integrity=True,
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
