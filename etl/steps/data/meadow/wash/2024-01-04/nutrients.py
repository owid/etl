"""Load a snapshot and create a meadow dataset."""

import os
import tempfile
import zipfile

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("nutrients.zip")
    # Load data from snapshot.
    tb = read_tb_from_snapshot_zip(snap)
    tb = tb.rename(columns={"countryName": "country", "phenomenonTimeReferenceYear": "year"})
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = (
        tb.underscore()
        .set_index(["country", "year", "waterbodycategory", "eeaindicator"], verify_integrity=True)
        .sort_index()
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def read_tb_from_snapshot_zip(snap: Snapshot) -> Table:
    """Build dataframe from zipped csvs in snapshot."""
    with tempfile.TemporaryDirectory() as temp_dir:
        z = zipfile.ZipFile(snap.path)
        z.extractall(temp_dir)
        tb = pr.read_csv(os.path.join(temp_dir, "aggregateddata_country.csv"), delimiter=";")
        tb.metadata.short_name = snap.metadata.short_name
    return tb
