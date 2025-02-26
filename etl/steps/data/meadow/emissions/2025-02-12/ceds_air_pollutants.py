"""Load a snapshot and create a meadow dataset."""

import zipfile

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def read_data_from_snapshot(snap: Snapshot) -> Table:
    tbs = []
    with zipfile.ZipFile(snap.path, "r") as zip_file:
        for filename in zip_file.namelist():
            if filename.endswith(".csv"):
                _tb = pr.read_csv(
                    zip_file.open(filename), origin=snap.metadata.origin, metadata=snap.to_table_metadata()
                )
                tbs.append(_tb)

    # Combine all tables into a single one.
    tb = pr.concat(tbs)

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_detailed = paths.load_snapshot("ceds_air_pollutants__detailed.zip")
    snap_bunkers = paths.load_snapshot("ceds_air_pollutants__bunkers.zip")

    # Read data from all csv files within each of the snapshot zip folders.
    tb_detailed = read_data_from_snapshot(snap=snap_detailed)
    tb_bunkers = read_data_from_snapshot(snap=snap_bunkers)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb_detailed.format(["em", "country", "sector", "fuel"]), tb_bunkers.format(["em", "iso", "sector"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap_detailed.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
