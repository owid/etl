"""Load a snapshot and create a meadow dataset."""

from zipfile import ZipFile

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("maternal_mortality.zip")
    snap_meta = snap.to_table_metadata()

    # Load data from snapshot.
    zf = ZipFile(snap.path)
    folder_name = zf.namelist()[0]
    tb = pr.read_csv(zf.open(f"{folder_name}estimates.csv"), metadata=snap_meta, origin=snap.metadata.origin)

    # drop unneeded column
    tb = tb.drop(columns=["estimate_version"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.rename(columns={"iso_alpha_3_code": "country", "year_mid": "year"})
    tb = tb.format(["country", "year", "parameter"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
