"""Load a snapshot and create a meadow dataset."""

import zipfile

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("icd_codes.zip")

    # Load data from snapshot.
    zf = zipfile.ZipFile(snap.path)
    tb = pr.read_excel(zf.open("list_ctry_years_21feb2024.xls"), skiprows=7)
    tb = tb[["name", "Year", "Icd"]]
    tb = tb.rename(columns={"name": "country", "Year": "year", "Icd": "icd"})
    tb = tb.drop_duplicates()
    #
    # Process data.
    #
    # Ensure the variable has origins metadata.

    tb["icd"].metadata.origins = [snap.metadata.origin]

    tb = tb.format(["country", "year"], short_name="icd_codes")

    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
