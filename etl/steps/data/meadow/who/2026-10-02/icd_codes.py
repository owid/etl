"""Load a snapshot and create a meadow dataset."""

import zipfile

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("icd_codes.zip")

    # Load data from snapshot.
    zf = zipfile.ZipFile(snap.path)
    # WHO names the file after the release month (list_ctry_years_feb2026.xls), so it changes with
    # every update, and the extension has changed between releases too. Find it instead of
    # hardcoding it, but insist on exactly one so a restructured archive fails loudly.
    spreadsheets = [name for name in zf.namelist() if name.lower().endswith((".xls", ".xlsx"))]
    assert len(spreadsheets) == 1, f"Expected exactly one spreadsheet in the archive, found {zf.namelist()}"
    tb = pr.read_excel(zf.open(spreadsheets[0]), skiprows=7)
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
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
