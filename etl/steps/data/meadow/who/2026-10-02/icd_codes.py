"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Step 1: Read the availability spreadsheet out of the archive.
    snap = paths.load_snapshot("icd_codes.zip")
    with snap.extracted() as archive:
        # WHO names the file after the release month (list_ctry_years_feb2026.xls), so it changes
        # with every update, and the extension has varied between releases. Find it rather than
        # hardcoding it, and insist on exactly one so a restructured archive fails loudly.
        spreadsheets = archive.glob("**/*.xls*")
        assert len(spreadsheets) == 1, f"Expected exactly one spreadsheet in the archive, found {archive.files}"
        # The sheet carries six rows of preamble above the header.
        tb = archive.read(spreadsheets[0], skiprows=7)

    # Step 2: Keep the ICD revision each country reported in each year.
    tb = tb[["name", "Year", "Icd"]]
    tb = tb.rename(columns={"name": "country", "Year": "year", "Icd": "icd"})
    tb = tb.drop_duplicates()

    tb = tb.format(["country", "year"], short_name="icd_codes")

    # Step 3: Save.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()
