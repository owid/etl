"""Load the spreadsheet of births and fertility rates from national statistics, and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns of the Data and Countries tabs, and their new names.
COLUMNS_DATA = {
    "Country": "country",
    "Year": "year",
    "Births": "births",
    "Fertility rate": "fertility_rate",
    "Sources": "sources",
    "Notes": "notes",
}
COLUMNS_COUNTRIES = {"Country": "country", "Status": "status", "Reason": "reason"}


def run() -> None:
    # Load the snapshot and read its Data and Countries tabs.
    snap = paths.load_snapshot("births_fertility_national_vs_un.xlsx")
    tb = snap.read_excel(sheet_name="Data")
    tb_countries = snap.read_excel(sheet_name="Countries")

    # Check the columns, then rename them.
    assert list(tb.columns) == list(COLUMNS_DATA), tb.columns
    assert list(tb_countries.columns) == list(COLUMNS_COUNTRIES), tb_countries.columns
    tb = tb.rename(columns=COLUMNS_DATA, errors="raise")
    tb_countries = tb_countries.rename(columns=COLUMNS_COUNTRIES, errors="raise")

    # Improve table formats.
    tb = tb.format(["country", "year"], short_name="births_fertility")
    tb_countries = tb_countries.format(["country"], short_name="countries")

    # Save outputs.
    ds_meadow = paths.create_dataset(tables=[tb, tb_countries])
    ds_meadow.save()
