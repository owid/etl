"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Years for which we have snapshots.
AVAILABLE_YEARS = [
    # TODO: We also have a snapshot for 2017, but the format is different. If needed, consider adapting the code to include it.
    # 2017,
    2018,
    2019,
]

# Names of columns (expected to result from concatenating the header rows).
COLUMNS = [
    "country",
    "Percent of consumer expenditures spent on food, alcoholic beverages, and tobacco that were consumed at home, by selected countries - Share of consumer expenditures on food2 - Percent",
    "Percent of consumer expenditures spent on food, alcoholic beverages, and tobacco that were consumed at home, by selected countries - Share of consumer expenditures on alcoholic beverages and tobacco - Percent",
    "Percent of consumer expenditures spent on food, alcoholic beverages, and tobacco that were consumed at home, by selected countries - Consumer expenditures3 - U.S. dollars per person",
    "Percent of consumer expenditures spent on food, alcoholic beverages, and tobacco that were consumed at home, by selected countries - Expenditures on food2 - U.S. dollars per person",
]


def combine_data_sheets(data: pd.ExcelFile) -> Table:
    # Initialize empty dataframe that will gather data for all sheets.
    combined = Table()
    for sheet_name in sorted(data.sheet_names):
        # Parse sheet for the current year.
        tb = data.parse(sheet_name, skiprows=3, header=None, names=COLUMNS)  # type: ignore

        # As a sanity check.
        columns_new = data.parse(sheet_name, skiprows=0, header=[0, 1, 2]).columns  # type: ignore

        # Combine multiline header.
        error = "Column names may have changed."
        assert ["country"] + [
            " - ".join(column).replace(f", {sheet_name}1", "") for column in columns_new[1:]
        ] == COLUMNS, error

        # Rename columns conveniently.
        tb = tb.rename(columns={tb.columns[i]: column for i, column in enumerate(COLUMNS)}, errors="raise")

        # Drop empty rows, and footer rows.
        tb = tb.dropna(subset=[column for column in tb.columns if column != "country"], how="all")

        # Add a year column
        tb = tb.assign(**{"year": int(sheet_name)})

        # Add current data to the combined dataframe.
        combined = pr.concat([combined, tb], ignore_index=False, short_name=paths.short_name)

    return combined


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots and read their data.
    data = {year: paths.load_snapshot(f"food_expenditure_since_{year}.xlsx").ExcelFile() for year in AVAILABLE_YEARS}

    #
    # Process data.
    #
    tables = {}
    for year in AVAILABLE_YEARS:
        tables[year] = combine_data_sheets(data=data[year]).assign(**{"update_year": year})

    # Concatenate tables.
    tb = pr.concat(list(tables.values()), ignore_index=True, short_name=paths.short_name)

    # On repeated years, keep the values from the latest update.
    tb = (
        tb.sort_values(by=["country", "year", "update_year"])
        .drop_duplicates(subset=["country", "year"], keep="last")
        .drop(columns=["update_year"])
        .reset_index(drop=True)
    )

    # Improve table format.
    tb = tb.format(sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
