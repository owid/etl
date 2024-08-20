"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define parameters to read tables.
READ_PARAMETERS = {
    2006: {"ext": "csv", "params": {}},
    2008: {"ext": "csv", "params": {}},
    2010: {"ext": "xls", "params": {"sheet_name": "Table_1", "usecols": "A,E", "skiprows": 30, "nrows": 12}},
    2012: {"ext": "xls", "params": {"sheet_name": "Table_2", "usecols": "A,J", "skiprows": 118, "nrows": 12}},
    2014: {"ext": "xls", "params": {"sheet_name": "Table_2", "usecols": "A,J", "skiprows": 5, "nrows": 12}},
    2016: {"ext": "xls", "params": {"sheet_name": "Table_2", "usecols": "A,G", "skiprows": 6, "nrows": 12}},
    2018: {"ext": "xls", "params": {"sheet_name": "Table_2", "usecols": "A,G", "skiprows": 6, "nrows": 12}},
    2021: {"ext": "xlsx", "params": {"sheet_name": "Table_2", "usecols": "A,G", "skiprows": 6, "nrows": 12}},
    2023: {"ext": "xlsx", "params": {"sheet_name": "Table_2", "usecols": "A,G", "skiprows": 6, "nrows": 12}},
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    tbs = []
    for year, values in READ_PARAMETERS.items():
        params = values["params"]
        ext = values["ext"]

        # Load snapshot
        snap = paths.load_snapshot(f"employee_earnings_and_hours_australia_{year}.{ext}")
        # Read snapshot as table
        tb = snap.read(**params)
        # Process table if applciable
        if year not in {2006, 2008}:
            tb = format_tables(tb, year=year)
        # Append to list of tables
        tbs.append(tb)

    # Merge all tables.
    tb = pr.concat(
        tbs,
        short_name="employee_earnings_and_hours_australia",
        ignore_index=True,
    )

    # Make indicator column lowercase.
    tb["indicator"] = tb["indicator"].str.lower()
    # Select only median and 90th percentile.
    tb = tb[tb["indicator"].str.contains("50th percentile|90th percentile")].reset_index(drop=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "indicator"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)  # type: ignore

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def format_tables(tb: Table, year: int) -> Table:
    # Define names of columns
    new_column_names = ["indicator", "value"]

    tb.columns = new_column_names
    tb["country"] = "Australia"
    tb["year"] = year

    return tb
