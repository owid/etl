"""Load a snapshot and create a meadow dataset."""

from pathlib import Path

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def find_number_of_lines_to_skip(file_name: Path) -> int:
    """Find number of lines to skip in file."""
    temp = pr.read_excel(file_name, header=None)
    line_number = [i for i, line in enumerate(temp[temp.columns[0]]) if str(line).strip().lower() == "country"]
    assert len(line_number) == 1, "Unable to find how many lines to skip in file {file_name}."
    return line_number[0]


def process_yearly_data(tb_i: Table, year: int) -> Table:
    # Remove empty rows and use last row for a global total.
    tb_i = tb_i.dropna(how="all")
    assert len(tb_i[tb_i["Country"].isnull()]) < 2, "At most one row was expected to be empty."
    tb_i["Country"] = tb_i["Country"].fillna("Totals")
    # Add a year to the row of total.
    tb_i.loc[tb_i["Country"] == "Totals", "Year"] = year

    # Ensure year column has the right format.
    tb_i = tb_i.astype({"Year": int})

    # Rename columns.
    tb_i = tb_i.rename(
        columns={
            "Numbers (lower) millions": "Estimated numbers (millions) lower",
            "Numbers (upper) millions": "Estimated numbers (millions) upper",
            # It is unclear whether the following mapping is correct, but we will not use these columns anyway.
            "mean weight (lower)": "Weighted estimated mean weight (lower) (same as L except for some Rainbow trout)",
            "mean weight (upper)": "Weighted estimated mean weight (upper) (same as L except for some Rainbow trout)",
        },
        errors="ignore",
    )

    # Ensure all columns are underscore.
    tb_i = tb_i.underscore()

    return tb_i


def run(dest_dir: str) -> None:
    #
    # Load and process inputs.
    #
    # Retrieve and process snapshots.
    tb = Table(short_name=paths.short_name)
    for year in [2015, 2016, 2017]:
        snap_i = paths.load_snapshot(f"number_of_farmed_fish_{year}.xlsx")
        tb_i = snap_i.read_excel(skiprows=find_number_of_lines_to_skip(snap_i.path))
        # Process yearly data.
        tb_i = process_yearly_data(tb_i=tb_i, year=year)
        # Combine all tables.
        tb = pr.concat([tb, tb_i], ignore_index=True)

    # Set an appropriate index and sort conveniently.
    tb = (
        tb.set_index(["country", "year", "fao_species_category"], verify_integrity=True).sort_index().sort_index(axis=1)
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
