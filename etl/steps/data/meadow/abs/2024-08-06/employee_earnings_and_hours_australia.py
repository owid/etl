"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_2006 = paths.load_snapshot("employee_earnings_and_hours_australia_2006.csv")
    snap_2008 = paths.load_snapshot("employee_earnings_and_hours_australia_2008.csv")
    snap_2010 = paths.load_snapshot("employee_earnings_and_hours_australia_2010.xls")
    snap_2012 = paths.load_snapshot("employee_earnings_and_hours_australia_2012.xls")
    snap_2014 = paths.load_snapshot("employee_earnings_and_hours_australia_2014.xls")
    snap_2016 = paths.load_snapshot("employee_earnings_and_hours_australia_2016.xls")
    snap_2018 = paths.load_snapshot("employee_earnings_and_hours_australia_2018.xlsx")
    snap_2021 = paths.load_snapshot("employee_earnings_and_hours_australia_2021.xlsx")
    snap_2023 = paths.load_snapshot("employee_earnings_and_hours_australia_2023.xlsx")

    # Load data from snapshot.
    tb_2006 = snap_2006.read()
    tb_2008 = snap_2008.read()
    tb_2010 = snap_2010.read(sheet_name="Table_1", usecols="A,E", skiprows=30, nrows=12)
    tb_2012 = snap_2012.read(sheet_name="Table_2", usecols="A,J", skiprows=118, nrows=12)
    tb_2014 = snap_2014.read(sheet_name="Table_2", usecols="A,J", skiprows=5, nrows=12)
    tb_2016 = snap_2016.read(sheet_name="Table_2", usecols="A,G", skiprows=6, nrows=12)
    tb_2018 = snap_2018.read(sheet_name="Table_2", usecols="A,G", skiprows=6, nrows=12)
    tb_2021 = snap_2021.read(sheet_name="Table_2", usecols="A,G", skiprows=6, nrows=12)
    tb_2023 = snap_2023.read(sheet_name="Table_2", usecols="A,G", skiprows=6, nrows=12)

    #
    # Process data.
    #
    # Format tables. I don't format the 2006 and 2008 tables because they are already in the correct format (they where uploaded manually)
    tb_2010 = format_tables(tb=tb_2010, year=2010)
    tb_2012 = format_tables(tb=tb_2012, year=2012)
    tb_2014 = format_tables(tb=tb_2014, year=2014)
    tb_2016 = format_tables(tb=tb_2016, year=2016)
    tb_2018 = format_tables(tb=tb_2018, year=2018)
    tb_2021 = format_tables(tb=tb_2021, year=2021)
    tb_2023 = format_tables(tb=tb_2023, year=2023)

    # Merge all tables.
    tb = pr.concat([tb_2006, tb_2008, tb_2010, tb_2012, tb_2014, tb_2016, tb_2018, tb_2021, tb_2023], ignore_index=True)

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
    ds_meadow = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_2023.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def format_tables(tb: Table, year: int) -> Table:
    # Define names of columns
    new_column_names = ["indicator", "value"]

    tb.columns = new_column_names
    tb["country"] = "Australia"
    tb["year"] = year

    return tb
