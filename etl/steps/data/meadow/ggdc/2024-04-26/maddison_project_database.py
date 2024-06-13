"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Column names and their new names
GDP_PC_AND_POP_COLUMNS = {
    "East Asia": "East Asia",
    "Eastern Europe": "Eastern Europe",
    "Latin America": "Latin America",
    "Middle East and North Africa": "Middle East and North Africa",
    "South and South East Asia": "South and South East Asia",
    "Sub Saharan Africa": "Sub Saharan Africa",
    "Western Europe": "Western Europe",
    "Western Offshoots": "Western offshoots",
    "World GDP pc": "World",
    "Sub Saharan SSA": "Sub Saharan Africa",
    "World Population": "World",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("maddison_project_database.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Full data")
    tb_regions = snap.read(sheet_name="Regional data", skiprows=1)

    #
    # Process data.
    tb_regions = format_regional_data(tb_regions)

    # Concatenate tb and tb_regions
    tb = pr.concat([tb, tb_regions], ignore_index=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def format_regional_data(tb: Table) -> Table:
    """
    Combine GDP pc and population data in the Regional data sheet and make it long.
    """

    tb = tb.copy()

    # Rename first column as "year".
    tb = tb.rename(columns={"Unnamed: 0": "year"})

    # Remove columns with no data.
    tb = tb.dropna(axis=1, how="all")

    # Assert if there a column named Unnamed:9
    assert "Unnamed: 9" in tb.columns, log.fatal("Column Unnamed: 9 not found in the table (expected as World GDP pc).")

    # Rename Unnamed: 9 to World GDP pc.
    tb = tb.rename(columns={"Unnamed: 9": "World GDP pc"})

    # Remove ".1" from column names (generated when column names are duplicated).
    tb = tb.rename(columns={region: region.replace(".1", "") for region in tb.columns})

    # Rename columns to a common format
    tb = tb.rename(columns=GDP_PC_AND_POP_COLUMNS, errors="raise")

    # Assert if there are 19 columns in the table.
    assert len(tb.columns) == 19, log.fatal(f"Expected 19 columns in the table, found {len(tb.columns)}.")

    # For columns 1-9, add _gdppc to the column name. For columns 10-18, add _popbto the column name.
    for i in range(1, 19):
        if i > 0 and i < 10:
            tb.columns.values[i] = tb.columns.values[i] + "_gdppc"
        elif i >= 10:
            tb.columns.values[i] = tb.columns.values[i] + "_pop"

    # Make the table long.
    tb = tb.melt(id_vars="year", var_name="country", value_name="value")

    # Split the column name into two columns.
    tb[["country", "indicator"]] = tb["country"].str.split("_", expand=True)

    # Make table wide
    tb = tb.pivot(
        index=["country", "year"],
        columns="indicator",
        values="value",
    ).reset_index()

    return tb
