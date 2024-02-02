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
    snap = paths.load_snapshot("maddison_federico_paper.xlsx")

    # Load data from snapshot.
    tb_africa = snap.read(sheet_name="Africa", skiprows=3)
    tb_americas = snap.read(sheet_name="Americas", skiprows=3)
    tb_asia = snap.read(sheet_name="Asia", skiprows=3)
    tb_europe = snap.read(sheet_name="Europe", skiprows=3)
    tb_oceania = snap.read(sheet_name="Oceania", skiprows=3)

    #
    # Process data.
    #
    ## Format
    tb_africa = process_table(tb_africa, row_end=222)
    tb_americas = process_table(tb_americas, row_end=222)
    tb_asia = process_table(tb_asia, year_max=2010)
    tb_europe = process_table(tb_europe, year_max=2010)
    tb_oceania = process_table(tb_oceania, year_max=1938)

    # Hot fix values in Africa
    tb_africa = tb_africa.loc[
        ~(
            (tb_africa["country"].isin(["Gambia", "Ghana-Gold Coast"]))
            & (tb_africa["year"] <= 1944)
            & (tb_africa["year"] >= 1940)
        )
    ]

    # Define list with tables
    tables = [
        tb_africa,
        tb_americas,
        tb_asia,
        tb_europe,
        tb_oceania,
    ]

    # Merge tables
    tb = pr.concat(tables, ignore_index=True, sort=False)
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Convert thousands into units
    tb["population"] = (tb["population"] * 1000).astype(int)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def process_table(
    tb: Table, row_end: int | None = None, row_start: int = 2, year_min: int = 1800, year_max: int = 2020
) -> Table:
    """Format input data.

    The data is given as a sheet per continent. The sheet has a complex structure, thought for humans and not machines. Hence, it needs some cleaning."""
    # Select subset of rows
    if row_end is not None:
        tb = tb.loc[row_start:row_end]
    else:
        tb = tb.loc[row_start:]
    # Get columns with name "Unnamed: X" (the first one corresponds to "year", the next one splits the population indicator columns from population share indicators, which we don't need here)
    cols_unnamed = [(i, col) for i, col in enumerate(tb.columns) if "Unnamed" in col]
    tb = tb.rename(
        columns={
            cols_unnamed[0][1]: "year",
        },
        errors="raise",
    )
    tb = tb[tb.columns[: cols_unnamed[1][0]]]
    # Drop rows with all-nan
    tb = tb.dropna(how="all", subset=[col for col in tb.columns if col != "year"])
    # Check years
    assert tb["year"].min() == year_min, f"First year is not {year_min}"
    assert tb["year"].max() == year_max, f"Last year is not {year_max}"
    # Unpivot
    tb = tb.melt("year", var_name="country", value_name="population")
    # Types
    tb["year"] = tb["year"].astype(int)
    # Dropnas
    tb = tb.dropna()

    return tb
