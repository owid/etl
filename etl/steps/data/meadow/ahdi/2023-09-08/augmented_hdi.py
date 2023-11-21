"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define the sheet names to load from the Excel file
excel_sheets = {
    1: "gdp_per_capita",
    2: "life_expectancy",
    3: "years_of_schooling",
    4: "liberal_democracy_index",
    5: "adjusted_income_index",
    6: "life_expectancy_index",
    7: "schooling_index",
    8: "ahdi",
    9: "ahdi_excluding_income",
    10: "population",
}


def run(dest_dir: str) -> None:
    # Load inputs.

    # Define empty table
    tb_merged = Table()

    for sheet, sheet_name in excel_sheets.items():
        # Retrieve snapshots
        snap = paths.load_snapshot("augmented_hdi.xlsx")
        tb = snap.read(sheet_name=sheet, skiprows=1)

        snap_region = paths.load_snapshot("augmented_hdi_region.xlsx")
        tb_region = snap_region.read(sheet_name=sheet + 1, skiprows=2)

        # Rename first column to country (it has a name)
        tb = tb.rename(columns={tb.columns[0]: "country"}, errors="raise")
        tb_region = tb_region.rename(columns={"Unnamed: 1": "year"}, errors="raise")

        # Remove columns with no data
        tb = tb.dropna(axis=1, how="all")
        tb_region = tb_region.dropna(axis=1, how="all")

        # Remove rows with no data (tb, they are the region separators) for all the columns that are not country
        tb = tb.dropna(axis=0, how="all", subset=tb.columns[1:])

        # Also, drop columns Russia, China and Japan in tb_region, because they have the same values as in the tb table
        tb_region = tb_region.drop(columns=["Russia", "China", "Japan"])

        # Make table long
        tb = tb.melt(id_vars=["country"], var_name="year", value_name=sheet_name)
        tb_region = tb_region.melt(id_vars=["year"], var_name="country", value_name=sheet_name)

        # Concatenate tables
        tb = pr.concat([tb, tb_region], ignore_index=True, sort=True)

        # Merge tables
        if sheet == 1:
            tb_merged = tb
        else:
            tb_merged = pr.merge(
                tb_merged,
                tb,
                on=["country", "year"],
                how="left",
                validate="one_to_one",
            )

    # Drop population column
    tb_merged = tb_merged.drop(columns=["population"])

    # Create a new table and ensure all columns are snake-case.
    tb_merged = tb_merged.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    # Save outputs.
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb_merged],
        check_variables_metadata=True,
        default_metadata=snap.metadata,  # type: ignore
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()
