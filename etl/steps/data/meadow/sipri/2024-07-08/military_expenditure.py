"""Load a snapshot and create a meadow dataset."""

from typing import Dict, List

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to drop.
COLUMNS_TO_DROP = {
    "constant_usd": ["Notes", "Unnamed: 1"],
    "constant_usd_regions": ["2023 (current prices)", "Unnamed: 38", "Omitted countries"],
    "share_gdp": ["Notes"],
    "per_capita": ["Notes"],
    "share_govt_spending": ["Notes", "Reporting year"],
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("military_expenditure.xlsx")

    # Load data from snapshot.
    tb_constant_usd = snap.read(sheet_name="Constant (2022) US$", skiprows=5, na_values=["...", "xxx"])
    tb_constant_usd_regions = snap.read(sheet_name="Regional totals", skiprows=13, na_values=["...", "xxx"])
    tb_share_gdp = snap.read(sheet_name="Share of GDP", skiprows=5, na_values=["...", "xxx"])
    tb_per_capita = snap.read(sheet_name="Per capita", skiprows=6, na_values=["...", "xxx"])
    tb_share_govt_spending = snap.read(sheet_name="Share of Govt. spending", skiprows=7, na_values=["...", "xxx"])

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_list = remove_columns_and_make_tables_long(
        tb_dict={
            "constant_usd": tb_constant_usd,
            "constant_usd_regions": tb_constant_usd_regions,
            "share_gdp": tb_share_gdp,
            "per_capita": tb_per_capita,
            "share_govt_spending": tb_share_govt_spending,
        }
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tb_list, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def remove_columns_and_make_tables_long(tb_dict: Dict[str, Table]) -> List[Table]:
    """Remove columns from a list of tables and make them long."""

    tb_list = []
    for short_name, tb in tb_dict.items():
        # Drop columns not needed and rename entity identifiers
        tb = tb.drop(columns=COLUMNS_TO_DROP[short_name], errors="raise")
        tb = tb.rename(columns={"Country": "country", "Region": "country"})

        # Make table long
        tb = tb.melt(id_vars=["country"], var_name="year", value_name=short_name)

        # Make year integer
        tb["year"] = tb["year"].astype(int)

        # Remove empty country rows
        tb = tb.dropna(subset=["country"]).reset_index(drop=True)

        # Format table and set short name
        tb = tb.format(["country", "year"], short_name=short_name)

        tb_list.append(tb)

    return tb_list
